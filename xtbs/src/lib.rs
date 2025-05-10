use std::{
    collections::{HashMap, hash_map::Entry},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        mpsc::{Receiver, Sender},
    },
};

pub use anyhow;
pub use serde;

pub trait IntoAnyhowResult<T> {
    fn into_anyhow_result(self) -> anyhow::Result<T>;
}

impl<T> IntoAnyhowResult<T> for anyhow::Result<T> {
    fn into_anyhow_result(self) -> anyhow::Result<T> {
        self.map_err(Into::into)
    }
}

impl<T> IntoAnyhowResult<T> for T {
    fn into_anyhow_result(self) -> anyhow::Result<T> {
        Ok(self)
    }
}

#[derive(Clone, Default)]
struct NodeRelation {
    input_edges: Vec<JobId>,
    output_edges: Vec<JobId>,
}

type BoxJobExec<'job> = Box<dyn FnOnce(&mut Job) -> anyhow::Result<()> + Send + 'job>;

pub struct TaskGraph<'job> {
    jobs: Vec<Mutex<Job>>,
    exec: Vec<Option<BoxJobExec<'job>>>,
}

impl<'job> TaskGraph<'job> {
    pub fn new() -> Self {
        TaskGraph {
            jobs: Vec::new(),
            exec: Vec::new(),
        }
    }

    pub fn add_job<F, R>(&mut self, spec: Job, f: F) -> JobId
    where
        F: FnOnce(&mut Job) -> R + Send + 'job,
        R: IntoAnyhowResult<()>,
    {
        let id = JobId(self.jobs.len());

        self.jobs.push(Mutex::new(spec));
        self.exec.push(Some(Box::new(move |job| {
            let result = f(job);
            result.into_anyhow_result()
        })));

        id
    }

    pub fn run(mut self) -> anyhow::Result<()> {
        let relations = self.build_relations()?;

        let (mut unresolved_input_counts, ready_jobs) = self.build_input_counts(&relations);

        // Channel to communicate finished jobs back to the main thread
        let (sender, receiver) = std::sync::mpsc::channel::<(JobId, anyhow::Result<()>)>();

        let results = rayon::in_place_scope(|scope| {
            self.run_scope(
                &relations,
                &mut unresolved_input_counts,
                ready_jobs,
                &sender,
                receiver,
                scope,
            )
        });

        // If there were any errors, print them out, then return an error.
        let mut any_failed = false;
        for (job_id, result) in results.into_iter().enumerate() {
            if let Some(Err(err)) = result {
                any_failed = true;
                eprintln!(
                    "Job \"{}\"(ID {}) failed: {:#?}",
                    self.jobs[job_id].get_mut().unwrap().name,
                    job_id,
                    err
                );
            }
        }

        if any_failed {
            return Err(anyhow::anyhow!("One or more jobs failed."));
        }

        // If there are any jobs that still have unresolved input edges,
        // there was a cycle in the graph.
        for (job_id, count) in unresolved_input_counts.iter().enumerate() {
            if *count > 0 {
                return Err(anyhow::anyhow!(
                    "Job \"{}\"(ID {}) was part of a cycle in the graph.",
                    self.jobs[job_id].get_mut().unwrap().name,
                    job_id,
                ));
            }
        }

        Ok(())
    }

    fn build_relations(&mut self) -> anyhow::Result<Vec<NodeRelation>> {
        let mut relations: Vec<NodeRelation> = vec![NodeRelation::default(); self.jobs.len()];
        let mut output_files: HashMap<PathBuf, JobId> = HashMap::with_capacity(self.jobs.len());

        // First iterate over all outputs, ensuring there are no duplicate outputs,
        // and building the mapping from path to job id.
        for (index, job) in self.jobs.iter_mut().enumerate() {
            let job_id = JobId(index);
            let job = job.get_mut().unwrap();

            for output in &job.outputs {
                match output {
                    JobOutput::File(path) => match output_files.entry(path.clone()) {
                        Entry::Occupied(_) => {
                            return Err(anyhow::anyhow!(
                                "Duplicate output file: {}",
                                path.display()
                            ));
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(job_id);
                        }
                    },
                    JobOutput::Data(_) => {
                        // Data outputs don't create edges
                    }
                }
            }
        }

        // Now iterate over all inputs, connecting them with the outputs.
        for (index, job) in self.jobs.iter_mut().enumerate() {
            let job_id = JobId(index);
            let job = job.get_mut().unwrap();

            for output in &job.inputs {
                match output {
                    JobInput::File(path) => {
                        if let Some(&matching_node) = output_files.get(path) {
                            relations[matching_node.0].output_edges.push(job_id);
                            relations[job_id.0].input_edges.push(matching_node);
                        }
                        // If there is no match, that's fine, the input is
                        // used for hashing, not dependency ordering.
                    }
                    JobInput::Job(matching_node) => {
                        relations[matching_node.0].output_edges.push(job_id);
                        relations[job_id.0].input_edges.push(*matching_node);
                    }
                    JobInput::Data(_) | JobInput::Always => {
                        // Data and always inputs don't create edges
                    }
                }
            }
        }
        Ok(relations)
    }

    fn build_input_counts(&self, relations: &Vec<NodeRelation>) -> (Vec<usize>, Vec<JobId>) {
        // Build a count of all the unresolved input edges for every job
        let mut unresolved_input_counts: Vec<usize> = Vec::with_capacity(self.jobs.len());
        let mut ready_jobs: Vec<JobId> = Vec::with_capacity(self.jobs.len());

        for (id, relation) in relations.iter().enumerate() {
            let job_id = JobId(id);

            let input_edges = relation.input_edges.len();
            unresolved_input_counts.push(input_edges);
            if input_edges == 0 {
                ready_jobs.push(job_id);
            }
        }
        (unresolved_input_counts, ready_jobs)
    }

    fn run_scope<'scope>(
        &'scope mut self,
        relations: &Vec<NodeRelation>,
        unresolved_input_counts: &mut Vec<usize>,
        mut ready_jobs: Vec<JobId>,
        sender: &'scope Sender<(JobId, anyhow::Result<()>)>,
        receiver: Receiver<(JobId, anyhow::Result<()>)>,
        scope: &impl Scope<'scope>,
    ) -> Vec<Option<anyhow::Result<()>>>
    where
        'job: 'scope,
    {
        let mut active_jobs = 0_usize;
        let mut results = Vec::new();
        results.resize_with(self.jobs.len(), || None);

        // Launch all the ready to run jobs
        for job_id in ready_jobs.drain(..) {
            Self::launch_job(
                &self.jobs,
                &mut self.exec,
                &mut active_jobs,
                scope,
                sender,
                job_id,
            );
        }

        // Wait for jobs to finish
        while let Some((job_id, result)) = {
            // If there are no more active jobs, we can break out of the loop
            if active_jobs != 0 {
                receiver.recv().ok()
            } else {
                None
            }
        } {
            // Decrement the active job count
            active_jobs -= 1;

            let errored = result.is_err();

            // Store the result
            results[job_id.0] = Some(result);

            if errored {
                continue;
            }

            // Decrement the unresolved input counts for all jobs that depend on this job
            for output_job_id in &relations[job_id.0].output_edges {
                let count = &mut unresolved_input_counts[output_job_id.0];
                *count -= 1;
                if *count == 0 {
                    // This job is now ready to run
                    ready_jobs.push(*output_job_id);
                }
            }

            // Launch all the ready to run jobs
            for job_id in ready_jobs.drain(..) {
                Self::launch_job(
                    &self.jobs,
                    &mut self.exec,
                    &mut active_jobs,
                    scope,
                    &sender,
                    job_id,
                );
            }
        }
        results
    }

    fn launch_job<'scope>(
        jobs: &'scope Vec<Mutex<Job>>,
        exec: &mut Vec<Option<BoxJobExec<'job>>>,
        active_jobs: &mut usize,
        scope: &impl Scope<'scope>,
        sender: &'scope Sender<(JobId, anyhow::Result<()>)>,
        job_id: JobId,
    ) where
        'job: 'scope,
    {
        *active_jobs += 1;
        let exec = exec[job_id.0].take().unwrap();

        scope.spawn(move || {
            let mut job = jobs[job_id.0].try_lock().unwrap();

            let result = exec(&mut *job);
            sender.send((job_id, result)).unwrap();
        });
    }
}

trait Scope<'scope> {
    fn spawn(&self, f: impl FnOnce() + Send + 'scope);
}

impl<'scope> Scope<'scope> for rayon::Scope<'scope> {
    fn spawn(&self, f: impl FnOnce() + Send + 'scope) {
        self.spawn(move |_| f());
    }
}

struct NoopScope;

impl<'scope> Scope<'scope> for NoopScope {
    fn spawn(&self, f: impl FnOnce() + Send + 'scope) {
        f();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobId(usize);

pub struct Job {
    pub name: String,
    pub inputs: Vec<JobInput>,
    pub outputs: Vec<JobOutput>,
}

pub enum JobInput {
    // Use the contents of the file as input to the job. Will automatically
    // match up to outputs of other jobs that output to this file.
    //
    // If the hash of the file is changed, the job will be re-run.
    File(PathBuf),
    // Use the contents of the vector as input to the job.
    //
    // If the hash of the contained data is changed, the job will be re-run.
    Data(Vec<u8>),
    // Depend explicitly on another job without any caching behavior. If
    // any of the job outputs change, this job will be re-run.
    Job(JobId),
    // Always run this job.
    Always,
}

impl JobInput {
    pub fn from_file(path: impl AsRef<Path>) -> Self {
        JobInput::File(path.as_ref().to_path_buf())
    }

    pub fn from_data(data: impl AsRef<[u8]>) -> Self {
        JobInput::Data(data.as_ref().to_vec())
    }

    pub fn from_job(id: JobId) -> Self {
        JobInput::Job(id)
    }

    pub fn always() -> Self {
        JobInput::Always
    }
}

pub enum JobOutput {
    // Use the contents of the file as output of the job. Will automatically
    // match up to inputs of other jobs that depend on this file.
    File(PathBuf),
    // Use the contents of the vector as output of the job.
    Data(Vec<u8>),
}

impl JobOutput {
    pub fn from_file(path: impl AsRef<Path>) -> Self {
        JobOutput::File(path.as_ref().to_path_buf())
    }

    pub fn from_data(data: impl AsRef<[u8]>) -> Self {
        JobOutput::Data(data.as_ref().to_vec())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_graph() {
        use super::*;

        let mut graph = TaskGraph::new();

        let job1 = Job {
            name: "job1".to_string(),
            inputs: vec![],
            outputs: vec![JobOutput::from_file("output1.txt")],
        };
        graph.add_job(job1, |_| {});

        let job2 = Job {
            name: "job2".to_string(),
            inputs: vec![JobInput::from_file("output1.txt")],
            outputs: vec![JobOutput::from_file("output2.txt")],
        };
        graph.add_job(job2, |_| {});

        let relations = graph.build_relations().unwrap();

        assert_eq!(relations.len(), 2);

        assert_eq!(relations[0].input_edges, &[]);
        assert_eq!(relations[0].output_edges, &[JobId(1)]);

        assert_eq!(relations[1].input_edges, &[JobId(0)]);
        assert_eq!(relations[1].output_edges, &[]);

        let (input_counts, ready_jobs) = graph.build_input_counts(&relations);

        assert_eq!(input_counts, &[0, 1]);
        assert_eq!(ready_jobs, vec![JobId(0)]);
    }
}
