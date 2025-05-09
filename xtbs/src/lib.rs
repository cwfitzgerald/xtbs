use std::{
    collections::{HashMap, hash_map::Entry},
    path::{Path, PathBuf},
};

pub use anyhow;
pub use serde;

struct GraphNode<'job> {
    job: Job<'job>,
    input_edges: Vec<JobId>,
    output_edges: Vec<JobId>,
}

pub struct TaskGraph<'job> {
    jobs: Vec<Job<'job>>,
}

impl<'job> TaskGraph<'job> {
    pub fn new() -> Self {
        TaskGraph { jobs: Vec::new() }
    }

    pub fn add_job<F>(&mut self, spec: JobSpec, f: F) -> JobId
    where
        F: FnOnce(&mut JobSpec) -> anyhow::Result<()> + Send + 'job,
    {
        let id = JobId(self.jobs.len());

        self.jobs.push(Job {
            spec,
            exec: Box::new(f),
        });

        id
    }

    pub fn run(self) -> anyhow::Result<()> {
        let mut graph: Vec<GraphNode> = Vec::with_capacity(self.jobs.len());
        let mut output_files: HashMap<PathBuf, JobId> = HashMap::with_capacity(self.jobs.len());

        for job in self.jobs {
            let job_id = JobId(graph.len());

            let mut this_node = GraphNode {
                job,
                input_edges: Vec::new(),
                output_edges: Vec::new(),
            };

            // First iterate over all inputs to connect them to the
            // outputs of other jobs.
            for input in &this_node.job.spec.inputs {
                match input {
                    JobInput::File(path) => {
                        if let Some(&matching_node) = output_files.get(path) {
                            this_node.input_edges.push(matching_node);
                            graph[matching_node.0].output_edges.push(job_id);
                        }
                    }
                    JobInput::Job(matching_node) => {
                        this_node.input_edges.push(*matching_node);
                        graph[matching_node.0].output_edges.push(job_id);
                    }
                    JobInput::Data(_) | JobInput::Always => {
                        // Data and always inputs don't create edges
                    }
                }
            }

            // Iterate over all outputs to ensure that there are no double outputs,
            // and to connect them to the inputs of other jobs.
            for output in &this_node.job.spec.outputs {
                match output {
                    JobOutput::File(path) => {
                        if let Some(&matching_node) = output_files.get(path) {
                            // We have more than one job that outputs to this file,
                            // so we treat it as both an input and an output.
                            this_node.input_edges.push(matching_node);
                            graph[matching_node.0].output_edges.push(job_id);
                        }
                        // Insert our output into the map, overwriting any
                        // existing entry. Because we establish an input edge,
                        // all future nodes only need to attach to us to get defined
                        // ordering.
                        output_files.insert(path.clone(), job_id);
                    }
                    JobOutput::Data(_) => {
                        // Data outputs don't create edges
                    }
                }
            }

            graph.push(this_node);
        }

        // Find any cycles in the graph and error out if we find one.

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobId(usize);

pub struct Job<'job> {
    spec: JobSpec,
    exec: Box<dyn FnOnce(&mut JobSpec) -> anyhow::Result<()> + Send + 'job>,
}

pub struct JobSpec {
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
