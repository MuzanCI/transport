use serde::{Deserialize, Serialize};

pub mod channel;
pub mod codec;
pub mod mux;

/// The HTTP Upgrade header value for the MuzanCI transport protocol.
pub const MUZANCI_TRANSPORT_V1: &str = "muzanci-transport/v1";

/// The HTTP header for the runner ID, sent by the server upon successful upgrade to the MuzanCI transport protocol.
/// The value must be a valid [`RunnerId`].
pub const MUZANCI_RUNNER_ID_HEADER: &str = "X-MuzanCI-Runner-ID";

pub type RunnerId = uuid::Uuid;
pub type WorkerId = uuid::Uuid;

/// A secret to be injected into a step's environment variables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Secret {
    pub name: String,
    pub key: String,
}

/// A step to be executed in a job sandbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    pub name: String,
    pub command: String,
    pub secrets: Vec<Secret>,
}

/// A rule for when a pipeline should be created.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Rule {
    Push {
        include_branches: Option<Vec<String>>,
        exclude_branches: Option<Vec<String>>,
        include_tags: Option<Vec<String>>,
        exclude_tags: Option<Vec<String>>,
        include_paths: Option<Vec<String>>,
        exclude_paths: Option<Vec<String>>,
    },
    PullRequest {
        include_branches: Option<Vec<String>>,
        exclude_branches: Option<Vec<String>>,
        include_paths: Option<Vec<String>>,
        exclude_paths: Option<Vec<String>>,
    },
}

pub type JobId = uuid::Uuid;

/// A sequence of steps that execute in an isolated sandbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub job_id: JobId,
    pub name: String,
    pub steps: Vec<Step>,
    pub depends_on: Vec<JobId>,
}

pub type PipelineId = uuid::Uuid;

/// A set of target jobs and a set of rules for when the pipeline should be created.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub pipeline_id: PipelineId,
    pub name: String,
    pub when: Vec<Rule>,
    pub targets: Vec<JobId>,
}
