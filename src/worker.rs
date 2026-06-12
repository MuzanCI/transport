/// An identifier for a worker.
pub type WorkerId = uuid::Uuid;

/// A configuration for a worker.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerConfig {
    /// The worker identifier.
    worker_id: WorkerId,

    /// The capacity to consume for this worker.
    worker_capacity: u64,

    /// GitHub App installation access token.
    /// Used for fetching code contents and updating checks.
    access_token: String,

    /// The GitHub repository owner.
    repo_owner: String,

    /// The GitHub repository name.
    repo_name: String,

    /// The specific commit to checkout and run the job on.
    commit_sha: String,

    /// The command to execute from the repository root directory.
    exec: String,
}

impl WorkerConfig {
    /// Constructs a new [`WorkerConfig`].
    pub fn new(
        worker_id: WorkerId,
        worker_capacity: u64,
        access_token: String,
        repo_owner: String,
        repo_name: String,
        commit_sha: String,
        exec: String,
    ) -> Self {
        WorkerConfig {
            worker_id,
            worker_capacity,
            access_token,
            repo_owner,
            repo_name,
            commit_sha,
            exec,
        }
    }

    /// Returns the [`WorkerId`].
    pub fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    /// Returns the worker capacity.
    pub fn worker_capacity(&self) -> u64 {
        self.worker_capacity
    }

    /// Returns the GitHub App installation access token.
    pub fn access_token(&self) -> &str {
        &self.access_token
    }

    /// Returns the GitHub repository owner.
    pub fn repo_owner(&self) -> &str {
        &self.repo_owner
    }

    /// Returns the GitHub repository name.
    pub fn repo_name(&self) -> &str {
        &self.repo_name
    }

    /// Returns the specific commit to checkout and run the job on.
    pub fn commit_sha(&self) -> &str {
        &self.commit_sha
    }

    /// Returns the command to execute from the repository root directory.
    pub fn exec(&self) -> &str {
        &self.exec
    }
}

/// An event in the lifecycle of a worker.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WorkerEvent {
    Started,
    Completed,
    Failed,
}
