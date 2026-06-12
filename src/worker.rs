pub type WorkerId = uuid::Uuid;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerConfig {
    /// The worker identifier.
    worker_id: WorkerId,

    /// The worker capacity to consume for this worker.
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

    pub fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    pub fn worker_capacity(&self) -> u64 {
        self.worker_capacity
    }

    pub fn access_token(&self) -> &str {
        &self.access_token
    }

    pub fn repo_owner(&self) -> &str {
        &self.repo_owner
    }

    pub fn repo_name(&self) -> &str {
        &self.repo_name
    }

    pub fn commit_sha(&self) -> &str {
        &self.commit_sha
    }

    pub fn exec(&self) -> &str {
        &self.exec
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WorkerEvent {
    Started,
    Completed,
    Failed,
}
