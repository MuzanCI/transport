/// An identifier for a job.
pub type JobId = uuid::Uuid;

/// A job that is ready to be acquired by a runner.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AvailableJob {
    job_id: JobId,
    runner_capacity_required: u64,
}

impl AvailableJob {
    /// Constructs a new [`AvailableJob`].
    pub fn new(job_id: JobId, runner_capacity_required: u64) -> Self {
        AvailableJob {
            job_id,
            runner_capacity_required,
        }
    }

    /// Returns the [`JobId`] of this available job.
    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    /// Returns the runner capacity required to acquire this job.
    pub fn runner_capacity_required(&self) -> u64 {
        self.runner_capacity_required
    }
}
