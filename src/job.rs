pub type JobId = uuid::Uuid;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AvailableJob {
    job_id: JobId,
    runner_capacity_required: u64,
}

impl AvailableJob {
    pub fn new(job_id: JobId, runner_capacity_required: u64) -> Self {
        AvailableJob {
            job_id,
            runner_capacity_required,
        }
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn runner_capacity_required(&self) -> u64 {
        self.runner_capacity_required
    }
}
