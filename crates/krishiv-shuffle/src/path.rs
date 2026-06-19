/// Identifies a shuffle partition on disk.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShufflePath {
    /// Job identifier.
    pub job_id: String,
    /// Stage identifier.
    pub stage_id: String,
    /// Partition index within the stage.
    pub partition_id: u32,
}

impl ShufflePath {
    pub fn new(job_id: impl Into<String>, stage_id: impl Into<String>, partition_id: u32) -> Self {
        Self {
            job_id: job_id.into(),
            stage_id: stage_id.into(),
            partition_id,
        }
    }

    /// Returns the staging path: `{job_id}/{stage_id}/{partition_id}.tmp`
    pub fn staging_name(&self) -> String {
        format!(
            "{}/{}/{}.tmp",
            self.job_id, self.stage_id, self.partition_id
        )
    }

    /// Returns the final path: `{job_id}/{stage_id}/{partition_id}.ipc`
    pub fn final_name(&self) -> String {
        format!(
            "{}/{}/{}.ipc",
            self.job_id, self.stage_id, self.partition_id
        )
    }
}
