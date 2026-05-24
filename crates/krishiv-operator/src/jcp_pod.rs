//! Per-job coordinator (JCP) pod naming and manifest helpers (WS-7.3).

use krishiv_proto::JobId;

/// Kubernetes-safe JCP pod name for a job.
pub fn jcp_pod_name(job_id: &JobId) -> String {
    let safe = job_id
        .as_str()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>();
    format!("krishiv-jcp-{safe}")
}

/// gRPC endpoint for a JCP pod in `namespace`.
pub fn jcp_grpc_endpoint(namespace: &str, job_id: &JobId) -> String {
    format!(
        "http://{}.{}.svc.cluster.local:9091",
        jcp_pod_name(job_id),
        namespace
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_proto::JobId;

    #[test]
    fn jcp_pod_name_sanitizes_job_id() {
        let id = JobId::try_new("job/batch_1").unwrap();
        assert_eq!(jcp_pod_name(&id), "krishiv-jcp-job-batch-1");
    }
}
