//! Per-job coordinator (JCP) pod naming and manifest helpers (WS-7.3).

use krishiv_proto::JobId;

/// gRPC port used by JCP pods.
pub const JCP_GRPC_PORT: u16 = 9091;

/// Maximum length of a Kubernetes resource name (RFC 1123 DNS label).
const K8S_NAME_MAX_LEN: usize = 63;

/// Kubernetes-safe JCP pod name for a job.
///
/// Rules applied:
/// - Lowercased.
/// - Non-alphanumeric characters replaced with `-`.
/// - Consecutive hyphens collapsed to one.
/// - Leading/trailing hyphens stripped from the sanitized suffix.
/// - Result prefixed with `krishiv-jcp-` and truncated to 63 characters.
pub fn jcp_pod_name(job_id: &JobId) -> String {
    // Sanitize: lowercase + replace non-alphanumeric with '-'.
    let raw: String = job_id
        .as_str()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();

    // Collapse runs of '-' and trim edge hyphens.
    let mut collapsed = String::with_capacity(raw.len());
    let mut prev_hyphen = false;
    for c in raw.chars() {
        if c == '-' {
            if !prev_hyphen && !collapsed.is_empty() {
                collapsed.push('-');
            }
            prev_hyphen = true;
        } else {
            collapsed.push(c);
            prev_hyphen = false;
        }
    }
    let suffix = collapsed.trim_end_matches('-');

    let full = format!("krishiv-jcp-{suffix}");
    if full.len() <= K8S_NAME_MAX_LEN {
        full
    } else {
        full[..K8S_NAME_MAX_LEN].trim_end_matches('-').to_owned()
    }
}

/// gRPC endpoint for a JCP pod in `namespace`.
pub fn jcp_grpc_endpoint(namespace: &str, job_id: &JobId) -> String {
    format!(
        "http://{}.{}.svc.cluster.local:{JCP_GRPC_PORT}",
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

    #[test]
    fn jcp_pod_name_lowercases() {
        let id = JobId::try_new("MyJob").unwrap();
        assert_eq!(jcp_pod_name(&id), "krishiv-jcp-myjob");
    }

    #[test]
    fn jcp_pod_name_collapses_consecutive_hyphens() {
        let id = JobId::try_new("ns..job").unwrap();
        assert_eq!(jcp_pod_name(&id), "krishiv-jcp-ns-job");
    }

    #[test]
    fn jcp_pod_name_truncates_long_ids() {
        // 60-char job id — "krishiv-jcp-" is 12 chars, so total would exceed 63.
        let long_id = "a".repeat(60);
        let id = JobId::try_new(&long_id).unwrap();
        let name = jcp_pod_name(&id);
        assert!(name.len() <= 63, "name too long: {} chars", name.len());
        assert!(!name.ends_with('-'));
    }
}
