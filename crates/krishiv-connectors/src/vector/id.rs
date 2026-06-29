use krishiv_common::hash::sha256_bytes_multi;

/// Deterministic point id per ADR-R17.3: SHA-256(doc_id || epoch), truncated to u64.
pub fn point_id_from_doc_epoch(doc_id: &str, epoch: u64) -> String {
    let digest = sha256_bytes_multi(&[doc_id.as_bytes(), &epoch.to_le_bytes()]);
    let truncated = u64::from_le_bytes(digest[..8].try_into().expect("8 bytes"));
    format!("{truncated:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_id_is_stable() {
        let a = point_id_from_doc_epoch("doc-1", 42);
        let b = point_id_from_doc_epoch("doc-1", 42);
        assert_eq!(a, b);
        assert_ne!(a, point_id_from_doc_epoch("doc-1", 43));
    }
}
