//! Canonical SHA-256 hashing utilities.
//!
//! All crates that need SHA-256 should import from here instead of
//! reimplementing the digest + hex-encoding pattern.

use sha2::{Digest, Sha256};

/// Compute the lowercase hex-encoded SHA-256 digest of `data`.
///
/// ```
/// let hash = krishiv_common::hash::sha256_hex(b"hello");
/// assert_eq!(hash.len(), 64);
/// assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
/// ```
pub fn sha256_hex(data: &[u8]) -> String {
    let hash = Sha256::digest(data);
    format!("{hash:x}")
}

/// Compute the raw SHA-256 digest of `data` as a `[u8; 32]`.
pub fn sha256_bytes(data: &[u8]) -> [u8; 32] {
    let hash = Sha256::digest(data);
    hash.into()
}

/// Compute a truncated SHA-256 dedup key: first 8 bytes of the digest as a
/// `u64` (little-endian).
///
/// Useful for dedup maps where full 32-byte hashes are unnecessary.
pub fn sha256_dedup_key(data: &[u8]) -> u64 {
    let hash = Sha256::digest(data);
    u64::from_le_bytes(hash[..8].try_into().expect("sha256 is at least 8 bytes"))
}

/// Incrementally hash multiple byte slices and return the hex-encoded digest.
///
/// ```
/// let h1 = krishiv_common::hash::sha256_hex(b"abc");
/// let h2 = krishiv_common::hash::sha256_hex_multi(&[b"abc"]);
/// assert_eq!(h1, h2);
/// ```
pub fn sha256_hex_multi(inputs: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    for input in inputs {
        hasher.update(input);
    }
    format!("{:x}", hasher.finalize())
}

/// Incrementally hash multiple byte slices and return the raw `[u8; 32]`.
pub fn sha256_bytes_multi(inputs: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for input in inputs {
        hasher.update(input);
    }
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_hex_deterministic() {
        let a = sha256_hex(b"test data");
        let b = sha256_hex(b"test data");
        assert_eq!(a, b);
    }

    #[test]
    fn sha256_hex_different_inputs() {
        let a = sha256_hex(b"hello");
        let b = sha256_hex(b"world");
        assert_ne!(a, b);
    }

    #[test]
    fn sha256_hex_empty() {
        // SHA-256 of empty input is a known constant
        let hash = sha256_hex(b"");
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn sha256_bytes_matches_hex() {
        let data = b"consistency check";
        let from_hex = sha256_hex(data);
        let from_bytes = sha256_bytes(data);
        assert_eq!(from_hex, hex::encode(from_bytes));
    }

    #[test]
    fn sha256_dedup_key_deterministic() {
        let a = sha256_dedup_key(b"audit-event");
        let b = sha256_dedup_key(b"audit-event");
        assert_eq!(a, b);
    }

    #[test]
    fn sha256_dedup_key_different() {
        let a = sha256_dedup_key(b"event-1");
        let b = sha256_dedup_key(b"event-2");
        assert_ne!(a, b);
    }

    #[test]
    fn sha256_hex_multi_matches_single() {
        let single = sha256_hex(b"hello world");
        let multi = sha256_hex_multi(&[b"hello ", b"world"]);
        assert_eq!(single, multi);
    }

    #[test]
    fn sha256_bytes_multi_matches_single() {
        let single = sha256_bytes(b"hello world");
        let multi = sha256_bytes_multi(&[b"hello ", b"world"]);
        assert_eq!(single, multi);
    }
}
