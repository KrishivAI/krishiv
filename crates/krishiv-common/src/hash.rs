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
#[must_use]
pub fn sha256_hex(data: &[u8]) -> String {
    let hash = Sha256::digest(data);
    format!("{hash:x}")
}

/// Incrementally hash multiple byte slices and return the raw `[u8; 32]`.
#[must_use]
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
    fn sha256_bytes_multi_deterministic() {
        let multi = sha256_bytes_multi(&[b"hello ", b"world"]);
        let expected = sha256_bytes_multi(&[b"hello ", b"world"]);
        assert_eq!(multi, expected);
    }
}
