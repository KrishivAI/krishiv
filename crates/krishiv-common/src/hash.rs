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
    // `hex::encode`, not `format!("{hash:x}")`: sha2 0.11 moved to digest 0.11,
    // whose `Array<u8, N>` (hybrid-array) does not implement `LowerHex` the way
    // the old `GenericArray` did. Both produce the same lowercase, unpadded,
    // 64-character encoding, so hashes computed before and after this change
    // are identical — which matters, because these strings are persisted.
    hex::encode(Sha256::digest(data))
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

    /// A non-empty known-answer vector. `sha256_hex_empty` alone is degenerate —
    /// it would still pass if the encoder mishandled content — and these digests
    /// are persisted, so the sha2 0.11 move from `GenericArray`+`{:x}` to
    /// `Array`+`hex::encode` had to be proven value-identical, not just
    /// well-formed.
    #[test]
    fn sha256_hex_matches_the_published_vector_for_non_empty_input() {
        assert_eq!(
            sha256_hex(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn sha256_bytes_multi_deterministic() {
        let multi = sha256_bytes_multi(&[b"hello ", b"world"]);
        let expected = sha256_bytes_multi(&[b"hello ", b"world"]);
        assert_eq!(multi, expected);
    }
}
