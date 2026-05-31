#![forbid(unsafe_code)]

//! Shared utilities for the Krishiv workspace.
//!
//! Provides canonical implementations of:
//! - SHA-256 hashing (`hash` module)
//! - Identifier and path validation (`validate` module)

pub mod arrow;
pub mod async_util;
pub mod blocking;
#[cfg(feature = "chaos")]
pub mod chaos;
pub mod durability;
pub mod hash;
pub mod validate;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_validate_independent() {
        let h = hash::sha256_hex(b"hello");
        assert_eq!(h.len(), 64);
        assert!(validate::is_safe_identifier("my-table"));
    }
}
