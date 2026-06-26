//! Durable shuffle partition lease token encoding and monotonic validation.

use crate::{ShuffleError, ShuffleResult};

/// Encode a lease token as 8 little-endian bytes.
pub fn encode_lease_token(token: u64) -> Vec<u8> {
    token.to_le_bytes().to_vec()
}

/// Decode a persisted lease token sidecar.
pub fn decode_lease_token(bytes: &[u8]) -> Option<u64> {
    let arr: [u8; 8] = bytes.get(..8)?.try_into().ok()?;
    Some(u64::from_le_bytes(arr))
}

/// Validate monotonic lease advancement.
pub fn enforce_monotonic_lease(current: Option<u64>, incoming: u64) -> ShuffleResult<u64> {
    if let Some(expected) = current
        && incoming < expected
    {
        return Err(ShuffleError::StaleLeaseToken {
            expected,
            actual: incoming,
        });
    }
    Ok(incoming.max(current.unwrap_or(0)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_token_round_trip() {
        let token = 42_u64;
        assert_eq!(decode_lease_token(&encode_lease_token(token)), Some(token));
    }

    #[test]
    fn monotonic_lease_rejects_stale_token() {
        let err = enforce_monotonic_lease(Some(9), 8).unwrap_err();
        assert!(matches!(
            err,
            ShuffleError::StaleLeaseToken {
                expected: 9,
                actual: 8
            }
        ));
    }
}
