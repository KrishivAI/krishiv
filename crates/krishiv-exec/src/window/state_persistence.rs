//! Shared state persistence helpers for window operators.
//!
//! This module provides generic persist/restore functions that eliminate
//! code duplication across tumbling, sliding, and session window operators.

use std::collections::HashMap;

use krishiv_state::{Namespace, StateBackend, StateError, StateResult};

use crate::aggregate::AggState;

/// Persist window accumulators to a state backend.
///
/// This is a generic helper that handles the common pattern of:
/// 1. Clearing the namespace to remove stale entries
/// 2. Serializing each accumulator to JSON
/// 3. Building length-prefixed state keys
/// 4. Writing entries in a batch
///
/// # Arguments
/// * `backend` - The state backend to write to
/// * `namespace` - The namespace for this operator's state
/// * `accumulators` - The window accumulators to persist
/// * `key_prefix` - The 3-byte prefix for state keys (e.g., `b"tw:"`, `b"sw:"`, `b"ses:"`)
pub fn persist_window_accumulators(
    backend: &mut dyn StateBackend,
    namespace: &Namespace,
    accumulators: &HashMap<(String, i64), AggState>,
    key_prefix: &[u8; 3],
) -> StateResult<()> {
    // Remove all previously persisted entries so closed windows don't
    // survive into the next checkpoint snapshot.
    backend.clear_namespace(namespace)?;

    if accumulators.is_empty() {
        return Ok(());
    }

    let op_id = namespace.operator_id();
    let name = namespace.state_name();
    let mut state_keys = Vec::with_capacity(accumulators.len());
    let mut values = Vec::with_capacity(accumulators.len());

    for ((key, win_start), agg) in accumulators {
        let payload = serde_json::json!({
            "values": agg.values,
            "has_value": agg.has_value,
            "avg_sums": agg.avg_sums,
            "avg_counts": agg.avg_counts,
        });
        let bytes = serde_json::to_vec(&payload).map_err(|e| StateError::CorruptEntry {
            message: e.to_string(),
        })?;

        // Length-prefix encoding: prefix | key_len_le_u32 | key_bytes | win_start_le_i64
        let key_bytes = key.as_bytes();
        let mut state_key = Vec::with_capacity(3 + 4 + key_bytes.len() + 8);
        state_key.extend_from_slice(key_prefix);
        state_key.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        state_key.extend_from_slice(key_bytes);
        state_key.extend_from_slice(&win_start.to_le_bytes());
        state_keys.push(state_key);
        values.push(bytes);
    }

    let batch_entries: Vec<(&str, &str, &[u8], &[u8])> = state_keys
        .iter()
        .zip(values.iter())
        .map(|(k, v)| (op_id, name, k.as_slice(), v.as_slice()))
        .collect();
    backend.put_batch(&batch_entries)?;
    Ok(())
}

/// Restore window accumulators from a state backend.
///
/// This is a generic helper that handles the common pattern of:
/// 1. Listing all keys in the namespace
/// 2. Reading and deserializing each entry
/// 3. Parsing length-prefixed state keys
/// 4. Building the accumulator map
///
/// # Arguments
/// * `backend` - The state backend to read from
/// * `namespace` - The namespace for this operator's state
/// * `key_prefix` - The 3-byte prefix for state keys (e.g., `b"tw:"`, `b"sw:"`, `b"ses:"`)
///
/// # Returns
/// A map of `(key, window_start) -> AggState` restored from the backend.
pub fn restore_window_accumulators(
    backend: &dyn StateBackend,
    namespace: &Namespace,
    key_prefix: &[u8; 3],
) -> StateResult<HashMap<(String, i64), AggState>> {
    let mut restored = HashMap::new();

    for key_bytes in backend.list_keys(namespace)? {
        if key_bytes.len() < 3 || &key_bytes[..3] != key_prefix {
            continue;
        }
        let Some(payload) = backend.get(namespace, &key_bytes)? else {
            continue;
        };

        let parsed: serde_json::Value =
            serde_json::from_slice(&payload).map_err(|e| StateError::CorruptEntry {
                message: e.to_string(),
            })?;

        let values: Vec<i64> = parsed["values"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_i64()).collect())
            .unwrap_or_default();
        let has_value: Vec<bool> = parsed["has_value"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_bool()).collect())
            .unwrap_or_default();
        let avg_sums: Vec<f64> = parsed["avg_sums"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_f64()).collect())
            .unwrap_or_default();
        let avg_counts: Vec<u64> = parsed["avg_counts"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_u64()).collect())
            .unwrap_or_default();

        if let Some((key, win_start)) = parse_window_state_key(&key_bytes, key_prefix) {
            restored.insert(
                (key, win_start),
                AggState {
                    values,
                    has_value,
                    avg_sums,
                    avg_counts,
                },
            );
        }
    }

    Ok(restored)
}

/// Persist the operator watermark (monotonic event-time progress) for checkpoint restore.
pub fn persist_operator_watermark_ms(
    backend: &mut dyn StateBackend,
    namespace: &Namespace,
    watermark_ms: i64,
) -> StateResult<()> {
    backend.put(
        namespace,
        b"wm:".to_vec(),
        watermark_ms.to_le_bytes().to_vec(),
    )
}

/// Restore a previously persisted operator watermark, if present.
pub fn restore_operator_watermark_ms(
    backend: &dyn StateBackend,
    namespace: &Namespace,
) -> StateResult<Option<i64>> {
    let Some(bytes) = backend.get(namespace, b"wm:")? else {
        return Ok(None);
    };
    if bytes.len() < 8 {
        return Err(StateError::CorruptEntry {
            message: format!("watermark entry too short ({} bytes)", bytes.len()),
        });
    }
    Ok(Some(i64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])))
}

/// Parse a length-prefixed window state key.
///
/// Format: `prefix | key_len_le_u32 | key_bytes | win_start_le_i64`
///
/// Returns `None` if the key doesn't match the expected prefix or is too short.
fn parse_window_state_key(key_bytes: &[u8], prefix: &[u8; 3]) -> Option<(String, i64)> {
    // Must start with the expected prefix
    if key_bytes.len() < 3 || &key_bytes[..3] != prefix {
        return None;
    }

    let rest = &key_bytes[3..];

    // Need at least 4 bytes for key_len + 8 bytes for win_start
    if rest.len() < 12 {
        return None;
    }

    let key_len = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;

    // Check that we have enough bytes for the key and win_start
    if rest.len() < 4 + key_len + 8 {
        return None;
    }

    let key_bytes = &rest[4..4 + key_len];
    let key = String::from_utf8(key_bytes.to_vec()).ok()?;

    let win_start_bytes = &rest[4 + key_len..4 + key_len + 8];
    let win_start = i64::from_le_bytes([
        win_start_bytes[0],
        win_start_bytes[1],
        win_start_bytes[2],
        win_start_bytes[3],
        win_start_bytes[4],
        win_start_bytes[5],
        win_start_bytes[6],
        win_start_bytes[7],
    ]);

    Some((key, win_start))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_window_state_key_valid() {
        let prefix = b"tw:";
        let key = "my_key";
        let win_start = 1000i64;

        // Build a valid key
        let key_bytes = key.as_bytes();
        let mut state_key = Vec::new();
        state_key.extend_from_slice(prefix);
        state_key.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        state_key.extend_from_slice(key_bytes);
        state_key.extend_from_slice(&win_start.to_le_bytes());

        let result = parse_window_state_key(&state_key, prefix);
        assert_eq!(result, Some(("my_key".to_string(), 1000)));
    }

    #[test]
    fn parse_window_state_key_wrong_prefix() {
        let prefix = b"tw:";
        let state_key = b"sw:xxxx";
        assert!(parse_window_state_key(state_key, prefix).is_none());
    }

    #[test]
    fn parse_window_state_key_too_short() {
        let prefix = b"tw:";
        let state_key = b"tw:";
        assert!(parse_window_state_key(state_key, prefix).is_none());
    }

    #[test]
    fn parse_window_state_key_empty_key() {
        let prefix = b"tw:";
        let win_start = 1000i64;

        // Build a key with empty string
        let mut state_key = Vec::new();
        state_key.extend_from_slice(prefix);
        state_key.extend_from_slice(&0u32.to_le_bytes()); // key_len = 0
        state_key.extend_from_slice(&win_start.to_le_bytes());

        let result = parse_window_state_key(&state_key, prefix);
        assert_eq!(result, Some(("".to_string(), 1000)));
    }
}
