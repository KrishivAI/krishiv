//! Shared state persistence helpers for window operators.
//!
//! This module provides generic persist/restore functions that eliminate
//! code duplication across tumbling, sliding, and session window operators.

use std::collections::HashMap;

use krishiv_state::{Namespace, StateBackend, StateError, StateResult};

use crate::aggregate::{AggEntry, AggState};

/// Version tag for the compact binary `AggState` encoding. Distinguishes the
/// binary layout from the legacy `serde_json` object (which always begins with
/// the ASCII byte `{` = `0x7B`), so a backend persisted by an older build is
/// still readable after upgrade.
const AGG_STATE_BINARY_V1: u8 = 0x01;
/// Bytes per encoded `AggEntry`: i64 value + u8 has_value + f64 avg_sum +
/// u64 avg_count + f64 float_value + f64 sq_sum.
const AGG_ENTRY_LEN: usize = 8 + 1 + 8 + 8 + 8 + 8;

/// Encode one accumulator to the compact binary layout:
/// `[u8 version=1][u32 n][n × AGG_ENTRY_LEN]`. This replaces the per-checkpoint
/// `serde_json` serialization, which dominated checkpoint CPU: JSON encodes
/// each numeric field as a string and re-parses it on restore. The fixed
/// little-endian layout is a single `extend_from_slice` per field.
fn encode_agg_state(agg: &AggState) -> Vec<u8> {
    let n = agg.entries.len();
    let mut out = Vec::with_capacity(1 + 4 + n * AGG_ENTRY_LEN);
    out.push(AGG_STATE_BINARY_V1);
    out.extend_from_slice(&(n as u32).to_le_bytes());
    for e in &agg.entries {
        out.extend_from_slice(&e.value.to_le_bytes());
        out.push(u8::from(e.has_value));
        out.extend_from_slice(&e.avg_sum.to_le_bytes());
        out.extend_from_slice(&e.avg_count.to_le_bytes());
        out.extend_from_slice(&e.float_value.to_le_bytes());
        out.extend_from_slice(&e.sq_sum.to_le_bytes());
    }
    out
}

/// Decode an accumulator. Accepts the binary v1 layout
/// ([`encode_agg_state`]) and, for backward compatibility, the legacy
/// `serde_json` object produced by older builds.
fn decode_agg_state(bytes: &[u8]) -> StateResult<AggState> {
    match bytes.first() {
        Some(&AGG_STATE_BINARY_V1) => decode_agg_state_binary(bytes),
        Some(&b'{') => decode_agg_state_legacy_json(bytes),
        _ => Err(StateError::CorruptEntry {
            message: "agg state: unrecognized encoding tag".into(),
        }),
    }
}

fn decode_agg_state_binary(bytes: &[u8]) -> StateResult<AggState> {
    let corrupt = |m: &str| StateError::CorruptEntry {
        message: format!("agg state binary: {m}"),
    };
    let body = bytes.get(1..).ok_or_else(|| corrupt("missing header"))?;
    let n_arr: [u8; 4] = body
        .get(..4)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| corrupt("missing entry count"))?;
    let n = u32::from_le_bytes(n_arr) as usize;
    let mut entries = Vec::with_capacity(n);
    let mut off = 4usize;
    let rd_i64 = |s: &[u8]| -> Option<i64> { s.try_into().ok().map(i64::from_le_bytes) };
    let rd_u64 = |s: &[u8]| -> Option<u64> { s.try_into().ok().map(u64::from_le_bytes) };
    let rd_f64 = |s: &[u8]| -> Option<f64> { s.try_into().ok().map(f64::from_le_bytes) };
    for _ in 0..n {
        let rec = body
            .get(off..off + AGG_ENTRY_LEN)
            .ok_or_else(|| corrupt("entry truncated"))?;
        let value = rd_i64(rec.get(0..8).unwrap_or_default()).ok_or_else(|| corrupt("value"))?;
        let has_value = rec.get(8).copied().unwrap_or(0) != 0;
        let avg_sum =
            rd_f64(rec.get(9..17).unwrap_or_default()).ok_or_else(|| corrupt("avg_sum"))?;
        let avg_count =
            rd_u64(rec.get(17..25).unwrap_or_default()).ok_or_else(|| corrupt("avg_count"))?;
        let float_value =
            rd_f64(rec.get(25..33).unwrap_or_default()).ok_or_else(|| corrupt("float_value"))?;
        let sq_sum =
            rd_f64(rec.get(33..41).unwrap_or_default()).ok_or_else(|| corrupt("sq_sum"))?;
        entries.push(AggEntry {
            value,
            has_value,
            avg_sum,
            avg_count,
            float_value,
            sq_sum,
        });
        off += AGG_ENTRY_LEN;
    }
    Ok(AggState { entries })
}

/// Legacy decoder for state persisted by builds that used `serde_json`.
fn decode_agg_state_legacy_json(payload: &[u8]) -> StateResult<AggState> {
    let parsed: serde_json::Value =
        serde_json::from_slice(payload).map_err(|e| StateError::CorruptEntry {
            message: e.to_string(),
        })?;
    let arr_i64 = |k: &str| -> Vec<i64> {
        parsed
            .get(k)
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|v| v.as_i64()).collect())
            .unwrap_or_default()
    };
    let arr_u64 = |k: &str| -> Vec<u64> {
        parsed
            .get(k)
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|v| v.as_u64()).collect())
            .unwrap_or_default()
    };
    let arr_f64 = |k: &str| -> Vec<f64> {
        parsed
            .get(k)
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|v| v.as_f64()).collect())
            .unwrap_or_default()
    };
    let arr_bool = |k: &str| -> Vec<bool> {
        parsed
            .get(k)
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|v| v.as_bool()).collect())
            .unwrap_or_default()
    };
    let values = arr_i64("values");
    let has_value = arr_bool("has_value");
    let avg_sums = arr_f64("avg_sums");
    let avg_counts = arr_u64("avg_counts");
    let float_values = arr_f64("float_values");
    let sq_sums = arr_f64("sq_sums");
    let n = values.len();
    let entries = (0..n)
        .map(|i| AggEntry {
            value: values.get(i).copied().unwrap_or(0),
            has_value: has_value.get(i).copied().unwrap_or(false),
            avg_sum: avg_sums.get(i).copied().unwrap_or(0.0),
            avg_count: avg_counts.get(i).copied().unwrap_or(0),
            float_value: float_values.get(i).copied().unwrap_or(0.0),
            sq_sum: sq_sums.get(i).copied().unwrap_or(0.0),
        })
        .collect();
    Ok(AggState { entries })
}

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
/// * `key_prefix` - The prefix bytes for state keys (e.g., `b"tw:"`, `b"sw:"`, `b"ses:"`)
pub fn persist_window_accumulators(
    backend: &mut dyn StateBackend,
    namespace: &Namespace,
    accumulators: &HashMap<(String, i64), AggState>,
    key_prefix: &[u8],
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
        // Compact binary encoding (see `encode_agg_state`); replaces the prior
        // per-accumulator `serde_json` serialization on the checkpoint path.
        let bytes = encode_agg_state(agg);

        // Length-prefix encoding: prefix | key_len_le_u32 | key_bytes | win_start_le_i64
        let key_bytes = key.as_bytes();
        let mut state_key = Vec::with_capacity(key_prefix.len() + 4 + key_bytes.len() + 8);
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
/// * `key_prefix` - The prefix bytes for state keys (e.g., `b"tw:"`, `b"sw:"`, `b"ses:"`)
///
/// # Returns
/// A map of `(key, window_start) -> AggState` restored from the backend.
pub fn restore_window_accumulators(
    backend: &dyn StateBackend,
    namespace: &Namespace,
    key_prefix: &[u8],
) -> StateResult<HashMap<(String, i64), AggState>> {
    let mut restored = HashMap::new();
    let plen = key_prefix.len();

    for key_bytes in backend.list_keys(namespace)? {
        if key_bytes.get(..plen).is_none_or(|p| p != key_prefix) {
            continue;
        }
        let Some(payload) = backend.get(namespace, &key_bytes)? else {
            continue;
        };

        if let Some((key, win_start)) = parse_window_state_key(&key_bytes, key_prefix) {
            let agg = decode_agg_state(&payload)?;
            restored.insert((key, win_start), agg);
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
    let arr: [u8; 8] = bytes
        .get(..8)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| StateError::CorruptEntry {
            message: "watermark bytes too short".into(),
        })?;
    Ok(Some(i64::from_le_bytes(arr)))
}

/// Parse a length-prefixed window state key.
///
/// Format: `prefix | key_len_le_u32 | key_bytes | win_start_le_i64`
///
/// Returns `None` if the key doesn't match the expected prefix or is too short.
fn parse_window_state_key(key_bytes: &[u8], prefix: &[u8]) -> Option<(String, i64)> {
    let plen = prefix.len();
    // Must start with the expected prefix
    if key_bytes.get(..plen).is_none_or(|p| p != prefix) {
        return None;
    }

    let rest = key_bytes.get(plen..).unwrap_or(&[]);

    // Need at least 4 bytes for key_len + 8 bytes for win_start
    if rest.len() < 12 {
        return None;
    }

    let key_len_bytes: [u8; 4] = rest.get(..4)?.try_into().ok()?;
    let key_len = u32::from_le_bytes(key_len_bytes) as usize;

    // Check that we have enough bytes for the key and win_start
    if rest.len() < 4 + key_len + 8 {
        return None;
    }

    let key_bytes = rest.get(4..4 + key_len)?;
    let key = String::from_utf8(key_bytes.to_vec()).ok()?;

    let win_start_arr: [u8; 8] = rest.get(4 + key_len..4 + key_len + 8)?.try_into().ok()?;
    let win_start = i64::from_le_bytes(win_start_arr);

    Some((key, win_start))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_agg_state() -> AggState {
        AggState {
            entries: vec![
                AggEntry {
                    value: 42,
                    has_value: true,
                    avg_sum: 12.5,
                    avg_count: 3,
                    float_value: -7.25,
                    sq_sum: 99.0,
                },
                AggEntry {
                    value: i64::MIN,
                    has_value: false,
                    avg_sum: 0.0,
                    avg_count: 0,
                    float_value: f64::NEG_INFINITY,
                    sq_sum: 0.0,
                },
            ],
        }
    }

    #[test]
    fn agg_state_binary_roundtrip_preserves_all_fields() {
        let original = sample_agg_state();
        let bytes = encode_agg_state(&original);
        assert_eq!(bytes.first(), Some(&AGG_STATE_BINARY_V1));
        let decoded = decode_agg_state(&bytes).expect("decode binary");
        assert_eq!(decoded.entries.len(), original.entries.len());
        for (d, o) in decoded.entries.iter().zip(original.entries.iter()) {
            assert_eq!(d.value, o.value);
            assert_eq!(d.has_value, o.has_value);
            assert_eq!(d.avg_sum.to_bits(), o.avg_sum.to_bits());
            assert_eq!(d.avg_count, o.avg_count);
            assert_eq!(d.float_value.to_bits(), o.float_value.to_bits());
            assert_eq!(d.sq_sum.to_bits(), o.sq_sum.to_bits());
        }
    }

    #[test]
    fn agg_state_decode_accepts_legacy_json() {
        // State persisted by an older build is a serde_json object; the decoder
        // must still read it after upgrade to the binary format.
        let legacy = serde_json::json!({
            "values":       [42_i64, 0_i64],
            "has_value":    [true, false],
            "avg_sums":     [12.5_f64, 0.0_f64],
            "avg_counts":   [3_u64, 0_u64],
            "float_values": [-7.25_f64, 0.0_f64],
            "sq_sums":      [99.0_f64, 0.0_f64],
        });
        let bytes = serde_json::to_vec(&legacy).unwrap();
        assert_eq!(bytes.first(), Some(&b'{'));
        let decoded = decode_agg_state(&bytes).expect("decode legacy json");
        assert_eq!(decoded.entries.len(), 2);
        assert_eq!(decoded.entries[0].value, 42);
        assert!(decoded.entries[0].has_value);
        assert_eq!(decoded.entries[0].avg_count, 3);
    }

    #[test]
    fn parse_window_state_key_valid() {
        let prefix: &[u8] = b"tw:";
        let key = "my_key";
        let win_start = 1000i64;

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
    fn parse_window_state_key_longer_prefix() {
        let prefix: &[u8] = b"sess:";
        let key = "k1";
        let win_start = 500i64;

        let key_bytes = key.as_bytes();
        let mut state_key = Vec::new();
        state_key.extend_from_slice(prefix);
        state_key.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        state_key.extend_from_slice(key_bytes);
        state_key.extend_from_slice(&win_start.to_le_bytes());

        let result = parse_window_state_key(&state_key, prefix);
        assert_eq!(result, Some(("k1".to_string(), 500)));
    }

    #[test]
    fn parse_window_state_key_wrong_prefix() {
        let prefix: &[u8] = b"tw:";
        let state_key = b"sw:xxxx";
        assert!(parse_window_state_key(state_key, prefix).is_none());
    }

    #[test]
    fn parse_window_state_key_too_short() {
        let prefix: &[u8] = b"tw:";
        let state_key = b"tw:";
        assert!(parse_window_state_key(state_key, prefix).is_none());
    }

    #[test]
    fn parse_window_state_key_empty_key() {
        let prefix: &[u8] = b"tw:";
        let win_start = 1000i64;

        let mut state_key = Vec::new();
        state_key.extend_from_slice(prefix);
        state_key.extend_from_slice(&0u32.to_le_bytes()); // key_len = 0
        state_key.extend_from_slice(&win_start.to_le_bytes());

        let result = parse_window_state_key(&state_key, prefix);
        assert_eq!(result, Some(("".to_string(), 1000)));
    }
}
