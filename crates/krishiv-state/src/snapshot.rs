use crate::error::{StateError, StateResult};

/// `(op_id, state_name, key, value)` tuple produced by snapshot decoding.
pub type SnapshotEntry = (String, String, Vec<u8>, Vec<u8>);

pub fn read_lp_bytes<'a>(buf: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    if buf.len() < *pos + 8 {
        return None;
    }
    let len = u64::from_le_bytes(buf.get(*pos..*pos + 8)?.try_into().ok()?) as usize;
    *pos += 8;
    if buf.len() < *pos + len {
        return None;
    }
    let v = buf.get(*pos..*pos + len)?;
    *pos += len;
    Some(v)
}

/// Write two length-prefixed segments (`op_id` and `name`) into `buf`.
///
/// Each segment is encoded as an 8-byte little-endian length followed by the
/// UTF-8 bytes of the string.  This is the shared prefix encoding used by
/// both `RocksDbStateBackend::redb_key` and `RocksDbStateBackend::redb_prefix`.
pub fn write_prefix(buf: &mut Vec<u8>, op_id: &str, name: &str) {
    let op = op_id.as_bytes();
    let nm = name.as_bytes();
    buf.extend_from_slice(&(op.len() as u64).to_le_bytes());
    buf.extend_from_slice(op);
    buf.extend_from_slice(&(nm.len() as u64).to_le_bytes());
    buf.extend_from_slice(nm);
}

/// Decode a snapshot byte buffer into `(op_id, state_name, key, value)` tuples.
///
/// Both `InMemoryStateBackend::load_snapshot` and `RocksDbStateBackend::load_snapshot`
/// share this parsing logic to avoid duplication.
///
/// Expected format:
/// `[4-byte LE version=1][8-byte LE entry_count][entries...]`
/// where each entry is `[8-byte LE op_id_len][op_id][8-byte LE name_len][name][8-byte LE key_len][key][8-byte LE val_len][val]`
pub fn decode_snapshot_entries(bytes: &[u8]) -> StateResult<Vec<SnapshotEntry>> {
    let corrupt = |msg: &str| StateError::SnapshotCorrupt {
        message: msg.to_owned(),
    };
    if bytes.len() < 12 {
        return Err(corrupt("too short"));
    }
    let version = u32::from_le_bytes(
        bytes
            .get(..4)
            .ok_or_else(|| corrupt("failed to read version bytes"))?
            .try_into()
            .map_err(|_| corrupt("failed to read version bytes"))?,
    );
    if version != 1 {
        return Err(corrupt(&format!("unsupported snapshot version {version}")));
    }
    let count = u64::from_le_bytes(
        bytes
            .get(4..12)
            .ok_or_else(|| corrupt("failed to read entry count bytes"))?
            .try_into()
            .map_err(|_| corrupt("failed to read entry count bytes"))?,
    ) as usize;
    const MAX_ENTRIES: usize = 1_000_000;
    if count > MAX_ENTRIES {
        return Err(corrupt(&format!(
            "entry count {count} exceeds maximum {MAX_ENTRIES}"
        )));
    }
    let mut pos = 12usize;
    let mut entries = Vec::new();
    entries
        .try_reserve(count)
        .map_err(|_| corrupt(&format!("failed to allocate {count} entries")))?;

    for _ in 0..count {
        let op_id_b = read_lp_bytes(bytes, &mut pos)
            .ok_or_else(|| corrupt("truncated op_id"))?
            .to_vec();
        let op_id = String::from_utf8(op_id_b).map_err(|_| corrupt("op_id not utf8"))?;
        let name_b = read_lp_bytes(bytes, &mut pos)
            .ok_or_else(|| corrupt("truncated state_name"))?
            .to_vec();
        let state_name = String::from_utf8(name_b).map_err(|_| corrupt("state_name not utf8"))?;
        let key = read_lp_bytes(bytes, &mut pos)
            .ok_or_else(|| corrupt("truncated key"))?
            .to_vec();
        let value = read_lp_bytes(bytes, &mut pos)
            .ok_or_else(|| corrupt("truncated value"))?
            .to_vec();
        entries.push((op_id, state_name, key, value));
    }

    if pos != bytes.len() {
        return Err(corrupt(&format!(
            "trailing garbage after {count} entries: {} extra bytes",
            bytes.len() - pos
        )));
    }

    Ok(entries)
}

/// Encode `(op_id, state_name, key, value)` tuples into the portable snapshot
/// format read by [`decode_snapshot_entries`].
///
/// This is the exact inverse of decoding: round-tripping a snapshot through
/// `decode_snapshot_entries` → `encode_snapshot_entries` produces byte-identical
/// output for the same entry order.  Used by key-group redistribution to build
/// per-task snapshots from a repartitioned entry set.
pub fn encode_snapshot_entries(entries: &[SnapshotEntry]) -> Vec<u8> {
    let payload_len: usize = entries
        .iter()
        .map(|(op, name, key, value)| 32 + op.len() + name.len() + key.len() + value.len())
        .sum();
    let mut buf = Vec::with_capacity(12 + payload_len);
    buf.extend_from_slice(&1u32.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for (op_id, state_name, key, value) in entries {
        write_prefix(&mut buf, op_id, state_name);
        buf.extend_from_slice(&(key.len() as u64).to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(&(value.len() as u64).to_le_bytes());
        buf.extend_from_slice(value);
    }
    buf
}
