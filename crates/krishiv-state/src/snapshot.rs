use crate::error::{StateError, StateResult};

/// `(op_id, state_name, key, value)` tuple produced by snapshot decoding.
pub type SnapshotEntry = (String, String, Vec<u8>, Vec<u8>);

pub fn read_lp_bytes<'a>(buf: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    if buf.len() < *pos + 8 {
        return None;
    }
    let len = u64::from_le_bytes(buf[*pos..*pos + 8].try_into().ok()?) as usize;
    *pos += 8;
    if buf.len() < *pos + len {
        return None;
    }
    let v = &buf[*pos..*pos + len];
    *pos += len;
    Some(v)
}

/// Write two length-prefixed segments (`op_id` and `name`) into `buf`.
///
/// Each segment is encoded as an 8-byte little-endian length followed by the
/// UTF-8 bytes of the string.  This is the shared prefix encoding used by
/// both `RedbStateBackend::redb_key` and `RedbStateBackend::redb_prefix`.
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
/// Both `InMemoryStateBackend::load_snapshot` and `RedbStateBackend::load_snapshot`
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
    let version = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if version != 1 {
        return Err(corrupt(&format!("unsupported snapshot version {version}")));
    }
    let count = u64::from_le_bytes(bytes[4..12].try_into().unwrap()) as usize;
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

    Ok(entries)
}
