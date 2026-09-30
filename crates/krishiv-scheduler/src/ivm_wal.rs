//! Write-ahead log entries for coordinator-hosted IVM jobs.
//!
//! # Why a log
//!
//! A job's durable form used to be one thing: a snapshot of its whole state,
//! rewritten after every `/step`. That made persistence O(state) per tick
//! against an O(Δ) engine (IVM-AUD-DIST-G3), and it made a fed delta
//! non-durable until the next step, because persisting the whole state per
//! feed was unaffordable (IVM-AUD-INT-F12, DIST-D3).
//!
//! The durable form is now a snapshot **plus a log of what the job accepted
//! since**: each fed delta and each completed step. Accepting a delta costs a
//! write the size of the delta; a step costs a marker; and the snapshot is
//! rewritten only every so many ticks, or when the log has grown, at which
//! point the log it covers is dropped. Recovery restores the snapshot and
//! replays the log in order.
//!
//! # Why replay is exact
//!
//! Feeds and steps for one job take the same per-job lock, so a step sees
//! exactly the feeds logged before it and none logged after. Replaying the log
//! in sequence order therefore rebuilds the same ticks the job ran — the same
//! inputs in the same order, through a deterministic engine.
//!
//! This deliberately does not use `checkpoint_delta` / `restore_delta`, whose
//! restore is set-materializing and not equivalent to the state it came from
//! (IVM-AUD-DIST-C2).

/// One thing a job accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WalEntry {
    /// A delta fed to `source` (`/feed`, `/stream-delta`), as the serialized
    /// `DeltaBatch` that arrived, with the caller's idempotency key if it sent
    /// one.
    Feed {
        source: String,
        idempotency_key: Option<Vec<u8>>,
        delta_ipc: Vec<u8>,
    },
    /// A full snapshot fed to `source` (`/stream-bridge`), as the Arrow IPC
    /// stream that arrived; the flow diffs it against the previous snapshot.
    Snapshot {
        source: String,
        idempotency_key: Option<Vec<u8>>,
        snapshot_ipc: Vec<u8>,
    },
    /// A tick ran to completion over everything fed before it.
    Step,
}

const KIND_FEED: u8 = 1;
const KIND_SNAPSHOT: u8 = 2;
const KIND_STEP: u8 = 3;

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// Read a length-prefixed field, advancing `rest` past it.
fn take_bytes<'a>(rest: &mut &'a [u8]) -> Result<&'a [u8], String> {
    let (len, tail) = rest
        .split_first_chunk::<8>()
        .ok_or("IVM log entry truncated in a length prefix")?;
    let len = usize::try_from(u64::from_le_bytes(*len))
        .map_err(|_| "IVM log entry field length overflows")?;
    if tail.len() < len {
        return Err(String::from("IVM log entry truncated in a field"));
    }
    let (field, tail) = tail.split_at(len);
    *rest = tail;
    Ok(field)
}

impl WalEntry {
    /// `[kind][source][has_key][key][payload]`, each field length-prefixed.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let (kind, source, key, payload) = match self {
            Self::Feed {
                source,
                idempotency_key,
                delta_ipc,
            } => (KIND_FEED, source, idempotency_key, delta_ipc),
            Self::Snapshot {
                source,
                idempotency_key,
                snapshot_ipc,
            } => (KIND_SNAPSHOT, source, idempotency_key, snapshot_ipc),
            Self::Step => return vec![KIND_STEP],
        };
        let mut out = Vec::with_capacity(payload.len() + source.len() + 32);
        out.push(kind);
        put_bytes(&mut out, source.as_bytes());
        match key {
            Some(key) => {
                out.push(1);
                put_bytes(&mut out, key);
            }
            None => out.push(0),
        }
        put_bytes(&mut out, payload);
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, String> {
        let (kind, mut rest) = bytes.split_first().ok_or("empty IVM log entry")?;
        if *kind == KIND_STEP {
            return Ok(Self::Step);
        }
        let source = std::str::from_utf8(take_bytes(&mut rest)?)
            .map_err(|e| format!("IVM log entry source name: {e}"))?
            .to_owned();
        let (has_key, tail) = rest
            .split_first()
            .ok_or("IVM log entry truncated before the key flag")?;
        rest = tail;
        let idempotency_key = match has_key {
            0 => None,
            1 => Some(take_bytes(&mut rest)?.to_vec()),
            other => return Err(format!("IVM log entry has key flag {other}")),
        };
        let payload = take_bytes(&mut rest)?.to_vec();
        match kind {
            &KIND_FEED => Ok(Self::Feed {
                source,
                idempotency_key,
                delta_ipc: payload,
            }),
            &KIND_SNAPSHOT => Ok(Self::Snapshot {
                source,
                idempotency_key,
                snapshot_ipc: payload,
            }),
            other => Err(format!("IVM log entry has unknown kind {other}")),
        }
    }
}

/// Decode the Arrow IPC stream a `/stream-bridge` body carries.
pub(crate) fn decode_snapshot_batches(
    ipc: &[u8],
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    use arrow::ipc::reader::StreamReader;
    let reader = StreamReader::try_new(std::io::Cursor::new(ipc), None)
        .map_err(|e| format!("IPC stream open: {e}"))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("IPC stream read: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entries_round_trip() {
        let entries = [
            WalEntry::Feed {
                source: "orders".into(),
                idempotency_key: None,
                delta_ipc: vec![1, 2, 3],
            },
            WalEntry::Feed {
                source: "ünïcode".into(),
                idempotency_key: Some(b"offset-7".to_vec()),
                delta_ipc: Vec::new(),
            },
            WalEntry::Snapshot {
                source: "s".into(),
                idempotency_key: Some(Vec::new()),
                snapshot_ipc: vec![9; 1000],
            },
            WalEntry::Step,
        ];
        for entry in entries {
            assert_eq!(WalEntry::decode(&entry.encode()), Ok(entry));
        }
    }

    /// A damaged entry must be an error, never a differently-shaped entry.
    #[test]
    fn a_truncated_or_unknown_entry_is_refused() {
        let encoded = WalEntry::Feed {
            source: "orders".into(),
            idempotency_key: Some(b"k".to_vec()),
            delta_ipc: vec![1, 2, 3, 4],
        }
        .encode();
        for len in 0..encoded.len() {
            let cut = encoded.get(..len).unwrap_or_default();
            assert!(WalEntry::decode(cut).is_err(), "prefix of {len} bytes");
        }
        assert!(WalEntry::decode(&[42]).is_err());
        // A length that claims more than is there.
        let mut lying = vec![KIND_FEED];
        lying.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(WalEntry::decode(&lying).is_err());
    }
}
