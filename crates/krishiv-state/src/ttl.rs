use krishiv_async_util::unix_now_ms;

use crate::backend::StateBackend;
use crate::error::{StateError, StateResult};
use crate::namespace::Namespace;
use crate::snapshot::decode_snapshot_entries;

/// State TTL (time-to-live) configuration (R5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TtlConfig {
    /// Duration in milliseconds.  State expires this many ms after it is written.
    pub ttl_ms: u64,
}

impl TtlConfig {
    /// Create a TTL config with the given duration.
    pub fn new(ttl_ms: u64) -> Self {
        Self { ttl_ms }
    }
}

/// A [`StateBackend`] wrapper that enforces TTL expiry on all stored values.
///
/// Values are encoded as `[8-byte LE expires_at_ms][raw value bytes]`.
/// Expired values are treated as absent (lazy deletion on read; the raw bytes
/// remain in the inner store until the next write or `clear_namespace`).
pub struct TtlStateBackend<B: StateBackend> {
    inner: B,
    config: TtlConfig,
}

impl<B: StateBackend> TtlStateBackend<B> {
    /// Wrap `inner` with the given TTL config.
    pub fn new(inner: B, config: TtlConfig) -> Self {
        Self { inner, config }
    }

    /// Access the underlying backend.
    pub fn inner(&self) -> &B {
        &self.inner
    }

    fn encode(value: Vec<u8>, expires_at_ms: i64) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(8 + value.len());
        encoded.extend_from_slice(&expires_at_ms.to_le_bytes());
        encoded.extend_from_slice(&value);
        encoded
    }

    fn decode_if_live(encoded: Vec<u8>, now_ms: i64) -> StateResult<Option<Vec<u8>>> {
        if encoded.len() < 8 {
            return Err(StateError::CorruptEntry {
                message: format!("ttl value is too short: {} bytes", encoded.len()),
            });
        }
        let expires_at_ms =
            i64::from_le_bytes(
                encoded[..8]
                    .try_into()
                    .map_err(|_| StateError::CorruptEntry {
                        message: "ttl expiry prefix is not 8 bytes".into(),
                    })?,
            );
        if now_ms >= expires_at_ms {
            Ok(None)
        } else {
            Ok(Some(encoded[8..].to_vec()))
        }
    }
}

impl<B: StateBackend> StateBackend for TtlStateBackend<B> {
    fn get(&self, namespace: &Namespace, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
        match self.inner.get(namespace, key)? {
            None => Ok(None),
            Some(encoded) => Self::decode_if_live(encoded, unix_now_ms()),
        }
    }

    fn put(&mut self, namespace: &Namespace, key: Vec<u8>, value: Vec<u8>) -> StateResult<()> {
        let expires_at_ms = unix_now_ms() + self.config.ttl_ms as i64;
        self.inner
            .put(namespace, key, Self::encode(value, expires_at_ms))
    }

    fn delete(&mut self, namespace: &Namespace, key: &[u8]) -> StateResult<()> {
        self.inner.delete(namespace, key)
    }

    fn clear_namespace(&mut self, namespace: &Namespace) -> StateResult<()> {
        self.inner.clear_namespace(namespace)
    }

    fn list_namespaces(&self) -> StateResult<Vec<Namespace>> {
        self.inner.list_namespaces()
    }

    fn list_keys(&self, namespace: &Namespace) -> StateResult<Vec<Vec<u8>>> {
        self.inner.list_keys(namespace)
    }

    /// Snapshot state with TTL prefix stripped from values.
    ///
    /// P0.16: The inner backend stores values as `[8-byte LE expires_at_ms][raw_value]`.
    /// We snapshot only the raw value bytes so that the snapshot format is portable
    /// and independent of wall-clock time.  `load_snapshot` re-applies fresh TTL
    /// prefixes using the current wall-clock time and the configured TTL duration.
    fn snapshot(&self) -> StateResult<Vec<u8>> {
        // Take a snapshot of the raw (TTL-prefixed) inner state.
        let raw_snap = self.inner.snapshot()?;
        // Decode all entries, strip the TTL prefix, then re-encode.
        let entries = decode_snapshot_entries(&raw_snap)?;
        let mut out = Vec::new();
        out.extend_from_slice(&1u32.to_le_bytes()); // version
        let count_offset = out.len();
        out.extend_from_slice(&0u64.to_le_bytes()); // placeholder; patched after filtering
        let mut written = 0u64;
        for (op_id, state_name, key, ttl_encoded_value) in &entries {
            // Strip the 8-byte TTL prefix if present; skip expired / corrupt entries.
            if ttl_encoded_value.len() < 8 {
                // Skip corrupt entries silently in snapshot — they're already invisible on read.
                continue;
            }
            let expires_at_ms =
                i64::from_le_bytes(ttl_encoded_value[..8].try_into().map_err(|_| {
                    StateError::CorruptEntry {
                        message: "ttl expiry prefix is not 8 bytes in snapshot".into(),
                    }
                })?);
            let now_ms = unix_now_ms();
            if now_ms >= expires_at_ms {
                // Skip already-expired entries — they're invisible on read anyway.
                continue;
            }
            let raw_value = &ttl_encoded_value[8..];
            let ob = op_id.as_bytes();
            let nb = state_name.as_bytes();
            out.extend_from_slice(&(ob.len() as u64).to_le_bytes());
            out.extend_from_slice(ob);
            out.extend_from_slice(&(nb.len() as u64).to_le_bytes());
            out.extend_from_slice(nb);
            out.extend_from_slice(&(key.len() as u64).to_le_bytes());
            out.extend_from_slice(key);
            out.extend_from_slice(&(raw_value.len() as u64).to_le_bytes());
            out.extend_from_slice(raw_value);
            written += 1;
        }
        out[count_offset..count_offset + 8].copy_from_slice(&written.to_le_bytes());
        Ok(out)
    }

    /// GAP-15: Eagerly remove all expired entries from the inner backend.
    ///
    /// `TtlStateBackend` uses lazy deletion on reads — expired values are only
    /// evicted when the key is explicitly fetched.  Keys that are written once
    /// and never read again after expiry remain in the inner store indefinitely,
    /// causing unbounded memory growth on long-running streaming jobs.
    ///
    /// This method performs an eager scan across all namespaces and deletes
    /// every entry whose `expires_at_ms ≤ now`.  It is intended to be called
    /// periodically (e.g. at the start of each `ContinuousWindowExecutor::drain`
    /// cycle) rather than on every read so that the amortised GC cost is low.
    ///
    /// Returns the number of entries removed.
    fn purge_expired(&mut self) -> StateResult<usize> {
        let now_ms = unix_now_ms();
        let namespaces = self.inner.list_namespaces()?;
        let mut evicted = 0usize;
        for ns in &namespaces {
            let keys = self.inner.list_keys(ns)?;
            for key in &keys {
                if let Some(encoded) = self.inner.get(ns, key)? {
                    if encoded.len() >= 8 {
                        let expires_at_ms = i64::from_le_bytes(
                            encoded[..8].try_into().unwrap_or([0u8; 8]),
                        );
                        if now_ms >= expires_at_ms {
                            self.inner.delete(ns, key)?;
                            evicted += 1;
                        }
                    }
                }
            }
        }
        Ok(evicted)
    }

    /// Restore state from a snapshot, re-applying fresh TTL prefixes.
    ///
    /// P0.16: The snapshot contains raw (non-TTL-prefixed) values.  We re-encode
    /// them with a fresh `expires_at_ms = now + ttl_ms` so that loaded state has
    /// the full configured TTL duration remaining.
    fn load_snapshot(&mut self, bytes: &[u8]) -> StateResult<()> {
        let entries = decode_snapshot_entries(bytes)?;
        let now_ms = unix_now_ms();
        let expires_at_ms = now_ms + self.config.ttl_ms as i64;
        // Pre-compute entries so the clear+insert phase has no fallible computation.
        let precomputed: Vec<(Namespace, Vec<u8>, Vec<u8>)> = entries
            .iter()
            .map(|(op_id, state_name, key, raw_value)| {
                let ns = Namespace::new(op_id, state_name);
                let encoded = Self::encode(raw_value.to_vec(), expires_at_ms);
                (ns, key.clone(), encoded)
            })
            .collect();
        let namespaces = self.inner.list_namespaces()?;
        for ns in &namespaces {
            self.inner.clear_namespace(ns)?;
        }
        for (ns, key, encoded) in precomputed {
            self.inner.put(&ns, key, encoded)?;
        }
        Ok(())
    }
}
