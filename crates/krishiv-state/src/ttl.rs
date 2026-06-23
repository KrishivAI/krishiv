use krishiv_common::async_util::unix_now_ms;

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
///
/// When a watermark is set via [`TtlStateBackend::set_watermark`], expiry checks
/// use event time instead of wall-clock time, enabling deterministic, reproducible
/// eviction driven by the streaming executor's watermark.
pub struct TtlStateBackend<B: StateBackend> {
    inner: B,
    config: TtlConfig,
    /// Event-time watermark in milliseconds set by the streaming executor.
    ///
    /// When `Some`, `purge_expired` and read-time expiry checks use this value
    /// as "current time" instead of `unix_now_ms()`.  This allows event-time-based
    /// eviction to be driven from the watermark rather than wall-clock time.
    watermark_ms: Option<i64>,
}

impl<B: StateBackend> TtlStateBackend<B> {
    /// Wrap `inner` with the given TTL config.
    pub fn new(inner: B, config: TtlConfig) -> Self {
        Self {
            inner,
            config,
            watermark_ms: None,
        }
    }

    /// Access the underlying backend.
    pub fn inner(&self) -> &B {
        &self.inner
    }

    /// Set the event-time watermark used for TTL expiry checks.
    ///
    /// After this is called, `purge_expired` and lazy read-time expiry checks
    /// will use `watermark_ms` as "current time" instead of `unix_now_ms()`.
    /// Call this each drain cycle with the executor's latest watermark so that
    /// TTL eviction is driven by event time rather than wall-clock time.
    pub fn set_watermark(&mut self, watermark_ms: i64) {
        self.watermark_ms = Some(watermark_ms);
    }

    /// Return the current "now" used for TTL comparisons.
    ///
    /// Returns the watermark if one has been set; otherwise falls back to
    /// `unix_now_ms()` so that wall-clock TTL still works without a watermark.
    fn now_ms(&self) -> i64 {
        self.watermark_ms.unwrap_or_else(unix_now_ms)
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
            Some(encoded) => Self::decode_if_live(encoded, self.now_ms()),
        }
    }

    /// Store `value` under `key` in `namespace` with a TTL expiry.
    ///
    /// The expiry deadline is computed via [`now_ms`](Self::now_ms): when a
    /// watermark has been set via [`set_watermark`](Self::set_watermark) it
    /// returns the event-time watermark; otherwise it falls back to wall-clock
    /// time.  Both `put` and `get` use `now_ms` for consistency — keys do not
    /// appear immediately expired when the event-time watermark lags wall clock.
    fn put(&mut self, namespace: &Namespace, key: Vec<u8>, value: Vec<u8>) -> StateResult<()> {
        // now_ms() is watermark-aware; see set_watermark / now_ms for details.
        let expires_at_ms = self
            .now_ms()
            .checked_add(self.config.ttl_ms as i64)
            .unwrap_or(i64::MAX);
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
        let now_ms = self.now_ms();
        let all_keys = self.inner.list_keys(namespace)?;
        let mut live = Vec::with_capacity(all_keys.len());
        for key in all_keys {
            match self.inner.get(namespace, &key)? {
                Some(encoded) if encoded.len() >= 8 => {
                    let expires_at_ms =
                        i64::from_le_bytes(encoded[..8].try_into().map_err(|_| {
                            StateError::CorruptEntry {
                                message: "TTL entry has invalid timestamp bytes".into(),
                            }
                        })?);
                    if now_ms < expires_at_ms {
                        live.push(key);
                    }
                }
                Some(_) => live.push(key), // not TTL-encoded — pass through
                None => {}                 // concurrently deleted
            }
        }
        Ok(live)
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
        let now_ms = self.now_ms();
        for (op_id, state_name, key, ttl_encoded_value) in &entries {
            // Strip the 8-byte TTL prefix if present; skip expired / corrupt entries.
            if ttl_encoded_value.len() < 8 {
                // M7: Log corrupt entries so operators can detect silent data loss
                // instead of silently dropping them.
                tracing::warn!(
                    op_id = %op_id,
                    state_name = %state_name,
                    key_len = key.len(),
                    value_len = ttl_encoded_value.len(),
                    "skipping corrupt TTL entry in snapshot (value too short for 8-byte prefix)"
                );
                continue;
            }
            let expires_at_ms =
                i64::from_le_bytes(ttl_encoded_value[..8].try_into().map_err(|_| {
                    StateError::CorruptEntry {
                        message: "ttl expiry prefix is not 8 bytes in snapshot".into(),
                    }
                })?);
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

    /// Forward the event-time watermark so that `purge_expired` and read-time
    /// expiry checks use event time rather than wall-clock time.
    ///
    /// Delegates to the inherent [`TtlStateBackend::set_watermark`] method.
    fn set_watermark(&mut self, watermark_ms: i64) {
        // Call the inherent method directly (inherent methods take priority over
        // trait methods when called via `self.`, so there is no recursion here).
        TtlStateBackend::set_watermark(self, watermark_ms);
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
    /// **Performance note:** Each expired key deletion opens a separate write
    /// transaction in `RocksDbStateBackend`.  For backends with many expired keys,
    /// consider calling this method in a background task or batching deletions
    /// to avoid latency spikes.
    ///
    /// Returns the number of entries removed.
    fn purge_expired(&mut self) -> StateResult<usize> {
        let now_ms = self.now_ms();
        let namespaces = self.inner.list_namespaces()?;
        let mut evicted = 0usize;
        let mut keys_to_delete = Vec::new();

        for ns in &namespaces {
            let keys = self.inner.list_keys(ns)?;
            for key in &keys {
                if let Some(encoded) = self.inner.get(ns, key)?
                    && encoded.len() >= 8
                {
                    let expires_at_ms =
                        i64::from_le_bytes(encoded[..8].try_into().unwrap_or([0u8; 8]));
                    if now_ms >= expires_at_ms {
                        keys_to_delete.push((ns.clone(), key.clone()));
                    }
                }
            }
        }

        if !keys_to_delete.is_empty() {
            let entries: Vec<(&Namespace, &[u8])> = keys_to_delete
                .iter()
                .map(|(ns, key)| (ns, key.as_slice()))
                .collect();
            self.inner.delete_batch(&entries)?;
            evicted = entries.len();
        }

        Ok(evicted)
    }

    /// Restore state from a snapshot, re-applying fresh TTL prefixes.
    ///
    /// P0.16: The snapshot contains raw (non-TTL-prefixed) values.  We re-encode
    /// them with a fresh `expires_at_ms = now + ttl_ms` so that loaded state has
    /// the full configured TTL duration remaining.
    ///
    /// Crash-safety: writes new entries first (idempotent overwrites), then
    /// deletes keys not in the new set. A crash after writes but before deletes
    /// leaves a superset (old + new) — never an empty backend. This is the
    /// opposite of the previous clear-then-insert pattern which left the
    /// backend empty on mid-restore crash.
    fn load_snapshot(&mut self, bytes: &[u8]) -> StateResult<()> {
        let entries = decode_snapshot_entries(bytes)?;
        let now_ms = self.now_ms();
        let expires_at_ms = now_ms
            .checked_add(self.config.ttl_ms as i64)
            .unwrap_or(i64::MAX);
        // Pre-compute entries so the write phase has no fallible computation.
        let precomputed: Vec<(Namespace, Vec<u8>, Vec<u8>)> = entries
            .iter()
            .map(|(op_id, state_name, key, raw_value)| {
                let ns = Namespace::new(op_id, state_name);
                let encoded = Self::encode(raw_value.to_vec(), expires_at_ms);
                (ns, key.clone(), encoded)
            })
            .collect();

        // Phase 1: Write all new entries (idempotent overwrites). A crash here
        // leaves the backend with old state + partially-written new state —
        // never empty.
        for (ns, key, encoded) in &precomputed {
            self.inner.put(ns, key.clone(), encoded.clone())?;
        }

        // Phase 2: Build the set of (namespace, key) pairs that are in the new
        // snapshot, then delete keys that exist in the backend but NOT in the
        // new snapshot.
        let new_keys: std::collections::HashSet<(Namespace, Vec<u8>)> = precomputed
            .iter()
            .map(|(ns, key, _)| (ns.clone(), key.clone()))
            .collect();

        let namespaces = self.inner.list_namespaces()?;
        for ns in &namespaces {
            let existing_keys = self.inner.list_keys(ns)?;
            for key in existing_keys {
                if !new_keys.contains(&(ns.clone(), key.clone())) {
                    self.inner.delete(ns, &key)?;
                }
            }
        }
        Ok(())
    }
}
