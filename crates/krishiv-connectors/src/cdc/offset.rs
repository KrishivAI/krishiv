#[cfg(feature = "state")]
use crate::error::ConnectorError;

#[cfg(feature = "state")]
pub struct CdcOffsetTracker {
    backend: Box<dyn krishiv_state::StateBackend>,
    ns: krishiv_state::Namespace,
    offsets: std::collections::HashMap<u32, i64>,
}

#[cfg(feature = "state")]
impl CdcOffsetTracker {
    /// Load the persisted per-partition offsets.
    ///
    /// A backend read failure is an error, not an empty map: this used to
    /// swallow `list_keys` / `get` errors and hand the pipeline a tracker with
    /// no offsets, which `pipeline.rs` reads as "nothing to resume from" — a
    /// transient backend fault at startup silently replayed every committed
    /// CDC record. A malformed key or value is likewise an error: it is
    /// corruption, and skipping it is the same replay in disguise.
    pub fn new(backend: Box<dyn krishiv_state::StateBackend>) -> Result<Self, ConnectorError> {
        let ns = krishiv_state::Namespace::new("cdc_operator", "cdc_offsets");
        let mut offsets = std::collections::HashMap::new();
        let keys = backend
            .list_keys(&ns)
            .map_err(|e| ConnectorError::Cdc(format!("cdc offsets: listing keys failed: {e}")))?;
        for k in keys {
            let key_arr: [u8; 4] = k.as_slice().try_into().map_err(|_| {
                ConnectorError::Cdc(format!(
                    "cdc offsets: key has {} bytes, expected 4 (corrupt offset store)",
                    k.len()
                ))
            })?;
            let partition = u32::from_le_bytes(key_arr);
            let val_bytes = backend
                .get(&ns, &k)
                .map_err(|e| {
                    ConnectorError::Cdc(format!(
                        "cdc offsets: reading partition {partition} failed: {e}"
                    ))
                })?
                .ok_or_else(|| {
                    ConnectorError::Cdc(format!(
                        "cdc offsets: partition {partition} listed but unreadable"
                    ))
                })?;
            let val_arr: [u8; 8] = val_bytes.as_slice().try_into().map_err(|_| {
                ConnectorError::Cdc(format!(
                    "cdc offsets: partition {partition} value has {} bytes, expected 8 \
                     (corrupt offset store)",
                    val_bytes.len()
                ))
            })?;
            offsets.insert(partition, i64::from_le_bytes(val_arr));
        }
        Ok(Self {
            backend,
            ns,
            offsets,
        })
    }

    pub fn commit_offset(&mut self, partition: u32, offset: i64) -> Result<(), ConnectorError> {
        self.offsets.insert(partition, offset);
        let key = partition.to_le_bytes().to_vec();
        let value = offset.to_le_bytes().to_vec();
        self.backend
            .put(&self.ns, key, value)
            .map_err(|e| ConnectorError::Cdc(format!("state backend error: {e:?}")))?;
        Ok(())
    }

    pub fn get_offset(&self, partition: u32) -> Option<i64> {
        self.offsets.get(&partition).copied()
    }

    /// All persisted per-partition offsets, loaded at construction and kept
    /// current by `commit_offset`.
    pub fn offsets(&self) -> &std::collections::HashMap<u32, i64> {
        &self.offsets
    }
}
