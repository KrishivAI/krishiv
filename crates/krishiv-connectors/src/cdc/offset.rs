#[cfg(feature = "state")]
pub struct CdcOffsetTracker {
    backend: Box<dyn krishiv_state::StateBackend>,
    ns: krishiv_state::Namespace,
    offsets: std::collections::HashMap<u32, i64>,
}

#[cfg(feature = "state")]
impl CdcOffsetTracker {
    pub fn new(backend: Box<dyn krishiv_state::StateBackend>) -> Self {
        let ns = krishiv_state::Namespace::new("cdc_operator", "cdc_offsets");
        let mut offsets = std::collections::HashMap::new();
        if let Ok(keys) = backend.list_keys(&ns) {
            for k in keys {
                if k.len() == 4 {
                    let Ok(key_arr) = k.as_slice().try_into() else {
                        tracing::warn!(
                            key_len = k.len(),
                            "cdc offset key has unexpected length, skipping"
                        );
                        continue;
                    };
                    let partition = u32::from_le_bytes(key_arr);
                    if let Ok(Some(val_bytes)) = backend.get(&ns, &k) {
                        if val_bytes.len() == 8 {
                            let Ok(val_arr) = val_bytes.as_slice().try_into() else {
                                tracing::warn!(
                                    partition,
                                    "cdc offset value has unexpected length, skipping"
                                );
                                continue;
                            };
                            let offset = i64::from_le_bytes(val_arr);
                            offsets.insert(partition, offset);
                        }
                    }
                }
            }
        }
        Self {
            backend,
            ns,
            offsets,
        }
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
}
