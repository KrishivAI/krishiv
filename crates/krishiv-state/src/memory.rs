use std::collections::BTreeMap;

use crate::backend::StateBackend;
use crate::error::StateResult;
use crate::namespace::Namespace;
use crate::snapshot::decode_snapshot_entries;

// Compound map key: (operator_id, state_name, record_key)
type InMemKey = (String, String, Vec<u8>);

/// In-memory keyed state backend for R5.1.
///
/// State survives for the job lifetime but is lost on executor restart.
#[derive(Debug, Default, Clone)]
pub struct InMemoryStateBackend {
    store: BTreeMap<InMemKey, Vec<u8>>,
}

impl InMemoryStateBackend {
    /// Create an empty in-memory backend.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of keys stored across all namespaces.
    pub fn key_count(&self) -> usize {
        self.store.len()
    }

    /// Return the key group range owned by this backend.
    pub fn key_group_range(&self) -> std::ops::RangeInclusive<u16> {
        0..=(crate::key_group::NUM_KEY_GROUPS - 1)
    }

    fn make_key(namespace: &Namespace, key: &[u8]) -> InMemKey {
        (
            namespace.operator_id().to_owned(),
            namespace.state_name().to_owned(),
            key.to_vec(),
        )
    }
}

impl StateBackend for InMemoryStateBackend {
    fn get(&self, namespace: &Namespace, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
        Ok(self.store.get(&Self::make_key(namespace, key)).cloned())
    }

    fn put(&mut self, namespace: &Namespace, key: Vec<u8>, value: Vec<u8>) -> StateResult<()> {
        self.store.insert(Self::make_key(namespace, &key), value);
        Ok(())
    }

    fn delete(&mut self, namespace: &Namespace, key: &[u8]) -> StateResult<()> {
        self.store.remove(&Self::make_key(namespace, key));
        Ok(())
    }

    fn clear_namespace(&mut self, namespace: &Namespace) -> StateResult<()> {
        let op = namespace.operator_id().to_owned();
        let name = namespace.state_name().to_owned();
        self.store.retain(|(o, n, _), _| o != &op || n != &name);
        Ok(())
    }

    fn list_namespaces(&self) -> StateResult<Vec<Namespace>> {
        let mut seen = std::collections::BTreeSet::new();
        for (op_id, state_name, _) in self.store.keys() {
            seen.insert(Namespace::new(op_id, state_name));
        }
        Ok(seen.into_iter().collect())
    }

    fn list_keys(&self, namespace: &Namespace) -> StateResult<Vec<Vec<u8>>> {
        let op = namespace.operator_id();
        let name = namespace.state_name();
        Ok(self
            .store
            .keys()
            .filter(|(o, n, _)| o == op && n == name)
            .map(|(_, _, k)| k.clone())
            .collect())
    }

    fn snapshot(&self) -> StateResult<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(&1u32.to_le_bytes()); // version
        out.extend_from_slice(&(self.store.len() as u64).to_le_bytes());
        for ((op_id, state_name, key), value) in &self.store {
            let ob = op_id.as_bytes();
            out.extend_from_slice(&(ob.len() as u64).to_le_bytes());
            out.extend_from_slice(ob);
            let nb = state_name.as_bytes();
            out.extend_from_slice(&(nb.len() as u64).to_le_bytes());
            out.extend_from_slice(nb);
            out.extend_from_slice(&(key.len() as u64).to_le_bytes());
            out.extend_from_slice(key);
            out.extend_from_slice(&(value.len() as u64).to_le_bytes());
            out.extend_from_slice(value);
        }
        Ok(out)
    }

    fn load_snapshot(&mut self, bytes: &[u8]) -> StateResult<()> {
        let entries = decode_snapshot_entries(bytes)?;
        let mut new_store = BTreeMap::new();
        for (op_id, state_name, key, value) in entries {
            new_store.insert((op_id, state_name, key), value);
        }
        self.store = new_store;
        Ok(())
    }
}
