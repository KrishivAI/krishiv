#![forbid(unsafe_code)]

//! Typed state descriptors for `ProcessFunction` operator state.
//!
//! The raw per-key state in [`crate::process_fn::ProcessContext`] is a `Vec<u8>`
//! that is treated as a JSON object `HashMap<String, serde_json::Value>`.
//! Each descriptor holds a string `key` that names its slot in that map.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

// ── StateValue marker trait ───────────────────────────────────────────────────

/// Marker trait for types that can be stored as operator state.
///
/// Any type that is `Serialize + DeserializeOwned + Default` automatically
/// qualifies. Implement this for your domain types or use the blanket impl.
pub trait StateValue: serde::Serialize + serde::de::DeserializeOwned + Default {}

/// Blanket impl: any `Serialize + DeserializeOwned + Default` type is a `StateValue`.
impl<T> StateValue for T where T: serde::Serialize + serde::de::DeserializeOwned + Default {}

// ── StateError ────────────────────────────────────────────────────────────────

/// Error type for state descriptor operations.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Serialization to the raw state bytes failed.
    #[error("serialization error: {0}")]
    Serialization(String),
    /// Deserialization from the raw state bytes failed.
    #[error("deserialization error: {0}")]
    Deserialization(String),
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Deserialise the raw state bytes into a `HashMap<String, serde_json::Value>`.
///
/// An empty or zero-length slice is treated as an empty map.
fn decode_map(raw: &[u8]) -> Result<HashMap<String, serde_json::Value>, StateError> {
    if raw.is_empty() {
        return Ok(HashMap::new());
    }
    serde_json::from_slice(raw).map_err(|e| StateError::Deserialization(e.to_string()))
}

/// Serialise a `HashMap<String, serde_json::Value>` back to the raw state buffer.
fn encode_map(
    raw: &mut Vec<u8>,
    map: HashMap<String, serde_json::Value>,
) -> Result<(), StateError> {
    let bytes = serde_json::to_vec(&map).map_err(|e| StateError::Serialization(e.to_string()))?;
    *raw = bytes;
    Ok(())
}

// ── ValueState ────────────────────────────────────────────────────────────────

/// Single-value state descriptor.
///
/// Reads and writes a single `T` value under a named key in the raw state map.
pub struct ValueState<T: StateValue> {
    key: String,
    _marker: PhantomData<T>,
}

impl<T: StateValue> ValueState<T> {
    /// Create a new `ValueState` with the given key.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            _marker: PhantomData,
        }
    }

    /// Read the current value from the raw state bytes.
    ///
    /// Returns `T::default()` if the key is absent.
    pub fn get(&self, raw: &[u8]) -> Result<T, StateError> {
        let map = decode_map(raw)?;
        match map.get(&self.key) {
            Some(v) => serde_json::from_value(v.clone())
                .map_err(|e| StateError::Deserialization(e.to_string())),
            None => Ok(T::default()),
        }
    }

    /// Serialise and write the value back into the raw state buffer.
    pub fn set(&self, raw: &mut Vec<u8>, value: &T) -> Result<(), StateError> {
        let mut map = decode_map(raw)?;
        let v =
            serde_json::to_value(value).map_err(|e| StateError::Serialization(e.to_string()))?;
        map.insert(self.key.clone(), v);
        encode_map(raw, map)
    }

    /// Remove this key's value from the raw state map (resets to `T::default()` on next `get`).
    pub fn clear(&self, raw: &mut Vec<u8>) {
        if let Ok(mut map) = decode_map(raw) {
            map.remove(&self.key);
            let _ = encode_map(raw, map);
        }
    }
}

// ── ListState ─────────────────────────────────────────────────────────────────

/// List-valued state descriptor.
///
/// Maintains an ordered list of `T` values under a named key.
pub struct ListState<T: StateValue> {
    key: String,
    _marker: PhantomData<T>,
}

impl<T: StateValue> ListState<T> {
    /// Create a new `ListState` with the given key.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            _marker: PhantomData,
        }
    }

    /// Read the current list from the raw state bytes.
    ///
    /// Returns an empty `Vec` if the key is absent.
    pub fn get(&self, raw: &[u8]) -> Result<Vec<T>, StateError> {
        let map = decode_map(raw)?;
        match map.get(&self.key) {
            Some(v) => serde_json::from_value(v.clone())
                .map_err(|e| StateError::Deserialization(e.to_string())),
            None => Ok(Vec::new()),
        }
    }

    /// Append one item to the list in the raw state buffer.
    pub fn add(&self, raw: &mut Vec<u8>, item: T) -> Result<(), StateError> {
        let mut map = decode_map(raw)?;
        let mut list: Vec<T> = match map.get(&self.key) {
            Some(v) => serde_json::from_value(v.clone())
                .map_err(|e| StateError::Deserialization(e.to_string()))?,
            None => Vec::new(),
        };
        list.push(item);
        let v = serde_json::to_value(list).map_err(|e| StateError::Serialization(e.to_string()))?;
        map.insert(self.key.clone(), v);
        encode_map(raw, map)
    }

    /// Remove all items from the list in the raw state buffer.
    pub fn clear(&self, raw: &mut Vec<u8>) {
        if let Ok(mut map) = decode_map(raw) {
            map.remove(&self.key);
            let _ = encode_map(raw, map);
        }
    }
}

// ── MapState ──────────────────────────────────────────────────────────────────

/// Map state descriptor.
///
/// Maintains a `HashMap<K, V>` under a named key in the raw state map.
pub struct MapState<K: StateValue, V: StateValue> {
    key: String,
    _marker: PhantomData<(K, V)>,
}

impl<K: StateValue + std::hash::Hash + Eq, V: StateValue> MapState<K, V> {
    /// Create a new `MapState` with the given key.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            _marker: PhantomData,
        }
    }

    /// Read the full map from the raw state bytes.
    ///
    /// Returns an empty `HashMap` if the key is absent.
    pub fn get_map(&self, raw: &[u8]) -> Result<HashMap<K, V>, StateError> {
        let map = decode_map(raw)?;
        match map.get(&self.key) {
            Some(v) => serde_json::from_value(v.clone())
                .map_err(|e| StateError::Deserialization(e.to_string())),
            None => Ok(HashMap::new()),
        }
    }

    /// Insert or update a key-value pair in the map.
    pub fn put(&self, raw: &mut Vec<u8>, k: K, v: V) -> Result<(), StateError> {
        let mut outer = decode_map(raw)?;
        let mut inner: HashMap<K, V> = match outer.get(&self.key) {
            Some(val) => serde_json::from_value(val.clone())
                .map_err(|e| StateError::Deserialization(e.to_string()))?,
            None => HashMap::new(),
        };
        inner.insert(k, v);
        let json_val =
            serde_json::to_value(inner).map_err(|e| StateError::Serialization(e.to_string()))?;
        outer.insert(self.key.clone(), json_val);
        encode_map(raw, outer)
    }

    /// Remove a key-value pair from the map.
    pub fn remove(&self, raw: &mut Vec<u8>, k: &K) -> Result<(), StateError> {
        let mut outer = decode_map(raw)?;
        let mut inner: HashMap<K, V> = match outer.get(&self.key) {
            Some(val) => serde_json::from_value(val.clone())
                .map_err(|e| StateError::Deserialization(e.to_string()))?,
            None => return Ok(()),
        };
        inner.remove(k);
        let json_val =
            serde_json::to_value(inner).map_err(|e| StateError::Serialization(e.to_string()))?;
        outer.insert(self.key.clone(), json_val);
        encode_map(raw, outer)
    }

    /// Remove all entries from the map.
    pub fn clear(&self, raw: &mut Vec<u8>) {
        if let Ok(mut map) = decode_map(raw) {
            map.remove(&self.key);
            let _ = encode_map(raw, map);
        }
    }
}

// ── ReducingState ─────────────────────────────────────────────────────────────

type ReducerFn<T> = Arc<dyn Fn(&T, &T) -> T + Send + Sync>;

/// Reducing state descriptor.
///
/// Folds incoming values with a combining function, storing only a single
/// accumulated value. Equivalent to Flink's `ReducingState`.
pub struct ReducingState<T: StateValue> {
    key: String,
    reducer: ReducerFn<T>,
}

impl<T: StateValue> ReducingState<T> {
    /// Create a new `ReducingState` with the given key and reducer function.
    pub fn new(
        key: impl Into<String>,
        reducer: impl Fn(&T, &T) -> T + Send + Sync + 'static,
    ) -> Self {
        Self {
            key: key.into(),
            reducer: Arc::new(reducer),
        }
    }

    /// Read the current accumulated value.
    ///
    /// Returns `None` if no values have been added yet.
    pub fn get(&self, raw: &[u8]) -> Result<Option<T>, StateError> {
        let map = decode_map(raw)?;
        match map.get(&self.key) {
            Some(v) => {
                let t: T = serde_json::from_value(v.clone())
                    .map_err(|e| StateError::Deserialization(e.to_string()))?;
                Ok(Some(t))
            }
            None => Ok(None),
        }
    }

    /// Add a new value, folding it into the accumulated state with the reducer.
    pub fn add(&self, raw: &mut Vec<u8>, value: T) -> Result<(), StateError> {
        let mut map = decode_map(raw)?;
        let new_val = match map.get(&self.key) {
            Some(existing) => {
                let acc: T = serde_json::from_value(existing.clone())
                    .map_err(|e| StateError::Deserialization(e.to_string()))?;
                (self.reducer)(&acc, &value)
            }
            None => value,
        };
        let json_val =
            serde_json::to_value(&new_val).map_err(|e| StateError::Serialization(e.to_string()))?;
        map.insert(self.key.clone(), json_val);
        encode_map(raw, map)
    }

    /// Remove the accumulated value from the state buffer.
    pub fn clear(&self, raw: &mut Vec<u8>) {
        if let Ok(mut map) = decode_map(raw) {
            map.remove(&self.key);
            let _ = encode_map(raw, map);
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_state_roundtrip() {
        let desc: ValueState<i64> = ValueState::new("counter");
        let mut raw: Vec<u8> = Vec::new();

        // Default when empty.
        assert_eq!(desc.get(&raw).unwrap(), 0i64);

        // Set and retrieve.
        desc.set(&mut raw, &42i64).unwrap();
        assert_eq!(desc.get(&raw).unwrap(), 42i64);

        // Overwrite.
        desc.set(&mut raw, &100i64).unwrap();
        assert_eq!(desc.get(&raw).unwrap(), 100i64);

        // Clear.
        desc.clear(&mut raw);
        assert_eq!(desc.get(&raw).unwrap(), 0i64);
    }

    #[test]
    fn list_state_accumulates() {
        let desc: ListState<String> = ListState::new("items");
        let mut raw: Vec<u8> = Vec::new();

        assert!(desc.get(&raw).unwrap().is_empty());

        desc.add(&mut raw, "a".to_string()).unwrap();
        desc.add(&mut raw, "b".to_string()).unwrap();
        desc.add(&mut raw, "c".to_string()).unwrap();

        let list = desc.get(&raw).unwrap();
        assert_eq!(list, vec!["a", "b", "c"]);

        desc.clear(&mut raw);
        assert!(desc.get(&raw).unwrap().is_empty());
    }

    #[test]
    fn map_state_put_and_remove() {
        let desc: MapState<String, i32> = MapState::new("scores");
        let mut raw: Vec<u8> = Vec::new();

        assert!(desc.get_map(&raw).unwrap().is_empty());

        desc.put(&mut raw, "alice".to_string(), 10).unwrap();
        desc.put(&mut raw, "bob".to_string(), 20).unwrap();

        let m = desc.get_map(&raw).unwrap();
        assert_eq!(m.get("alice"), Some(&10));
        assert_eq!(m.get("bob"), Some(&20));

        desc.remove(&mut raw, &"alice".to_string()).unwrap();
        let m2 = desc.get_map(&raw).unwrap();
        assert!(!m2.contains_key("alice"));
        assert_eq!(m2.get("bob"), Some(&20));

        desc.clear(&mut raw);
        assert!(desc.get_map(&raw).unwrap().is_empty());
    }

    #[test]
    fn reducing_state_folds_values() {
        let desc: ReducingState<i64> = ReducingState::new("sum", |a: &i64, b: &i64| a + b);
        let mut raw: Vec<u8> = Vec::new();

        // Empty returns None.
        assert!(desc.get(&raw).unwrap().is_none());

        desc.add(&mut raw, 5).unwrap();
        assert_eq!(desc.get(&raw).unwrap(), Some(5));

        desc.add(&mut raw, 10).unwrap();
        assert_eq!(desc.get(&raw).unwrap(), Some(15));

        desc.add(&mut raw, 3).unwrap();
        assert_eq!(desc.get(&raw).unwrap(), Some(18));

        desc.clear(&mut raw);
        assert!(desc.get(&raw).unwrap().is_none());
    }
}
