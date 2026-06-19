//! Key-group backend tests (R16 S4.1).

use krishiv_state::key_group::{
    NUM_KEY_GROUPS, key_group_for_key, key_group_ranges_for_parallelism,
};
use krishiv_state::{Namespace, RocksDbStateBackend, StateBackend};

#[test]
fn keys_hash_into_valid_key_groups() {
    let kg = key_group_for_key(b"user-42");
    assert!(kg < NUM_KEY_GROUPS);
}

#[test]
fn parallelism_four_covers_all_groups() {
    let ranges = key_group_ranges_for_parallelism(4);
    assert_eq!(ranges.len(), 4);
    assert_eq!(ranges[0].start, 0);
    assert_eq!(ranges[3].end, NUM_KEY_GROUPS - 1);
}

#[test]
fn put_get_roundtrip_with_key_group_prefix_in_redb() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.redb");
    let mut backend = RocksDbStateBackend::open(&path).unwrap();
    let ns = Namespace::new("op", "state");
    backend.put(&ns, b"k".to_vec(), b"v".to_vec()).unwrap();
    assert_eq!(backend.get(&ns, b"k").unwrap().unwrap(), b"v");
}
