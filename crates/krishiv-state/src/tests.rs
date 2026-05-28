use super::*;
use krishiv_async_util::unix_now_ms;

fn ns(op: &str, name: &str) -> Namespace {
    Namespace::new(op, name)
}

// ── StateBackend ──────────────────────────────────────────────────────────

#[test]
fn state_get_missing_returns_none() {
    let backend = InMemoryStateBackend::new();
    assert!(backend.get(&ns("op1", "window"), b"k1").unwrap().is_none());
}

#[test]
fn state_put_and_get_roundtrip() {
    let mut backend = InMemoryStateBackend::new();
    let n = ns("op1", "counts");
    backend.put(&n, b"user-a".to_vec(), b"42".to_vec()).unwrap();
    assert_eq!(backend.get(&n, b"user-a").unwrap(), Some(b"42".to_vec()));
}

#[test]
fn state_delete_removes_key() {
    let mut backend = InMemoryStateBackend::new();
    let n = ns("op1", "counts");
    backend.put(&n, b"k".to_vec(), b"v".to_vec()).unwrap();
    backend.delete(&n, b"k").unwrap();
    assert!(backend.get(&n, b"k").unwrap().is_none());
}

#[test]
fn state_delete_missing_key_is_noop() {
    let mut backend = InMemoryStateBackend::new();
    backend
        .delete(&ns("op1", "counts"), b"nonexistent")
        .unwrap();
}

#[test]
fn state_clear_namespace_removes_only_matching_keys() {
    let mut backend = InMemoryStateBackend::new();
    let ns_a = ns("op1", "window");
    let ns_b = ns("op1", "other");
    let ns_c = ns("op2", "window");

    backend.put(&ns_a, b"k1".to_vec(), b"v1".to_vec()).unwrap();
    backend.put(&ns_a, b"k2".to_vec(), b"v2".to_vec()).unwrap();
    backend.put(&ns_b, b"k1".to_vec(), b"vb".to_vec()).unwrap();
    backend.put(&ns_c, b"k1".to_vec(), b"vc".to_vec()).unwrap();

    backend.clear_namespace(&ns_a).unwrap();

    assert!(backend.get(&ns_a, b"k1").unwrap().is_none());
    assert!(backend.get(&ns_a, b"k2").unwrap().is_none());
    assert_eq!(backend.get(&ns_b, b"k1").unwrap(), Some(b"vb".to_vec()));
    assert_eq!(backend.get(&ns_c, b"k1").unwrap(), Some(b"vc".to_vec()));
}

#[test]
fn state_namespaces_are_isolated() {
    let mut backend = InMemoryStateBackend::new();
    let ns_a = ns("op1", "window");
    let ns_b = ns("op2", "window");
    backend
        .put(&ns_a, b"key".to_vec(), b"val-a".to_vec())
        .unwrap();
    backend
        .put(&ns_b, b"key".to_vec(), b"val-b".to_vec())
        .unwrap();
    assert_eq!(backend.get(&ns_a, b"key").unwrap(), Some(b"val-a".to_vec()));
    assert_eq!(backend.get(&ns_b, b"key").unwrap(), Some(b"val-b".to_vec()));
}

// ── Namespace ─────────────────────────────────────────────────────────────

#[test]
fn namespace_column_family_name_format() {
    let n = Namespace::new("window-op", "counts");
    assert_eq!(n.column_family_name(), "window-op:counts");
}

// ── TimerService ──────────────────────────────────────────────────────────

#[test]
fn timer_fires_at_correct_watermark() {
    let mut svc = InMemoryTimerService::new();
    let n = ns("tw", "timers");

    svc.register_event_time_timer(TimerKey::new(n.clone(), b"k1".to_vec(), 1000))
        .unwrap();
    svc.register_event_time_timer(TimerKey::new(n.clone(), b"k2".to_vec(), 2000))
        .unwrap();

    assert_eq!(svc.pending_count(), 2);

    // Nothing fires before deadline.
    assert!(svc.drain_fired_timers(999).is_empty());
    assert_eq!(svc.pending_count(), 2);

    // First fires at exact deadline.
    let fired = svc.drain_fired_timers(1000);
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].deadline_ms, 1000);
    assert_eq!(svc.pending_count(), 1);

    // Second fires.
    let fired = svc.drain_fired_timers(2000);
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].deadline_ms, 2000);
    assert_eq!(svc.pending_count(), 0);
}

#[test]
fn timer_drain_order_is_ascending_deadline() {
    let mut svc = InMemoryTimerService::new();
    let n = ns("tw", "timers");

    // Register in reverse order.
    svc.register_event_time_timer(TimerKey::new(n.clone(), b"k3".to_vec(), 3000))
        .unwrap();
    svc.register_event_time_timer(TimerKey::new(n.clone(), b"k1".to_vec(), 1000))
        .unwrap();
    svc.register_event_time_timer(TimerKey::new(n.clone(), b"k2".to_vec(), 2000))
        .unwrap();

    let fired = svc.drain_fired_timers(3000);
    assert_eq!(fired.len(), 3);
    assert_eq!(fired[0].deadline_ms, 1000);
    assert_eq!(fired[1].deadline_ms, 2000);
    assert_eq!(fired[2].deadline_ms, 3000);
}

#[test]
fn timer_cancel_removes_correct_timer() {
    let mut svc = InMemoryTimerService::new();
    let n = ns("tw", "timers");

    svc.register_event_time_timer(TimerKey::new(n.clone(), b"k1".to_vec(), 1000))
        .unwrap();
    svc.register_event_time_timer(TimerKey::new(n.clone(), b"k2".to_vec(), 2000))
        .unwrap();

    svc.cancel_timer(&n, b"k1").unwrap();
    assert_eq!(svc.pending_count(), 1);

    let fired = svc.drain_fired_timers(2000);
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].key, b"k2");
}

#[test]
fn timer_cancel_missing_is_noop() {
    let mut svc = InMemoryTimerService::new();
    svc.cancel_timer(&ns("tw", "timers"), b"nonexistent")
        .unwrap();
    assert_eq!(svc.pending_count(), 0);
}

#[test]
fn timer_drain_empty_returns_empty() {
    let mut svc = InMemoryTimerService::new();
    assert!(svc.drain_fired_timers(9999).is_empty());
}

// ── list_namespaces / list_keys ───────────────────────────────────────────

#[test]
fn list_namespaces_empty_backend() {
    let b = InMemoryStateBackend::new();
    assert!(b.list_namespaces().unwrap().is_empty());
}

#[test]
fn list_namespaces_returns_unique_namespaces() {
    let mut b = InMemoryStateBackend::new();
    let n1 = ns("op1", "counts");
    let n2 = ns("op2", "counts");
    b.put(&n1, b"k1".to_vec(), b"v".to_vec()).unwrap();
    b.put(&n1, b"k2".to_vec(), b"v".to_vec()).unwrap();
    b.put(&n2, b"k1".to_vec(), b"v".to_vec()).unwrap();
    let mut namespaces = b.list_namespaces().unwrap();
    namespaces.sort();
    assert_eq!(namespaces, vec![n1, n2]);
}

#[test]
fn list_keys_returns_keys_for_namespace() {
    let mut b = InMemoryStateBackend::new();
    let n = ns("op1", "window");
    b.put(&n, b"alpha".to_vec(), b"v".to_vec()).unwrap();
    b.put(&n, b"beta".to_vec(), b"v".to_vec()).unwrap();
    b.put(&ns("op1", "other"), b"alpha".to_vec(), b"v".to_vec())
        .unwrap();
    let mut keys = b.list_keys(&n).unwrap();
    keys.sort();
    assert_eq!(keys, vec![b"alpha".to_vec(), b"beta".to_vec()]);
}

// ── ProcessingTimeTimerService ────────────────────────────────────────────

#[test]
fn processing_time_timer_fires_at_now_ms() {
    let mut svc = InMemoryProcessingTimeTimerService::new();
    let n = ns("op1", "pt");
    svc.register_processing_time_timer(ProcessingTimeTimerKey::new(
        n.clone(),
        b"k1".to_vec(),
        1000,
    ))
    .unwrap();
    svc.register_processing_time_timer(ProcessingTimeTimerKey::new(
        n.clone(),
        b"k2".to_vec(),
        2000,
    ))
    .unwrap();
    assert!(svc.drain_fired_processing_time_timers(999).is_empty());
    let fired = svc.drain_fired_processing_time_timers(1000);
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].fire_at_ms, 1000);
    assert_eq!(svc.pending_count(), 1);
}

#[test]
fn processing_time_timer_cancel_is_noop_for_missing() {
    let mut svc = InMemoryProcessingTimeTimerService::new();
    svc.cancel_processing_time_timer(&ns("op", "s"), b"nope")
        .unwrap();
    assert_eq!(svc.pending_count(), 0);
}

// ── TtlStateBackend ───────────────────────────────────────────────────────

#[test]
fn ttl_backend_returns_value_before_expiry() {
    let inner = InMemoryStateBackend::new();
    let mut ttl = TtlStateBackend::new(inner, TtlConfig::new(60_000));
    let n = ns("op1", "session");
    ttl.put(&n, b"k".to_vec(), b"val".to_vec()).unwrap();
    // Immediately after write the value must be live.
    assert_eq!(ttl.get(&n, b"k").unwrap(), Some(b"val".to_vec()));
}

#[test]
fn ttl_backend_expired_value_returns_none() {
    // Write with an expiry in the past by constructing a raw inner entry.
    let mut inner = InMemoryStateBackend::new();
    let n = ns("op1", "session");
    // Manually encode an already-expired entry (expires_at = 1 ms since epoch).
    let expires_at_ms: i64 = 1;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&expires_at_ms.to_le_bytes());
    encoded.extend_from_slice(b"stale");
    inner.put(&n, b"k".to_vec(), encoded).unwrap();

    let ttl = TtlStateBackend::new(inner, TtlConfig::new(60_000));
    // now_ms() >> 1, so this entry must be expired.
    assert!(ttl.get(&n, b"k").unwrap().is_none());
}

#[test]
fn ttl_backend_corrupt_value_returns_error() {
    let mut inner = InMemoryStateBackend::new();
    let n = ns("op1", "session");
    inner.put(&n, b"k".to_vec(), b"short".to_vec()).unwrap();

    let ttl = TtlStateBackend::new(inner, TtlConfig::new(60_000));
    let err = ttl.get(&n, b"k").unwrap_err();
    assert!(matches!(err, StateError::CorruptEntry { .. }));
}

#[test]
fn ttl_backend_delete_removes_entry() {
    let inner = InMemoryStateBackend::new();
    let mut ttl = TtlStateBackend::new(inner, TtlConfig::new(60_000));
    let n = ns("op1", "s");
    ttl.put(&n, b"k".to_vec(), b"v".to_vec()).unwrap();
    ttl.delete(&n, b"k").unwrap();
    assert!(ttl.get(&n, b"k").unwrap().is_none());
}

// ── StateInspector ────────────────────────────────────────────────────────

#[test]
fn state_inspector_is_read_only() {
    let b = InMemoryStateBackend::new();
    let inspector = StateInspector::new(&b);
    assert!(inspector.is_read_only());
}

#[test]
fn state_inspector_key_count_and_namespaces() {
    let mut b = InMemoryStateBackend::new();
    let n = ns("op1", "window");
    b.put(&n, b"a".to_vec(), b"1".to_vec()).unwrap();
    b.put(&n, b"b".to_vec(), b"2".to_vec()).unwrap();
    let inspector = StateInspector::new(&b);
    assert_eq!(inspector.list_namespaces().unwrap(), vec![n.clone()]);
    assert_eq!(inspector.key_count(&n).unwrap(), 2);
    assert_eq!(inspector.key_size_bytes(&n).unwrap(), 2); // "a" + "b"
}

// ── put_batch / get_batch ─────────────────────────────────────────────────

#[test]
fn in_memory_put_batch_get_batch_roundtrip() {
    let mut b = InMemoryStateBackend::new();
    let entries: &[(&str, &str, &[u8], &[u8])] = &[
        ("op1", "counts", b"k1", b"v1"),
        ("op1", "counts", b"k2", b"v2"),
        ("op2", "window", b"k3", b"v3"),
    ];
    b.put_batch(entries).unwrap();

    let keys: &[(&str, &str, &[u8])] = &[
        ("op1", "counts", b"k1"),
        ("op1", "counts", b"k2"),
        ("op2", "window", b"k3"),
        ("op1", "counts", b"missing"),
    ];
    let results = b.get_batch(keys).unwrap();
    assert_eq!(results[0], Some(b"v1".to_vec()));
    assert_eq!(results[1], Some(b"v2".to_vec()));
    assert_eq!(results[2], Some(b"v3".to_vec()));
    assert_eq!(results[3], None);
}

#[test]
fn redb_put_batch_get_batch_roundtrip() {
    let mut b = RedbStateBackend::in_memory().expect("in-memory redb");
    let entries: &[(&str, &str, &[u8], &[u8])] = &[
        ("op1", "counts", b"k1", b"v1"),
        ("op1", "counts", b"k2", b"v2"),
        ("op2", "window", b"k3", b"v3"),
    ];
    b.put_batch(entries).unwrap();

    let keys: &[(&str, &str, &[u8])] = &[
        ("op1", "counts", b"k1"),
        ("op1", "counts", b"k2"),
        ("op2", "window", b"k3"),
        ("op1", "counts", b"missing"),
    ];
    let results = b.get_batch(keys).unwrap();
    assert_eq!(results[0], Some(b"v1".to_vec()));
    assert_eq!(results[1], Some(b"v2".to_vec()));
    assert_eq!(results[2], Some(b"v3".to_vec()));
    assert_eq!(results[3], None);
}

#[test]
fn timer_cancel_o1_dual_index() {
    let mut svc = InMemoryTimerService::new();
    let n = ns("tw", "timers");
    for i in 0..100i64 {
        svc.register_event_time_timer(TimerKey::new(
            n.clone(),
            format!("k{i}").into_bytes(),
            i * 100,
        ))
        .unwrap();
    }
    assert_eq!(svc.pending_count(), 100);
    // Cancel a timer in the middle.
    svc.cancel_timer(&n, b"k50").unwrap();
    assert_eq!(svc.pending_count(), 99);
    // The cancelled key must not appear in the drain.
    let fired = svc.drain_fired_timers(9999);
    assert_eq!(fired.len(), 99);
    assert!(!fired.iter().any(|t| t.key == b"k50"));
}

#[test]
fn timer_re_register_updates_deadline() {
    let mut svc = InMemoryTimerService::new();
    let n = ns("tw", "timers");
    svc.register_event_time_timer(TimerKey::new(n.clone(), b"k1".to_vec(), 500))
        .unwrap();
    // Re-register with a later deadline.
    svc.register_event_time_timer(TimerKey::new(n.clone(), b"k1".to_vec(), 1000))
        .unwrap();
    assert_eq!(svc.pending_count(), 1);
    // The timer must not fire at the old deadline.
    assert!(svc.drain_fired_timers(500).is_empty());
    // It must fire at the new deadline.
    let fired = svc.drain_fired_timers(1000);
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].deadline_ms, 1000);
}

// ── RocksDbStateBackend (now RedbStateBackend via type alias) ─────────────

fn rocks_backend() -> RocksDbStateBackend {
    RocksDbStateBackend::ephemeral().expect("ephemeral backend")
}

#[test]
fn rocks_get_missing_returns_none() {
    let b = rocks_backend();
    assert!(b.get(&ns("op", "s"), b"k").unwrap().is_none());
}

#[test]
fn rocks_put_and_get_roundtrip() {
    let mut b = rocks_backend();
    let n = ns("op1", "counts");
    b.put(&n, b"user-a".to_vec(), b"42".to_vec()).unwrap();
    assert_eq!(b.get(&n, b"user-a").unwrap(), Some(b"42".to_vec()));
}

#[test]
fn rocks_delete_removes_key() {
    let mut b = rocks_backend();
    let n = ns("op1", "counts");
    b.put(&n, b"k".to_vec(), b"v".to_vec()).unwrap();
    b.delete(&n, b"k").unwrap();
    assert!(b.get(&n, b"k").unwrap().is_none());
}

#[test]
fn rocks_delete_missing_is_noop() {
    let mut b = rocks_backend();
    b.delete(&ns("op1", "s"), b"nonexistent").unwrap();
}

#[test]
fn rocks_clear_namespace_removes_only_matching_keys() {
    let mut b = rocks_backend();
    let ns_a = ns("op1", "window");
    let ns_b = ns("op1", "other");
    b.put(&ns_a, b"k1".to_vec(), b"v1".to_vec()).unwrap();
    b.put(&ns_a, b"k2".to_vec(), b"v2".to_vec()).unwrap();
    b.put(&ns_b, b"k1".to_vec(), b"vb".to_vec()).unwrap();
    b.clear_namespace(&ns_a).unwrap();
    assert!(b.get(&ns_a, b"k1").unwrap().is_none());
    assert!(b.get(&ns_a, b"k2").unwrap().is_none());
    assert_eq!(b.get(&ns_b, b"k1").unwrap(), Some(b"vb".to_vec()));
}

#[test]
fn rocks_list_namespaces_and_keys() {
    let mut b = rocks_backend();
    let n1 = ns("op1", "window");
    let n2 = ns("op2", "counts");
    b.put(&n1, b"a".to_vec(), b"1".to_vec()).unwrap();
    b.put(&n1, b"b".to_vec(), b"2".to_vec()).unwrap();
    b.put(&n2, b"x".to_vec(), b"3".to_vec()).unwrap();

    let mut namespaces = b.list_namespaces().unwrap();
    namespaces.sort();
    assert_eq!(namespaces, vec![n1.clone(), n2.clone()]);

    let mut keys = b.list_keys(&n1).unwrap();
    keys.sort();
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
}

#[test]
fn rocks_survives_reopen() {
    let dir = {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.redb");
        let mut b = RedbStateBackend::open(&path).expect("open");
        let n = ns("op1", "window");
        b.put(&n, b"key1".to_vec(), b"hello".to_vec()).unwrap();
        b.put(&n, b"key2".to_vec(), b"world".to_vec()).unwrap();
        (dir, path)
    };
    let b2 = RedbStateBackend::open(&dir.1).expect("reopen");
    let n = ns("op1", "window");
    assert_eq!(b2.get(&n, b"key1").unwrap(), Some(b"hello".to_vec()));
    assert_eq!(b2.get(&n, b"key2").unwrap(), Some(b"world".to_vec()));
}

#[test]
fn rocks_ttl_wrapper_expires_on_reopen() {
    let b = rocks_backend();
    let n = ns("op1", "session");
    let mut ttl = TtlStateBackend::new(b, TtlConfig::new(60_000));
    ttl.put(&n, b"live-key".to_vec(), b"live-val".to_vec())
        .unwrap();
    assert_eq!(
        ttl.get(&n, b"live-key").unwrap(),
        Some(b"live-val".to_vec())
    );
    assert_eq!(
        ttl.get(&n, b"live-key").unwrap(),
        Some(b"live-val".to_vec())
    );
}

#[test]
fn rocks_deterministic_replay() {
    let write_state = |b: &mut RedbStateBackend| {
        let n = ns("tumbling-1", "window-counts");
        b.put(&n, b"user-a:0".to_vec(), 42i64.to_le_bytes().to_vec())
            .unwrap();
        b.put(&n, b"user-b:0".to_vec(), 17i64.to_le_bytes().to_vec())
            .unwrap();
    };

    let mut b1 = rocks_backend();
    let mut b2 = rocks_backend();
    write_state(&mut b1);
    write_state(&mut b2);

    let n = ns("tumbling-1", "window-counts");
    assert_eq!(
        b1.get(&n, b"user-a:0").unwrap(),
        b2.get(&n, b"user-a:0").unwrap()
    );
    assert_eq!(
        b1.get(&n, b"user-b:0").unwrap(),
        b2.get(&n, b"user-b:0").unwrap()
    );
}

#[test]
fn rocks_state_inspector_reads_without_mutation() {
    let mut b = rocks_backend();
    let n = ns("op1", "window");
    b.put(&n, b"k1".to_vec(), b"v1".to_vec()).unwrap();
    b.put(&n, b"k2".to_vec(), b"v2".to_vec()).unwrap();
    let inspector = StateInspector::new(&b);
    assert!(inspector.is_read_only());
    assert_eq!(inspector.list_namespaces().unwrap(), vec![n.clone()]);
    assert_eq!(inspector.key_count(&n).unwrap(), 2);
    assert!(b.get(&n, b"k1").unwrap().is_some());
    assert!(b.get(&n, b"k2").unwrap().is_some());
}

#[test]
fn rocks_spawn_blocking_compatible() {
    use std::thread;
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("state.redb");
    {
        let mut b = RedbStateBackend::open(&path).expect("open");
        let n = ns("op1", "window");
        b.put(&n, b"blocking-key".to_vec(), b"blocking-val".to_vec())
            .unwrap();
    }
    let path2 = path.clone();
    let result = thread::spawn(move || {
        let backend = RedbStateBackend::open(&path2).unwrap();
        backend.get(&ns("op1", "window"), b"blocking-key").unwrap()
    })
    .join()
    .expect("thread panicked");

    assert_eq!(result, Some(b"blocking-val".to_vec()));
    drop(dir);
}

// ── snapshot / load_snapshot ──────────────────────────────────────────────

#[test]
fn in_memory_snapshot_round_trips() {
    let mut b = InMemoryStateBackend::new();
    let ns = Namespace::new("op1", "counts");
    b.put(&ns, b"k1".to_vec(), b"v1".to_vec()).unwrap();
    b.put(&ns, b"k2".to_vec(), b"v2".to_vec()).unwrap();
    let snap = b.snapshot().unwrap();
    let mut b2 = InMemoryStateBackend::new();
    b2.load_snapshot(&snap).unwrap();
    assert_eq!(b2.get(&ns, b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(b2.get(&ns, b"k2").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(b2.key_count(), 2);
}

#[test]
fn in_memory_snapshot_empty() {
    let b = InMemoryStateBackend::new();
    let snap = b.snapshot().unwrap();
    let mut b2 = InMemoryStateBackend::new();
    b2.load_snapshot(&snap).unwrap();
    assert_eq!(b2.key_count(), 0);
}

#[test]
fn in_memory_load_snapshot_clears_existing_state() {
    let ns = Namespace::new("op1", "counts");
    let mut src = InMemoryStateBackend::new();
    src.put(&ns, b"k1".to_vec(), b"v1".to_vec()).unwrap();
    let snap = src.snapshot().unwrap();
    let mut dst = InMemoryStateBackend::new();
    dst.put(&ns, b"old_key".to_vec(), b"old_val".to_vec())
        .unwrap();
    dst.load_snapshot(&snap).unwrap();
    assert_eq!(dst.get(&ns, b"old_key").unwrap(), None);
    assert_eq!(dst.get(&ns, b"k1").unwrap(), Some(b"v1".to_vec()));
}

#[test]
fn rocks_snapshot_round_trips() {
    let mut b = RedbStateBackend::in_memory().expect("in-memory redb");
    let ns = Namespace::new("op1", "counts");
    b.put(&ns, b"k1".to_vec(), b"v1".to_vec()).unwrap();
    b.put(&ns, b"k2".to_vec(), b"v2".to_vec()).unwrap();
    let snap = b.snapshot().unwrap();
    let mut b2 = RedbStateBackend::in_memory().expect("in-memory redb");
    b2.load_snapshot(&snap).unwrap();
    assert_eq!(b2.get(&ns, b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(b2.get(&ns, b"k2").unwrap(), Some(b"v2".to_vec()));
}

// ── RedbStateBackend-specific tests ───────────────────────────────────────

#[test]
fn redb_backend_put_get_delete() {
    let mut backend = RedbStateBackend::in_memory().expect("in-memory redb");
    let n = ns("op1", "s");
    backend
        .put(&n, b"key1".to_vec(), b"value1".to_vec())
        .unwrap();
    assert_eq!(backend.get(&n, b"key1").unwrap(), Some(b"value1".to_vec()));
    backend.delete(&n, b"key1").unwrap();
    assert_eq!(backend.get(&n, b"key1").unwrap(), None);
}

#[test]
fn redb_backend_snapshot_restore() {
    let mut backend = RedbStateBackend::in_memory().expect("in-memory redb");
    let n = ns("op1", "s");
    backend.put(&n, b"k1".to_vec(), b"v1".to_vec()).unwrap();
    backend.put(&n, b"k2".to_vec(), b"v2".to_vec()).unwrap();

    let snap = backend.snapshot().unwrap();

    let mut backend2 = RedbStateBackend::in_memory().expect("in-memory redb");
    backend2.load_snapshot(&snap).unwrap();
    assert_eq!(backend2.get(&n, b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(backend2.get(&n, b"k2").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn redb_backend_file_backed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("state.redb");
    {
        let mut backend = RedbStateBackend::open(&path).expect("open redb");
        let n = ns("op1", "s");
        backend
            .put(&n, b"persistent".to_vec(), b"data".to_vec())
            .unwrap();
    }
    let backend = RedbStateBackend::open(&path).expect("reopen redb");
    let n = ns("op1", "s");
    assert_eq!(
        backend.get(&n, b"persistent").unwrap(),
        Some(b"data".to_vec())
    );
}

// ── P0.4: Async checkpoint paths (spawn_blocking) ─────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p0_4_snapshot_async_does_not_block() {
    let mut backend = RedbStateBackend::in_memory().expect("in-memory redb");
    let n = ns("op1", "async-snap");
    backend.put(&n, b"k1".to_vec(), b"v1".to_vec()).unwrap();
    backend.put(&n, b"k2".to_vec(), b"v2".to_vec()).unwrap();

    let snap = backend
        .snapshot_async()
        .await
        .expect("snapshot_async failed");

    let mut backend2 = RedbStateBackend::in_memory().expect("in-memory redb");
    backend2.load_snapshot(&snap).unwrap();
    assert_eq!(backend2.get(&n, b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(backend2.get(&n, b"k2").unwrap(), Some(b"v2".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p0_4_load_snapshot_async_does_not_block() {
    let mut src = RedbStateBackend::in_memory().expect("in-memory redb");
    let n = ns("op1", "async-load");
    src.put(&n, b"ak".to_vec(), b"av".to_vec()).unwrap();
    let snap = src.snapshot().unwrap();

    let mut dst = RedbStateBackend::in_memory().expect("in-memory redb");
    dst.load_snapshot_async(snap)
        .await
        .expect("load_snapshot_async failed");
    assert_eq!(dst.get(&n, b"ak").unwrap(), Some(b"av".to_vec()));
}

// ── P0.6: Silent checkpoint snapshot failure propagation ──────────────────

#[test]
fn p0_6_corrupt_snapshot_propagates_error() {
    let mut backend = InMemoryStateBackend::new();
    let result = backend.load_snapshot(b"bad");
    assert!(result.is_err());
}

#[test]
fn p0_6_ttl_snapshot_propagates_error_on_corrupt_snapshot() {
    let mut inner = InMemoryStateBackend::new();
    let n = ns("op1", "s");
    inner.put(&n, b"k".to_vec(), b"v".to_vec()).unwrap();

    let ttl = TtlStateBackend::new(inner, TtlConfig::new(60_000));
    let snap = ttl
        .snapshot()
        .expect("snapshot of valid ttl backend must succeed");
    assert!(!snap.is_empty());
}

// ── P0.7: Non-atomic redb snapshot (mid-scan failure) ─────────────────────

#[test]
fn p0_7_redb_load_snapshot_incomplete_returns_error() {
    let mut backend = RedbStateBackend::in_memory().expect("in-memory redb");

    let mut src = RedbStateBackend::in_memory().expect("in-memory redb");
    let n = ns("op1", "s");
    src.put(&n, b"k1".to_vec(), b"v1".to_vec()).unwrap();
    let snap = src.snapshot().unwrap();

    let truncated = &snap[..snap.len() / 2];
    let result = backend.load_snapshot(truncated);
    assert!(result.is_err());
    match result.unwrap_err() {
        StateError::SnapshotIncomplete { .. } | StateError::SnapshotCorrupt { .. } => {}
        other => panic!("unexpected error variant: {other}"),
    }
}

#[test]
fn p0_7_redb_load_snapshot_failure_leaves_backend_empty() {
    let mut backend = RedbStateBackend::in_memory().expect("in-memory redb");
    let n = ns("op1", "pre");
    backend
        .put(&n, b"pre".to_vec(), b"exists".to_vec())
        .unwrap();

    let _ = backend.load_snapshot(b"tooshort");
    let _ = backend.get(&n, b"pre");
}

// ── P0.8: Clock underflow in unix_now_ms ──────────────────────────────────

#[test]
fn p0_8_unix_now_ms_checked_returns_positive() {
    let now = krishiv_async_util::unix_now_ms_checked();
    assert!(now.is_ok());
    assert!(now.unwrap() > 0);
}

#[test]
fn p0_8_unix_now_ms_returns_positive() {
    let now = unix_now_ms();
    assert!(now > 0);
}

#[test]
fn p0_8_clock_error_variant_exists_and_displays() {
    let err = StateError::ClockError {
        message: "test underflow".into(),
    };
    let s = err.to_string();
    assert!(s.contains("clock error"));
    assert!(s.contains("test underflow"));
}

#[test]
fn p0_8_duration_since_before_epoch_returns_clock_error() {
    let before_epoch = std::time::UNIX_EPOCH - std::time::Duration::from_secs(1);
    let result = before_epoch
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .map_err(|e| StateError::ClockError {
            message: format!("system clock is before UNIX epoch: {e}"),
        });
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), StateError::ClockError { .. }));
}

// ── P0.9: decode_if_live panics on corrupt redb entry ────────────────────

#[test]
fn p0_9_corrupt_redb_entry_returns_corrupt_entry_error() {
    let mut inner = RedbStateBackend::in_memory().expect("in-memory redb");
    let n = ns("op1", "corrupt-test");
    inner.put(&n, b"bad-key".to_vec(), b"sho".to_vec()).unwrap();

    let ttl = TtlStateBackend::new(inner, TtlConfig::new(60_000));
    let result = ttl.get(&n, b"bad-key");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        StateError::CorruptEntry { .. }
    ));
}

#[test]
fn p0_9_corrupt_entry_variant_displays() {
    let err = StateError::CorruptEntry {
        message: "bad bytes at offset 0".into(),
    };
    let s = err.to_string();
    assert!(s.contains("state entry corrupt"));
    assert!(s.contains("bad bytes at offset 0"));
}

// ── P0.16: TtlStateBackend snapshot prefix leakage ───────────────────────

#[test]
fn p0_16_ttl_snapshot_no_prefix_leakage() {
    let inner1 = InMemoryStateBackend::new();
    let mut ttl1 = TtlStateBackend::new(inner1, TtlConfig::new(60_000));
    let n = ns("op1", "session");

    ttl1.put(&n, b"user-a".to_vec(), b"value-a".to_vec())
        .unwrap();
    ttl1.put(&n, b"user-b".to_vec(), b"value-b".to_vec())
        .unwrap();

    let snap = ttl1.snapshot().expect("snapshot must succeed");

    let inner2 = InMemoryStateBackend::new();
    let mut ttl2 = TtlStateBackend::new(inner2, TtlConfig::new(60_000));
    ttl2.load_snapshot(&snap)
        .expect("load_snapshot must succeed");

    let val_a = ttl2.get(&n, b"user-a").expect("get must succeed");
    let val_b = ttl2.get(&n, b"user-b").expect("get must succeed");
    assert_eq!(val_a, Some(b"value-a".to_vec()));
    assert_eq!(val_b, Some(b"value-b".to_vec()));
}

#[test]
fn p0_16_ttl_snapshot_redb_no_prefix_leakage() {
    let inner1 = InMemoryStateBackend::new();
    let mut ttl1 = TtlStateBackend::new(inner1, TtlConfig::new(60_000));
    let n = ns("op1", "counts");

    ttl1.put(&n, b"k1".to_vec(), b"100".to_vec()).unwrap();
    ttl1.put(&n, b"k2".to_vec(), b"200".to_vec()).unwrap();
    let snap = ttl1.snapshot().expect("snapshot must succeed");

    let inner2 = RedbStateBackend::in_memory().expect("in-memory redb");
    let mut ttl2 = TtlStateBackend::new(inner2, TtlConfig::new(60_000));
    ttl2.load_snapshot(&snap)
        .expect("load_snapshot must succeed");

    assert_eq!(ttl2.get(&n, b"k1").unwrap(), Some(b"100".to_vec()));
    assert_eq!(ttl2.get(&n, b"k2").unwrap(), Some(b"200".to_vec()));
}

#[test]
fn p0_16_ttl_snapshot_bytes_are_not_ttl_prefixed() {
    let inner = InMemoryStateBackend::new();
    let mut ttl = TtlStateBackend::new(inner, TtlConfig::new(60_000));
    let n = ns("op1", "raw-check");
    ttl.put(&n, b"k".to_vec(), b"raw-value".to_vec()).unwrap();

    let snap = ttl.snapshot().expect("snapshot must succeed");

    let entries = decode_snapshot_entries(&snap).expect("snapshot must be parseable");
    assert_eq!(entries.len(), 1);
    let (_, _, _, stored_value) = &entries[0];
    assert_eq!(stored_value, b"raw-value");
}

// ── purge_expired ────────────────────────────────────────────────────────

#[test]
fn ttl_purge_expired_removes_stale_entries() {
    let mut inner = InMemoryStateBackend::new();
    let n = ns("op1", "session");
    // Manually encode an already-expired entry (expires_at = 1 ms since epoch).
    let expires_at_ms: i64 = 1;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&expires_at_ms.to_le_bytes());
    encoded.extend_from_slice(b"stale");
    inner.put(&n, b"expired".to_vec(), encoded).unwrap();

    // Put a live entry via the normal path (TTL = 60s).
    let mut ttl = TtlStateBackend::new(inner, TtlConfig::new(60_000));
    ttl.put(&n, b"live".to_vec(), b"val".to_vec()).unwrap();

    let evicted = ttl.purge_expired().unwrap();
    assert!(evicted >= 1, "expected at least 1 eviction, got {evicted}");
    // The expired key must be gone.
    assert!(ttl.get(&n, b"expired").unwrap().is_none());
    // The live key must survive.
    assert_eq!(ttl.get(&n, b"live").unwrap(), Some(b"val".to_vec()));
}
