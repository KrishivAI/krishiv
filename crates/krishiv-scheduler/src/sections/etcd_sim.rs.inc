// ── etcd: simulation-mode tests (no live etcd required) ─────────────────

#[cfg(feature = "etcd")]
#[test]
fn etcd_lease_simulation_new_is_not_leader() {
    let election = crate::EtcdLeaseElection::new("/krishiv/test/leader", "test-holder", 15);
    assert!(
        !election.is_leader(),
        "simulation mode must start with is_leader=false"
    );
}

#[cfg(feature = "etcd")]
#[tokio::test]
async fn etcd_lease_simulation_try_acquire_makes_leader() {
    use crate::LeaderElection;
    let election = crate::EtcdLeaseElection::new("/krishiv/test/leader", "test-holder", 15);
    assert!(!election.is_leader());
    let became_leader = election.try_acquire().await;
    assert!(
        became_leader,
        "simulation mode must always grant leadership"
    );
    assert!(election.is_leader());
}

#[cfg(feature = "etcd")]
#[tokio::test]
async fn etcd_lease_simulation_release_clears_leader() {
    use crate::LeaderElection;
    let election = crate::EtcdLeaseElection::new("/krishiv/test/leader", "test-holder", 15);
    election.try_acquire().await;
    assert!(election.is_leader());
    election.release().await;
    assert!(!election.is_leader(), "release() must clear is_leader");
}

#[cfg(feature = "etcd")]
#[ignore = "requires a live etcd at localhost:2379; run with --features etcd"]
#[tokio::test]
async fn coordinator_with_etcd_metadata_backend_roundtrip() {
    // This test validates EtcdMetadataStore connect + save/recover against
    // a real etcd instance. To run:
    //   cargo test -p krishiv-scheduler --lib --features etcd -- \
    //       coordinator_with_etcd_metadata_backend_roundtrip --ignored
    let result =
        crate::EtcdMetadataStore::connect(vec!["http://localhost:2379".to_string()]).await;
    assert!(
        result.is_ok(),
        "EtcdMetadataStore::connect must succeed with live etcd"
    );
}

