#![forbid(unsafe_code)]
//! `PushShuffleClient` against a real External Shuffle Service.
//!
//! # Why this exists
//!
//! The audit found `PushShuffleClient` had **zero constructors anywhere in the
//! workspace**: the ESS server routes are reachable via `krishiv shuffle-svc`,
//! but nothing in the engine builds the client — `ctx.push_store` is the
//! in-process `PushShuffleStore`, a different type. So a 200-line HTTP client
//! for a live server surface had never once talked to it, and the two real
//! bugs found inside it (a timeout it reported but did not enforce, and a
//! second full copy of every merged partition) were bugs nobody could hit.
//!
//! Whether T12 push-shuffle should be wired into the executor's hot path or
//! deleted is a product decision. What is *not* a judgement call is that the
//! client and the server must agree on the wire contract — route shapes, bearer
//! auth, and the merged-read completeness gate — and until now nothing checked
//! that. These tests stand up the real service on a real port and drive the
//! real client at it.

// Integration-test crate: helpers run outside `#[test]` fns, so clippy.toml's
// `allow-unwrap-in-tests` does not reach them. A panic is the failure signal here.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr
)]
use std::time::Duration;

use krishiv_executor::ess_client::PushShuffleClient;

/// Start the ESS on an ephemeral port and return its base URL.
///
/// `run_shuffle_svc` takes an address rather than a listener, so the port is
/// chosen by binding, dropping, and handing the number over. That is racy in
/// principle; in a test process that binds one port it is fine, and the
/// alternative is a `pub` listener variant that exists only for tests.
async fn start_ess(dir: &std::path::Path) -> String {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let addr = probe.local_addr().expect("probe addr");
    drop(probe);

    let base = dir.to_path_buf();
    tokio::spawn(async move {
        let _ = krishiv_shuffle::shuffle_svc::run_shuffle_svc(&base, addr).await;
    });

    // Wait for the listener rather than sleeping a fixed amount.
    let url = format!("http://{addr}");
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return url;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("ESS did not start listening on {addr}");
}

/// Push two map tasks' payloads and read them back merged, in push order.
///
/// This is the whole point of T12: one reduce-side fetch instead of one per map
/// task. If the client and server disagreed about the route shape, this is
/// where it would show.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pushed_partition_comes_back_merged_in_push_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let url = start_ess(dir.path()).await;
    let client = PushShuffleClient::new(&url, None).expect("client");

    client
        .push_partition("job-1", "stage-0", "task-a", 0, vec![0xAA; 16])
        .await
        .expect("push a");
    client
        .push_partition("job-1", "stage-0", "task-b", 0, vec![0xBB; 8])
        .await
        .expect("push b");

    let merged = client
        .fetch_merged("job-1", "stage-0", 0)
        .await
        .expect("merged read");
    assert_eq!(merged.len(), 24, "both pushes must appear");
    assert_eq!(&merged[..16], &[0xAAu8; 16], "push order must be preserved");
    assert_eq!(&merged[16..], &[0xBBu8; 8]);
}

/// A partition nothing has pushed is not an empty result — it is absent.
///
/// Serving empty here would let a reduce task read a partition that does not
/// exist yet and silently compute on nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetching_a_partition_nobody_pushed_is_an_error_not_an_empty_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let url = start_ess(dir.path()).await;
    let client = PushShuffleClient::new(&url, None).expect("client");

    let err = client
        .fetch_merged("job-missing", "stage-0", 0)
        .await
        .expect_err("an unpushed partition must not read as empty");
    assert!(
        err.to_string().contains("404"),
        "the client must surface the server's not-found: {err}"
    );
}

/// GC must actually release the job, as observed through the client.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_releases_the_job_through_the_client() {
    let dir = tempfile::tempdir().expect("tempdir");
    let url = start_ess(dir.path()).await;
    let client = PushShuffleClient::new(&url, None).expect("client");

    client
        .push_partition("job-gc", "stage-0", "task-a", 0, vec![1; 4])
        .await
        .expect("push");
    assert!(client.fetch_merged("job-gc", "stage-0", 0).await.is_ok());

    client.gc("job-gc").await.expect("gc");

    assert!(
        client.fetch_merged("job-gc", "stage-0", 0).await.is_err(),
        "GC must drop the pushed data"
    );
}

/// The client's configured timeout must be the one it enforces.
///
/// `with_timeout` used to set the field unconditionally and rebuild the HTTP
/// client only `if let Ok(...)`, so a failed rebuild left a client reporting
/// one timeout and enforcing another — and `push_partition`'s error message
/// interpolates the field, so it would have named a duration that never
/// applied. The old test asserted the private field, which could not catch
/// that; this asserts the effect against a black-hole address.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_configured_timeout_is_the_one_that_fires() {
    // A listener that accepts and then never answers: the connect succeeds, so
    // what bounds the call is the request timeout and nothing else.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream); // accepted, never read, never answered
        }
    });

    let client = PushShuffleClient::new(format!("http://{addr}"), None)
        .expect("client")
        .with_timeout(Duration::from_millis(300));
    assert_eq!(client.timeout(), Duration::from_millis(300));

    let started = std::time::Instant::now();
    let err = client
        .fetch_merged("job", "stage", 0)
        .await
        .expect_err("a server that never answers must time the client out");
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(5),
        "the 300 ms timeout did not fire — took {elapsed:?} (the default is 60 s, \
         which is what a client that reports one timeout and enforces another \
         would show)"
    );
    assert!(
        err.to_string().contains("failed"),
        "the error should name the failed call: {err}"
    );
}
