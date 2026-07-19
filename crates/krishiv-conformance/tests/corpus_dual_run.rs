//! Phase 54 dual-run corpus gate: the corpus must stay green with the AQE
//! machinery OFF. Runtime (dynamic) filters are the adaptive mechanism that
//! touches embedded execution, so this binary — its own process, isolated
//! from the flag state of the main corpus run — disables them before any
//! engine is built and replays the full embedded corpus. Identical expected
//! results on both runs is the corpus-neutrality rule: an optimization that
//! changes any answer fails one of the two binaries.
//!
//! The scheduler-side AQE mechanisms (partition coalescing, skew split) act
//! only on staged dfplan jobs, which the scalar corpus tier never produces;
//! their on/off result-identity is covered by the coordinator tests in
//! `krishiv-scheduler` (`aqe_*`) and the staged-execution equality tests in
//! `krishiv-sql::distributed_plan`.

// Test harness: panicking on setup/corpus failure is the assertion.
#![allow(clippy::expect_used, clippy::panic)]

use std::path::PathBuf;

use krishiv_conformance::EmbeddedDriver;

fn corpus_files(tier: &str) -> Vec<PathBuf> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("corpus")
        .join(tier);
    let mut files = Vec::new();
    let mut stack = vec![root];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read corpus dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("slt") {
                files.push(path);
            }
        }
    }
    files.sort();
    assert!(!files.is_empty(), "corpus must not be empty");
    files
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corpus_embedded_runtime_filters_off() {
    // Own-process flag flip (the main corpus binary runs with defaults).
    krishiv_sql::set_runtime_filters_for_tests(false);
    for file in ["scalar", "stateful"].iter().flat_map(|t| corpus_files(t)) {
        let mut runner = sqllogictest::Runner::new(|| async { Ok(EmbeddedDriver::new()) });
        runner
            .run_file_async(&file)
            .await
            .unwrap_or_else(|e| panic!("corpus (runtime filters off) {}: {e}", file.display()));
    }
}
