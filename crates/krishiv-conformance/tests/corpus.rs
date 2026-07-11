//! Runs every `corpus/**/*.slt` file against the three execution placements.
//!
//! Regenerate expected results after an intentional behavior change with:
//! `KRISHIV_BLESS_CORPUS=1 cargo test -p krishiv-conformance` — then review
//! the corpus diff like any other code change.

// Test harness: panicking on setup/corpus failure is the assertion.
#![allow(clippy::expect_used, clippy::panic)]

use std::path::PathBuf;

use krishiv_conformance::{EmbeddedDriver, SessionDriver};

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

fn bless() -> bool {
    std::env::var("KRISHIV_BLESS_CORPUS").is_ok()
}

async fn run_corpus<D, F, Fut>(tiers: &[&str], make: F)
where
    D: sqllogictest::AsyncDB<ColumnType = sqllogictest::DefaultColumnType>,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<D, D::Error>>,
{
    for file in tiers.iter().flat_map(|t| corpus_files(t)) {
        // Fresh driver per file: files own their table namespaces.
        let mut runner = sqllogictest::Runner::new(&make);
        if bless() {
            runner
                .update_test_file(
                    &file,
                    " ",
                    sqllogictest::default_validator,
                    sqllogictest::default_normalizer,
                    sqllogictest::default_column_validator,
                )
                .await
                .unwrap_or_else(|e| panic!("bless {}: {e}", file.display()));
        } else {
            runner
                .run_file_async(&file)
                .await
                .unwrap_or_else(|e| panic!("corpus {}: {e}", file.display()));
        }
    }
}

// Placement matrix: embedded runs everything; the Flight placements run the
// scalar tier only until the Phase 60 SQL front door makes DDL state persist
// across remote statements (see corpus/stateful/README.md).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corpus_embedded() {
    run_corpus(&["scalar", "stateful"], || async {
        Ok(EmbeddedDriver::new())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corpus_single_node() {
    run_corpus(&["scalar"], SessionDriver::single_node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corpus_distributed_in_process() {
    run_corpus(&["scalar"], SessionDriver::distributed_in_process).await;
}
