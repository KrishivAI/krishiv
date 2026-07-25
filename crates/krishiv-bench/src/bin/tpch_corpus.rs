// This binary's entire output is the corpus on stdout; it is a pipe source,
// not a service.
#![allow(clippy::print_stdout, clippy::print_stderr)]

//! Print the TPC-H corpus as JSON on stdout.
//!
//! The distributed runner is a Python script (it drives the coordinator's HTTP
//! API and has to survive multi-minute SF100 queries), but the queries
//! themselves must not be re-typed there. Two copies of 22 queries is two
//! copies that drift, and a drifted copy makes the single-node and distributed
//! numbers incomparable while still looking like a clean A/B.
//!
//! So the Rust corpus stays the single source of truth and this binary is the
//! seam: `tpch_corpus | python runner`.
//!
//! Usage: cargo run -p krishiv-bench --bin tpch_corpus --release

use krishiv_bench::tpch_queries::TPCH_QUERIES;

fn main() {
    let queries: Vec<serde_json::Value> = TPCH_QUERIES
        .iter()
        .map(|q| {
            serde_json::json!({
                "id": q.id,
                "name": q.name,
                "sql": q.sql,
                "tables": q.tables,
            })
        })
        .collect();
    let out = serde_json::json!({
        "count": queries.len(),
        "queries": queries,
    });
    // Serialisation of a `json!` tree cannot fail, but a panic here would
    // print a backtrace onto the very stdout the consumer is parsing. Exit
    // with a diagnostic on stderr and a non-zero status instead, so the
    // runner sees a failed pipe rather than corrupt JSON.
    match serde_json::to_string_pretty(&out) {
        Ok(json) => println!("{json}"),
        Err(error) => {
            eprintln!("failed to serialise the TPC-H corpus: {error}");
            std::process::exit(1);
        }
    }
}
