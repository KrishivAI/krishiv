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
    println!(
        "{}",
        serde_json::to_string_pretty(&out).expect("corpus serialises")
    );
}
