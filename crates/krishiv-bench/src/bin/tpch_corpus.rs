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
//! # Scale factor
//!
//! Q11's threshold is `0.0001 / SF` in the spec, so the emitted SQL depends on
//! the scale factor being measured. Pass `--scale-factor` to match the dataset;
//! it is recorded in the output so a corpus file can be checked against the run
//! that used it rather than being assumed to match.
//!
//! Usage: cargo run -p krishiv-bench --bin tpch_corpus --release -- [--scale-factor 100]

use krishiv_bench::tpch_queries::{TPCH_PRIMARY_KEYS, TPCH_QUERIES};

fn main() {
    // Default 1, the scale the spec's literal is written for: an unflagged
    // caller gets the SF1 corpus, which is wrong only if they are measuring
    // something else — and they had to pick a dataset to do that.
    let mut scale_factor = 1.0_f64;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--scale-factor" | "--scale" | "--sf" => {
                let Some(value) = args.next().and_then(|v| v.parse::<f64>().ok()) else {
                    eprintln!("--scale-factor needs a positive number");
                    std::process::exit(2);
                };
                if !(value.is_finite() && value > 0.0) {
                    eprintln!("--scale-factor must be finite and > 0 (got {value})");
                    std::process::exit(2);
                }
                scale_factor = value;
            }
            other => {
                eprintln!("unknown argument {other:?}; usage: tpch_corpus [--scale-factor N]");
                std::process::exit(2);
            }
        }
    }

    let queries: Vec<serde_json::Value> = TPCH_QUERIES
        .iter()
        .map(|q| {
            serde_json::json!({
                "id": q.id,
                "name": q.name,
                "sql": q.sql_at_scale(scale_factor),
                "tables": q.tables,
            })
        })
        .collect();
    // The schema's primary keys travel with the queries for the same reason the
    // SQL does: a runner that re-typed them would be running against a
    // different schema than the one this corpus describes, and the difference
    // would show up as a performance result rather than as an error.
    let primary_keys: serde_json::Map<String, serde_json::Value> = TPCH_PRIMARY_KEYS
        .iter()
        .map(|(table, key)| ((*table).to_owned(), serde_json::json!(key)))
        .collect();
    let out = serde_json::json!({
        "count": queries.len(),
        "scale_factor": scale_factor,
        "primary_keys": primary_keys,
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
