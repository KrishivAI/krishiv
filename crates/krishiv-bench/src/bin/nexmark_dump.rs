//! Dump the NEXMark generator's streams to CSV shards so an EXTERNAL engine
//! (the Spark Structured Streaming baseline, task #151 follow-up) consumes
//! byte-identical input to the terminal harness: same seed, same batch size,
//! same row counts. One file per 1000-row batch mirrors the push granularity.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]

use krishiv_bench::nexmark::NexmarkGenerator;

const BATCH_ROWS: usize = 1_000;
const BATCHES: usize = 100;
const MAX_LATENESS_MS: i64 = 200;

fn main() {
    let out = std::env::args()
        .nth(1)
        .expect("usage: nexmark_dump <out-dir>");
    for source in ["bid", "auction", "person"] {
        let dir = format!("{out}/{source}");
        std::fs::create_dir_all(&dir).expect("mkdir");
        let mut generator = NexmarkGenerator::new(0x4E45_584D, 1_000_000, 0, MAX_LATENESS_MS);
        let mut rows = 0usize;
        for i in 0..BATCHES {
            let batch = match source {
                "bid" => generator.next_bid_batch(BATCH_ROWS),
                "auction" => generator.next_auction_batch(BATCH_ROWS),
                _ => generator.next_person_batch(BATCH_ROWS),
            }
            .expect("generate");
            rows += batch.num_rows();
            let file = std::fs::File::create(format!("{dir}/part-{i:04}.csv")).expect("create");
            let mut writer = arrow::csv::WriterBuilder::new()
                .with_header(true)
                .build(file);
            writer.write(&batch).expect("write csv");
        }
        println!("{source}: {rows} rows -> {dir}");
    }
}
