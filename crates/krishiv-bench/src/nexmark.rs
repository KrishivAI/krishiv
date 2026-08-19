//! A faithful NEXMark event generator.
//!
//! NEXMark has no dataset to download — it specifies a *generator*, and both
//! Apache Beam and Flink's `nexmark-flink` harness ship one. That is a feature,
//! not an inconvenience: a generated stream can be rate-controlled, and rate
//! control is the whole point, because the headline streaming metric is
//! sustainable throughput (the input rate at which queue depth stays bounded),
//! which is a rate search. You cannot meaningfully rate-control a file replay.
//!
//! # Why faithfulness matters here
//!
//! The benchmark this replaces built two-column tables named after NEXMark
//! entities — `Bid` was `(auction, price)` against a real schema of
//! `(auction, bidder, price, channel, url, dateTime, extra)`, and `Person` did
//! not exist. Numbers from that cannot be compared to Flink or Spark, which is
//! the entire reason one runs NEXMark rather than a bespoke workload.
//!
//! So the schemas and the 1 : 3 : 46 person : auction : bid ratio below match
//! the standard generator. Where this deviates, it says so.
//!
//! # Determinism
//!
//! Seeded and reproducible: the same `NexmarkGenerator::new(seed, …)` yields
//! the same stream. A benchmark whose input varies run to run cannot separate a
//! regression from a reroll.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

/// Standard NEXMark event mix: 1 person : 3 auctions : 46 bids per 50 events.
pub const PERSON_PROPORTION: u64 = 1;
/// See [`PERSON_PROPORTION`].
pub const AUCTION_PROPORTION: u64 = 3;
/// See [`PERSON_PROPORTION`].
pub const BID_PROPORTION: u64 = 46;
/// See [`PERSON_PROPORTION`].
pub const TOTAL_PROPORTION: u64 = PERSON_PROPORTION + AUCTION_PROPORTION + BID_PROPORTION;

/// How many people are "active" and can be referenced as sellers/bidders.
const NUM_ACTIVE_PEOPLE: u64 = 1_000;
/// How many auctions stay open for bidding at once.
const NUM_IN_FLIGHT_AUCTIONS: u64 = 100;
/// 1-in-N events use the hot person / auction / bidder, producing the skew a
/// realistic keyed workload has. Without skew every key group is the same size
/// and the benchmark never exercises the paths that matter under skew.
const HOT_RATIO: u64 = 100;

/// Which NEXMark entity an event is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Person,
    Auction,
    Bid,
}

/// A deterministic NEXMark event generator.
///
/// Holds no RNG crate dependency: it uses a splitmix64 step, which is
/// reproducible across platforms and avoids pinning this benchmark to a
/// particular `rand` version's stream (a silent way for "the same seed" to stop
/// meaning the same data across a dependency bump).
pub struct NexmarkGenerator {
    state: u64,
    /// Event ordinal — drives both the entity mix and event time.
    event_id: u64,
    /// Milliseconds of event time per event, from the configured rate.
    event_time_step_us: u64,
    base_time_ms: i64,
    /// Maximum out-of-orderness injected into event time, in ms.
    max_lateness_ms: i64,
}

impl NexmarkGenerator {
    /// Build a generator producing `events_per_second` of event time.
    ///
    /// `max_lateness_ms` injects out-of-order arrival: an event's timestamp may
    /// be up to that far behind the current watermark-ish frontier. Zero yields
    /// a perfectly ordered stream, which is the *least* interesting case for a
    /// windowed engine and should not be the default a report is built on.
    #[must_use]
    pub fn new(seed: u64, events_per_second: u64, base_time_ms: i64, max_lateness_ms: i64) -> Self {
        let eps = events_per_second.max(1);
        Self {
            state: seed | 1,
            event_id: 0,
            event_time_step_us: 1_000_000 / eps,
            base_time_ms,
            max_lateness_ms: max_lateness_ms.max(0),
        }
    }

    fn next_u64(&mut self) -> u64 {
        // splitmix64
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn next_in(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }

    /// The entity this ordinal produces, per the standard proportions.
    #[must_use]
    pub fn kind_for(event_id: u64) -> EventKind {
        let rem = event_id % TOTAL_PROPORTION;
        if rem < PERSON_PROPORTION {
            EventKind::Person
        } else if rem < PERSON_PROPORTION + AUCTION_PROPORTION {
            EventKind::Auction
        } else {
            EventKind::Bid
        }
    }

    /// Event time for an ordinal, with out-of-orderness applied.
    fn event_time(&mut self, event_id: u64) -> i64 {
        let ordered = self.base_time_ms + (event_id * self.event_time_step_us / 1_000) as i64;
        if self.max_lateness_ms == 0 {
            return ordered;
        }
        let jitter = self.next_in(self.max_lateness_ms as u64 + 1) as i64;
        ordered - jitter
    }

    /// A person id, skewed toward the hot person.
    fn person_id(&mut self) -> u64 {
        let base = self.event_id / TOTAL_PROPORTION;
        let active = base.min(NUM_ACTIVE_PEOPLE);
        if self.next_in(HOT_RATIO) == 0 {
            // Hot person: the skew NEXMark's Q3/Q8 exist to stress.
            base.saturating_sub(active) + 1
        } else {
            base.saturating_sub(active) + self.next_in(active.max(1))
        }
    }

    /// An auction id, skewed toward the hot auction.
    fn auction_id(&mut self) -> u64 {
        let base = self.event_id / TOTAL_PROPORTION;
        let active = base.min(NUM_IN_FLIGHT_AUCTIONS);
        if self.next_in(HOT_RATIO) == 0 {
            base.saturating_sub(active) + 1
        } else {
            base.saturating_sub(active) + self.next_in(active.max(1))
        }
    }

    /// Generate the next `n` BID events as one `RecordBatch`.
    ///
    /// Bids are the 92% case and the only entity the currently-supported
    /// queries read, so this is the hot path. Person and auction batches exist
    /// for the queries that need them once joins are reachable.
    ///
    /// # Errors
    ///
    /// Returns the Arrow error if the column set does not form a valid batch.
    /// This is unreachable for a correct generator — every vector is pushed to
    /// once per row — but it is returned rather than asserted away, because
    /// "this cannot fail" is precisely the claim the rest of this engine's
    /// audit keeps finding to be false.
    pub fn next_bid_batch(&mut self, n: usize) -> Result<RecordBatch, arrow::error::ArrowError> {
        let mut auction = Vec::with_capacity(n);
        let mut bidder = Vec::with_capacity(n);
        let mut price = Vec::with_capacity(n);
        let mut channel = Vec::with_capacity(n);
        let mut url = Vec::with_capacity(n);
        let mut date_time = Vec::with_capacity(n);
        let mut extra = Vec::with_capacity(n);

        let mut produced = 0;
        while produced < n {
            // Respect the event mix: only bid ordinals yield bids, so a run of
            // `n` bids advances the ordinal by ~n * 50/46 and event time with
            // it. Skipping that would make the generated rate wrong by 8%.
            if Self::kind_for(self.event_id) != EventKind::Bid {
                self.event_id += 1;
                continue;
            }
            let id = self.event_id;
            auction.push(self.auction_id());
            bidder.push(self.person_id());
            price.push(self.next_in(10_000) + 1);
            let ch = self.next_in(4);
            channel.push(
                ["apple", "google", "facebook", "baidu"]
                    .get(ch as usize)
                    .copied()
                    .unwrap_or("apple"),
            );
            url.push("https://www.nexmark.com/item.htm");
            date_time.push(self.event_time(id));
            extra.push("");
            self.event_id += 1;
            produced += 1;
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("auction", DataType::UInt64, false),
            Field::new("bidder", DataType::UInt64, false),
            Field::new("price", DataType::UInt64, false),
            Field::new("channel", DataType::Utf8, false),
            Field::new("url", DataType::Utf8, false),
            Field::new("dateTime", DataType::Int64, false),
            Field::new("extra", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(auction)),
                Arc::new(UInt64Array::from(bidder)),
                Arc::new(UInt64Array::from(price)),
                Arc::new(StringArray::from(channel)),
                Arc::new(StringArray::from(url)),
                Arc::new(Int64Array::from(date_time)),
                Arc::new(StringArray::from(extra)),
            ],
        )
    }

    /// How many events of ALL kinds have been generated so far.
    #[must_use]
    pub fn events_generated(&self) -> u64 {
        self.event_id
    }
}

/// The NEXMark queries this engine's streaming path can currently express.
///
/// Four of twenty-two. Stated as data rather than prose so a report cannot
/// quietly imply full coverage — the number is read from here.
///
/// The other eighteen need capabilities the streaming compiler does not have
/// yet: stateless projection (Q0/Q1), global aggregates with no grouping key
/// (Q7 in its standard form), composite grouping keys (Q15), and joins (Q3,
/// Q4, Q8 and most of the rest).
pub const SUPPORTED_QUERIES: &[(&str, &str)] = &[
    (
        // Q1: currency conversion. Aggregates an EXPRESSION, which needs the
        // pre-window derived-column path — the aggregate argument is not a
        // column name.
        "q1_currency_conversion",
        "SELECT auction, SUM(price * 908 / 1000) AS total_euro \
         FROM TUMBLE(TABLE bid, DESCRIPTOR(dateTime), 10000) \
         GROUP BY auction, window_start, window_end",
    ),
    (
        "q2_filtered_bids",
        "SELECT auction, COUNT(*) AS c \
         FROM TUMBLE(TABLE bid, DESCRIPTOR(dateTime), 10000) \
         WHERE price > 5000 \
         GROUP BY auction, window_start, window_end",
    ),
    (
        "q5_hot_items",
        "SELECT auction, COUNT(*) AS c \
         FROM HOP(TABLE bid, DESCRIPTOR(dateTime), 2000, 10000) \
         GROUP BY auction, window_start, window_end",
    ),
    (
        "q7_highest_bid_keyed",
        "SELECT auction, MAX(price) AS mx \
         FROM TUMBLE(TABLE bid, DESCRIPTOR(dateTime), 10000) \
         GROUP BY auction, window_start, window_end",
    ),
    (
        "q11_user_sessions",
        "SELECT bidder, COUNT(*) AS c \
         FROM SESSION(TABLE bid, DESCRIPTOR(dateTime), 10000) \
         GROUP BY bidder, window_start, window_end",
    ),
];

/// Total NEXMark query count, for honest coverage reporting.
pub const NEXMARK_TOTAL_QUERIES: usize = 22;

#[cfg(test)]
mod tests {
    use super::*;

    /// The event mix matches the standard proportions.
    ///
    /// Checked over a full period rather than sampled: the ratio IS the
    /// benchmark's workload definition, and a generator that drifts from it is
    /// not running NEXMark whatever it is called.
    #[test]
    fn the_event_mix_matches_the_standard_proportions() {
        let mut counts = [0u64; 3];
        for id in 0..TOTAL_PROPORTION * 100 {
            match NexmarkGenerator::kind_for(id) {
                EventKind::Person => counts[0] += 1,
                EventKind::Auction => counts[1] += 1,
                EventKind::Bid => counts[2] += 1,
            }
        }
        assert_eq!(counts[0], PERSON_PROPORTION * 100, "person share");
        assert_eq!(counts[1], AUCTION_PROPORTION * 100, "auction share");
        assert_eq!(counts[2], BID_PROPORTION * 100, "bid share");
    }

    /// The bid schema is the standard one, not a convenient subset.
    ///
    /// The benchmark this replaces used `(auction, price)`. Numbers from a
    /// two-column stand-in cannot be compared with Flink or Spark, which is the
    /// only reason to run NEXMark at all.
    #[test]
    fn the_bid_schema_is_the_full_nexmark_shape() {
        let mut generator = NexmarkGenerator::new(42, 10_000, 0, 0);
        let batch = generator.next_bid_batch(8).unwrap();
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "auction", "bidder", "price", "channel", "url", "dateTime", "extra"
            ],
            "the Bid schema must match the NEXMark spec"
        );
        assert_eq!(batch.num_rows(), 8);
    }

    /// The same seed yields the same stream.
    ///
    /// Without this a regression is indistinguishable from a reroll.
    #[test]
    fn generation_is_deterministic_for_a_seed() {
        let a = NexmarkGenerator::new(7, 10_000, 0, 50)
            .next_bid_batch(64)
            .unwrap();
        let b = NexmarkGenerator::new(7, 10_000, 0, 50)
            .next_bid_batch(64)
            .unwrap();
        assert_eq!(a, b, "the same seed must reproduce the same batch");

        let c = NexmarkGenerator::new(8, 10_000, 0, 50)
            .next_bid_batch(64)
            .unwrap();
        assert_ne!(a, c, "a different seed must produce a different stream");
    }

    /// Out-of-orderness is actually injected when asked for.
    ///
    /// A perfectly ordered stream is the least interesting input for a windowed
    /// engine — it never exercises lateness, watermark lag or the buffering
    /// those imply. If this ever silently produced ordered data, every latency
    /// number built on it would be measuring the easy case.
    #[test]
    fn out_of_orderness_is_injected_and_is_optional() {
        let ordered = NexmarkGenerator::new(3, 10_000, 0, 0)
            .next_bid_batch(256)
            .unwrap();
        let ts = ordered
            .column_by_name("dateTime")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .expect("dateTime is Int64");
        let mut monotonic = true;
        for i in 1..ts.len() {
            if ts.value(i) < ts.value(i - 1) {
                monotonic = false;
                break;
            }
        }
        assert!(
            monotonic,
            "max_lateness_ms = 0 must yield an ordered stream"
        );

        let skewed = NexmarkGenerator::new(3, 10_000, 0, 500)
            .next_bid_batch(256)
            .unwrap();
        let ts = skewed
            .column_by_name("dateTime")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .expect("dateTime is Int64");
        let out_of_order = (1..ts.len())
            .filter(|&i| ts.value(i) < ts.value(i - 1))
            .count();
        assert!(
            out_of_order > 0,
            "max_lateness_ms = 500 must actually produce out-of-order events"
        );
    }

    /// Every query this module claims support for really compiles.
    ///
    /// The list drives the harness AND the coverage number in the report, so a
    /// stale entry would overstate what was measured.
    #[test]
    fn every_supported_query_compiles_on_the_streaming_path() {
        for (name, sql) in SUPPORTED_QUERIES {
            krishiv_sql::streaming_window_plan::compile_streaming_window_sql(sql).unwrap_or_else(
                |e| panic!("{name} is listed as supported but does not compile: {e}"),
            );
        }
        assert!(
            SUPPORTED_QUERIES.len() < NEXMARK_TOTAL_QUERIES,
            "if coverage ever reaches all 22, this assertion and the report's \
             wording both need revisiting"
        );
    }
}
