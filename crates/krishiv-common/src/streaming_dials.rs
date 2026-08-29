//! The streaming loop's runtime dials, in one place.
//!
//! # Why this module exists
//!
//! `KRISHIV_IDLE_TICK_MS` and `KRISHIV_STREAM_PROFILE` were each read and
//! interpreted independently by the embedded loop (`krishiv-engines`) and the
//! distributed run-loop (`krishiv-executor`), with their own copies of the
//! default and their own `"throughput"` comparison. The two agreed — but
//! nothing made them agree, and that is precisely the shape that produced the
//! `task_engine_parallelism` bug, where one site read `KRISHIV_TASK_SLOTS` and
//! the other read the real slot count, so `--slots 1` on a four-core executor
//! silently used a quarter of the CPU for months.
//!
//! A dial that means one thing embedded and another thing distributed is worse
//! than a dial that does nothing, because it is invisible: both engines run,
//! both look configured, and only a side-by-side benchmark shows the
//! difference. These are the same knob, so there is one implementation.
//!
//! The parsing is exposed as pure functions over `Option<&str>` so it can be
//! tested without `std::env::set_var`, which is `unsafe` under edition 2024 and
//! forbidden by the workspace lint.

use std::time::Duration;

/// Environment variable naming the continuous-loop idle tick.
pub const IDLE_TICK_MS_ENV: &str = "KRISHIV_IDLE_TICK_MS";

/// Environment variable naming the streaming profile.
pub const STREAM_PROFILE_ENV: &str = "KRISHIV_STREAM_PROFILE";

/// Environment variable overriding the profile's batch/linger choice.
pub const STREAM_LINGER_MS_ENV: &str = "KRISHIV_STREAM_LINGER_MS";

/// How often the loop advances the watermark while its sources are idle, so a
/// session window whose inactivity gap has elapsed can still close.
pub const DEFAULT_IDLE_TICK_MS: u64 = 500;

/// Batch/linger under the throughput profile: accumulate for this long before
/// draining, trading latency for amortised per-drain overhead.
pub const THROUGHPUT_LINGER_MS: u64 = 5;

/// Environment variable overriding the run-loop egress buffer cap.
pub const RLOOP_EGRESS_CAP_ENV: &str = "KRISHIV_RLOOP_EGRESS_CAP";

/// Run-loop egress buffer cap, in batches.
///
/// The buffer drops its OLDEST batch on overflow, so this is the amount of
/// computed output a slow drain consumer may lose before it catches up — a
/// durability dial in everything but name. It was a hard-coded `const` with no
/// override while every neighbouring streaming dial had one, which meant the
/// only way to trade memory for less loss was to recompile.
pub const DEFAULT_RLOOP_EGRESS_CAP: usize = 512;

/// Parse an egress cap, falling back to [`DEFAULT_RLOOP_EGRESS_CAP`].
///
/// Zero is rejected along with unparseable values: a cap of 0 drops every batch
/// the moment it is staged, which is not a configuration anyone means, and
/// would silently turn the job into a no-op.
pub fn parse_rloop_egress_cap(raw: Option<&str>) -> usize {
    raw.and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_RLOOP_EGRESS_CAP)
}

/// The configured run-loop egress buffer cap.
pub fn rloop_egress_cap() -> usize {
    parse_rloop_egress_cap(std::env::var(RLOOP_EGRESS_CAP_ENV).ok().as_deref())
}

/// Env var naming how long a run-loop stalls when its egress ring is full and
/// the ring is the job's ONLY delivery path (no durable sink).
pub const RLOOP_EGRESS_BACKPRESSURE_MS_ENV: &str = "KRISHIV_RLOOP_EGRESS_BACKPRESSURE_MS";

/// Default egress backpressure budget, in milliseconds.
///
/// ADR §73: when the ring is the sole way out, dropping computed output is
/// silent data loss, so the loop waits for a consumer instead. The wait is
/// BOUNDED because an unbounded one wedges a job nobody ever drains — after
/// the budget the job faults, which is visible and recoverable where a silent
/// hole in a result set is neither. 30s is long enough to ride out a slow or
/// briefly-disconnected consumer and short enough that an abandoned job is
/// reported inside a scrape interval.
pub const DEFAULT_RLOOP_EGRESS_BACKPRESSURE_MS: u64 = 30_000;

/// Parse the egress backpressure budget. `0` is meaningful and preserved: it
/// means "never wait, fault immediately", which is how an operator asks for
/// fail-fast. Only garbage falls back to the default.
pub fn parse_rloop_egress_backpressure_ms(raw: Option<&str>) -> u64 {
    raw.and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_RLOOP_EGRESS_BACKPRESSURE_MS)
}

/// The configured egress backpressure budget.
pub fn rloop_egress_backpressure_ms() -> u64 {
    parse_rloop_egress_backpressure_ms(
        std::env::var(RLOOP_EGRESS_BACKPRESSURE_MS_ENV)
            .ok()
            .as_deref(),
    )
}

/// Env var naming the per-buffer continuous INPUT cap (pending pushed
/// batches per `{job}#{task}` key before pushes are refused with
/// backpressure).
pub const RLOOP_INPUT_BUFFER_CAP_ENV: &str = "KRISHIV_RLOOP_INPUT_BUFFER_CAP";

/// Default continuous input buffer cap, in batches (task #149 fix 11 — this
/// used to be a hardcoded constant, so sustained throughput was pinned at
/// drain-rate × 64 with no operator dial).
pub const DEFAULT_RLOOP_INPUT_BUFFER_CAP: usize = 64;

/// Parse the input buffer cap. Zero and unparseable values fall back to the
/// default: a cap of 0 refuses every push and turns the job into a no-op
/// nobody meant to configure.
pub fn parse_rloop_input_buffer_cap(raw: Option<&str>) -> usize {
    raw.and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_RLOOP_INPUT_BUFFER_CAP)
}

/// The configured continuous input buffer cap.
pub fn rloop_input_buffer_cap() -> usize {
    parse_rloop_input_buffer_cap(std::env::var(RLOOP_INPUT_BUFFER_CAP_ENV).ok().as_deref())
}

/// Parse an idle-tick interval, falling back to [`DEFAULT_IDLE_TICK_MS`].
///
/// Unset, blank, or unparseable all mean "use the default" rather than "0",
/// because a zero tick is a busy loop.
pub fn parse_idle_tick_ms(raw: Option<&str>) -> u64 {
    raw.and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_IDLE_TICK_MS)
}

/// The configured idle-tick interval.
pub fn idle_tick_interval() -> Duration {
    Duration::from_millis(parse_idle_tick_ms(
        std::env::var(IDLE_TICK_MS_ENV).ok().as_deref(),
    ))
}

/// Streaming loop profile.
///
/// * `LowLatency` — emit as soon as there is anything to emit, checkpoint
///   often. Lowest end-to-end latency, most per-drain overhead.
/// * `Throughput` — micro-batch before draining and checkpoint less often,
///   trading a longer latency tail and a larger recovery-replay bound for
///   sustained rows/sec.
///
/// # No loop applies both halves, and the doc above used to imply otherwise
///
/// Those two sentences describe an intent, not any single placement's
/// behaviour. Measured:
///
/// | half | reached via | applied by |
/// |---|---|---|
/// | micro-batch linger | [`stream_linger`] → [`StreamProfile::linger`] | the run-loop only |
/// | checkpoint cadence | `krishiv-engines::checkpoint_every` | the embedded continuous loop only |
///
/// So `KRISHIV_STREAM_PROFILE=throughput` gives a run-loop job a longer linger
/// and its ORIGINAL checkpoint cadence (barrier checkpoints are driven by the
/// coordinator's interval, not by this profile), and gives an embedded job
/// fewer checkpoints and NO linger. Setting it does something different
/// depending on where the job landed.
///
/// That is not necessarily wrong — the two loops have genuinely different
/// checkpoint machinery, and a barrier cadence is the coordinator's to set —
/// but it was undocumented, which made the profile read as one dial with one
/// meaning. Pinned by `each_profile_half_is_reached_by_exactly_one_placement`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamProfile {
    LowLatency,
    Throughput,
}

impl StreamProfile {
    /// Resolve from [`STREAM_PROFILE_ENV`].
    pub fn from_env() -> Self {
        Self::parse(std::env::var(STREAM_PROFILE_ENV).ok().as_deref())
    }

    /// Pure parse: `throughput` (any case, surrounding whitespace ignored)
    /// selects throughput; anything else, including unset, is low-latency.
    ///
    /// Defaulting an unrecognised value to low-latency rather than rejecting it
    /// is deliberate: this is read on a hot startup path in both engines, and a
    /// typo must not stop a job from running.
    pub fn parse(raw: Option<&str>) -> Self {
        match raw {
            Some(s) if s.trim().eq_ignore_ascii_case("throughput") => Self::Throughput,
            _ => Self::LowLatency,
        }
    }

    /// This profile's batch/linger before each drain.
    pub fn linger(self) -> Duration {
        match self {
            Self::LowLatency => Duration::ZERO,
            Self::Throughput => Duration::from_millis(THROUGHPUT_LINGER_MS),
        }
    }
}

/// The effective batch/linger: [`STREAM_LINGER_MS_ENV`] when set and parseable,
/// otherwise the profile's own choice.
pub fn stream_linger() -> Duration {
    if let Some(ms) = std::env::var(STREAM_LINGER_MS_ENV)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
    {
        return Duration::from_millis(ms);
    }
    StreamProfile::from_env().linger()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Walk `crates/` and collect files containing `needle`, skipping this file.
    fn crates_containing(needle: &str) -> Vec<String> {
        fn walk(
            root: &std::path::Path,
            dir: &std::path::Path,
            needle: &str,
            out: &mut Vec<String>,
        ) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if path.file_name().is_some_and(|n| n == "target") {
                        continue;
                    }
                    walk(root, &path, needle, out);
                } else if path.extension().is_some_and(|e| e == "rs" || e == "inc")
                    && path.file_name().is_some_and(|n| n != "streaming_dials.rs")
                    && std::fs::read_to_string(&path).is_ok_and(|text| text.contains(needle))
                {
                    // Record the crate name, which is what the claim is about.
                    let name = path
                        .strip_prefix(root)
                        .ok()
                        .and_then(|r| r.components().next().map(|c| c.as_os_str().to_owned()))
                        .and_then(|c| c.into_string().ok());
                    if let Some(name) = name
                        && !out.contains(&name)
                    {
                        out.push(name);
                    }
                }
            }
        }
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crates dir")
            .to_path_buf();
        let mut out = Vec::new();
        walk(&root, &root, needle, &mut out);
        out.sort();
        out
    }

    /// `KRISHIV_STREAM_PROFILE` does two different things in two places, and the
    /// doc on [`StreamProfile`] now says so.
    ///
    /// This pins the measurement behind that doc. Source-scanning is brittle by
    /// nature — and here that is the feature: wiring linger into the embedded
    /// loop, or profile-driven checkpoint cadence into the run-loop, would make
    /// the documented table wrong, and this is what forces it to be revisited
    /// rather than silently drifting.
    ///
    /// Precedent: `env_registry` already scans the workspace this way.
    #[test]
    fn each_profile_half_is_reached_by_exactly_one_placement() {
        let linger = crates_containing("stream_linger()");
        let cadence = crates_containing("checkpoint_every(");

        assert!(
            linger.contains(&String::from("krishiv-executor")),
            "the micro-batch linger half is applied by the run-loop; found in {linger:?}"
        );
        assert!(
            !linger.contains(&String::from("krishiv-engines")),
            "krishiv-engines does NOT apply the linger half — if that changed, the table \
             on StreamProfile is now wrong and must be updated; found in {linger:?}"
        );

        assert!(
            cadence.contains(&String::from("krishiv-engines")),
            "the checkpoint-cadence half is applied by the embedded continuous loop; \
             found in {cadence:?}"
        );
        assert!(
            !cadence.contains(&String::from("krishiv-executor")),
            "the run-loop's checkpoints are barrier-driven by the coordinator, not by this \
             profile — if that changed, the table on StreamProfile is now wrong; found in \
             {cadence:?}"
        );
    }

    #[test]
    fn an_unset_or_unparseable_idle_tick_uses_the_default() {
        assert_eq!(parse_idle_tick_ms(None), DEFAULT_IDLE_TICK_MS);
        assert_eq!(parse_idle_tick_ms(Some("")), DEFAULT_IDLE_TICK_MS);
        assert_eq!(parse_idle_tick_ms(Some("   ")), DEFAULT_IDLE_TICK_MS);
        assert_eq!(parse_idle_tick_ms(Some("nonsense")), DEFAULT_IDLE_TICK_MS);
        assert_eq!(parse_idle_tick_ms(Some("-1")), DEFAULT_IDLE_TICK_MS);
    }

    #[test]
    fn an_explicit_idle_tick_is_honoured_including_zero() {
        assert_eq!(parse_idle_tick_ms(Some("50")), 50);
        assert_eq!(parse_idle_tick_ms(Some(" 1200 ")), 1200);
        // An explicit 0 is the operator asking for it; only *absence* defaults.
        assert_eq!(parse_idle_tick_ms(Some("0")), 0);
    }

    #[test]
    fn the_profile_recognises_throughput_in_any_case() {
        assert_eq!(
            StreamProfile::parse(Some("throughput")),
            StreamProfile::Throughput
        );
        assert_eq!(
            StreamProfile::parse(Some("  ThroughPut ")),
            StreamProfile::Throughput
        );
        assert_eq!(
            StreamProfile::parse(Some("THROUGHPUT")),
            StreamProfile::Throughput
        );
    }

    #[test]
    fn anything_else_is_low_latency() {
        for raw in [None, Some(""), Some("low-latency"), Some("typo")] {
            assert_eq!(
                StreamProfile::parse(raw),
                StreamProfile::LowLatency,
                "{raw:?}"
            );
        }
    }

    /// The two engines must agree on what each profile *does*, not just on how
    /// it parses — that is the half the duplicated copies could have drifted on
    /// without any test noticing.
    #[test]
    fn low_latency_emits_immediately_and_throughput_micro_batches() {
        assert_eq!(StreamProfile::LowLatency.linger(), Duration::ZERO);
        assert_eq!(
            StreamProfile::Throughput.linger(),
            Duration::from_millis(THROUGHPUT_LINGER_MS)
        );
        assert!(StreamProfile::Throughput.linger() > StreamProfile::LowLatency.linger());
    }

    /// The egress cap is a durability dial — it is how much computed output a
    /// slow drain consumer may lose before catching up — and it was the one
    /// streaming dial with no override, so trading memory for less loss meant
    /// recompiling.
    ///
    /// Zero is rejected along with garbage: a cap of 0 drops every batch the
    /// instant it is staged, turning the job into a silent no-op. That is never
    /// what a `0` in an env var means, so it must not be honoured as a value.
    #[test]
    fn the_egress_cap_is_overridable_but_never_zero() {
        assert_eq!(parse_rloop_egress_cap(Some("4096")), 4096);
        assert_eq!(parse_rloop_egress_cap(Some("  64  ")), 64);

        for refused in [None, Some(""), Some("0"), Some("-1"), Some("lots")] {
            assert_eq!(
                parse_rloop_egress_cap(refused),
                DEFAULT_RLOOP_EGRESS_CAP,
                "{refused:?} must fall back to the default, not disable buffering"
            );
        }
    }
}

/// Accounting for output a run-loop egress buffer dropped.
///
/// One place owning the counter, the operator-facing warning, and the question
/// of whether a drop actually lost anything. Before this, the count lived in a
/// map on the runner, the warning text next to it, and the "is this job's output
/// incomplete?" judgement three crates away in the coordinator — so the three
/// could disagree about what a nonzero count meant, and did.
///
/// The buffers themselves stay per-loop and are NOT unified: the embedded loop
/// awaits sink writes, the seam writes inline, the coordinator holds an inline
/// result store, and the run-loop keeps this capped staging buffer. Those are
/// four genuinely different mechanisms. Only the accounting is shared.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EgressLoss {
    dropped_batches: u64,
}

impl EgressLoss {
    /// No loss recorded.
    #[must_use]
    pub const fn none() -> Self {
        Self { dropped_batches: 0 }
    }

    /// Record `batches` dropped from an egress buffer.
    pub const fn record(&mut self, batches: u64) {
        self.dropped_batches = self.dropped_batches.saturating_add(batches);
    }

    /// How many batches have been dropped.
    #[must_use]
    pub const fn dropped_batches(&self) -> u64 {
        self.dropped_batches
    }

    /// Did anything get dropped?
    #[must_use]
    pub const fn is_lossless(&self) -> bool {
        self.dropped_batches == 0
    }

    /// The operator-facing explanation of a drop that genuinely lost output.
    ///
    /// Deliberately a function rather than a `Display` impl: this text is only
    /// correct when the dropped batches were the job's ONLY delivery path. A
    /// job whose real output is a durable sink loses nothing when this buffer
    /// overflows, and telling its operator otherwise is a false alarm — see
    /// [`EgressLoss::drop_lost_output`].
    #[must_use]
    pub const fn lost_output_advice() -> &'static str {
        "run-loop egress buffer overflowed; oldest batches dropped (drain is best-effort \
         — consume durably via the sink or queryable state, or raise \
         KRISHIV_RLOOP_EGRESS_CAP)"
    }

    /// Did dropping from the egress buffer actually lose output?
    ///
    /// Only when the buffer is the job's sole delivery path. A run-loop job
    /// writing to a durable Iceberg or Kafka sink fills this buffer too — it is
    /// populated unconditionally, before the sink dispatch — so its overflow
    /// counter climbs forever while its output is in fact complete. Reporting
    /// that as lost output is a false alarm sitting on top of a real defect,
    /// which is the worst place for one: it trains the reader to ignore the
    /// signal that matters.
    #[must_use]
    pub const fn drop_lost_output(has_durable_sink: bool) -> bool {
        !has_durable_sink
    }
}

#[cfg(test)]
mod egress_backpressure_tests {
    use super::*;

    /// ADR §73. `0` is a MEANINGFUL budget — "never wait, fault immediately",
    /// how an operator asks for fail-fast — so it must survive parsing. The
    /// egress CAP dial filters `0` out because a zero cap is a job-killing
    /// no-op nobody means; copying that filter here would silently convert an
    /// explicit fail-fast request into a 30-second stall.
    #[test]
    fn a_zero_backpressure_budget_means_fail_fast_not_the_default() {
        assert_eq!(parse_rloop_egress_backpressure_ms(Some("0")), 0);
        assert_eq!(parse_rloop_egress_backpressure_ms(Some("500")), 500);
    }

    /// Garbage and absence fall back; an unparseable dial must not become 0
    /// and turn every slow consumer into an instant job failure.
    #[test]
    fn garbage_falls_back_to_the_default_rather_than_fail_fast() {
        for raw in [None, Some(""), Some("  "), Some("soon"), Some("-1")] {
            assert_eq!(
                parse_rloop_egress_backpressure_ms(raw),
                DEFAULT_RLOOP_EGRESS_BACKPRESSURE_MS,
                "raw={raw:?} must fall back, not fail fast"
            );
        }
    }
}

#[cfg(test)]
mod input_buffer_cap_tests {
    use super::*;

    /// The input cap is a dial, not a constant (task #149 fix 11): set it,
    /// get it; garbage and 0 (a job-killing no-op nobody means) fall back.
    #[test]
    fn input_buffer_cap_parses_and_falls_back() {
        assert_eq!(parse_rloop_input_buffer_cap(Some("128")), 128);
        assert_eq!(
            parse_rloop_input_buffer_cap(None),
            DEFAULT_RLOOP_INPUT_BUFFER_CAP
        );
        assert_eq!(
            parse_rloop_input_buffer_cap(Some("0")),
            DEFAULT_RLOOP_INPUT_BUFFER_CAP
        );
        assert_eq!(
            parse_rloop_input_buffer_cap(Some("nope")),
            DEFAULT_RLOOP_INPUT_BUFFER_CAP
        );
    }
}
