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
