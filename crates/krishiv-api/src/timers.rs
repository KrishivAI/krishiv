//! High-level timer-service API for streaming operators.
//!
//! Re-exports [`krishiv_state::timer`] and adds convenience helpers
//! that match the shape of the dataflow process function so user code
//! can register timers with a single call.

pub use krishiv_dataflow::process_fn::{TimerEntry, TimerKind};
pub use krishiv_state::timer::{InMemoryTimerService, TimerKey, TimerService};

/// Build a new in-memory timer service for a streaming operator.
///
/// Convenience wrapper so callers don't need to import the type
/// directly. The returned service is safe to share across tasks via
/// `Arc`; the [`TimerService`] trait methods take `&mut self` so
/// callers wrap it in a `Mutex` (or use the dataflow executor that
/// already serializes access).
pub fn build_in_memory_timer_service() -> InMemoryTimerService {
    InMemoryTimerService::new()
}

/// Schedule an event-time timer that fires when the watermark
/// reaches `fire_at_ms`.
///
/// Returns a typed [`TimerKey`] that the caller can later pass to
/// [`TimerService::cancel_timer`] to cancel.
pub fn schedule_event_time_timer(
    service_namespace: &str,
    state_name: &str,
    key: &[u8],
    fire_at_ms: i64,
) -> TimerKey {
    TimerKey::new(
        krishiv_state::namespace::Namespace::new(service_namespace, state_name),
        key.to_vec(),
        fire_at_ms,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_event_time_timer_builds_key() {
        let k = schedule_event_time_timer("svc", "state", b"k1", 100);
        assert_eq!(k.deadline_ms, 100);
        assert_eq!(k.key, b"k1");
    }

    #[test]
    fn in_memory_service_drains_fired_timers() {
        let mut svc = build_in_memory_timer_service();
        let _ = svc.register_event_time_timer(schedule_event_time_timer("svc", "s", b"a", 50));
        let _ = svc.register_event_time_timer(schedule_event_time_timer("svc", "s", b"b", 100));
        let fired_at_75 = svc.drain_fired_timers(75);
        assert_eq!(fired_at_75.len(), 1);
        assert_eq!(fired_at_75[0].key, b"a");
    }
}
