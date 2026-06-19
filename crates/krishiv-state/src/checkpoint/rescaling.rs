//! Key-group rescaling for checkpoint restore (R16 S4.2).

use crate::error::{StateError, StateResult};
use crate::key_group::{KeyGroupRange, key_group_for_key, key_group_ranges_for_parallelism};
use crate::snapshot::{SnapshotEntry, decode_snapshot_entries, encode_snapshot_entries};

// ── RescaleChecksum ──────────────────────────────────────────────────────────

/// Lightweight integrity checkpoint for a partition split or merge operation.
///
/// Inspired by the Netflix Planner/Splitter pattern: the coordinator computes
/// a `RescaleChecksum` from the pre-split data and stores it. After the split,
/// the executor computes the post-split checksum and the two must match before
/// the operation is marked complete. This makes split operations idempotent and
/// verifiable without shipping full record batches back to the coordinator.
///
/// The checksum is deliberately simple (row count + column count) so it can be
/// computed in O(1) from metadata without reading batch payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RescaleChecksum {
    /// Total row count across all input batches / shards.
    pub total_rows: u64,
    /// Number of columns (schema width). Used to catch schema divergence.
    pub column_count: u32,
    /// Parallelism before the rescale.
    pub old_parallelism: u32,
    /// Parallelism after the rescale.
    pub new_parallelism: u32,
}

impl RescaleChecksum {
    /// Create a checksum from aggregate statistics collected by the planner
    /// before the split begins.
    pub fn new(
        total_rows: u64,
        column_count: u32,
        old_parallelism: u32,
        new_parallelism: u32,
    ) -> Self {
        Self {
            total_rows,
            column_count,
            old_parallelism,
            new_parallelism,
        }
    }

    /// Verify that the post-split shards are consistent with this pre-split
    /// checksum.
    ///
    /// `post_shard_row_counts` must contain one entry per post-split shard.
    /// Returns `true` when the split is valid (no rows lost or duplicated and
    /// schema width is unchanged).
    pub fn verify(&self, post_shard_row_counts: &[u64], post_column_count: u32) -> bool {
        let post_total: u64 = post_shard_row_counts.iter().sum();
        post_total == self.total_rows
            && post_column_count == self.column_count
            && post_shard_row_counts.len() == self.new_parallelism as usize
    }
}

/// Computes key-group → task slot mapping when restoring with new parallelism.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyGroupRescaler {
    /// Parallelism of the checkpoint being restored (stored for diagnostic
    /// logging; not used in the key-group → task mapping arithmetic).
    pub old_parallelism: u32,
    pub new_parallelism: u32,
    pub new_ranges: Vec<KeyGroupRange>,
}

impl KeyGroupRescaler {
    pub fn new(old_parallelism: u32, new_parallelism: u32) -> Self {
        Self {
            old_parallelism: old_parallelism.max(1),
            new_parallelism: new_parallelism.max(1),
            new_ranges: key_group_ranges_for_parallelism(new_parallelism.max(1)),
        }
    }

    /// Task slot index in the new deployment for `key_group`.
    ///
    /// Uses binary search over [`new_ranges`] to stay consistent with the
    /// potentially uneven range distribution produced by
    /// [`key_group_ranges_for_parallelism`].
    pub fn task_for_key_group(&self, key_group: u16) -> u32 {
        match self.new_ranges.partition_point(|r| r.end < key_group) as u32 {
            idx if (idx as usize) < self.new_ranges.len() => idx,
            _ => self.new_parallelism.saturating_sub(1),
        }
    }

    /// Key-group range assigned to `task_index` in the new deployment.
    /// Returns `None` if `task_index` is out of range.
    pub fn range_for_task(&self, task_index: u32) -> Option<KeyGroupRange> {
        self.new_ranges.get(task_index as usize).copied()
    }
}

// ── State-key routing for redistribution ────────────────────────────────────

/// Canonical window-operator state-key prefixes.
///
/// These mirror the on-disk layout written by the window operators in
/// `krishiv-dataflow` (`persist_window_accumulators` and the session-window
/// persistence path).  Each entry is `(prefix, trailing_bytes)` where
/// `trailing_bytes` is the fixed-width suffix after the embedded group key:
///
/// - `tw:` / `sw:`  → `prefix | key_len_le_u32 | key | win_start_le_i64` (8)
/// - `ses:`         → `prefix | key_len_le_u32 | key | session_start_le_i64 | last_event_le_i64` (16)
pub const WINDOW_STATE_KEY_LAYOUTS: &[(&[u8], usize)] = &[(b"tw:", 8), (b"sw:", 8), (b"ses:", 16)];

/// Operator-broadcast state keys that must be copied to every post-rescale
/// task rather than routed by group key (e.g. the persisted watermark).
pub const BROADCAST_STATE_KEYS: &[&[u8]] = &[b"wm:"];

/// How [`redistribute_snapshots`] derives the routing key for a state entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryRouting {
    /// Route by the full state-key bytes.  Correct for operators whose state
    /// key *is* the group key (generic keyed state).
    ByStateKey,
    /// Route window-operator state by the group key embedded in the state key
    /// (see [`WINDOW_STATE_KEY_LAYOUTS`]).  All windows of one group key land
    /// on the same post-rescale task, matching runtime data partitioning.
    /// Keys in [`BROADCAST_STATE_KEYS`] and keys that do not parse as window
    /// state keys are broadcast to every post-rescale task.
    WindowGroupKey,
}

/// Extract the embedded group key from a window-operator state key.
///
/// Returns `None` when `state_key` does not match any known window layout —
/// callers treat such entries as broadcast state.
pub fn window_group_key(state_key: &[u8]) -> Option<&[u8]> {
    for (prefix, trailing) in WINDOW_STATE_KEY_LAYOUTS {
        let plen = prefix.len();
        if state_key.len() < plen + 4 + trailing || &state_key[..plen] != *prefix {
            continue;
        }
        let rest = &state_key[plen..];
        let key_len = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
        // Exact structural match required: prefix + len + key + trailing.
        if rest.len() == 4 + key_len + trailing {
            return Some(&rest[4..4 + key_len]);
        }
    }
    None
}

fn routing_key(state_key: &[u8], routing: EntryRouting) -> Option<&[u8]> {
    match routing {
        EntryRouting::ByStateKey => Some(state_key),
        EntryRouting::WindowGroupKey => {
            if BROADCAST_STATE_KEYS.contains(&state_key) {
                return None;
            }
            window_group_key(state_key)
        }
    }
}

/// Redistribute per-task operator snapshots from `old_parallelism` (implied by
/// `old_snapshots.len()`) to `new_parallelism` tasks by key group.
///
/// Every entry from every old snapshot is decoded, hashed into one of the
/// `NUM_KEY_GROUPS` key groups via [`key_group_for_key`], and routed to the
/// new task that owns that key group under
/// [`key_group_ranges_for_parallelism`].  Broadcast entries (per
/// [`EntryRouting`]) are copied to every new task.  The result is one portable
/// snapshot per new task, loadable through `StateBackend::load_snapshot`.
///
/// The redistribution is verified with a [`RescaleChecksum`] before returning:
/// the per-task routed-entry counts must sum to the input routed-entry count.
/// A checksum failure indicates an internal invariant breach and returns
/// `StateError::SnapshotCorrupt` rather than silently dropping state.
pub fn redistribute_snapshots(
    old_snapshots: &[Vec<u8>],
    new_parallelism: u32,
    routing: EntryRouting,
) -> StateResult<Vec<Vec<u8>>> {
    let new_parallelism = new_parallelism.max(1);
    let rescaler = KeyGroupRescaler::new(old_snapshots.len().max(1) as u32, new_parallelism);

    let mut routed: Vec<Vec<SnapshotEntry>> = vec![Vec::new(); new_parallelism as usize];
    let mut broadcast: Vec<SnapshotEntry> = Vec::new();
    let mut routed_total: u64 = 0;

    for snapshot in old_snapshots {
        // Stateless tasks ack with empty snapshot bytes; nothing to move.
        if snapshot.is_empty() {
            continue;
        }
        for entry in decode_snapshot_entries(snapshot)? {
            match routing_key(&entry.2, routing) {
                Some(key) => {
                    let task = rescaler.task_for_key_group(key_group_for_key(key)) as usize;
                    routed[task].push(entry);
                    routed_total += 1;
                }
                None => broadcast.push(entry),
            }
        }
    }

    // Deduplicate broadcast entries across old tasks: every old task persisted
    // its own copy (e.g. per-task watermark).  Keep the one with the largest
    // value interpretation when the key repeats — for the watermark this is
    // the safe (most conservative for late-data suppression is the *minimum*)
    // choice.  Watermark semantics require the MINIMUM across tasks: a key
    // group moving from a slow task must not skip ahead of its old watermark.
    let mut deduped_broadcast: Vec<SnapshotEntry> = Vec::new();
    for entry in broadcast {
        match deduped_broadcast.iter_mut().find(|existing| {
            existing.0 == entry.0 && existing.1 == entry.1 && existing.2 == entry.2
        }) {
            None => deduped_broadcast.push(entry),
            Some(existing) => {
                // Watermarks are 8-byte LE i64 values: keep the minimum.
                if existing.3.len() == 8 && entry.3.len() == 8 {
                    let old_bytes: [u8; 8] =
                        existing.3[..8]
                            .try_into()
                            .map_err(|_| StateError::SnapshotCorrupt {
                                message: "watermark value is not 8 bytes".into(),
                            })?;
                    let new_bytes: [u8; 8] =
                        entry.3[..8]
                            .try_into()
                            .map_err(|_| StateError::SnapshotCorrupt {
                                message: "watermark value is not 8 bytes".into(),
                            })?;
                    let old = i64::from_le_bytes(old_bytes);
                    let new = i64::from_le_bytes(new_bytes);
                    if new < old {
                        existing.3 = entry.3;
                    }
                } else if entry.3 != existing.3 {
                    return Err(StateError::SnapshotCorrupt {
                        message: format!(
                            "broadcast state key {:?} has conflicting non-watermark values \
                             across old tasks; cannot redistribute safely",
                            String::from_utf8_lossy(&entry.2)
                        ),
                    });
                }
            }
        }
    }

    // Verify no routed entry was lost or duplicated.
    let checksum = RescaleChecksum::new(
        routed_total,
        0,
        old_snapshots.len().max(1) as u32,
        new_parallelism,
    );
    let per_task_counts: Vec<u64> = routed.iter().map(|v| v.len() as u64).collect();
    if !checksum.verify(&per_task_counts, 0) {
        return Err(StateError::SnapshotCorrupt {
            message: format!(
                "rescale verification failed: {routed_total} routed entries split into \
                 {per_task_counts:?} across {new_parallelism} tasks"
            ),
        });
    }

    Ok(routed
        .into_iter()
        .map(|mut entries| {
            entries.extend(deduped_broadcast.iter().cloned());
            if entries.is_empty() {
                // Stateless target task: empty bytes, matching the convention
                // used by stateless checkpoint acks.
                Vec::new()
            } else {
                encode_snapshot_entries(&entries)
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key_group::NUM_KEY_GROUPS;

    #[test]
    fn rescale_4_to_2_maps_all_key_groups() {
        let rescaler = KeyGroupRescaler::new(4, 2);
        assert_eq!(rescaler.new_ranges.len(), 2);
        for kg in 0..NUM_KEY_GROUPS {
            let task = rescaler.task_for_key_group(kg);
            assert!(task < 2);
            let range = rescaler.range_for_task(task).expect("task index in range");
            assert!(range.contains(kg));
        }
    }

    #[test]
    fn rescale_2_to_4_expands_ranges() {
        let rescaler = KeyGroupRescaler::new(2, 4);
        assert_eq!(rescaler.new_ranges.len(), 4);
    }

    /// C12 regression: range_for_task must return None for out-of-range indices
    /// (task_index >= new_parallelism) instead of panicking or returning garbage.
    #[test]
    fn range_for_task_out_of_range_returns_none() {
        let rescaler = KeyGroupRescaler::new(4, 2);
        assert!(rescaler.range_for_task(0).is_some());
        assert!(rescaler.range_for_task(1).is_some());
        assert!(
            rescaler.range_for_task(2).is_none(),
            "task_index >= parallelism must return None"
        );
        assert!(rescaler.range_for_task(999).is_none());
    }

    // ── new() clamping ──────────────────────────────────────────────────

    #[test]
    fn rescaler_new_clamps_zero_to_one() {
        let rescaler = KeyGroupRescaler::new(0, 0);
        assert_eq!(rescaler.old_parallelism, 1);
        assert_eq!(rescaler.new_parallelism, 1);
        assert_eq!(rescaler.new_ranges.len(), 1);
        let range = rescaler.range_for_task(0).unwrap();
        assert_eq!(range.start, 0);
        assert_eq!(range.end, NUM_KEY_GROUPS - 1);
    }

    #[test]
    fn rescaler_new_clamps_large_to_one() {
        let rescaler = KeyGroupRescaler::new(100, 1);
        assert_eq!(rescaler.old_parallelism, 100);
        assert_eq!(rescaler.new_parallelism, 1);
        assert_eq!(rescaler.new_ranges.len(), 1);
    }

    // ── task_for_key_group boundaries ───────────────────────────────────

    #[test]
    fn task_for_key_group_first_key_group() {
        let rescaler = KeyGroupRescaler::new(4, 4);
        let task = rescaler.task_for_key_group(0);
        assert_eq!(task, 0);
    }

    #[test]
    fn task_for_key_group_last_key_group() {
        let rescaler = KeyGroupRescaler::new(4, 4);
        let task = rescaler.task_for_key_group(NUM_KEY_GROUPS - 1);
        assert!(task < 4);
    }

    #[test]
    fn task_for_key_group_single_parallelism() {
        let rescaler = KeyGroupRescaler::new(4, 1);
        for kg in 0..NUM_KEY_GROUPS {
            assert_eq!(rescaler.task_for_key_group(kg), 0);
        }
    }

    // ── range_for_task coverage ─────────────────────────────────────────

    #[test]
    fn range_for_task_covers_full_range() {
        for p in [1u32, 2, 3, 4, 8, 16, 32] {
            let rescaler = KeyGroupRescaler::new(p, p);
            let mut covered = 0u32;
            for task in 0..p {
                let range = rescaler.range_for_task(task).unwrap();
                covered += (range.end - range.start + 1) as u32;
            }
            assert_eq!(covered, NUM_KEY_GROUPS as u32, "p={p}");
        }
    }

    #[test]
    fn range_for_task_no_gaps() {
        let rescaler = KeyGroupRescaler::new(4, 4);
        for i in 0..(rescaler.new_ranges.len() - 1) {
            let r1 = rescaler.range_for_task(i as u32).unwrap();
            let r2 = rescaler.range_for_task(i as u32 + 1).unwrap();
            assert_eq!(r1.end + 1, r2.start, "gap between task {i} and {}", i + 1);
        }
    }

    // ── Consistency: task → range → key_group mapping ───────────────────

    #[test]
    fn key_group_consistency_all_parallelisms() {
        for new_p in [1u32, 2, 3, 4, 5, 7, 8, 16, 32, 64, 128, 256] {
            let rescaler = KeyGroupRescaler::new(4, new_p);
            for kg in 0..NUM_KEY_GROUPS {
                let task = rescaler.task_for_key_group(kg);
                let range = rescaler
                    .range_for_task(task)
                    .unwrap_or_else(|| panic!("task={task} not in range for new_p={new_p}"));
                assert!(
                    range.contains(kg),
                    "kg={kg} task={task} range={range:?} new_p={new_p}"
                );
            }
        }
    }

    // ── Display / Debug ─────────────────────────────────────────────────

    #[test]
    fn rescaler_debug_output() {
        let rescaler = KeyGroupRescaler::new(4, 2);
        let debug = format!("{:?}", rescaler);
        assert!(debug.contains("old_parallelism"));
        assert!(debug.contains("new_parallelism"));
        assert!(debug.contains("new_ranges"));
    }

    // ── Old parallelism is stored but not used in arithmetic ────────────

    #[test]
    fn old_parallelism_is_stored() {
        let rescaler = KeyGroupRescaler::new(8, 4);
        assert_eq!(rescaler.old_parallelism, 8);
        // The mapping only depends on new_parallelism
        let rescaler2 = KeyGroupRescaler::new(16, 4);
        for kg in 0..NUM_KEY_GROUPS {
            assert_eq!(
                rescaler.task_for_key_group(kg),
                rescaler2.task_for_key_group(kg)
            );
        }
    }

    // ── Single key group to single task ─────────────────────────────────

    #[test]
    fn rescale_1_to_1_single_range() {
        let rescaler = KeyGroupRescaler::new(1, 1);
        assert_eq!(rescaler.new_ranges.len(), 1);
        let range = rescaler.range_for_task(0).unwrap();
        assert_eq!(range.start, 0);
        assert_eq!(range.end, NUM_KEY_GROUPS - 1);
        assert_eq!(rescaler.task_for_key_group(0), 0);
        assert_eq!(rescaler.task_for_key_group(NUM_KEY_GROUPS - 1), 0);
    }

    // ── KeyGroupRange::contains ─────────────────────────────────────────

    #[test]
    fn key_group_range_contains_boundaries() {
        let range = KeyGroupRange::new(10, 20);
        assert!(!range.contains(9));
        assert!(range.contains(10));
        assert!(range.contains(15));
        assert!(range.contains(20));
        assert!(!range.contains(21));
    }

    #[test]
    fn key_group_range_as_range() {
        let range = KeyGroupRange::new(5, 10);
        let r = range.as_range();
        assert_eq!(r, 5..=10);
    }

    #[test]
    fn key_group_range_equality() {
        let a = KeyGroupRange::new(0, 100);
        let b = KeyGroupRange::new(0, 100);
        let c = KeyGroupRange::new(0, 101);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    // ── Uneven division ─────────────────────────────────────────────────

    #[test]
    fn rescale_uneven_division_no_gaps() {
        let rescaler = KeyGroupRescaler::new(4, 3);
        assert_eq!(rescaler.new_ranges.len(), 3);
        // All key groups must map to a valid task with containing range
        for kg in 0..NUM_KEY_GROUPS {
            let task = rescaler.task_for_key_group(kg);
            let range = rescaler.range_for_task(task).unwrap();
            assert!(range.contains(kg), "kg={kg} task={task} range={range:?}");
        }
    }

    // ── RescaleChecksum ───────────────────────────────────────────────────────

    #[test]
    fn checksum_verifies_matching_split() {
        let pre = RescaleChecksum::new(1000, 5, 2, 4);
        // 4 shards totalling 1000 rows, 5 columns
        let counts = [250u64, 250, 250, 250];
        assert!(pre.verify(&counts, 5));
    }

    #[test]
    fn checksum_rejects_lost_rows() {
        let pre = RescaleChecksum::new(1000, 5, 2, 4);
        let counts = [200u64, 250, 250, 250]; // only 950 total
        assert!(!pre.verify(&counts, 5));
    }

    #[test]
    fn checksum_rejects_duplicated_rows() {
        let pre = RescaleChecksum::new(1000, 5, 2, 4);
        let counts = [300u64, 250, 250, 250]; // 1050 total
        assert!(!pre.verify(&counts, 5));
    }

    #[test]
    fn checksum_rejects_wrong_shard_count() {
        let pre = RescaleChecksum::new(1000, 5, 2, 4);
        let counts = [500u64, 500]; // only 2 shards, expected 4
        assert!(!pre.verify(&counts, 5));
    }

    #[test]
    fn checksum_rejects_schema_divergence() {
        let pre = RescaleChecksum::new(1000, 5, 2, 4);
        let counts = [250u64, 250, 250, 250];
        assert!(!pre.verify(&counts, 6)); // column count changed
    }

    #[test]
    fn checksum_zero_rows_splits_cleanly() {
        let pre = RescaleChecksum::new(0, 3, 1, 2);
        assert!(pre.verify(&[0, 0], 3));
        assert!(!pre.verify(&[1, 0], 3));
    }

    // ── redistribute_snapshots ───────────────────────────────────────────────

    fn window_state_key(prefix: &[u8], group_key: &str, win_start: i64) -> Vec<u8> {
        let kb = group_key.as_bytes();
        let mut key = Vec::new();
        key.extend_from_slice(prefix);
        key.extend_from_slice(&(kb.len() as u32).to_le_bytes());
        key.extend_from_slice(kb);
        key.extend_from_slice(&win_start.to_le_bytes());
        key
    }

    fn entry(op: &str, name: &str, key: Vec<u8>, value: &[u8]) -> SnapshotEntry {
        (op.to_owned(), name.to_owned(), key, value.to_vec())
    }

    #[test]
    fn window_group_key_extracts_embedded_key() {
        let key = window_state_key(b"tw:", "user-7", 10_000);
        assert_eq!(window_group_key(&key), Some("user-7".as_bytes()));

        let key = window_state_key(b"sw:", "k", 0);
        assert_eq!(window_group_key(&key), Some("k".as_bytes()));

        // Session keys carry two trailing i64s.
        let kb = "sess-key".as_bytes();
        let mut key = Vec::new();
        key.extend_from_slice(b"ses:");
        key.extend_from_slice(&(kb.len() as u32).to_le_bytes());
        key.extend_from_slice(kb);
        key.extend_from_slice(&500i64.to_le_bytes());
        key.extend_from_slice(&900i64.to_le_bytes());
        assert_eq!(window_group_key(&key), Some(kb));

        // Watermark and arbitrary keys do not parse.
        assert_eq!(window_group_key(b"wm:"), None);
        assert_eq!(window_group_key(b"arbitrary"), None);
    }

    #[test]
    fn redistribute_1_to_3_routes_every_key_exactly_once() {
        // Build one snapshot with 100 distinct group keys across two windows.
        let mut entries = Vec::new();
        for i in 0..100 {
            let group = format!("key-{i}");
            for win in [0i64, 10_000] {
                entries.push(entry(
                    "op",
                    "window",
                    window_state_key(b"tw:", &group, win),
                    format!("v-{i}-{win}").as_bytes(),
                ));
            }
        }
        let snapshot = encode_snapshot_entries(&entries);

        let out = redistribute_snapshots(&[snapshot], 3, EntryRouting::WindowGroupKey).unwrap();
        assert_eq!(out.len(), 3);

        let mut seen = std::collections::HashMap::new();
        let rescaler = KeyGroupRescaler::new(1, 3);
        for (task, snap) in out.iter().enumerate() {
            for (op, name, key, value) in decode_snapshot_entries(snap).unwrap() {
                assert_eq!(op, "op");
                assert_eq!(name, "window");
                let group = window_group_key(&key).expect("window key").to_vec();
                // Both windows of one group key must land on the same task,
                // and that task must own the group's key group.
                let expected = rescaler.task_for_key_group(key_group_for_key(&group)) as usize;
                assert_eq!(
                    task,
                    expected,
                    "group {:?}",
                    String::from_utf8_lossy(&group)
                );
                *seen.entry((key, value)).or_insert(0u32) += 1;
            }
        }
        assert_eq!(seen.len(), 200, "all entries present");
        assert!(seen.values().all(|&n| n == 1), "no entry duplicated");
    }

    #[test]
    fn redistribute_3_to_1_merges_all_entries() {
        let mut snaps = Vec::new();
        for task in 0..3 {
            let entries = vec![entry(
                "op",
                "window",
                window_state_key(b"tw:", &format!("key-{task}"), 0),
                b"v",
            )];
            snaps.push(encode_snapshot_entries(&entries));
        }
        let out = redistribute_snapshots(&snaps, 1, EntryRouting::WindowGroupKey).unwrap();
        assert_eq!(out.len(), 1);
        let merged = decode_snapshot_entries(&out[0]).unwrap();
        assert_eq!(merged.len(), 3);
    }

    #[test]
    fn redistribute_broadcasts_minimum_watermark_to_all_tasks() {
        // Two old tasks with different persisted watermarks: the minimum must
        // be broadcast so a key group from the slower task cannot skip ahead.
        let snap_a = encode_snapshot_entries(&[
            entry("op", "window", b"wm:".to_vec(), &5_000i64.to_le_bytes()),
            entry("op", "window", window_state_key(b"tw:", "a", 0), b"va"),
        ]);
        let snap_b = encode_snapshot_entries(&[
            entry("op", "window", b"wm:".to_vec(), &2_000i64.to_le_bytes()),
            entry("op", "window", window_state_key(b"tw:", "b", 0), b"vb"),
        ]);

        let out =
            redistribute_snapshots(&[snap_a, snap_b], 4, EntryRouting::WindowGroupKey).unwrap();
        assert_eq!(out.len(), 4);
        for snap in &out {
            let entries = decode_snapshot_entries(snap).unwrap();
            let wm = entries
                .iter()
                .find(|(_, _, k, _)| k == b"wm:")
                .expect("watermark broadcast to every task");
            assert_eq!(
                i64::from_le_bytes(wm.3[..8].try_into().unwrap()),
                2_000,
                "minimum watermark wins"
            );
        }
    }

    #[test]
    fn redistribute_by_state_key_routes_generic_state() {
        let entries: Vec<SnapshotEntry> = (0..50)
            .map(|i| entry("op", "kv", format!("user-{i}").into_bytes(), b"v"))
            .collect();
        let snapshot = encode_snapshot_entries(&entries);
        let out = redistribute_snapshots(&[snapshot], 2, EntryRouting::ByStateKey).unwrap();

        let rescaler = KeyGroupRescaler::new(1, 2);
        let mut total = 0;
        for (task, snap) in out.iter().enumerate() {
            for (_, _, key, _) in decode_snapshot_entries(snap).unwrap() {
                let expected = rescaler.task_for_key_group(key_group_for_key(&key)) as usize;
                assert_eq!(task, expected);
                total += 1;
            }
        }
        assert_eq!(total, 50);
    }

    #[test]
    fn redistribute_skips_empty_snapshots_and_emits_empty_for_idle_tasks() {
        // One stateless old task (empty bytes) plus one single-key task.
        let snap = encode_snapshot_entries(&[entry(
            "op",
            "window",
            window_state_key(b"tw:", "only-key", 0),
            b"v",
        )]);
        let out =
            redistribute_snapshots(&[Vec::new(), snap], 2, EntryRouting::WindowGroupKey).unwrap();
        assert_eq!(out.len(), 2);
        let non_empty: Vec<_> = out.iter().filter(|s| !s.is_empty()).collect();
        assert_eq!(non_empty.len(), 1, "single key lands on exactly one task");
        let entries = decode_snapshot_entries(non_empty[0]).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn redistribute_rejects_conflicting_non_watermark_broadcast_values() {
        let snap_a = encode_snapshot_entries(&[entry("op", "s", b"custom".to_vec(), b"value-a")]);
        let snap_b = encode_snapshot_entries(&[entry("op", "s", b"custom".to_vec(), b"value-b!")]);
        // "custom" does not parse as a window key → broadcast; values differ
        // and are not 8-byte watermarks (7- and 8-byte values) → conflict.
        let err = redistribute_snapshots(&[snap_a, snap_b], 2, EntryRouting::WindowGroupKey)
            .expect_err("conflicting broadcast values must be rejected");
        assert!(err.to_string().contains("conflicting"));
    }

    #[test]
    fn redistribute_roundtrip_via_state_backend() {
        // End-to-end: state backend → snapshot → redistribute → load into N
        // backends → union of contents equals the original.
        use crate::StateBackend;
        use crate::namespace::Namespace;
        use crate::rocksdb_backend::RocksDbStateBackend;

        let ns = Namespace::new("op", "window");
        let mut source = RocksDbStateBackend::new().unwrap();
        for i in 0..40 {
            source
                .put(
                    &ns,
                    window_state_key(b"tw:", &format!("g{i}"), 0),
                    format!("value-{i}").into_bytes(),
                )
                .unwrap();
        }
        let snapshot = source.snapshot().unwrap();

        let parts = redistribute_snapshots(&[snapshot], 3, EntryRouting::WindowGroupKey).unwrap();
        let mut restored_total = 0;
        for part in &parts {
            if part.is_empty() {
                continue;
            }
            let mut target = RocksDbStateBackend::new().unwrap();
            target.load_snapshot(part).unwrap();
            for key in target.list_keys(&ns).unwrap() {
                let value = target.get(&ns, &key).unwrap().unwrap();
                let original = source.get(&ns, &key).unwrap().unwrap();
                assert_eq!(value, original);
                restored_total += 1;
            }
        }
        assert_eq!(restored_total, 40);
    }
}
