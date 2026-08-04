//! Distributed physical-plan fragments and the stage builder (ADR-0003).
//!
//! Phase 52 replaces stringly `sql: <query>` task bodies with
//! protobuf-encoded DataFusion physical-plan subtrees. The
//! [`krishiv_plan::TypedTaskFragment`] envelope stays as the carrier; this
//! module owns the `dfplan:` body kind — encoding on the coordinator (stage
//! builder) and decoding on the executor.
//!
//! Body format: `dfplan:v1:<partspec>:<base64(plan proto bytes)>` where
//! `<partspec>` names the output partition(s) of the decoded plan this task
//! executes. The stage builder emits one partition per task
//! (`dfplan:v1:3:<b64>`); Phase 54 AQE rewrites extend the grammar:
//!
//! - **Coalescing**: `dfplan:v1:1,4,7:<b64>` — the task executes each listed
//!   root partition and concatenates the streams. Correct for any plan
//!   shape: root partitions are independent (each is a complete hash
//!   group), so the union of a task group's outputs equals the union the
//!   original one-task-per-partition layout would produce.
//! - **Skew split**: `dfplan:v1:5/s0m2-4:<b64>` — the task executes root
//!   partition 5 but, for upstream stage 0, reads only map tasks `[2, 4)`.
//!   Splitting is only correct when nothing above the shuffle read blocks
//!   on seeing the whole partition (see [`dfplan_body_is_split_safe`]).
//!
//! The `v1` segment is independent of the envelope version so plan-proto
//! evolution (e.g. a DataFusion upgrade that changes the proto) is detected
//! explicitly instead of failing deep inside prost decoding.
//!
//! # Stage building
//!
//! [`build_distributed_stages`] cuts an optimized physical plan at hash
//! `RepartitionExec` boundaries (Ballista-style): the subtree below each cut
//! becomes a ShuffleMap stage whose tasks hash-partition their output into
//! the shuffle store; the cut point is replaced by a [`ShuffleReadExec`]
//! leaf that streams those partitions back on the reduce side. Any shape
//! the builder cannot prove correct returns `None` — the caller falls back
//! to today's single-task `sql:` path (capability honesty).

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use base64::Engine as _;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::execution_props::ScalarSubqueryResults;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::scalar_subquery::{ScalarSubqueryExec, ScalarSubqueryLink};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties as _, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use futures::{StreamExt as _, TryStreamExt as _};

use crate::{SqlError, SqlResult};

/// Task-fragment body prefix for proto-encoded physical-plan subtrees.
pub const DFPLAN_BODY_PREFIX: &str = "dfplan:v1:";

/// Env var overriding the target partition count used when planning a
/// distributed batch query (bounds both scan parallelism and shuffle
/// partition count). Unset, the count is derived from the cluster — see
/// [`resolve_stage_target_partitions`].
pub const STAGE_TARGET_PARTITIONS_ENV: &str = "KRISHIV_STAGE_TARGET_PARTITIONS";

/// Env var that disables stage splitting entirely (`off`/`0`/`false`).
pub const STAGE_SPLIT_ENV: &str = "KRISHIV_STAGE_SPLIT";

/// Build-side byte ceiling under which a join is broadcast rather than
/// hash-shuffled, on the **staged** path only. See `planning_session_context`
/// for why the distributed default differs from DataFusion's.
pub const BROADCAST_JOIN_BYTES_ENV: &str = "KRISHIV_BROADCAST_JOIN_BYTES";

/// Default build-side byte ceiling for broadcasting on the staged path.
///
/// DataFusion's own default is 1 MiB, tuned for a single process where a
/// shuffle is a memcpy; here a shuffle is the pod network. See
/// [`planning_session_context_with_options`] for the measurement behind 32 MiB.
const DEFAULT_BROADCAST_JOIN_BYTES: usize = 32 * 1024 * 1024;

/// Default build-side row ceiling, used only when a byte estimate is absent.
const DEFAULT_BROADCAST_JOIN_ROWS: usize = 1_000_000;

/// How many tasks to create per available slot.
///
/// One task per slot fills the cluster in a single wave, but a single wave is
/// as slow as its slowest task: any skew, any straggler, any cold cache is
/// paid in full with no other work to overlap it. Splitting the same work into
/// two waves lets fast slots pick up a second task while a slow one is still
/// on its first, and halves the bytes each task holds at once. Spark's
/// long-standing guidance is 2–3× the core count for the same reasons; 2 is
/// the conservative end, since each extra wave also multiplies shuffle
/// fragments by the partition count.
const TASKS_PER_SLOT: usize = 2;

/// Never plan fewer than this many partitions: below 2 there is no exchange to
/// cut and the query degrades to a single task.
const MIN_STAGE_PARTITIONS: usize = 2;

/// Upper bound on planned partitions. Past this the shuffle fragment count
/// (partitions², written and then fetched individually) costs more than the
/// added parallelism returns.
const MAX_STAGE_PARTITIONS: usize = 512;

/// The compute capacity a query is being planned against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterCapacity {
    /// Task slots currently schedulable across every live executor.
    pub total_slots: usize,
}

/// Resolve the planning-time target partition count for distributed stages.
///
/// `cluster` is the live capacity the coordinator sees; `None` means the
/// caller has no cluster view (the embedded in-process runtime), in which case
/// the local machine's parallelism stands in for it.
///
/// This used to be the constant 4 regardless of anything. A 4-partition plan
/// leaves a 32-slot cluster 87% idle, and on a 2-slot cluster it queues work
/// two deep — the number was never related to the hardware it ran on. Deriving
/// it from live slots is what makes a query fill the cluster it was actually
/// submitted to.
///
/// The result is an upper bound, not a promise: DataFusion groups scan files
/// into at most one partition per file group, so a query over three files
/// plans three scan partitions however high this is set. Small inputs
/// therefore stay cheap without needing a size term here.
#[must_use]
pub fn resolve_stage_target_partitions(cluster: Option<ClusterCapacity>) -> usize {
    derive_stage_target_partitions(
        std::env::var(STAGE_TARGET_PARTITIONS_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok()),
        cluster,
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1),
    )
}

/// The pure derivation behind [`resolve_stage_target_partitions`], with the
/// environment and the machine passed in so it is testable (the workspace
/// forbids `unsafe`, so tests cannot set environment variables).
#[must_use]
pub fn derive_stage_target_partitions(
    explicit: Option<usize>,
    cluster: Option<ClusterCapacity>,
    local_cores: usize,
) -> usize {
    if let Some(explicit) = explicit.filter(|&n| n >= MIN_STAGE_PARTITIONS) {
        return explicit;
    }
    cluster
        .map_or(local_cores, |c| c.total_slots)
        .saturating_mul(TASKS_PER_SLOT)
        .clamp(MIN_STAGE_PARTITIONS, MAX_STAGE_PARTITIONS)
}

/// True unless stage splitting is disabled via [`STAGE_SPLIT_ENV`].
pub fn stage_split_enabled() -> bool {
    !matches!(
        std::env::var(STAGE_SPLIT_ENV)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "off" | "0" | "false" | "disabled"
    )
}

/// Session context used to plan a query for distributed stage execution.
///
/// Round-robin repartitioning is disabled: a `RoundRobinBatch` exchange left
/// inside a stage subtree would make every task of that stage re-execute all
/// input partitions (RepartitionExec drives all inputs per process), so only
/// hash exchanges — which the builder cuts into shuffle boundaries — are
/// allowed into the plan.
pub fn planning_session_context(target_partitions: usize) -> SessionContext {
    planning_session_context_with_join_threshold(target_partitions, None)
}

/// As [`planning_session_context`], with the spillable-join build-side
/// threshold supplied instead of derived from this process's cgroup.
///
/// `None` keeps the derived threshold, which is what production uses. Tests
/// pin it because the rule's behaviour is the whole difference between a
/// 3-core executor with a ~700 MB per-task share and a build box with tens of
/// gigabytes: a defect that only appears once joins actually convert is
/// invisible on the machine the tests run on.
pub fn planning_session_context_with_join_threshold(
    target_partitions: usize,
    spill_join_build_bytes: Option<u64>,
) -> SessionContext {
    planning_session_context_with_options(target_partitions, spill_join_build_bytes, None)
}

/// As [`planning_session_context_with_join_threshold`], with the broadcast
/// (`CollectLeft`) build-side ceiling supplied instead of read from
/// [`BROADCAST_JOIN_BYTES_ENV`].
///
/// `None` keeps the env-or-default value, which is what production uses.
///
/// Pinning it is the only way to reach the *cluster's* join shape from a test.
/// The staged path broadcasts any build side under 32 MiB, and every fixture
/// small enough to run in-process is far under that — so a two-table join that
/// hash-shuffles **both** sides at SF100, and therefore plans a reduce stage
/// with two `ShuffleReadExec` leaves reading two different upstream stages,
/// collapses in tests to one broadcast join with a single shuffle input. The
/// two shapes exercise different code, and only the small one was ever tested.
pub fn planning_session_context_with_options(
    target_partitions: usize,
    spill_join_build_bytes: Option<u64>,
    broadcast_join_bytes: Option<usize>,
) -> SessionContext {
    // A6: this was `SessionConfig::new()` — a bare DataFusion config carrying
    // none of the engine's settings, so `KRISHIV_RUNTIME_FILTERS` was a no-op
    // distributed, the SQL dialect differed from the one the query was written
    // against, and the batch size was DataFusion's rather than the engine's.
    // Sharing `build_single_node_session_config` is what makes the staged plan
    // the same plan the engine would have produced.
    let tp = std::num::NonZeroUsize::new(target_partitions.max(1))
        .unwrap_or(std::num::NonZeroUsize::MIN);
    let mut config = crate::build_single_node_session_config(tp, None);
    // The one deliberate divergence, and the reason this cannot simply call a
    // SqlEngine constructor: a RoundRobinBatch exchange left inside a stage
    // subtree would make every task of that stage re-execute all input
    // partitions. Only hash exchanges — which the builder cuts into shuffle
    // boundaries — may enter a staged plan.
    config
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = false;

    // D1 interim (review 2026-07-27): broadcast a small build side instead of
    // hash-shuffling both sides of the join.
    //
    // DataFusion's defaults are 1 MiB / 128k rows, tuned for a single process
    // where a shuffle is a memcpy. Here a shuffle is the pod network, measured
    // at ~11 MiB/s across three separate VPS hosts. q8/q9 hash-partition the
    // raw 600 M-row `lineitem` scan — ~36 GiB on the wire, a ~55-minute floor —
    // because the filtered dimension side lands just over 1 MiB and so is not
    // eligible to broadcast. Collecting a few tens of MiB once per task is
    // enormously cheaper than moving lineitem, and it is bounded: the build
    // side is collected into the task's memory pool, whose per-task share on
    // this cluster is ~732 MB, so the ceiling below is ~4% of it.
    //
    // Deliberately set only on the STAGED path — the embedded engine keeps
    // DataFusion's defaults, where they are correct.
    //
    // A build side that only *looks* small is caught afterwards by
    // [`is_degenerate_broadcast_join`], but only for the one case DataFusion
    // gets wrong: it happily broadcasts on an estimate of ZERO rows, which is
    // how q21 serialised. The ceiling itself is left entirely to DataFusion —
    // overriding it cost q8 4x and q9 2.5x on the cluster.
    let broadcast_bytes = broadcast_join_bytes.unwrap_or_else(|| {
        std::env::var(BROADCAST_JOIN_BYTES_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_BROADCAST_JOIN_BYTES)
    });
    let opts = config.options_mut();
    opts.optimizer.hash_join_single_partition_threshold = broadcast_bytes;
    // Both ceilings gate the same decision, so a caller asking for "never
    // broadcast" (0 bytes) must get the row ceiling zeroed too — otherwise a
    // small build side still collects and the request is silently ignored.
    opts.optimizer.hash_join_single_partition_threshold_rows = if broadcast_bytes == 0 {
        0
    } else {
        DEFAULT_BROADCAST_JOIN_ROWS
    };

    // A6: the rules. `planning_session_context` is where every distributed
    // query is planned, and it carried no engine rules at all — so
    // `SpillableJoinSelection` (q18), the semi-join reductions (q17) and
    // `CooperativeAmplifiers` (distributed cancel) were dead on exactly the
    // path being benchmarked. See `crate::with_krishiv_optimizer_rules`.
    let state_builder = crate::with_krishiv_optimizer_rules_with_join_threshold(
        datafusion::execution::session_state::SessionStateBuilder::new().with_default_features(),
        spill_join_build_bytes,
    )
    .with_config(config);

    // Object-store tables must be plannable here: if schema inference fails,
    // the caller reads that as "decline to stage" and the query silently runs
    // as a single task on one executor.
    let state_builder = match datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
        .with_object_store_registry(Arc::new(
            crate::object_store_registry::LazyCloudObjectStoreRegistry::new(),
        ))
        .build_arc()
    {
        Ok(runtime) => state_builder.with_runtime_env(runtime),
        // A runtime that will not build is not worth failing planning over —
        // the default one still plans local paths, and object-store tables
        // fall back to the single-task path as they did before.
        Err(error) => {
            tracing::warn!(%error, "cloud object-store registry unavailable for staged planning");
            state_builder
        }
    };
    SessionContext::new_with_state(state_builder.build())
}

/// Shuffle-store sub-stage key for one map task's output.
///
/// Multiple map tasks of the same stage write the same reduce-partition
/// space; the shuffle store replaces on duplicate `(job, stage, partition)`
/// keys, so each map task writes under its own sub-stage key and the reduce
/// side merges across `0..num_map_tasks`. Both sides derive the key from
/// this function — it is a wire contract between coordinator and executor.
pub fn shuffle_stage_key(stage_index: usize, map_task_index: usize) -> String {
    format!("s{stage_index}.m{map_task_index}")
}

// ── Fragment body encode/decode ────────────────────────────────────────────

/// Encode a physical plan (sub)tree to raw proto bytes.
pub fn encode_dfplan_bytes(
    plan: Arc<dyn ExecutionPlan>,
    codec: &dyn PhysicalExtensionCodec,
) -> SqlResult<Vec<u8>> {
    datafusion_proto::bytes::physical_plan_to_bytes_with_extension_codec(plan, codec)
        .map(|b| b.to_vec())
        .map_err(|e| SqlError::DataFusion {
            message: format!("physical plan proto encode: {e}"),
        })
}

/// Restriction of a task's shuffle reads to a subrange of one upstream
/// stage's map tasks (Phase 54 skew split).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DfplanMapRange {
    /// Builder index of the upstream stage whose reads are restricted.
    pub upstream_stage_index: usize,
    /// First map-task index read (inclusive).
    pub start: usize,
    /// One past the last map-task index read (exclusive).
    pub end: usize,
}

/// Parsed partition assignment of a `dfplan:v1:` task body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DfplanTaskSpec {
    /// Root output partitions this task executes (non-empty, in order).
    pub partitions: Vec<usize>,
    /// Optional skew-split map-task restriction.
    pub map_range: Option<DfplanMapRange>,
}

impl DfplanTaskSpec {
    /// Single-partition spec (the stage builder's default shape).
    pub fn single(partition: usize) -> Self {
        Self {
            partitions: vec![partition],
            map_range: None,
        }
    }

    /// Render the partition segment of the body grammar.
    fn render(&self) -> String {
        let mut out = self
            .partitions
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",");
        if let Some(range) = &self.map_range {
            out.push_str(&format!(
                "/s{}m{}-{}",
                range.upstream_stage_index, range.start, range.end
            ));
        }
        out
    }
}

/// Assemble the per-task fragment body: `dfplan:v1:<partition>:<b64>`.
pub fn dfplan_task_body(plan_bytes_b64: &str, partition: usize) -> String {
    format!("{DFPLAN_BODY_PREFIX}{partition}:{plan_bytes_b64}")
}

/// Assemble a fragment body executing several root partitions (coalescing).
pub fn dfplan_task_body_for_spec(plan_bytes_b64: &str, spec: &DfplanTaskSpec) -> String {
    format!("{DFPLAN_BODY_PREFIX}{}:{plan_bytes_b64}", spec.render())
}

/// Rewrite an existing dfplan body to a new partition spec, preserving the
/// encoded plan bytes verbatim (no proto decode — coordinator-side AQE
/// rewrites reuse the b64 payload untouched).
pub fn dfplan_body_with_spec(body: &str, spec: &DfplanTaskSpec) -> SqlResult<String> {
    let (_, b64) = split_dfplan_body(body)?;
    // Carry any leading Python-UDF directive(s) through to the rebuilt body.
    //
    // The parsers *skip* those directives; this function *re-emits* the body,
    // and re-emitting from `DFPLAN_BODY_PREFIX` onward silently dropped them.
    // AQE rewrites a reduce stage by rebuilding every task body through here, so
    // an AQE-rewritten task shipped a plan that references the Python UDF with
    // no directive telling the executor to reconstruct it — and the task died
    // with "PhysicalExtensionCodec is not provided for scalar function <name>"
    // while its sibling map tasks, whose bodies were never rebuilt, ran fine.
    let trimmed = body.trim_start();
    let rest = strip_leading_python_udf_directives(trimmed);
    let directives = trimmed.get(..trimmed.len() - rest.len()).unwrap_or("");
    Ok(format!(
        "{directives}{DFPLAN_BODY_PREFIX}{}:{b64}",
        spec.render()
    ))
}

/// Strip the leading `/* krishiv-register-python-udf(a)f:… */` directive
/// comment(s) a staged Python-UDF fragment carries ahead of its `dfplan:` body,
/// returning the remaining body. A cheap no-op for any body without a leading
/// directive. Keeps every dfplan-body parser — and the coordinator's
/// `is_dfplan_body` shuffle-input wiring / AQE split analysis — working on a
/// fragment that still carries its executor-side UDF registration directive.
pub(crate) fn strip_leading_python_udf_directives(body: &str) -> &str {
    const CLOSE: &str = " */";
    let mut rest = body.trim_start();
    while rest.starts_with("/* krishiv-register-python-udf:")
        || rest.starts_with("/* krishiv-register-python-udaf:")
    {
        let Some(end) = rest.find(CLOSE) else { break };
        rest = rest[end + CLOSE.len()..].trim_start();
    }
    rest
}

/// Split a body into its raw (partition segment, b64 payload) halves.
fn split_dfplan_body(body: &str) -> SqlResult<(&str, &str)> {
    let rest = strip_leading_python_udf_directives(body)
        .strip_prefix(DFPLAN_BODY_PREFIX)
        .ok_or_else(|| SqlError::DataFusion {
            message: format!(
                "task body is not a {DFPLAN_BODY_PREFIX} fragment: {}",
                body.chars().take(48).collect::<String>()
            ),
        })?;
    rest.split_once(':').ok_or_else(|| SqlError::DataFusion {
        message: String::from("dfplan body missing partition segment"),
    })
}

fn parse_partition_segment(segment: &str) -> SqlResult<DfplanTaskSpec> {
    let (list, range) = match segment.split_once('/') {
        Some((list, range_str)) => {
            // `/s<stage>m<start>-<end>`
            let rest = range_str
                .strip_prefix('s')
                .ok_or_else(|| SqlError::DataFusion {
                    message: format!("dfplan map range missing 's' prefix: {range_str}"),
                })?;
            let (stage, span) = rest.split_once('m').ok_or_else(|| SqlError::DataFusion {
                message: format!("dfplan map range missing 'm' separator: {range_str}"),
            })?;
            let (start, end) = span.split_once('-').ok_or_else(|| SqlError::DataFusion {
                message: format!("dfplan map range missing '-' separator: {range_str}"),
            })?;
            let parse = |s: &str, what: &str| {
                s.trim().parse::<usize>().map_err(|e| SqlError::DataFusion {
                    message: format!("dfplan map range {what}: {e}"),
                })
            };
            let range = DfplanMapRange {
                upstream_stage_index: parse(stage, "stage")?,
                start: parse(start, "start")?,
                end: parse(end, "end")?,
            };
            if range.start >= range.end {
                return Err(SqlError::DataFusion {
                    message: format!("dfplan map range is empty: m{}-{}", range.start, range.end),
                });
            }
            (list, Some(range))
        }
        None => (segment, None),
    };
    let partitions = list
        .split(',')
        .map(|p| {
            p.trim().parse::<usize>().map_err(|e| SqlError::DataFusion {
                message: format!("dfplan partition index: {e}"),
            })
        })
        .collect::<SqlResult<Vec<_>>>()?;
    if partitions.is_empty() {
        return Err(SqlError::DataFusion {
            message: String::from("dfplan body has no partitions"),
        });
    }
    Ok(DfplanTaskSpec {
        partitions,
        map_range: range,
    })
}

/// Parse the partition spec of a body without decoding the plan payload
/// (cheap coordinator-side inspection).
pub fn dfplan_body_partition_spec(body: &str) -> SqlResult<DfplanTaskSpec> {
    let (segment, _) = split_dfplan_body(body)?;
    parse_partition_segment(segment)
}

/// Split a `dfplan:v1:` body into (partition spec, plan proto bytes).
pub fn parse_dfplan_body(body: &str) -> SqlResult<(DfplanTaskSpec, Vec<u8>)> {
    let (segment, b64) = split_dfplan_body(body)?;
    let spec = parse_partition_segment(segment)?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64.as_bytes())
        .map_err(|e| SqlError::DataFusion {
            message: format!("dfplan base64 decode: {e}"),
        })?;
    Ok((spec, bytes))
}

/// Prove that encoded fragment bytes can be decoded again.
///
/// This existed once before (b278d67b) and was reverted: the first version
/// verified against a bare `SessionContext::new()`, whose runtime has no
/// object-store registry, so every fragment scanning `s3://` failed the check
/// and *silently fell back to single-task* — q1 ran as 1 task instead of 13
/// and took 595 s instead of 156 s. The commit message claimed "anything
/// needing session state to decode would fail on the executor too", which was
/// exactly backwards: the executor's runtime has `LazyCloudObjectStoreRegistry`
/// installed, and the throwaway context did not.
///
/// So the verification context must mirror the executor's decode environment.
///
/// A5 (review 2026-07-27): mirroring the executor's *object-store registry* was
/// only half of that. The executor decodes on `krishiv-executor`'s
/// `task_sql_engine`, a real [`crate::SqlEngine`] carrying Krishiv's whole
/// function registry, dialect and config; this rehearsed against
/// `planning_session_context`, a bare `SessionContext` carrying none of it. A
/// fragment referencing any engine-registered UDF therefore decoded fine on the
/// executor and failed here — and a failed verify means "decline to stage", so
/// the query silently ran as a single task. `ctx` must come from
/// [`fragment_decode_session_context`]; it is passed in so the stage builder
/// pays for building it once per query rather than once per stage.
///
/// What it is for: TPC-H q22 encodes cleanly and dies on the executor with
/// "ScalarSubqueryExpr can only be deserialized as part of a surrounding
/// ScalarSubqueryExec" — a real encode/decode asymmetry in datafusion-proto.
/// Catching it here converts a remote failure minutes in to an instant local
/// fallback.
fn verify_dfplan_roundtrip(
    bytes: &[u8],
    codec: &dyn PhysicalExtensionCodec,
    ctx: &Arc<TaskContext>,
    expected_plan: Option<&Arc<dyn ExecutionPlan>>,
) -> SqlResult<()> {
    let decoded =
        datafusion_proto::bytes::physical_plan_from_bytes_with_extension_codec(bytes, ctx, codec)
            .map_err(|e| SqlError::DataFusion {
            message: format!("physical plan proto decode: {e}"),
        })?;
    // Decoding is not the same as reconstructing. A fragment can decode into a
    // plan whose *output type* differs from the one the coordinator encoded —
    // `datafusion-proto` re-resolves aggregate UDFs by name, and a decimal
    // `avg` re-resolved on the executor can coerce to a different return type.
    // Nothing downstream notices: `ShuffleReadExec` labels its stream with the
    // schema the coordinator baked in, `RecordBatchStreamAdapter` does not
    // validate, and the disagreement only surfaces much later, deep in an
    // executor, as a bare Arrow error. TPC-H q17:
    //
    //   column types must match schema types, expected Decimal128(15, 2)
    //   but found Decimal128(30, 15) at column index 0
    //
    // The guard already exists to answer "can the executor rebuild this?", and
    // producing the same columns is the minimum meaning of that. Checking it
    // here turns a remote runtime failure into a local, named refusal that
    // degrades to correct-but-serial execution.
    if let Some(expected) = expected_plan
        && let Some(difference) = first_schema_difference(expected, &decoded, "root")
    {
        return Err(SqlError::DataFusion {
            message: format!(
                "decoded plan differs from the encoded plan; the fragment would produce \
                 columns the reader does not expect. {difference}"
            ),
        });
    }
    Ok(())
}

/// The first node where a decoded plan stops matching the plan it came from.
///
/// Comparing only the **root** schema is not enough, and q17 is why. Its
/// divergence is an `avg` over a decimal re-resolved by name during decode:
/// the aggregate node's schema changes from `Decimal128(15, 2)` to
/// `Decimal128(30, 15)`, but a projection above it casts back, so the root
/// schemas agree and the root-only check passed the fragment as sound. The
/// executor then ran a plan whose *interior* produced different types and died
/// with a bare Arrow error, and the guard written to prevent exactly that
/// reported nothing.
///
/// A mismatch in child count is a difference too: a decode that restructures
/// the tree has not reproduced the plan, whatever the schemas say.
fn first_schema_difference(
    original: &Arc<dyn ExecutionPlan>,
    decoded: &Arc<dyn ExecutionPlan>,
    path: &str,
) -> Option<String> {
    let original_children = original.children();
    let decoded_children = decoded.children();
    if original_children.len() != decoded_children.len() {
        return Some(format!(
            "at {path}: {} has {} children, decoded {} has {}",
            original.name(),
            original_children.len(),
            decoded.name(),
            decoded_children.len()
        ));
    }
    for (index, (a, b)) in original_children
        .iter()
        .zip(decoded_children.iter())
        .enumerate()
    {
        let child_path = format!("{path}/{}[{index}]", a.name());
        if let Some(difference) = first_schema_difference(a, b, &child_path) {
            return Some(difference);
        }
    }
    // Children first, deliberately. `datafusion-proto` carries no output type
    // for an aggregate: `AggregateExprBuilder::build()` re-derives it from the
    // resolved UDAF and the *input* types (see `physical_plan/mod.rs`, the
    // `UserDefinedAggrFunction` arm). Types therefore propagate upward, so the
    // deepest disagreeing node is the cause and every node above it is that
    // cause's shadow. Reporting the root first named the symptom.
    if original.schema() != decoded.schema() {
        return Some(format!(
            "at {path} ({} vs {}):\n  encoded: {:?}\n  decoded: {:?}",
            original.name(),
            decoded.name(),
            original.schema(),
            decoded.schema()
        ));
    }
    None
}

/// The session context a `dfplan:v1:` fragment is **decoded** on.
///
/// A5: the round-trip guard is the one contract on this path exercised from
/// both sides, and it was rehearsing the decode against the wrong session. The
/// executor decodes on `krishiv-executor`'s `task_sql_engine`, i.e. a real
/// [`crate::SqlEngine`] — its function registry, its SQL dialect, its config,
/// its object-store registry. `planning_session_context` is a bare
/// `SessionContext` that shares none of that, so a fragment referencing any
/// Krishiv-registered UDF (`get_json_object`, `tumble_start`, …) decoded fine
/// on the executor and failed here — and the caller reads a failed verify as
/// "decline to stage", which silently runs the whole query as a single task.
///
/// Built from the same constructor the executor uses, differing only in the
/// memory source: nothing executes on this context, so the rehearsal takes an
/// unbounded pool instead of the task's per-slot share. `target_partitions`
/// is likewise irrelevant — a fragment is decoded, never re-planned.
#[must_use]
pub fn fragment_decode_session_context() -> SessionContext {
    crate::SqlEngine::new_with_engine_memory(crate::EngineMemory::Unbounded)
        .session_context()
        .clone()
}

/// Decode a `dfplan:v1:` fragment body into (partition spec, plan).
///
/// `ctx` supplies the runtime environment (object stores, UDFs) the decoded
/// plan executes under; it does not need the original tables registered —
/// scan nodes carry their own file/split descriptions in the proto.
pub fn decode_dfplan_task(
    body: &str,
    ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
) -> SqlResult<(DfplanTaskSpec, Arc<dyn ExecutionPlan>)> {
    let (spec, bytes) = parse_dfplan_body(body)?;
    let plan =
        datafusion_proto::bytes::physical_plan_from_bytes_with_extension_codec(&bytes, ctx, codec)
            .map_err(|e| SqlError::DataFusion {
                message: format!("physical plan proto decode: {e}"),
            })?;
    let plan = pin_file_scans_to_partitions(plan)?;
    Ok((spec, plan))
}

/// Force strict file-group↔partition binding on every file scan.
///
/// Distributed tasks execute exactly one root partition of a fresh plan
/// instance. This DataFusion's file scans default to a work-stealing queue
/// shared across sibling partitions (`SharedWorkSource`), so the single
/// partition a task drives would drain *all* files — every task would read
/// the whole table. Setting `preserve_order` disables the shared queue
/// (`create_sibling_state` returns None) and each partition reads exactly
/// its own file group. Applied at decode time because the plan proto does
/// not carry the flag.
fn pin_file_scans_to_partitions(plan: Arc<dyn ExecutionPlan>) -> SqlResult<Arc<dyn ExecutionPlan>> {
    use datafusion::datasource::source::DataSourceExec;
    if let Some(source_exec) = plan.downcast_ref::<DataSourceExec>() {
        if let Some(pinned) = source_exec.data_source().with_preserve_order(true) {
            return Ok(Arc::new(DataSourceExec::new(pinned)));
        }
        return Ok(plan);
    }
    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }
    let mut new_children = Vec::with_capacity(children.len());
    let mut changed = false;
    for child in children {
        let pinned = pin_file_scans_to_partitions(Arc::clone(child))?;
        changed = changed || !Arc::ptr_eq(&pinned, child);
        new_children.push(pinned);
    }
    if !changed {
        return Ok(plan);
    }
    plan.with_new_children(new_children)
        .map_err(|e| SqlError::DataFusion {
            message: format!("scan pinning rewrite: {e}"),
        })
}

/// True when a task-fragment body carries a proto-encoded physical plan.
///
/// Tolerates a leading Python-UDF registration directive (a staged Python-UDF
/// fragment prepends one ahead of its `dfplan:` body), so the coordinator's
/// shuffle-input wiring and AQE analysis classify it correctly.
pub fn is_dfplan_body(body: &str) -> bool {
    strip_leading_python_udf_directives(body).starts_with(DFPLAN_BODY_PREFIX)
}

/// Decode a dfplan body and execute its assigned partition (executor seam).
///
/// Keeps DataFusion types out of the executor crate: the result streams as
/// the crate-level [`crate::SqlStream`]. `session` supplies the runtime
/// environment (memory pool, object stores); the decoded plan needs no
/// tables registered on it. Map-stage plans read upstream shuffle data
/// through `reader`; passing `None` leaves any [`ShuffleReadExec`] leaves
/// unexecutable (coordinator-side decode).
/// Convert over-budget hash joins in a decoded fragment to the grace hash join.
///
/// # Why this runs after decode, and not during planning
///
/// [`crate::grace_hash_join::GraceHashJoinExec`] is a Krishiv node, and
/// `datafusion-proto` cannot serialize it. When the rule that produces it ran on
/// the *coordinator*, the encoded stage plan became unencodable, and the
/// scheduler's answer to an unencodable stage is to give up on staging and run
/// the query as a **single task**. Turning the flag on therefore looked like a
/// memory fix while quietly un-distributing q10 and q21 — a silent Bar-2
/// regression, which is the worst shape a bug can take here.
///
/// Running it here is not a workaround, it is the right layer. Which algorithm
/// an operator uses to spill depends on the memory *this executor* has at *this
/// moment*; it is not part of what the plan means, so it does not belong on the
/// wire. The coordinator keeps converting known-large joins to sort-merge (which
/// proto handles), and this pass picks up what is left.
///
/// That residue is exactly the failure case: the joins the coordinator declined
/// because their build sides looked small enough are the ones that together
/// exhaust the pool and refuse a later join 877 bytes.
///
/// Off unless [`crate::grace_hash_join::enabled`]. A failure to rewrite returns
/// the plan untouched — a spill strategy must never be why a query dies.
fn apply_local_spill_strategy(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    use datafusion::physical_optimizer::PhysicalOptimizerRule;

    if !crate::grace_hash_join::enabled() {
        return plan;
    }
    let rule = crate::spillable_join::SpillableJoinSelection::for_local_execution();
    match rule.optimize(
        Arc::clone(&plan),
        &datafusion::common::config::ConfigOptions::default(),
    ) {
        Ok(rewritten) => rewritten,
        Err(error) => {
            tracing::warn!(%error, "local spill strategy declined; running the decoded plan as-is");
            plan
        }
    }
}

pub fn execute_dfplan_body(
    body: &str,
    session: &SessionContext,
    reader: Option<Arc<dyn ShufflePartitionReader>>,
) -> SqlResult<(SchemaRef, crate::SqlStream)> {
    // Peek the spec first: a skew-split map range wraps the reader BEFORE
    // codec construction so every ShuffleReadExec decoded from this body
    // sees the restricted view.
    let spec_peek = dfplan_body_partition_spec(body)?;
    let reader = match (&spec_peek.map_range, reader) {
        (Some(range), Some(inner)) => Some(Arc::new(MapRangeShuffleReader {
            inner,
            range: range.clone(),
        }) as Arc<dyn ShufflePartitionReader>),
        (_, reader) => reader,
    };
    let codec = match reader {
        Some(reader) => KrishivPhysicalCodec::executor(reader),
        None => KrishivPhysicalCodec::coordinator(),
    };
    let task_ctx = session.task_ctx();
    let (spec, plan) = decode_dfplan_task(body, &task_ctx, &codec)?;
    // Choose the spill strategy here, on the executor, not upstream: see
    // `apply_local_spill_strategy`.
    let plan = apply_local_spill_strategy(plan);
    let partition_count = plan.output_partitioning().partition_count();
    if let Some(&bad) = spec.partitions.iter().find(|&&p| p >= partition_count) {
        return Err(SqlError::DataFusion {
            message: format!(
                "dfplan partition {bad} out of range: decoded plan has \
                 {partition_count} partitions"
            ),
        });
    }
    let schema = plan.schema();
    // Execute each listed root partition and chain the streams. Root
    // partitions are independent hash groups, so concatenation is exactly
    // the union the original one-task-per-partition layout produces.
    let mut streams = Vec::with_capacity(spec.partitions.len());
    for &partition in &spec.partitions {
        let stream = plan
            .execute(partition, Arc::clone(&task_ctx))
            .map_err(|e| SqlError::DataFusion {
                message: format!("dfplan execute (partition {partition}): {e}"),
            })?;
        streams.push(stream.map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        }));
    }
    let chained = futures::stream::iter(streams).flatten();
    Ok((schema, Box::pin(chained)))
}

/// Reader wrapper implementing the skew-split map-task restriction: reads
/// of the restricted upstream stage outside `[start, end)` return empty
/// (those map tasks belong to sibling split tasks); every other read passes
/// through untouched.
#[derive(Debug)]
struct MapRangeShuffleReader {
    inner: Arc<dyn ShufflePartitionReader>,
    range: DfplanMapRange,
}

impl ShufflePartitionReader for MapRangeShuffleReader {
    fn open_partition(
        &self,
        upstream_stage_index: usize,
        map_task_index: usize,
        partition: usize,
    ) -> futures::future::BoxFuture<'static, Result<ShuffleFragmentStream, String>> {
        if upstream_stage_index == self.range.upstream_stage_index
            && !(self.range.start..self.range.end).contains(&map_task_index)
        {
            return Box::pin(async {
                Ok(Box::pin(futures::stream::empty()) as ShuffleFragmentStream)
            });
        }
        self.inner
            .open_partition(upstream_stage_index, map_task_index, partition)
    }
}

/// True when a dfplan body's decoded plan may be split by map-task ranges
/// (Phase 54 skew split) without changing results.
///
/// Splitting hands each split task a disjoint subset of the skewed
/// upstream's map outputs, so any operator that must observe the WHOLE
/// partition before emitting (final-mode aggregation, sort, window, limit,
/// distinct) would produce partial results per split. Safe plans are
/// whitelisted structurally: shuffle reads, projections, filters, batch
/// coalescing, and INNER hash joins (each row of the restricted side lands
/// in exactly one split and joins against the other side read in full, so
/// every match pair appears exactly once across splits; outer joins are
/// excluded — unmatched-row padding would be emitted per split).
pub fn dfplan_body_is_split_safe(body: &str) -> bool {
    let ctx = SessionContext::new();
    let codec = KrishivPhysicalCodec::coordinator();
    let Ok((_, plan)) = decode_dfplan_task(body, &ctx.task_ctx(), &codec) else {
        return false;
    };
    plan_is_split_safe(&plan)
}

fn plan_is_split_safe(plan: &Arc<dyn ExecutionPlan>) -> bool {
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::HashJoinExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    let safe = if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        *join.join_type() == datafusion::logical_expr::JoinType::Inner
    } else {
        plan.is::<ShuffleReadExec>()
            || plan.is::<ProjectionExec>()
            || plan.is::<FilterExec>()
            // Name match: the concrete type is deprecated in DataFusion 54
            // (BatchCoalescer replaces it) but still appears in plans.
            || plan.name() == "CoalesceBatchesExec"
    };
    safe && plan.children().iter().all(|c| plan_is_split_safe(c))
}

/// Register the backing object store for an `s3://`/`s3a://` `path` on `ctx`.
///
/// DataFusion keys object stores by scheme+authority, so this registers once
/// per bucket; re-registering the same bucket replaces the prior store. A
/// no-op for local filesystem paths.
///
/// This mirrors `SqlEngine::register_s3_object_store_for_warehouse`, which
/// does the same thing for the engine's long-lived context. The stage builder
/// plans on a throwaway context instead, so it needs its own registration —
/// that asymmetry is exactly what made object-store tables un-stageable.
fn register_object_store_for_path(ctx: &SessionContext, path: &str) -> SqlResult<()> {
    if !(path.starts_with("s3://") || path.starts_with("s3a://")) {
        return Ok(());
    }
    let url = url::Url::parse(path).map_err(|e| SqlError::DataFusion {
        message: format!("staged planning: invalid object-store url {path}: {e}"),
    })?;
    let bucket = url.host_str().unwrap_or_default();
    let store_url =
        url::Url::parse(&format!("s3://{bucket}")).map_err(|e| SqlError::DataFusion {
            message: format!("staged planning: invalid bucket url for {path}: {e}"),
        })?;
    let store = crate::build_s3_object_store(bucket).map_err(|e| SqlError::DataFusion {
        message: format!("staged planning: object store init for {path}: {e}"),
    })?;
    ctx.register_object_store(&store_url, store);
    Ok(())
}

/// A parquet table to plan against, and what is known to be true about it.
///
/// `primary_key` is an **informational constraint**, in the sense Spark and
/// Databricks use the word (`RELY`): the engine does not verify it, it trusts
/// it. Declaring a key that does not hold changes results, exactly as a wrong
/// `RELY` constraint does there. It is empty by default, so a caller that says
/// nothing gets the previous behaviour.
///
/// # Why the engine wants to be told
///
/// DataFusion already derives [`FunctionalDependencies`] from a provider's
/// constraints, and `optimize_projections` already uses
/// `get_required_group_by_exprs_indices` to shrink a `GROUP BY` to the minimal
/// functionally-equivalent subset. All of that machinery is live and does
/// nothing here, because a parquet file carries no key and every table we
/// register declares none — so the dependency set is always empty.
///
/// # What this does and does NOT buy — read before assuming
///
/// DataFusion's rule keeps `(columns the parent requires) ∪ (minimal FD
/// subset)`. So a declared key removes a column from a `GROUP BY` **only when
/// nothing downstream selects it**. That is a real and common shape —
/// `GROUP BY a, b` projecting only `a` — and it is what
/// `a_declared_primary_key_shrinks_the_group_by` proves.
///
/// It is **not** TPC-H q10's shape. q10 selects all seven grouped columns, so
/// the parent requires them and no key declaration can prune them. Measured
/// 2026-07-31: rewriting q10 to carry only the key is worth **14.8x**
/// (1784.6 s → 120.9 s; the `orders⋈customer` stage alone 8,968 → 38.9
/// task-seconds), but capturing that needs **late materialisation** —
/// aggregate on the key, take the top N, then re-join for the display columns.
/// DataFusion has no such rule and neither do we. Declaring a key is a
/// precondition for writing one, not a substitute.
///
/// The 230x on that stage is superlinear in the columns rather than the bytes:
/// per-row string handling (hashing and copying ~227 B rows), which is why
/// neither bandwidth nor fetch concurrency moved it
/// (`q10-dist-s2-is-the-whole-query`).
///
/// [`FunctionalDependencies`]: datafusion::common::FunctionalDependencies
#[derive(Debug, Clone)]
pub struct ParquetTableSpec {
    /// Name the query refers to the table by.
    pub name: String,
    /// Single parquet file or a directory dataset; local or object storage.
    pub path: String,
    /// Columns that jointly form a primary key, if the caller declares one.
    pub primary_key: Vec<String>,
}

impl ParquetTableSpec {
    /// A table with nothing declared about it.
    pub fn new(name: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
            primary_key: Vec::new(),
        }
    }

    /// Declare a (possibly composite) primary key.
    #[must_use]
    pub fn with_primary_key<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.primary_key = columns.into_iter().map(Into::into).collect();
        self
    }
}

/// Mark an extension-less object-store path as a directory so
/// [`ListingTableUrl`] treats it as a prefix rather than a single file.
///
/// # The failure this fixes
///
/// `ListingTableUrl::parse` decides file-vs-directory by **statting the
/// filesystem**. For a local path that works: `/data/sf100/lineitem` is seen to
/// be a directory and gets a trailing slash added. For an object store there is
/// nothing to stat, so `s3://bucket/sf100/lineitem` is taken to be a file, and
/// the `.parquet` extension filter then rejects it:
///
/// ```text
/// File path 's3://krishiv-bench/tpch/sf100/lineitem' does not match the
/// expected extension '.parquet'
/// ```
///
/// The message blames the extension, but the data is a directory of parts and
/// nothing is wrong with it — so the reader goes looking for a naming problem
/// that does not exist. The same registration works locally and fails remotely,
/// which is the kind of asymmetry that reads as "object stores are broken".
///
/// The rule is the one a caller means: a final segment containing no `.` is a
/// directory. `s3://b/data.parquet` keeps its file semantics; a path that
/// already ends in `/` is left alone; local paths are untouched, because
/// statting them is strictly better information than this heuristic.
fn directory_aware_url(path: &str) -> String {
    if !path.contains("://") || path.ends_with('/') {
        return path.to_owned();
    }
    let looks_like_a_file = path
        .rsplit('/')
        .next()
        .is_some_and(|segment| segment.contains('.'));
    if looks_like_a_file {
        path.to_owned()
    } else {
        format!("{path}/")
    }
}

/// Register one parquet table, attaching its declared key as a DataFusion
/// constraint so the optimizer's functional-dependency machinery can see it.
///
/// With no declared key this is exactly `register_parquet`. With one, the
/// table is built explicitly so [`ListingTable::with_constraints`] can be
/// applied — `register_parquet` has no way to pass them.
///
/// A declared column that is not in the file's schema is an error rather than
/// a silent no-op: a typo would otherwise turn into "the optimization
/// mysteriously does not apply", which is the least debuggable outcome.
///
/// # Why the options come from `ParquetReadOptions` instead of `ListingOptions::new`
///
/// The two branches below must describe the *same table*; the only intended
/// difference is the constraint. Building the keyed branch's options by hand
/// silently made them differ, because `ListingOptions::new` does not mean
/// "defaults" — it means:
///
/// ```text
/// collect_stat:      false   // vs. session `collect_statistics`, default true
/// target_partitions: 1       // vs. session `target_partitions`
/// ```
///
/// `register_parquet` reaches `ListingOptions` through
/// [`ReadOptions::to_listing_options`], which ends in
/// `.with_session_config_options(config)` and sets both from the session. The
/// hand-built branch never did, so **declaring a primary key turned that
/// table's statistics off** and collapsed its scan to one partition.
///
/// Statistics off is not a slow path, it is a blind one. Every rule that keys
/// on a known size stops seeing anything: measured on the SF100 cluster,
/// `SpillableJoinSelection` reported `unmeasurable == hash_joins` in **all 414
/// passes** across coordinator and executors and converted **zero** joins, so
/// no oversized hash join could ever be made spillable and q21 died on a build
/// side nothing was left to catch. It also blinds broadcast selection,
/// `ShuffleReadExec`'s cut-subtree estimate, and join ordering.
///
/// So the options are produced by DataFusion's own conversion, from the same
/// `ParquetReadOptions::default()` the unkeyed branch passes. The two paths
/// cannot drift again without DataFusion changing under both at once.
pub async fn register_parquet_table(
    ctx: &SessionContext,
    spec: &ParquetTableSpec,
) -> SqlResult<()> {
    use datafusion::common::{Constraint, Constraints};
    use datafusion::datasource::TableProvider as _;
    use datafusion::datasource::file_format::options::ReadOptions as _;
    use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};

    let read_options = datafusion::prelude::ParquetReadOptions::default();

    // `directory_aware_url` on BOTH branches. Applying it only to the keyed one
    // left the common case broken: with the minimal-key corpus (keys on
    // customer and nation), `lineitem` takes this branch, so the very first
    // query still failed with "does not match the expected extension" while the
    // fix looked applied. The two branches must agree about what a path MEANS,
    // exactly as they must agree about `ListingOptions` — this is the same
    // divergence that turned statistics off for keyed tables.
    if spec.primary_key.is_empty() {
        return ctx
            .register_parquet(&spec.name, &directory_aware_url(&spec.path), read_options)
            .await
            .map_err(|e| SqlError::DataFusion {
                message: format!("staged planning: register '{}': {e}", spec.name),
            });
    }

    let url = ListingTableUrl::parse(directory_aware_url(&spec.path)).map_err(|e| {
        SqlError::DataFusion {
            message: format!("staged planning: table url for '{}': {e}", spec.name),
        }
    })?;
    let options = read_options.to_listing_options(&ctx.copied_config(), ctx.copied_table_options());
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .infer_schema(&ctx.state())
        .await
        .map_err(|e| SqlError::DataFusion {
            message: format!("staged planning: infer schema for '{}': {e}", spec.name),
        })?;
    let table = ListingTable::try_new(config).map_err(|e| SqlError::DataFusion {
        message: format!("staged planning: listing table '{}': {e}", spec.name),
    })?;

    let schema = table.schema();
    let mut indices = Vec::with_capacity(spec.primary_key.len());
    for column in &spec.primary_key {
        let index = schema.index_of(column).map_err(|_| SqlError::DataFusion {
            message: format!(
                "declared primary key column '{column}' is not in table '{}' \
                     (columns: {})",
                spec.name,
                schema
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
        })?;
        indices.push(index);
    }

    let table = table.with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
        indices,
    )]));
    ctx.register_table(spec.name.as_str(), Arc::new(table))
        .map_err(|e| SqlError::DataFusion {
            message: format!("staged planning: register '{}': {e}", spec.name),
        })?;
    tracing::debug!(
        table = %spec.name,
        primary_key = ?spec.primary_key,
        "registered parquet table with a declared primary key"
    );
    Ok(())
}

/// Plan a query over parquet tables and cut it into stages
/// (coordinator seam — keeps DataFusion types out of the scheduler crate).
///
/// `tables` are `(table_name, path)` pairs; a path may be a single parquet
/// file or a directory dataset, on the local filesystem or in object storage.
/// To declare a primary key, use [`build_stages_for_parquet_tables`].
/// Planning happens on a fresh [`planning_session_context`], so krishiv SQL
/// extensions (streaming windows, catalog DML, UDFs) fail to plan here and
/// surface as `Err` — callers treat any error as "fall back to the
/// single-task path".
/// `cluster` sizes the plan to the capacity it will run on; `None` falls back
/// to the local machine (see [`resolve_stage_target_partitions`]).
pub async fn build_stages_for_parquet_query(
    query: &str,
    tables: &[(String, String)],
    cluster: Option<ClusterCapacity>,
) -> SqlResult<Option<DistributedStagePlan>> {
    let specs: Vec<ParquetTableSpec> = tables
        .iter()
        .map(|(name, path)| ParquetTableSpec::new(name, path))
        .collect();
    build_stages_for_parquet_tables(query, &specs, cluster).await
}

/// As [`build_stages_for_parquet_query`], but each table may declare what is
/// known about it — currently a primary key, which the optimizer turns into a
/// functional dependency and uses to shrink `GROUP BY` lists.
///
/// See [`ParquetTableSpec`] for the semantics and for what it is worth.
pub async fn build_stages_for_parquet_tables(
    query: &str,
    tables: &[ParquetTableSpec],
    cluster: Option<ClusterCapacity>,
) -> SqlResult<Option<DistributedStagePlan>> {
    let target_partitions = resolve_stage_target_partitions(cluster);
    tracing::debug!(
        target_partitions,
        total_slots = cluster.map(|c| c.total_slots),
        "planning distributed stages"
    );
    let ctx = planning_session_context(target_partitions);
    for spec in tables {
        // An `s3://` table needs its object store on the planning context
        // before the schema can be inferred. Without this the registration
        // errors, the caller swallows the error as "decline to stage", and the
        // job silently runs as a SINGLE task — an entire object-store-backed
        // dataset scanned by one executor while the rest of the cluster idles.
        // That degradation is invisible: the query still returns correct rows,
        // just without any distribution. Local paths are unaffected (the
        // helper is a no-op for them).
        register_object_store_for_path(&ctx, &spec.path)?;
        register_parquet_table(&ctx, spec).await?;
    }
    // A Python scalar UDF shipped inline (`/* krishiv-register-python-udf */`)
    // must be known by name/signature for planning to resolve it, then stripped
    // so the parser sees clean SQL. The stage bodies carry the same directive so
    // the executor reconstructs the worker-backed UDF before decoding the plan;
    // here the coordinator only needs the signature (the closure is never
    // invoked during planning — Volatile keeps it out of const-folding).
    let udf_directive_source = query;
    let query = register_python_udf_signatures_and_strip(&ctx, query)?;
    let df = ctx.sql(&query).await.map_err(|e| SqlError::DataFusion {
        message: format!("staged planning: {e}"),
    })?;
    // q22: fold uncorrelated scalar subqueries to constants before physical
    // planning, or the whole query silently runs as a single task. See
    // `inline_uncorrelated_scalar_subqueries`.
    let df = inline_uncorrelated_scalar_subqueries(&ctx, df).await?;
    let plan = df
        .create_physical_plan()
        .await
        .map_err(|e| SqlError::DataFusion {
            message: format!("staged physical planning: {e}"),
        })?;
    build_distributed_stages_with_udf_directives(plan, udf_directive_source)
}

/// Evaluate **uncorrelated** scalar subqueries on the coordinator and replace
/// each with the constant it produces.
///
/// **q22 (review 2026-07-27).** `datafusion-proto` cannot round-trip a
/// `ScalarSubqueryExpr` — it decodes only "as part of a surrounding
/// ScalarSubqueryExec" — so `verify_dfplan_roundtrip` refuses the fragment and
/// the caller reads that refusal as "decline to stage". The query then runs as
/// ONE task on ONE executor. It returns the right answer, so the sweep recorded
/// a clean pass while an entire query class had silently stopped being
/// distributed. That is worse than a failure, because nothing points at it.
///
/// An uncorrelated scalar subquery *is a constant*: it references no column of
/// the outer query, so evaluating it once up front and substituting the literal
/// is semantically exact, not an approximation. It also removes the
/// un-encodable node outright rather than routing around it, which is why this
/// is preferred over teaching the codec a new trick.
///
/// Deliberately conservative: anything unexpected — a correlated subquery, more
/// than one row, an execution error, a shape we do not recognise — is left
/// untouched, so the worst case is exactly today's behaviour rather than a new
/// failure mode. The subquery runs on the coordinator, which is the right place
/// for it: it is by definition small (one row), and folding it is what lets the
/// *expensive* outer query distribute.
/// Input bytes above which a scalar subquery is not worth folding on the
/// coordinator.
///
/// The coordinator is a control plane, not a worker: it typically has a
/// fraction of an executor's cores and pool. Anything it evaluates inline is
/// single-node, un-spillable in practice, and blocks the submit handler. 256
/// MiB is generous for a genuine constant lookup and far below a fact-table
/// scan.
const MAX_FOLDABLE_SUBQUERY_INPUT_BYTES: usize = 256 * 1024 * 1024;

/// Whether `df`'s subquery is small enough to evaluate on the coordinator.
///
/// Unknown size counts as *not* cheap. That is the conservative direction
/// here: the penalty for declining to fold is the pre-existing single-task
/// fallback, while the penalty for folding something huge is a coordinator
/// that stops answering submits.
async fn subquery_is_cheap_to_fold(df: &datafusion::dataframe::DataFrame) -> bool {
    use datafusion::common::stats::Precision;

    let Ok(plan) = df.clone().create_physical_plan().await else {
        return false;
    };
    let Ok(stats) = plan.partition_statistics(None) else {
        return false;
    };
    match stats.total_byte_size {
        Precision::Exact(bytes) | Precision::Inexact(bytes) => {
            bytes <= MAX_FOLDABLE_SUBQUERY_INPUT_BYTES
        }
        Precision::Absent => false,
    }
}

async fn inline_uncorrelated_scalar_subqueries(
    ctx: &SessionContext,
    df: datafusion::dataframe::DataFrame,
) -> SqlResult<datafusion::dataframe::DataFrame> {
    use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::{Expr, LogicalPlan};

    let plan = df.logical_plan().clone();

    // Pass 1 — collect the distinct uncorrelated scalar subqueries. Keyed by
    // the subquery plan's rendering so the same subquery written twice is
    // executed once.
    let mut pending: Vec<(String, LogicalPlan)> = Vec::new();
    let collect = plan.apply(|node| {
        for expr in node.expressions() {
            expr.apply(|e| {
                if let Expr::ScalarSubquery(sub) = e
                    && sub.outer_ref_columns.is_empty()
                {
                    let key = sub.subquery.display_indent().to_string();
                    if !pending.iter().any(|(k, _)| *k == key) {
                        pending.push((key, sub.subquery.as_ref().clone()));
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if collect.is_err() || pending.is_empty() {
        return Ok(df);
    }

    // Pass 2 — execute each. A failure here is not fatal: drop that entry and
    // the expression stays as it was.
    let mut folded: Vec<(String, datafusion::scalar::ScalarValue)> = Vec::new();
    for (key, sub_plan) in pending {
        let sub_df = datafusion::dataframe::DataFrame::new(ctx.state(), sub_plan);
        // Cheapness gate. A scalar subquery *returns* one row; that says
        // nothing about what it costs to *compute*. TPC-H q15's
        // `(SELECT max(total_revenue) FROM revenue0)` aggregates 600 M
        // lineitem rows to produce its single value — so folding it ran a
        // full SF100 aggregate single-node on the coordinator, inside the
        // synchronous submit handler, and `/batch-sql/submit` stopped
        // answering within the client's 60 s timeout. The client then
        // retried, and each retry started another one.
        //
        // Judge by the subquery's *input*, not its output. Over the
        // threshold, leave the expression alone: that is exactly the
        // pre-existing behaviour (the query declines to stage and runs as one
        // task), which is a known, survivable cost — unlike a coordinator
        // that stops accepting work.
        if !subquery_is_cheap_to_fold(&sub_df).await {
            tracing::info!(
                subquery = %key.lines().next().unwrap_or_default(),
                "scalar subquery too large to fold on the coordinator; leaving it in the plan"
            );
            continue;
        }
        let Ok(batches) = sub_df.collect().await else {
            continue;
        };
        let rows: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        // Zero rows is SQL NULL; more than one row is a runtime error that the
        // normal path must keep raising, so leave it alone.
        if rows > 1 {
            continue;
        }
        let Some(batch) = batches.iter().find(|b| b.num_rows() == 1) else {
            // No rows at all: the subquery is NULL of its declared type.
            let Some(first) = batches.first() else {
                continue;
            };
            let Some(field) = first.schema().fields().first().cloned() else {
                continue;
            };
            if let Ok(null) = datafusion::scalar::ScalarValue::try_from(field.data_type()) {
                folded.push((key, null));
            }
            continue;
        };
        let Some(column) = batch.columns().first() else {
            continue;
        };
        if let Ok(value) = datafusion::scalar::ScalarValue::try_from_array(column, 0) {
            folded.push((key, value));
        }
    }
    if folded.is_empty() {
        return Ok(df);
    }

    // Pass 3 — substitute.
    let rewritten = plan.transform_up(|node| {
        let exprs = node.expressions();
        if !exprs.iter().any(Expr::contains_scalar_subquery) {
            return Ok(Transformed::no(node));
        }
        let mut changed = false;
        let mut new_exprs = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let out = expr.transform_up(|e| {
                if let Expr::ScalarSubquery(sub) = &e
                    && sub.outer_ref_columns.is_empty()
                {
                    let key = sub.subquery.display_indent().to_string();
                    if let Some((_, value)) = folded.iter().find(|(k, _)| *k == key) {
                        return Ok(Transformed::yes(datafusion::prelude::lit(value.clone())));
                    }
                }
                Ok(Transformed::no(e))
            })?;
            changed |= out.transformed;
            new_exprs.push(out.data);
        }
        if !changed {
            return Ok(Transformed::no(node));
        }
        let inputs = node.inputs().into_iter().cloned().collect::<Vec<_>>();
        let rebuilt = node.with_new_exprs(new_exprs, inputs)?;
        Ok(Transformed::yes(rebuilt))
    });

    match rewritten {
        Ok(t) if t.transformed => {
            tracing::info!(
                folded = folded.len(),
                "q22: folded uncorrelated scalar subqueries to constants so the \
                 query can be staged instead of running as a single task"
            );
            Ok(datafusion::dataframe::DataFrame::new(ctx.state(), t.data))
        }
        // Rewrite declined or failed: the original plan is still correct, and
        // the pre-existing single-task fallback still applies.
        _ => Ok(df),
    }
}

/// Register a signature-only DataFusion scalar UDF for every inline
/// `/* krishiv-register-python-udf:name:in,…:out:pickle */` directive in
/// `query`, so the coordinator can plan a staged query that references it, and
/// return `query` with the directives stripped (clean SQL for the parser).
///
/// The registered UDF's implementation errors if invoked — it exists only to
/// carry the name, argument types, and return type through planning and physical
/// serialization (the plan references the UDF by name; the executor supplies the
/// real worker-backed implementation on decode). Marked `Volatile` so the
/// optimizer never tries to const-fold it at plan time. Aggregate directives
/// (`python-udaf`) are intentionally left in place: staged aggregation is not
/// planned here, so those queries fall back to the single-task path.
pub fn register_python_udf_signatures_and_strip(
    ctx: &SessionContext,
    query: &str,
) -> SqlResult<String> {
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    const PREFIX: &str = "/* krishiv-register-python-udf:";
    if !query.contains(PREFIX) {
        return Ok(query.to_string());
    }
    let mut out = String::with_capacity(query.len());
    let mut rest = query;
    while let Some(start) = rest.find(PREFIX) {
        out.push_str(&rest[..start]);
        let after = &rest[start + PREFIX.len()..];
        let Some(end) = after.find(" */") else {
            out.push_str(&rest[start..]);
            return Ok(out);
        };
        let body = &after[..end];
        rest = &after[end + " */".len()..];
        // name:in1,in2:out:pickle_b64  (pickle unused for planning)
        let mut parts = body.splitn(4, ':');
        let (name, in_types, out_type) = match (parts.next(), parts.next(), parts.next()) {
            (Some(n), Some(i), Some(o)) => (n, i, o),
            _ => continue,
        };
        let input_types: Vec<arrow::datatypes::DataType> = if in_types.is_empty() {
            Vec::new()
        } else {
            in_types
                .split(',')
                .map(crate::python_udf_arrow_type)
                .collect()
        };
        let return_type = crate::python_udf_arrow_type(out_type);
        let name_owned = name.to_string();
        let udf = create_udf(
            name,
            input_types,
            return_type,
            Volatility::Volatile,
            Arc::new(move |_: &[ColumnarValue]| {
                Err(DataFusionError::NotImplemented(format!(
                    "python UDF '{name_owned}' executes on the executor, not during \
                     coordinator planning"
                )))
            }),
        );
        ctx.register_udf(udf);
    }
    out.push_str(rest);
    Ok(out)
}

// ── Shuffle partition reader (executor-injected) ───────────────────────────

/// Executor-side access to upstream shuffle partitions.
///
/// The executor implements this over its shuffle store (local reads) and
/// the Flight endpoints delivered with the task assignment (remote reads);
/// `krishiv-sql` stays free of shuffle/transport dependencies.
pub trait ShufflePartitionReader: fmt::Debug + Send + Sync {
    /// Open one map task's output for `partition` of `upstream_stage_index`.
    ///
    /// The returned future resolves once the fragment has been *located* — a
    /// missing partition is an error here, before any rows are produced — and
    /// the stream then yields its batches as they are decoded.
    ///
    /// # Why this streams
    ///
    /// This used to return `Vec<RecordBatch>`: a reduce task materialised each
    /// upstream fragment whole, and neither the Flight decode buffers nor the
    /// resulting batches passed through the DataFusion memory pool. With three
    /// task slots and several fragments in flight per slot, that is hundreds of
    /// megabytes the pool cannot see and therefore cannot make anyone spill for
    /// — which is how an executor with a 2.6 GB pool reached 4.5 GiB of heap and
    /// was OOM-killed on TPC-H q10. Streaming makes a reduce task's fragment
    /// cost one batch instead of one fragment.
    ///
    /// A missing fragment (the map task produced no rows for this partition)
    /// yields an empty stream, not an error.
    fn open_partition(
        &self,
        upstream_stage_index: usize,
        map_task_index: usize,
        partition: usize,
    ) -> futures::future::BoxFuture<'static, Result<ShuffleFragmentStream, String>>;
}

/// One upstream fragment's batches, in write order.
pub type ShuffleFragmentStream =
    futures::stream::BoxStream<'static, Result<arrow::record_batch::RecordBatch, String>>;

/// Env var overriding how many map fragments ONE reduce partition fetches at
/// once. See the `buffered` call in [`ShuffleReadExec::execute`].
///
/// Deliberately **not** `KRISHIV_SHUFFLE_FETCH_CONCURRENCY`: that name is
/// already taken by the executor-wide Flight semaphore
/// (`fragment/common.rs`, default 8 in-flight requests per process). The two
/// are different limits — this one is per reduce partition, that one is the
/// process-wide ceiling every partition's fetches must also pass through — and
/// giving them one name would mean a single number silently driving both.
pub const SHUFFLE_FETCH_BUFFER_ENV: &str = "KRISHIV_SHUFFLE_FETCH_BUFFER";

/// Map fragment opens in flight per reduce partition, by default.
///
/// **Must be 1 unless the shuffle *server* is changed first.** Raising it hangs
/// the cluster, and the hang is total rather than slow.
///
/// # Why prefetching fragments deadlocks
///
/// `ShuffleFlightService::do_get` takes a permit from `serve_permits` *before*
/// the store reads anything and holds it for the **response stream's lifetime**
/// (`PermitHoldingStream`) — because for any partition at or below
/// `INLINE_READ_LIMIT` the store has already read the whole file into memory,
/// and that buffer lives as long as the response. So an open-but-undrained
/// response occupies a server permit.
///
/// A collecting client could not expose that: it drained each fragment
/// immediately, so permits were released promptly. A client that opens `n`
/// fragments ahead and consumes them in order holds `n` server permits while
/// draining one. Demand is then `executors x slots x n` against a per-server
/// limit derived from its page-cache budget (`serve_concurrency_limit`, single
/// digits on these 5 GiB nodes), and because every executor is both a client and
/// a server the waits form cycles across nodes. Nothing times out; the job sits
/// at 0% CPU forever.
///
/// Measured 2026-07-30 on the 3-node SF100 cluster: with this at 8, TPC-H q2
/// wedged permanently at 132/181 tasks with executors at 0-5% CPU and no errors
/// logged. With it at 1, the identical image ran q2 in **104.3 s**.
///
/// # Why it is no longer 1
///
/// The precondition named above has been met: `ShuffleFlightService` now admits
/// a `do_get` by the **bytes** it will hold resident (one `stat`, no read)
/// rather than by counting open responses. A response-count cap had to price
/// every response at `INLINE_READ_LIMIT`, so at SF100's ~3.5 MB average
/// fragment it reserved ~9x the headroom each fetch actually needed — which is
/// what made single-digit response caps the binding constraint.
///
/// What the pin cost, measured on q10 at SF100 (`fast-c91a7b26`):
///
/// ```text
/// dist-s2  reads 3.49 GB  ->  11,304 task-seconds  (0.31 MB/s, CPU 0.3 of 3)
/// dist-s4  reads 7.27 GB  ->   2,339 task-seconds
/// ```
///
/// Twice the bytes, a fifth of the time: both stages make the same 36 round
/// trips, so the stage with fewer bytes per fetch is the one the fixed per-fetch
/// stall dominates. That is the signature of serialised latency, not bandwidth.
/// The tail is worse — the final gather stage is a single task making
/// 18 partitions x 18 map tasks = 324 strictly serial fetches.
///
/// # The bound that decides how high this may go
///
/// With prefetch `P`, a reduce stream may hold `P - 1` admitted-but-unconsumed
/// responses while waiting for its head. Cluster-wide that is
/// `executors x slots x reads_per_task x (P - 1)` responses pinned against each
/// server's budget. The deadlock the old note describes is exactly the case
/// where those pinned responses exhaust every server, so no head can be
/// admitted and nothing can drain.
///
/// FIFO admission does *not* by itself rule this out, because a stream's head
/// and its siblings can be queued on **different** executors' semaphores, so the
/// head has no ordering guarantee relative to them.
///
/// That paragraph used to continue: "the safety margin is therefore
/// quantitative, not structural … 3 x 3 x 2 = 18 cluster-wide, ~72 granules of
/// a 256-granule budget". **Both halves of that were wrong, and were measured
/// wrong on 2026-07-31.**
///
///  * The count omitted `reads_per_task`. q10's reduce stage reads 18 map
///    fragments, so the real figure is 3 x 3 x **18** x (P-1), an order of
///    magnitude larger.
///  * The margin is **not** quantitative. Re-run with
///    `KRISHIV_SHUFFLE_SERVE_CONCURRENCY=24` — a 768 MiB budget, 3x the
///    default — and `P = 2` **still deadlocked**: q10's `dist-s2` stalled at
///    15/18 tasks having moved 0.00 MB of network in 40 s. A cycle can form at
///    any budget, because the head has no ordering guarantee relative to the
///    siblings holding the capacity it needs.
///
/// It was also *slower* before it stalled: `dist-s2` median 497 s at P = 2
/// versus 433 s at P = 1.
///
/// So there is no number to tune here. Prefetch above 1 requires the server to
/// stop holding a permit for the response stream's lifetime — which means not
/// buffering the whole fragment (`INLINE_READ_LIMIT`), not pricing the buffer
/// more cleverly.
///
/// Raising this further is a measurement, not a judgement call: it needs a live
/// SF100 run that both completes and shows the win, because the failure mode is
/// a silent cluster-wide hang with no error logged.
///
/// # That measurement was taken, and `2` FAILED — back to 1
///
/// TPC-H q10 at SF100 on 2026-07-31, stage `dist-s2`: nine tasks reported
/// `Running` for **40+ minutes** while the cluster did nothing at all.
///
/// ```text
///                     network moved in 45 s (3 executors, eth0 rx)
///   prefetch = 2      ~2.5 KB          <- heartbeats only
///   prefetch = 1      1.77 GB          <- 39 MB/s aggregate
/// ```
///
/// Executors sat at 10-20% CPU, `shuffle_bytes_written` did not advance across
/// a 60 s window, no task completed, and **nothing was logged**. That is
/// precisely the hang described above, and the quantitative safety margin
/// argued for `P = 2` did not hold: it assumed a small `reads_per_task`, but
/// the bound is `executors x slots x reads_per_task x (P - 1)`, and q10's
/// reduce stage reads 18 map fragments per task, so the pinned-response count
/// is an order of magnitude past the estimate.
///
/// The same change is the likeliest cause of q3 regressing from 505 s to over
/// 3300 s on the preceding image.
///
/// So: **1 until the server stops holding a permit for the response's
/// lifetime.** Byte-based admission was a necessary step, not a sufficient one
/// — the permit is still held across the whole stream, so an
/// admitted-but-undrained response still pins server capacity. Making the
/// prefetch safe needs the *hold* removed, not the accounting improved.
const DEFAULT_SHUFFLE_FETCH_BUFFER: usize = 1;

/// Resolve [`DEFAULT_SHUFFLE_FETCH_BUFFER`], honouring the env override.
///
/// Clamped to at least 1: a zero would make `buffered` yield nothing and hang
/// the query, which is a worse failure than any concurrency choice.
fn shuffle_fetch_buffer() -> usize {
    std::env::var(SHUFFLE_FETCH_BUFFER_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_SHUFFLE_FETCH_BUFFER)
        .max(1)
}

// ── ShuffleReadExec ────────────────────────────────────────────────────────

/// Leaf node that streams an upstream ShuffleMap stage's output partitions.
///
/// `execute(p)` merges partition `p` across all map tasks of the upstream
/// stage. On the coordinator (encode side) the node carries no reader and
/// cannot execute; the executor's codec injects one at decode time.
#[derive(Debug)]
pub struct ShuffleReadExec {
    upstream_stage_index: usize,
    num_map_tasks: usize,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    reader: Option<Arc<dyn ShufflePartitionReader>>,
    /// D3(2): estimated size of the stage feeding this read.
    ///
    /// Without this, `partition_statistics` falls through to DataFusion's
    /// `Statistics::new_unknown` and **every shuffle-fed join side reports
    /// `Precision::Absent`**. `SpillableJoinSelection` keeps hash join on
    /// absent statistics by design (guessing "big" is what took q2 from 189 s
    /// past a 2400 s timeout), so the rule could never fire on a distributed
    /// plan no matter where it was registered — q18 failed with
    /// `Resources exhausted: HashJoinInput` on every run. A6 registers the
    /// rule; this is what lets it decide anything.
    ///
    /// It is the *planning-time estimate* of the subtree that was cut, taken
    /// at the moment of the cut, not a post-execution measurement: the plan is
    /// built and optimized before any stage runs, so measured sizes do not
    /// exist yet. Reported as `Inexact` for exactly that reason.
    upstream_rows: Option<usize>,
    upstream_bytes: Option<usize>,
}

impl ShuffleReadExec {
    pub fn new(
        upstream_stage_index: usize,
        num_map_tasks: usize,
        partition_count: usize,
        schema: SchemaRef,
        reader: Option<Arc<dyn ShufflePartitionReader>>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(partition_count.max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            upstream_stage_index,
            num_map_tasks,
            schema,
            properties,
            reader,
            upstream_rows: None,
            upstream_bytes: None,
        }
    }

    /// Attach the cut subtree's estimated size. See [`Self::upstream_rows`].
    #[must_use]
    pub fn with_upstream_estimate(mut self, rows: Option<usize>, bytes: Option<usize>) -> Self {
        self.upstream_rows = rows;
        self.upstream_bytes = bytes;
        self
    }

    /// The estimate as `(rows, bytes)`, for encoding onto the wire.
    pub fn upstream_estimate(&self) -> (Option<usize>, Option<usize>) {
        (self.upstream_rows, self.upstream_bytes)
    }

    /// Read the usable part of a `Statistics`: a `Precision::Absent` value
    /// stays `None` so an unknown estimate is never laundered into a
    /// confident one.
    fn precision_value(p: &datafusion::common::stats::Precision<usize>) -> Option<usize> {
        match p {
            datafusion::common::stats::Precision::Exact(v)
            | datafusion::common::stats::Precision::Inexact(v) => Some(*v),
            datafusion::common::stats::Precision::Absent => None,
        }
    }

    /// Capture a cut subtree's estimate at the point the exchange is replaced.
    pub fn estimate_of(plan: &Arc<dyn ExecutionPlan>) -> (Option<usize>, Option<usize>) {
        plan.partition_statistics(None).map_or((None, None), |s| {
            (
                Self::precision_value(&s.num_rows),
                Self::precision_value(&s.total_byte_size),
            )
        })
    }

    pub fn upstream_stage_index(&self) -> usize {
        self.upstream_stage_index
    }

    /// Copy of this read pointed at a different upstream stage.
    ///
    /// Used by stage reuse, which collapses identical stages and must then
    /// repoint every reader at the surviving index. Everything else — map-task
    /// count, partition count, schema, size estimates — is unchanged, because
    /// the stage being pointed at computes exactly the same thing; that
    /// identity is what made the collapse legal in the first place.
    pub(crate) fn clone_with_upstream_stage_index(&self, upstream_stage_index: usize) -> Self {
        Self {
            upstream_stage_index,
            num_map_tasks: self.num_map_tasks,
            schema: Arc::clone(&self.schema),
            properties: Arc::clone(&self.properties),
            reader: self.reader.clone(),
            upstream_rows: self.upstream_rows,
            upstream_bytes: self.upstream_bytes,
        }
    }

    pub fn num_map_tasks(&self) -> usize {
        self.num_map_tasks
    }

    pub fn partition_count(&self) -> usize {
        self.properties.partitioning.partition_count()
    }
}

impl DisplayAs for ShuffleReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ShuffleReadExec: upstream_stage={}, map_tasks={}, partitions={}",
            self.upstream_stage_index,
            self.num_map_tasks,
            self.partition_count()
        )
    }
}

impl ExecutionPlan for ShuffleReadExec {
    fn name(&self) -> &str {
        "ShuffleReadExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// D3(2): report the cut subtree's estimate instead of "unknown".
    ///
    /// `Inexact` is the honest precision — this is a planning-time estimate of
    /// a stage that has not run. Absent stays absent: a rule that keys on
    /// known-size (as `SpillableJoinSelection` deliberately does) must still
    /// be able to tell "no idea" from "small".
    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion::error::Result<Arc<datafusion::common::Statistics>> {
        use datafusion::common::stats::Precision;
        let partitions = self.properties.partitioning.partition_count().max(1);
        // A per-partition question gets the even-split share. Shuffle output is
        // hash-partitioned, so even split is the right null hypothesis; skew is
        // AQE's business and it works from measured sizes, not from this.
        let divisor = if partition.is_some() { partitions } else { 1 };
        let scale = |v: Option<usize>| -> Precision<usize> {
            v.map_or(Precision::Absent, |v| Precision::Inexact(v / divisor))
        };
        let mut stats = datafusion::common::Statistics::new_unknown(&self.schema);
        stats.num_rows = scale(self.upstream_rows);
        stats.total_byte_size = scale(self.upstream_bytes);
        Ok(Arc::new(stats))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let reader = self.reader.clone().ok_or_else(|| {
            DataFusionError::Execution(String::from(
                "ShuffleReadExec has no shuffle reader: this plan was decoded without an \
                 executor-side codec (coordinator-side plans are not executable)",
            ))
        })?;
        let stage = self.upstream_stage_index;
        let schema = Arc::clone(&self.schema);
        let expected = Arc::clone(&self.schema);
        let stream = futures::stream::iter(0..self.num_map_tasks)
            .map(move |map_task| {
                let reader = Arc::clone(&reader);
                async move {
                    reader
                        .open_partition(stage, map_task, partition)
                        .await
                        .map(|batches| (map_task, batches))
                        .map_err(|e| {
                            DataFusionError::Execution(format!(
                                "shuffle read (stage {stage}, map {map_task}, partition \
                                 {partition}): {e}"
                            ))
                        })
                }
            })
            // Open several map fragments at once instead of one at a time.
            //
            // This was `.then(..)`, which awaits each future before creating
            // the next: a reduce task fetched its fragment from map task 0,
            // waited for the whole round trip, then map task 1, and so on. With
            // 18 map tasks that is 18 strictly serial network round trips per
            // reduce partition, and a query with five shuffles pays it five
            // times over — all of it latency, none of it overlapped, while the
            // CPU that is meant to be joining sits idle.
            //
            // `buffered` keeps the output in map-task order, so the merge stays
            // deterministic, and bounds how many fragments are open. Because
            // `open_partition` streams, an open-but-not-yet-consumed fragment
            // costs a flow-control window rather than its full size.
            .buffered(shuffle_fetch_buffer())
            .map_ok(move |(map_task, batches)| {
                let expected = Arc::clone(&expected);
                batches.map(move |batch| {
                    let batch = batch.map_err(|e| {
                        DataFusionError::Execution(format!(
                            "shuffle read (stage {stage}, map {map_task}, partition \
                             {partition}): {e}"
                        ))
                    })?;
                    check_shuffle_batch_schema(&expected, batch, stage, map_task, partition)
                })
            })
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Reject a shuffle batch whose columns disagree with the schema this read
/// declares, naming the stage, map task, partition and column.
///
/// `ShuffleReadExec` labels its stream with the schema the **coordinator**
/// baked into the fragment, and `RecordBatchStreamAdapter` does not check that
/// the batches it yields match. So when a map stage produces something else,
/// nothing objects here — the rows flow on and die later, in whichever
/// downstream operator first builds a `RecordBatch` against the plan's schema,
/// as a bare Arrow error with no stage, no partition and no producer:
///
///   column types must match schema types, expected Decimal128(15, 2)
///   but found Decimal128(30, 15) at column index 0
///
/// That is TPC-H q17 at SF100, and it names nothing that identifies where the
/// disagreement came from. Checking at the seam turns it into an error that
/// does. This is the boundary between two independently-produced schemas, so
/// it is the only place with both of them in hand.
///
/// Deliberately narrow: column **count** and column **types** only. Those are
/// exactly what makes Arrow fail, and they cannot differ legitimately. Field
/// metadata and nullability can and do differ harmlessly across a Parquet read
/// and an IPC round trip, so comparing whole `Schema`s here would reject
/// correct queries.
fn check_shuffle_batch_schema(
    expected: &SchemaRef,
    batch: arrow::record_batch::RecordBatch,
    stage: usize,
    map_task: usize,
    partition: usize,
) -> Result<arrow::record_batch::RecordBatch, DataFusionError> {
    let actual = batch.schema();
    // Same allocation is the overwhelmingly common case — the map task and the
    // reader share it — so this costs a pointer compare per batch.
    if Arc::ptr_eq(expected, &actual) {
        return Ok(batch);
    }
    let where_ = || format!("stage {stage}, map {map_task}, partition {partition}");
    if expected.fields().len() != actual.fields().len() {
        return Err(DataFusionError::Execution(format!(
            "shuffle read ({}) produced {} columns but the plan declares {}; \
             the map stage did not produce the schema the reduce side was planned \
             against.\n  declared: {:?}\n  produced: {:?}",
            where_(),
            actual.fields().len(),
            expected.fields().len(),
            expected.fields(),
            actual.fields(),
        )));
    }
    for (index, (want, got)) in expected
        .fields()
        .iter()
        .zip(actual.fields().iter())
        .enumerate()
    {
        if want.data_type() != got.data_type() {
            return Err(DataFusionError::Execution(format!(
                "shuffle read ({}) column {index} ({}) is {:?} but the plan declares {:?} \
                 ({}); the map stage did not produce the schema the reduce side was \
                 planned against",
                where_(),
                got.name(),
                got.data_type(),
                want.data_type(),
                want.name(),
            )));
        }
    }
    Ok(batch)
}

// ── Extension codec ────────────────────────────────────────────────────────

/// Serialized form of a [`ShuffleReadExec`] inside the plan proto.
#[derive(serde::Serialize, serde::Deserialize)]
struct ShuffleReadNodePayload {
    v: u32,
    stage: usize,
    map_tasks: usize,
    partitions: usize,
    schema_ipc_b64: String,
    /// D3(2): planning-time estimate of the upstream stage. `#[serde(default)]`
    /// keeps `v: 1` fragments from an older coordinator decodable — they simply
    /// carry no estimate, which is the pre-fix behaviour and stays correct.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    upstream_rows: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    upstream_bytes: Option<usize>,
}

/// Serialized form of the runtime-filter nodes inside the plan proto.
///
/// Tagged on `node`, which [`ShuffleReadNodePayload`] does not carry — so the
/// decoder can tell the two apart by whether this parse succeeds, without a
/// version bump that would make new coordinators and old executors disagree
/// about a field they both already understand.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "node")]
enum KrishivNodePayload {
    RuntimeFilterBuild {
        key_index: usize,
        filter_bytes: usize,
    },
    RuntimeFilterProbe {
        key_index: usize,
    },
}

fn schema_to_ipc_bytes(schema: &arrow::datatypes::Schema) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, schema)
        .map_err(|e| format!("schema ipc writer: {e}"))?;
    writer
        .finish()
        .map_err(|e| format!("schema ipc finish: {e}"))?;
    Ok(buf)
}

fn schema_from_ipc_bytes(bytes: &[u8]) -> Result<SchemaRef, String> {
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| format!("schema ipc reader: {e}"))?;
    Ok(reader.schema())
}

/// Krishiv physical extension codec: (de)serializes [`ShuffleReadExec`].
///
/// The coordinator constructs it without a reader (encode only); the
/// executor constructs it with its shuffle reader so decoded plans execute.
#[derive(Debug, Default)]
pub struct KrishivPhysicalCodec {
    reader: Option<Arc<dyn ShufflePartitionReader>>,
}

impl KrishivPhysicalCodec {
    pub fn coordinator() -> Self {
        Self { reader: None }
    }

    pub fn executor(reader: Arc<dyn ShufflePartitionReader>) -> Self {
        Self {
            reader: Some(reader),
        }
    }
}

impl PhysicalExtensionCodec for KrishivPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        use crate::runtime_filter_exec::{RuntimeFilterBuildExec, RuntimeFilterProbeExec};
        // The runtime-filter nodes carry a `node` tag; a shuffle-read payload
        // does not, so this parse is an unambiguous discriminator.
        if let Ok(payload) = serde_json::from_slice::<KrishivNodePayload>(buf) {
            return match payload {
                KrishivNodePayload::RuntimeFilterBuild {
                    key_index,
                    filter_bytes,
                } => {
                    let [input] = inputs else {
                        return Err(DataFusionError::Internal(format!(
                            "RuntimeFilterBuildExec expects one input, got {}",
                            inputs.len()
                        )));
                    };
                    Ok(Arc::new(RuntimeFilterBuildExec::try_new(
                        Arc::clone(input),
                        key_index,
                        filter_bytes,
                    )?))
                }
                KrishivNodePayload::RuntimeFilterProbe { key_index } => {
                    let [data, filter] = inputs else {
                        return Err(DataFusionError::Internal(format!(
                            "RuntimeFilterProbeExec expects two inputs, got {}",
                            inputs.len()
                        )));
                    };
                    Ok(Arc::new(RuntimeFilterProbeExec::try_new(
                        Arc::clone(data),
                        Arc::clone(filter),
                        key_index,
                    )?))
                }
            };
        }
        let payload: ShuffleReadNodePayload = serde_json::from_slice(buf)
            .map_err(|e| DataFusionError::Internal(format!("shuffle-read node decode: {e}")))?;
        if payload.v != 1 {
            return Err(DataFusionError::Internal(format!(
                "unsupported shuffle-read node version {}",
                payload.v
            )));
        }
        let schema_bytes = base64::engine::general_purpose::STANDARD
            .decode(payload.schema_ipc_b64.as_bytes())
            .map_err(|e| DataFusionError::Internal(format!("shuffle-read schema b64: {e}")))?;
        let schema = schema_from_ipc_bytes(&schema_bytes).map_err(DataFusionError::Internal)?;
        Ok(Arc::new(
            ShuffleReadExec::new(
                payload.stage,
                payload.map_tasks,
                payload.partitions,
                schema,
                self.reader.clone(),
            )
            // D3(2): carry the estimate across the wire so the plan the
            // executor holds describes the same sizes the coordinator
            // optimized against. Absent on a fragment encoded by an older
            // coordinator, which reads exactly as it did before.
            .with_upstream_estimate(payload.upstream_rows, payload.upstream_bytes),
        ))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        use crate::runtime_filter_exec::{RuntimeFilterBuildExec, RuntimeFilterProbeExec};
        let filter_payload = if let Some(build) = node.downcast_ref::<RuntimeFilterBuildExec>() {
            Some(KrishivNodePayload::RuntimeFilterBuild {
                key_index: build.key_index(),
                filter_bytes: build.filter_bytes(),
            })
        } else {
            node.downcast_ref::<RuntimeFilterProbeExec>().map(|probe| {
                KrishivNodePayload::RuntimeFilterProbe {
                    key_index: probe.key_index(),
                }
            })
        };
        if let Some(payload) = filter_payload {
            let json = serde_json::to_vec(&payload).map_err(|e| {
                DataFusionError::Internal(format!("runtime filter node encode: {e}"))
            })?;
            buf.extend_from_slice(&json);
            return Ok(());
        }
        let read = node.downcast_ref::<ShuffleReadExec>().ok_or_else(|| {
            DataFusionError::NotImplemented(format!(
                "KrishivPhysicalCodec cannot encode node {}",
                node.name()
            ))
        })?;
        let schema_bytes = schema_to_ipc_bytes(&read.schema).map_err(DataFusionError::Internal)?;
        let (upstream_rows, upstream_bytes) = read.upstream_estimate();
        let payload = ShuffleReadNodePayload {
            v: 1,
            stage: read.upstream_stage_index,
            map_tasks: read.num_map_tasks,
            partitions: read.partition_count(),
            schema_ipc_b64: base64::engine::general_purpose::STANDARD.encode(&schema_bytes),
            upstream_rows,
            upstream_bytes,
        };
        let json = serde_json::to_vec(&payload)
            .map_err(|e| DataFusionError::Internal(format!("shuffle-read node encode: {e}")))?;
        buf.extend_from_slice(&json);
        Ok(())
    }
}

// ── Stage builder ──────────────────────────────────────────────────────────

/// Shuffle-output contract of a ShuffleMap stage.
#[derive(Debug, Clone)]
pub struct StageShuffleOutput {
    /// Hash-partitioning key columns (names in the stage output schema).
    pub key_columns: Vec<String>,
    /// Number of reduce partitions the map output is split into.
    pub num_output_partitions: usize,
}

/// One stage of a distributed batch plan.
#[derive(Debug, Clone)]
pub struct DistributedStage {
    /// Per-task fragment bodies (`dfplan:v1:<partition>:<b64>`), one per
    /// output partition of the stage subtree.
    pub task_bodies: Vec<String>,
    /// `Some` for ShuffleMap stages; `None` for the terminal Result stage.
    pub shuffle: Option<StageShuffleOutput>,
    /// Builder indexes of stages this stage reads via [`ShuffleReadExec`].
    pub upstream_stage_indexes: Vec<usize>,
}

impl DistributedStage {
    pub fn task_count(&self) -> usize {
        self.task_bodies.len()
    }
}

/// A batch query cut into shuffle-connected stages (Result stage last).
#[derive(Debug, Clone)]
pub struct DistributedStagePlan {
    pub stages: Vec<DistributedStage>,
}

struct StageDraft {
    plan: Arc<dyn ExecutionPlan>,
    shuffle: Option<StageShuffleOutput>,
    /// Set when this stage subtree was cut out from beneath a
    /// [`ScalarSubqueryExec`]. See [`StageSubqueryContext`].
    subqueries: Option<StageSubqueryContext>,
}

/// The uncorrelated-scalar-subquery context a stage subtree was cut out from.
///
/// DataFusion's physical planner wraps the WHOLE plan in a single
/// [`ScalarSubqueryExec`] at the root (`physical_planner.rs`,
/// `create_initial_plan`): that node runs each subquery once and stores the
/// scalar in a shared results container, and every `ScalarSubqueryExpr` left
/// in the plan reads its value out of that container by index.
///
/// Cutting the plan into stages severs that relationship. TPC-H q22's
/// `c_acctbal > (SELECT avg(c_acctbal) …)` sits in a filter *below* the hash
/// exchange, so the map stage ships the `ScalarSubqueryExpr` while the
/// `ScalarSubqueryExec` that populates it stays behind in the result stage.
/// The fragment encodes happily and then refuses to decode —
///
/// > ScalarSubqueryExpr can only be deserialized as part of a surrounding
/// > ScalarSubqueryExec
///
/// — which the caller reads as "decline to stage", running all of q22 as ONE
/// task. That is our stage cut breaking an invariant, not an upstream
/// serialization gap: `datafusion-proto` round-trips `ScalarSubqueryExec`
/// perfectly well, and re-establishes the container↔expr link on decode.
///
/// So a severed stage is repaired by giving it back the wrapper it lost. The
/// links are carried here, uncut, and re-applied to whichever stages actually
/// need them (see `build_distributed_stages`).
struct StageSubqueryContext {
    /// The subquery plans, exactly as planned — never cut into stages.
    ///
    /// `ScalarSubqueryExec` evaluates each through
    /// `execute_stream`, which coalesces the plan to one partition and runs it
    /// whole. A subquery containing a `ShuffleReadExec` would therefore have a
    /// task read a sibling stage's output out of dependency order, so
    /// `cut_exchanges` deliberately does not descend into them.
    links: Vec<ScalarSubqueryLink>,
    /// The root exec's results container.
    ///
    /// Shared rather than freshly allocated so the coordinator-side plan stays
    /// internally consistent (its exprs hold this same container). Identity is
    /// irrelevant to what executors run: decoding a fragment mints a fresh
    /// container and wires that stage's exprs to it.
    results: ScalarSubqueryResults,
}

/// Internal marker for shapes the builder cannot prove correct.
struct Unsupported(String);

/// Cut a physical plan into shuffle-connected stages.
///
/// Returns `Ok(None)` when the plan has no hash exchange (nothing to gain)
/// or uses a shape the builder cannot prove correct (fallback to the
/// single-task path). The result stage is always last; map stages appear in
/// dependency order before it.
pub fn build_distributed_stages(
    plan: Arc<dyn ExecutionPlan>,
) -> SqlResult<Option<DistributedStagePlan>> {
    build_distributed_stages_with_udf_directives(plan, "")
}

/// See `build_distributed_stages`; `udf_directive_source` supplies the query's
/// inline Python-UDF directives so the decode rehearsal can resolve them.
pub fn build_distributed_stages_with_udf_directives(
    plan: Arc<dyn ExecutionPlan>,
    udf_directive_source: &str,
) -> SqlResult<Option<DistributedStagePlan>> {
    // Before cutting: a broadcast join whose unmatched build rows are emitted
    // only after the last probe partition cannot be split one-partition-per-task
    // without silently dropping those rows. Convert such joins to
    // hash-partitioned ones, which are split-safe by construction.
    let plan = redistribute_unsplittable_broadcast_joins(plan)?;

    // Re-run the spillable-join rule over the rewritten plan.
    //
    // This is a sequencing fix, not a belt-and-braces repeat. `SpillableJoinSelection`
    // is a physical optimizer rule, so it ran BEFORE the rewrite above — when
    // q21's joins were still `CollectLeft` over a multi-partition probe, a shape
    // it declines by design (`convertible_mode`). The rewrite then turns them
    // into `Partitioned` joins whose build side each task must hold as a hash
    // table, and nothing had re-examined whether it fits.
    //
    // Measured: with the rewrite but without this pass, q21 at SF100 stopped
    // being slow and started FAILING — `Resources exhausted: HashJoinInput[4]
    // with 806.0 MB already allocated` out of a 2.6 GB pool. One degenerate
    // statistic was poisoning two decisions; fixing only the first turned a slow
    // query into a broken one.
    let plan = {
        use datafusion::physical_optimizer::PhysicalOptimizerRule as _;
        crate::spillable_join::SpillableJoinSelection::from_capacity()
            // This plan is about to be cut into stages, so an added exchange is
            // an added stage boundary — see `without_broadcast_rescue`.
            .without_broadcast_rescue()
            .optimize(plan, &datafusion::common::config::ConfigOptions::default())
    }
    .map_err(|e| SqlError::DataFusion {
        message: format!("spillable-join pass over the redistributed plan: {e}"),
    })?;

    let mut drafts: Vec<StageDraft> = Vec::new();
    let mut root = match cut_exchanges(plan, &mut drafts) {
        Ok(root) => root,
        Err(Unsupported(reason)) => {
            return Err(SqlError::DataFusion {
                message: format!("stage split unsupported: {reason}"),
            });
        }
    };
    if drafts.is_empty() {
        return Err(SqlError::DataFusion {
            message: String::from("plan has no exchange to cut, so it cannot be split into stages"),
        });
    }
    // Collapse identical leaf stages FIRST, so the runtime-filter rule sees the
    // deduplicated stage list and cannot build a filter for a stage that is
    // about to be removed (and thereby leave a dangling upstream index).
    // A no-op unless `KRISHIV_STAGE_REUSE` is on.
    dedupe_identical_stages(&mut root, &mut drafts);

    // Cross-stage runtime filters, before the root is pushed so the Result
    // stage stays last. Appends filter stages and rewrites probe stages in
    // place; a no-op unless `KRISHIV_CROSS_STAGE_RUNTIME_FILTER` is on.
    inject_runtime_filters(&root, &mut drafts);

    drafts.push(StageDraft {
        plan: root,
        shuffle: None,
        // The root keeps whatever `ScalarSubqueryExec` it was planned with, so
        // it is never the severed side.
        subqueries: None,
    });

    // Prove every stage subtree is partition-independent: no exchange may
    // remain inside a stage (each task executes one root partition; a
    // leftover RepartitionExec would re-drive all inputs per task).
    for draft in &drafts {
        if let Some(reason) = find_unsupported_stage_node(&draft.plan) {
            return Err(SqlError::DataFusion {
                message: format!("stage subtree not partition-independent: {reason}"),
            });
        }
    }

    let codec = KrishivPhysicalCodec::coordinator();
    // One executor-equivalent decode context for the whole query: building a
    // `SqlEngine` registers the full UDF set, and every stage rehearses against
    // the same one the executor would use (A5).
    let decode_session = fragment_decode_session_context();
    if !udf_directive_source.is_empty() {
        register_python_udf_signatures_and_strip(&decode_session, udf_directive_source)?;
    }
    let decode_ctx = decode_session.task_ctx();
    let mut stages = Vec::with_capacity(drafts.len());
    for draft in drafts {
        let partition_count = draft.plan.output_partitioning().partition_count();
        if partition_count == 0 {
            return Err(SqlError::DataFusion {
                message: String::from("stage subtree has zero output partitions"),
            });
        }
        let upstream_stage_indexes = collect_upstream_stage_indexes(&draft.plan);
        // Encoding successfully is not the same as being shippable — a fragment
        // can encode and then fail to decode on the executor. Rehearse the
        // decode locally (same codec, same object-store registry as the
        // executor's runtime) so an encode/decode asymmetry degrades to
        // correct-but-serial execution instead of a remote fragment failure.
        //
        // A stage cut out from beneath a `ScalarSubqueryExec` gets two attempts:
        // bare first, then wrapped. Trying bare first is what keeps the repair
        // precise — only the stage that genuinely carries a `ScalarSubqueryExpr`
        // pays to re-evaluate the subquery, and the rest of the query's stages
        // are shipped exactly as before. There is no generic way to ask a
        // physical plan "do you contain this expression" (`ExecutionPlan` has no
        // expression accessor), and the decoder's own answer is the
        // authoritative one anyway.
        let attempts = match &draft.subqueries {
            Some(context) => vec![
                Arc::clone(&draft.plan),
                wrap_in_scalar_subquery_exec(Arc::clone(&draft.plan), context),
            ],
            None => vec![Arc::clone(&draft.plan)],
        };
        let mut shippable = None;
        let mut last_error = None;
        for stage_plan in attempts {
            let bytes = match encode_dfplan_bytes(Arc::clone(&stage_plan), &codec) {
                Ok(bytes) => bytes,
                Err(error) => {
                    // Plans over non-serializable providers (memory tables,
                    // custom scans) fall back rather than fail the query.
                    last_error = Some(error.to_string());
                    continue;
                }
            };
            match verify_dfplan_roundtrip(&bytes, &codec, &decode_ctx, Some(&stage_plan)) {
                Ok(()) => {
                    shippable = Some(bytes);
                    break;
                }
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        let Some(bytes) = shippable else {
            // At `warn`: declining to distribute is not a detail, it is the
            // difference between a query using the cluster and one task
            // scanning the whole table. This was `debug` on a coordinator that
            // runs at `info`, so TPC-H q22 quietly ran serially for three
            // sweeps with nothing in the logs saying why.
            tracing::warn!(
                error = %last_error.unwrap_or_else(|| String::from("unknown")),
                "stage plan cannot be encoded and decoded; running this query as a SINGLE TASK"
            );
            return Ok(None);
        };
        let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let task_bodies = (0..partition_count)
            .map(|p| dfplan_task_body(&b64, p))
            .collect();
        stages.push(DistributedStage {
            task_bodies,
            shuffle: draft.shuffle,
            upstream_stage_indexes,
        });
    }
    Ok(Some(DistributedStagePlan { stages }))
}

/// Identity of a cut subtree, for deciding whether two exchanges can share one
/// stage.
///
/// # Why reuse is worth having
///
/// The cutter gave every exchange its own stage, so a subtree feeding two
/// consumers was scanned and shuffled twice. TPC-H q21 shuffles
/// `["l_orderkey","l_suppkey"]` in **three** separate stages — one per
/// EXISTS/NOT EXISTS self-join over `lineitem` — and q7 shuffles the **25-row**
/// `nation` table in two. On a cluster whose pod network is the binding
/// constraint, a redundant shuffle is redundant wire time. Spark calls this
/// `ReusedExchange`.
///
/// # Why this key is the whole safety argument
///
/// A false match merges two stages that are *not* equivalent, and the consumers
/// then read someone else's rows — a wrong answer with no error. So the key is
/// the full indented physical plan of the subtree, which renders operators,
/// projections, filter predicates and scanned file groups, plus the schema and
/// the shuffle's own key columns and partition count (compared separately by
/// the caller). Anything that changes what the stage *emits* changes this
/// string.
///
/// Deliberately conservative in two ways:
///
/// * Stages carrying a [`StageSubqueryContext`] are never reused. Their tasks
///   are parameterised by a subquery result, so identical plan text does not
///   imply identical output.
/// * It matches on rendered text rather than pointer identity, so it finds the
///   real duplicates (separately-planned subqueries) rather than only shared
///   `Arc`s — which is the case that actually occurs.
///
/// The empirical guard is `every_tpch_query_stages_to_the_same_answer_in_every_configuration`
/// in krishiv-bench: all 22 queries, four join/broadcast configurations, staged
/// through this cutter and compared against single-node execution.
fn exchange_reuse_key(
    plan: &Arc<dyn ExecutionPlan>,
    key_columns: &[String],
    num_partitions: usize,
) -> String {
    use datafusion::physical_plan::displayable;
    format!(
        "keys={key_columns:?}|parts={num_partitions}|schema={:?}|plan=\n{}",
        plan.schema(),
        displayable(plan.as_ref()).indent(true)
    )
}

/// Largest `fetch` for which a `SortPreservingMergeExec` is worth turning into
/// a gather + re-sort.
///
/// Cutting the merge buys distribution of everything beneath it, and costs
/// buffering `partitions x fetch` rows in the stage that re-sorts them. At
/// TPC-H's fetches (10-100) over 18 partitions that is under two thousand
/// rows. A very large `LIMIT` inverts the trade: the merge would stream, the
/// replacement would buffer, and the subtree below is usually cheap anyway —
/// so those keep the streaming k-way merge they were planned with.
const MAX_GATHERED_SORT_FETCH: usize = 10_000;

/// Does this subtree contain a hash-partitioned join?
///
/// The signal that a gather is stranding real distributed work. A
/// `Partitioned` join exists *because* N tasks should each handle one
/// partition; finding one below a gather means the plan paid for the shuffle
/// and is about to throw the parallelism away. A `CollectLeft` join says
/// nothing — its build side was chosen to be small and a single probe
/// partition may be entirely correct.
fn contains_partitioned_join(plan: &Arc<dyn ExecutionPlan>) -> bool {
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    if let Some(join) = plan.downcast_ref::<HashJoinExec>()
        && *join.partition_mode() == PartitionMode::Partitioned
    {
        return true;
    }
    plan.children()
        .iter()
        .any(|child| contains_partitioned_join(child))
}

fn cut_exchanges(
    plan: Arc<dyn ExecutionPlan>,
    stages: &mut Vec<StageDraft>,
) -> Result<Arc<dyn ExecutionPlan>, Unsupported> {
    if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
        let Partitioning::Hash(exprs, num_partitions) = repartition.partitioning() else {
            return Err(Unsupported(format!(
                "non-hash exchange in plan: {}",
                repartition.partitioning()
            )));
        };
        let key_columns = hash_expr_column_names(exprs).ok_or_else(|| {
            Unsupported(String::from(
                "hash exchange uses non-column expressions; cannot derive shuffle keys",
            ))
        })?;
        let input = cut_exchanges(Arc::clone(repartition.input()), stages)?;
        let map_task_count = input.output_partitioning().partition_count();
        if map_task_count == 0 {
            return Err(Unsupported(String::from("hash exchange over empty input")));
        }
        let schema = input.schema();
        // D3(2): capture the estimate before `input` is moved into the stage —
        // this is the only point where the cut subtree is still in hand.
        let estimate = ShuffleReadExec::estimate_of(&input);

        // Exchange reuse: an identical subtree shuffled on identical keys into
        // identical partitions produces byte-identical output, so cut it once
        // and let both consumers read the same stage.
        let reuse_key = exchange_reuse_key(&input, &key_columns, *num_partitions);
        let stage_index = match stages.iter().position(|draft| {
            draft.subqueries.is_none()
                && draft.shuffle.as_ref().is_some_and(|sh| {
                    sh.key_columns == key_columns && sh.num_output_partitions == *num_partitions
                })
                && exchange_reuse_key(&draft.plan, &key_columns, *num_partitions) == reuse_key
        }) {
            Some(existing) => existing,
            None => {
                let index = stages.len();
                stages.push(StageDraft {
                    plan: input,
                    shuffle: Some(StageShuffleOutput {
                        key_columns,
                        num_output_partitions: *num_partitions,
                    }),
                    subqueries: None,
                });
                index
            }
        };
        return Ok(Arc::new(
            ShuffleReadExec::new(stage_index, map_task_count, *num_partitions, schema, None)
                .with_upstream_estimate(estimate.0, estimate.1),
        ));
    }

    // A gather (N partitions -> 1) is an exchange too, and cutting it is what
    // makes ungrouped aggregates distributable. `SELECT sum(x) FROM lineitem`
    // plans as Final(gather(Partial(scan))) with no hash exchange anywhere, so
    // a cutter that only recognised RepartitionExec declined the whole query
    // and one executor scanned the entire table — 518 s for TPC-H q6 at SF100
    // on a 3-node cluster, with the other two nodes idle.
    //
    // Cutting here puts the Partial aggregate in a map stage (one task per file
    // group, running everywhere) and the Final aggregate in a reduce stage
    // reading a single shuffle partition. The shuffle writer already routes
    // every row to partition 0 when no key column is given, so a keyless
    // 1-partition output is exactly a gather.
    if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
        let input = cut_exchanges(Arc::clone(coalesce.input()), stages)?;
        let map_task_count = input.output_partitioning().partition_count();
        if map_task_count <= 1 {
            // Nothing to spread: a one-partition input gathers to itself, and
            // a stage boundary here would add a shuffle round trip for no
            // parallelism. Keep the node as-is.
            return plan
                .with_new_children(vec![input])
                .map_err(|e| Unsupported(format!("gather rewrite: {e}")));
        }
        let schema = input.schema();
        // D3(2): as in the hash-exchange arm, capture before the move.
        let estimate = ShuffleReadExec::estimate_of(&input);
        let stage_index = stages.len();
        stages.push(StageDraft {
            plan: input,
            shuffle: Some(StageShuffleOutput {
                key_columns: Vec::new(),
                num_output_partitions: 1,
            }),
            subqueries: None,
        });
        // The read replaces the whole gather: coalesce(N->1) and
        // shuffle(N->1)+read(partition 0) produce the same single stream, and
        // CoalescePartitionsExec carries no ordering guarantee to preserve.
        return Ok(Arc::new(
            ShuffleReadExec::new(stage_index, map_task_count, 1, schema, None)
                .with_upstream_estimate(estimate.0, estimate.1),
        ));
    }

    // A `SortPreservingMergeExec` with a fetch is a **bounded** gather, and not
    // cutting it is what leaves the whole query's real work in a one-task stage.
    //
    // TPC-H q3 at SF100, measured 2026-07-30: the plan is
    //
    //   SortPreservingMerge(fetch=10)          <- 1 partition
    //     SortExec TopK(fetch=10)              <- 18 partitions
    //       Aggregate(SinglePartitioned)       <- 18 partitions
    //         HashJoin(Partitioned)            <- 18 partitions
    //           ShuffleRead(stage 1) / ShuffleRead(stage 2)
    //
    // The only exchanges are the two `RepartitionExec`s *below* the join, so
    // everything above them became one stage — and because the merge outputs a
    // single partition, that stage got exactly **one task**. One executor then
    // ran the entire 18-partition join and aggregate by itself and pulled both
    // shuffles (13.2 GB) across an ~11 MiB/s pod network
    // ([[bench-storage-longhorn-bottleneck]]): 13.2 GB / 11 MiB/s is ~20
    // minutes, which is exactly what the live run showed, with 8 of 9 slots
    // idle throughout.
    //
    // Cutting here puts the join, the aggregate and the per-partition TopK in a
    // real 18-task stage; only each partition's `fetch` rows cross the wire.
    //
    // **Only when `fetch` is set.** `SortPreservingMerge` streams a k-way merge
    // of already-sorted inputs; the replacement is a blocking `SortExec`, which
    // buffers what it gathers. With a fetch that is bounded by
    // `partitions x fetch` (180 rows at q3's shape) and the sort is trivial.
    // Without one it would buffer the entire result to re-sort rows that were
    // already sorted — trading a distribution win for an unbounded memory
    // liability, so those merges are left exactly as they are.
    if let Some(merge) = plan.downcast_ref::<SortPreservingMergeExec>() {
        let input = cut_exchanges(Arc::clone(merge.input()), stages)?;
        let fetch = merge.fetch();
        // Two ways this merge is worth cutting:
        //
        //  * it has a small `fetch`, so the gather is bounded by
        //    `partitions x fetch` and the re-sort is trivial; or
        //  * there is no fetch, but a **hash-partitioned join** sits below it,
        //    which is the q11/q15/q20 shape: without a cut, one task performs
        //    that whole join. Buffering the gathered rows is worth it because
        //    what reaches the final task is the join's *output*, while the
        //    uncut plan drags the join's much larger *inputs* to a single node.
        //
        // Anything else keeps the streaming k-way merge it was planned with:
        // an unbounded gather + blocking sort over a subtree that was never
        // distributed in the first place buys nothing and risks a large spill.
        let worth_cutting = match fetch {
            Some(n) => n <= MAX_GATHERED_SORT_FETCH,
            None => contains_partitioned_join(&input),
        };
        if !worth_cutting {
            return plan
                .with_new_children(vec![input])
                .map_err(|e| Unsupported(format!("sort-merge passthrough: {e}")));
        }
        let map_task_count = input.output_partitioning().partition_count();
        if map_task_count <= 1 {
            // Already a single stream: a stage boundary here would add a
            // shuffle round trip and buy no parallelism.
            return plan
                .with_new_children(vec![input])
                .map_err(|e| Unsupported(format!("sort-merge gather rewrite: {e}")));
        }
        let schema = input.schema();
        let estimate = ShuffleReadExec::estimate_of(&input);
        let stage_index = stages.len();
        stages.push(StageDraft {
            plan: input,
            shuffle: Some(StageShuffleOutput {
                key_columns: Vec::new(),
                num_output_partitions: 1,
            }),
            subqueries: None,
        });
        let read = Arc::new(
            ShuffleReadExec::new(stage_index, map_task_count, 1, schema, None)
                .with_upstream_estimate(estimate.0, estimate.1),
        );
        // The gather loses the cross-partition ordering the merge guaranteed,
        // so re-establish it. Sorting the gathered rows is equivalent: every
        // upstream partition already emitted its own sorted run (its top-`fetch`
        // when there is a fetch), so a sort with the same expressions and the
        // same fetch yields the same rows in the same order.
        return Ok(Arc::new(
            SortExec::new(merge.expr().clone(), read).with_fetch(fetch),
        ));
    }

    // An uncorrelated scalar subquery is not an exchange, but it is a boundary:
    // `ScalarSubqueryExec::children()` returns `[main_input, subquery…]`, and
    // the generic recursion below would treat a subquery plan as ordinary
    // pipeline and cut it. It must not: a subquery runs *whole* inside whatever
    // task evaluates it, so a `ShuffleReadExec` left in one would read a
    // sibling stage's output out of dependency order.
    //
    // Cut only the main input, and record the subquery context on every stage
    // that came out of it — those are exactly the stages that may have been
    // severed from the wrapper their `ScalarSubqueryExpr` nodes need. See
    // [`StageSubqueryContext`].
    if let Some(subquery_exec) = plan.downcast_ref::<ScalarSubqueryExec>() {
        let first_new_stage = stages.len();
        let input = cut_exchanges(Arc::clone(subquery_exec.input()), stages)?;
        let context = || StageSubqueryContext {
            links: subquery_exec.subqueries().to_vec(),
            results: subquery_exec.results().clone(),
        };
        if let Some(new_stages) = stages.get_mut(first_new_stage..) {
            for draft in new_stages {
                // Nested levels compose: an inner exec records its own
                // subqueries first, and only stages with no context yet belong
                // to this level.
                draft.subqueries.get_or_insert_with(context);
            }
        }
        // Rebuild in `children()` order: main input first, subqueries after.
        let mut children = Vec::with_capacity(subquery_exec.subqueries().len() + 1);
        children.push(input);
        children.extend(
            subquery_exec
                .subqueries()
                .iter()
                .map(|link| Arc::clone(&link.plan)),
        );
        return plan
            .with_new_children(children)
            .map_err(|e| Unsupported(format!("scalar-subquery rewrite: {e}")));
    }

    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }
    let mut new_children = Vec::with_capacity(children.len());
    let mut changed = false;
    for child in children {
        let rewritten = cut_exchanges(Arc::clone(child), stages)?;
        changed = changed || !Arc::ptr_eq(&rewritten, child);
        new_children.push(rewritten);
    }
    if !changed {
        return Ok(plan);
    }
    plan.with_new_children(new_children)
        .map_err(|e| Unsupported(format!("plan rewrite: {e}")))
}

/// Join types whose unmatched BUILD-side rows are emitted only after every
/// probe partition has been seen.
///
/// `HashJoinExec` tracks which build rows matched in a shared bitmap and emits
/// the unmatched ones from whichever probe partition finishes last
/// (`report_probe_completed`). Everything else streams straight through from
/// the probe side and needs no such rendezvous.
fn emits_unmatched_build_rows(join_type: datafusion::logical_expr::JoinType) -> bool {
    use datafusion::logical_expr::JoinType;
    matches!(
        join_type,
        JoinType::Left
            | JoinType::LeftAnti
            | JoinType::LeftSemi
            | JoinType::LeftMark
            | JoinType::Full
    )
}

/// Is this join a broadcast join that cannot survive being split across tasks?
///
/// `PartitionMode::CollectLeft` sizes its probe-completion counter from the
/// PLAN's probe partition count (`hash_join/exec.rs`: `probe_threads_count =
/// self.right().output_partitioning().partition_count()`). A distributed task
/// executes exactly ONE partition of that plan, so the counter is decremented
/// once and never reaches "last probe" — and the unmatched build rows are
/// never emitted at all.
///
/// That is a silent wrong answer, not an error: TPC-H q22's `NOT EXISTS`
/// anti-join returned ZERO rows per task, and nothing in the plan, the logs or
/// the schema said so. `PartitionMode::Partitioned` passes `1` for the same
/// counter — each task owns a disjoint hash range and is its own last probe —
/// which is why the fix is to convert rather than to decline.
///
/// A single-partition probe side is safe as it stands: the count is already 1.
fn is_unsplittable_broadcast_join(join: &datafusion::physical_plan::joins::HashJoinExec) -> bool {
    use datafusion::physical_plan::joins::PartitionMode;
    *join.partition_mode() == PartitionMode::CollectLeft
        && emits_unmatched_build_rows(*join.join_type())
        && join.right().output_partitioning().partition_count() > 1
}

/// Does this build side's estimate say the relation is **empty**?
///
/// Not "small" — empty. This is deliberately the narrowest possible
/// disagreement with DataFusion, and the narrowness is the whole design.
///
/// DataFusion's `supports_collect_by_thresholds` asks `estimate < ceiling`,
/// which a degenerate estimate of **zero** passes more convincingly than any
/// real small table. TPC-H q21 at SF100, verbatim: its `NOT EXISTS` becomes a
/// `LeftAnti` join whose two sides are both `lineitem` on `l_orderkey`, so
/// `estimate_join_statistics` computes `outer_rows - semi_estimate` =
/// `593462145 - 593462145` = **0 rows, 0 bytes** (`joins/utils.rs`, the
/// semi/anti arm). The real output is tens of millions of rows. Three
/// `CollectLeft` joins stacked above it each believed they were broadcasting
/// nothing, so the stage cutter gathered that intermediate to ONE partition
/// three times over (`shuffle=([], 1)` in the stage dump) and the whole top
/// half of the query ran on a single task.
///
/// # Why this does not also enforce a ceiling
///
/// It used to, and that was a **measured regression**. An earlier version of
/// this rule demanded a positive estimate *below the ceiling*, which converted
/// q8's and q9's `CollectLeft` build sides — estimated at `rows=~4000000,
/// bytes=absent`, i.e. above the 1M row ceiling but perfectly plausible. On the
/// cluster q8 went 92 s -> 375 s and q9 226 s -> 576 s, because the alternative
/// to broadcasting those few million rows is hash-partitioning the 600M-row
/// `lineitem` scan across an ~11 MiB/s pod network. That ceiling is DataFusion's
/// decision to make and it was already made with the numbers this rule can see;
/// second-guessing it lost more than the q21 bug cost.
///
/// So: a positive estimate is trusted, however large. Only "the planner thinks
/// there is nothing here" is overridden — because for a non-empty relation that
/// is not a measurement, it is the estimator giving up. A genuinely empty
/// relation pays one extra pair of hash exchanges over an empty stream.
fn broadcast_build_estimate_is_empty(
    join: &datafusion::physical_plan::joins::HashJoinExec,
) -> bool {
    // One shared reading of the statistics — see `crate::join_estimates` for
    // why this is not two hand-rolled matches any more, and for why the
    // broadcast override and the spill choice are allowed to want different
    // things from the same numbers.
    crate::join_estimates::BuildSideEstimate::of(join.left()).is_wholly_degenerate()
}

/// Is this a broadcast join chosen on an estimate that says its build side is
/// empty when it is not?
///
/// Distinct from [`is_unsplittable_broadcast_join`], which is a *correctness*
/// test. This one is about throughput, and the two must stay separate: the
/// correctness test also gates [`find_unsupported_stage_node`], which refuses
/// to stage a plan rather than return wrong rows. Folding a performance
/// heuristic into that gate would turn a merely slow plan into a query that
/// declines to distribute at all.
///
/// Only fires where there is parallelism to lose: if both the probe side and
/// the build side's own input are already single-partition, the gather costs
/// nothing and the exchanges would be pure overhead.
fn is_degenerate_broadcast_join(join: &datafusion::physical_plan::joins::HashJoinExec) -> bool {
    use datafusion::physical_plan::joins::PartitionMode;

    if *join.partition_mode() != PartitionMode::CollectLeft {
        return false;
    }
    // A null-aware anti join tracks probe-side state across the whole build and
    // is only correct as `CollectLeft` (DataFusion rejects any other mode for
    // it at construction). Never convert one.
    if join.null_aware {
        return false;
    }
    if !broadcast_build_estimate_is_empty(join) {
        return false;
    }
    let build_input_partitions = match join.left().downcast_ref::<CoalescePartitionsExec>() {
        Some(coalesce) => coalesce.input().output_partitioning().partition_count(),
        None => join.left().output_partitioning().partition_count(),
    };
    join.right().output_partitioning().partition_count() > 1 || build_input_partitions > 1
}

/// The broadcast byte ceiling, resolved exactly as
/// [`planning_session_context_with_options`] resolves it.
///
/// Read here rather than threaded through because this pass runs after
/// planning, where the `SessionConfig` is gone. A caller that overrode the
/// ceiling to 0 ("never broadcast") cannot be affected: DataFusion then never
/// picks `CollectLeft`, so nothing downstream of this can fire.
fn broadcast_byte_ceiling() -> usize {
    std::env::var(BROADCAST_JOIN_BYTES_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_BROADCAST_JOIN_BYTES)
}

/// Is this a broadcast whose build side is *wide* enough that the row ceiling
/// admitted something the byte ceiling would have refused?
///
/// `supports_collect_by_thresholds` prefers `total_byte_size` and falls back to
/// `num_rows`. Above a shuffle boundary the byte estimate is frequently absent —
/// `ShuffleReadExec` reports what the cut subtree reported, and DataFusion loses
/// `total_byte_size` through joins and aggregates — so the decision lands on the
/// row ceiling, which cannot see how wide a row is. At q10's customer shape
/// (~180 B/row) the 1,000,000-row ceiling admits ~155 MB, five times the 32 MiB
/// byte ceiling it stands in for, and `CollectLeft` then copies that to *every*
/// task of the stage. That is the shape of q10 moving ~63 GB in 13.5 minutes
/// against a plan implying ~3.5 GB of shuffle.
///
/// Narrow on purpose, because widening it is what regressed four queries
/// (see [`crate::join_estimates`]):
///
/// * Only fires where DataFusion *already chose* `CollectLeft`. A build side it
///   left partitioned — q8/q9/q17, whose row counts are far over the ceiling —
///   is never examined.
/// * Only when the byte estimate is **absent**. A positive byte estimate means
///   DataFusion decided on a real number with the same information, and
///   overriding that is precisely the regression.
fn broadcast_build_is_too_wide(join: &datafusion::physical_plan::joins::HashJoinExec) -> bool {
    use datafusion::physical_plan::joins::PartitionMode;

    if *join.partition_mode() != PartitionMode::CollectLeft {
        return false;
    }
    // Null-aware anti joins are only correct as `CollectLeft` — never convert.
    if join.null_aware {
        return false;
    }
    let build = join.left();
    let Some(implied) =
        crate::join_estimates::BuildSideEstimate::of(build).bytes_implied_by_rows(&build.schema())
    else {
        return false;
    };
    implied > broadcast_byte_ceiling()
}

/// Convert broadcast joins that cannot be split — or that were chosen on an
/// estimate claiming their build side is empty, or on a row ceiling blind to
/// how wide those rows are — into hash-partitioned joins (see
/// [`is_unsplittable_broadcast_join`], [`is_degenerate_broadcast_join`] and
/// [`broadcast_build_is_too_wide`]).
///
/// Both sides gain a hash exchange on the join keys, which the stage cutter
/// then turns into ordinary map stages — so the join keeps running across the
/// cluster instead of being declined back to a single task.
///
/// The build side's `CoalescePartitionsExec` is dropped when present: it exists
/// only to satisfy `CollectLeft`'s `Distribution::SinglePartition` requirement,
/// and keeping it would funnel the whole build side through one partition
/// before re-splitting it.
pub fn redistribute_unsplittable_broadcast_joins(
    plan: Arc<dyn ExecutionPlan>,
) -> SqlResult<Arc<dyn ExecutionPlan>> {
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

    // Bottom-up: children are rewritten before the node that joins them, so a
    // converted child's new partitioning is what the parent sees.
    let children = plan.children();
    let plan = if children.is_empty() {
        plan
    } else {
        let mut new_children = Vec::with_capacity(children.len());
        let mut changed = false;
        for child in children {
            let rewritten = redistribute_unsplittable_broadcast_joins(Arc::clone(child))?;
            changed = changed || !Arc::ptr_eq(&rewritten, child);
            new_children.push(rewritten);
        }
        if changed {
            plan.with_new_children(new_children)
                .map_err(|e| SqlError::DataFusion {
                    message: format!("broadcast-join redistribution rewrite: {e}"),
                })?
        } else {
            plan
        }
    };

    let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
        return Ok(plan);
    };
    let unsplittable = is_unsplittable_broadcast_join(join);
    if !unsplittable && !is_degenerate_broadcast_join(join) && !broadcast_build_is_too_wide(join) {
        return Ok(plan);
    }

    let partitions = join.right().output_partitioning().partition_count();
    let (left_keys, right_keys): (Vec<_>, Vec<_>) = join
        .on()
        .iter()
        .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
        .unzip();

    let build_side = match join.left().downcast_ref::<CoalescePartitionsExec>() {
        Some(coalesce) => Arc::clone(coalesce.input()),
        None => Arc::clone(join.left()),
    };
    let exchange = |input: Arc<dyn ExecutionPlan>,
                    keys: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>|
     -> SqlResult<Arc<dyn ExecutionPlan>> {
        RepartitionExec::try_new(input, Partitioning::Hash(keys, partitions))
            .map(|r| Arc::new(r) as Arc<dyn ExecutionPlan>)
            .map_err(|e| SqlError::DataFusion {
                message: format!("broadcast-join redistribution exchange: {e}"),
            })
    };

    let converted = join
        .builder()
        .with_new_children(vec![
            exchange(build_side, left_keys)?,
            exchange(Arc::clone(join.right()), right_keys)?,
        ])
        .and_then(|b| {
            b.with_partition_mode(PartitionMode::Partitioned)
                .recompute_properties()
                .reset_state()
                .build_exec()
        })
        .map_err(|e| SqlError::DataFusion {
            message: format!("broadcast-join redistribution rebuild: {e}"),
        })?;
    tracing::debug!(
        join_type = ?join.join_type(),
        partitions,
        reason = if unsplittable { "unsplittable" } else { "oversized" },
        "converted a broadcast join to a hash-partitioned join"
    );
    Ok(converted)
}

/// Give a severed stage subtree back the [`ScalarSubqueryExec`] wrapper its
/// `ScalarSubqueryExpr` nodes need in order to decode and to resolve.
///
/// A pass-through node: it reports its input's partitioning and statistics
/// verbatim, so wrapping changes neither the stage's task count nor its
/// shuffle keys — only whether the fragment can be rebuilt on an executor.
fn wrap_in_scalar_subquery_exec(
    plan: Arc<dyn ExecutionPlan>,
    context: &StageSubqueryContext,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(ScalarSubqueryExec::new(
        plan,
        context.links.clone(),
        context.results.clone(),
    ))
}

/// Extract plain column names from hash-partitioning expressions.
fn hash_expr_column_names(
    exprs: &[Arc<dyn datafusion::physical_expr::PhysicalExpr>],
) -> Option<Vec<String>> {
    use datafusion::physical_expr::expressions::Column;
    let mut names = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let column = (expr.as_ref() as &dyn std::any::Any).downcast_ref::<Column>()?;
        names.push(column.name().to_owned());
    }
    (!names.is_empty()).then_some(names)
}

/// Detect nodes that break the task-per-partition execution model.
fn find_unsupported_stage_node(plan: &Arc<dyn ExecutionPlan>) -> Option<String> {
    if plan.is::<RepartitionExec>() {
        return Some(String::from("RepartitionExec inside stage subtree"));
    }
    // The safety net behind `redistribute_unsplittable_broadcast_joins`. If a
    // broadcast join that emits unmatched build rows ever reaches a stage
    // subtree unconverted, declining to stage is the only correct outcome:
    // shipping it returns the wrong ANSWER rather than an error, and a wrong
    // answer that looks like a clean pass is the worst failure this builder
    // can produce.
    if let Some(join) = plan.downcast_ref::<datafusion::physical_plan::joins::HashJoinExec>()
        && is_unsplittable_broadcast_join(join)
    {
        return Some(format!(
            "broadcast {:?} join inside a stage subtree: its unmatched build rows are \
             emitted only after the last probe partition, which a task executing one \
             partition can never observe",
            join.join_type()
        ));
    }
    // A scalar subquery is executed WHOLE by whichever task evaluates it —
    // `ScalarSubqueryExec` runs each through `execute_stream`, which coalesces
    // the plan to a single partition. An exchange inside one is therefore
    // ordinary single-node execution, not a violation of the task-per-partition
    // model, and the rule below must not reach into it: descending would reject
    // any query whose subquery happens to contain a hash exchange and quietly
    // run the whole thing as one task.
    if let Some(subquery_exec) = plan.downcast_ref::<ScalarSubqueryExec>() {
        return find_unsupported_stage_node(subquery_exec.input());
    }
    for child in plan.children() {
        if let Some(reason) = find_unsupported_stage_node(child) {
            return Some(reason);
        }
    }
    None
}

// ── Cross-stage runtime filters ────────────────────────────────────────────

/// How many times larger the probe side must be estimated before a filter is
/// worth its stage.
///
/// The filter costs one extra scan of the build side plus a broadcast of a few
/// MB to every probe task. Below this ratio that is not obviously repaid, and a
/// rule that fires on marginal cases is exactly how the semi-join rule and the
/// broadcast over-reach each cost more than they gained.
const RUNTIME_FILTER_MIN_RATIO: usize = 8;

/// A join that can carry a cross-stage runtime filter, with everything the
/// rewrite needs already validated.
#[derive(Debug, Clone, Copy)]
struct RuntimeFilterCandidate {
    build_stage: usize,
    probe_stage: usize,
    build_key_index: usize,
    probe_key_index: usize,
    filter_bytes: usize,
}

/// What a join child reads, when it reads one upstream stage with the column
/// layout that stage's root emits.
#[derive(Debug, Clone, Copy)]
struct JoinSideRead {
    stage: usize,
    rows: Option<usize>,
}

/// Find the [`ShuffleReadExec`] under a join child, if the column indexes at the
/// join are the same indexes the upstream stage's root emits.
///
/// The whole rewrite hinges on that equality: the key index comes from the
/// join's `on` expressions, which are `Column`s into the join child's schema,
/// and it is applied at the *root of the upstream stage*. So this descends only
/// through single-child nodes that leave the field list untouched — a
/// projection that reorders or renames stops the walk rather than silently
/// shifting which column gets filtered.
fn join_side_read(plan: &Arc<dyn ExecutionPlan>) -> Option<JoinSideRead> {
    let mut current = Arc::clone(plan);
    loop {
        if let Some(read) = current.downcast_ref::<ShuffleReadExec>() {
            return Some(JoinSideRead {
                stage: read.upstream_stage_index(),
                rows: read.upstream_estimate().0,
            });
        }
        let next = {
            let children = current.children();
            let [child] = children.as_slice() else {
                return None;
            };
            if current.schema().fields() != child.schema().fields() {
                return None;
            }
            Arc::clone(child)
        };
        current = next;
    }
}

/// Decide whether one join earns a runtime filter, and on which key.
///
/// Every guard here exists because a plan rule that fires too widely has
/// already cost this engine more than it gained, twice.
/// Why joins were not even considered as runtime-filter candidates.
///
/// The guards inside [`runtime_filter_candidate`] all return `None`, so a join
/// rejected there never reaches the injection loop and never appears in its
/// counters. Instrumenting only the injection loop would have answered "how
/// many candidates were rejected" while leaving "why were there no candidates"
/// exactly as invisible as before — which is the half-fix that makes a rule
/// look installed-but-idle.
#[derive(Debug, Default, Clone, Copy)]
struct RuntimeFilterRejects {
    /// Joins inspected.
    joins: usize,
    /// Not an INNER join, so dropping probe rows would change the answer.
    not_inner: usize,
    /// A side does not resolve to a `ShuffleReadExec` through schema-preserving
    /// parents — a projection between the join and the read stops the walk.
    side_not_a_shuffle_read: usize,
    /// Both sides read the same stage; DataFusion's own dynamic filter covers it.
    same_stage: usize,
    /// No row estimate on one of the sides.
    no_row_estimate: usize,
    /// The build side is not selective enough to be worth a stage.
    not_selective: usize,
    /// The bloom would be clamped at the size ceiling, degrading to
    /// "matches everything".
    filter_too_large: usize,
    /// No equijoin pair of a type the filter can encode on both sides.
    no_encodable_key: usize,
    /// A join node this pass cannot even look at, because it is not a
    /// `HashJoinExec`.
    ///
    /// This is not a rejection — it is the pass being structurally blind, and
    /// it is why every other counter here read zero on the query this feature
    /// was built for. `SpillableJoinSelection` runs *before* stage cutting and
    /// converts an oversized hash join into a sort-merge join; on TPC-H q21 at
    /// SF100 it converted all five (`hash_joins: 5, converted: 3`, plus two it
    /// declined that were already sort-merge). By the time this pass walks the
    /// plan there is no `HashJoinExec` left to inspect, so it logged
    /// `joins_inspected: 0` — indistinguishable, in a log, from a query with no
    /// joins at all.
    ///
    /// Counting them separately makes "the rule declined" and "the rule could
    /// not see it" different observations. Generalising the rule to sort-merge
    /// (and grace) joins is the actual fix: a runtime filter is a semantic
    /// operation on the join's keys and does not care which algorithm executes
    /// it, and `runtime_filter_candidate` only needs `join_type`, `left`,
    /// `right` and the equijoin keys, all of which those nodes also have.
    joins_of_unsupported_kind: usize,
}

/// The four things a runtime filter needs from a join, independent of which
/// algorithm executes it.
///
/// A bloom filter is a statement about the join's *keys*: probe rows whose key
/// cannot appear on the build side cannot join, whichever way the rows are
/// matched. Hash, sort-merge and grace joins all answer the same four
/// questions, so the rule reads them through this view instead of being
/// hard-wired to one node type — which is what left it inspecting zero joins on
/// q21 (see `joins_of_unsupported_kind`).
struct JoinView<'a> {
    /// By value: `HashJoinExec` hands back a reference and
    /// `SortMergeJoinExec` a copy, and `JoinType` is `Copy`.
    join_type: datafusion::logical_expr::JoinType,
    left: &'a Arc<dyn ExecutionPlan>,
    right: &'a Arc<dyn ExecutionPlan>,
    /// DataFusion's own name for the equijoin-pair slice, so this reads the
    /// same shape both join nodes expose rather than restating it.
    on: datafusion::physical_plan::joins::utils::JoinOnRef<'a>,
}

fn runtime_filter_candidate(
    join: &JoinView<'_>,
    rejects: &mut RuntimeFilterRejects,
) -> Option<RuntimeFilterCandidate> {
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_expr::expressions::Column;
    use krishiv_shuffle::{FilterKeyType, MAX_FILTER_BYTES, plan_filter_bytes};

    // Guard — INNER only. A bloom drops probe rows that cannot match, which is
    // invisible to an inner join and catastrophic to anything that preserves
    // unmatched probe rows: RightAnti emits exactly the rows this removes, and
    // Full/Right pad them with nulls. `RuntimeFilter::contains` also drops null
    // keys, which is correct only where a null key cannot produce output.
    rejects.joins += 1;
    if join.join_type != JoinType::Inner {
        rejects.not_inner += 1;
        return None;
    }
    // Guard 1 — different stages. A same-stage join already gets DataFusion's
    // own dynamic filter, so firing there duplicates work for nothing.
    let (Some(build), Some(probe)) = (join_side_read(join.left), join_side_read(join.right)) else {
        rejects.side_not_a_shuffle_read += 1;
        return None;
    };
    if build.stage == probe.stage {
        rejects.same_stage += 1;
        return None;
    }

    // Guard 2 — selectivity, on estimates that exist. `Precision::Absent`
    // arrives here as `None` and means "no idea", never "small": guessing is
    // the `SpillableJoinSelection` lesson, and guessing wrong here adds a stage
    // and a broadcast to a query that gains nothing from either.
    let (Some(build_rows), Some(probe_rows)) = (build.rows, probe.rows) else {
        rejects.no_row_estimate += 1;
        return None;
    };
    if build_rows == 0 || probe_rows / RUNTIME_FILTER_MIN_RATIO < build_rows {
        rejects.not_selective += 1;
        return None;
    }

    // Guard 3 — size cap. `plan_filter_bytes` clamps at the ceiling, and a
    // clamped filter is one whose false-positive rate has quietly degraded
    // towards "matches everything" — correct, but pure cost.
    let filter_bytes = plan_filter_bytes(build_rows as u64);
    if filter_bytes >= MAX_FILTER_BYTES {
        rejects.filter_too_large += 1;
        return None;
    }

    // Guard 6 — read the join's own equijoin pairs rather than re-deriving
    // them. Only plain columns of a type the filter can encode canonically;
    // for a composite key the first usable column is enough, because a row that
    // matches on every key column necessarily matches on one of them.
    let found = join.on.iter().find_map(|(left, right)| {
        let build_column = (left.as_ref() as &dyn std::any::Any).downcast_ref::<Column>()?;
        let probe_column = (right.as_ref() as &dyn std::any::Any).downcast_ref::<Column>()?;
        let build_schema = join.left.schema();
        let probe_schema = join.right.schema();
        let build_type =
            FilterKeyType::for_data_type(build_schema.field(build_column.index()).data_type())?;
        let probe_type =
            FilterKeyType::for_data_type(probe_schema.field(probe_column.index()).data_type())?;
        // A disagreement fails open at runtime, but there is no reason to build
        // a filter that will be ignored.
        (build_type == probe_type).then_some(RuntimeFilterCandidate {
            build_stage: build.stage,
            probe_stage: probe.stage,
            build_key_index: build_column.index(),
            probe_key_index: probe_column.index(),
            filter_bytes,
        })
    });
    if found.is_none() {
        rejects.no_encodable_key += 1;
    }
    found
}

fn collect_runtime_filter_candidates(
    plan: &Arc<dyn ExecutionPlan>,
    out: &mut Vec<RuntimeFilterCandidate>,
    rejects: &mut RuntimeFilterRejects,
) {
    // Both equijoin algorithms are read through the same view. Which one the
    // planner picked is not a property of the filter: `SpillableJoinSelection`
    // converts an oversized hash join to sort-merge *before* stage cutting, and
    // on q21 at SF100 it converted all five — so keying on `HashJoinExec` alone
    // meant the rule saw nothing at all on the query it was written for.
    if let Some(join) = plan.downcast_ref::<datafusion::physical_plan::joins::HashJoinExec>() {
        let view = JoinView {
            join_type: *join.join_type(),
            left: join.left(),
            right: join.right(),
            on: join.on(),
        };
        if let Some(candidate) = runtime_filter_candidate(&view, rejects) {
            out.push(candidate);
        }
    } else if let Some(join) =
        plan.downcast_ref::<datafusion::physical_plan::joins::SortMergeJoinExec>()
    {
        let view = JoinView {
            join_type: join.join_type(),
            left: join.left(),
            right: join.right(),
            on: join.on(),
        };
        if let Some(candidate) = runtime_filter_candidate(&view, rejects) {
            out.push(candidate);
        }
    } else if plan
        .downcast_ref::<datafusion::physical_plan::joins::NestedLoopJoinExec>()
        .is_some()
        || plan
            .downcast_ref::<crate::grace_hash_join::GraceHashJoinExec>()
            .is_some()
    {
        // Still unreachable, for different reasons, and both honest:
        //
        // * `NestedLoopJoinExec` has no equijoin pairs at all — it is the node
        //   DataFusion picks when there is no equality to key on, so there is
        //   no key to build a filter over. Not a gap; a category error.
        // * `GraceHashJoinExec` is ours and does carry equi keys, but it
        //   partitions its build side into buckets on disk, and whether a probe
        //   filter composes with that spill protocol is a question this pass
        //   should not answer by assumption. Left counted until it is measured.
        rejects.joins_of_unsupported_kind += 1;
    }
    for child in plan.children() {
        collect_runtime_filter_candidates(child, out, rejects);
    }
}

/// Does stage `from` depend, transitively, on stage `target`?
///
/// Guard 4. The filter stage inherits the build stage's upstreams, and the probe
/// stage gains a dependency on the filter stage — so if the build side already
/// depends on the probe side, that new edge closes a loop. The scheduler would
/// catch it (Kahn's algorithm, `validate_job`) but only by rejecting the whole
/// job, which turns an optimization into an outage.
fn stage_depends_on(drafts: &[StageDraft], from: usize, target: usize) -> bool {
    let mut seen = vec![false; drafts.len()];
    let mut stack = vec![from];
    while let Some(index) = stack.pop() {
        if index == target {
            return true;
        }
        match seen.get_mut(index) {
            Some(flag) if !*flag => *flag = true,
            _ => continue,
        }
        if let Some(draft) = drafts.get(index) {
            stack.extend(collect_upstream_stage_indexes(&draft.plan));
        }
    }
    false
}

// ── Stage reuse (Spark's ReuseExchange) ────────────────────────────────────

/// Env flag for cross-stage reuse of identical leaf stages. Default **off**.
pub const STAGE_REUSE_ENV: &str = "KRISHIV_STAGE_REUSE";

/// Whether identical leaf stages are collapsed into one.
pub fn stage_reuse_enabled() -> bool {
    std::env::var(STAGE_REUSE_ENV)
        .map(|v| {
            let v = v.trim();
            v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on")
        })
        .unwrap_or(false)
}

/// Function names whose presence makes a subtree non-reusable.
///
/// Reuse replaces two evaluations with one, which is only sound when the
/// subtree is **deterministic**. `ExecutionPlan` exposes no expression
/// accessor, so the rendered plan text is the only place a volatile call is
/// visible — this matches on that text. Matching is deliberately over-eager: a
/// column merely *named* `random_score` also blocks reuse. A false positive
/// costs one missed optimization; a false negative would silently collapse two
/// evaluations that were supposed to differ.
const VOLATILE_MARKERS: &[&str] = &[
    "random(",
    "rand(",
    "uuid(",
    "now(",
    "current_timestamp",
    "current_date",
    "current_time",
    "nextval",
];

/// Rewrite every `ShuffleReadExec` in `plan`, remapping its upstream stage
/// index through `remap`.
fn remap_shuffle_reads(
    plan: &Arc<dyn ExecutionPlan>,
    remap: &std::collections::HashMap<usize, usize>,
) -> Arc<dyn ExecutionPlan> {
    if let Some(read) = plan.downcast_ref::<ShuffleReadExec>() {
        let old = read.upstream_stage_index();
        if let Some(&new) = remap.get(&old)
            && new != old
        {
            return Arc::new(read.clone_with_upstream_stage_index(new));
        }
        return Arc::clone(plan);
    }
    let children = plan.children();
    if children.is_empty() {
        return Arc::clone(plan);
    }
    let new_children: Vec<_> = children
        .iter()
        .map(|child| remap_shuffle_reads(child, remap))
        .collect();
    let changed = new_children
        .iter()
        .zip(children.iter())
        .any(|(new, old)| !Arc::ptr_eq(new, old));
    if !changed {
        return Arc::clone(plan);
    }
    Arc::clone(plan)
        .with_new_children(new_children)
        .unwrap_or_else(|_| Arc::clone(plan))
}

/// Collapse identical leaf stages into one, so a subtree computed twice is
/// computed once and both consumers read the same shuffle output.
///
/// This is Spark's `ReuseExchange`, and it fits our model exactly: the cutter
/// already materializes every stage boundary, so two stages that compute the
/// same thing are two writes of the same bytes.
///
/// # What it does NOT reach, measured
///
/// The three cases originally listed here (q18/q21 `lineitem`, q2 `partsupp`)
/// were identified as duplicated **scans**, and this rule was written as if
/// that made them duplicated **stages**. It does not. On the SF100 sweep of
/// `fast-6f586954` the rule fired **twice across all 22 queries**, one stage
/// each.
///
/// `tests/stage_reuse_duplicate_scan.rs` reproduces q18's shape over real
/// parquet and shows why:
///
/// ```text
/// stage A:  AggregateExec(Partial, gby=l_orderkey, sum(l_quantity))
///             DataSourceExec lineitem[l_orderkey, l_quantity]  DynamicFilter [ empty ]
/// stage B:  DataSourceExec lineitem[l_orderkey, l_quantity]    DynamicFilter [ empty ]
/// ```
///
/// Identical scan, identical projection, identical predicate — and a partial
/// aggregate fused onto one of them. They are not the same stage and never
/// will be. (The `DynamicFilter` is *identical* on both sides and is not the
/// blocker, which is what an earlier note here guessed.)
///
/// # And sharing the scan is probably the wrong trade here anyway
///
/// Both consumers want `lineitem` partitioned by `l_orderkey`, so one shuffle
/// could serve both — but only by lifting the partial aggregate above the
/// exchange, which puts ~600M raw rows on the wire in place of the partially
/// aggregated ~150M. This cluster's floor is the network (pod-to-pod ~11 MiB/s
/// VXLAN vs 150-286 MB/s node-local MinIO — `bench-storage-longhorn-bottleneck`,
/// `bench-storage-locality-fix`), so converting a cheap local re-read into an
/// expensive shuffle read is a loss, not a win.
///
/// The lever for q18 is not scanning `lineitem` once; it is not shipping 600M
/// rows at all (`cross-stage-runtime-filter-design`).
///
/// **Restricted to leaf stages** (no `ShuffleReadExec` inside, no severed
/// scalar-subquery context). Two reasons, both load-bearing: a leaf stage
/// contains no stage indexes, so collapsing it can never invalidate a
/// reference *inside* it; and a leaf stage is a scan + row-wise work, the case
/// where textual identity is most trustworthy. Non-leaf reuse (q17's
/// projection-subsumed scan) needs a different, wider rule.
///
/// Returns the number of stages removed.
fn dedupe_identical_stages(
    root: &mut Arc<dyn ExecutionPlan>,
    drafts: &mut Vec<StageDraft>,
) -> usize {
    if !stage_reuse_enabled() {
        return 0;
    }
    dedupe_identical_stages_unconditionally(root, drafts)
}

/// [`dedupe_identical_stages`] without the flag check, so the rewrite and every
/// guard are testable without `set_var` racing across test threads.
fn dedupe_identical_stages_unconditionally(
    root: &mut Arc<dyn ExecutionPlan>,
    drafts: &mut Vec<StageDraft>,
) -> usize {
    use datafusion::physical_plan::displayable;

    // The identity is the **encoded plan**, not the rendered plan text.
    //
    // Plan text is a display function, not a semantic identity: two
    // `DataSourceExec`s over different in-memory data render identically,
    // because the printer has nothing to show. Keying on text collapsed two
    // stages producing different rows — a wrong answer, caught by
    // `different_content_does_not_collapse`. The protobuf encoding is the
    // bytes we actually ship to executors, so byte-identical encodings are the
    // same computation by construction, and a plan that will not encode is
    // simply not eligible (fails closed).
    let codec = KrishivPhysicalCodec::coordinator();

    // canonical key -> first draft index carrying it
    let mut first_seen: std::collections::HashMap<Vec<u8>, usize> =
        std::collections::HashMap::new();
    // duplicate index -> index it is replaced by
    let mut replaced_by: std::collections::HashMap<usize, usize> = std::collections::HashMap::new();

    for (index, draft) in drafts.iter().enumerate() {
        if draft.subqueries.is_some() {
            continue;
        }
        if !collect_upstream_stage_indexes(&draft.plan).is_empty() {
            continue;
        }
        let Some(shuffle) = &draft.shuffle else {
            continue;
        };
        let text = displayable(draft.plan.as_ref()).indent(true).to_string();
        let lowered = text.to_ascii_lowercase();
        if VOLATILE_MARKERS.iter().any(|m| lowered.contains(m)) {
            continue;
        }
        // The shuffle contract is part of the identity: two stages computing
        // the same rows but partitioning them differently are NOT
        // interchangeable, because the consumer reads by partition index.
        //
        // `map_tasks` matters for a less obvious reason: a `ShuffleReadExec` is
        // constructed with the producer's task count, so repointing a reader at
        // a stage with a different task count would make it look for map
        // outputs that do not exist.
        let Ok(encoded) = encode_dfplan_bytes(Arc::clone(&draft.plan), &codec) else {
            // Not shippable, so not reusable. The main loop has its own
            // fallback for this; here it just means "skip".
            continue;
        };
        let mut key = encoded;
        key.extend_from_slice(
            format!(
                "|keys={:?}|parts={}|map_tasks={}|schema={:?}",
                shuffle.key_columns,
                shuffle.num_output_partitions,
                draft.plan.output_partitioning().partition_count(),
                draft.plan.schema()
            )
            .as_bytes(),
        );
        match first_seen.get(&key) {
            Some(&canonical) => {
                replaced_by.insert(index, canonical);
            }
            None => {
                first_seen.insert(key, index);
            }
        }
    }

    if replaced_by.is_empty() {
        return 0;
    }

    // Compact the draft list, building old -> new for the survivors.
    let mut remap: std::collections::HashMap<usize, usize> = std::collections::HashMap::new();
    let mut survivors: Vec<StageDraft> = Vec::with_capacity(drafts.len() - replaced_by.len());
    for (old_index, draft) in std::mem::take(drafts).into_iter().enumerate() {
        if replaced_by.contains_key(&old_index) {
            continue;
        }
        remap.insert(old_index, survivors.len());
        survivors.push(draft);
    }
    // Duplicates point at their canonical stage's NEW index. The canonical is
    // always a survivor (it is the first occurrence, and only later
    // occurrences are removed), so this lookup cannot fail.
    for (duplicate, canonical) in &replaced_by {
        if let Some(&new_canonical) = remap.get(canonical) {
            remap.insert(*duplicate, new_canonical);
        }
    }

    let removed = replaced_by.len();
    for draft in &mut survivors {
        draft.plan = remap_shuffle_reads(&draft.plan, &remap);
    }
    *root = remap_shuffle_reads(root, &remap);
    *drafts = survivors;

    tracing::info!(
        removed_stages = removed,
        "collapsed identical leaf stages (stage reuse)"
    );
    removed
}

/// Insert cross-stage runtime filters: for each qualifying join, add a stage
/// that builds a bloom of the build-side key and make the probe stage filter
/// its rows through it before shuffling them.
///
/// Returns the number of filters injected. Off unless
/// [`crate::runtime_filter_exec::enabled`]; a failure to build any single
/// filter skips that filter and leaves the plan untouched, because a
/// throughput optimization must never be why a query fails.
fn inject_runtime_filters(root: &Arc<dyn ExecutionPlan>, drafts: &mut Vec<StageDraft>) -> usize {
    if !crate::runtime_filter_exec::enabled() {
        // Count and report anyway, then change nothing.
        //
        // The early return used to happen here, *before* the pass computed its
        // rejection breakdown — so the one diagnostic that would tell an
        // operator whether enabling the flag is worth trying was available only
        // after enabling it. Every run since the counters landed in `59243a94`
        // has therefore produced zero `runtime-filter: pass complete` lines, and
        // the note-to-self to "read that line before theorising" was unsatisfiable
        // by construction.
        //
        // A dry run costs one read-only walk of a plan that has just been built
        // and cut, and it makes "would this fire?" answerable from an ordinary
        // benchmark log.
        report_runtime_filter_candidates(root, drafts);
        return 0;
    }
    inject_runtime_filters_unconditionally(root, drafts)
}

/// Walk the plan exactly as the injector would and log the same
/// `runtime-filter: pass complete` breakdown, without rewriting anything.
///
/// Deliberately shares [`collect_runtime_filter_candidates`] with the real pass:
/// a dry run that used its own traversal would answer a question nobody asked.
fn report_runtime_filter_candidates(root: &Arc<dyn ExecutionPlan>, drafts: &[StageDraft]) {
    let mut candidates = Vec::new();
    let mut rejects = RuntimeFilterRejects::default();
    collect_runtime_filter_candidates(root, &mut candidates, &mut rejects);
    for draft in drafts {
        collect_runtime_filter_candidates(&draft.plan, &mut candidates, &mut rejects);
    }
    tracing::info!(
        joins_inspected = rejects.joins,
        not_inner = rejects.not_inner,
        side_not_a_shuffle_read = rejects.side_not_a_shuffle_read,
        same_stage = rejects.same_stage,
        no_row_estimate = rejects.no_row_estimate,
        not_selective = rejects.not_selective,
        filter_too_large = rejects.filter_too_large,
        no_encodable_key = rejects.no_encodable_key,
        joins_of_unsupported_kind = rejects.joins_of_unsupported_kind,
        candidates = candidates.len(),
        injected = 0,
        enabled = false,
        "runtime-filter: pass complete"
    );
}

/// [`inject_runtime_filters`] without the flag check.
///
/// Split out so the rewrite and every guard can be tested directly. Reading the
/// flag inside the tested function would make the whole rule depend on process
/// environment, and `set_var` across parallel test threads is a race, not a
/// fixture.
fn inject_runtime_filters_unconditionally(
    root: &Arc<dyn ExecutionPlan>,
    drafts: &mut Vec<StageDraft>,
) -> usize {
    use crate::runtime_filter_exec::{
        RuntimeFilterBuildExec, RuntimeFilterProbeExec, filter_schema,
    };
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::projection::ProjectionExec;

    let mut candidates = Vec::new();
    let mut rejects = RuntimeFilterRejects::default();
    collect_runtime_filter_candidates(root, &mut candidates, &mut rejects);
    for draft in drafts.iter() {
        collect_runtime_filter_candidates(&draft.plan, &mut candidates, &mut rejects);
    }

    let mut touched: Vec<usize> = Vec::new();
    let mut injected = 0usize;
    // Why each candidate was turned away.
    //
    // Every rejection below was previously either `debug!` (invisible at the
    // executors' and coordinator's `info` level) or a bare `continue`. So a run
    // that found no candidates and a run that rejected all of them produced
    // *identical* output: nothing. Turning the flag on for q18 changed the
    // wall time by 2.4% and left 0 log lines, and there was no way to tell
    // whether the rule had declined or was simply not installed.
    //
    // That is the exact failure that made `SpillableJoinSelection` cost hours
    // of live investigation before its "pass complete" line existed. One
    // counted summary, at info, is the whole fix.
    let candidate_count = candidates.len();
    let (mut already_touched, mut missing_stage, mut severed_subquery) = (0usize, 0usize, 0usize);
    let (mut would_cycle, mut no_key_field, mut build_failed, mut probe_failed) =
        (0usize, 0usize, 0usize, 0usize);
    for candidate in candidates {
        // One filter per stage, and never over a stage this pass has already
        // rewritten: stacking rewrites would have each filter stage clone the
        // previous one's probe node, which is correct but compounds cost for a
        // shrinking return.
        if touched.contains(&candidate.build_stage) || touched.contains(&candidate.probe_stage) {
            already_touched += 1;
            continue;
        }
        let (Some(build), Some(probe)) = (
            drafts.get(candidate.build_stage),
            drafts.get(candidate.probe_stage),
        ) else {
            missing_stage += 1;
            continue;
        };
        // A stage severed from a `ScalarSubqueryExec` is parameterised by a
        // subquery result; cloning its subtree without the wrapper produces a
        // fragment that cannot decode.
        if build.subqueries.is_some() || probe.subqueries.is_some() {
            severed_subquery += 1;
            continue;
        }
        if stage_depends_on(drafts, candidate.build_stage, candidate.probe_stage) {
            would_cycle += 1;
            continue;
        }

        let source = Arc::clone(&build.plan);
        let probe_plan = Arc::clone(&probe.plan);
        let schema = source.schema();
        let Some(field) = schema.fields().get(candidate.build_key_index) else {
            no_key_field += 1;
            continue;
        };
        let name = field.name().clone();
        // Project to the key column alone before coalescing: the filter stage
        // needs one column, and carrying the rest through a single task is
        // memory spent to be thrown away.
        let projected = ProjectionExec::try_new(
            vec![(
                Arc::new(Column::new(&name, candidate.build_key_index)) as _,
                name.clone(),
            )],
            source,
        );
        let filter_plan = projected.and_then(|projected| {
            let coalesced = Arc::new(CoalescePartitionsExec::new(Arc::new(projected)));
            RuntimeFilterBuildExec::try_new(coalesced, 0, candidate.filter_bytes)
        });
        let filter_plan = match filter_plan {
            Ok(plan) => Arc::new(plan) as Arc<dyn ExecutionPlan>,
            Err(error) => {
                build_failed += 1;
                tracing::debug!(%error, "declined to build a runtime filter stage");
                continue;
            }
        };

        let filter_stage = drafts.len();
        // A single map task (the coalesce above) writing one keyless partition:
        // exactly the gather shape the cutter already emits for ungrouped
        // aggregates, so the writer and reader need no special case.
        let read = ShuffleReadExec::new(filter_stage, 1, 1, filter_schema(), None);
        let rewritten =
            RuntimeFilterProbeExec::try_new(probe_plan, Arc::new(read), candidate.probe_key_index);
        let rewritten = match rewritten {
            Ok(plan) => Arc::new(plan) as Arc<dyn ExecutionPlan>,
            Err(error) => {
                probe_failed += 1;
                tracing::debug!(%error, "declined to apply a runtime filter to the probe stage");
                continue;
            }
        };
        let Some(probe_draft) = drafts.get_mut(candidate.probe_stage) else {
            missing_stage += 1;
            continue;
        };
        probe_draft.plan = rewritten;
        drafts.push(StageDraft {
            plan: filter_plan,
            shuffle: Some(StageShuffleOutput {
                key_columns: Vec::new(),
                num_output_partitions: 1,
            }),
            subqueries: None,
        });
        touched.push(candidate.build_stage);
        touched.push(candidate.probe_stage);
        injected += 1;
        tracing::info!(
            build_stage = candidate.build_stage,
            probe_stage = candidate.probe_stage,
            filter_stage,
            filter_bytes = candidate.filter_bytes,
            "injected a cross-stage runtime filter"
        );
    }
    // At info, unconditionally: "no candidates" and "rejected every candidate"
    // must never again be indistinguishable from "rule not installed".
    tracing::info!(
        joins_inspected = rejects.joins,
        not_inner = rejects.not_inner,
        side_not_a_shuffle_read = rejects.side_not_a_shuffle_read,
        same_stage = rejects.same_stage,
        no_row_estimate = rejects.no_row_estimate,
        not_selective = rejects.not_selective,
        filter_too_large = rejects.filter_too_large,
        no_encodable_key = rejects.no_encodable_key,
        joins_of_unsupported_kind = rejects.joins_of_unsupported_kind,
        candidates = candidate_count,
        injected,
        already_touched,
        missing_stage,
        severed_subquery,
        would_cycle,
        no_key_field,
        build_failed,
        probe_failed,
        "runtime-filter: pass complete"
    );
    injected
}

fn collect_upstream_stage_indexes(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
    let mut indexes = Vec::new();
    collect_upstream_inner(plan, &mut indexes);
    indexes.sort_unstable();
    indexes.dedup();
    indexes
}

fn collect_upstream_inner(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<usize>) {
    if let Some(read) = plan.downcast_ref::<ShuffleReadExec>() {
        out.push(read.upstream_stage_index());
    }
    for child in plan.children() {
        collect_upstream_inner(child, out);
    }
}

#[cfg(test)]
mod tests {

    /// Does ANY node of the plan carry a scalar subquery?
    ///
    /// `LogicalPlan::expressions()` returns only the expressions of the node it
    /// is called on — for `SELECT ... WHERE x > (subquery)` the root is a
    /// Projection and the subquery lives in the Filter beneath it. Checking the
    /// root alone silently proves nothing, which is what the precondition
    /// assertion in these tests exists to catch.
    fn plan_has_scalar_subquery(plan: &datafusion::logical_expr::LogicalPlan) -> bool {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        use datafusion::logical_expr::Expr;
        let mut found = false;
        let _ = plan.apply(|node| {
            if node
                .expressions()
                .iter()
                .any(Expr::contains_scalar_subquery)
            {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        found
    }

    /// q22: the whole point is that a query carrying an uncorrelated scalar
    /// subquery becomes stageable. Before the fold, `ScalarSubqueryExpr` cannot
    /// round-trip through `dfplan` encoding, the verify step refuses the
    /// fragment, and the caller runs the query as ONE task while reporting a
    /// clean pass.
    ///
    /// Asserts the outcome (stages exist, and the plan no longer carries a
    /// scalar subquery) rather than the mechanism, so the test survives a
    /// change of folding strategy.
    #[tokio::test]
    async fn an_uncorrelated_scalar_subquery_is_folded_so_the_query_can_stage() {
        let ctx = planning_session_context(4);
        ctx.sql(
            "CREATE TABLE acct(id BIGINT, bal DOUBLE) AS VALUES (1, 10.0), (2, 30.0), (3, 50.0)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

        let sql = "SELECT id FROM acct WHERE bal > (SELECT avg(bal) FROM acct)";
        let before = ctx.sql(sql).await.unwrap();
        assert!(
            plan_has_scalar_subquery(before.logical_plan()),
            "precondition: the planned query must actually carry a scalar \
             subquery, or this test proves nothing"
        );

        let after = inline_uncorrelated_scalar_subqueries(&ctx, before)
            .await
            .unwrap();
        assert!(
            !plan_has_scalar_subquery(after.logical_plan()),
            "the uncorrelated subquery must be folded to a constant"
        );

        // And the fold must not change the answer: avg is 30.0, so only id=3.
        let rows = after.collect().await.unwrap();
        let total: usize = rows.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, 1,
            "folding a constant must not change the result set"
        );
    }

    /// A CORRELATED subquery references the outer row, so it is not a constant
    /// and must be left exactly as it was. Getting this wrong would produce
    /// silently wrong answers, which is far worse than the single-task
    /// fallback this fix exists to remove.
    #[tokio::test]
    async fn a_correlated_scalar_subquery_is_left_alone() {
        let ctx = planning_session_context(4);
        ctx.sql("CREATE TABLE t(k BIGINT, v DOUBLE) AS VALUES (1, 10.0), (2, 30.0)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx.sql("CREATE TABLE u(k BIGINT, w DOUBLE) AS VALUES (1, 5.0), (2, 40.0)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let sql = "SELECT k FROM t WHERE v > (SELECT max(w) FROM u WHERE u.k = t.k)";
        let Ok(before) = ctx.sql(sql).await else {
            // Some correlated shapes are decorrelated by the optimizer before
            // we ever see them; nothing to assert if this one is rejected.
            return;
        };
        let had_subquery = plan_has_scalar_subquery(before.logical_plan());
        let after = inline_uncorrelated_scalar_subqueries(&ctx, before)
            .await
            .unwrap();
        let still_has = plan_has_scalar_subquery(after.logical_plan());
        assert_eq!(
            had_subquery, still_has,
            "a correlated subquery depends on the outer row and must never be \
             folded to a constant"
        );
    }
    use datafusion::prelude::SessionConfig;

    /// D3(2): the point of carrying the upstream estimate is that a
    /// shuffle-fed join side stops reporting `Absent`. This asserts the
    /// property the optimizer rules actually key on, not the field value —
    /// `SpillableJoinSelection` returns `Ok(None)` on `Absent` by design, so
    /// "absent" and "known" is the whole distinction that matters.
    #[test]
    fn a_shuffle_read_reports_its_upstream_estimate_instead_of_unknown() {
        use datafusion::common::stats::Precision;
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, false),
        ]));

        let unknown = ShuffleReadExec::new(0, 4, 4, Arc::clone(&schema), None);
        assert_eq!(
            unknown.partition_statistics(None).unwrap().total_byte_size,
            Precision::Absent,
            "a read with no estimate must stay Absent — inventing a size is how \
             a spill decision gets made on a guess"
        );

        let known = ShuffleReadExec::new(0, 4, 4, Arc::clone(&schema), None)
            .with_upstream_estimate(Some(1_000), Some(800_000));
        let whole = known.partition_statistics(None).unwrap();
        assert_eq!(whole.num_rows, Precision::Inexact(1_000));
        assert_eq!(
            whole.total_byte_size,
            Precision::Inexact(800_000),
            "the whole-plan question gets the whole stage's size"
        );

        let one = known.partition_statistics(Some(0)).unwrap();
        assert_eq!(
            one.total_byte_size,
            Precision::Inexact(200_000),
            "a per-partition question gets the even-split share of 4 partitions"
        );
    }

    /// The estimate has to survive the wire, or the executor runs a plan whose
    /// sizes disagree with the plan the coordinator optimized.
    #[test]
    fn the_upstream_estimate_survives_encode_decode() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, false),
        ]));
        let node: Arc<dyn ExecutionPlan> = Arc::new(
            ShuffleReadExec::new(3, 2, 4, schema, None)
                .with_upstream_estimate(Some(77), Some(4_096)),
        );
        let codec = KrishivPhysicalCodec::coordinator();
        let mut buf = Vec::new();
        codec.try_encode(Arc::clone(&node), &mut buf).unwrap();

        let ctx = crate::SqlEngine::new_with_engine_memory(crate::EngineMemory::Unbounded);
        let task_ctx = ctx.session_context().task_ctx();
        let decoded = codec.try_decode(&buf, &[], &task_ctx).unwrap();

        // Re-encode and compare bytes rather than downcasting: it asserts the
        // same property (the estimate made the trip intact) and it also catches
        // a field that decodes but is dropped on the way back out.
        let mut round_tripped = Vec::new();
        codec.try_encode(decoded, &mut round_tripped).unwrap();
        assert_eq!(
            String::from_utf8(round_tripped).unwrap(),
            String::from_utf8(buf).unwrap(),
            "the upstream estimate must survive encode -> decode -> encode"
        );
    }
    use super::*;
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::displayable;
    use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// The stage builder plans on a throwaway context, so an `s3://` table must
    /// resolve to an object store there. When it did not, `register_parquet`
    /// errored, the caller read that as "decline to stage", and the whole
    /// dataset was scanned by a single executor — correct results, silently
    /// zero distribution.
    ///
    /// The property under test is that the bucket resolves, and that an
    /// explicit registration takes precedence over the lazy fallback (explicit
    /// registration is what carries endpoint and credential configuration).
    /// This test previously opened by asserting a fresh context could *not*
    /// resolve the bucket; installing `LazyCloudObjectStoreRegistry` on the
    /// planning context made that precondition false, so the assertion, not
    /// the behavior, was wrong.
    ///
    /// The round-trip guard must reject bytes that do not decode — feeding it
    /// garbage proves it inspects them rather than returning Ok
    /// unconditionally.
    #[test]
    fn the_roundtrip_guard_rejects_bytes_that_do_not_decode() {
        let codec = DefaultPhysicalExtensionCodec {};
        let err = verify_dfplan_roundtrip(
            b"not a physical plan proto",
            &codec,
            &fragment_decode_session_context().task_ctx(),
            None,
        )
        .expect_err("undecodable bytes must be rejected");
        assert!(format!("{err}").contains("decode"), "got: {err}");
    }

    /// The regression that got the first guard reverted: it verified against a
    /// bare context with no object-store registry, so every s3-scanning
    /// fragment failed the check and silently fell back to single-task (q1:
    /// 13 tasks -> 1 task, 156 s -> 595 s). The verify context must resolve
    /// object stores exactly like the executor's runtime — this asserts the
    /// capability delta that broke, without needing a network round trip
    /// (constructing a lazy store does not contact the endpoint).
    #[test]
    fn the_verify_context_resolves_object_stores_like_the_executor() {
        use datafusion::execution::object_store::ObjectStoreUrl;
        let url = ObjectStoreUrl::parse("s3://roundtrip-bucket").expect("url");

        // A bare context cannot resolve the bucket — the first guard's bug.
        assert!(
            SessionContext::new()
                .runtime_env()
                .object_store(url.clone())
                .is_err(),
            "precondition: a bare context must NOT resolve s3, or this test proves nothing"
        );

        // The context the guard actually uses must.
        planning_session_context(1)
            .task_ctx()
            .runtime_env()
            .object_store(url)
            .expect("the verify context must resolve s3 buckets like the executor runtime");
    }

    /// Logical and physical optimizer rule names installed on a session.
    fn optimizer_rule_names(ctx: &SessionContext) -> (Vec<String>, Vec<String>) {
        let state = ctx.state();
        (
            state
                .optimizers()
                .iter()
                .map(|r| r.name().to_owned())
                .collect(),
            state
                .physical_optimizers()
                .iter()
                .map(|r| r.name().to_owned())
                .collect(),
        )
    }

    /// E4 (review 2026-07-27): the staged planner must carry the same optimizer
    /// rules as the engine, or every rule the engine installs is dead on the
    /// distributed path.
    ///
    /// This is the whole of finding A6 in one assertion, and it currently
    /// FAILS: `planning_session_context` is a bare `SessionContext`, so it
    /// carries none of `CooperativeAmplifiers` (distributed cancel cannot
    /// preempt an amplifying operator without it), `SpillableJoinSelection`
    /// (q18's shipped fix), `SemiJoinReductionThroughAggregate` or
    /// `SemiJoinPushdownThroughInnerJoin` (q17's shipped fix — 88 % of a 252 s
    /// query). Two shipped performance fixes do not apply to the path being
    /// benchmarked, and nothing said so.
    ///
    /// Left executable-but-ignored deliberately: the fix is A6 (Batch 3, plan
    /// the staged query on `SqlEngine`'s own `SessionStateBuilder`), and this
    /// documents the gap in a form that turns green the moment it lands rather
    /// than in prose that can rot.
    #[test]
    fn the_staging_context_carries_the_engines_optimizer_rules() {
        let engine = crate::SqlEngine::new_with_engine_memory(crate::EngineMemory::Unbounded);
        let (engine_logical, engine_physical) = optimizer_rule_names(engine.session_context());
        let staging = planning_session_context(engine.target_parallelism().get());
        let (staging_logical, staging_physical) = optimizer_rule_names(&staging);

        assert_eq!(
            engine_logical, staging_logical,
            "the staged planner must run the engine's logical optimizer rules; \
             missing here means SemiJoinReductionThroughAggregate / \
             SemiJoinPushdownThroughInnerJoin never fire distributed (D4)"
        );
        assert_eq!(
            engine_physical, staging_physical,
            "the staged planner must run the engine's physical optimizer rules; \
             missing here means SpillableJoinSelection (D3) and \
             CooperativeAmplifiers (distributed cancel) never fire"
        );

        // The config half of A6: the four runtime-filter switches and the
        // lambda-capable dialect are what make `KRISHIV_RUNTIME_FILTERS` mean
        // anything distributed, and what let a Phase-60 lambda query stage at
        // all instead of silently degrading to one task.
        let engine_opts = engine.session_context().copied_config();
        let staging_opts = staging.copied_config();
        for option in [
            "datafusion.optimizer.enable_dynamic_filter_pushdown",
            "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
            "datafusion.optimizer.enable_topk_dynamic_filter_pushdown",
            "datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown",
        ] {
            assert_eq!(
                engine_opts
                    .options()
                    .entries()
                    .iter()
                    .find(|e| e.key == option)
                    .map(|e| e.value.clone()),
                staging_opts
                    .options()
                    .entries()
                    .iter()
                    .find(|e| e.key == option)
                    .map(|e| e.value.clone()),
                "{option} must match the engine's setting on the staged planner"
            );
        }
        assert_eq!(
            engine_opts.options().sql_parser.dialect,
            staging_opts.options().sql_parser.dialect,
            "the staged planner must parse in the engine's dialect"
        );
        assert_eq!(
            engine_opts.options().execution.batch_size,
            staging_opts.options().execution.batch_size,
            "the staged planner must use the engine's batch size"
        );
    }

    /// A5: the guard rehearses the decode against the wrong session.
    ///
    /// The executor decodes a fragment on `task_sql_engine`, a real
    /// `SqlEngine` carrying Krishiv's registered UDFs. The guard decoded on
    /// `planning_session_context`, a bare `SessionContext` that carries none
    /// of them — so a fragment referencing `get_json_object` (a Phase-60 front
    /// door function, always available on the engine) fails the guard, the
    /// caller reads that as "decline to stage", and the query silently runs as
    /// a single task on one executor.
    ///
    /// The plan is built on the engine and decoded through the guard, which is
    /// exactly the asymmetry: encode-side capability the verify side lacks.
    #[tokio::test]
    async fn the_roundtrip_guard_accepts_a_fragment_using_an_engine_udf() {
        let engine = crate::SqlEngine::new_with_engine_memory(crate::EngineMemory::Unbounded);
        let ctx = engine.session_context();
        ctx.sql("CREATE TABLE docs AS VALUES ('{\"a\":1}'), ('{\"a\":2}')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        // The argument must be a column: a literal one is const-folded away and
        // the encoded plan then carries no UDF reference at all.
        let plan = ctx
            .sql("SELECT get_json_object(column1, '$.a') AS a FROM docs")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let codec = DefaultPhysicalExtensionCodec {};
        let bytes = encode_dfplan_bytes(plan, &codec).expect("encode");

        // Precondition: the bare planning context genuinely cannot decode it,
        // or this test proves nothing about which context the guard uses.
        let bare = planning_session_context(1).task_ctx();
        assert!(
            datafusion_proto::bytes::physical_plan_from_bytes_with_extension_codec(
                &bytes, &bare, &codec
            )
            .is_err(),
            "precondition: a bare planning context must NOT resolve engine UDFs"
        );

        verify_dfplan_roundtrip(
            &bytes,
            &codec,
            &fragment_decode_session_context().task_ctx(),
            None,
        )
        .expect(
            "the guard must decode on the engine the executor uses; failing here \
             silently degrades the query to a single task",
        );
    }

    /// And it must not reject ordinary plans, or every query silently loses
    /// distribution — the worse failure of the two.
    #[tokio::test]
    async fn the_roundtrip_guard_accepts_an_ordinary_plan() {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE t AS VALUES (1, 'a'), (2, 'b')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let plan = ctx
            .sql("SELECT column1 FROM t WHERE column1 > 1")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let codec = DefaultPhysicalExtensionCodec {};
        let bytes = encode_dfplan_bytes(plan, &codec).expect("encode");
        verify_dfplan_roundtrip(
            &bytes,
            &codec,
            &fragment_decode_session_context().task_ctx(),
            None,
        )
        .expect("ordinary plans must pass");
    }

    #[tokio::test]
    async fn s3_paths_resolve_on_the_planning_context_and_explicit_registration_wins() {
        // No environment setup: `build_s3_object_store` defaults the region and
        // constructing a store does not contact the endpoint, so this stays a
        // pure unit test rather than one that mutates process-wide env.
        use datafusion::execution::object_store::ObjectStoreUrl;
        let ctx = planning_session_context(4);
        let url = ObjectStoreUrl::parse("s3://tpch-bucket").expect("bucket url");

        let lazily_built = ctx
            .runtime_env()
            .object_store(url.clone())
            .expect("the planning context must resolve an s3 bucket on demand");

        register_object_store_for_path(&ctx, "s3://tpch-bucket/tpch/sf100/lineitem/")
            .expect("registering an s3 path must succeed");

        let explicit = ctx
            .runtime_env()
            .object_store(url)
            .expect("after registration the planning context must resolve the bucket");
        assert!(
            !Arc::ptr_eq(&lazily_built, &explicit),
            "explicit registration must replace the lazily-constructed store, \
             or configured endpoints and credentials would be ignored"
        );
    }

    /// Local paths must not be routed through the S3 builder — it reads
    /// credentials from the environment and would fail on a machine that has
    /// none, turning every ordinary filesystem-backed staged job into a
    /// single-task job.
    #[tokio::test]
    async fn local_paths_are_left_alone_by_object_store_registration() {
        let ctx = planning_session_context(4);
        register_object_store_for_path(&ctx, "/home/krishiv-bench-data/tpch/sf1/lineitem.parquet")
            .expect("a local path must be a no-op, not an error");
        register_object_store_for_path(&ctx, "relative/dir")
            .expect("a relative local path must be a no-op, not an error");
    }

    /// Write a 4-file parquet dataset (1000 rows total) and return the
    /// directory path (registered as a multi-file table so scans genuinely
    /// have multiple partitions, like real distributed inputs).
    async fn write_test_parquet(dir: &std::path::Path) -> std::path::PathBuf {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let table_dir = dir.join("t");
        std::fs::create_dir_all(&table_dir).expect("table dir");
        for file_index in 0..4i64 {
            let ids: Vec<i64> = (0..250).map(|i| file_index * 250 + i).collect();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids.clone())),
                    Arc::new(StringArray::from(
                        ids.iter()
                            .map(|i| match i % 3 {
                                0 => "red",
                                1 => "green",
                                _ => "blue",
                            })
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        ids.iter().map(|i| i * 3).collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("test batch");
            let path = table_dir.join(format!("part-{file_index}.parquet"));
            let file = std::fs::File::create(&path).expect("create parquet");
            let mut writer =
                datafusion::parquet::arrow::ArrowWriter::try_new(file, schema.clone(), None)
                    .expect("writer init");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }
        table_dir
    }

    /// A declared primary key must actually shrink the GROUP BY.
    ///
    /// This is the whole point of `ParquetTableSpec::with_primary_key`:
    /// DataFusion's `optimize_projections` already calls
    /// `get_required_group_by_exprs_indices` to reduce a GROUP BY to the
    /// minimal functionally-equivalent subset, but it can only do so when the
    /// table declares a key. Without the declaration the rule is live and
    /// inert.
    ///
    /// Measured stakes (TPC-H q10, SF100, 2026-07-31): grouping by seven
    /// customer columns instead of the one key costs **14.8x** end to end —
    /// 1784.6 s versus 120.9 s — because the six determined columns ride
    /// through every join and shuffle.
    #[tokio::test]
    async fn a_declared_primary_key_shrinks_the_group_by() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let table_dir = write_test_parquet(tmp.path()).await;
        let path = table_dir.to_string_lossy().to_string();
        // `category` and `amount` are determined by `id`, so grouping by all
        // three is equivalent to grouping by `id`.
        //
        // The two dependent columns are deliberately NOT selected. DataFusion's
        // rule keeps `(what the parent requires) ∪ (minimal FD subset)`, so a
        // column the output still needs stays in the GROUP BY no matter what
        // the key says. Selecting them would test nothing.
        let sql = "SELECT id, count(*) AS n FROM t GROUP BY id, category, amount";

        let plan_text = |spec: ParquetTableSpec| async move {
            let ctx = planning_session_context(4);
            register_parquet_table(&ctx, &spec)
                .await
                .expect("register table");
            let df = ctx.sql(sql).await.expect("plan sql");
            // The OPTIMIZED plan: `optimize_projections` is where
            // `get_required_group_by_exprs_indices` runs, so the unoptimized
            // plan always shows the full GROUP BY and proves nothing.
            let optimized = df.into_optimized_plan().expect("optimize");
            format!("{}", optimized.display_indent())
        };

        let without = plan_text(ParquetTableSpec::new("t", &path)).await;
        let with = plan_text(ParquetTableSpec::new("t", &path).with_primary_key(["id"])).await;

        // With the key declared, the aggregate groups by `id` alone.
        let group_line = |text: &str| {
            text.lines()
                .find(|line| line.contains("Aggregate:"))
                .unwrap_or("<no Aggregate>")
                .to_owned()
        };
        let (g_without, g_with) = (group_line(&without), group_line(&with));
        assert_ne!(
            g_without, g_with,
            "declaring a primary key changed nothing about the aggregate; \
             the constraint is not reaching DataFusion's functional-dependency \
             machinery.\n  without: {g_without}\n  with:    {g_with}"
        );
        assert!(
            g_with.len() < g_without.len(),
            "the declared key should SHRINK the grouping list, not grow it.\n\
               without: {g_without}\n  with:    {g_with}"
        );
    }

    /// A key naming a column the table does not have is an error, not a
    /// silently ignored declaration — a typo would otherwise present as "the
    /// optimization mysteriously never applies".
    #[tokio::test]
    async fn an_unknown_primary_key_column_is_rejected() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let table_dir = write_test_parquet(tmp.path()).await;
        let ctx = planning_session_context(4);
        let spec = ParquetTableSpec::new("t", table_dir.to_string_lossy().as_ref())
            .with_primary_key(["nonexistent_column"]);
        let error = register_parquet_table(&ctx, &spec)
            .await
            .expect_err("an unknown key column must be rejected");
        let message = error.to_string();
        assert!(
            message.contains("nonexistent_column") && message.contains("not in table"),
            "the error must name the offending column and the table: {message}"
        );
    }

    /// ADR-0003 risk gate: a scan→filter→hash-aggregate plan round-trips
    /// An ungrouped aggregate must split into stages.
    ///
    /// `SELECT sum(x) FROM t` plans as Final(gather(Partial(scan))) — there is
    /// no hash exchange anywhere, because there are no grouping keys to hash
    /// on. A cutter that only recognised `RepartitionExec` therefore declined
    /// the entire query class and ran it as one task: TPC-H q6 at SF100 took
    /// 518 s on a 3-node cluster with two nodes idle. The work is
    /// embarrassingly parallel — partial aggregates per file group, combined
    /// once — so declining was a pure loss.
    ///
    /// Asserting on stage COUNT is what makes this a regression test: a plan
    /// that merely round-trips proves nothing about distribution.
    #[test]
    fn target_partitions_scale_with_the_cluster_not_a_constant() {
        // The defect: this was 4 regardless of the cluster, so a large cluster
        // sat mostly idle and a small one queued work behind itself.
        let two_slots = ClusterCapacity { total_slots: 2 };
        let thirty_two = ClusterCapacity { total_slots: 32 };
        let small = derive_stage_target_partitions(None, Some(two_slots), 8);
        let large = derive_stage_target_partitions(None, Some(thirty_two), 8);
        assert!(
            large > small,
            "a 16x larger cluster planned {large} vs {small} partitions"
        );
        assert_eq!(large, 32 * TASKS_PER_SLOT);
    }

    #[test]
    fn multiple_waves_per_slot_leave_room_to_absorb_stragglers() {
        // One task per slot makes a stage as slow as its slowest task. More
        // tasks than slots lets a fast slot take a second while a slow one is
        // still on its first.
        let cluster = ClusterCapacity { total_slots: 8 };
        assert!(
            derive_stage_target_partitions(None, Some(cluster), 8) > cluster.total_slots,
            "a stage should plan more tasks than slots, not exactly one wave"
        );
    }

    #[test]
    fn an_explicit_setting_overrides_the_derivation() {
        let cluster = ClusterCapacity { total_slots: 64 };
        assert_eq!(derive_stage_target_partitions(Some(6), Some(cluster), 8), 6);
        // ...but a value that would defeat stage splitting entirely does not:
        // below 2 partitions there is no exchange to cut.
        assert!(derive_stage_target_partitions(Some(1), Some(cluster), 8) >= MIN_STAGE_PARTITIONS);
        assert!(derive_stage_target_partitions(Some(0), Some(cluster), 8) >= MIN_STAGE_PARTITIONS);
    }

    #[test]
    fn no_cluster_view_falls_back_to_the_local_machine() {
        // The embedded runtime and any caller without a coordinator.
        assert_eq!(
            derive_stage_target_partitions(None, None, 6),
            6 * TASKS_PER_SLOT
        );
    }

    #[test]
    fn partition_counts_stay_inside_the_shuffle_fragment_budget() {
        // Shuffle fragments grow as partitions², so an enormous cluster must
        // not translate into an unbounded fragment count.
        let huge = ClusterCapacity {
            total_slots: usize::MAX,
        };
        assert_eq!(
            derive_stage_target_partitions(None, Some(huge), 8),
            MAX_STAGE_PARTITIONS
        );
        // A single-slot cluster still gets a splittable plan.
        let one = ClusterCapacity { total_slots: 1 };
        assert!(derive_stage_target_partitions(None, Some(one), 1) >= MIN_STAGE_PARTITIONS);
    }

    #[tokio::test]
    async fn ungrouped_aggregate_splits_into_map_and_reduce_stages() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = write_test_parquet(tmp.path()).await;
        let tables = vec![(
            String::from("t"),
            path.to_str().expect("utf8 path").to_owned(),
        )];

        let staged = build_stages_for_parquet_query(
            "SELECT SUM(amount) AS total, COUNT(*) AS n FROM t WHERE id >= 100",
            &tables,
            Some(ClusterCapacity { total_slots: 4 }),
        )
        .await
        .expect("planning must not error")
        .expect("an ungrouped aggregate must be stage-split, not declined");

        assert!(
            staged.stages.len() >= 2,
            "expected a map stage and a reduce stage, got {} stage(s) — \
             the gather was not cut, so the whole scan runs in one task",
            staged.stages.len()
        );

        // The map stage gathers to exactly one reduce partition, and carries no
        // hash key: every row goes to partition 0, which is what a gather means.
        let map = &staged.stages[0];
        let shuffle = map
            .shuffle
            .as_ref()
            .expect("the map stage must write a shuffle output");
        assert_eq!(
            shuffle.num_output_partitions, 1,
            "a gather must produce exactly one reduce partition"
        );
        assert!(
            shuffle.key_columns.is_empty(),
            "a gather has no partitioning key; got {:?}",
            shuffle.key_columns
        );
    }

    /// A grouped aggregate keeps cutting at the hash exchange, with real hash
    /// keys — the gather cut must not have swallowed that path.
    #[tokio::test]
    async fn grouped_aggregate_still_cuts_at_the_hash_exchange() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = write_test_parquet(tmp.path()).await;
        let tables = vec![(
            String::from("t"),
            path.to_str().expect("utf8 path").to_owned(),
        )];

        let staged = build_stages_for_parquet_query(
            "SELECT category, SUM(amount) AS total FROM t GROUP BY category",
            &tables,
            Some(ClusterCapacity { total_slots: 4 }),
        )
        .await
        .expect("planning must not error")
        .expect("a grouped aggregate must be stage-split");

        let map = &staged.stages[0];
        let shuffle = map.shuffle.as_ref().expect("map stage writes a shuffle");
        assert_eq!(
            shuffle.key_columns,
            vec![String::from("category")],
            "a grouped aggregate must shuffle on its grouping key"
        );
    }

    /// through datafusion-proto on the pinned DataFusion and executes
    /// identically from a fresh context.
    #[tokio::test]
    async fn aggregate_plan_round_trips_through_proto() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = write_test_parquet(tmp.path()).await;

        let ctx = SessionContext::new();
        ctx.register_parquet(
            "t",
            path.to_str().expect("utf8 path"),
            datafusion::prelude::ParquetReadOptions::default(),
        )
        .await
        .expect("register parquet");
        let df = ctx
            .sql("SELECT category, COUNT(*) AS n, SUM(amount) AS total FROM t WHERE id >= 100 GROUP BY category")
            .await
            .expect("sql");
        let plan = df.create_physical_plan().await.expect("physical plan");
        let original_display = displayable(plan.as_ref()).indent(true).to_string();

        let codec = DefaultPhysicalExtensionCodec {};
        let bytes = encode_dfplan_bytes(Arc::clone(&plan), &codec).expect("encode");
        let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let body = dfplan_task_body(&b64, 0);
        assert!(is_dfplan_body(&body));

        // Decode on a FRESH context with no tables registered — the executor
        // side never re-registers coordinator tables.
        let exec_ctx = SessionContext::new();
        let (spec, decoded) =
            decode_dfplan_task(&body, &exec_ctx.task_ctx(), &codec).expect("decode");
        assert_eq!(spec, DfplanTaskSpec::single(0));
        assert_eq!(
            original_display,
            displayable(decoded.as_ref()).indent(true).to_string(),
            "decoded plan display must match original"
        );

        let task_ctx = exec_ctx.task_ctx();
        let mut results = Vec::new();
        for partition in 0..decoded.output_partitioning().partition_count() {
            let stream = decoded
                .execute(partition, Arc::clone(&task_ctx))
                .expect("execute decoded partition");
            let batches: Vec<_> = futures::TryStreamExt::try_collect(stream)
                .await
                .expect("collect decoded stream");
            results.extend(batches);
        }
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "three category groups expected");
    }

    #[test]
    fn non_dfplan_body_is_rejected() {
        let err = parse_dfplan_body("sql: SELECT 1").unwrap_err();
        assert!(err.to_string().contains("not a dfplan:v1: fragment"));
    }

    /// The shuffle seam names the producer when the two sides disagree.
    ///
    /// Without this check the mismatch flows on and dies in whichever
    /// downstream operator first builds a batch against the plan's schema —
    /// q17's bare `expected Decimal128(15, 2) but found Decimal128(30, 15)`,
    /// which identifies no stage, no partition and no map task.
    #[test]
    fn a_shuffle_batch_that_contradicts_the_declared_schema_is_named_not_passed_on() {
        use arrow::array::{Int64Array, StringViewArray};
        use arrow::datatypes::{DataType, Field, Schema};

        // q19's exact disagreement: the plan declares the revenue decimal, the
        // batch carries a `Utf8View` string column (Parquet reads produce view
        // types by default in DataFusion 54).
        let declared: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "revenue",
            DataType::Decimal128(15, 2),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "p_brand",
                DataType::Utf8View,
                false,
            )])),
            vec![Arc::new(StringViewArray::from(vec!["Brand#23"]))],
        )
        .expect("utf8view batch");

        let error = check_shuffle_batch_schema(&declared, batch, 3, 7, 5)
            .expect_err("a contradicting batch must not be passed on");
        let text = error.to_string();
        for expected in ["stage 3", "map 7", "partition 5", "revenue", "p_brand"] {
            assert!(
                text.contains(expected),
                "error must name {expected}, got: {text}"
            );
        }

        // Arity disagreement is reported too, and separately.
        let two_col = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![2])),
            ],
        )
        .expect("two column batch");
        let error = check_shuffle_batch_schema(&declared, two_col, 0, 0, 0)
            .expect_err("column-count disagreement must not be passed on");
        assert!(
            error
                .to_string()
                .contains("2 columns but the plan declares 1"),
            "got: {error}"
        );
    }

    /// The check must not reject a batch that merely carries different field
    /// metadata or nullability — those differ harmlessly across a Parquet read
    /// and an IPC round trip, and rejecting them would fail correct queries.
    #[test]
    fn matching_column_types_pass_even_when_metadata_and_nullability_differ() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let declared: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false).with_metadata(
                [(String::from("origin"), String::from("coordinator"))]
                    .into_iter()
                    .collect(),
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("batch");

        check_shuffle_batch_schema(&declared, batch, 0, 0, 0)
            .expect("metadata and nullability differences must not fail the query");
    }

    /// In-memory [`ShufflePartitionReader`] + writer used to execute a
    /// stage plan end-to-end in tests (the executor's store stands in).
    #[derive(Debug, Default)]
    struct TestShuffleStore {
        partitions: Mutex<HashMap<(usize, usize, usize), Vec<RecordBatch>>>,
    }

    impl TestShuffleStore {
        fn write(&self, stage: usize, map_task: usize, partition: usize, batch: RecordBatch) {
            self.partitions
                .lock()
                .expect("store lock")
                .entry((stage, map_task, partition))
                .or_default()
                .push(batch);
        }
    }

    impl ShufflePartitionReader for Arc<TestShuffleStore> {
        fn open_partition(
            &self,
            upstream_stage_index: usize,
            map_task_index: usize,
            partition: usize,
        ) -> futures::future::BoxFuture<'static, Result<ShuffleFragmentStream, String>> {
            let batches = self
                .partitions
                .lock()
                .expect("store lock")
                .get(&(upstream_stage_index, map_task_index, partition))
                .cloned()
                .unwrap_or_default();
            Box::pin(async move {
                Ok(Box::pin(futures::stream::iter(batches.into_iter().map(Ok)))
                    as ShuffleFragmentStream)
            })
        }
    }

    /// A reader that models the shuffle server's `serve_permits`: a permit is
    /// taken before the fragment is served and released only when the response
    /// stream is fully consumed.
    ///
    /// `reverse_open_order` makes the *last* map task acquire first and the
    /// first acquire last. Without it the deadlock is not reproducible, because
    /// `buffered` polls the futures in order, so map task 0 wins the permit race
    /// by accident and the whole read drains sequentially. The production race is
    /// decided by network timing, not poll order, so the ordering must be forced
    /// to test the invariant rather than the scheduler's luck.
    #[derive(Debug)]
    struct ServeLimitedReader {
        inner: Arc<TestShuffleStore>,
        permits: Arc<tokio::sync::Semaphore>,
        reverse_open_order: bool,
        map_tasks: usize,
    }

    impl ShufflePartitionReader for ServeLimitedReader {
        fn open_partition(
            &self,
            stage: usize,
            map_task: usize,
            partition: usize,
        ) -> futures::future::BoxFuture<'static, Result<ShuffleFragmentStream, String>> {
            let batches = self
                .inner
                .partitions
                .lock()
                .expect("store lock")
                .get(&(stage, map_task, partition))
                .cloned()
                .unwrap_or_default();
            let permits = Arc::clone(&self.permits);
            let delay = if self.reverse_open_order {
                // Later map tasks reach the semaphore first.
                20 * (self.map_tasks.saturating_sub(map_task)) as u64
            } else {
                0
            };
            Box::pin(async move {
                if delay > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                }
                let permit = permits
                    .acquire_owned()
                    .await
                    .map_err(|_| String::from("serve semaphore closed"))?;
                // The permit rides along with the stream, exactly as
                // `PermitHoldingStream` does on the server.
                let held = futures::stream::iter(batches.into_iter().map(Ok)).chain(
                    futures::stream::unfold(Some(permit), |permit| async move {
                        // Releasing the permit only when the stream is fully
                        // consumed is the whole point: that is what the server's
                        // `PermitHoldingStream` does.
                        drop(permit?);
                        None
                    }),
                );
                Ok(Box::pin(held) as ShuffleFragmentStream)
            })
        }
    }

    /// A reduce read must complete when the producer serves fewer concurrent
    /// responses than the reduce side has fragments to read.
    ///
    /// This pins the second deadlock found on 2026-07-30, and it is a *cluster*
    /// hang rather than a slow query: `ShuffleFlightService::do_get` holds a
    /// `serve_permits` permit for its response stream's lifetime, so a client
    /// that opens `n` fragments ahead and drains them in order holds `n` server
    /// permits while consuming one. With every executor acting as both client and
    /// server the waits form cycles across nodes, nothing times out, and the job
    /// sits at 0% CPU forever. Measured live: TPC-H q2 wedged at 132/181 tasks
    /// with a prefetch of 8, and ran in 104.3 s on the identical image with a
    /// prefetch of 1.
    ///
    /// Four fragments against one serve permit is the smallest case that
    /// reproduces it. If `DEFAULT_SHUFFLE_FETCH_BUFFER` is ever raised without
    /// first changing `do_get` to bound resident bytes instead of open
    /// responses, this test hangs and the timeout fails it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_reduce_read_completes_even_when_the_producer_serves_one_at_a_time() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let store = Arc::new(TestShuffleStore::default());
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let map_tasks = 4usize;
        for map_task in 0..map_tasks {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![map_task as i64; 3]))],
            )
            .expect("batch");
            store.write(0, map_task, 0, batch);
        }

        let reader: Arc<dyn ShufflePartitionReader> = Arc::new(ServeLimitedReader {
            inner: Arc::clone(&store),
            // One fewer permit than there are fragments, and the LAST map task
            // reaches the semaphore first. With a prefetch of `map_tasks` the
            // later fragments take every permit, map task 0 waits for one, and
            // nothing releases because `buffered` cannot yield fragment 1 before
            // fragment 0 has opened.
            permits: Arc::new(tokio::sync::Semaphore::new(map_tasks - 1)),
            reverse_open_order: true,
            map_tasks,
        });
        let read = ShuffleReadExec::new(0, map_tasks, 1, Arc::clone(&schema), Some(reader));
        let ctx = SessionContext::new();
        let stream = read.execute(0, ctx.task_ctx()).expect("execute");

        let batches = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            futures::TryStreamExt::try_collect::<Vec<_>>(stream),
        )
        .await
        .expect(
            "the reduce read deadlocked: it is holding more producer response \
             streams open than the producer will serve, and only downstream \
             consumption releases them",
        )
        .expect("read");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, map_tasks * 3, "every fragment's rows must arrive");
    }

    /// Consistent test-side hash partitioner (any consistent hash is
    /// correct; the executor uses krishiv-shuffle's seeded partitioner).
    fn partition_batch_by_key(
        batch: &RecordBatch,
        key_column: &str,
        num_partitions: usize,
    ) -> Vec<RecordBatch> {
        use std::hash::{Hash as _, Hasher as _};
        let key_idx = batch.schema().index_of(key_column).expect("key column");
        let column = batch.column(key_idx);
        let mut selections: Vec<Vec<u32>> = vec![Vec::new(); num_partitions];
        for row in 0..batch.num_rows() {
            let value = arrow::util::display::array_value_to_string(column, row).expect("value");
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            value.hash(&mut hasher);
            let bucket = (hasher.finish() as usize) % num_partitions;
            selections[bucket].push(row as u32);
        }
        selections
            .into_iter()
            .map(|rows| {
                let indices = arrow::array::UInt32Array::from(rows);
                arrow::compute::take_record_batch(batch, &indices).expect("take")
            })
            .collect()
    }

    /// End-to-end stage execution: build stages for a GROUP BY, execute the
    /// map tasks (hash-partition into the test store), execute the result
    /// stage through ShuffleReadExec, and compare with direct execution.
    #[tokio::test]
    async fn staged_group_by_matches_direct_execution() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = write_test_parquet(tmp.path()).await;

        let plan_ctx = planning_session_context(4);
        plan_ctx
            .register_parquet(
                "t",
                path.to_str().expect("utf8 path"),
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await
            .expect("register parquet");
        let query = "SELECT category, COUNT(*) AS n, SUM(amount) AS total FROM t GROUP BY category ORDER BY category";
        let df = plan_ctx.sql(query).await.expect("sql");
        let plan = df.create_physical_plan().await.expect("physical plan");

        let staged = build_distributed_stages(plan)
            .expect("build stages")
            .expect("plan must be splittable");
        assert_eq!(staged.stages.len(), 2, "one map stage + one result stage");
        let map_stage = &staged.stages[0];
        let result_stage = &staged.stages[1];
        let shuffle = map_stage.shuffle.as_ref().expect("map stage shuffles");
        assert_eq!(shuffle.key_columns, vec!["category".to_owned()]);
        assert!(
            map_stage.task_count() > 1,
            "multi-file scan must yield a multi-task map stage, got {}",
            map_stage.task_count()
        );
        assert!(result_stage.shuffle.is_none());
        assert_eq!(result_stage.upstream_stage_indexes, vec![0]);

        // Execute map tasks: each runs its partition of the decoded subtree
        // and hash-partitions the output into the test store.
        let store = Arc::new(TestShuffleStore::default());
        let exec_ctx = SessionContext::new();
        let exec_codec = KrishivPhysicalCodec::executor(Arc::new(Arc::clone(&store)));
        for (task_index, body) in map_stage.task_bodies.iter().enumerate() {
            let (spec, plan) =
                decode_dfplan_task(body, &exec_ctx.task_ctx(), &exec_codec).expect("decode map");
            assert_eq!(spec, DfplanTaskSpec::single(task_index));
            let stream = plan
                .execute(task_index, exec_ctx.task_ctx())
                .expect("execute map partition");
            let batches: Vec<_> = futures::TryStreamExt::try_collect(stream)
                .await
                .expect("collect map output");
            for batch in batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                for (bucket, part) in partition_batch_by_key(
                    &batch,
                    &shuffle.key_columns[0],
                    shuffle.num_output_partitions,
                )
                .into_iter()
                .enumerate()
                {
                    if part.num_rows() > 0 {
                        store.write(0, task_index, bucket, part);
                    }
                }
            }
        }

        // Execute the result stage through ShuffleReadExec.
        let mut staged_results = Vec::new();
        for (task_index, body) in result_stage.task_bodies.iter().enumerate() {
            let (spec, plan) =
                decode_dfplan_task(body, &exec_ctx.task_ctx(), &exec_codec).expect("decode result");
            assert_eq!(spec, DfplanTaskSpec::single(task_index));
            let stream = plan
                .execute(task_index, exec_ctx.task_ctx())
                .expect("execute result partition");
            let batches: Vec<_> = futures::TryStreamExt::try_collect(stream)
                .await
                .expect("collect result output");
            staged_results.extend(batches);
        }

        let direct = plan_ctx
            .sql(query)
            .await
            .expect("direct sql")
            .collect()
            .await
            .expect("direct collect");

        let render = |batches: &[RecordBatch]| {
            let mut rows: Vec<String> = batches
                .iter()
                .flat_map(|b| {
                    (0..b.num_rows()).map(move |r| {
                        (0..b.num_columns())
                            .map(|c| {
                                arrow::util::display::array_value_to_string(b.column(c), r)
                                    .expect("cell")
                            })
                            .collect::<Vec<_>>()
                            .join("|")
                    })
                })
                .collect();
            rows.sort();
            rows
        };
        assert_eq!(
            render(&staged_results),
            render(&direct),
            "staged execution must match direct execution"
        );
    }

    /// A plain scan (no exchange) is not worth splitting: builder says None.
    #[tokio::test]
    async fn scan_only_plan_declines_with_a_stated_reason() {
        // A projection-and-filter plan has no exchange, so there is nothing to
        // cut and it correctly runs as one task. What changed is how that is
        // reported: declining used to be a bare `Ok(None)`, indistinguishable
        // at the call site from every other reason to fall back, which is how
        // a genuine planning bug hid behind "the planner declined" for a whole
        // benchmarking session. The reason is now a value, and this asserts it
        // says which plan property was missing.
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = write_test_parquet(tmp.path()).await;
        let plan_ctx = planning_session_context(4);
        plan_ctx
            .register_parquet(
                "t",
                path.to_str().expect("utf8 path"),
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await
            .expect("register parquet");
        let df = plan_ctx
            .sql("SELECT id, amount FROM t WHERE id < 10")
            .await
            .expect("sql");
        let plan = df.create_physical_plan().await.expect("physical plan");
        let reason = build_distributed_stages(plan)
            .expect_err("a scan-only plan has no exchange and must decline")
            .to_string();
        assert!(
            reason.contains("no exchange"),
            "the decline must name the missing plan property, got: {reason}"
        );
    }

    /// Hash-join splits into two map stages + a result stage, and staged
    /// execution matches direct execution.
    #[tokio::test]
    async fn staged_join_matches_direct_execution() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = write_test_parquet(tmp.path()).await;

        // Force a partitioned (repartition-both-sides) hash join: the test
        // table is tiny, and DF would otherwise broadcast it below the
        // single-partition thresholds — which the builder correctly declines
        // to split (`scan_only_plan_is_not_split` covers that shape).
        let mut config = SessionConfig::new().with_target_partitions(4);
        config
            .options_mut()
            .optimizer
            .enable_round_robin_repartition = false;
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 0;
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 0;
        let plan_ctx = SessionContext::new_with_config(config);
        for name in ["a", "b"] {
            plan_ctx
                .register_parquet(
                    name,
                    path.to_str().expect("utf8 path"),
                    datafusion::prelude::ParquetReadOptions::default(),
                )
                .await
                .expect("register parquet");
        }
        let query = "SELECT a.category, COUNT(*) AS n, SUM(b.amount) AS total \
                     FROM a JOIN b ON a.id = b.id GROUP BY a.category";
        let df = plan_ctx.sql(query).await.expect("sql");
        let plan = df.create_physical_plan().await.expect("physical plan");
        let staged = build_distributed_stages(plan)
            .expect("build stages")
            .expect("partitioned join must split into stages");
        assert!(
            staged.stages.len() >= 3,
            "expected two join-side map stages + result, got {}",
            staged.stages.len()
        );

        let store = Arc::new(TestShuffleStore::default());
        let exec_ctx = SessionContext::new();
        let exec_codec = KrishivPhysicalCodec::executor(Arc::new(Arc::clone(&store)));

        // Execute stages in order (map stages precede the result stage).
        let mut staged_results = Vec::new();
        for (stage_index, stage) in staged.stages.iter().enumerate() {
            for (task_index, body) in stage.task_bodies.iter().enumerate() {
                let (spec, plan) = decode_dfplan_task(body, &exec_ctx.task_ctx(), &exec_codec)
                    .expect("decode stage task");
                assert_eq!(spec, DfplanTaskSpec::single(task_index));
                let stream = plan
                    .execute(task_index, exec_ctx.task_ctx())
                    .expect("execute stage partition");
                let batches: Vec<_> = futures::TryStreamExt::try_collect(stream)
                    .await
                    .expect("collect stage output");
                match &stage.shuffle {
                    Some(shuffle) => {
                        for batch in batches {
                            if batch.num_rows() == 0 {
                                continue;
                            }
                            for (bucket, part) in partition_batch_by_key(
                                &batch,
                                &shuffle.key_columns[0],
                                shuffle.num_output_partitions,
                            )
                            .into_iter()
                            .enumerate()
                            {
                                if part.num_rows() > 0 {
                                    store.write(stage_index, task_index, bucket, part);
                                }
                            }
                        }
                    }
                    None => staged_results.extend(batches),
                }
            }
        }

        let direct = plan_ctx
            .sql(query)
            .await
            .expect("direct sql")
            .collect()
            .await
            .expect("direct collect");

        let render = |batches: &[RecordBatch]| {
            let mut rows: Vec<String> = batches
                .iter()
                .flat_map(|b| {
                    (0..b.num_rows()).map(move |r| {
                        (0..b.num_columns())
                            .map(|c| {
                                arrow::util::display::array_value_to_string(b.column(c), r)
                                    .expect("cell")
                            })
                            .collect::<Vec<_>>()
                            .join("|")
                    })
                })
                .collect();
            rows.sort();
            rows
        };
        assert_eq!(
            render(&staged_results),
            render(&direct),
            "staged join must match direct execution"
        );
    }

    // ── Phase 54: partition-spec grammar ─────────────────────────────────

    #[test]
    fn partition_spec_grammar_round_trips() {
        let multi = DfplanTaskSpec {
            partitions: vec![1, 4, 7],
            map_range: None,
        };
        let body = dfplan_task_body_for_spec("QUJD", &multi);
        assert_eq!(body, "dfplan:v1:1,4,7:QUJD");
        assert_eq!(dfplan_body_partition_spec(&body).expect("parse"), multi);

        let split = DfplanTaskSpec {
            partitions: vec![5],
            map_range: Some(DfplanMapRange {
                upstream_stage_index: 0,
                start: 2,
                end: 4,
            }),
        };
        let body = dfplan_task_body_for_spec("QUJD", &split);
        assert_eq!(body, "dfplan:v1:5/s0m2-4:QUJD");
        assert_eq!(dfplan_body_partition_spec(&body).expect("parse"), split);

        // Legacy single-partition form parses as a single spec.
        assert_eq!(
            dfplan_body_partition_spec("dfplan:v1:3:QUJD").expect("parse"),
            DfplanTaskSpec::single(3)
        );
    }

    #[test]
    fn partition_spec_rewrite_preserves_payload() {
        let original = dfplan_task_body("cGF5bG9hZA==", 2);
        let rewritten = dfplan_body_with_spec(
            &original,
            &DfplanTaskSpec {
                partitions: vec![0, 2],
                map_range: None,
            },
        )
        .expect("rewrite");
        assert_eq!(rewritten, "dfplan:v1:0,2:cGF5bG9hZA==");
    }

    #[test]
    fn partition_spec_rejects_malformed_segments() {
        assert!(dfplan_body_partition_spec("dfplan:v1::QUJD").is_err());
        assert!(dfplan_body_partition_spec("dfplan:v1:x:QUJD").is_err());
        assert!(dfplan_body_partition_spec("dfplan:v1:1/s0m4-4:QUJD").is_err());
        assert!(dfplan_body_partition_spec("dfplan:v1:1/m0-2:QUJD").is_err());
    }

    /// Coalescing correctness: a Result-stage task executing SEVERAL root
    /// partitions produces exactly the union the one-task-per-partition
    /// layout produces (the exit-gate mechanism for AQE coalescing).
    #[tokio::test]
    async fn coalesced_result_stage_matches_direct_execution() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = write_test_parquet(tmp.path()).await;
        let plan_ctx = planning_session_context(4);
        plan_ctx
            .register_parquet(
                "t",
                path.to_str().expect("utf8 path"),
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await
            .expect("register parquet");
        let query = "SELECT category, COUNT(*) AS n, SUM(amount) AS total FROM t GROUP BY category";
        let df = plan_ctx.sql(query).await.expect("sql");
        let plan = df.create_physical_plan().await.expect("physical plan");
        let staged = build_distributed_stages(plan)
            .expect("build stages")
            .expect("splittable");
        let map_stage = staged.stages.first().expect("map stage");
        let result_stage = staged.stages.get(1).expect("result stage");
        let shuffle = map_stage.shuffle.as_ref().expect("map shuffles");

        // Run the map stage into the test store (as in the staged tests).
        let store = Arc::new(TestShuffleStore::default());
        let exec_ctx = SessionContext::new();
        for (task_index, body) in map_stage.task_bodies.iter().enumerate() {
            let reader: Arc<dyn ShufflePartitionReader> = Arc::new(Arc::clone(&store));
            let (_, mut stream) =
                execute_dfplan_body(body, &exec_ctx, Some(reader)).expect("map exec");
            while let Some(batch) = futures::StreamExt::next(&mut stream).await {
                let batch = batch.expect("map batch");
                if batch.num_rows() == 0 {
                    continue;
                }
                for (bucket, part) in partition_batch_by_key(
                    &batch,
                    &shuffle.key_columns[0],
                    shuffle.num_output_partitions,
                )
                .into_iter()
                .enumerate()
                {
                    if part.num_rows() > 0 {
                        store.write(0, task_index, bucket, part);
                    }
                }
            }
        }

        // ONE coalesced task executing every result partition.
        let all_partitions: Vec<usize> = (0..result_stage.task_count()).collect();
        let coalesced_body = dfplan_body_with_spec(
            result_stage.task_bodies.first().expect("result body"),
            &DfplanTaskSpec {
                partitions: all_partitions,
                map_range: None,
            },
        )
        .expect("coalesce rewrite");
        let reader: Arc<dyn ShufflePartitionReader> = Arc::new(Arc::clone(&store));
        let (_, stream) =
            execute_dfplan_body(&coalesced_body, &exec_ctx, Some(reader)).expect("coalesced exec");
        let coalesced: Vec<RecordBatch> = futures::TryStreamExt::try_collect(stream)
            .await
            .expect("coalesced results");

        // Per-partition baseline through the ORIGINAL bodies.
        let mut baseline = Vec::new();
        for body in &result_stage.task_bodies {
            let reader: Arc<dyn ShufflePartitionReader> = Arc::new(Arc::clone(&store));
            let (_, stream) =
                execute_dfplan_body(body, &exec_ctx, Some(reader)).expect("baseline exec");
            let batches: Vec<RecordBatch> = futures::TryStreamExt::try_collect(stream)
                .await
                .expect("baseline results");
            baseline.extend(batches);
        }

        let render = |batches: &[RecordBatch]| {
            let mut rows: Vec<String> = batches
                .iter()
                .flat_map(|b| {
                    (0..b.num_rows()).map(move |r| {
                        (0..b.num_columns())
                            .map(|c| {
                                arrow::util::display::array_value_to_string(b.column(c), r)
                                    .expect("cell")
                            })
                            .collect::<Vec<_>>()
                            .join("|")
                    })
                })
                .collect();
            rows.sort();
            rows
        };
        assert_eq!(
            render(&coalesced),
            render(&baseline),
            "coalesced task must produce the same union as per-partition tasks"
        );
        assert!(!coalesced.is_empty(), "group-by must produce rows");
    }

    /// Skew-split correctness: splitting a Result-stage partition of a pure
    /// inner join into map-task ranges yields the same union as the unsplit
    /// task (the exit-gate mechanism for AQE skew handling), and the
    /// split-safety gate admits the join while rejecting an aggregation.
    #[tokio::test]
    async fn skew_split_result_tasks_match_unsplit_execution() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = write_test_parquet(tmp.path()).await;

        let mut config = SessionConfig::new().with_target_partitions(4);
        config
            .options_mut()
            .optimizer
            .enable_round_robin_repartition = false;
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 0;
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 0;
        let plan_ctx = SessionContext::new_with_config(config);
        for name in ["a", "b"] {
            plan_ctx
                .register_parquet(
                    name,
                    path.to_str().expect("utf8 path"),
                    datafusion::prelude::ParquetReadOptions::default(),
                )
                .await
                .expect("register parquet");
        }
        // Pure inner join — no blocking operator above the shuffle reads.
        let query = "SELECT a.id, a.category, b.amount FROM a JOIN b ON a.id = b.id";
        let df = plan_ctx.sql(query).await.expect("sql");
        let plan = df.create_physical_plan().await.expect("physical plan");
        let staged = build_distributed_stages(plan)
            .expect("build stages")
            .expect("partitioned join must split");
        let result_stage = staged.stages.last().expect("result stage");
        assert!(result_stage.shuffle.is_none());
        let result_body = result_stage.task_bodies.first().expect("result body");
        assert!(
            dfplan_body_is_split_safe(result_body),
            "pure inner join result stage must be split-safe"
        );

        // Execute all map stages into the store.
        let store = Arc::new(TestShuffleStore::default());
        let exec_ctx = SessionContext::new();
        let mut probe_map_tasks = 0usize;
        for (stage_index, stage) in staged.stages.iter().enumerate() {
            let Some(shuffle) = &stage.shuffle else {
                continue;
            };
            if stage_index == 0 {
                probe_map_tasks = stage.task_count();
            }
            for (task_index, body) in stage.task_bodies.iter().enumerate() {
                let reader: Arc<dyn ShufflePartitionReader> = Arc::new(Arc::clone(&store));
                let (_, stream) =
                    execute_dfplan_body(body, &exec_ctx, Some(reader)).expect("map exec");
                let batches: Vec<RecordBatch> = futures::TryStreamExt::try_collect(stream)
                    .await
                    .expect("map results");
                for batch in batches {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    for (bucket, part) in partition_batch_by_key(
                        &batch,
                        &shuffle.key_columns[0],
                        shuffle.num_output_partitions,
                    )
                    .into_iter()
                    .enumerate()
                    {
                        if part.num_rows() > 0 {
                            store.write(stage_index, task_index, bucket, part);
                        }
                    }
                }
            }
        }
        assert!(
            probe_map_tasks >= 2,
            "need >=2 map tasks to split, got {probe_map_tasks}"
        );

        let collect_body = |body: String| {
            let store = Arc::clone(&store);
            let exec_ctx = exec_ctx.clone();
            async move {
                let reader: Arc<dyn ShufflePartitionReader> = Arc::new(store);
                let (_, stream) =
                    execute_dfplan_body(&body, &exec_ctx, Some(reader)).expect("exec");
                let batches: Vec<RecordBatch> = futures::TryStreamExt::try_collect(stream)
                    .await
                    .expect("results");
                batches
            }
        };

        let render = |batches: &[RecordBatch]| {
            let mut rows: Vec<String> = batches
                .iter()
                .flat_map(|b| {
                    (0..b.num_rows()).map(move |r| {
                        (0..b.num_columns())
                            .map(|c| {
                                arrow::util::display::array_value_to_string(b.column(c), r)
                                    .expect("cell")
                            })
                            .collect::<Vec<_>>()
                            .join("|")
                    })
                })
                .collect();
            rows.sort();
            rows
        };

        // Every result partition: unsplit baseline vs two map-range splits
        // of upstream stage 0 (the probe side in builder order).
        for (partition, body) in result_stage.task_bodies.iter().enumerate() {
            let baseline = collect_body(body.clone()).await;
            let mid = probe_map_tasks / 2;
            let mut split_union = Vec::new();
            for (start, end) in [(0, mid), (mid, probe_map_tasks)] {
                let split_body = dfplan_body_with_spec(
                    body,
                    &DfplanTaskSpec {
                        partitions: vec![partition],
                        map_range: Some(DfplanMapRange {
                            upstream_stage_index: 0,
                            start,
                            end,
                        }),
                    },
                )
                .expect("split rewrite");
                split_union.extend(collect_body(split_body).await);
            }
            assert_eq!(
                render(&split_union),
                render(&baseline),
                "partition {partition}: split union must equal unsplit output"
            );
        }

        // The safety gate must reject a plan with a blocking aggregation.
        let agg_ctx = planning_session_context(4);
        agg_ctx
            .register_parquet(
                "t",
                path.to_str().expect("utf8 path"),
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await
            .expect("register parquet");
        let agg_plan = agg_ctx
            .sql("SELECT category, COUNT(*) FROM t GROUP BY category")
            .await
            .expect("sql")
            .create_physical_plan()
            .await
            .expect("plan");
        let agg_staged = build_distributed_stages(agg_plan)
            .expect("build stages")
            .expect("splittable");
        let agg_body = agg_staged
            .stages
            .last()
            .expect("result stage")
            .task_bodies
            .first()
            .expect("body");
        assert!(
            !dfplan_body_is_split_safe(agg_body),
            "final aggregation must NOT be split-safe"
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod roundtrip_schema_guard_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    /// A schema deliberately unlike anything the encoded plan produces.
    fn alien_schema() -> Schema {
        Schema::new(vec![Field::new(
            "not_a_real_column",
            DataType::Boolean,
            true,
        )])
    }

    #[tokio::test]
    async fn the_guard_rejects_a_decode_whose_schema_differs() {
        // The property the guard was missing. It only ever checked that decode
        // *succeeded*, so a fragment could decode into a plan producing
        // different column types and ship anyway — `ShuffleReadExec` labels its
        // stream with the coordinator's schema, `RecordBatchStreamAdapter` does
        // not validate, and the disagreement surfaced much later inside an
        // executor as a bare Arrow error (q17: Decimal128(15,2) declared,
        // Decimal128(30,15) produced).
        let ctx = fragment_decode_session_context();
        ctx.sql("CREATE TABLE t(a INT) AS VALUES (1), (2)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let plan = ctx
            .sql("SELECT a FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let codec = KrishivPhysicalCodec::coordinator();
        let bytes = encode_dfplan_bytes(Arc::clone(&plan), &codec).unwrap();
        let task_ctx = ctx.task_ctx();

        // Its own plan passes.
        verify_dfplan_roundtrip(&bytes, &codec, &task_ctx, Some(&plan))
            .expect("a plan must round-trip against itself");

        // A different plan is refused, and the message names the mismatch so
        // the fallback is explainable rather than mysterious.
        let alien: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::empty::EmptyExec::new(Arc::new(alien_schema())),
        );
        let err = verify_dfplan_roundtrip(&bytes, &codec, &task_ctx, Some(&alien))
            .expect_err("a schema disagreement must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("decoded plan differs"),
            "unexpected message: {msg}"
        );
    }

    /// The root-only check was not enough, and q17 is the proof.
    ///
    /// A decode can re-resolve an interior aggregate to a different type while
    /// a projection above it casts back, so the *root* schemas agree and the
    /// guard passes a fragment whose interior will not run. Comparing the tree
    /// is what makes the guard mean "the executor can rebuild this plan"
    /// rather than "the executor can rebuild this plan's last node".
    #[tokio::test]
    async fn the_guard_compares_the_whole_tree_not_just_the_root() {
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_plan::empty::EmptyExec;

        let same_root = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        // Two plans with identical root schemas and different interiors.
        let original: Arc<dyn ExecutionPlan> =
            Arc::new(datafusion::physical_plan::limit::GlobalLimitExec::new(
                Arc::new(EmptyExec::new(Arc::clone(&same_root))),
                0,
                None,
            ));
        let decoded: Arc<dyn ExecutionPlan> =
            Arc::new(datafusion::physical_plan::limit::GlobalLimitExec::new(
                Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new(
                    "a",
                    DataType::Int64,
                    true,
                )])))),
                0,
                None,
            ));
        // Roots agree only if the interiors do for CoalesceBatchesExec, so
        // assert on the child directly: the walk must reach it and name it.
        let difference = first_schema_difference(&original, &decoded, "root")
            .expect("an interior disagreement must be reported");
        assert!(
            difference.contains("root"),
            "the difference must name where it is: {difference}"
        );

        // Identical trees agree.
        assert!(first_schema_difference(&original, &original, "root").is_none());
    }

    #[tokio::test]
    async fn passing_no_expected_schema_keeps_the_old_decode_only_behaviour() {
        // Callers that only care whether the bytes decode (the existing
        // regression tests) must keep working unchanged.
        let ctx = fragment_decode_session_context();
        ctx.sql("CREATE TABLE t2(a INT) AS VALUES (1)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let plan = ctx
            .sql("SELECT a FROM t2")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let codec = KrishivPhysicalCodec::coordinator();
        let bytes = encode_dfplan_bytes(plan, &codec).unwrap();
        verify_dfplan_roundtrip(&bytes, &codec, &ctx.task_ctx(), None)
            .expect("decode-only checking must still pass");
    }
}

/// Staged TPC-H over a miniature fixture: the whole cut-encode-ship-execute
/// path, in process.
///
/// The SF100 cluster is the only place several of this module's defects have
/// ever appeared, and a cluster cycle costs an hour. These tests run the same
/// path — the same planner, the same stage cut, the same fragment bodies, the
/// same `ShuffleReadExec` — over a few hundred rows, so a schema disagreement
/// between what a stage *declares* and what it *produces* fails in seconds on
/// a laptop instead of in an overnight sweep.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod staged_tpch_tests {
    use super::*;
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::{ParquetReadOptions, SessionContext};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// q17 and q19 verbatim from the benchmark corpus (`krishiv-bench`), which
    /// is the point: a paraphrase would not reproduce the plan shape.
    const Q17: &str = "SELECT sum(l_extendedprice) / 7.0 AS avg_yearly FROM lineitem, part \
         WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX' \
         AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem \
                           WHERE l_partkey = p_partkey)";

    const Q19: &str = "SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue \
         FROM lineitem, part \
         WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12' \
           AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') \
           AND l_quantity >= 1 AND l_quantity <= 11 AND p_size BETWEEN 1 AND 5 \
           AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') \
         OR (p_partkey = l_partkey AND p_brand = 'Brand#23' \
           AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') \
           AND l_quantity >= 10 AND l_quantity <= 20 AND p_size BETWEEN 1 AND 10 \
           AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') \
         OR (p_partkey = l_partkey AND p_brand = 'Brand#34' \
           AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') \
           AND l_quantity >= 20 AND l_quantity <= 30 AND p_size BETWEEN 1 AND 15 \
           AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')";

    #[derive(Debug, Default)]
    struct StageStore {
        partitions: Mutex<HashMap<(usize, usize, usize), Vec<RecordBatch>>>,
    }

    impl ShufflePartitionReader for Arc<StageStore> {
        fn open_partition(
            &self,
            upstream_stage_index: usize,
            map_task_index: usize,
            partition: usize,
        ) -> futures::future::BoxFuture<'static, Result<ShuffleFragmentStream, String>> {
            let batches = self
                .partitions
                .lock()
                .expect("store lock")
                .get(&(upstream_stage_index, map_task_index, partition))
                .cloned()
                .unwrap_or_default();
            Box::pin(async move {
                Ok(Box::pin(futures::stream::iter(batches.into_iter().map(Ok)))
                    as ShuffleFragmentStream)
            })
        }
    }

    fn write_parquet(path: &std::path::Path, batch: &RecordBatch) {
        let file = std::fs::File::create(path).expect("create parquet");
        let mut writer =
            datafusion::parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)
                .expect("writer init");
        writer.write(batch).expect("write batch");
        writer.close().expect("close writer");
    }

    /// Miniature `lineitem` and `part`, two files each so map stages get more
    /// than one task. Column types match the TPC-H DDL — the `Decimal128(15,2)`
    /// money columns especially, since the defect under test is a decimal
    /// precision disagreement.
    fn write_tpch_fixture(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
        use arrow::array::{Decimal128Array, Int32Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let lineitem_schema = Arc::new(Schema::new(vec![
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_quantity", DataType::Decimal128(15, 2), false),
            Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
            Field::new("l_discount", DataType::Decimal128(15, 2), false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
        ]));
        let part_schema = Arc::new(Schema::new(vec![
            Field::new("p_partkey", DataType::Int64, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
        ]));
        let money = |values: Vec<i128>| -> Arc<dyn arrow::array::Array> {
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(15, 2)
                    .expect("decimal(15,2)"),
            )
        };

        let lineitem_dir = dir.join("lineitem");
        std::fs::create_dir_all(&lineitem_dir).expect("lineitem dir");
        for file_index in 0..2i64 {
            let keys: Vec<i64> = (0..200).map(|i| (file_index * 200 + i) % 60).collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&lineitem_schema),
                vec![
                    Arc::new(Int64Array::from(keys.clone())),
                    money(keys.iter().map(|k| i128::from(k % 30 + 1) * 100).collect()),
                    money(keys.iter().map(|k| i128::from(k + 1) * 1_000).collect()),
                    money(keys.iter().map(|k| i128::from(k % 10)).collect()),
                    Arc::new(StringArray::from(
                        keys.iter()
                            .map(|k| if k % 2 == 0 { "AIR" } else { "RAIL" })
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        keys.iter()
                            .map(|k| {
                                if k % 3 == 0 {
                                    "DELIVER IN PERSON"
                                } else {
                                    "TAKE BACK RETURN"
                                }
                            })
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("lineitem batch");
            write_parquet(
                &lineitem_dir.join(format!("l-{file_index}.parquet")),
                &batch,
            );
        }

        let part_dir = dir.join("part");
        std::fs::create_dir_all(&part_dir).expect("part dir");
        for file_index in 0..2i64 {
            let keys: Vec<i64> = (0..30).map(|i| file_index * 30 + i).collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&part_schema),
                vec![
                    Arc::new(Int64Array::from(keys.clone())),
                    Arc::new(StringArray::from(
                        keys.iter()
                            .map(|k| match k % 3 {
                                0 => "Brand#12",
                                1 => "Brand#23",
                                _ => "Brand#34",
                            })
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        keys.iter()
                            .map(|k| match k % 4 {
                                0 => "SM BOX",
                                1 => "MED BOX",
                                2 => "LG BOX",
                                _ => "JUMBO BOX",
                            })
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int32Array::from(
                        keys.iter().map(|k| (k % 15 + 1) as i32).collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("part batch");
            write_parquet(&part_dir.join(format!("p-{file_index}.parquet")), &batch);
        }
        (lineitem_dir, part_dir)
    }

    /// q22 verbatim: a correlated NOT EXISTS plus a scalar-subquery threshold,
    /// over `customer`/`orders`.
    const Q22: &str = "SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal FROM ( \
           SELECT substr(c_phone, 1, 2) AS cntrycode, c_acctbal FROM customer \
           WHERE substr(c_phone, 1, 2) IN ('13','31','23','29','30','18','17') \
           AND c_acctbal > (SELECT avg(c_acctbal) FROM customer \
                            WHERE c_acctbal > 0.00 \
                            AND substr(c_phone, 1, 2) IN ('13','31','23','29','30','18','17')) \
           AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)) AS custsale \
         GROUP BY cntrycode ORDER BY cntrycode";

    /// Miniature `customer` and `orders`, two files each.
    fn write_q22_fixture(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
        use arrow::array::{Decimal128Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let customer_schema = Arc::new(Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
        ]));
        let orders_schema = Arc::new(Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
        ]));
        let codes = ["13", "31", "23", "29", "30", "18", "17", "44"];

        let customer_dir = dir.join("customer");
        std::fs::create_dir_all(&customer_dir).expect("customer dir");
        for file_index in 0..2i64 {
            let keys: Vec<i64> = (0..120).map(|i| file_index * 120 + i).collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&customer_schema),
                vec![
                    Arc::new(Int64Array::from(keys.clone())),
                    Arc::new(StringArray::from(
                        keys.iter()
                            .map(|k| format!("{}-555-0100", codes[(*k as usize) % codes.len()]))
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(
                        Decimal128Array::from(
                            keys.iter()
                                .map(|k| i128::from(k % 900) * 100)
                                .collect::<Vec<_>>(),
                        )
                        .with_precision_and_scale(15, 2)
                        .expect("decimal(15,2)"),
                    ),
                ],
            )
            .expect("customer batch");
            write_parquet(
                &customer_dir.join(format!("c-{file_index}.parquet")),
                &batch,
            );
        }

        let orders_dir = dir.join("orders");
        std::fs::create_dir_all(&orders_dir).expect("orders dir");
        for file_index in 0..2i64 {
            let keys: Vec<i64> = (0..80).map(|i| file_index * 80 + i).collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&orders_schema),
                vec![
                    Arc::new(Int64Array::from(keys.clone())),
                    // Only some customers have orders, so NOT EXISTS keeps rows.
                    Arc::new(Int64Array::from(
                        keys.iter().map(|k| k * 3 % 240).collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("orders batch");
            write_parquet(&orders_dir.join(format!("o-{file_index}.parquet")), &batch);
        }
        (customer_dir, orders_dir)
    }

    async fn q22_context(dir: &std::path::Path) -> SessionContext {
        q22_context_with_broadcast(dir, None).await
    }

    async fn q22_context_with_broadcast(
        dir: &std::path::Path,
        broadcast_bytes: Option<usize>,
    ) -> SessionContext {
        let (customer, orders) = write_q22_fixture(dir);
        let ctx = planning_session_context_with_options(4, None, broadcast_bytes);
        for (name, path) in [("customer", customer), ("orders", orders)] {
            ctx.register_parquet(
                name,
                path.to_str().expect("utf8 path"),
                ParquetReadOptions::default(),
            )
            .await
            .expect("register parquet");
        }
        ctx
    }

    /// The q22 defect and its repair, both pinned in one test.
    ///
    /// `c_acctbal > (SELECT avg(c_acctbal) …)` leaves a `ScalarSubqueryExpr` in
    /// a filter below the exchange, while the `ScalarSubqueryExec` that
    /// populates it — which DataFusion puts at the very ROOT of the plan —
    /// stays behind in the result stage. The map fragment then encodes happily
    /// and refuses to decode, and the builder reads that as "decline to stage",
    /// running all of q22 as ONE task.
    ///
    /// Asserting BOTH halves is the point. Without the first assertion the test
    /// would keep passing if the severing ever stopped happening, and the
    /// repair would be a no-op nobody noticed.
    #[tokio::test]
    async fn a_severed_scalar_subquery_stage_does_not_decode_until_the_wrapper_is_restored() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let ctx = q22_context(tmp.path()).await;
        let plan = ctx
            .sql(Q22)
            .await
            .expect("sql")
            .create_physical_plan()
            .await
            .expect("physical plan");

        let mut drafts: Vec<StageDraft> = Vec::new();
        let root = cut_exchanges(Arc::clone(&plan), &mut drafts)
            .unwrap_or_else(|Unsupported(reason)| panic!("q22 stage split: {reason}"));
        drafts.push(StageDraft {
            plan: root,
            shuffle: None,
            subqueries: None,
        });
        assert!(
            drafts.iter().any(|d| d.subqueries.is_some()),
            "q22 must cut at least one stage out from beneath the ScalarSubqueryExec, \
             or there is nothing for the repair to act on"
        );

        let codec = KrishivPhysicalCodec::coordinator();
        let decode_ctx = fragment_decode_session_context().task_ctx();
        let mut saw_severed_stage = false;
        for draft in &drafts {
            let Some(context) = &draft.subqueries else {
                continue;
            };
            let bytes =
                encode_dfplan_bytes(Arc::clone(&draft.plan), &codec).expect("q22 stage encodes");
            let Err(error) =
                verify_dfplan_roundtrip(&bytes, &codec, &decode_ctx, Some(&draft.plan))
            else {
                // This stage carried no `ScalarSubqueryExpr`; nothing severed.
                continue;
            };
            assert!(
                error
                    .to_string()
                    .contains("ScalarSubqueryExpr can only be deserialized"),
                "expected the severed-wrapper decode failure, got: {error}"
            );
            saw_severed_stage = true;

            let repaired = wrap_in_scalar_subquery_exec(Arc::clone(&draft.plan), context);
            let bytes =
                encode_dfplan_bytes(Arc::clone(&repaired), &codec).expect("repaired stage encodes");
            verify_dfplan_roundtrip(&bytes, &codec, &decode_ctx, Some(&repaired))
                .expect("restoring the wrapper must make the fragment decodable");
        }
        assert!(
            saw_severed_stage,
            "precondition: a q22 stage must actually fail to decode bare, or this \
             test proves nothing about the repair"
        );
    }

    /// Bar 2 for q22: it must genuinely use the cluster, not merely return the
    /// right answer on one executor. A staged plan that produced one task per
    /// stage would satisfy `Some(_)` and still be a single-task query.
    #[tokio::test]
    async fn q22_distributes_instead_of_running_as_a_single_task() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let ctx = q22_context(tmp.path()).await;
        let plan = ctx
            .sql(Q22)
            .await
            .expect("sql")
            .create_physical_plan()
            .await
            .expect("physical plan");

        let staged = build_distributed_stages(plan)
            .expect("build stages")
            .expect("q22 must stage: a severed scalar-subquery wrapper is repaired, not declined");
        assert!(
            staged.stages.len() >= 2,
            "expected a map stage and a result stage, got {}",
            staged.stages.len()
        );
        assert!(
            staged.stages.iter().any(|s| s.task_count() > 1),
            "some stage must run more than one task, or 'distributed' means nothing: {:?}",
            staged
                .stages
                .iter()
                .map(DistributedStage::task_count)
                .collect::<Vec<_>>()
        );
    }

    /// The silent wrong-answer bug q22 exposed, pinned deterministically.
    ///
    /// `PartitionMode::CollectLeft` emits its unmatched BUILD rows only after
    /// the last probe partition reports in. A distributed task executes ONE
    /// partition, so that rendezvous never happens and those rows are dropped —
    /// no error, no schema mismatch, just a wrong answer. q22's `NOT EXISTS`
    /// returned zero rows per task.
    ///
    /// Built by hand rather than planned from SQL, deliberately: DataFusion
    /// usually SWAPS the inputs so the smaller side builds, turning `LeftAnti`
    /// into `RightAnti` — which streams from the probe side and is perfectly
    /// safe to split. That swap is why this shape is rare, why it survived
    /// every sweep unnoticed, and why a test that just runs a `NOT EXISTS`
    /// query proves nothing: it would silently exercise the safe plan. The
    /// end-to-end proof over a real severed plan is
    /// `staged_q22_matches_direct_execution`.
    #[tokio::test]
    async fn an_unsplittable_broadcast_join_is_detected_and_converted() {
        use datafusion::logical_expr::JoinType;
        use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

        let tmp = tempfile::tempdir().expect("tempdir");
        let ctx = q22_context(tmp.path()).await;
        let scan = |sql: &'static str| {
            let ctx = ctx.clone();
            async move {
                ctx.sql(sql)
                    .await
                    .expect("sql")
                    .create_physical_plan()
                    .await
                    .expect("physical plan")
            }
        };
        let build = scan("SELECT c_custkey FROM customer").await;
        let probe = scan("SELECT o_custkey FROM orders").await;
        assert!(
            probe.output_partitioning().partition_count() > 1,
            "precondition: the probe side must have several partitions, or there \
             is no rendezvous to miss"
        );

        let on = vec![(
            datafusion::physical_plan::expressions::col("c_custkey", &build.schema())
                .expect("build key"),
            datafusion::physical_plan::expressions::col("o_custkey", &probe.schema())
                .expect("probe key"),
        )];
        let unsafe_join: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                Arc::new(CoalescePartitionsExec::new(build)),
                probe,
                on,
                None,
                &JoinType::LeftAnti,
                None,
                PartitionMode::CollectLeft,
                datafusion::common::NullEquality::NullEqualsNothing,
                false,
            )
            .expect("hand-built broadcast anti-join"),
        );

        let join_ref = unsafe_join
            .downcast_ref::<HashJoinExec>()
            .expect("hash join");
        assert!(
            is_unsplittable_broadcast_join(join_ref),
            "a CollectLeft LeftAnti join over a multi-partition probe must be \
             recognised as unsplittable"
        );
        assert!(
            find_unsupported_stage_node(&unsafe_join).is_some(),
            "and the stage guard must refuse it, so it can never ship unconverted"
        );

        let converted = redistribute_unsplittable_broadcast_joins(Arc::clone(&unsafe_join))
            .expect("conversion must succeed");
        let converted_join = converted
            .downcast_ref::<HashJoinExec>()
            .expect("still a hash join");
        assert_eq!(
            *converted_join.partition_mode(),
            PartitionMode::Partitioned,
            "conversion must switch to the mode whose probe counter is per-task"
        );
        assert!(
            !is_unsplittable_broadcast_join(converted_join),
            "the converted join must no longer be unsplittable"
        );
        assert_eq!(
            *converted_join.join_type(),
            JoinType::LeftAnti,
            "conversion must not change the join's meaning"
        );
        assert_eq!(
            converted.schema(),
            unsafe_join.schema(),
            "conversion must preserve the join's output schema"
        );
    }

    /// Hand-build a `CollectLeft` join over `build`, coalesced as the planner
    /// would coalesce it, probing a multi-partition `orders` scan.
    ///
    /// Shared by the broadcast-policy tests below so they differ only in the
    /// build side, which is the variable under test.
    async fn collect_left_over(
        ctx: &SessionContext,
        build_sql: &str,
        build_key: &str,
        null_aware: bool,
        join_type: datafusion::logical_expr::JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

        let plan = |sql: String| {
            let ctx = ctx.clone();
            async move {
                ctx.sql(&sql)
                    .await
                    .expect("sql")
                    .create_physical_plan()
                    .await
                    .expect("physical plan")
            }
        };
        let build = plan(build_sql.to_owned()).await;
        let probe = plan(String::from("SELECT o_custkey FROM orders")).await;
        let on = vec![(
            datafusion::physical_plan::expressions::col(build_key, &build.schema())
                .expect("build key"),
            datafusion::physical_plan::expressions::col("o_custkey", &probe.schema())
                .expect("probe key"),
        )];
        Arc::new(
            HashJoinExec::try_new(
                Arc::new(CoalescePartitionsExec::new(build)),
                probe,
                on,
                None,
                &join_type,
                None,
                PartitionMode::CollectLeft,
                datafusion::common::NullEquality::NullEqualsNothing,
                null_aware,
            )
            .expect("hand-built broadcast join"),
        )
    }

    /// The q21 defect: an estimate of ZERO must not read as "small enough to
    /// broadcast".
    ///
    /// At SF100 DataFusion estimates q21's `LeftAnti` self-join at
    /// `593462145 - 593462145 = 0` rows, and three `CollectLeft` joins stacked
    /// above it each broadcast on the strength of that — gathering tens of
    /// millions of rows to ONE partition three times over. `0 < ceiling` is the
    /// most convincing "broadcast me" a build side can produce, which is
    /// exactly backwards.
    #[tokio::test]
    async fn a_zero_estimate_is_not_proof_that_a_build_side_is_small() {
        use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

        let tmp = tempfile::tempdir().expect("tempdir");
        let ctx = q22_context(tmp.path()).await;
        // `WHERE false` gives the planner an exact zero — the same input the
        // anti-join's arithmetic produces, without needing SF100 to reach it.
        let join = collect_left_over(
            &ctx,
            "SELECT c_custkey FROM customer WHERE 1 = 0",
            "c_custkey",
            false,
            datafusion::logical_expr::JoinType::Inner,
        )
        .await;
        let join_ref = join.downcast_ref::<HashJoinExec>().expect("hash join");

        assert!(
            broadcast_build_estimate_is_empty(join_ref),
            "a zero estimate is the estimator giving up, not a measurement"
        );
        assert!(
            is_degenerate_broadcast_join(join_ref),
            "so the join must be recognised as one that should not broadcast"
        );
        // The correctness gate must stay untouched: an Inner join drops no
        // unmatched build rows, so refusing to STAGE it would turn a merely
        // slow plan into one that declines to distribute at all.
        assert!(
            !is_unsplittable_broadcast_join(join_ref),
            "this is a throughput problem, not a correctness one"
        );
        assert!(
            find_unsupported_stage_node(&join).is_none(),
            "and the stage guard must not refuse it"
        );

        let converted =
            redistribute_unsplittable_broadcast_joins(Arc::clone(&join)).expect("conversion");
        let converted_join = converted
            .downcast_ref::<HashJoinExec>()
            .expect("still a hash join");
        assert_eq!(
            *converted_join.partition_mode(),
            PartitionMode::Partitioned,
            "the build side must be hash-partitioned instead of gathered"
        );
        assert_eq!(
            converted.schema(),
            join.schema(),
            "conversion must preserve the join's output schema"
        );
    }

    /// The opposite regression, and the reason this rule is not simply "never
    /// broadcast".
    ///
    /// Broadcasting a genuinely small dimension side is what keeps q8/q9 from
    /// hash-partitioning the raw 600M-row `lineitem` scan across a ~11 MiB/s
    /// pod network. A scan of a small table has EXACT parquet statistics, so it
    /// is provably small and must survive this rule untouched.
    #[tokio::test]
    async fn a_provably_small_build_side_is_still_broadcast() {
        use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

        let tmp = tempfile::tempdir().expect("tempdir");
        let ctx = q22_context(tmp.path()).await;
        let join = collect_left_over(
            &ctx,
            "SELECT c_custkey FROM customer",
            "c_custkey",
            false,
            datafusion::logical_expr::JoinType::Inner,
        )
        .await;
        let join_ref = join.downcast_ref::<HashJoinExec>().expect("hash join");

        assert!(
            !broadcast_build_estimate_is_empty(join_ref),
            "a non-empty parquet scan must report a positive estimate"
        );
        assert!(
            !is_degenerate_broadcast_join(join_ref),
            "so it must keep its broadcast"
        );
        let after =
            redistribute_unsplittable_broadcast_joins(Arc::clone(&join)).expect("conversion");
        assert_eq!(
            *after
                .downcast_ref::<HashJoinExec>()
                .expect("still a hash join")
                .partition_mode(),
            PartitionMode::CollectLeft,
            "the rule must leave a legitimately small broadcast alone"
        );
    }

    /// The regression this rule caused once, pinned so it cannot come back.
    ///
    /// An earlier version demanded a positive estimate *below a ceiling*. That
    /// converted q8's and q9's `CollectLeft` build sides — estimated at
    /// `rows=~4000000, bytes=absent`, above the 1M row ceiling but entirely
    /// plausible — and on the cluster **q8 went 92 s -> 375 s and q9 226 s ->
    /// 576 s**, because the alternative to broadcasting a few million rows is
    /// hash-partitioning the 600M-row `lineitem` scan across an ~11 MiB/s pod
    /// network.
    ///
    /// The ceiling is DataFusion's call, made with the same numbers this rule
    /// can see. A large but positive estimate must therefore be left alone; only
    /// "the planner thinks this is empty" is overridden.
    #[tokio::test]
    async fn a_large_but_positive_estimate_keeps_its_broadcast() {
        use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

        let tmp = tempfile::tempdir().expect("tempdir");
        let ctx = q22_context(tmp.path()).await;
        // A cross join squares the row estimate, which is how a build side
        // reaches a number far above any ceiling while staying honest — the
        // shape of q8/q9's estimate, reachable without SF100.
        let join = collect_left_over(
            &ctx,
            "SELECT a.c_custkey FROM customer a CROSS JOIN customer b",
            "c_custkey",
            false,
            datafusion::logical_expr::JoinType::Inner,
        )
        .await;
        let join_ref = join.downcast_ref::<HashJoinExec>().expect("hash join");

        let stats = join_ref
            .left()
            .partition_statistics(None)
            .expect("statistics");
        assert!(
            matches!(
                stats.num_rows,
                datafusion::common::stats::Precision::Exact(n)
                    | datafusion::common::stats::Precision::Inexact(n) if n > 0
            ),
            "precondition: the build side must estimate a positive row count, \
             got {:?}",
            stats.num_rows
        );
        assert!(
            !broadcast_build_estimate_is_empty(join_ref),
            "a positive estimate is a measurement, however large"
        );
        assert!(
            !is_degenerate_broadcast_join(join_ref),
            "and must not be converted — overriding DataFusion's ceiling cost \
             q8 4x and q9 2.5x"
        );
        let after =
            redistribute_unsplittable_broadcast_joins(Arc::clone(&join)).expect("conversion");
        assert_eq!(
            *after
                .downcast_ref::<HashJoinExec>()
                .expect("still a hash join")
                .partition_mode(),
            PartitionMode::CollectLeft,
            "the plan must come back untouched"
        );
    }

    /// A null-aware anti join is only correct as `CollectLeft`.
    ///
    /// It tracks probe-side state across the whole build side, and DataFusion
    /// rejects any other partition mode for it at construction — so however
    /// badly estimated its build side is, converting it would trade a slow
    /// query for one that does not run.
    #[tokio::test]
    async fn a_null_aware_anti_join_is_never_converted() {
        use datafusion::physical_plan::joins::HashJoinExec;

        let tmp = tempfile::tempdir().expect("tempdir");
        let ctx = q22_context(tmp.path()).await;
        let join = collect_left_over(
            &ctx,
            "SELECT c_custkey FROM customer WHERE 1 = 0",
            "c_custkey",
            true,
            datafusion::logical_expr::JoinType::LeftAnti,
        )
        .await;
        let join_ref = join.downcast_ref::<HashJoinExec>().expect("hash join");

        assert!(
            broadcast_build_estimate_is_empty(join_ref),
            "precondition: its build-side estimate is degenerate, so only the \
             null-aware check can be what spares it"
        );
        assert!(
            !is_degenerate_broadcast_join(join_ref),
            "a null-aware anti join must never be converted for throughput"
        );
    }

    /// And the repaired plan must still compute q22's actual answer. The
    /// wrapper is re-evaluated per stage, so every task resolves the subquery
    /// independently — this is what proves they all resolve it to the same
    /// value the single-node plan uses.
    #[tokio::test]
    async fn staged_q22_matches_direct_execution() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let ctx = q22_context(tmp.path()).await;
        let expected = render(&direct(&ctx, Q22).await);
        let actual = run_staged(&ctx, Q22)
            .await
            .unwrap_or_else(|e| panic!("q22: staged execution failed: {e}"));
        assert_eq!(
            render(&actual),
            expected,
            "q22: staged result differs from single-node execution"
        );
        assert!(!expected.is_empty(), "the q22 fixture must produce rows");
    }

    /// `broadcast_bytes: Some(0)` reproduces the cluster's join shape: neither
    /// side is small enough to collect, so both hash-shuffle and the reduce
    /// stage gets **two** `ShuffleReadExec` leaves over two upstream stages.
    /// Every other test here runs the broadcast shape, because a fixture that
    /// fits in a process is always under the 32 MiB ceiling.
    async fn tpch_context_with_broadcast(
        dir: &std::path::Path,
        join_threshold: Option<u64>,
        broadcast_bytes: Option<usize>,
    ) -> SessionContext {
        let (lineitem, part) = write_tpch_fixture(dir);
        let ctx = planning_session_context_with_options(4, join_threshold, broadcast_bytes);
        for (name, path) in [("lineitem", lineitem), ("part", part)] {
            ctx.register_parquet(
                name,
                path.to_str().expect("utf8 path"),
                ParquetReadOptions::default(),
            )
            .await
            .expect("register parquet");
        }
        ctx
    }

    /// Consistent test-side routing; any consistent hash is correct here.
    fn route(batch: &RecordBatch, key_column: &str, num_partitions: usize) -> Vec<RecordBatch> {
        use std::hash::{Hash as _, Hasher as _};
        let key_idx = batch.schema().index_of(key_column).expect("key column");
        let column = batch.column(key_idx);
        let mut selections: Vec<Vec<u32>> = vec![Vec::new(); num_partitions];
        for row in 0..batch.num_rows() {
            let value = arrow::util::display::array_value_to_string(column, row).expect("value");
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            value.hash(&mut hasher);
            let bucket = (hasher.finish() as usize) % num_partitions;
            selections[bucket].push(row as u32);
        }
        selections
            .into_iter()
            .map(|rows| {
                let indices = arrow::array::UInt32Array::from(rows);
                arrow::compute::take_record_batch(batch, &indices).expect("take")
            })
            .collect()
    }

    /// Run every stage in dependency order, exactly as the cluster does.
    async fn run_staged(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>, String> {
        let df = ctx.sql(sql).await.map_err(|e| e.to_string())?;
        let plan = df.create_physical_plan().await.map_err(|e| e.to_string())?;
        let staged = build_distributed_stages(plan)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| String::from("declined to stage"))?;

        let store = Arc::new(StageStore::default());
        let exec_ctx = fragment_decode_session_context();
        let mut result = Vec::new();
        for (stage_index, stage) in staged.stages.iter().enumerate() {
            for (task_index, body) in stage.task_bodies.iter().enumerate() {
                let reader: Arc<dyn ShufflePartitionReader> = Arc::new(Arc::clone(&store));
                let (declared, mut stream) = execute_dfplan_body(body, &exec_ctx, Some(reader))
                    .map_err(|e| format!("stage {stage_index} task {task_index} start: {e}"))?;
                while let Some(batch) = futures::StreamExt::next(&mut stream).await {
                    let batch =
                        batch.map_err(|e| format!("stage {stage_index} task {task_index}: {e}"))?;
                    // The invariant the cluster depends on and this harness
                    // would otherwise hide: the store here hands the *same*
                    // batches back, so a stage whose declared schema disagrees
                    // with its produced batches sails through in process and
                    // only dies on the wire, where the reduce side concatenates
                    // real IPC data against the declared schema and Arrow says
                    // "column types must match schema types".
                    if batch.schema() != declared {
                        return Err(format!(
                            "stage {stage_index} task {task_index} declares {declared:?} but \
                             produced {:?}; ShuffleReadExec labels the reduce side with the \
                             declared schema, so this disagreement becomes a reduce-side Arrow \
                             error on a real cluster",
                            batch.schema()
                        ));
                    }
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    match &stage.shuffle {
                        None => result.push(batch),
                        Some(shuffle) => match shuffle.key_columns.first() {
                            Some(key) => {
                                for (bucket, part) in
                                    route(&batch, key, shuffle.num_output_partitions)
                                        .into_iter()
                                        .enumerate()
                                {
                                    if part.num_rows() > 0 {
                                        store
                                            .partitions
                                            .lock()
                                            .expect("store lock")
                                            .entry((stage_index, task_index, bucket))
                                            .or_default()
                                            .push(part);
                                    }
                                }
                            }
                            // A keyless shuffle is a gather: everything to 0.
                            None => store
                                .partitions
                                .lock()
                                .expect("store lock")
                                .entry((stage_index, task_index, 0))
                                .or_default()
                                .push(batch),
                        },
                    }
                }
            }
        }
        Ok(result)
    }

    fn render(batches: &[RecordBatch]) -> Vec<String> {
        let mut rows: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(move |r| {
                    (0..b.num_columns())
                        .map(|c| {
                            arrow::util::display::array_value_to_string(b.column(c), r)
                                .expect("cell")
                        })
                        .collect::<Vec<_>>()
                        .join("|")
                })
            })
            .collect();
        rows.sort();
        rows
    }

    async fn direct(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        ctx.sql(sql)
            .await
            .expect("sql")
            .collect()
            .await
            .expect("direct execution")
    }

    /// The staged answer must equal the single-node answer, with and without
    /// the spillable-join conversion active.
    ///
    /// `Some(0)` forces every join whose build size is known to convert to
    /// sort-merge — the state a memory-capped executor is in, and the state
    /// this build box never reaches on its own. The rule claims to preserve
    /// the join's output schema exactly (`schema_check()` returns true), so
    /// converting *more* joins than production would must still be correct;
    /// if it is not, the claim is false.
    async fn staged_matches_direct(sql: &str, join_threshold: Option<u64>, label: &str) {
        staged_matches_direct_with_broadcast(sql, join_threshold, None, label).await;
    }

    async fn staged_matches_direct_with_broadcast(
        sql: &str,
        join_threshold: Option<u64>,
        broadcast_bytes: Option<usize>,
        label: &str,
    ) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let ctx = tpch_context_with_broadcast(tmp.path(), join_threshold, broadcast_bytes).await;
        let expected = render(&direct(&ctx, sql).await);
        let actual = run_staged(&ctx, sql)
            .await
            .unwrap_or_else(|e| panic!("{label}: staged execution failed: {e}"));
        assert_eq!(
            render(&actual),
            expected,
            "{label}: staged result differs from single-node execution"
        );
    }

    #[tokio::test]
    async fn staged_q17_matches_direct_execution() {
        staged_matches_direct(Q17, None, "q17/unconverted").await;
    }

    #[tokio::test]
    async fn staged_q17_matches_direct_execution_with_converted_joins() {
        staged_matches_direct(Q17, Some(0), "q17/converted").await;
    }

    #[tokio::test]
    async fn staged_q19_matches_direct_execution() {
        staged_matches_direct(Q19, None, "q19/unconverted").await;
    }

    #[tokio::test]
    async fn staged_q19_matches_direct_execution_with_converted_joins() {
        staged_matches_direct(Q19, Some(0), "q19/converted").await;
    }

    /// The shape the cluster actually runs: no broadcast, so both join sides
    /// hash-shuffle and the reduce stage reads two upstream stages.
    ///
    /// q17 and q19 pass every broadcast-shaped test above and still fail at
    /// SF100 with a bare Arrow type error, so the defect lives in what the
    /// broadcast shape never builds.
    #[tokio::test]
    async fn staged_q17_matches_direct_execution_without_broadcast() {
        staged_matches_direct_with_broadcast(Q17, None, Some(0), "q17/no-broadcast").await;
    }

    #[tokio::test]
    async fn staged_q19_matches_direct_execution_without_broadcast() {
        staged_matches_direct_with_broadcast(Q19, None, Some(0), "q19/no-broadcast").await;
    }

    /// The cell the matrix was missing — and the only one the cluster is in.
    ///
    /// The conversion tests above all run the *broadcast* shape, and the
    /// no-broadcast tests all run *unconverted* joins. SF100 does both at once:
    /// no build side is under the 32 MiB ceiling, so both sides hash-shuffle,
    /// **and** the build sides are far over the spill threshold, so
    /// `SpillableJoinSelection` rewrites them to sort-merge. Two settings that
    /// are each covered alone and never together.
    ///
    /// That combination is what `reapply_projection` runs in: a projected join
    /// whose converted form is a `SortMergeJoinExec` (which has no projection of
    /// its own) sitting under a shuffle, where the reduce side concatenates real
    /// IPC data against the declared schema.
    #[tokio::test]
    async fn staged_q17_matches_direct_execution_converted_and_without_broadcast() {
        staged_matches_direct_with_broadcast(Q17, Some(0), Some(0), "q17/converted+no-broadcast")
            .await;
    }

    #[tokio::test]
    async fn staged_q19_matches_direct_execution_converted_and_without_broadcast() {
        staged_matches_direct_with_broadcast(Q19, Some(0), Some(0), "q19/converted+no-broadcast")
            .await;
    }

    #[tokio::test]
    async fn staged_q22_matches_direct_execution_without_broadcast() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let ctx = q22_context_with_broadcast(tmp.path(), Some(0)).await;
        let expected = render(&direct(&ctx, Q22).await);
        let actual = run_staged(&ctx, Q22)
            .await
            .unwrap_or_else(|e| panic!("q22/no-broadcast: staged execution failed: {e}"));
        assert_eq!(
            render(&actual),
            expected,
            "q22/no-broadcast: staged result differs from single-node execution"
        );
        assert!(!expected.is_empty(), "the q22 fixture must produce rows");
    }

    /// `avg` over a decimal, cut so the Final aggregate lands in a different
    /// stage from its Partial — q17's shape, reduced to the one operator.
    ///
    /// `datafusion-proto` carries no output type for an aggregate: the decoder
    /// resolves the UDAF by name and `AggregateExprBuilder::build()` re-derives
    /// the return type from the resolved function and its *input* types. A
    /// Final aggregate's inputs are the Partial's **state** columns, not the
    /// original column, so if the rebuild reads them as ordinary inputs it
    /// produces a wider decimal than the coordinator planned — which is exactly
    /// what q17 reports from SF100:
    ///
    ///   expected Decimal128(15, 2) but found Decimal128(30, 15)
    ///
    /// Both the ungrouped (gather-cut) and grouped (hash-exchange-cut) forms
    /// are covered: they take different arms of `cut_exchanges`.
    #[tokio::test]
    async fn a_final_avg_over_a_decimal_survives_the_fragment_round_trip() {
        for sql in [
            "SELECT avg(l_quantity) FROM lineitem",
            "SELECT l_partkey, avg(l_quantity) FROM lineitem GROUP BY l_partkey",
            "SELECT sum(l_extendedprice) / 7.0 FROM lineitem",
        ] {
            let tmp = tempfile::tempdir().expect("tempdir");
            let ctx = tpch_context_with_broadcast(tmp.path(), None, None).await;
            let expected = render(&direct(&ctx, sql).await);
            let actual = run_staged(&ctx, sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: staged execution failed: {e}"));
            assert_eq!(
                render(&actual),
                expected,
                "{sql}: staged result differs from single-node execution"
            );
        }
    }

    /// A reduce stage really does read two distinct upstream stages once
    /// broadcasting is off — the precondition the three tests above depend on.
    /// Without this, a planner change that quietly restored a broadcast join
    /// would turn them into duplicates of the tests they were written to
    /// complement, and nothing would say so.
    #[tokio::test]
    async fn without_broadcast_a_reduce_stage_reads_two_upstream_stages() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let ctx = tpch_context_with_broadcast(tmp.path(), None, Some(0)).await;
        let plan = ctx
            .sql(Q19)
            .await
            .expect("sql")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let staged = build_distributed_stages(plan)
            .expect("staging must not error")
            .expect("q19 must stage");
        let widest = staged
            .stages
            .iter()
            .map(|stage| stage.upstream_stage_indexes.len())
            .max()
            .unwrap_or(0);
        assert!(
            widest >= 2,
            "expected a stage reading 2+ upstream stages, widest was {widest}; \
             the no-broadcast tests are not exercising the cluster's join shape"
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod codec_completeness_tests {
    /// Every custom `ExecutionPlan` in this crate must be either encodable by
    /// [`KrishivPhysicalCodec`] or explicitly declared execution-local.
    ///
    /// # The failure this prevents
    ///
    /// `datafusion-proto` cannot encode a node the extension codec does not
    /// know. The scheduler's response to a stage plan it cannot encode is not an
    /// error — it is to abandon staging and run the whole query as a **single
    /// task**:
    ///
    /// ```text
    /// stage plan cannot be encoded and decoded; running this query as a
    /// SINGLE TASK ... Unsupported plan and extension codec failed
    /// ```
    ///
    /// So adding an operator without a codec entry does not break loudly. It
    /// quietly un-distributes every query the operator touches while continuing
    /// to report success. `GraceHashJoinExec` did exactly that to TPC-H q10,
    /// q17, q19 and q21 — hours of cluster time reading as passes.
    ///
    /// A source scan rather than a type-level check because Rust cannot
    /// enumerate trait impls at runtime; this mirrors
    /// `krishiv_common::env_registry`'s
    /// `every_flag_read_in_source_is_declared`, which exists for the same
    /// reason.
    #[test]
    fn every_custom_execution_plan_is_encodable_or_declared_local() {
        // Nodes that may appear in a plan the coordinator encodes. Adding one
        // here without a `try_encode`/`try_decode` arm re-opens the bug.
        // `runtime_filters::the_injected_stages_round_trip_through_the_codec` is
        // what makes listing the two filter nodes here a fact rather than a
        // promise: it encodes and decodes a plan containing both.
        const ENCODABLE: &[&str] = &[
            "RuntimeFilterBuildExec",
            "RuntimeFilterProbeExec",
            "ShuffleReadExec",
        ];
        // Nodes that are constructed only AFTER decode and never serialized.
        // `GraceHashJoinExec` is chosen per-executor from live memory pressure
        // (`apply_local_spill_strategy`); `OnceStreamExec` wraps an already-open
        // spill-file stream, which has no meaning on another machine.
        const EXECUTION_LOCAL: &[&str] = &["GraceHashJoinExec", "OnceStreamExec"];

        let mut found = Vec::new();
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut stack = vec![dir];
        while let Some(path) = stack.pop() {
            for entry in std::fs::read_dir(&path).expect("read src") {
                let entry = entry.expect("dir entry").path();
                if entry.is_dir() {
                    stack.push(entry);
                    continue;
                }
                if entry.extension().is_none_or(|e| e != "rs") {
                    continue;
                }
                let text = std::fs::read_to_string(&entry).expect("read file");
                for line in text.lines() {
                    if let Some(rest) = line.trim().strip_prefix("impl ExecutionPlan for ") {
                        let name = rest
                            .trim_end_matches(" {")
                            .split(['<', ' '])
                            .next()
                            .unwrap_or(rest)
                            .to_string();
                        found.push(name);
                    }
                }
            }
        }
        found.sort();
        found.dedup();
        assert!(
            !found.is_empty(),
            "the scan found no ExecutionPlan impls at all"
        );

        let undeclared: Vec<&String> = found
            .iter()
            .filter(|n| !ENCODABLE.contains(&n.as_str()) && !EXECUTION_LOCAL.contains(&n.as_str()))
            .collect();
        assert!(
            undeclared.is_empty(),
            "custom ExecutionPlan(s) {undeclared:?} are neither encodable nor declared \
             execution-local. If such a node can reach a stage plan, the coordinator will \
             silently run the query as a SINGLE TASK. Add a codec arm, or confine it to \
             post-decode and list it in EXECUTION_LOCAL."
        );
    }
    /// An AQE rewrite rebuilds every reduce task body through
    /// `dfplan_body_with_spec`. If that drops the Python-UDF directive prefix,
    /// the rebuilt task ships a plan referencing a UDF the executor was never
    /// told to reconstruct, and it dies with "PhysicalExtensionCodec is not
    /// provided for scalar function <name>" — while its sibling map tasks,
    /// whose bodies are never rebuilt, run fine.
    #[test]
    fn rebuilding_a_body_keeps_the_python_udf_directive() {
        use super::{
            DfplanTaskSpec, dfplan_body_partition_spec, dfplan_body_with_spec, is_dfplan_body,
        };
        let directive = "/* krishiv-register-python-udf:addk:int64:int64:QUJD */";
        let body = format!("{directive}\ndfplan:v1:0:QUJD");
        let spec = DfplanTaskSpec {
            partitions: vec![3, 4],
            map_range: None,
        };
        let rebuilt = dfplan_body_with_spec(&body, &spec).expect("rebuild");
        assert!(
            rebuilt.starts_with(directive),
            "the rebuilt body must still carry the UDF directive: {rebuilt}"
        );
        assert!(
            is_dfplan_body(&rebuilt),
            "and must still parse as a dfplan body: {rebuilt}"
        );
        assert_eq!(
            dfplan_body_partition_spec(&rebuilt)
                .expect("spec")
                .partitions,
            vec![3, 4],
            "the new partition spec must be the one asked for"
        );
    }

    // ── Stage reuse (ReuseExchange) ────────────────────────────────────────

    mod stage_reuse {
        use super::super::*;

        fn scan_schema() -> SchemaRef {
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int64, false),
                arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, false),
            ]))
        }

        /// A leaf stage: an in-memory scan, no shuffle read inside it.
        fn leaf_draft(rows: usize, key: &str, parts: usize) -> StageDraft {
            use datafusion::catalog::memory::MemorySourceConfig;
            use datafusion::datasource::source::DataSourceExec;
            let schema = scan_schema();
            let batches: Vec<Vec<arrow::record_batch::RecordBatch>> = vec![vec![
                arrow::record_batch::RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(arrow::array::Int64Array::from(
                            (0..rows as i64).collect::<Vec<_>>(),
                        )),
                        Arc::new(arrow::array::Int64Array::from(
                            (0..rows as i64).collect::<Vec<_>>(),
                        )),
                    ],
                )
                .expect("batch"),
            ]];
            let source = MemorySourceConfig::try_new(&batches, Arc::clone(&schema), None)
                .expect("memory source");
            StageDraft {
                plan: Arc::new(DataSourceExec::new(Arc::new(source))),
                shuffle: Some(StageShuffleOutput {
                    key_columns: vec![String::from(key)],
                    num_output_partitions: parts,
                }),
                subqueries: None,
            }
        }

        fn reader(stage: usize) -> Arc<dyn ExecutionPlan> {
            Arc::new(ShuffleReadExec::new(stage, 4, 4, scan_schema(), None))
        }

        /// Two identical leaf stages collapse into one, and the consumer that
        /// pointed at the removed stage is repointed at the survivor. This is
        /// q18: `lineitem[l_orderkey, l_quantity]` scanned twice, unfiltered.
        #[test]
        fn identical_leaf_stages_collapse_and_readers_are_repointed() {
            let mut drafts = vec![leaf_draft(4, "k", 4), leaf_draft(4, "k", 4)];
            // Root reads BOTH stages; after the collapse both reads must
            // resolve to the surviving stage 0.
            let mut root: Arc<dyn ExecutionPlan> =
                datafusion::physical_plan::union::UnionExec::try_new(vec![reader(0), reader(1)])
                    .expect("union");

            let removed = dedupe_identical_stages_unconditionally(&mut root, &mut drafts);

            assert_eq!(
                removed, 1,
                "one of the two identical stages must be removed"
            );
            assert_eq!(drafts.len(), 1, "one stage must survive");
            let upstreams = collect_upstream_stage_indexes(&root);
            assert_eq!(
                upstreams,
                vec![0],
                "both readers must point at the surviving stage, got {upstreams:?}"
            );
        }

        /// Stages that compute the same rows but partition them differently are
        /// NOT interchangeable — the consumer reads by partition index.
        #[test]
        fn different_shuffle_contracts_do_not_collapse() {
            let mut drafts = vec![leaf_draft(4, "k", 4), leaf_draft(4, "k", 8)];
            let mut root: Arc<dyn ExecutionPlan> =
                datafusion::physical_plan::union::UnionExec::try_new(vec![reader(0), reader(1)])
                    .expect("union");
            assert_eq!(
                dedupe_identical_stages_unconditionally(&mut root, &mut drafts),
                0,
                "a different output-partition count is a different stage"
            );
            assert_eq!(drafts.len(), 2);
        }

        /// Same rows, different hash key: also not interchangeable.
        #[test]
        fn different_shuffle_keys_do_not_collapse() {
            let mut drafts = vec![leaf_draft(4, "k", 4), leaf_draft(4, "v", 4)];
            let mut root: Arc<dyn ExecutionPlan> =
                datafusion::physical_plan::union::UnionExec::try_new(vec![reader(0), reader(1)])
                    .expect("union");
            assert_eq!(
                dedupe_identical_stages_unconditionally(&mut root, &mut drafts),
                0,
                "a different partitioning key is a different stage"
            );
        }

        /// Stages that differ in content must never be merged — the guard that
        /// keeps this from silently returning wrong answers.
        #[test]
        fn different_content_does_not_collapse() {
            let mut drafts = vec![leaf_draft(4, "k", 4), leaf_draft(9, "k", 4)];
            let mut root: Arc<dyn ExecutionPlan> =
                datafusion::physical_plan::union::UnionExec::try_new(vec![reader(0), reader(1)])
                    .expect("union");
            assert_eq!(
                dedupe_identical_stages_unconditionally(&mut root, &mut drafts),
                0,
                "stages producing different rows must stay separate"
            );
            assert_eq!(drafts.len(), 2);
        }

        /// Non-leaf stages are out of scope: a stage containing a shuffle read
        /// carries stage indexes of its own, and collapsing it could invalidate
        /// a reference inside it.
        #[test]
        fn non_leaf_stages_are_left_alone() {
            let mk = || StageDraft {
                plan: reader(7),
                shuffle: Some(StageShuffleOutput {
                    key_columns: vec![String::from("k")],
                    num_output_partitions: 4,
                }),
                subqueries: None,
            };
            let mut drafts = vec![mk(), mk()];
            let mut root: Arc<dyn ExecutionPlan> =
                datafusion::physical_plan::union::UnionExec::try_new(vec![reader(0), reader(1)])
                    .expect("union");
            assert_eq!(
                dedupe_identical_stages_unconditionally(&mut root, &mut drafts),
                0,
                "stages that read a shuffle are not eligible for leaf reuse"
            );
        }

        /// Three identical stages collapse to one, and every reader lands on it.
        /// This is q21's shape, where lineitem is scanned three times.
        #[test]
        fn three_identical_stages_collapse_to_one() {
            let mut drafts = vec![
                leaf_draft(4, "k", 4),
                leaf_draft(4, "k", 4),
                leaf_draft(4, "k", 4),
            ];
            let mut root: Arc<dyn ExecutionPlan> =
                datafusion::physical_plan::union::UnionExec::try_new(vec![
                    reader(0),
                    reader(1),
                    reader(2),
                ])
                .expect("union");
            assert_eq!(
                dedupe_identical_stages_unconditionally(&mut root, &mut drafts),
                2
            );
            assert_eq!(drafts.len(), 1);
            assert_eq!(collect_upstream_stage_indexes(&root), vec![0]);
        }

        /// Surviving stages keep their relative order and their readers are
        /// renumbered — an off-by-one here silently feeds a consumer the wrong
        /// stage's data, which is a wrong answer, not a slow query.
        #[test]
        fn survivor_indexes_are_compacted_correctly() {
            // 0: A, 1: B, 2: A(dup of 0), 3: C
            let mut drafts = vec![
                leaf_draft(4, "k", 4),
                leaf_draft(7, "k", 4),
                leaf_draft(4, "k", 4),
                leaf_draft(9, "k", 4),
            ];
            let mut root: Arc<dyn ExecutionPlan> =
                datafusion::physical_plan::union::UnionExec::try_new(vec![
                    reader(1),
                    reader(2),
                    reader(3),
                ])
                .expect("union");
            assert_eq!(
                dedupe_identical_stages_unconditionally(&mut root, &mut drafts),
                1
            );
            assert_eq!(drafts.len(), 3, "A, B, C survive");
            // B was 1 -> 1, the dup of A was 2 -> 0, C was 3 -> 2.
            assert_eq!(collect_upstream_stage_indexes(&root), vec![0, 1, 2]);
        }

        /// Reuse replaces two evaluations with one, which is only sound for a
        /// deterministic subtree.
        #[test]
        fn volatile_markers_block_reuse() {
            assert!(
                VOLATILE_MARKERS.contains(&"random("),
                "random() must block reuse"
            );
            assert!(VOLATILE_MARKERS.contains(&"now("), "now() must block reuse");
            assert!(
                VOLATILE_MARKERS.contains(&"uuid("),
                "uuid() must block reuse"
            );
        }

        /// The flag gates it: default off, so a plan is untouched until the
        /// rule has been measured.
        #[test]
        fn reuse_is_off_by_default() {
            let mut drafts = vec![leaf_draft(4, "k", 4), leaf_draft(4, "k", 4)];
            let mut root: Arc<dyn ExecutionPlan> =
                datafusion::physical_plan::union::UnionExec::try_new(vec![reader(0), reader(1)])
                    .expect("union");
            if std::env::var(STAGE_REUSE_ENV).is_err() {
                assert_eq!(
                    dedupe_identical_stages(&mut root, &mut drafts),
                    0,
                    "stage reuse must be off unless {STAGE_REUSE_ENV} is set"
                );
            }
        }
    }

    // ── Cross-stage runtime filters ────────────────────────────────────────

    mod runtime_filters {
        use super::super::*;
        use datafusion::common::{JoinType, NullEquality};
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

        /// A sort-merge join across two stages now yields a candidate, exactly
        /// as the equivalent hash join does.
        ///
        /// This is the shape TPC-H q21 reaches the cutter in — see
        /// `a_sort_merge_join_is_counted_as_unreadable_not_as_no_join` for the
        /// measurement that produced it. Which algorithm the planner picked is
        /// not a property of the filter: a bloom over the join keys drops probe
        /// rows that cannot match either way.
        #[test]
        fn a_sort_merge_join_across_stages_is_a_candidate() {
            use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
            use datafusion::physical_plan::joins::SortMergeJoinExec;

            let build = read(0, Some(1_000), "k", arrow::datatypes::DataType::Int64);
            let probe = read(1, Some(10_000_000), "k", arrow::datatypes::DataType::Int64);
            let on: Vec<(
                Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            )> = vec![(Arc::new(Column::new("k", 0)), Arc::new(Column::new("k", 0)))];
            let sort_options = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(
                Column::new("k", 0),
            ))])
            .expect("ordering")
            .iter()
            .map(|e| e.options)
            .collect();
            let smj = SortMergeJoinExec::try_new(
                build,
                probe,
                on,
                None,
                JoinType::Inner,
                sort_options,
                NullEquality::NullEqualsNothing,
            )
            .expect("sort-merge join over two shuffle reads");

            let plan: Arc<dyn ExecutionPlan> = Arc::new(smj);
            let mut candidates = Vec::new();
            let mut rejects = RuntimeFilterRejects::default();
            collect_runtime_filter_candidates(&plan, &mut candidates, &mut rejects);

            assert_eq!(
                rejects.joins, 1,
                "the sort-merge join must now be inspected like any other equijoin"
            );
            assert_eq!(
                rejects.joins_of_unsupported_kind, 0,
                "and must no longer be written off as unreadable"
            );
            assert_eq!(
                candidates.len(),
                1,
                "a selective cross-stage join is a candidate"
            );
            assert_eq!(candidates[0].build_stage, 0);
            assert_eq!(candidates[0].probe_stage, 1);
        }

        /// A nested-loop join stays uncounted-as-inspected: it has no equijoin
        /// pairs at all, so there is no key to build a filter over. That is a
        /// category error, not a gap to close.
        #[test]
        fn a_nested_loop_join_remains_unreadable() {
            use datafusion::physical_plan::joins::NestedLoopJoinExec;

            let left = read(0, Some(1_000), "k", arrow::datatypes::DataType::Int64);
            let right = read(1, Some(10_000_000), "k", arrow::datatypes::DataType::Int64);
            let nlj = NestedLoopJoinExec::try_new(left, right, None, &JoinType::Inner, None)
                .expect("nested-loop join");

            let plan: Arc<dyn ExecutionPlan> = Arc::new(nlj);
            let mut candidates = Vec::new();
            let mut rejects = RuntimeFilterRejects::default();
            collect_runtime_filter_candidates(&plan, &mut candidates, &mut rejects);

            assert_eq!(rejects.joins_of_unsupported_kind, 1);
            assert_eq!(rejects.joins, 0);
            assert!(candidates.is_empty());
        }

        fn schema(name: &str, key: arrow::datatypes::DataType) -> SchemaRef {
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new(name, key, false),
                arrow::datatypes::Field::new("payload", arrow::datatypes::DataType::Utf8, false),
            ]))
        }

        /// A stage draft whose plan is a bare shuffle read of `stage`, standing
        /// in for whatever subtree really produced it.
        fn draft(stage: usize, rows: Option<usize>, key: &str) -> StageDraft {
            StageDraft {
                plan: Arc::new(
                    ShuffleReadExec::new(
                        stage,
                        4,
                        4,
                        schema(key, arrow::datatypes::DataType::Int64),
                        None,
                    )
                    .with_upstream_estimate(rows, None),
                ),
                shuffle: Some(StageShuffleOutput {
                    key_columns: vec![String::from(key)],
                    num_output_partitions: 4,
                }),
                subqueries: None,
            }
        }

        fn read(
            stage: usize,
            rows: Option<usize>,
            key: &str,
            key_type: arrow::datatypes::DataType,
        ) -> Arc<dyn ExecutionPlan> {
            Arc::new(
                ShuffleReadExec::new(stage, 4, 4, schema(key, key_type), None)
                    .with_upstream_estimate(rows, None),
            )
        }

        fn join_of(
            build: Arc<dyn ExecutionPlan>,
            probe: Arc<dyn ExecutionPlan>,
            join_type: JoinType,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(HashJoinExec::try_new(
                build,
                probe,
                vec![(
                    Arc::new(Column::new("bkey", 0)),
                    Arc::new(Column::new("pkey", 0)),
                )],
                None,
                &join_type,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )?))
        }

        /// The q10 shape: a small build stage, a probe stage 100x bigger, joined
        /// across a stage boundary.
        fn q10_shaped() -> datafusion::error::Result<(Arc<dyn ExecutionPlan>, Vec<StageDraft>)> {
            let root = join_of(
                read(
                    0,
                    Some(1_000_000),
                    "bkey",
                    arrow::datatypes::DataType::Int64,
                ),
                read(
                    1,
                    Some(100_000_000),
                    "pkey",
                    arrow::datatypes::DataType::Int64,
                ),
                JoinType::Inner,
            )?;
            Ok((
                root,
                vec![
                    draft(0, Some(1_000_000), "bkey"),
                    draft(1, Some(100_000_000), "pkey"),
                ],
            ))
        }

        #[test]
        fn a_filter_stage_is_injected_and_the_probe_stage_waits_on_it() {
            let (root, mut drafts) = q10_shaped().expect("plan");
            assert_eq!(
                inject_runtime_filters_unconditionally(&root, &mut drafts),
                1
            );
            assert_eq!(drafts.len(), 3, "one filter stage must have been appended");

            let filter = &drafts[2];
            let shuffle = filter.shuffle.as_ref().expect("filter stage shuffles");
            assert!(
                shuffle.key_columns.is_empty() && shuffle.num_output_partitions == 1,
                "the filter must gather to ONE keyless partition; any other shape means \
                 every probe task fetches N partials instead of one filter"
            );
            assert_eq!(
                filter.plan.output_partitioning().partition_count(),
                1,
                "the filter stage must be a single task, or the broadcast it feeds \
                 multiplies by the task count"
            );

            // The inverted edge: the PROBE stage now depends on the filter stage.
            assert!(
                collect_upstream_stage_indexes(&drafts[1].plan).contains(&2),
                "the probe stage must declare the filter stage upstream, or the \
                 scheduler will run it before the filter exists"
            );
        }

        /// The probe stage's output schema is what its shuffle key columns are
        /// resolved against by name. Changing it would misroute every row.
        #[test]
        fn the_probe_stages_output_schema_is_untouched() {
            let (root, mut drafts) = q10_shaped().expect("plan");
            let before = drafts[1].plan.schema();
            inject_runtime_filters_unconditionally(&root, &mut drafts);
            assert_eq!(
                before.fields(),
                drafts[1].plan.schema().fields(),
                "wrapping the probe stage must not change its columns"
            );
        }

        #[test]
        fn the_filter_stage_emits_one_binary_column() {
            let (root, mut drafts) = q10_shaped().expect("plan");
            inject_runtime_filters_unconditionally(&root, &mut drafts);
            assert_eq!(
                drafts[2].plan.schema().fields().len(),
                1,
                "a filter stage carries only the serialized bloom"
            );
        }

        #[test]
        fn a_non_inner_join_gets_no_filter() {
            for join_type in [
                JoinType::Full,
                JoinType::Right,
                JoinType::RightAnti,
                JoinType::LeftAnti,
            ] {
                let root = join_of(
                    read(
                        0,
                        Some(1_000_000),
                        "bkey",
                        arrow::datatypes::DataType::Int64,
                    ),
                    read(
                        1,
                        Some(100_000_000),
                        "pkey",
                        arrow::datatypes::DataType::Int64,
                    ),
                    join_type,
                )
                .expect("join");
                let mut drafts = vec![
                    draft(0, Some(1_000_000), "bkey"),
                    draft(1, Some(100_000_000), "pkey"),
                ];
                assert_eq!(
                    inject_runtime_filters_unconditionally(&root, &mut drafts),
                    0,
                    "{join_type:?} can preserve unmatched PROBE rows; dropping them is a \
                     wrong answer, not a slow one"
                );
            }
        }

        #[test]
        fn a_join_inside_one_stage_gets_no_filter() {
            let root = join_of(
                read(
                    0,
                    Some(1_000_000),
                    "bkey",
                    arrow::datatypes::DataType::Int64,
                ),
                read(
                    0,
                    Some(100_000_000),
                    "pkey",
                    arrow::datatypes::DataType::Int64,
                ),
                JoinType::Inner,
            )
            .expect("join");
            let mut drafts = vec![draft(0, Some(1_000_000), "bkey")];
            assert_eq!(
                inject_runtime_filters_unconditionally(&root, &mut drafts),
                0,
                "a same-stage join already gets DataFusion's own dynamic filter"
            );
        }

        #[test]
        fn an_absent_estimate_refuses_rather_than_guesses() {
            for (build_rows, probe_rows) in [(None, Some(100_000_000)), (Some(1_000_000), None)] {
                let root = join_of(
                    read(0, build_rows, "bkey", arrow::datatypes::DataType::Int64),
                    read(1, probe_rows, "pkey", arrow::datatypes::DataType::Int64),
                    JoinType::Inner,
                )
                .expect("join");
                let mut drafts = vec![draft(0, build_rows, "bkey"), draft(1, probe_rows, "pkey")];
                assert_eq!(
                    inject_runtime_filters_unconditionally(&root, &mut drafts),
                    0,
                    "Precision::Absent means 'no idea', never 'small' — guessing here is \
                     the SpillableJoinSelection lesson"
                );
            }
        }

        #[test]
        fn a_probe_barely_bigger_than_the_build_is_not_worth_a_stage() {
            let root = join_of(
                read(
                    0,
                    Some(1_000_000),
                    "bkey",
                    arrow::datatypes::DataType::Int64,
                ),
                read(
                    1,
                    Some(2_000_000),
                    "pkey",
                    arrow::datatypes::DataType::Int64,
                ),
                JoinType::Inner,
            )
            .expect("join");
            let mut drafts = vec![
                draft(0, Some(1_000_000), "bkey"),
                draft(1, Some(2_000_000), "pkey"),
            ];
            assert_eq!(
                inject_runtime_filters_unconditionally(&root, &mut drafts),
                0,
                "2x is not enough to repay an extra scan plus a broadcast"
            );
        }

        #[test]
        fn an_oversized_filter_is_refused() {
            // Enough distinct keys that the planned filter hits the 16 MB cap,
            // where the false-positive rate has degraded towards "matches
            // everything" and the broadcast costs more than the scan it saves.
            let huge = 5_000_000_000usize;
            let root = join_of(
                read(0, Some(huge), "bkey", arrow::datatypes::DataType::Int64),
                read(1, Some(huge * 8), "pkey", arrow::datatypes::DataType::Int64),
                JoinType::Inner,
            )
            .expect("join");
            let mut drafts = vec![
                draft(0, Some(huge), "bkey"),
                draft(1, Some(huge * 8), "pkey"),
            ];
            assert_eq!(
                inject_runtime_filters_unconditionally(&root, &mut drafts),
                0
            );
        }

        #[test]
        fn an_unsupported_key_type_is_refused_not_guessed() {
            let root = join_of(
                read(
                    0,
                    Some(1_000_000),
                    "bkey",
                    arrow::datatypes::DataType::Float64,
                ),
                read(
                    1,
                    Some(100_000_000),
                    "pkey",
                    arrow::datatypes::DataType::Float64,
                ),
                JoinType::Inner,
            )
            .expect("join");
            let mut drafts = vec![
                draft(0, Some(1_000_000), "bkey"),
                draft(1, Some(100_000_000), "pkey"),
            ];
            assert_eq!(
                inject_runtime_filters_unconditionally(&root, &mut drafts),
                0,
                "-0.0 == 0.0 compares equal but encodes differently, so a float bloom \
                 would produce false negatives"
            );
        }

        /// Guard 4: the new edge must never close a loop. If the build stage
        /// already reads the probe stage, `probe -> filter -> ... -> probe` is a
        /// cycle, and the scheduler's answer to a cyclic job is to reject it.
        #[test]
        fn a_filter_that_would_close_a_cycle_is_not_injected() {
            let root = join_of(
                read(
                    0,
                    Some(1_000_000),
                    "bkey",
                    arrow::datatypes::DataType::Int64,
                ),
                read(
                    1,
                    Some(100_000_000),
                    "pkey",
                    arrow::datatypes::DataType::Int64,
                ),
                JoinType::Inner,
            )
            .expect("join");
            let mut drafts = vec![
                draft(0, Some(1_000_000), "bkey"),
                draft(1, Some(100_000_000), "pkey"),
            ];
            // Make stage 0 (build) read stage 1 (probe).
            drafts[0].plan = Arc::new(
                ShuffleReadExec::new(
                    1,
                    4,
                    4,
                    schema("bkey", arrow::datatypes::DataType::Int64),
                    None,
                )
                .with_upstream_estimate(Some(1_000_000), None),
            );
            assert!(
                stage_depends_on(&drafts, 0, 1),
                "precondition: build reads probe"
            );
            assert_eq!(
                inject_runtime_filters_unconditionally(&root, &mut drafts),
                0
            );
        }

        /// With the feature off, the pass must still change nothing *and* still
        /// be able to count — the diagnostic is what tells an operator whether
        /// turning the flag on is worth trying.
        ///
        /// Before the dry run existed, `inject_runtime_filters` returned before
        /// computing its rejection breakdown, so every run since the counters
        /// landed produced zero `runtime-filter: pass complete` lines and the
        /// question stayed unanswerable without first enabling the thing being
        /// evaluated.
        #[test]
        fn the_dry_run_reports_without_rewriting_when_the_flag_is_off() {
            let root: Arc<dyn ExecutionPlan> = Arc::new(ShuffleReadExec::new(
                0,
                1,
                1,
                schema("k", arrow::datatypes::DataType::Int64),
                None,
            ));
            let mut drafts = vec![draft(0, Some(1), "a"), draft(1, Some(1), "b")];
            let before = drafts.len();
            // `enabled()` reads the environment, which this test does not touch:
            // the default is off, which is the case under audit.
            assert!(
                !crate::runtime_filter_exec::enabled(),
                "precondition: the feature ships dark"
            );
            assert_eq!(
                inject_runtime_filters(&root, &mut drafts),
                0,
                "a disabled pass must inject nothing"
            );
            assert_eq!(drafts.len(), before, "a disabled pass must not add a stage");
        }

        #[test]
        fn stage_dependency_reachability_is_transitive_and_terminates_on_cycles() {
            let mut drafts = vec![
                draft(0, Some(1), "a"),
                draft(1, Some(1), "b"),
                draft(2, Some(1), "c"),
            ];
            // 2 -> 1 -> 0
            drafts[1].plan = Arc::new(ShuffleReadExec::new(
                0,
                1,
                1,
                schema("b", arrow::datatypes::DataType::Int64),
                None,
            ));
            drafts[2].plan = Arc::new(ShuffleReadExec::new(
                1,
                1,
                1,
                schema("c", arrow::datatypes::DataType::Int64),
                None,
            ));
            assert!(
                stage_depends_on(&drafts, 2, 0),
                "reachability must be transitive"
            );
            assert!(!stage_depends_on(&drafts, 0, 2), "and directional");
        }

        /// A stage severed from a `ScalarSubqueryExec` is parameterised by a
        /// subquery result; cloning its subtree without the wrapper yields a
        /// fragment that cannot decode.
        #[test]
        fn a_subquery_parameterised_stage_is_never_cloned_into_a_filter() {
            let (root, mut drafts) = q10_shaped().expect("plan");
            drafts[0].subqueries = Some(StageSubqueryContext {
                links: Vec::new(),
                results: Default::default(),
            });
            assert_eq!(
                inject_runtime_filters_unconditionally(&root, &mut drafts),
                0
            );
        }

        #[test]
        fn the_feature_is_off_unless_the_flag_says_otherwise() {
            let (root, mut drafts) = q10_shaped().expect("plan");
            assert_eq!(
                inject_runtime_filters(&root, &mut drafts),
                0,
                "the flag-checking entry point must decline by default: this rule \
                 rewrites the stage DAG and ships dark until a clean 22-query sweep"
            );
            assert_eq!(drafts.len(), 2, "and it must not have touched the drafts");
        }

        /// Every node in an injected plan must survive the proto round trip.
        /// A node the codec cannot encode does not fail loudly — the coordinator
        /// silently runs the whole query as a SINGLE TASK.
        #[test]
        fn the_injected_stages_round_trip_through_the_codec() {
            let (root, mut drafts) = q10_shaped().expect("plan");
            assert_eq!(
                inject_runtime_filters_unconditionally(&root, &mut drafts),
                1
            );

            let codec = KrishivPhysicalCodec::coordinator();
            let session = fragment_decode_session_context();
            let ctx = session.task_ctx();
            for (index, draft) in drafts.iter().enumerate() {
                let bytes = encode_dfplan_bytes(Arc::clone(&draft.plan), &codec)
                    .unwrap_or_else(|e| panic!("stage {index} did not encode: {e}"));
                verify_dfplan_roundtrip(&bytes, &codec, &ctx, Some(&draft.plan))
                    .unwrap_or_else(|e| panic!("stage {index} did not decode: {e}"));
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod registration_parity_tests {
    use super::*;

    /// Write one small parquet file and return the path to register.
    async fn parquet_at(dir: &std::path::Path) -> String {
        let path = dir.join("t.parquet");
        let path = path.to_str().expect("temp path is utf-8").to_owned();
        let ctx = SessionContext::new();
        ctx.sql(&format!(
            "COPY (SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(k, v)) \
             TO '{path}' STORED AS PARQUET"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        path
    }

    /// Register the same file both ways and hand back the two providers.
    async fn both_ways(
        path: &str,
    ) -> (
        Arc<dyn datafusion::datasource::TableProvider>,
        Arc<dyn datafusion::datasource::TableProvider>,
    ) {
        let ctx = planning_session_context(4);
        register_parquet_table(&ctx, &ParquetTableSpec::new("plain", path))
            .await
            .unwrap();
        register_parquet_table(
            &ctx,
            &ParquetTableSpec::new("keyed", path).with_primary_key(["k"]),
        )
        .await
        .unwrap();
        (
            ctx.table_provider("plain").await.unwrap(),
            ctx.table_provider("keyed").await.unwrap(),
        )
    }

    fn listing_options(
        provider: &Arc<dyn datafusion::datasource::TableProvider>,
    ) -> datafusion::datasource::listing::ListingOptions {
        // `TableProvider: Any` — upcast to downcast, as elsewhere in this file
        // (DF 54 has no `as_any` on the trait).
        let any = provider.as_ref() as &dyn std::any::Any;
        any.downcast_ref::<datafusion::datasource::listing::ListingTable>()
            .expect("parquet registration produces a ListingTable")
            .options()
            .clone()
    }

    /// An object-store directory must reach `ListingTableUrl` as a prefix.
    ///
    /// `ListingTableUrl::parse` decides file-vs-directory by statting, which an
    /// object store cannot answer, so `s3://b/sf100/lineitem` was read as a
    /// file and rejected for not ending in `.parquet` — a message that blames
    /// the extension for a path that is simply a directory. The same
    /// registration worked locally and failed remotely.
    #[test]
    fn an_extensionless_object_store_path_is_treated_as_a_directory() {
        assert_eq!(
            super::directory_aware_url("s3://b/sf100/lineitem"),
            "s3://b/sf100/lineitem/"
        );
        // Already a prefix: unchanged, no doubled slash.
        assert_eq!(
            super::directory_aware_url("s3://b/sf100/lineitem/"),
            "s3://b/sf100/lineitem/"
        );
        // A named file keeps file semantics.
        assert_eq!(
            super::directory_aware_url("s3://b/sf100/nation.parquet"),
            "s3://b/sf100/nation.parquet"
        );
        // Local paths are untouched: statting them is better information than
        // any guess this function could make.
        assert_eq!(
            super::directory_aware_url("/data/sf100/lineitem"),
            "/data/sf100/lineitem"
        );
        assert_eq!(super::directory_aware_url("relative/dir"), "relative/dir");
    }

    /// The invariant the two branches of [`register_parquet_table`] exist under:
    /// declaring a key changes the *constraints* and nothing else.
    ///
    /// It was violated silently. The keyed branch built its own
    /// `ListingOptions::new(..)`, whose documented defaults are `collect_stat:
    /// false` and `target_partitions: 1` — while `register_parquet` routes
    /// through `ReadOptions::to_listing_options`, which ends in
    /// `.with_session_config_options(config)` and takes both from the session.
    ///
    /// So every table with a declared primary key had **statistics collection
    /// off**. On the SF100 cluster that made `SpillableJoinSelection` report
    /// `unmeasurable == hash_joins` in all 414 passes and convert zero joins,
    /// leaving q21's oversized build side with nothing to catch it.
    ///
    /// Comparing the whole `ListingOptions` rather than the two fields that
    /// were wrong is deliberate: the next field DataFusion adds to
    /// `with_session_config_options` fails this test instead of quietly
    /// re-opening the same hole.
    #[tokio::test]
    async fn declaring_a_primary_key_changes_only_the_constraints() {
        let dir = tempfile::tempdir().unwrap();
        let path = parquet_at(dir.path()).await;
        let (plain, keyed) = both_ways(&path).await;

        let (plain_options, keyed_options) = (listing_options(&plain), listing_options(&keyed));
        assert_eq!(
            format!("{plain_options:?}"),
            format!("{keyed_options:?}"),
            "a declared key must not change how the table is read"
        );
        assert!(
            keyed_options.collect_stat,
            "statistics collection must stay on: every size-based rule goes \
             blind without it"
        );

        // The one difference that is supposed to exist.
        assert!(plain.constraints().is_none_or(|c| c.is_empty()));
        assert!(
            keyed.constraints().is_some_and(|c| !c.is_empty()),
            "the declared key must reach the optimizer as a constraint"
        );
    }

    /// The behavioural half: a keyed table must still report row counts.
    ///
    /// The options comparison above pins the mechanism; this pins the outcome
    /// the mechanism exists for, so the test still fails if a future change
    /// keeps `collect_stat` set but loses statistics some other way.
    #[tokio::test]
    async fn a_keyed_table_still_reports_row_counts() {
        use datafusion::common::stats::Precision;
        let dir = tempfile::tempdir().unwrap();
        let path = parquet_at(dir.path()).await;
        let (_, keyed) = both_ways(&path).await;

        let ctx = planning_session_context(4);
        ctx.register_table("keyed", Arc::clone(&keyed)).unwrap();
        let stats = ctx
            .sql("SELECT k, v FROM keyed")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
            .partition_statistics(None)
            .unwrap();
        assert!(
            matches!(stats.num_rows, Precision::Exact(3) | Precision::Inexact(3)),
            "expected a row count for a keyed table, got {:?} — this is the \
             shape that made every join unmeasurable at SF100",
            stats.num_rows
        );
    }
}
