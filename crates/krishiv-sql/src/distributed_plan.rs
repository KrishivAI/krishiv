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
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties as _, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use futures::{StreamExt as _, TryStreamExt as _};

use crate::{SqlError, SqlResult};

/// Task-fragment body prefix for proto-encoded physical-plan subtrees.
pub const DFPLAN_BODY_PREFIX: &str = "dfplan:v1:";

/// Env var overriding the target partition count used when planning a
/// distributed batch query (bounds both scan parallelism and shuffle
/// partition count). Default: 4.
pub const STAGE_TARGET_PARTITIONS_ENV: &str = "KRISHIV_STAGE_TARGET_PARTITIONS";

/// Env var that disables stage splitting entirely (`off`/`0`/`false`).
pub const STAGE_SPLIT_ENV: &str = "KRISHIV_STAGE_SPLIT";

/// Resolve the planning-time target partition count for distributed stages.
pub fn stage_target_partitions_from_env() -> usize {
    std::env::var(STAGE_TARGET_PARTITIONS_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| n >= 2)
        .unwrap_or(4)
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
    let mut config = SessionConfig::new().with_target_partitions(target_partitions.max(1));
    config
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = false;
    SessionContext::new_with_config(config)
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
    Ok(format!("{DFPLAN_BODY_PREFIX}{}:{b64}", spec.render()))
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
    fn read_partition(
        &self,
        upstream_stage_index: usize,
        map_task_index: usize,
        partition: usize,
    ) -> futures::future::BoxFuture<'static, Result<Vec<arrow::record_batch::RecordBatch>, String>>
    {
        if upstream_stage_index == self.range.upstream_stage_index
            && !(self.range.start..self.range.end).contains(&map_task_index)
        {
            return Box::pin(async { Ok(Vec::new()) });
        }
        self.inner
            .read_partition(upstream_stage_index, map_task_index, partition)
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

/// Plan a query over local parquet tables and cut it into stages
/// (coordinator seam — keeps DataFusion types out of the scheduler crate).
///
/// `tables` are `(table_name, path)` pairs; a path may be a single parquet
/// file or a directory dataset. Planning happens on a fresh
/// [`planning_session_context`], so krishiv SQL extensions (streaming
/// windows, catalog DML, UDFs) fail to plan here and surface as `Err` —
/// callers treat any error as "fall back to the single-task path".
pub async fn build_stages_for_parquet_query(
    query: &str,
    tables: &[(String, String)],
) -> SqlResult<Option<DistributedStagePlan>> {
    let ctx = planning_session_context(stage_target_partitions_from_env());
    for (name, path) in tables {
        ctx.register_parquet(
            name,
            path,
            datafusion::prelude::ParquetReadOptions::default(),
        )
        .await
        .map_err(|e| SqlError::DataFusion {
            message: format!("staged planning: register '{name}': {e}"),
        })?;
    }
    // A Python scalar UDF shipped inline (`/* krishiv-register-python-udf */`)
    // must be known by name/signature for planning to resolve it, then stripped
    // so the parser sees clean SQL. The stage bodies carry the same directive so
    // the executor reconstructs the worker-backed UDF before decoding the plan;
    // here the coordinator only needs the signature (the closure is never
    // invoked during planning — Volatile keeps it out of const-folding).
    let query = register_python_udf_signatures_and_strip(&ctx, query)?;
    let df = ctx.sql(&query).await.map_err(|e| SqlError::DataFusion {
        message: format!("staged planning: {e}"),
    })?;
    let plan = df
        .create_physical_plan()
        .await
        .map_err(|e| SqlError::DataFusion {
            message: format!("staged physical planning: {e}"),
        })?;
    build_distributed_stages(plan)
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
    /// Read one map task's output for `partition` of `upstream_stage_index`.
    ///
    /// A missing partition (map task produced no rows for it) returns an
    /// empty vec, not an error.
    fn read_partition(
        &self,
        upstream_stage_index: usize,
        map_task_index: usize,
        partition: usize,
    ) -> futures::future::BoxFuture<'static, Result<Vec<arrow::record_batch::RecordBatch>, String>>;
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
        }
    }

    pub fn upstream_stage_index(&self) -> usize {
        self.upstream_stage_index
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
        let stream = futures::stream::iter(0..self.num_map_tasks)
            .then(move |map_task| {
                let reader = Arc::clone(&reader);
                async move {
                    reader
                        .read_partition(stage, map_task, partition)
                        .await
                        .map_err(|e| {
                            DataFusionError::Execution(format!(
                                "shuffle read (stage {stage}, map {map_task}, partition \
                                 {partition}): {e}"
                            ))
                        })
                }
            })
            .map_ok(|batches| {
                futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>))
            })
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
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
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
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
        Ok(Arc::new(ShuffleReadExec::new(
            payload.stage,
            payload.map_tasks,
            payload.partitions,
            schema,
            self.reader.clone(),
        )))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        let read = node.downcast_ref::<ShuffleReadExec>().ok_or_else(|| {
            DataFusionError::NotImplemented(format!(
                "KrishivPhysicalCodec cannot encode node {}",
                node.name()
            ))
        })?;
        let schema_bytes = schema_to_ipc_bytes(&read.schema).map_err(DataFusionError::Internal)?;
        let payload = ShuffleReadNodePayload {
            v: 1,
            stage: read.upstream_stage_index,
            map_tasks: read.num_map_tasks,
            partitions: read.partition_count(),
            schema_ipc_b64: base64::engine::general_purpose::STANDARD.encode(&schema_bytes),
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
    let mut drafts: Vec<StageDraft> = Vec::new();
    let root = match cut_exchanges(plan, &mut drafts) {
        Ok(root) => root,
        Err(Unsupported(reason)) => {
            tracing::debug!(
                reason,
                "stage split unsupported; falling back to single task"
            );
            return Ok(None);
        }
    };
    if drafts.is_empty() {
        return Ok(None);
    }
    drafts.push(StageDraft {
        plan: root,
        shuffle: None,
    });

    // Prove every stage subtree is partition-independent: no exchange may
    // remain inside a stage (each task executes one root partition; a
    // leftover RepartitionExec would re-drive all inputs per task).
    for draft in &drafts {
        if let Some(reason) = find_unsupported_stage_node(&draft.plan) {
            tracing::debug!(
                reason,
                "stage subtree not partition-independent; falling back to single task"
            );
            return Ok(None);
        }
    }

    let codec = KrishivPhysicalCodec::coordinator();
    let mut stages = Vec::with_capacity(drafts.len());
    for draft in drafts {
        let partition_count = draft.plan.output_partitioning().partition_count();
        if partition_count == 0 {
            tracing::debug!("stage subtree has zero output partitions; falling back");
            return Ok(None);
        }
        let upstream_stage_indexes = collect_upstream_stage_indexes(&draft.plan);
        let bytes = match encode_dfplan_bytes(Arc::clone(&draft.plan), &codec) {
            Ok(bytes) => bytes,
            Err(error) => {
                // Plans over non-serializable providers (memory tables,
                // custom scans) fall back rather than fail the query.
                tracing::debug!(%error, "stage plan not proto-serializable; falling back");
                return Ok(None);
            }
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
        let stage_index = stages.len();
        stages.push(StageDraft {
            plan: input,
            shuffle: Some(StageShuffleOutput {
                key_columns,
                num_output_partitions: *num_partitions,
            }),
        });
        return Ok(Arc::new(ShuffleReadExec::new(
            stage_index,
            map_task_count,
            *num_partitions,
            schema,
            None,
        )));
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
    for child in plan.children() {
        if let Some(reason) = find_unsupported_stage_node(child) {
            return Some(reason);
        }
    }
    None
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
    use super::*;
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::displayable;
    use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
    use std::collections::HashMap;
    use std::sync::Mutex;

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

    /// ADR-0003 risk gate: a scan→filter→hash-aggregate plan round-trips
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
        fn read_partition(
            &self,
            upstream_stage_index: usize,
            map_task_index: usize,
            partition: usize,
        ) -> futures::future::BoxFuture<'static, Result<Vec<RecordBatch>, String>> {
            let batches = self
                .partitions
                .lock()
                .expect("store lock")
                .get(&(upstream_stage_index, map_task_index, partition))
                .cloned()
                .unwrap_or_default();
            Box::pin(async move { Ok(batches) })
        }
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
    async fn scan_only_plan_is_not_split() {
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
        assert!(
            build_distributed_stages(plan)
                .expect("build stages")
                .is_none()
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
