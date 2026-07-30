//! Cross-stage runtime filter: the two plan nodes that carry a bloom filter
//! from a join's build side into the probe side's *map stage*.
//!
//! # Why this exists at all
//!
//! DataFusion builds a dynamic filter from a join's build side and pushes it
//! into the probe side's scan. That works single-node because both live in one
//! plan. We cut the plan at every exchange, so the probe's scan runs in its own
//! stage *before* the join stage exists — there is no build side to learn from,
//! and every SF100 plan dump shows the placeholder unfilled:
//!
//! ```text
//! lineitem ... predicate=l_returnflag = R AND DynamicFilter [ empty ]
//! ```
//!
//! The cost is not the scan, it is the shuffle. TPC-H q10 hash-partitions ALL
//! ~150M returned lineitem rows across a pod network measured at ~11 MiB/s when
//! the orders side selects one quarter (~3.5%) and only ~7M of those rows can
//! possibly join. Dropping the other 95% *before* the shuffle write removes the
//! bytes from the wire, which is the binding constraint.
//!
//! # The shape
//!
//! ```text
//!   stage B (build side)          stage F (filter)         stage P (probe)
//!   ────────────────────          ────────────────         ───────────────
//!   scan orders                   scan orders              scan lineitem
//!   filter o_orderdate            filter o_orderdate       filter l_returnflag
//!   └─> shuffle(o_orderkey)       project o_orderkey       └─> RuntimeFilterProbeExec
//!                                 coalesce -> 1 task            ├── data
//!                                 RuntimeFilterBuildExec        └── ShuffleReadExec(F)
//!                                 └─> shuffle(keyless, 1)   └─> shuffle(l_orderkey)
//! ```
//!
//! Stage F is a **clone of stage B's subtree**, not a read of its output: the
//! build side is small by construction (the planner's selectivity gate refuses
//! otherwise) and re-scanning it from local disk at ~300 MB/s beats re-reading
//! its shuffle output across a ~11 MiB/s pod network. It is also what Spark's
//! `InjectRuntimeFilter` does.
//!
//! Coalescing F to a single task is deliberate. The filter is a broadcast: every
//! task of stage P must fetch the whole thing. One task producing one ~6.5 MB
//! blob costs `P × 6.5 MB`; N tasks producing N partials costs `P × N × 6.5 MB`,
//! which at q10's shape is 2 GB of wire to save a shuffle — the fix paying for
//! itself many times over in the wrong direction.
//!
//! # Why the filter travels as ordinary shuffle data
//!
//! It is a one-row `RecordBatch` with a single `Binary` column, written through
//! the same keyless single-partition gather the stage cutter already emits for
//! ungrouped aggregates, and read back through an ordinary [`ShuffleReadExec`].
//! So there is no new RPC, no new store key, no new transport, and the stage DAG
//! edge `P → F` is an edge the scheduler already knows how to order and
//! cycle-check.
//!
//! # Correctness
//!
//! A bloom filter has false positives but never false negatives, so a row that
//! *can* join is never dropped and the join's output is unchanged. Everything
//! here is built to keep that one property true: see
//! [`krishiv_shuffle::RuntimeFilter`] for the encoding, union and fail-open
//! rules, and `runtime_filter_candidates` in `distributed_plan` for the join
//! shapes this may fire on (inner only).

use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, BinaryArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use futures::{StreamExt, TryStreamExt};
use krishiv_shuffle::{FilterKeyType, RuntimeFilter, RuntimeFilterBuilder};

/// Env flag gating the whole feature. Off by default.
///
/// The name is long on purpose. `KRISHIV_RUNTIME_FILTERS` already exists — it is
/// DataFusion's *in-plan* dynamic-filter master switch, and it defaults to on.
/// Naming this one the singular of that would put two flags one letter apart,
/// with opposite defaults and different mechanisms, in the same namespace: a
/// trap an operator only discovers by setting one and measuring nothing change.
///
/// Guard 5 of the design register: two prior plan rules that fired too widely
/// cost more than they gained (`semi-join-rule-was-the-pessimization` took q2
/// from 189 s to a nested-loop join, and the broadcast over-reach regressed four
/// queries). A rule that rewrites the stage DAG ships dark until a full 22-query
/// sweep is clean against the queries we currently *win*, not only against q10.
pub const RUNTIME_FILTER_ENV: &str = "KRISHIV_CROSS_STAGE_RUNTIME_FILTER";

/// Is cross-stage runtime filtering enabled?
#[must_use]
pub fn enabled() -> bool {
    std::env::var(RUNTIME_FILTER_ENV)
        .ok()
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            v == "1" || v == "true" || v == "on" || v == "yes"
        })
        .unwrap_or(false)
}

/// Column name of the single `Binary` column a filter stage emits.
pub const FILTER_COLUMN: &str = "krishiv_runtime_filter";

/// Schema of a filter stage's output: one row, one serialized bloom.
#[must_use]
pub fn filter_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        FILTER_COLUMN,
        DataType::Binary,
        false,
    )]))
}

fn exec_err(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

// ── RuntimeFilterBuildExec ─────────────────────────────────────────────────

/// Consumes its input and emits ONE row: the serialized bloom filter of a
/// single key column.
///
/// Input rows are not forwarded — this node's output *is* the filter. It is the
/// root of a dedicated filter stage, never spliced into a data path.
#[derive(Debug)]
pub struct RuntimeFilterBuildExec {
    input: Arc<dyn ExecutionPlan>,
    /// Index of the key column in `input`'s schema.
    key_index: usize,
    key_type: FilterKeyType,
    /// Filter size in bytes, fixed by the planner.
    ///
    /// Fixed rather than derived per task because [`RuntimeFilter::union`]
    /// refuses partials of differing sizes: OR-ing different-sized bitsets
    /// misplaces blocks and *loses* set bits, which is the one failure mode that
    /// produces false negatives — i.e. wrong answers.
    filter_bytes: usize,
    properties: Arc<PlanProperties>,
}

impl RuntimeFilterBuildExec {
    /// Build a filter node over `input`'s column `key_index`.
    ///
    /// Errors rather than panics on an out-of-range index or an unsupported key
    /// type: this runs inside the stage builder, where declining to inject a
    /// filter is always an acceptable outcome and a panic never is.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        key_index: usize,
        filter_bytes: usize,
    ) -> Result<Self, DataFusionError> {
        let schema = input.schema();
        let field = schema.fields().get(key_index).ok_or_else(|| {
            exec_err(format!(
                "runtime filter key index {key_index} is out of range for a {}-column input",
                schema.fields().len()
            ))
        })?;
        let key_type = FilterKeyType::for_data_type(field.data_type()).ok_or_else(|| {
            exec_err(format!(
                "runtime filter cannot key on {} ({:?})",
                field.name(),
                field.data_type()
            ))
        })?;
        let out = filter_schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&out)),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(
                input.output_partitioning().partition_count().max(1),
            ),
            // The filter exists only once the last input row has been seen.
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            key_index,
            key_type,
            filter_bytes,
            properties,
        })
    }

    pub fn key_index(&self) -> usize {
        self.key_index
    }

    pub fn filter_bytes(&self) -> usize {
        self.filter_bytes
    }
}

impl DisplayAs for RuntimeFilterBuildExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RuntimeFilterBuildExec: key_index={}, key_type={:?}, bytes={}",
            self.key_index, self.key_type, self.filter_bytes
        )
    }
}

impl ExecutionPlan for RuntimeFilterBuildExec {
    fn name(&self) -> &str {
        "RuntimeFilterBuildExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let [input] = <[Arc<dyn ExecutionPlan>; 1]>::try_from(children).map_err(|c| {
            exec_err(format!(
                "RuntimeFilterBuildExec takes exactly one child, got {}",
                c.len()
            ))
        })?;
        Ok(Arc::new(Self::try_new(
            input,
            self.key_index,
            self.filter_bytes,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, context)?;
        let key_index = self.key_index;
        let key_type = self.key_type;
        let filter_bytes = self.filter_bytes;
        let out = filter_schema();
        let batch_schema = Arc::clone(&out);
        let built = async move {
            let mut builder = RuntimeFilterBuilder::new(filter_bytes, key_type);
            while let Some(batch) = input.next().await {
                let batch = batch?;
                let column = batch.column(key_index);
                builder
                    .insert_array(column.as_ref())
                    .map_err(|e| exec_err(format!("runtime filter build: {e}")))?;
            }
            let keys = builder.keys_inserted();
            let bytes = builder
                .finish()
                .to_bytes()
                .map_err(|e| exec_err(format!("runtime filter encode: {e}")))?;
            tracing::debug!(
                keys,
                bytes = bytes.len(),
                "built a cross-stage runtime filter"
            );
            let column = BinaryArray::from_vec(vec![bytes.as_slice()]);
            RecordBatch::try_new(batch_schema, vec![Arc::new(column)])
                .map_err(|e| exec_err(format!("runtime filter batch: {e}")))
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            out,
            futures::stream::once(built),
        )))
    }
}

// ── RuntimeFilterProbeExec ─────────────────────────────────────────────────

/// Drops input rows whose key is provably absent from the build side.
///
/// Two children: `[data, filter_source]`. The filter source is an ordinary
/// [`ShuffleReadExec`](crate::distributed_plan::ShuffleReadExec) over the filter
/// stage, so awaiting it is what makes this stage wait for that one — the whole
/// inverted dependency is expressed by having the node as a child.
///
/// **Fails open in every degenerate case.** No filter rows, an undecodable
/// payload, or a key type that disagrees with the build side all yield "keep
/// every row": slower than intended, never wrong.
#[derive(Debug)]
pub struct RuntimeFilterProbeExec {
    input: Arc<dyn ExecutionPlan>,
    filter_source: Arc<dyn ExecutionPlan>,
    /// Index of the key column in `input`'s schema.
    key_index: usize,
    properties: Arc<PlanProperties>,
}

impl RuntimeFilterProbeExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        filter_source: Arc<dyn ExecutionPlan>,
        key_index: usize,
    ) -> Result<Self, DataFusionError> {
        let schema = input.schema();
        if key_index >= schema.fields().len() {
            return Err(exec_err(format!(
                "runtime filter probe index {key_index} is out of range for a {}-column input",
                schema.fields().len()
            )));
        }
        // Row count changes, so equivalences and ordering are the input's but
        // statistics are not: report the input's partitioning verbatim, which is
        // what keeps the stage's task count unchanged.
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            filter_source,
            key_index,
            properties,
        })
    }

    pub fn key_index(&self) -> usize {
        self.key_index
    }
}

impl DisplayAs for RuntimeFilterProbeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RuntimeFilterProbeExec: key_index={}", self.key_index)
    }
}

/// Read every filter row the source yields and union them into one filter.
///
/// `None` means "no filter available" — the caller must then pass all rows
/// through. Partials are unioned rather than assumed single so that a filter
/// stage with more than one task stays correct: a probe that saw only some of
/// the partials would produce false negatives, which is a wrong answer.
async fn collect_filter(
    mut source: SendableRecordBatchStream,
) -> Result<Option<RuntimeFilter>, DataFusionError> {
    let mut merged: Option<RuntimeFilter> = None;
    while let Some(batch) = source.next().await {
        let batch = batch?;
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                exec_err(format!(
                    "runtime filter stage produced {:?}, not Binary",
                    batch.column(0).data_type()
                ))
            })?;
        for i in 0..column.len() {
            if column.is_null(i) {
                continue;
            }
            let filter = RuntimeFilter::from_bytes(column.value(i))
                .map_err(|e| exec_err(format!("runtime filter decode: {e}")))?;
            match &mut merged {
                Some(acc) => acc
                    .union(&filter)
                    .map_err(|e| exec_err(format!("runtime filter union: {e}")))?,
                None => merged = Some(filter),
            }
        }
    }
    Ok(merged)
}

impl ExecutionPlan for RuntimeFilterProbeExec {
    fn name(&self) -> &str {
        "RuntimeFilterProbeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input, &self.filter_source]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let [input, filter_source] =
            <[Arc<dyn ExecutionPlan>; 2]>::try_from(children).map_err(|c| {
                exec_err(format!(
                    "RuntimeFilterProbeExec takes exactly two children, got {}",
                    c.len()
                ))
            })?;
        Ok(Arc::new(Self::try_new(input, filter_source, self.key_index)?))
    }

    /// Row counts shrink by an unknown amount, so the row estimate becomes
    /// inexact-at-best and the byte estimate with it.
    ///
    /// Reported as the input's numbers rather than as unknown: `Absent` is read
    /// by `SpillableJoinSelection` as "no idea, keep hash join", and laundering a
    /// known size into an unknown one downstream of this node is how q18 ran out
    /// of memory. An over-estimate is the safe direction — the filter only ever
    /// removes rows.
    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion::error::Result<Arc<datafusion::common::Statistics>> {
        let stats = self.input.partition_statistics(partition)?;
        let mut stats = stats.as_ref().clone();
        stats.num_rows = stats.num_rows.to_inexact();
        stats.total_byte_size = stats.total_byte_size.to_inexact();
        Ok(Arc::new(stats))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        // Partition 0 unconditionally: a filter stage gathers to exactly one
        // output partition, and every probe task needs the whole filter.
        let filter_stream = self.filter_source.execute(0, Arc::clone(&context))?;
        let data = self.input.execute(partition, context)?;
        let key_index = self.key_index;
        let schema = self.input.schema();
        let out = Arc::clone(&schema);
        let filtered = futures::stream::once(async move {
            let filter = collect_filter(filter_stream).await?;
            let Some(filter) = filter else {
                // No filter was produced. Passing every row through is the
                // correct degradation; refusing rows here would be the one
                // outcome this design must never have.
                tracing::warn!(
                    "runtime filter stage produced no filter; passing all probe rows through"
                );
                return Ok::<_, DataFusionError>(data.boxed());
            };
            Ok(data
                .map(move |batch| {
                    let batch = batch?;
                    let mask = filter
                        .contains(batch.column(key_index).as_ref())
                        .map_err(|e| exec_err(format!("runtime filter probe: {e}")))?;
                    arrow::compute::filter_record_batch(&batch, &mask)
                        .map_err(|e| exec_err(format!("runtime filter apply: {e}")))
                })
                .boxed())
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(out, filtered)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use datafusion::catalog::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::prelude::SessionContext;

    fn keys(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))])
            .expect("batch")
    }

    fn source(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        let schema = batch.schema();
        let config = MemorySourceConfig::try_new(&[vec![batch]], schema, None).expect("source");
        Arc::new(DataSourceExec::new(Arc::new(config)))
    }

    async fn collect(plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
        let ctx = SessionContext::new();
        datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("collect")
    }

    /// The end-to-end property: a probe row whose key exists on the build side
    /// always survives, and the surviving set never grows.
    #[tokio::test]
    async fn probe_keeps_every_row_that_could_join_and_drops_most_that_cannot() {
        let build = source(keys(&(0..1000).map(|i| i * 2).collect::<Vec<_>>()));
        let filter = Arc::new(
            RuntimeFilterBuildExec::try_new(build, 0, 4096).expect("build node"),
        ) as Arc<dyn ExecutionPlan>;

        // Probe carries the 1000 matching evens and 1000 non-matching odds.
        let probe_keys: Vec<i64> = (0..2000).collect();
        let probe = source(keys(&probe_keys));
        let node = Arc::new(
            RuntimeFilterProbeExec::try_new(probe, filter, 0).expect("probe node"),
        ) as Arc<dyn ExecutionPlan>;

        let out = collect(node).await;
        let kept: Vec<i64> = out
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("i64")
                    .values()
                    .to_vec()
            })
            .collect();

        for even in (0..2000).step_by(2) {
            assert!(
                kept.contains(&even),
                "key {even} was inserted on the build side but the probe dropped it — \
                 a false negative is a wrong answer, not a slow one"
            );
        }
        assert!(
            kept.len() < 1500,
            "kept {} of 2000 rows; the filter is not rejecting the 1000 absent odd keys, \
             so it removes no shuffle bytes at all",
            kept.len()
        );
    }

    /// A filter source that yields nothing must keep every row.
    #[tokio::test]
    async fn an_absent_filter_keeps_every_row_rather_than_dropping_them() {
        let empty = MemorySourceConfig::try_new(&[vec![]], filter_schema(), None).expect("source");
        let empty = Arc::new(DataSourceExec::new(Arc::new(empty))) as Arc<dyn ExecutionPlan>;
        let probe = source(keys(&[1, 2, 3, 4, 5]));
        let node = Arc::new(
            RuntimeFilterProbeExec::try_new(probe, empty, 0).expect("probe node"),
        ) as Arc<dyn ExecutionPlan>;

        let rows: usize = collect(node).await.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            rows, 5,
            "a missing filter must fail OPEN; dropping rows because the filter stage \
             produced nothing would turn a transport problem into a wrong answer"
        );
    }

    /// An empty build side rejects everything — the filter is real, not a
    /// pass-through that happens to look right in the test above.
    #[tokio::test]
    async fn an_empty_build_side_rejects_every_probe_row() {
        let build = MemorySourceConfig::try_new(
            &[vec![]],
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)])),
            None,
        )
        .expect("source");
        let build = Arc::new(DataSourceExec::new(Arc::new(build))) as Arc<dyn ExecutionPlan>;
        let filter = Arc::new(RuntimeFilterBuildExec::try_new(build, 0, 4096).expect("build"))
            as Arc<dyn ExecutionPlan>;
        let probe = source(keys(&[1, 2, 3, 4, 5]));
        let node = Arc::new(
            RuntimeFilterProbeExec::try_new(probe, filter, 0).expect("probe node"),
        ) as Arc<dyn ExecutionPlan>;

        let rows: usize = collect(node).await.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 0, "nothing on the build side can join anything");
    }

    #[test]
    fn an_unsupported_key_type_is_refused_at_construction() {
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, false)]));
        let config = MemorySourceConfig::try_new(&[vec![]], schema, None).expect("source");
        let input = Arc::new(DataSourceExec::new(Arc::new(config))) as Arc<dyn ExecutionPlan>;
        assert!(
            RuntimeFilterBuildExec::try_new(input, 0, 4096).is_err(),
            "float keys must be refused, not guessed: -0.0 == 0.0 compares equal but \
             hashes differently, so the filter would produce false negatives"
        );
    }

    #[test]
    fn an_out_of_range_key_index_is_an_error_not_a_panic() {
        let input = source(keys(&[1]));
        assert!(RuntimeFilterBuildExec::try_new(Arc::clone(&input), 7, 4096).is_err());
        let filter = source(keys(&[1]));
        assert!(RuntimeFilterProbeExec::try_new(input, filter, 7).is_err());
    }


    /// String keys are the other half of the corpus (q17/q20 filter `part` by
    /// brand and container), and they take a different encoding path from
    /// integers — raw bytes rather than a widened little-endian word.
    #[tokio::test]
    async fn string_keys_survive_the_round_trip_through_both_nodes() {
        use arrow::array::StringArray;
        fn names(values: &[&str]) -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values.to_vec()))])
                .expect("batch")
        }
        let build = source(names(&["BRAND#11", "BRAND#23", "BRAND#42"]));
        let filter = Arc::new(RuntimeFilterBuildExec::try_new(build, 0, 4096).expect("build"))
            as Arc<dyn ExecutionPlan>;
        let probe = source(names(&[
            "BRAND#11", "BRAND#99", "BRAND#23", "BRAND#77", "BRAND#42",
        ]));
        let node = Arc::new(
            RuntimeFilterProbeExec::try_new(probe, filter, 0).expect("probe node"),
        ) as Arc<dyn ExecutionPlan>;

        let kept: Vec<String> = collect(node)
            .await
            .iter()
            .flat_map(|b| {
                let column = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8");
                (0..column.len())
                    .map(|i| column.value(i).to_owned())
                    .collect::<Vec<_>>()
            })
            .collect();
        for present in ["BRAND#11", "BRAND#23", "BRAND#42"] {
            assert!(
                kept.iter().any(|k| k == present),
                "{present} was on the build side and must survive"
            );
        }
    }

    /// Every probe partition must see the WHOLE filter. The filter stage gathers
    /// to one output partition, so a node that passed its own partition index
    /// through to the filter child would read an out-of-range partition on
    /// partition 1 — or, worse, a partial filter.
    #[tokio::test]
    async fn every_probe_partition_reads_the_whole_filter() {
        let build = source(keys(&[10, 20, 30]));
        let filter = Arc::new(RuntimeFilterBuildExec::try_new(build, 0, 4096).expect("build"))
            as Arc<dyn ExecutionPlan>;

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let part = |values: Vec<i64>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(values))],
            )
            .expect("batch")
        };
        let probe = MemorySourceConfig::try_new(
            &[vec![part(vec![10, 11])], vec![part(vec![20, 21])], vec![part(vec![30, 31])]],
            Arc::clone(&schema),
            None,
        )
        .expect("source");
        let probe = Arc::new(DataSourceExec::new(Arc::new(probe))) as Arc<dyn ExecutionPlan>;
        assert_eq!(
            probe.output_partitioning().partition_count(),
            3,
            "precondition: the probe must actually be multi-partition"
        );

        let node = Arc::new(
            RuntimeFilterProbeExec::try_new(probe, filter, 0).expect("probe node"),
        ) as Arc<dyn ExecutionPlan>;
        let kept: Vec<i64> = collect(node)
            .await
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("i64")
                    .values()
                    .to_vec()
            })
            .collect();
        for present in [10, 20, 30] {
            assert!(
                kept.contains(&present),
                "key {present} lives in a different probe partition from the others; \
                 missing it means that partition did not get the full filter"
            );
        }
    }

    /// `with_new_children` is what `datafusion-proto` and every optimizer rule
    /// use to rebuild a node. A version that dropped the key index or swapped
    /// the two children would filter on the wrong column — silently, and only
    /// after a round trip.
    #[test]
    fn rebuilding_with_new_children_preserves_the_key_and_child_order() {
        let data = source(keys(&[1, 2, 3]));
        let filter = source(keys(&[1]));
        let node = Arc::new(
            RuntimeFilterProbeExec::try_new(Arc::clone(&data), Arc::clone(&filter), 0)
                .expect("probe"),
        );
        let rebuilt = ExecutionPlan::with_new_children(node, vec![data, filter])
            .expect("rebuild");
        let rebuilt = rebuilt
            .downcast_ref::<RuntimeFilterProbeExec>()
            .expect("still a probe node");
        assert_eq!(rebuilt.key_index(), 0);
        assert_eq!(rebuilt.children().len(), 2);

        let build = Arc::new(
            RuntimeFilterBuildExec::try_new(source(keys(&[1])), 0, 8192).expect("build"),
        );
        let rebuilt = ExecutionPlan::with_new_children(build, vec![source(keys(&[1]))])
            .expect("rebuild");
        let rebuilt = rebuilt
            .downcast_ref::<RuntimeFilterBuildExec>()
            .expect("still a build node");
        assert_eq!(
            rebuilt.filter_bytes(),
            8192,
            "the planner-fixed size must survive a rebuild: partials of differing \
             sizes cannot be unioned without losing set bits"
        );
    }

    #[test]
    fn rebuilding_with_the_wrong_number_of_children_is_an_error() {
        let node = Arc::new(
            RuntimeFilterProbeExec::try_new(source(keys(&[1])), source(keys(&[1])), 0)
                .expect("probe"),
        );
        assert!(ExecutionPlan::with_new_children(node, vec![source(keys(&[1]))]).is_err());
    }
}
