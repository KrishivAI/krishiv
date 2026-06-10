#![forbid(unsafe_code)]

//! Arrow-native physical execution operators for Krishiv.

pub use krishiv_plan::lower_to_physical;

// ── Error type ────────────────────────────────────────────────────────────────

/// Errors that can occur during physical execution.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExecError {
    /// An Arrow error occurred.
    #[error("arrow error: {0}")]
    Arrow(String),
    /// A required column was not found in the schema.
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    /// A data type is not supported for this operation.
    #[error("unsupported type: {0}")]
    UnsupportedType(String),
    /// An input batch contains values that violate an operator contract.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// An upstream stream failed before the operator could process its input.
    #[error("upstream stream error: {0}")]
    Upstream(String),
    /// A window operator was constructed with an invalid configuration.
    #[error("invalid window config: {0}")]
    InvalidWindowConfig(String),
    /// Incoming batch schema cannot be evolved to the target schema.
    #[error("incompatible schema evolution: {0}")]
    IncompatibleSchemaEvolution(String),
    /// A CEP pattern matching error occurred.
    #[error("cep error: {0}")]
    Cep(String),
}

impl From<arrow::error::ArrowError> for ExecError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::Arrow(e.to_string())
    }
}

/// Convenience alias for `Result<T, ExecError>`.
pub type ExecResult<T> = Result<T, ExecError>;

// ── JoinType ──────────────────────────────────────────────────────────────────

pub use krishiv_plan::JoinType;

// ── Sub-modules ───────────────────────────────────────────────────────────────

pub mod adaptive;
pub mod aggregate;
pub mod barrier_align;
pub mod cep;
pub mod chunk;
pub mod coalesce_partitions;
pub mod continuous;
pub mod interval_join;
pub mod join;
pub mod live_table;
pub mod memo;
pub mod operator_runtime;
pub mod queue;
pub mod schema_normalize;
pub mod side_output;
pub mod temporal_join;
#[cfg(test)]
mod watermark_e2e;
pub mod watermark_util;
pub mod window;

pub use chunk::ChunkOperator;

pub use adaptive::{
    AdaptiveDecisionKind, AdaptiveDecisionLog, AdaptiveOverrideConfig, HeavyHittersTracker,
    HotKeyReport, RateLimiter, SinkLatencyTracker, StreamingPartitionAdvisor, ThrottleCommand,
};
pub use aggregate::{AggExpr, AggFunction, LocalAggregator};
pub use coalesce_partitions::{CoalescePartitionsOperator, coalesce_partition_batches};
pub use continuous::ContinuousWindowExecutor;
pub use join::{BroadcastJoin, BuiltBroadcastJoin, HashJoin, StreamTableJoin};
pub use operator_runtime::{
    LocalWindowKindBridge, LocalWindowParams, execute_bounded_window, execute_streaming_window,
    local_spec_to_window_execution,
};
pub use queue::{
    OperatorMessage, OperatorQueueError, OperatorQueueMetrics, OperatorQueueReceiver,
    OperatorQueueSender, operator_queue,
};
pub use schema_normalize::{ColumnRenameMap, SchemaNormalizeOperator};
pub use window::{
    MultiSourceWatermarkState, SessionWindowOperator, SessionWindowSpec, SlidingWindowOperator,
    SlidingWindowSpec, StateBackedSessionWindowOperator, StateBackedSlidingWindowOperator,
    StateBackedTumblingWindowOperator, TumblingWindowOperator, TumblingWindowSpec, WatermarkState,
};
// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use krishiv_plan::{ExecutionKind, LogicalPlan, PlanNode};

    use super::lower_to_physical;

    #[test]
    fn lowers_logical_nodes_to_physical_nodes() {
        let logical = LogicalPlan::new("demo", ExecutionKind::Batch).with_node(PlanNode::new(
            "scan",
            "scan parquet",
            ExecutionKind::Batch,
        ));

        let physical = lower_to_physical(&logical).expect("lower");

        assert_eq!(physical.name(), "demo");
        assert_eq!(physical.nodes().len(), 1);
        assert_eq!(physical.nodes()[0].id(), "physical:scan");
    }

    // ── HashJoin tests ────────────────────────────────────────────────────────

    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{
        AggExpr, AggFunction, BroadcastJoin, ExecError, HashJoin, LocalAggregator,
        TumblingWindowOperator, TumblingWindowSpec, WatermarkState,
    };

    fn make_int32_batch(
        key_name: &str,
        keys: Vec<i32>,
        val_name: &str,
        vals: Vec<i32>,
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(key_name, DataType::Int32, false),
            Field::new(val_name, DataType::Int32, false),
        ]));
        let k = Arc::new(Int32Array::from(keys));
        let v = Arc::new(Int32Array::from(vals));
        RecordBatch::try_new(schema, vec![k, v]).unwrap()
    }

    fn make_int32_keyed_batch(key_name: &str, keys: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            key_name,
            DataType::Int32,
            false,
        )]));
        let k = Arc::new(Int32Array::from(keys));
        RecordBatch::try_new(schema, vec![k]).unwrap()
    }

    #[test]
    fn hash_join_inner_produces_correct_rows() {
        // left: id=[1,2,3], val=[10,20,30]
        // right: id=[2,3,4], rval=[200,300,400]
        // inner join on id → rows (2,200) and (3,300)
        let left = make_int32_batch("id", vec![1, 2, 3], "val", vec![10, 20, 30]);
        let right = make_int32_batch("id", vec![2, 3, 4], "rval", vec![200, 300, 400]);

        let join = HashJoin::new("id", "id");
        let result = join.join(&left, &right).unwrap();

        // Should have 2 rows.
        assert_eq!(result.num_rows(), 2);

        // Schema: id (left), val, rval (right key excluded).
        assert_eq!(result.schema().fields().len(), 3);
        assert_eq!(result.schema().field(0).name(), "id");
        assert_eq!(result.schema().field(1).name(), "val");
        assert_eq!(result.schema().field(2).name(), "rval");

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let rvals = result
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Collect (id, val, rval) pairs.
        let mut rows: Vec<(i32, i32, i32)> = (0..result.num_rows())
            .map(|i| (ids.value(i), vals.value(i), rvals.value(i)))
            .collect();
        rows.sort();

        assert_eq!(rows, vec![(2, 20, 200), (3, 30, 300)]);
    }

    #[test]
    fn hash_join_no_match_produces_empty_result() {
        let left = make_int32_batch("id", vec![1, 2], "val", vec![10, 20]);
        let right = make_int32_batch("id", vec![3, 4], "rval", vec![30, 40]);

        let join = HashJoin::new("id", "id");
        let result = join.join(&left, &right).unwrap();

        assert_eq!(result.num_rows(), 0);
        // Schema still correct.
        assert_eq!(result.schema().fields().len(), 3);
    }

    #[test]
    fn hash_join_output_schema_excludes_right_join_key() {
        let left = make_int32_batch("left_id", vec![1], "a", vec![10]);
        let right = make_int32_batch("right_id", vec![1], "b", vec![100]);

        let join = HashJoin::new("left_id", "right_id");
        let result = join.join(&left, &right).unwrap();

        let schema = result.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        // right_id should NOT be in the output.
        assert!(!field_names.contains(&"right_id"));
        assert!(field_names.contains(&"left_id"));
        assert!(field_names.contains(&"a"));
        assert!(field_names.contains(&"b"));
    }

    #[test]
    fn hash_join_unsupported_key_type_returns_error() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Date32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let id_col = Arc::new(arrow::array::Date32Array::from(vec![1i32]));
        let val_col = Arc::new(Int32Array::from(vec![10i32]));
        let left = RecordBatch::try_new(schema.clone(), vec![id_col, val_col]).unwrap();
        // Build a right batch with Date32 key too.
        let right_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Date32, false)]));
        let right_id = Arc::new(arrow::array::Date32Array::from(vec![1i32]));
        let right_date = RecordBatch::try_new(right_schema, vec![right_id]).unwrap();

        let join = HashJoin::new("id", "id");
        let err = join.join(&left, &right_date).unwrap_err();
        assert!(
            matches!(err, ExecError::UnsupportedType(_)),
            "expected UnsupportedType, got {err}"
        );
    }

    // ── BroadcastJoin tests ───────────────────────────────────────────────────

    #[test]
    fn broadcast_join_produces_same_result_as_hash_join() {
        let left = make_int32_batch("id", vec![1, 2, 3], "val", vec![10, 20, 30]);
        let right = make_int32_batch("id", vec![2, 3, 4], "rval", vec![200, 300, 400]);

        let hash_join = HashJoin::new("id", "id");
        let hash_result = hash_join.join(&left, &right).unwrap();

        let broadcast = BroadcastJoin::new("id").build(Arc::new(right)).unwrap();
        let broadcast_result = broadcast.probe(&left).unwrap();

        assert_eq!(hash_result.num_rows(), broadcast_result.num_rows());
        assert_eq!(hash_result.schema(), broadcast_result.schema());
    }

    #[test]
    fn broadcast_join_probe_side_larger() {
        // broadcast (build): 3 rows with id=[1,2,3]
        // probe: 5 rows with id=[1,1,2,3,4]
        // expected matches: rows with id=1 (×2), id=2, id=3 → 4 matches
        let broadcast = make_int32_keyed_batch("id", vec![1, 2, 3]);
        let probe = make_int32_keyed_batch("id", vec![1, 1, 2, 3, 4]);

        let built = BroadcastJoin::new("id").build(Arc::new(broadcast)).unwrap();
        let result = built.probe(&probe).unwrap();

        // id=1 matches twice, id=2 once, id=3 once → 4 rows
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn broadcast_join_empty_probe_returns_empty() {
        let broadcast = make_int32_keyed_batch("id", vec![1, 2, 3]);
        let probe = make_int32_keyed_batch("id", vec![]);

        let built = BroadcastJoin::new("id").build(Arc::new(broadcast)).unwrap();
        let result = built.probe(&probe).unwrap();

        assert_eq!(result.num_rows(), 0);
    }

    // ── LocalAggregator tests ─────────────────────────────────────────────────

    fn make_agg_batch(groups: Vec<&str>, vals: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("grp", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let g = Arc::new(StringArray::from(groups));
        let v = Arc::new(Int64Array::from(vals));
        RecordBatch::try_new(schema, vec![g, v]).unwrap()
    }

    fn make_int32_agg_batch(groups: Vec<i32>, vals: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("grp", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let g = Arc::new(Int32Array::from(groups));
        let v = Arc::new(Int32Array::from(vals));
        RecordBatch::try_new(schema, vec![g, v]).unwrap()
    }

    #[test]
    fn local_agg_count_per_group() {
        // grp: a,a,b,b,b  → count(*): a=2, b=3
        let batch = make_agg_batch(vec!["a", "a", "b", "b", "b"], vec![1, 2, 3, 4, 5]);
        let agg = LocalAggregator::new(
            vec!["grp".into()],
            vec![AggExpr {
                function: AggFunction::Count,
                input_column: "".into(),
                output_column: "cnt".into(),
            }],
        );
        let result = agg.aggregate(&batch).unwrap();
        assert_eq!(result.num_rows(), 2);

        // Sorted by key: a then b.
        let grp = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let cnt = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let rows: Vec<(&str, i64)> = (0..result.num_rows())
            .map(|i| (grp.value(i), cnt.value(i)))
            .collect();

        assert!(rows.contains(&("a", 2)));
        assert!(rows.contains(&("b", 3)));
    }

    #[test]
    fn local_agg_sum_per_group() {
        // grp: a,a,b → sum(val): a=3, b=5
        let batch = make_agg_batch(vec!["a", "a", "b"], vec![1, 2, 5]);
        let agg = LocalAggregator::new(
            vec!["grp".into()],
            vec![AggExpr {
                function: AggFunction::Sum,
                input_column: "val".into(),
                output_column: "total".into(),
            }],
        );
        let result = agg.aggregate(&batch).unwrap();
        assert_eq!(result.num_rows(), 2);

        let grp = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let total = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let rows: Vec<(&str, i64)> = (0..result.num_rows())
            .map(|i| (grp.value(i), total.value(i)))
            .collect();

        assert!(rows.contains(&("a", 3)));
        assert!(rows.contains(&("b", 5)));
    }

    #[test]
    fn local_agg_min_max_int32_per_group() {
        // grp: 1,1,2,2 → min/max
        let batch = make_int32_agg_batch(vec![1, 1, 2, 2], vec![10, 30, 5, 20]);
        let agg = LocalAggregator::new(
            vec!["grp".into()],
            vec![
                AggExpr {
                    function: AggFunction::Min,
                    input_column: "val".into(),
                    output_column: "min_val".into(),
                },
                AggExpr {
                    function: AggFunction::Max,
                    input_column: "val".into(),
                    output_column: "max_val".into(),
                },
            ],
        );
        let result = agg.aggregate(&batch).unwrap();
        assert_eq!(result.num_rows(), 2);

        let grp = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let min_v = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let max_v = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut rows: Vec<(i32, i64, i64)> = (0..result.num_rows())
            .map(|i| (grp.value(i), min_v.value(i), max_v.value(i)))
            .collect();
        rows.sort();

        assert_eq!(rows[0], (1, 10, 30));
        assert_eq!(rows[1], (2, 5, 20));
    }

    #[test]
    fn local_agg_single_group_produces_one_row() {
        let batch = make_agg_batch(vec!["x", "x", "x"], vec![1, 2, 3]);
        let agg = LocalAggregator::new(
            vec!["grp".into()],
            vec![AggExpr {
                function: AggFunction::Count,
                input_column: "".into(),
                output_column: "cnt".into(),
            }],
        );
        let result = agg.aggregate(&batch).unwrap();
        assert_eq!(result.num_rows(), 1);
        let cnt = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(cnt.value(0), 3);
    }

    #[test]
    fn local_agg_empty_group_min_max_avg_semantics() {
        // Verify that AggState finalized values for empty groups use sentinel semantics.
        use crate::aggregate::AggState;
        let exprs = vec![
            AggExpr {
                function: AggFunction::Min,
                input_column: "v".into(),
                output_column: "min_v".into(),
            },
            AggExpr {
                function: AggFunction::Max,
                input_column: "v".into(),
                output_column: "max_v".into(),
            },
            AggExpr {
                function: AggFunction::Avg,
                input_column: "v".into(),
                output_column: "avg_v".into(),
            },
        ];
        let state = AggState::new(&exprs);
        // No updates → empty group.
        assert_eq!(
            state.finalized_value(0, &exprs[0]),
            i64::MAX,
            "Min on empty group should be i64::MAX"
        );
        assert_eq!(
            state.finalized_value(1, &exprs[1]),
            i64::MIN,
            "Max on empty group should be i64::MIN"
        );
        assert!(
            state.finalized_avg(2).is_nan(),
            "Avg on empty group should be NaN"
        );
    }

    #[test]
    fn local_agg_one_row_per_unique_key() {
        let batch = make_agg_batch(vec!["a", "b", "c", "a", "b"], vec![1, 2, 3, 4, 5]);
        let agg = LocalAggregator::new(
            vec!["grp".into()],
            vec![AggExpr {
                function: AggFunction::Sum,
                input_column: "val".into(),
                output_column: "total".into(),
            }],
        );
        let result = agg.aggregate(&batch).unwrap();
        // 3 unique groups: a, b, c
        assert_eq!(result.num_rows(), 3);
    }

    // ── WatermarkState tests ──────────────────────────────────────────────────

    #[test]
    fn watermark_starts_at_min() {
        let wm = WatermarkState::new(0);
        assert_eq!(wm.current_watermark_ms(), i64::MIN);
    }

    #[test]
    fn watermark_advances_monotonically() {
        let mut wm = WatermarkState::new(0);
        wm.advance(1000);
        assert_eq!(wm.current_watermark_ms(), 1000);
        wm.advance(500); // older — must not reduce watermark
        assert_eq!(wm.current_watermark_ms(), 1000);
        wm.advance(2000);
        assert_eq!(wm.current_watermark_ms(), 2000);
    }

    #[test]
    fn watermark_lag_subtracted_correctly() {
        let mut wm = WatermarkState::new(500);
        wm.advance(1000);
        assert_eq!(wm.current_watermark_ms(), 500); // 1000 − 500
    }

    #[test]
    fn watermark_is_late_detects_late_events() {
        let mut wm = WatermarkState::new(0);
        wm.advance(1000);
        assert!(!wm.is_late(1000)); // exact watermark — not late
        assert!(wm.is_late(999)); // below watermark — late
        assert!(!wm.is_late(1001));
    }

    // ── TumblingWindowOperator tests ──────────────────────────────────────────

    fn make_stream_batch(keys: Vec<&str>, timestamps: Vec<i64>, vals: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)) as ArrayRef,
                Arc::new(Int64Array::from(timestamps)) as ArrayRef,
                Arc::new(Int64Array::from(vals)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn count_window_spec() -> TumblingWindowSpec {
        TumblingWindowSpec {
            key_column: "key".into(),
            key_column_type: "utf8".into(),
            event_time_column: "ts".into(),
            window_size_ms: 1000, // 1-second windows
            agg_exprs: vec![AggExpr {
                function: AggFunction::Count,
                input_column: String::new(),
                output_column: "count".into(),
            }],
        }
    }

    #[test]
    fn window_does_not_flush_before_watermark() {
        let mut op = TumblingWindowOperator::new(count_window_spec());
        // Events at t=100 and t=200 both land in window [0, 1000).
        // Watermark = 0 (no lag) → window_end = 1000 > 0, so nothing flushes.
        let batch = make_stream_batch(vec!["a", "a"], vec![100, 200], vec![1, 1]);
        let output = op.process_batch(&batch, 0).unwrap();
        assert!(
            output.is_empty(),
            "window should not flush before watermark reaches window_end"
        );
        assert_eq!(op.open_window_count(), 1);
    }

    #[test]
    fn window_flushes_when_watermark_reaches_window_end() {
        let mut op = TumblingWindowOperator::new(count_window_spec());
        // Feed events into window [0, 1000).
        let batch = make_stream_batch(vec!["a", "b", "a"], vec![100, 200, 300], vec![1, 1, 1]);
        // Watermark = 1000 → window [0,1000) closes.
        let output = op.process_batch(&batch, 1000).unwrap();
        assert_eq!(output.len(), 2, "one batch per unique key: a and b");

        // Collect counts.
        let total_rows: usize = output.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        // Find a's count (should be 2).
        let a_batch = output
            .iter()
            .find(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0)
                    == "a"
            })
            .expect("expected output for key 'a'");
        let count_col = a_batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 2);
    }

    #[test]
    fn late_events_are_dropped() {
        let mut op = TumblingWindowOperator::new(count_window_spec());

        // First batch: establish prev_watermark = 500 by processing an event
        // at ts=500.  After this call prev_watermark_ms = 500.
        let wm_batch = make_stream_batch(vec!["x"], vec![500], vec![0]);
        let _ = op.process_batch(&wm_batch, 500).unwrap();

        // Second batch: ts=100 and ts=200 are late (< prev_watermark=500);
        // ts=600 is valid and lands in window [0, 1000).
        let batch = make_stream_batch(vec!["a", "a", "a"], vec![100, 200, 600], vec![1, 1, 1]);
        // Pass new_watermark=500 (unchanged — no later event in this batch).
        let output = op.process_batch(&batch, 500).unwrap();
        // Window [0,1000) still open (window_end=1000 > 500).
        assert!(output.is_empty());

        // Flush by advancing watermark past window end.
        let final_out = op.flush_closed_windows(1000).unwrap();
        // Two keys: "x" (count=1 from first batch) and "a" (count=1 from ts=600).
        let total: i64 = final_out
            .iter()
            .map(|b| {
                b.column(3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .sum();
        assert_eq!(total, 2); // "x"=1 + "a"=1 (ts=100,200 were late and dropped)
    }

    #[test]
    fn window_sum_aggregation() {
        let spec = TumblingWindowSpec {
            key_column: "key".into(),
            key_column_type: "utf8".into(),
            event_time_column: "ts".into(),
            window_size_ms: 1000,
            agg_exprs: vec![AggExpr {
                function: AggFunction::Sum,
                input_column: "val".into(),
                output_column: "sum_val".into(),
            }],
        };
        let mut op = TumblingWindowOperator::new(spec);
        let batch = make_stream_batch(vec!["x", "x", "x"], vec![0, 100, 200], vec![10, 20, 30]);
        let output = op.process_batch(&batch, 1000).unwrap();
        assert_eq!(output.len(), 1);
        let sum = output[0]
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(sum, 60);
    }

    #[test]
    fn window_avg_aggregation() {
        let spec = TumblingWindowSpec {
            key_column: "key".into(),
            key_column_type: "utf8".into(),
            event_time_column: "ts".into(),
            window_size_ms: 1000,
            agg_exprs: vec![AggExpr {
                function: AggFunction::Avg,
                input_column: "val".into(),
                output_column: "avg_val".into(),
            }],
        };
        let mut op = TumblingWindowOperator::new(spec);
        let batch = make_stream_batch(vec!["x", "x", "x"], vec![0, 100, 200], vec![10, 20, 30]);
        let output = op.process_batch(&batch, 1000).unwrap();
        assert_eq!(output.len(), 1);
        let avg = output[0]
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap()
            .value(0);
        assert!(
            (avg - 20.0).abs() < 1e-9,
            "avg of 10,20,30 should be 20, got {avg}"
        );
        assert_eq!(
            output[0].schema().field(3).data_type(),
            &DataType::Float64,
            "Avg output column must be Float64"
        );
    }

    #[test]
    fn window_output_schema_is_correct() {
        let mut op = TumblingWindowOperator::new(count_window_spec());
        let batch = make_stream_batch(vec!["a"], vec![100], vec![1]);
        let output = op.process_batch(&batch, 1000).unwrap();
        assert_eq!(output.len(), 1);
        let schema = output[0].schema();
        assert_eq!(schema.field(0).name(), "key");
        assert_eq!(schema.field(1).name(), "window_start_ms");
        assert_eq!(schema.field(2).name(), "window_end_ms");
        assert_eq!(schema.field(3).name(), "count");
    }

    #[test]
    fn window_start_end_values_are_correct() {
        let mut op = TumblingWindowOperator::new(count_window_spec());
        // Event at t=100, window_size=1000 → window [0, 1000).
        let batch = make_stream_batch(vec!["a"], vec![100], vec![1]);
        let output = op.process_batch(&batch, 1000).unwrap();
        let win_start = output[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let win_end = output[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(win_start, 0);
        assert_eq!(win_end, 1000);
    }

    #[test]
    fn deterministic_replay_produces_identical_output() {
        // Slice G — same input must produce identical output on two runs.
        let run = |spec: TumblingWindowSpec, batch: &RecordBatch| -> Vec<RecordBatch> {
            let mut op = TumblingWindowOperator::new(spec);
            let mut out = op.process_batch(batch, 1000).unwrap();
            out.extend(op.flush_closed_windows(i64::MAX).unwrap());
            out
        };

        let batch = make_stream_batch(
            vec!["a", "b", "a", "b", "a"],
            vec![100, 150, 200, 250, 300],
            vec![1, 2, 3, 4, 5],
        );

        let run1 = run(count_window_spec(), &batch);
        let run2 = run(count_window_spec(), &batch);

        assert_eq!(
            run1.len(),
            run2.len(),
            "run1 and run2 must produce the same number of output batches"
        );
        for (b1, b2) in run1.iter().zip(run2.iter()) {
            assert_eq!(b1.schema(), b2.schema());
            assert_eq!(b1.num_rows(), b2.num_rows());
            // Compare column by column.
            for col_idx in 0..b1.num_columns() {
                let c1 = b1.column(col_idx);
                let c2 = b2.column(col_idx);
                assert_eq!(c1.data_type(), c2.data_type());
                // Compare as debug strings — sufficient for Int64/Utf8.
                assert_eq!(
                    format!("{c1:?}"),
                    format!("{c2:?}"),
                    "column {col_idx} differs between run1 and run2"
                );
            }
        }
    }

    // ── MultiSourceWatermarkState tests ───────────────────────────────────────

    use super::{
        MultiSourceWatermarkState, SessionWindowOperator, SessionWindowSpec, SlidingWindowOperator,
        SlidingWindowSpec, StreamTableJoin,
    };

    #[test]
    fn multi_source_watermark_effective_is_min() {
        let mut state = MultiSourceWatermarkState::new();
        state.update("src-a", 5000);
        state.update("src-b", 3000);
        assert_eq!(state.effective_watermark_ms(), 3000);
        state.update("src-b", 7000);
        assert_eq!(state.effective_watermark_ms(), 5000);
    }

    #[test]
    fn multi_source_watermark_empty_returns_min() {
        let state = MultiSourceWatermarkState::new();
        assert_eq!(state.effective_watermark_ms(), i64::MIN);
    }

    #[test]
    fn multi_source_watermark_ignores_decrease() {
        let mut state = MultiSourceWatermarkState::new();
        state.update("src", 1000);
        state.update("src", 500); // decrease — must be ignored
        assert_eq!(state.effective_watermark_ms(), 1000);
    }

    // ── SlidingWindowOperator tests ───────────────────────────────────────────

    fn sliding_spec() -> SlidingWindowSpec {
        SlidingWindowSpec {
            key_column: "key".into(),
            key_column_type: "utf8".into(),
            event_time_column: "ts".into(),
            window_size_ms: 1000,
            slide_ms: 500,
            agg_exprs: vec![AggExpr {
                function: AggFunction::Count,
                input_column: "val".into(),
                output_column: "cnt".into(),
            }],
        }
    }

    fn make_stream_batch_i64(keys: Vec<&str>, times: Vec<i64>, vals: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int64Array::from(times)),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn sliding_window_event_belongs_to_two_windows() {
        // window_size=1000, slide=500: event at t=600 belongs to [0,1000) and [500,1500).
        let mut op = SlidingWindowOperator::new(sliding_spec()).unwrap();
        let batch = make_stream_batch_i64(vec!["a"], vec![600], vec![1]);
        // watermark high enough to close both windows
        let out = op.process_batch(&batch, 2000).unwrap();
        // Two windows should close: [0,1000) and [500,1500)
        assert_eq!(
            out.len(),
            2,
            "event at t=600 must appear in two sliding windows"
        );
    }

    #[test]
    fn sliding_window_late_events_dropped() {
        // size=1000, slide=500: event at t=1500 belongs to [1000,2000) and [1500,2500).
        let mut op = SlidingWindowOperator::new(sliding_spec()).unwrap();
        let b1 = make_stream_batch_i64(vec!["a"], vec![1500], vec![1]);
        op.process_batch(&b1, 1500).unwrap();

        // Attempt to add a late event (t=100 < prev_watermark=1500) — must be dropped.
        let b2 = make_stream_batch_i64(vec!["a"], vec![100], vec![1]);
        op.process_batch(&b2, 1500).unwrap();

        // Advance watermark past both window ends (>2500) to force closure.
        let out = op
            .process_batch(&make_stream_batch_i64(vec![], vec![], vec![]), 3000)
            .unwrap();
        // Each of the two windows should have count=1 (only the t=1500 event).
        assert_eq!(
            out.len(),
            2,
            "both windows [1000,2000) and [1500,2500) must close"
        );
        let total_counts: i64 = out
            .iter()
            .map(|b| {
                b.column_by_name("cnt")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .sum();
        assert_eq!(
            total_counts, 2,
            "each window has count=1 from the t=1500 event only"
        );
    }

    #[test]
    fn sliding_window_avg_aggregation() {
        let spec = SlidingWindowSpec {
            key_column: "key".into(),
            key_column_type: "utf8".into(),
            event_time_column: "ts".into(),
            window_size_ms: 1000,
            slide_ms: 500,
            agg_exprs: vec![AggExpr {
                function: AggFunction::Avg,
                input_column: "val".into(),
                output_column: "avg_val".into(),
            }],
        };
        let mut op = SlidingWindowOperator::new(spec).unwrap();
        let batch = make_stream_batch_i64(vec!["a", "a"], vec![100, 200], vec![10, 30]);
        let out = op.process_batch(&batch, 2000).unwrap();
        assert!(!out.is_empty(), "windows should close");
        for b in &out {
            assert_eq!(
                b.schema().field(3).data_type(),
                &DataType::Float64,
                "Avg output column must be Float64"
            );
        }
    }

    // ── SessionWindowOperator tests ───────────────────────────────────────────

    fn session_spec() -> SessionWindowSpec {
        SessionWindowSpec {
            key_column: "key".into(),
            key_column_type: "utf8".into(),
            event_time_column: "ts".into(),
            session_gap_ms: 500,
            agg_exprs: vec![AggExpr {
                function: AggFunction::Count,
                input_column: "val".into(),
                output_column: "cnt".into(),
            }],
        }
    }

    #[test]
    fn session_window_closes_after_gap() {
        let mut op = SessionWindowOperator::new(session_spec());
        // Events at t=100, 200 for key "a" — session gap = 500
        let b1 = make_stream_batch_i64(vec!["a", "a"], vec![100, 200], vec![1, 1]);
        let out1 = op.process_batch(&b1, 600).unwrap();
        // watermark=600 >= last_event(200)+gap(500)=700 — NOT yet closed
        assert!(out1.is_empty(), "session should not close at watermark=600");

        let out2 = op
            .process_batch(&make_stream_batch_i64(vec![], vec![], vec![]), 800)
            .unwrap();
        // watermark=800 >= 200+500=700 — session must close
        assert_eq!(
            out2.len(),
            1,
            "session must close when watermark passes last_event+gap"
        );
        let cnt = out2[0]
            .column_by_name("cnt")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt, 2);
    }

    #[test]
    fn session_window_separate_keys_independent() {
        let mut op = SessionWindowOperator::new(session_spec());
        let batch = make_stream_batch_i64(vec!["a", "b"], vec![100, 200], vec![1, 1]);
        let out = op.process_batch(&batch, 1000).unwrap();
        // Both sessions close: "a" at 100+500=600 ≤ 1000, "b" at 200+500=700 ≤ 1000
        assert_eq!(out.len(), 2, "each key's session must close independently");
    }

    #[test]
    fn session_window_avg_aggregation() {
        let spec = SessionWindowSpec {
            key_column: "key".into(),
            key_column_type: "utf8".into(),
            event_time_column: "ts".into(),
            session_gap_ms: 500,
            agg_exprs: vec![AggExpr {
                function: AggFunction::Avg,
                input_column: "val".into(),
                output_column: "avg_val".into(),
            }],
        };
        let mut op = SessionWindowOperator::new(spec);
        let b1 = make_stream_batch_i64(vec!["a", "a"], vec![100, 200], vec![10, 30]);
        let out = op.process_batch(&b1, 1000).unwrap();
        assert!(!out.is_empty(), "session should close");
        for b in &out {
            assert_eq!(
                b.schema().field(3).data_type(),
                &DataType::Float64,
                "Avg output column must be Float64"
            );
            let avg = b
                .column(3)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap()
                .value(0);
            assert!(
                (avg - 20.0).abs() < 1e-9,
                "avg of 10,30 should be 20, got {avg}"
            );
        }
    }

    // ── StreamTableJoin tests ─────────────────────────────────────────────────

    fn make_table() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn stream_table_join_inner_join() {
        let mut join = StreamTableJoin::new(make_table(), "key");
        let stream = make_stream_batch_i64(vec!["a", "b", "z"], vec![1, 2, 3], vec![10, 20, 30]);
        let result = join.process_batch(&stream).unwrap();
        // "z" has no match — only 2 output rows
        assert_eq!(result.num_rows(), 2);
        let labels = result
            .column_by_name("label")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut label_vals: Vec<&str> = (0..result.num_rows()).map(|i| labels.value(i)).collect();
        label_vals.sort();
        assert_eq!(label_vals, vec!["alpha", "beta"]);
    }

    #[test]
    fn stream_table_join_no_matches_returns_empty() {
        let mut join = StreamTableJoin::new(make_table(), "key");
        let stream = make_stream_batch_i64(vec!["x", "y"], vec![1, 2], vec![10, 20]);
        let result = join.process_batch(&stream).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    // ── R7.2 OperatorQueue tests ─────────────────────────────────────────────

    use super::{
        AdaptiveDecisionKind, AdaptiveDecisionLog, AdaptiveOverrideConfig, HeavyHittersTracker,
        OperatorMessage, RateLimiter, SinkLatencyTracker, ThrottleCommand, operator_queue,
    };

    #[tokio::test]
    async fn operator_queue_data_flows_through() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>],
        )
        .unwrap();

        let (tx, mut rx) = operator_queue(8);
        tx.send_data(batch.clone()).await.unwrap();
        let msg = rx.recv().await.unwrap();
        assert!(matches!(msg, OperatorMessage::Data(_)));
    }

    #[tokio::test]
    async fn operator_queue_barrier_arrives_before_queued_data() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![42])) as Arc<dyn arrow::array::Array>],
        )
        .unwrap();

        let (tx, mut rx) = operator_queue(8);
        // Send one data item.
        tx.send_data(batch.clone()).await.unwrap();
        // Then inject a barrier (unbounded, bypass backpressure).
        tx.send_barrier(7).await.unwrap();

        // First receive must be the barrier (barrier_rx is drained first).
        let first = rx.recv().await.unwrap();
        assert!(
            matches!(first, OperatorMessage::Barrier { epoch: 7 }),
            "barrier must arrive before queued data"
        );

        // Second receive gives the data.
        let second = rx.recv().await.unwrap();
        assert!(matches!(second, OperatorMessage::Data(_)));
    }

    #[tokio::test]
    async fn operator_queue_metrics_reflect_capacity() {
        let (tx, rx) = operator_queue(4);
        let metrics = rx.metrics();
        assert_eq!(metrics.capacity, 4);
        assert_eq!(metrics.len, 0);
        assert!(!metrics.is_full());
        drop(tx);
    }

    // ── P0.5: pending_barrier test ────────────────────────────────────────────

    /// Verify that a barrier injected when the data channel is empty is
    /// delivered on the very next `recv()` call (not lost).
    #[tokio::test]
    async fn operator_queue_barrier_at_empty_queue_delivered_next_recv() {
        let (tx, mut rx) = operator_queue(8);

        // Inject a barrier while the data channel is empty.
        tx.send_barrier(42).await.unwrap();

        // First recv must be the barrier.
        let first = rx.recv().await.unwrap();
        assert!(
            matches!(first, OperatorMessage::Barrier { epoch: 42 }),
            "barrier injected at empty queue must be delivered immediately: got {first:?}"
        );

        // Now send data and a barrier together to exercise the pending_barrier path.
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1])) as Arc<dyn arrow::array::Array>],
        )
        .unwrap();
        tx.send_data(batch).await.unwrap();
        tx.send_barrier(99).await.unwrap();

        // The barrier channel is drained before data, so we get the barrier first.
        let second = rx.recv().await.unwrap();
        assert!(
            matches!(second, OperatorMessage::Barrier { epoch: 99 }),
            "barrier must arrive before data when both are queued: got {second:?}"
        );

        // Then the data item.
        let third = rx.recv().await.unwrap();
        assert!(
            matches!(third, OperatorMessage::Data(_)),
            "data must follow the barrier: got {third:?}"
        );
    }

    // ── P0.10: Wrong schema returns error test ────────────────────────────────

    /// Feed a batch whose event-time column is Float64 (not Int64) to
    /// `TumblingWindowOperator::process_batch` and verify an error is returned
    /// (not a panic).
    #[test]
    fn tumbling_window_wrong_schema_returns_error_not_panic() {
        use arrow::array::Float64Array;

        let bad_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Float64, false), // wrong: should be Int64
            Field::new("val", DataType::Int64, false),
        ]));
        let bad_batch = RecordBatch::try_new(
            bad_schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.0_f64])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
            ],
        )
        .unwrap();

        let mut op = TumblingWindowOperator::new(count_window_spec());
        let result = op.process_batch(&bad_batch, 1000);
        assert!(
            result.is_err(),
            "wrong column type must return Err, not panic"
        );
    }

    // ── P0.18: SlidingWindowOperator slide_ms == 0 guard test ─────────────────

    #[test]
    fn sliding_window_zero_slide_returns_error() {
        let bad_spec = SlidingWindowSpec {
            key_column: "key".into(),
            key_column_type: "utf8".into(),
            event_time_column: "ts".into(),
            window_size_ms: 1000,
            slide_ms: 0, // invalid — would cause infinite loop
            agg_exprs: vec![],
        };
        let result = SlidingWindowOperator::new(bad_spec);
        assert!(
            matches!(result, Err(ExecError::InvalidWindowConfig(_))),
            "slide_ms == 0 must return Err(InvalidWindowConfig), got {result:?}"
        );
    }

    // ── R7.2 HeavyHittersTracker tests ──────────────────────────────────────

    #[test]
    fn heavy_hitters_tracks_single_key() {
        let mut tracker = HeavyHittersTracker::new(10);
        tracker.observe("a");
        tracker.observe("a");
        tracker.observe("a");
        let top = tracker.top_k();
        assert_eq!(top[0].key, "a");
        assert_eq!(top[0].estimated_count, 3);
        assert_eq!(top[0].max_error, 0);
    }

    #[test]
    fn heavy_hitters_eviction_replaces_min_count() {
        // Capacity=2 — once full, the 3rd unique key evicts the lowest-count entry.
        let mut tracker = HeavyHittersTracker::new(2);
        tracker.observe("a"); // counters: [("a",1,0)]
        tracker.observe("a"); // counters: [("a",2,0)]
        tracker.observe("b"); // counters: [("a",2,0), ("b",1,0)]
        tracker.observe("c"); // full, min="b"(1) → evict, ("c",2,1)
        let top = tracker.top_k();
        // Both entries should have estimated_count >= 2.
        for entry in &top {
            assert!(
                entry.estimated_count >= 2,
                "entry count must be >= eviction threshold"
            );
        }
        // "b" should no longer be tracked.
        assert!(
            !top.iter().any(|e| e.key == "b"),
            "b must have been evicted"
        );
        assert_eq!(tracker.total(), 4);
    }

    #[test]
    fn heavy_hitters_heat_score_calculation() {
        let mut tracker = HeavyHittersTracker::new(5);
        for _ in 0..8 {
            tracker.observe("hot");
        }
        for _ in 0..2 {
            tracker.observe("cold");
        }
        let top = tracker.top_k();
        let hot = top.iter().find(|r| r.key == "hot").unwrap();
        assert!((hot.heat_score - 0.8).abs() < 1e-9);
    }

    #[test]
    fn heavy_hitters_hot_keys_filter_works() {
        let mut tracker = HeavyHittersTracker::new(5);
        for _ in 0..10 {
            tracker.observe("dominant");
        }
        tracker.observe("minor");
        let hot = tracker.hot_keys(0.5); // threshold 50%
        assert_eq!(hot.len(), 1);
        assert_eq!(hot[0].key, "dominant");
    }

    #[test]
    fn heavy_hitters_reset_clears_state() {
        let mut tracker = HeavyHittersTracker::new(5);
        tracker.observe("x");
        tracker.reset();
        assert_eq!(tracker.total(), 0);
        assert!(tracker.top_k().is_empty());
    }

    // ── R7.2 RateLimiter tests ───────────────────────────────────────────────

    #[test]
    fn rate_limiter_initially_full_allows_consume() {
        let mut rl = RateLimiter::new(1000);
        // Should succeed immediately (bucket starts full).
        let wait = rl.try_consume(500, 0);
        assert!(wait.is_none(), "initial consume must succeed immediately");
    }

    #[test]
    fn rate_limiter_depleted_returns_wait_time() {
        let mut rl = RateLimiter::new(1000);
        // Drain the bucket completely.
        let _ = rl.try_consume(1000, 0);
        // Now try to consume 500 more — bucket empty, should wait.
        let wait = rl.try_consume(500, 0);
        assert!(wait.is_some(), "empty bucket must return a wait time");
        assert!(wait.unwrap() >= 1, "wait time must be at least 1ms");
    }

    #[test]
    fn rate_limiter_refills_over_time() {
        let mut rl = RateLimiter::new(1000); // 1000 tokens/sec
        let _ = rl.try_consume(1000, 0); // drain
        // 500ms later → 500 new tokens added.
        let wait = rl.try_consume(400, 500);
        assert!(
            wait.is_none(),
            "500ms refill must cover a 400-token request"
        );
    }

    #[test]
    fn rate_limiter_set_rate_clamps_tokens() {
        let mut rl = RateLimiter::new(2000);
        rl.set_rate(100);
        assert_eq!(rl.rate(), 100);
        // Tokens should be clamped to new rate.
        let wait = rl.try_consume(101, 0);
        assert!(wait.is_some(), "tokens clamped to 100, cannot consume 101");
    }

    // ── P1.28: RateLimiter first-call must not over-refill ───────────────────

    #[test]
    fn rate_limiter_does_not_burst_past_ceiling_on_first_call() {
        // P1.28: After the initial full bucket is drained, the first call with a
        // large elapsed time must not grant MORE than `capacity` tokens in total
        // (the min-cap in try_consume ensures this).
        let mut rl = RateLimiter::new(500);
        // Drain the bucket fully at t=0.
        let wait = rl.try_consume(500, 0);
        assert!(wait.is_none(), "initial full-capacity consume must succeed");

        // At t=10_000ms (10s later) try to consume the whole capacity again.
        // Due to 500 tokens/sec × 10s = 5000 tokens would be added, but the
        // bucket is capped at capacity (500).  The result must be <= capacity.
        let wait = rl.try_consume(501, 10_000);
        assert!(
            wait.is_some(),
            "consuming 501 when bucket is capped at 500 must be blocked"
        );

        // Consuming exactly capacity after a long refill window must succeed.
        let _ = rl.try_consume(1, 10_001); // consume the previous blocking amount's wait
        let wait2 = rl.try_consume(500, 11_000);
        assert!(
            wait2.is_none(),
            "consuming exactly capacity after 1s refill must succeed immediately"
        );
    }

    #[test]
    fn rate_limiter_no_double_refill_across_window() {
        // P1.28: Verify tokens never exceed capacity even after a very long idle period.
        let mut rl = RateLimiter::new(100);
        // Drain fully at t=0.
        rl.try_consume(100, 0);
        // 1 000 000 ms later — would add 100_000 tokens without the cap.
        // With the cap, tokens must be clamped to 100.
        let wait = rl.try_consume(101, 1_000_000);
        assert!(
            wait.is_some(),
            "tokens must be capped at capacity regardless of idle duration"
        );
        // But consuming exactly capacity must succeed.
        let wait = rl.try_consume(100, 1_000_000);
        assert!(
            wait.is_none(),
            "capacity-sized consume after long idle must succeed"
        );
    }

    // ── R7.2 SinkLatencyTracker tests ───────────────────────────────────────

    #[test]
    fn sink_latency_tracker_avg_zero_when_empty() {
        let tracker = SinkLatencyTracker::default();
        assert_eq!(tracker.avg_latency_ms(), 0.0);
        assert!(!tracker.is_slow(100));
    }

    #[test]
    fn sink_latency_tracker_records_avg_and_max() {
        let mut tracker = SinkLatencyTracker::default();
        tracker.record_write(10);
        tracker.record_write(30);
        assert_eq!(tracker.write_count(), 2);
        assert_eq!(tracker.avg_latency_ms(), 20.0);
        assert_eq!(tracker.max_latency_ms(), 30);
    }

    #[test]
    fn sink_latency_tracker_is_slow_detection() {
        let mut tracker = SinkLatencyTracker::default();
        tracker.record_write(200);
        tracker.record_write(400);
        // avg = 300 > threshold 100 → slow
        assert!(tracker.is_slow(100));
        // avg = 300 < threshold 500 → not slow
        assert!(!tracker.is_slow(500));
    }

    // ── R7.2 AdaptiveDecisionLog / AdaptiveOverrideConfig tests ─────────────

    #[test]
    fn adaptive_decision_kind_display() {
        assert_eq!(
            AdaptiveDecisionKind::HotKeySplit.to_string(),
            "hot-key-split"
        );
        assert_eq!(AdaptiveDecisionKind::Repartition.to_string(), "repartition");
        assert_eq!(
            AdaptiveDecisionKind::SourceThrottle.to_string(),
            "source-throttle"
        );
        assert_eq!(
            AdaptiveDecisionKind::SlowSinkDetected.to_string(),
            "slow-sink"
        );
    }

    #[test]
    fn adaptive_decision_log_fields_accessible() {
        let log = AdaptiveDecisionLog {
            timestamp_ms: 12345,
            kind: AdaptiveDecisionKind::Repartition,
            affected_job_id: "job-42".into(),
            details: "partition count increased from 4 to 8".into(),
            applied: true,
        };
        assert_eq!(log.timestamp_ms, 12345);
        assert!(log.applied);
        assert_eq!(log.affected_job_id, "job-42");
    }

    #[test]
    fn adaptive_override_config_defaults_all_false() {
        let cfg = AdaptiveOverrideConfig::default();
        assert!(!cfg.disable_hot_key_splitting);
        assert!(!cfg.disable_adaptive_repartition);
        assert!(!cfg.disable_source_throttling);
    }

    #[test]
    fn throttle_command_fields() {
        let cmd = ThrottleCommand {
            source_id: "src-1".into(),
            rows_per_second: Some(5000),
        };
        assert_eq!(cmd.source_id, "src-1");
        assert_eq!(cmd.rows_per_second, Some(5000));

        let clear = ThrottleCommand {
            source_id: "src-1".into(),
            rows_per_second: None,
        };
        assert!(clear.rows_per_second.is_none());
    }

    // ── PerKeyIntervalJoin tests ─────────────────────────────────────────────

    use super::interval_join::{IntervalJoinSpec, PerKeyIntervalJoin};

    fn make_interval_batch(col_name: &str, values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            col_name,
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    #[test]
    fn interval_join_basic_match() {
        let mut join = PerKeyIntervalJoin::new(IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
            key_column: "k".into(),
            max_buffer_per_side: 1000,
        });
        // left event at t=1000
        let left = make_interval_batch("lv", vec![1]);
        join.push_left("k", 1000, left.clone());

        // right event at t=1050 → delta=50, within [-100,100]
        let right = make_interval_batch("rv", vec![2]);
        let matches = join.push_right("k", 1050, right);
        assert_eq!(matches.len(), 1);
        // left batch in match should equal the original left
        assert_eq!(matches[0].0.schema(), left.schema());
        assert_eq!(matches[0].0.num_rows(), 1);
    }

    #[test]
    fn interval_join_empty_right_produces_no_match() {
        let mut join = PerKeyIntervalJoin::new(IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
            key_column: "k".into(),
            max_buffer_per_side: 1000,
        });
        // push left with no right events buffered
        let left = make_interval_batch("lv", vec![1]);
        let matches = join.push_left("k", 1000, left);
        assert!(matches.is_empty());
    }

    #[test]
    fn interval_join_empty_left_produces_no_match() {
        let mut join = PerKeyIntervalJoin::new(IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
            key_column: "k".into(),
            max_buffer_per_side: 1000,
        });
        let right = make_interval_batch("rv", vec![1]);
        let matches = join.push_right("k", 1000, right);
        assert!(matches.is_empty());
    }

    #[test]
    fn interval_join_schema_mismatch_still_joins() {
        // Left and right have different schemas — interval join is on event time,
        // not column names, so both RecordBatches pass through untouched.
        let mut join = PerKeyIntervalJoin::new(IntervalJoinSpec {
            lower_bound_ms: -50,
            upper_bound_ms: 50,
            key_column: "k".into(),
            max_buffer_per_side: 1000,
        });

        let left_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let left = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![10])),
                Arc::new(StringArray::from(vec!["hello"])),
            ],
        )
        .unwrap();
        join.push_left("k", 1000, left.clone());

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]));
        let right = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1000_i64])),
                Arc::new(arrow::array::Float64Array::from(vec![3.15])),
            ],
        )
        .unwrap();
        let matches = join.push_right("k", 1020, right);

        assert_eq!(matches.len(), 1);
        // Verify schemas are preserved from each side
        assert_eq!(matches[0].0.schema(), left_schema);
        assert_eq!(matches[0].1.schema(), right_schema);
    }

    // ── SchemaNormalizeOperator tests (in lib.rs) ───────────────────────────

    use super::schema_normalize::SchemaNormalizeOperator;

    #[test]
    fn schema_normalize_add_column() {
        let src = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(src, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();

        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let out = SchemaNormalizeOperator::new(target)
            .normalize(&batch)
            .unwrap();

        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.schema().field(1).name(), "name");
        // New column should be all nulls
        assert_eq!(out.column(1).null_count(), 3);
    }

    #[test]
    fn schema_normalize_remove_column() {
        let src = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            src,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(Int32Array::from(vec![3])),
            ],
        )
        .unwrap();

        // Target keeps only "a" and "c", drops "b"
        let target = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));
        let out = SchemaNormalizeOperator::new(target)
            .normalize(&batch)
            .unwrap();

        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.schema().field(0).name(), "a");
        assert_eq!(out.schema().field(1).name(), "c");
        let a_col = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a_col.value(0), 1);
        let c_col = out.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(c_col.value(0), 3);
    }

    #[test]
    fn schema_normalize_reorder_columns() {
        let src = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            src,
            vec![
                Arc::new(Int32Array::from(vec![10])),
                Arc::new(Int32Array::from(vec![20])),
            ],
        )
        .unwrap();

        // Target reverses the column order: y before x
        let target = Arc::new(Schema::new(vec![
            Field::new("y", DataType::Int32, false),
            Field::new("x", DataType::Int32, false),
        ]));
        let out = SchemaNormalizeOperator::new(target)
            .normalize(&batch)
            .unwrap();

        assert_eq!(out.schema().field(0).name(), "y");
        assert_eq!(out.schema().field(1).name(), "x");
        let y_col = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(y_col.value(0), 20);
        let x_col = out.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(x_col.value(0), 10);
    }
}
