#[cfg(test)]
mod gap_tests {
    use crate::delta_batch::DeltaBatch;
    use crate::error::DeltaError;
    use crate::lateness::SourceOrdinal;
    use crate::trace::Trace;
    use crate::view::{IncrementalView, IncrementalViewRegistry, IncrementalViewSpec};
    use arrow::array::{Int32Array, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use std::sync::Arc;

    fn id_batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
    }

    fn id_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn ts_batch(ids: &[i32], timestamps: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(Int64Array::from(timestamps.to_vec())),
            ],
        )
        .unwrap()
    }

    fn ts_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("ts", DataType::Int64, false),
        ]))
    }

    fn x_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]))
    }

    fn view_spec(name: &str, materialized: bool) -> IncrementalViewSpec {
        IncrementalViewSpec {
            name: name.to_string(),
            body_sql: format!("SELECT 1 AS x -- {name}"),
            output_schema: x_schema(),
            is_materialized: materialized,
            is_recursive: false,
            lateness: vec![],
        }
    }

    // ── Trace: with_lateness_column ───────────────────────────────────────────

    #[test]
    fn trace_with_lateness_column_sets_idx() {
        let trace = Trace::new(ts_schema(), &["id"])
            .unwrap()
            .with_lateness_column("ts")
            .unwrap();
        assert_eq!(
            trace.data_schema().field_with_name("ts").unwrap().name(),
            "ts"
        );
    }

    #[test]
    fn trace_with_lateness_column_invalid_col_errors() {
        let result = Trace::new(ts_schema(), &["id"])
            .unwrap()
            .with_lateness_column("missing");
        assert!(matches!(result, Err(DeltaError::ColumnNotFound(_))));
    }

    // ── Trace: key_column_names ───────────────────────────────────────────────

    #[test]
    fn trace_key_column_names_single() {
        let trace = Trace::new(id_schema(), &["id"]).unwrap();
        assert_eq!(trace.key_column_names(), &["id".to_string()]);
    }

    #[test]
    fn trace_key_column_names_multiple() {
        let trace = Trace::new(ts_schema(), &["id", "ts"]).unwrap();
        assert_eq!(
            trace.key_column_names(),
            &["id".to_string(), "ts".to_string()]
        );
    }

    // ── Trace: gc_below_watermark ─────────────────────────────────────────────

    #[test]
    fn trace_gc_below_watermark_removes_old_entries() {
        let mut trace = Trace::new(ts_schema(), &["id"])
            .unwrap()
            .with_lateness_column("ts")
            .unwrap();
        trace.insert(DeltaBatch::from_inserts(ts_batch(&[1, 2, 3], &[100, 200, 300])).unwrap());
        // AUD-2: watermark 250 expires ts=100 and ts=200 (both < 250) and
        // keeps ts=300 (>= 250). The mask is now a correct keep-mask.
        let removed = trace.gc_below_watermark(250).unwrap();
        assert_eq!(removed, 2);
        let snap = trace.snapshot().unwrap();
        assert_eq!(snap.num_rows(), 1);
    }

    #[test]
    fn trace_gc_below_watermark_noop_without_lateness_col() {
        let mut trace = Trace::new(ts_schema(), &["id"]).unwrap();
        trace.insert(DeltaBatch::from_inserts(ts_batch(&[1, 2], &[100, 200])).unwrap());
        let removed = trace.gc_below_watermark(150).unwrap();
        assert_eq!(removed, 0);
    }

    #[test]
    fn trace_gc_below_watermark_keeps_all_when_watermark_low() {
        let mut trace = Trace::new(ts_schema(), &["id"])
            .unwrap()
            .with_lateness_column("ts")
            .unwrap();
        trace.insert(DeltaBatch::from_inserts(ts_batch(&[1, 2], &[100, 200])).unwrap());
        // AUD-2: watermark 50 is below every value, so nothing is expired.
        let removed = trace.gc_below_watermark(50).unwrap();
        assert_eq!(removed, 0);
        let snap = trace.snapshot().unwrap();
        assert_eq!(snap.num_rows(), 2);
    }

    // ── Trace: snapshot ───────────────────────────────────────────────────────

    #[test]
    fn trace_snapshot_empty_returns_empty() {
        let trace = Trace::new(id_schema(), &["id"]).unwrap();
        let snap = trace.snapshot().unwrap();
        assert_eq!(snap.num_rows(), 0);
    }

    #[test]
    fn trace_snapshot_returns_positive_weight_rows() {
        let mut trace = Trace::new(id_schema(), &["id"]).unwrap();
        trace.insert(DeltaBatch::from_inserts(id_batch(&[1, 2, 3])).unwrap());
        let snap = trace.snapshot().unwrap();
        assert_eq!(snap.num_rows(), 3);
    }

    #[test]
    fn trace_snapshot_excludes_cancelled_rows() {
        let mut trace = Trace::new(id_schema(), &["id"]).unwrap();
        trace.insert(DeltaBatch::from_inserts(id_batch(&[1, 2])).unwrap());
        trace.insert(DeltaBatch::from_deletes(id_batch(&[2])).unwrap());
        trace.consolidate().unwrap();
        let snap = trace.snapshot().unwrap();
        assert_eq!(snap.num_rows(), 1);
        let ids: Vec<i32> = (0..snap.num_rows())
            .map(|i| {
                snap.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(i)
            })
            .collect();
        assert_eq!(ids, vec![1]);
    }

    #[test]
    fn trace_snapshot_across_levels() {
        let mut trace = Trace::new(id_schema(), &["id"]).unwrap();
        for i in 0..5 {
            trace.insert(DeltaBatch::from_inserts(id_batch(&[i])).unwrap());
        }
        let snap = trace.snapshot().unwrap();
        assert_eq!(snap.num_rows(), 5);
    }

    // ── IncrementalView: last_output ──────────────────────────────────────────

    #[test]
    fn view_last_output_none_before_publish() {
        let (view, _rx) = IncrementalView::new(view_spec("v", false));
        let out = view.last_output().unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn view_last_output_returns_published() {
        let (view, _rx) = IncrementalView::new(view_spec("v", false));
        let delta = DeltaBatch::from_inserts(id_batch(&[1, 2])).unwrap();
        view.publish_output(delta).unwrap();
        let out = view.last_output().unwrap();
        assert!(out.is_some());
        assert_eq!(out.unwrap().num_rows(), 2);
    }

    // ── IncrementalView: snapshot ─────────────────────────────────────────────

    #[test]
    fn view_snapshot_none_before_publish() {
        let (view, _rx) = IncrementalView::new(view_spec("v", true));
        let snap = view.snapshot().unwrap();
        assert!(snap.is_none());
    }

    #[test]
    fn view_snapshot_accumulates_for_materialized() {
        let (view, _rx) = IncrementalView::new(view_spec("v", true));
        let delta1 = DeltaBatch::from_inserts(id_batch(&[1, 2])).unwrap();
        view.publish_output(delta1).unwrap();
        let snap = view.snapshot().unwrap().unwrap();
        assert_eq!(snap.num_rows(), 2);
        let delta2 = DeltaBatch::from_inserts(id_batch(&[3])).unwrap();
        view.publish_output(delta2).unwrap();
        let snap = view.snapshot().unwrap().unwrap();
        assert_eq!(snap.num_rows(), 3);
    }

    #[test]
    fn view_snapshot_not_updated_for_non_materialized() {
        let (view, _rx) = IncrementalView::new(view_spec("v", false));
        let delta = DeltaBatch::from_inserts(id_batch(&[1])).unwrap();
        view.publish_output(delta).unwrap();
        let snap = view.snapshot().unwrap();
        assert!(snap.is_none());
    }

    // ── IncrementalView: subscribe ────────────────────────────────────────────

    #[test]
    fn view_subscribe_receives_published() {
        let (view, _rx) = IncrementalView::new(view_spec("v", false));
        let mut rx = view.subscribe();
        let delta = DeltaBatch::from_inserts(id_batch(&[42])).unwrap();
        view.publish_output(delta).unwrap();
        let received = rx.borrow_and_update().clone();
        assert!(received.is_some());
        assert_eq!(received.unwrap().num_rows(), 1);
    }

    #[test]
    fn view_subscribe_initially_none() {
        let (view, _rx) = IncrementalView::new(view_spec("v", false));
        let rx = view.subscribe();
        assert!(rx.borrow().is_none());
    }

    // ── IncrementalView: diff_and_update ──────────────────────────────────────

    #[test]
    fn view_diff_and_update_first_call_all_inserts() {
        let (view, _rx) = IncrementalView::new(view_spec("v", false));
        let batch =
            RecordBatch::try_new(x_schema(), vec![Arc::new(Int64Array::from(vec![10, 20]))])
                .unwrap();
        let delta = view.diff_and_update(batch).unwrap();
        assert_eq!(delta.num_rows(), 2);
        assert!(delta.weights().iter().all(|w| w == Some(1)));
    }

    #[test]
    fn view_diff_and_update_detects_new_rows() {
        let (view, _rx) = IncrementalView::new(view_spec("v", false));
        let batch1 =
            RecordBatch::try_new(x_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        view.diff_and_update(batch1).unwrap();
        let batch2 =
            RecordBatch::try_new(x_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let delta = view.diff_and_update(batch2).unwrap();
        assert_eq!(delta.num_rows(), 1);
        let data = delta.data_batch();
        let col = data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 2);
    }

    #[test]
    fn view_diff_and_update_detects_removed_rows() {
        let (view, _rx) = IncrementalView::new(view_spec("v", false));
        let batch1 =
            RecordBatch::try_new(x_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        view.diff_and_update(batch1).unwrap();
        let batch2 =
            RecordBatch::try_new(x_schema(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let delta = view.diff_and_update(batch2).unwrap();
        assert_eq!(delta.num_rows(), 1);
        assert_eq!(delta.weights().value(0), -1);
    }

    // ── IncrementalView: reset_full_output ────────────────────────────────────

    #[test]
    fn view_reset_full_output_clears_state() {
        let (view, _rx) = IncrementalView::new(view_spec("v", false));
        let batch =
            RecordBatch::try_new(x_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        view.diff_and_update(batch).unwrap();
        view.reset_full_output().unwrap();
        let new_batch =
            RecordBatch::try_new(x_schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let delta = view.diff_and_update(new_batch).unwrap();
        assert_eq!(delta.num_rows(), 2);
        assert!(delta.weights().iter().all(|w| w == Some(1)));
    }

    // ── IncrementalViewRegistry: view_names ───────────────────────────────────

    #[test]
    fn registry_view_names_empty() {
        let reg = IncrementalViewRegistry::new();
        assert!(reg.view_names().unwrap().is_empty());
    }

    #[test]
    fn registry_view_names_after_register() {
        let reg = IncrementalViewRegistry::new();
        reg.register(view_spec("a", false)).unwrap();
        reg.register(view_spec("b", false)).unwrap();
        let mut names = reg.view_names().unwrap();
        names.sort();
        assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
    }

    // ── IncrementalViewRegistry: contains ─────────────────────────────────────

    #[test]
    fn registry_contains_existing() {
        let reg = IncrementalViewRegistry::new();
        reg.register(view_spec("v1", false)).unwrap();
        assert!(reg.contains("v1"));
    }

    #[test]
    fn registry_contains_missing() {
        let reg = IncrementalViewRegistry::new();
        assert!(!reg.contains("nope"));
    }

    #[test]
    fn registry_contains_after_drop() {
        let reg = IncrementalViewRegistry::new();
        reg.register(view_spec("v1", false)).unwrap();
        reg.drop_view("v1").unwrap();
        assert!(!reg.contains("v1"));
    }

    // ── SourceOrdinal::new ────────────────────────────────────────────────────

    #[test]
    fn source_ordinal_new_stores_fields() {
        let so = SourceOrdinal::new("kafka_orders", vec![0, 1, 2]);
        assert_eq!(so.source_name, "kafka_orders");
        assert_eq!(so.last_processed, vec![0, 1, 2]);
    }

    #[test]
    fn source_ordinal_new_with_empty_offset() {
        let so = SourceOrdinal::new("sensor", vec![]);
        assert_eq!(so.source_name, "sensor");
        assert!(so.last_processed.is_empty());
    }
}
