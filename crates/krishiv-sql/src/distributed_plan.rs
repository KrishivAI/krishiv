//! Distributed physical-plan fragments (ADR-0003).
//!
//! Phase 52 replaces stringly `sql: <query>` task bodies with
//! protobuf-encoded DataFusion physical-plan subtrees. The
//! [`krishiv_plan::TypedTaskFragment`] envelope stays as the carrier; this
//! module owns the `dfplan:` body kind — encoding on the coordinator (stage
//! builder) and decoding on the executor.
//!
//! Body format: `dfplan:v1:<base64(datafusion-proto physical plan bytes)>`.
//! The version segment is independent of the envelope version so plan-proto
//! evolution (e.g. a DataFusion upgrade that changes the proto) is detected
//! explicitly instead of failing deep inside prost decoding.

use std::sync::Arc;

use base64::Engine as _;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::{SqlError, SqlResult};

/// Task-fragment body prefix for proto-encoded physical-plan subtrees.
pub const DFPLAN_BODY_PREFIX: &str = "dfplan:v1:";

/// Encode a physical plan (sub)tree as a `dfplan:v1:` fragment body.
pub fn encode_dfplan_body(
    plan: Arc<dyn ExecutionPlan>,
    codec: &dyn PhysicalExtensionCodec,
) -> SqlResult<String> {
    let bytes = datafusion_proto::bytes::physical_plan_to_bytes_with_extension_codec(plan, codec)
        .map_err(|e| SqlError::DataFusion {
        message: format!("physical plan proto encode: {e}"),
    })?;
    let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
    Ok(format!("{DFPLAN_BODY_PREFIX}{b64}"))
}

/// Decode a `dfplan:v1:` fragment body back into an executable plan.
///
/// `ctx` supplies the runtime environment (object stores, UDFs) the decoded
/// plan executes under; it does not need the original tables registered —
/// scan nodes carry their own file/split descriptions in the proto.
pub fn decode_dfplan_body(
    body: &str,
    ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
) -> SqlResult<Arc<dyn ExecutionPlan>> {
    let b64 = body
        .strip_prefix(DFPLAN_BODY_PREFIX)
        .ok_or_else(|| SqlError::DataFusion {
            message: format!(
                "task body is not a {DFPLAN_BODY_PREFIX} fragment: {}",
                body.chars().take(48).collect::<String>()
            ),
        })?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64.as_bytes())
        .map_err(|e| SqlError::DataFusion {
            message: format!("dfplan base64 decode: {e}"),
        })?;
    datafusion_proto::bytes::physical_plan_from_bytes_with_extension_codec(&bytes, ctx, codec)
        .map_err(|e| SqlError::DataFusion {
            message: format!("physical plan proto decode: {e}"),
        })
}

/// True when a task-fragment body carries a proto-encoded physical plan.
pub fn is_dfplan_body(body: &str) -> bool {
    body.starts_with(DFPLAN_BODY_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::{ExecutionPlanProperties as _, displayable};
    use datafusion::prelude::SessionContext;
    use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;

    async fn write_test_parquet(dir: &std::path::Path) -> std::path::PathBuf {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from((0..1000).collect::<Vec<_>>())),
                Arc::new(StringArray::from(
                    (0..1000)
                        .map(|i| if i % 2 == 0 { "even" } else { "odd" })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    (0..1000).map(|i| i * 3).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("test batch");
        let path = dir.join("dfplan_spike.parquet");
        let file = std::fs::File::create(&path).expect("create parquet");
        let mut writer = datafusion::parquet::arrow::ArrowWriter::try_new(file, schema, None)
            .expect("writer init");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
        path
    }

    /// ADR-0003 risk gate: a scan→filter→hash-aggregate plan round-trips
    /// through datafusion-proto on the pinned DataFusion and produces
    /// identical results when executed from the decoded plan.
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
        let body = encode_dfplan_body(Arc::clone(&plan), &codec).expect("encode");
        assert!(is_dfplan_body(&body));

        // Decode on a FRESH context with no tables registered — the executor
        // side never re-registers coordinator tables.
        let exec_ctx = SessionContext::new();
        let decoded = decode_dfplan_body(&body, &exec_ctx.task_ctx(), &codec).expect("decode");
        let decoded_display = displayable(decoded.as_ref()).indent(true).to_string();
        assert_eq!(
            original_display, decoded_display,
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
        assert_eq!(total_rows, 2, "two category groups expected");
    }

    /// Round-trip a hash-join plan — joins are the first scope target for
    /// stage splitting (phase risk note: joins/aggregates first).
    #[tokio::test]
    async fn join_plan_round_trips_through_proto() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = write_test_parquet(tmp.path()).await;

        let ctx = SessionContext::new();
        for name in ["a", "b"] {
            ctx.register_parquet(
                name,
                path.to_str().expect("utf8 path"),
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await
            .expect("register parquet");
        }
        let df = ctx
            .sql(
                "SELECT a.category, COUNT(*) AS n FROM a JOIN b ON a.id = b.id GROUP BY a.category",
            )
            .await
            .expect("sql");
        let plan = df.create_physical_plan().await.expect("physical plan");
        let original_display = displayable(plan.as_ref()).indent(true).to_string();

        let codec = DefaultPhysicalExtensionCodec {};
        let body = encode_dfplan_body(Arc::clone(&plan), &codec).expect("encode");
        let exec_ctx = SessionContext::new();
        let decoded = decode_dfplan_body(&body, &exec_ctx.task_ctx(), &codec).expect("decode");
        assert_eq!(
            original_display,
            displayable(decoded.as_ref()).indent(true).to_string()
        );
    }

    #[test]
    fn non_dfplan_body_is_rejected() {
        let ctx = SessionContext::new();
        let codec = DefaultPhysicalExtensionCodec {};
        let err = decode_dfplan_body("sql: SELECT 1", &ctx.task_ctx(), &codec).unwrap_err();
        assert!(err.to_string().contains("not a dfplan:v1: fragment"));
    }
}
