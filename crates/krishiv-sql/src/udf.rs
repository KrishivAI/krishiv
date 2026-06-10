//! Bridge Krishiv [`ScalarUdf`] implementations into DataFusion.

use std::sync::Arc;

use arrow::array::RecordBatchOptions;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::AccumulatorFactoryFunction;
use datafusion::logical_expr::{Accumulator, ColumnarValue, Volatility, create_udaf, create_udf};

use krishiv_plan::udf::{DefaultSandboxedExecutor, ResourceLimits, SandboxedUdfExecutor};

/// Register scalar UDFs with explicit ResourceLimits.
/// Higher layers (JobSpec / scheduler / executor runner) supply real budgets
/// from the job; DefaultSandboxedExecutor will enforce them at execution time.
pub fn sync_scalar_udfs_with_limits(
    ctx: &datafusion::prelude::SessionContext,
    registry: &krishiv_plan::udf::UdfRegistry,
    limits: ResourceLimits,
) -> Result<(), DataFusionError> {
    sync_scalar_udfs_with_limits_for_profile(
        ctx,
        registry,
        limits,
        krishiv_common::resolve_durability_profile(),
    )
}

/// Register scalar UDFs using one caller-resolved durability profile.
///
/// Passing the profile explicitly keeps policy validation stable for the
/// duration of a higher-level registration operation.
pub fn sync_scalar_udfs_with_limits_for_profile(
    ctx: &datafusion::prelude::SessionContext,
    registry: &krishiv_plan::udf::UdfRegistry,
    limits: ResourceLimits,
    profile: krishiv_common::DurabilityProfile,
) -> Result<(), DataFusionError> {
    sync_scalar_udfs_with_limits_for_policy(
        ctx,
        registry,
        limits,
        krishiv_common::NativeScalarUdfPolicy::resolve(profile),
    )
}

pub(crate) fn sync_scalar_udfs_with_limits_for_policy(
    ctx: &datafusion::prelude::SessionContext,
    registry: &krishiv_plan::udf::UdfRegistry,
    limits: ResourceLimits,
    policy: krishiv_common::NativeScalarUdfPolicy,
) -> Result<(), DataFusionError> {
    let scalar_names = registry.scalar_names();
    if scalar_names.iter().any(|name| name.trim().is_empty()) {
        return Err(DataFusionError::External(
            "scalar UDF name must not be empty".into(),
        ));
    }
    if policy.is_forbidden() && !scalar_names.is_empty() {
        return Err(DataFusionError::External(
            format!(
                "native scalar UDF registration is forbidden under durability profile '{}' \
                 (set KRISHIV_ALLOW_FULL_PRIVILEGE_UDFS=1 to override)",
                policy.profile()
            )
            .into(),
        ));
    }

    let limits = Arc::new(limits);
    for name in scalar_names {
        let Some(udf) = registry.get_scalar(name) else {
            continue;
        };
        let udf = Arc::clone(udf);
        let udf_name = udf.name().to_string();
        let input_types: Vec<DataType> = udf
            .input_schema()
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let return_type = udf.output_field().data_type().clone();
        let input_schema = udf.input_schema().clone();
        let limits = Arc::clone(&limits);

        let df_udf = create_udf(
            &udf_name,
            input_types,
            return_type,
            Volatility::Immutable,
            Arc::new(move |args: &[ColumnarValue]| {
                let batch = columnar_values_to_record_batch(&input_schema, args)?;
                // Sandboxed execution with caller-supplied ResourceLimits (Track E).
                // Enforcement (time + memory) happens inside DefaultSandboxedExecutor.
                let executor = DefaultSandboxedExecutor;
                let array = executor
                    .execute_with_limits(udf.as_ref(), &batch, &limits)
                    .map_err(|e| DataFusionError::External(e.to_string().into()))?;
                Ok(ColumnarValue::Array(array))
            }),
        );
        ctx.register_udf(df_udf);
    }
    Ok(())
}

/// Register aggregate UDFs from `registry` with DataFusion (P1-21).
pub fn sync_aggregate_udfs(
    ctx: &datafusion::prelude::SessionContext,
    registry: &krishiv_plan::udf::UdfRegistry,
) -> Result<(), DataFusionError> {
    let profile = krishiv_common::resolve_durability_profile();
    if krishiv_common::profile_forbids_native_scalar_udfs(profile)
        && !registry.aggregate_names().is_empty()
    {
        return Err(DataFusionError::External(
            format!(
                "native aggregate UDF registration is forbidden under durability profile '{profile}' \
                 (set KRISHIV_ALLOW_FULL_PRIVILEGE_UDFS=1 to override)"
            )
            .into(),
        ));
    }

    for name in registry.aggregate_names() {
        let Some(udf) = registry.get_aggregate(name) else {
            continue;
        };
        let udf = Arc::clone(udf);
        let udf_name = udf.name().to_string();
        let input_types: Vec<DataType> = udf
            .input_schema()
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let return_type = Arc::new(udf.output_field().data_type().clone());
        let state_type = Arc::new(vec![DataType::Binary]);

        let accumulator: AccumulatorFactoryFunction = Arc::new({
            let udf = Arc::clone(&udf);
            move |_args| {
                let udf = Arc::clone(&udf);
                Ok(Box::new(KrishivAggregateAccumulator {
                    udf,
                    state: krishiv_plan::udf::AggState::default(),
                }) as Box<dyn Accumulator>)
            }
        });

        let df_udaf = create_udaf(
            &udf_name,
            input_types,
            Arc::clone(&return_type),
            Volatility::Immutable,
            accumulator,
            state_type,
        );

        ctx.register_udaf(df_udaf);
    }
    Ok(())
}

/// DataFusion Accumulator bridge that delegates to a [`krishiv_plan::udf::AggregateUdf`].
#[derive(Debug)]
struct KrishivAggregateAccumulator {
    udf: Arc<dyn krishiv_plan::udf::AggregateUdf>,
    state: krishiv_plan::udf::AggState,
}

impl Accumulator for KrishivAggregateAccumulator {
    fn update_batch(&mut self, values: &[arrow::array::ArrayRef]) -> datafusion::error::Result<()> {
        let schema = self.udf.input_schema();
        if values.len() != schema.fields().len() {
            return Err(DataFusionError::Plan(format!(
                "aggregate UDF '{}' expected {} arguments, got {}",
                self.udf.name(),
                schema.fields().len(),
                values.len()
            )));
        }
        let batch = RecordBatch::try_new_with_options(
            Arc::new(schema.clone()),
            values.to_vec(),
            &RecordBatchOptions::new().with_row_count(Some(values[0].len())),
        )
        .map_err(|e| DataFusionError::External(e.to_string().into()))?;
        self.udf
            .accumulate(&mut self.state, &batch)
            .map_err(|e| DataFusionError::External(e.to_string().into()))
    }

    fn merge_batch(&mut self, states: &[arrow::array::ArrayRef]) -> datafusion::error::Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        let data = states[0].to_data();
        let buffers = data.buffers();
        if buffers.len() < 2 {
            return Err(DataFusionError::Execution(
                "merge_batch: expected BinaryArray with 2 buffers (offsets + data)".into(),
            ));
        }
        let offset_slice = buffers[0].as_slice();
        let data_slice = buffers[1].as_slice();
        let len = states[0].len();
        // Determine offset width from the data type (i32 = 4, i64 = 8).
        let offset_bytes = match states[0].data_type() {
            arrow::datatypes::DataType::Binary
            | arrow::datatypes::DataType::Utf8
            | arrow::datatypes::DataType::LargeUtf8
            | arrow::datatypes::DataType::LargeBinary => 8,
            _ => 4, // i32 offsets (default BinaryArray)
        };
        for i in 0..len {
            let start = read_offset(offset_slice, i * offset_bytes, offset_bytes)?;
            let end = read_offset(offset_slice, (i + 1) * offset_bytes, offset_bytes)?;
            if end > data_slice.len() || start > end {
                return Err(DataFusionError::Execution(
                    "merge_batch: offset out of bounds".into(),
                ));
            }
            let other = krishiv_plan::udf::AggState {
                data: data_slice[start..end].to_vec(),
            };
            let old_state = std::mem::take(&mut self.state);
            self.state = self
                .udf
                .merge(old_state, other)
                .map_err(|e| DataFusionError::External(e.to_string().into()))?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion::error::Result<datafusion::scalar::ScalarValue> {
        let state = std::mem::take(&mut self.state);
        let result = self
            .udf
            .finalize(state)
            .map_err(|e| DataFusionError::External(e.to_string().into()))?;
        krishiv_scalar_to_datafusion(&result)
    }

    fn size(&self) -> usize {
        self.state.data.len() + std::mem::size_of::<Self>()
    }

    fn state(&mut self) -> datafusion::error::Result<Vec<datafusion::scalar::ScalarValue>> {
        use datafusion::scalar::ScalarValue as DfScalar;
        Ok(vec![DfScalar::Binary(Some(self.state.data.clone()))])
    }
}

fn read_offset(buf: &[u8], pos: usize, width: usize) -> datafusion::error::Result<usize> {
    if pos + width > buf.len() {
        return Err(DataFusionError::Execution(
            "merge_batch: offset buffer underrun".into(),
        ));
    }
    match width {
        4 => {
            let arr: [u8; 4] = buf[pos..pos + 4].try_into().map_err(|_| {
                DataFusionError::Execution("merge_batch: invalid i32 offset".into())
            })?;
            Ok(i32::from_le_bytes(arr) as usize)
        }
        8 => {
            let arr: [u8; 8] = buf[pos..pos + 8].try_into().map_err(|_| {
                DataFusionError::Execution("merge_batch: invalid i64 offset".into())
            })?;
            Ok(i64::from_le_bytes(arr) as usize)
        }
        _ => Err(DataFusionError::Execution(format!(
            "merge_batch: unsupported offset width {width}"
        ))),
    }
}

fn krishiv_scalar_to_datafusion(
    value: &krishiv_plan::udf::ScalarValue,
) -> datafusion::error::Result<datafusion::scalar::ScalarValue> {
    use datafusion::scalar::ScalarValue as DfScalar;
    match value {
        krishiv_plan::udf::ScalarValue::Null => Ok(DfScalar::Null),
        krishiv_plan::udf::ScalarValue::Int64(v) => Ok(DfScalar::Int64(Some(*v))),
        krishiv_plan::udf::ScalarValue::Float64(v) => Ok(DfScalar::Float64(Some(*v))),
        krishiv_plan::udf::ScalarValue::Utf8(v) => Ok(DfScalar::Utf8(Some(v.clone()))),
        krishiv_plan::udf::ScalarValue::Boolean(v) => Ok(DfScalar::Boolean(Some(*v))),
        krishiv_plan::udf::ScalarValue::Bytes(v) => Ok(DfScalar::Binary(Some(v.clone()))),
    }
}

/// Register a single table UDF directly with DataFusion (used by
/// `SqlEngine` when registering a `LANGUAGE sql` UDTF at DDL time).
pub fn register_single_table_udf(
    ctx: &datafusion::prelude::SessionContext,
    udf: Arc<dyn krishiv_plan::udf::TableUdf>,
) -> Result<(), DataFusionError> {
    let udf_name = udf.name().to_string();
    let output_schema = udf.output_schema().clone();
    ctx.register_udtf(
        &udf_name,
        Arc::new(KrishivTableFunctionImpl {
            inner: udf,
            schema: output_schema,
        }),
    );
    Ok(())
}

/// Register table UDFs from `registry` with DataFusion (P1-21).
pub fn sync_table_udfs(
    ctx: &datafusion::prelude::SessionContext,
    registry: &krishiv_plan::udf::UdfRegistry,
) -> Result<(), DataFusionError> {
    for name in registry.table_names() {
        let Some(udf) = registry.get_table(name) else {
            continue;
        };
        let udf = Arc::clone(udf);
        let udf_name = udf.name().to_string();
        let output_schema = udf.output_schema().clone();
        let inner_udf = Arc::clone(&udf);

        ctx.register_udtf(
            &udf_name,
            Arc::new(KrishivTableFunctionImpl {
                inner: Arc::clone(&inner_udf),
                schema: output_schema.clone(),
            }),
        );
    }
    Ok(())
}

#[derive(Debug)]
struct KrishivTableFunctionImpl {
    inner: Arc<dyn krishiv_plan::udf::TableUdf>,
    schema: arrow::datatypes::Schema,
}

impl TableFunctionImpl for KrishivTableFunctionImpl {
    fn call(
        &self,
        args: &[datafusion::logical_expr::Expr],
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        // Extract literal scalar values from the DataFusion Expr arguments and
        // pass them to the UDTF body. Computed expressions cannot be evaluated
        // correctly at this synchronous table-function boundary, so fail
        // closed instead of silently replacing them with NULL.
        let scalar_args: Vec<krishiv_plan::udf::ScalarValue> =
            args.iter()
                .map(expr_to_scalar)
                .collect::<datafusion::error::Result<_>>()?;
        let batch = self
            .inner
            .call(&scalar_args)
            .map_err(|e| DataFusionError::External(e.to_string().into()))?;
        let table = MemTable::try_new(Arc::new(self.schema.clone()), vec![vec![batch]])?;
        Ok(Arc::new(table))
    }
}

/// Extract a [`krishiv_plan::udf::ScalarValue`] from a DataFusion literal expression.
fn expr_to_scalar(
    expr: &datafusion::logical_expr::Expr,
) -> datafusion::error::Result<krishiv_plan::udf::ScalarValue> {
    use datafusion::logical_expr::Expr;
    use datafusion::scalar::ScalarValue as DfScalar;
    match expr {
        Expr::Literal(value, _) if value.is_null() => Ok(krishiv_plan::udf::ScalarValue::Null),
        Expr::Literal(DfScalar::Int8(Some(v)), _) => {
            Ok(krishiv_plan::udf::ScalarValue::Int64(i64::from(*v)))
        }
        Expr::Literal(DfScalar::Int16(Some(v)), _) => {
            Ok(krishiv_plan::udf::ScalarValue::Int64(i64::from(*v)))
        }
        Expr::Literal(DfScalar::Int32(Some(v)), _) => {
            Ok(krishiv_plan::udf::ScalarValue::Int64(i64::from(*v)))
        }
        Expr::Literal(DfScalar::Int64(Some(v)), _) => Ok(krishiv_plan::udf::ScalarValue::Int64(*v)),
        Expr::Literal(DfScalar::UInt8(Some(v)), _) => {
            Ok(krishiv_plan::udf::ScalarValue::Int64(i64::from(*v)))
        }
        Expr::Literal(DfScalar::UInt16(Some(v)), _) => {
            Ok(krishiv_plan::udf::ScalarValue::Int64(i64::from(*v)))
        }
        Expr::Literal(DfScalar::UInt32(Some(v)), _) => {
            Ok(krishiv_plan::udf::ScalarValue::Int64(i64::from(*v)))
        }
        Expr::Literal(DfScalar::UInt64(Some(v)), _) => i64::try_from(*v)
            .map(krishiv_plan::udf::ScalarValue::Int64)
            .map_err(|_| {
                DataFusionError::Execution(format!(
                    "UDTF unsigned integer argument {v} exceeds the supported i64 range"
                ))
            }),
        Expr::Literal(DfScalar::Float32(Some(v)), _) => {
            Ok(krishiv_plan::udf::ScalarValue::Float64(f64::from(*v)))
        }
        Expr::Literal(DfScalar::Float64(Some(v)), _) => {
            Ok(krishiv_plan::udf::ScalarValue::Float64(*v))
        }
        Expr::Literal(DfScalar::Utf8(Some(v)), _)
        | Expr::Literal(DfScalar::Utf8View(Some(v)), _)
        | Expr::Literal(DfScalar::LargeUtf8(Some(v)), _) => {
            Ok(krishiv_plan::udf::ScalarValue::Utf8(v.clone()))
        }
        Expr::Literal(DfScalar::Boolean(Some(v)), _) => {
            Ok(krishiv_plan::udf::ScalarValue::Boolean(*v))
        }
        Expr::Literal(DfScalar::Binary(Some(v)), _)
        | Expr::Literal(DfScalar::BinaryView(Some(v)), _)
        | Expr::Literal(DfScalar::LargeBinary(Some(v)), _)
        | Expr::Literal(DfScalar::FixedSizeBinary(_, Some(v)), _) => {
            Ok(krishiv_plan::udf::ScalarValue::Bytes(v.clone()))
        }
        Expr::Literal(value, _) => Err(DataFusionError::Execution(format!(
            "unsupported UDTF literal argument {value:?}"
        ))),
        other => Err(DataFusionError::Execution(format!(
            "UDTF arguments must be scalar literals; got {other:?}"
        ))),
    }
}

fn columnar_values_to_record_batch(
    schema: &Schema,
    values: &[ColumnarValue],
) -> Result<RecordBatch, DataFusionError> {
    if values.len() != schema.fields().len() {
        return Err(DataFusionError::External(
            format!(
                "expected {} arguments, got {}",
                schema.fields().len(),
                values.len()
            )
            .into(),
        ));
    }

    let num_rows = values
        .iter()
        .map(|v| match v {
            ColumnarValue::Array(a) => a.len(),
            ColumnarValue::Scalar(_) => 1,
        })
        .max()
        .unwrap_or(0);

    let mut columns = Vec::with_capacity(values.len());
    for (value, field) in values.iter().zip(schema.fields()) {
        let array = match value {
            ColumnarValue::Array(a) => {
                if a.len() != num_rows {
                    return Err(DataFusionError::External(
                        format!(
                            "column '{}' length {} does not match batch length {}",
                            field.name(),
                            a.len(),
                            num_rows
                        )
                        .into(),
                    ));
                }
                Arc::clone(a)
            }
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows)?,
        };
        columns.push(array);
    }

    RecordBatch::try_new_with_options(
        Arc::new(schema.clone()),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )
    .map_err(DataFusionError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use krishiv_plan::udf::{MultiplyScalarUdf, ResourceLimits, UdfRegistry};

    #[test]
    fn sync_scalar_udfs_with_limits_accepts_non_default_budget() {
        // Track E wiring test: the new limits-aware registration path must accept
        // a real ResourceLimits from a higher layer (JobSpec / scheduler) without
        // panicking or falling back to the unlimited default internally.
        let ctx = SessionContext::new();
        let registry = UdfRegistry::new();

        let limits = ResourceLimits {
            max_execution_time_ms: Some(5_000),
            max_memory_bytes: Some(64 * 1024 * 1024),
            ..ResourceLimits::default()
        };

        // Should succeed and register the (empty) set of UDFs with the supplied limits
        // captured in the closure. Real enforcement is proven in krishiv-udf tests.
        let res = sync_scalar_udfs_with_limits(&ctx, &registry, limits);
        assert!(res.is_ok(), "limits-aware UDF sync must succeed");
    }

    #[test]
    fn explicit_durable_profile_rejects_native_scalar_udfs() {
        let ctx = SessionContext::new();
        let mut registry = UdfRegistry::new();
        registry.register_scalar(Arc::new(MultiplyScalarUdf::new("double", "x", 2)));

        let error = sync_scalar_udfs_with_limits_for_policy(
            &ctx,
            &registry,
            ResourceLimits::default(),
            krishiv_common::NativeScalarUdfPolicy::from_decision(
                krishiv_common::DurabilityProfile::SingleNodeDurable,
                true,
            ),
        )
        .expect_err("durable profile must reject native scalar UDFs");

        assert!(error.to_string().contains("single-node-durable"));
    }

    #[test]
    fn scalar_udf_sync_rejects_empty_names() {
        let ctx = SessionContext::new();
        let mut registry = UdfRegistry::new();
        registry.register_scalar(Arc::new(MultiplyScalarUdf::new(" ", "x", 2)));

        let error = sync_scalar_udfs_with_limits_for_policy(
            &ctx,
            &registry,
            ResourceLimits::default(),
            krishiv_common::NativeScalarUdfPolicy::from_decision(
                krishiv_common::DurabilityProfile::DevLocal,
                false,
            ),
        )
        .expect_err("empty scalar UDF names must be rejected");

        assert!(error.to_string().contains("must not be empty"));
    }
}
