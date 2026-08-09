//! Bridge Krishiv [`ScalarUdf`] implementations into DataFusion.

use std::sync::Arc;

use arrow::array::{Array, RecordBatchOptions};
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
    // Reject zero-argument scalar UDFs up front, before anything is registered.
    //
    // `create_udf` takes a `Fn(&[ColumnarValue])`, which drops the
    // `number_rows` that `ScalarFunctionArgs` carries. With no arguments there
    // is nothing left to infer the row count from, so
    // `columnar_values_to_record_batch` builds a 0-row batch and the UDF returns
    // a 0-length array for a projection over N rows — an opaque Arrow
    // "all columns must have the same length" failure deep in execution. Fail at
    // registration with a message that names the actual problem instead.
    for name in &scalar_names {
        let Some(udf) = registry.get_scalar(name) else {
            continue;
        };
        if udf.input_schema().fields().is_empty() {
            return Err(DataFusionError::External(
                format!(
                    "scalar UDF '{name}' declares no input columns; zero-argument \
                     scalar UDFs are not supported because the row count cannot be \
                     recovered at the DataFusion boundary"
                )
                .into(),
            ));
        }
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
            volatility_to_df(udf.volatility()),
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

/// Map a `krishiv_plan::udf::Volatility` to a `datafusion::logical_expr::Volatility`.
fn volatility_to_df(v: krishiv_plan::udf::Volatility) -> Volatility {
    use krishiv_plan::udf::Volatility as Plan;
    match v {
        Plan::Immutable => Volatility::Immutable,
        Plan::Stable => Volatility::Stable,
        Plan::Volatile => Volatility::Volatile,
    }
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
        let udaf_volatility = volatility_to_df(udf.volatility());
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
            udaf_volatility,
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
            &RecordBatchOptions::new()
                .with_row_count(Some(values.first().map(|v| v.len()).unwrap_or(0))),
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
        let array = states
            .first()
            .ok_or_else(|| DataFusionError::Internal("empty states".to_string()))?
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "merge_batch: expected BinaryArray for aggregate state".into(),
                )
            })?;
        for i in 0..array.len() {
            if array.is_null(i) {
                continue;
            }
            let other = krishiv_plan::udf::AggState {
                data: array.value(i).to_vec(),
            };
            let old_state = std::mem::take(&mut self.state);
            self.state = self
                .udf
                .merge(old_state, other)
                .map_err(|e| DataFusionError::External(e.to_string().into()))?;
        }
        Ok(())
    }

    /// Finalise **without consuming** the accumulated state.
    ///
    /// DataFusion's `Accumulator::evaluate` contract is explicit: "This function
    /// must not consume the internal state, as it is also used in window
    /// aggregate functions where it can be executed multiple times depending on
    /// the current window frame. Consuming the internal state can cause the next
    /// invocation to have incorrect results."
    ///
    /// This used to `std::mem::take` the state. Two reachable consequences:
    ///
    /// * a window frame — `udaf(x) OVER (ORDER BY t ROWS BETWEEN UNBOUNDED
    ///   PRECEDING AND CURRENT ROW)` — calls `evaluate` once per row, so every
    ///   row after the first aggregated only the rows since the previous call;
    /// * `AggregateStream::maybe_update_dyn_filter` calls `evaluate` *mid-stream*
    ///   to refresh a dynamic filter bound, selecting accumulators by function
    ///   name alone (`eq_ignore_ascii_case("min"|"max")`, flagged as a HACK in
    ///   datafusion#18643) — so a UDAF registered as `min`/`max` was hit too.
    ///
    /// Both produced silently wrong numbers rather than an error. Cloning is
    /// cheap: `AggState` is a `Vec<u8>` the UDF owns the format of.
    fn evaluate(&mut self) -> datafusion::error::Result<datafusion::scalar::ScalarValue> {
        let result = self
            .udf
            .finalize(self.state.clone())
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
///
/// # Why there is no durability-profile gate here
///
/// Scalar and aggregate sync both refuse to register under a durable or
/// production profile unless `KRISHIV_ALLOW_FULL_PRIVILEGE_UDFS=1`. Table UDFs
/// are deliberately exempt, and the asymmetry is documented here so it is not
/// "fixed" by adding an over-broad gate.
///
/// The policy exists to keep *arbitrary native code* out of a durable engine.
/// Only two things put a `TableUdf` in the registry, and neither is that:
///
/// * `SqlEngine::register_table_udf_fn` takes a Rust closure — a caller who can
///   reach it is already executing native code in this process, so the gate
///   would deny nothing;
/// * `CREATE FUNCTION … RETURNS TABLE … LANGUAGE SQL` is a SQL body run through
///   the session, with no native code at all. Gating it would break a supported
///   SQL feature under the production profile for no security gain.
///
/// The remote vector the gate does close — `register_python_udf`/`_udaf`
/// accepting cloudpickled bytes over the wire — lands in the scalar and
/// aggregate registries, both of which are gated.
pub fn sync_table_udfs(
    ctx: &datafusion::prelude::SessionContext,
    registry: &krishiv_plan::udf::UdfRegistry,
) -> Result<(), DataFusionError> {
    for name in registry.table_names() {
        let Some(udf) = registry.get_table(name) else {
            continue;
        };
        let udf_name = udf.name().to_string();
        let schema = udf.output_schema().clone();
        ctx.register_udtf(
            &udf_name,
            Arc::new(KrishivTableFunctionImpl {
                inner: Arc::clone(udf),
                schema,
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

    /// A zero-argument scalar UDF cannot work: `create_udf` erases
    /// `number_rows`, so the bridge has no way to size the output array. Reject
    /// at registration rather than failing with an Arrow length mismatch inside
    /// the query.
    #[test]
    fn zero_argument_scalar_udfs_are_rejected_at_registration() {
        #[derive(Debug)]
        struct NoArgUdf {
            input: Schema,
            output: arrow::datatypes::Field,
        }
        impl krishiv_plan::udf::ScalarUdf for NoArgUdf {
            fn name(&self) -> &str {
                "no_args"
            }
            fn input_schema(&self) -> &Schema {
                &self.input
            }
            fn output_field(&self) -> &arrow::datatypes::Field {
                &self.output
            }
            fn call(
                &self,
                _batch: &RecordBatch,
            ) -> Result<arrow::array::ArrayRef, krishiv_plan::udf::UdfError> {
                unreachable!("registration must fail before the UDF is ever called")
            }
        }

        let ctx = SessionContext::new();
        let mut registry = UdfRegistry::new();
        registry.register_scalar(Arc::new(NoArgUdf {
            input: Schema::empty(),
            output: arrow::datatypes::Field::new("out", DataType::Int64, true),
        }));

        let error = sync_scalar_udfs_with_limits_for_policy(
            &ctx,
            &registry,
            ResourceLimits::default(),
            krishiv_common::NativeScalarUdfPolicy::from_decision(
                krishiv_common::DurabilityProfile::DevLocal,
                false,
            ),
        )
        .expect_err("zero-argument scalar UDFs must be rejected");

        assert!(
            error.to_string().contains("declares no input columns"),
            "{error}"
        );
    }

    /// A counting UDAF whose state is the running total, little-endian.
    #[derive(Debug)]
    struct CountingUdf {
        input: Schema,
        output: arrow::datatypes::Field,
    }

    impl CountingUdf {
        fn new() -> Self {
            Self {
                input: Schema::new(vec![arrow::datatypes::Field::new(
                    "x",
                    DataType::Int64,
                    true,
                )]),
                output: arrow::datatypes::Field::new("out", DataType::Int64, true),
            }
        }

        fn total(state: &krishiv_plan::udf::AggState) -> i64 {
            let mut buf = [0u8; 8];
            let n = state.data.len().min(8);
            buf[..n].copy_from_slice(&state.data[..n]);
            i64::from_le_bytes(buf)
        }
    }

    impl krishiv_plan::udf::AggregateUdf for CountingUdf {
        fn name(&self) -> &str {
            "counting"
        }
        fn input_schema(&self) -> &Schema {
            &self.input
        }
        fn output_field(&self) -> &arrow::datatypes::Field {
            &self.output
        }
        fn accumulate(
            &self,
            state: &mut krishiv_plan::udf::AggState,
            batch: &RecordBatch,
        ) -> Result<(), krishiv_plan::udf::UdfError> {
            let next = Self::total(state) + batch.num_rows() as i64;
            state.data = next.to_le_bytes().to_vec();
            Ok(())
        }
        fn finalize(
            &self,
            state: krishiv_plan::udf::AggState,
        ) -> Result<krishiv_plan::udf::ScalarValue, krishiv_plan::udf::UdfError> {
            Ok(krishiv_plan::udf::ScalarValue::Int64(Self::total(&state)))
        }
        fn merge(
            &self,
            a: krishiv_plan::udf::AggState,
            b: krishiv_plan::udf::AggState,
        ) -> Result<krishiv_plan::udf::AggState, krishiv_plan::udf::UdfError> {
            let sum = Self::total(&a) + Self::total(&b);
            Ok(krishiv_plan::udf::AggState {
                data: sum.to_le_bytes().to_vec(),
            })
        }
    }

    /// `Accumulator::evaluate` must not consume the state — DataFusion calls it
    /// repeatedly for window frames, and mid-stream to refresh dynamic filter
    /// bounds. Consuming it made every call after the first see an empty state.
    #[test]
    fn evaluate_does_not_consume_the_accumulator_state() {
        use arrow::array::Int64Array;
        use datafusion::logical_expr::Accumulator as _;

        let udf = Arc::new(CountingUdf::new());
        let mut acc = KrishivAggregateAccumulator {
            udf: udf.clone(),
            state: krishiv_plan::udf::AggState::default(),
        };

        let values: Vec<arrow::array::ArrayRef> =
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))];
        acc.update_batch(&values).expect("update should succeed");

        let first = acc.evaluate().expect("first evaluate");
        let second = acc.evaluate().expect("second evaluate");
        assert_eq!(
            first, second,
            "evaluate must be repeatable; a differing second call means the \
             state was consumed (window frames call this once per row)"
        );

        // And accumulation must continue correctly afterwards.
        acc.update_batch(&values).expect("second update");
        let third = acc.evaluate().expect("third evaluate");
        assert_eq!(
            third,
            datafusion::scalar::ScalarValue::Int64(Some(6)),
            "3 rows + 3 rows = 6; a smaller number means evaluate reset the state"
        );
    }
}
