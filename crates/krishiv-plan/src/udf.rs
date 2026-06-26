#![forbid(unsafe_code)]

//! User-defined function (UDF) extension framework for Krishiv.
//!
//! Provides stable Rust contracts for scalar UDFs, aggregate UDAFs, and
//! table-valued UDTFs, along with a runtime registry.
//!
//! Location for UDF sandboxing and resource limits.
//! resource limits (CPU time, memory), and secure execution environments for
//! untrusted user code. Currently UDFs run with full process privileges.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use arrow::record_batch::RecordBatch;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can occur during UDF execution.
#[derive(Debug, thiserror::Error)]
pub enum UdfError {
    /// An error originating from the Arrow library.
    #[error("Arrow error: {0}")]
    Arrow(String),
    /// A general execution error.
    #[error("Execution error: {message}")]
    Execution { message: String },
    /// A panic was caught during UDF execution.
    #[error("Panic: {0}")]
    Panic(String),
    /// An invalid argument was supplied to the UDF.
    #[error("Invalid argument: {message}")]
    InvalidArgument { message: String },
}

impl From<arrow::error::ArrowError> for UdfError {
    fn from(e: arrow::error::ArrowError) -> Self {
        UdfError::Arrow(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Scalar UDF trait
// ---------------------------------------------------------------------------

/// Volatility classification of a UDF (S3).
///
/// Mirrors DataFusion / Spark's `Volatility` enum so the optimizer can
/// decide whether constant-folding, predicate pushdown, and result caching
/// are safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Volatility {
    /// Always returns the same output for the same input. Default.
    #[default]
    Immutable,
    /// Returns the same output for the same input within a single query
    /// (e.g. a function that reads a session-scoped parameter).
    Stable,
    /// May return a different value on every invocation. `current_timestamp`,
    /// `rand`, `uuid` etc. The optimizer must not fold or cache the result.
    Volatile,
}

/// A vectorized scalar function that operates over a [`RecordBatch`].
///
/// Implementations receive an entire batch and must return an [`ArrayRef`]
/// with the same number of rows.
pub trait ScalarUdf: Send + Sync + fmt::Debug {
    /// Unique name used to look up this UDF in a [`UdfRegistry`].
    fn name(&self) -> &str;

    /// The schema of the input columns this UDF expects.
    fn input_schema(&self) -> &Schema;

    /// The output field (name + data-type) produced by this UDF.
    fn output_field(&self) -> &Field;

    /// Volatility classification of the UDF (S3).
    ///
    /// Defaults to [`Volatility::Immutable`] so existing UDFs are unaffected.
    /// Non-deterministic UDFs (`current_timestamp`, `rand`, `uuid`, …)
    /// should override this to [`Volatility::Volatile`] so the optimizer
    /// does not fold or cache their results.
    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    /// Execute the UDF over `batch`, returning one value per row.
    fn call(&self, batch: &RecordBatch) -> Result<ArrayRef, UdfError>;
}

// ---------------------------------------------------------------------------
// Aggregate UDF types and trait
// ---------------------------------------------------------------------------

/// Opaque serialised accumulator state owned by an aggregate UDF.
#[derive(Debug, Default, Clone)]
pub struct AggState {
    /// Raw bytes in a UDF-defined format.
    pub data: Vec<u8>,
}

/// A scalar value emitted by a finalised aggregate.
#[derive(Debug, Clone)]
pub enum ScalarValue {
    Null,
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Boolean(bool),
    Bytes(Vec<u8>),
}

/// A streaming aggregate UDF that accumulates Arrow batches and produces a
/// single [`ScalarValue`] per group.
pub trait AggregateUdf: Send + Sync + fmt::Debug {
    /// Unique name used to look up this UDAF in a [`UdfRegistry`].
    fn name(&self) -> &str;

    /// The schema of the input columns this UDAF expects.
    fn input_schema(&self) -> &Schema;

    /// The output field produced when the UDAF is finalised.
    fn output_field(&self) -> &Field;

    /// Volatility classification of the UDAF (S3).
    ///
    /// Defaults to [`Volatility::Immutable`]. UDAFs that depend on external
    /// state (e.g. session timeouts, randomness) should return
    /// [`Volatility::Volatile`].
    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    /// Merge new data from `batch` into `state`.
    fn accumulate(&self, state: &mut AggState, batch: &RecordBatch) -> Result<(), UdfError>;

    /// Produce a final result from an accumulated `state`.
    fn finalize(&self, state: AggState) -> Result<ScalarValue, UdfError>;

    /// Merge two partial states into one (for distributed execution).
    fn merge(&self, a: AggState, b: AggState) -> Result<AggState, UdfError>;
}

// ---------------------------------------------------------------------------
// Table UDF trait
// ---------------------------------------------------------------------------

/// A table-valued function that produces a [`RecordBatch`] from scalar
/// arguments.
pub trait TableUdf: Send + Sync + fmt::Debug {
    /// Unique name used to look up this UDTF in a [`UdfRegistry`].
    fn name(&self) -> &str;

    /// The schema of the [`RecordBatch`] returned by [`TableUdf::call`].
    fn output_schema(&self) -> &Schema;

    /// Invoke the UDTF with the supplied scalar arguments.
    fn call(&self, args: &[ScalarValue]) -> Result<RecordBatch, UdfError>;
}

// ---------------------------------------------------------------------------
// CoGroup map UDF trait
// ---------------------------------------------------------------------------

/// A co-group map function that receives all rows for a single key from two
/// input streams and emits zero or more output rows.
///
/// Used for streaming ML feature joins:
/// ```text
/// features = left_stream.co_group(right_stream, key="user_id", fn=join_fn)
/// ```
///
/// The function receives all batches for a key from both sides and must
/// return zero or more output batches.
pub trait CoGroupUdf: Send + Sync + fmt::Debug {
    /// Unique name used to look up this UDF in a [`UdfRegistry`].
    fn name(&self) -> &str;

    /// Schema of the left input stream.
    fn left_schema(&self) -> &Schema;

    /// Schema of the right input stream.
    fn right_schema(&self) -> &Schema;

    /// Schema of the output stream.
    fn output_schema(&self) -> &Schema;

    /// Invoke the function with all `left` and `right` batches for one key.
    ///
    /// Returns zero or more output batches.
    fn call(
        &self,
        key: &str,
        left: &[RecordBatch],
        right: &[RecordBatch],
    ) -> Result<Vec<RecordBatch>, UdfError>;
}

// ---------------------------------------------------------------------------
// Map-pandas-iter UDF trait
// ---------------------------------------------------------------------------

/// A stateful iterator-over-batches map function.
///
/// Receives batches from one partition one at a time; may return multiple
/// output batches per input batch. The Python callable receives a pandas
/// DataFrame iterator and must yield pandas DataFrames.
///
/// This mirrors PySpark's `mapInPandas` / Flink Python DataStream `map`.
pub trait MapPandasIterUdf: Send + Sync + fmt::Debug {
    /// Unique name used to look up this UDF in a [`UdfRegistry`].
    fn name(&self) -> &str;

    /// Schema of the input batches.
    fn input_schema(&self) -> &Schema;

    /// Schema of the output batches.
    fn output_schema(&self) -> &Schema;

    /// Process the provided `batches` and return all output batches.
    ///
    /// Implementations may buffer or emit eagerly.
    fn map_batches(&self, batches: &[RecordBatch]) -> Result<Vec<RecordBatch>, UdfError>;
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Runtime registry that maps names to registered UDFs.
#[derive(Debug, Default)]
pub struct UdfRegistry {
    scalars: HashMap<String, Arc<dyn ScalarUdf>>,
    aggregates: HashMap<String, Arc<dyn AggregateUdf>>,
    tables: HashMap<String, Arc<dyn TableUdf>>,
    co_groups: HashMap<String, Arc<dyn CoGroupUdf>>,
    map_pandas_iters: HashMap<String, Arc<dyn MapPandasIterUdf>>,
}

impl UdfRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a scalar UDF; replaces any existing registration with the same
    /// name.
    pub fn register_scalar(&mut self, udf: Arc<dyn ScalarUdf>) {
        self.scalars.insert(udf.name().to_owned(), udf);
    }

    /// Remove and return a scalar UDF registration by name.
    pub fn remove_scalar(&mut self, name: &str) -> Option<Arc<dyn ScalarUdf>> {
        self.scalars.remove(name)
    }

    /// Register an aggregate UDAF; replaces any existing registration with the
    /// same name.
    pub fn register_aggregate(&mut self, udf: Arc<dyn AggregateUdf>) {
        self.aggregates.insert(udf.name().to_owned(), udf);
    }

    /// Register a table UDTF; replaces any existing registration with the same
    /// name.
    pub fn register_table(&mut self, udf: Arc<dyn TableUdf>) {
        self.tables.insert(udf.name().to_owned(), udf);
    }

    /// Register a co-group map UDF; replaces any existing registration with
    /// the same name.
    pub fn register_co_group(&mut self, udf: Arc<dyn CoGroupUdf>) {
        self.co_groups.insert(udf.name().to_owned(), udf);
    }

    /// Register a map-pandas-iter UDF; replaces any existing registration with
    /// the same name.
    pub fn register_map_pandas_iter(&mut self, udf: Arc<dyn MapPandasIterUdf>) {
        self.map_pandas_iters.insert(udf.name().to_owned(), udf);
    }

    /// Look up a scalar UDF by name.
    pub fn get_scalar(&self, name: &str) -> Option<&Arc<dyn ScalarUdf>> {
        self.scalars.get(name)
    }

    /// Look up an aggregate UDAF by name.
    pub fn get_aggregate(&self, name: &str) -> Option<&Arc<dyn AggregateUdf>> {
        self.aggregates.get(name)
    }

    /// Look up a table UDTF by name.
    pub fn get_table(&self, name: &str) -> Option<&Arc<dyn TableUdf>> {
        self.tables.get(name)
    }

    /// Look up a co-group map UDF by name.
    pub fn get_co_group(&self, name: &str) -> Option<&Arc<dyn CoGroupUdf>> {
        self.co_groups.get(name)
    }

    /// Look up a map-pandas-iter UDF by name.
    pub fn get_map_pandas_iter(&self, name: &str) -> Option<&Arc<dyn MapPandasIterUdf>> {
        self.map_pandas_iters.get(name)
    }

    /// Return the names of all registered scalar UDFs.
    pub fn scalar_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.scalars.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    }

    /// Return the names of all registered aggregate UDAFs.
    pub fn aggregate_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.aggregates.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    }

    /// Return the names of all registered table UDTFs.
    pub fn table_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.tables.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    }

    /// Return the names of all registered co-group map UDFs.
    pub fn co_group_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.co_groups.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    }

    /// Return the names of all registered map-pandas-iter UDFs.
    pub fn map_pandas_iter_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.map_pandas_iters.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    }

    /// Execute a scalar UDF with resource limits enforced using the provided executor.
    pub fn execute_scalar_with_limits(
        &self,
        name: &str,
        batch: &RecordBatch,
        limits: &ResourceLimits,
        executor: &dyn SandboxedUdfExecutor,
    ) -> Result<ArrayRef, UdfError> {
        let udf = self
            .get_scalar(name)
            .ok_or_else(|| UdfError::InvalidArgument {
                message: format!("unknown scalar UDF: {}", name),
            })?;
        executor.execute_with_limits(udf.as_ref(), batch, limits)
    }
}

// ---------------------------------------------------------------------------
// Concrete example: MultiplyScalarUdf
// ---------------------------------------------------------------------------

/// A concrete [`ScalarUdf`] that multiplies an Int64 column by a constant
/// factor.  Intended as a testable reference implementation.
#[derive(Debug)]
pub struct MultiplyScalarUdf {
    name: String,
    column: String,
    factor: i64,
    input_schema: Schema,
    output_field: Field,
}

impl MultiplyScalarUdf {
    /// Create a new `MultiplyScalarUdf`.
    ///
    /// * `name`   – registry name.
    /// * `column` – name of the Int64 input column.
    /// * `factor` – constant multiplier.
    pub fn new(name: impl Into<String>, column: impl Into<String>, factor: i64) -> Self {
        let column: String = column.into();
        let input_schema = Schema::new(vec![Field::new(column.clone(), DataType::Int64, true)]);
        let output_field = Field::new("result", DataType::Int64, true);
        Self {
            name: name.into(),
            column,
            factor,
            input_schema,
            output_field,
        }
    }
}

impl ScalarUdf for MultiplyScalarUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> &Schema {
        &self.input_schema
    }

    fn output_field(&self) -> &Field {
        &self.output_field
    }

    fn call(&self, batch: &RecordBatch) -> Result<ArrayRef, UdfError> {
        let col_idx =
            batch
                .schema()
                .index_of(&self.column)
                .map_err(|_| UdfError::InvalidArgument {
                    message: format!("column '{}' not found in batch", self.column),
                })?;

        let array = batch.column(col_idx);
        let int_array = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
            UdfError::InvalidArgument {
                message: format!("column '{}' is not Int64", self.column),
            }
        })?;

        let factor = self.factor;
        let result =
            arrow::compute::kernels::arity::unary::<Int64Type, _, Int64Type>(int_array, |x| {
                x.wrapping_mul(factor)
            });

        Ok(Arc::new(result))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    /// Read an i64 from an 8-byte little-endian AggState, returning 0 for empty/corrupt state.
    fn read_i64_state(state: &AggState) -> i64 {
        if state.data.len() == 8 {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&state.data[..8]);
            i64::from_le_bytes(buf)
        } else {
            0
        }
    }

    // -----------------------------------------------------------------------
    // Mock aggregate UDF
    // -----------------------------------------------------------------------

    /// Accumulates Int64 values by summing them; state is a little-endian i64.
    #[derive(Debug)]
    struct SumAggUdf {
        input_schema: Schema,
        output_field: Field,
    }

    impl SumAggUdf {
        fn new() -> Self {
            let input_schema = Schema::new(vec![Field::new("value", DataType::Int64, true)]);
            let output_field = Field::new("sum", DataType::Int64, false);
            Self {
                input_schema,
                output_field,
            }
        }
    }

    impl AggregateUdf for SumAggUdf {
        fn name(&self) -> &str {
            "sum_agg"
        }

        fn input_schema(&self) -> &Schema {
            &self.input_schema
        }

        fn output_field(&self) -> &Field {
            &self.output_field
        }

        fn accumulate(&self, state: &mut AggState, batch: &RecordBatch) -> Result<(), UdfError> {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| UdfError::InvalidArgument {
                    message: "expected Int64".into(),
                })?;

            let mut current: i64 = read_i64_state(&state);

            for v in col.iter().flatten() {
                current += v;
            }
            state.data = current.to_le_bytes().to_vec();
            Ok(())
        }

        fn finalize(&self, state: AggState) -> Result<ScalarValue, UdfError> {
            Ok(ScalarValue::Int64(read_i64_state(&state)))
        }

        fn merge(&self, a: AggState, b: AggState) -> Result<AggState, UdfError> {
            Ok(AggState {
                data: (read_i64_state(&a) + read_i64_state(&b))
                    .to_le_bytes()
                    .to_vec(),
            })
        }
    }

    // -----------------------------------------------------------------------
    // Mock table UDF
    // -----------------------------------------------------------------------

    /// Returns a single-row batch with a constant Int64 column.
    #[derive(Debug)]
    struct ConstantTableUdf {
        schema: Schema,
        value: i64,
    }

    impl ConstantTableUdf {
        fn new(value: i64) -> Self {
            let schema = Schema::new(vec![Field::new("constant", DataType::Int64, false)]);
            Self { schema, value }
        }
    }

    impl TableUdf for ConstantTableUdf {
        fn name(&self) -> &str {
            "constant_table"
        }

        fn output_schema(&self) -> &Schema {
            &self.schema
        }

        fn call(&self, _args: &[ScalarValue]) -> Result<RecordBatch, UdfError> {
            let array = Int64Array::from(vec![self.value]);
            RecordBatch::try_new(Arc::new(self.schema.clone()), vec![Arc::new(array)])
                .map_err(UdfError::from)
        }
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[test]
    fn scalar_udf_registry_round_trip() {
        let mut registry = UdfRegistry::new();
        let udf = Arc::new(MultiplyScalarUdf::new("double", "x", 2));
        registry.register_scalar(udf);

        let found = registry
            .get_scalar("double")
            .expect("UDF must be registered");
        assert_eq!(found.name(), "double");

        // Build a batch with column "x" = [1, 2, 3]
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let array = Int64Array::from(vec![1_i64, 2, 3]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("valid batch");

        let result = found.call(&batch).expect("call must succeed");
        let result_array = result
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("result must be Int64");

        assert_eq!(result_array.len(), 3);
        assert_eq!(result_array.value(0), 2);
        assert_eq!(result_array.value(1), 4);
        assert_eq!(result_array.value(2), 6);
    }

    #[test]
    fn aggregate_udf_state_lifecycle() {
        let udf = SumAggUdf::new();

        // Build a batch with values [10, 20]
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let array = Int64Array::from(vec![10_i64, 20]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("valid batch");

        let mut state = AggState::default();
        udf.accumulate(&mut state, &batch).expect("accumulate ok");

        let result = udf.finalize(state).expect("finalize ok");
        match result {
            ScalarValue::Int64(v) => assert_eq!(v, 30),
            other => panic!("unexpected ScalarValue: {other:?}"),
        }
    }

    #[test]
    fn udf_error_display() {
        let e1 = UdfError::Arrow("bad array".to_owned());
        assert!(e1.to_string().contains("Arrow error"));
        assert!(e1.to_string().contains("bad array"));

        let e2 = UdfError::Execution {
            message: "runtime fault".to_owned(),
        };
        assert!(e2.to_string().contains("Execution error"));
        assert!(e2.to_string().contains("runtime fault"));

        let e3 = UdfError::Panic("thread panicked".to_owned());
        assert!(e3.to_string().contains("Panic"));
        assert!(e3.to_string().contains("thread panicked"));

        let e4 = UdfError::InvalidArgument {
            message: "wrong type".to_owned(),
        };
        assert!(e4.to_string().contains("Invalid argument"));
        assert!(e4.to_string().contains("wrong type"));
    }

    #[test]
    fn registry_scalar_names_returns_registered_names() {
        let mut registry = UdfRegistry::new();
        registry.register_scalar(Arc::new(MultiplyScalarUdf::new("triple", "v", 3)));
        registry.register_scalar(Arc::new(MultiplyScalarUdf::new("quadruple", "v", 4)));

        let names = registry.scalar_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"triple"));
        assert!(names.contains(&"quadruple"));
    }

    #[test]
    fn table_udf_produces_record_batch() {
        let mut registry = UdfRegistry::new();
        let udtf = Arc::new(ConstantTableUdf::new(42));
        registry.register_table(udtf);

        let found = registry
            .get_table("constant_table")
            .expect("UDTF must be registered");

        let batch = found.call(&[]).expect("call must succeed");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema().field(0).name(), "constant");

        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64");
        assert_eq!(col.value(0), 42);
    }

    /// Verifies that a two-phase distributed UDAF merge produces the same
    /// result as a single-partition aggregation over the concatenated data.
    ///
    /// Phase 1: each partition accumulates its own partial [`AggState`].
    /// Phase 2: the partial states are merged via [`AggregateUdf::merge`].
    /// The merged state is finalised and compared against a single-pass result
    /// computed over all data in one shot.
    #[test]
    fn udaf_distributed_merge_matches_single_partition() {
        let udf = SumAggUdf::new();

        // -----------------------------------------------------------------
        // Build two partitions with known values.
        //   partition_a : [1, 2, 3, 4]  -> partial sum = 10
        //   partition_b : [5, 6, 7]     -> partial sum = 18
        //   combined                    -> total sum   = 28
        // -----------------------------------------------------------------
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));

        let partition_a = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4]))],
        )
        .expect("valid partition_a batch");

        let partition_b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![5_i64, 6, 7]))],
        )
        .expect("valid partition_b batch");

        // -----------------------------------------------------------------
        // Phase 1 – accumulate each partition independently.
        // -----------------------------------------------------------------
        let mut state_a = AggState::default();
        udf.accumulate(&mut state_a, &partition_a)
            .expect("accumulate partition_a");

        let mut state_b = AggState::default();
        udf.accumulate(&mut state_b, &partition_b)
            .expect("accumulate partition_b");

        // Sanity-check the partial sums before merging.
        let partial_a = udf
            .finalize(AggState {
                data: state_a.data.clone(),
            })
            .expect("finalize partial_a");
        let partial_b = udf
            .finalize(AggState {
                data: state_b.data.clone(),
            })
            .expect("finalize partial_b");
        assert!(
            matches!(partial_a, ScalarValue::Int64(10)),
            "partial sum of partition_a must be 10, got {partial_a:?}",
        );
        assert!(
            matches!(partial_b, ScalarValue::Int64(18)),
            "partial sum of partition_b must be 18, got {partial_b:?}",
        );

        // -----------------------------------------------------------------
        // Phase 2 – merge the two partial states.
        // -----------------------------------------------------------------
        let merged_state = udf.merge(state_a, state_b).expect("merge partial states");

        // -----------------------------------------------------------------
        // Finalise the merged state (distributed path result).
        // -----------------------------------------------------------------
        let distributed_result = udf.finalize(merged_state).expect("finalize merged state");

        // -----------------------------------------------------------------
        // Reference path: accumulate all rows in a single pass.
        // -----------------------------------------------------------------
        let all_values = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5, 6, 7]))],
        )
        .expect("valid all-values batch");

        let mut single_state = AggState::default();
        udf.accumulate(&mut single_state, &all_values)
            .expect("accumulate single partition");
        let single_result = udf
            .finalize(single_state)
            .expect("finalize single-partition state");

        // -----------------------------------------------------------------
        // Both paths must produce the same result (28).
        // -----------------------------------------------------------------
        assert!(
            matches!(distributed_result, ScalarValue::Int64(28)),
            "distributed merge must produce 28, got {distributed_result:?}",
        );
        assert!(
            matches!(single_result, ScalarValue::Int64(28)),
            "single-partition path must produce 28, got {single_result:?}",
        );

        // Also compare as i64 values for a cleaner assertion.
        let distributed_val = match distributed_result {
            ScalarValue::Int64(v) => v,
            other => panic!("expected Int64, got {other:?}"),
        };
        let single_val = match single_result {
            ScalarValue::Int64(v) => v,
            other => panic!("expected Int64, got {other:?}"),
        };
        assert_eq!(
            distributed_val, single_val,
            "distributed merge ({distributed_val}) must equal single-partition result ({single_val})",
        );
    }

    /// Verifies that merging with an empty (default) partial state is a
    /// no-op, so a partition that contributes zero rows does not corrupt
    /// the merged total.
    #[test]
    fn udaf_merge_with_empty_state_is_noop() {
        let udf = SumAggUdf::new();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));

        let partition = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![10_i64, 20, 30]))],
        )
        .expect("valid partition batch");

        let mut non_empty_state = AggState::default();
        udf.accumulate(&mut non_empty_state, &partition)
            .expect("accumulate");

        // Merge with an uninitialised (empty) state on the right.
        let merged_right = udf
            .merge(
                AggState {
                    data: non_empty_state.data.clone(),
                },
                AggState::default(),
            )
            .expect("merge with empty right");

        // Merge with an uninitialised (empty) state on the left.
        let merged_left = udf
            .merge(
                AggState::default(),
                AggState {
                    data: non_empty_state.data.clone(),
                },
            )
            .expect("merge with empty left");

        let result_right = udf.finalize(merged_right).expect("finalize right merge");
        let result_left = udf.finalize(merged_left).expect("finalize left merge");

        assert!(
            matches!(result_right, ScalarValue::Int64(60)),
            "merge with empty right must yield 60, got {result_right:?}",
        );
        assert!(
            matches!(result_left, ScalarValue::Int64(60)),
            "merge with empty left must yield 60, got {result_left:?}",
        );
    }

    /// Verifies that merging three partial states in sequence (simulating a
    /// three-partition distributed job) still yields the correct total.
    #[test]
    fn udaf_merge_three_partitions() {
        let udf = SumAggUdf::new();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));

        // Partition values and expected partial sums:
        //   p1 : [100]        -> 100
        //   p2 : [200, 300]   -> 500
        //   p3 : [400, 500, 600] -> 1500
        //   total             -> 2100
        let make_batch = |vals: Vec<i64>| {
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(vals))])
                .expect("valid batch")
        };

        let mut s1 = AggState::default();
        let mut s2 = AggState::default();
        let mut s3 = AggState::default();

        udf.accumulate(&mut s1, &make_batch(vec![100]))
            .expect("acc p1");
        udf.accumulate(&mut s2, &make_batch(vec![200, 300]))
            .expect("acc p2");
        udf.accumulate(&mut s3, &make_batch(vec![400, 500, 600]))
            .expect("acc p3");

        // Merge left-to-right: ((s1 merge s2) merge s3)
        let m12 = udf.merge(s1, s2).expect("merge s1+s2");
        let m123 = udf.merge(m12, s3).expect("merge (s1+s2)+s3");

        let result = udf.finalize(m123).expect("finalize three-partition merge");

        assert!(
            matches!(result, ScalarValue::Int64(2100)),
            "three-partition merge must yield 2100, got {result:?}",
        );
    }

    // ── Additional deep-coverage tests ─────────────────────────────────

    #[test]
    fn multiply_scalar_negative_factor() {
        let udf = MultiplyScalarUdf::new("neg", "x", -3);
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let array = Int64Array::from(vec![2_i64, -5, 0]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let result = udf.call(&batch).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), -6);
        assert_eq!(arr.value(1), 15);
        assert_eq!(arr.value(2), 0);
    }

    #[test]
    fn multiply_scalar_zero_factor() {
        let udf = MultiplyScalarUdf::new("zero", "x", 0);
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let array = Int64Array::from(vec![100_i64, 200]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let result = udf.call(&batch).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 0);
        assert_eq!(arr.value(1), 0);
    }

    #[test]
    fn multiply_scalar_one_factor() {
        let udf = MultiplyScalarUdf::new("id", "x", 1);
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let array = Int64Array::from(vec![42_i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let result = udf.call(&batch).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 42);
    }

    #[test]
    fn multiply_scalar_large_values() {
        let udf = MultiplyScalarUdf::new("large", "x", 2);
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let array = Int64Array::from(vec![i64::MAX / 2, i64::MIN / 2]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let result = udf.call(&batch).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), i64::MAX / 2 * 2);
        assert_eq!(arr.value(1), i64::MIN / 2 * 2);
    }

    #[test]
    fn multiply_scalar_empty_batch() {
        let udf = MultiplyScalarUdf::new("empty", "x", 5);
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let array = Int64Array::from(Vec::<i64>::new());
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let result = udf.call(&batch).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.len(), 0);
    }

    #[test]
    fn multiply_scalar_column_not_found() {
        let udf = MultiplyScalarUdf::new("m", "missing_col", 1);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            true,
        )]));
        let array = Int64Array::from(vec![1_i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let err = udf.call(&batch).unwrap_err();
        assert!(matches!(err, UdfError::InvalidArgument { .. }));
        assert!(err.to_string().contains("missing_col"));
    }

    #[test]
    fn multiply_scalar_wrong_type_column() {
        let udf = MultiplyScalarUdf::new("m", "x", 1);
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
        let array = arrow::array::StringArray::from(vec!["hello"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let err = udf.call(&batch).unwrap_err();
        assert!(matches!(err, UdfError::InvalidArgument { .. }));
        assert!(err.to_string().contains("not Int64"));
    }

    #[test]
    fn multiply_scalar_null_values() {
        let udf = MultiplyScalarUdf::new("m", "x", 10);
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let mut builder = arrow::array::Int64Builder::new();
        builder.append_value(5);
        builder.append_null();
        builder.append_value(3);
        let array = builder.finish();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let result = udf.call(&batch).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 50);
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), 30);
    }

    #[test]
    fn multiply_scalar_output_schema() {
        let udf = MultiplyScalarUdf::new("m", "input", 2);
        assert_eq!(udf.output_field().name(), "result");
        assert_eq!(udf.output_field().data_type(), &DataType::Int64);
    }

    #[test]
    fn multiply_scalar_input_schema() {
        let udf = MultiplyScalarUdf::new("m", "my_col", 1);
        let schema = udf.input_schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "my_col");
    }

    #[test]
    fn udf_registry_scalar_override() {
        let mut registry = UdfRegistry::new();
        registry.register_scalar(Arc::new(MultiplyScalarUdf::new("f", "x", 2)));
        registry.register_scalar(Arc::new(MultiplyScalarUdf::new("f", "x", 3)));
        let udf = registry.get_scalar("f").unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let array = Int64Array::from(vec![1_i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let result = udf.call(&batch).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 3); // factor=3 wins
    }

    #[test]
    fn udf_registry_aggregate_override() {
        let mut registry = UdfRegistry::new();
        registry.register_aggregate(Arc::new(SumAggUdf::new()));
        // Registering same name replaces
        registry.register_aggregate(Arc::new(SumAggUdf::new()));
        assert_eq!(registry.aggregate_names().len(), 1);
    }

    #[test]
    fn udf_registry_table_override() {
        let mut registry = UdfRegistry::new();
        registry.register_table(Arc::new(ConstantTableUdf::new(1)));
        registry.register_table(Arc::new(ConstantTableUdf::new(2)));
        assert_eq!(registry.table_names().len(), 1);
        let udf = registry.get_table("constant_table").unwrap();
        let batch = udf.call(&[]).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 2);
    }

    #[test]
    fn udf_registry_missing_scalar_returns_none() {
        let registry = UdfRegistry::new();
        assert!(registry.get_scalar("nonexistent").is_none());
    }

    #[test]
    fn udf_registry_missing_aggregate_returns_none() {
        let registry = UdfRegistry::new();
        assert!(registry.get_aggregate("nonexistent").is_none());
    }

    #[test]
    fn udf_registry_missing_table_returns_none() {
        let registry = UdfRegistry::new();
        assert!(registry.get_table("nonexistent").is_none());
    }

    #[test]
    fn udf_registry_empty_names() {
        let registry = UdfRegistry::new();
        assert!(registry.scalar_names().is_empty());
        assert!(registry.aggregate_names().is_empty());
        assert!(registry.table_names().is_empty());
    }

    #[test]
    fn udf_registry_multiple_scalars_sorted() {
        let mut registry = UdfRegistry::new();
        registry.register_scalar(Arc::new(MultiplyScalarUdf::new("z", "x", 1)));
        registry.register_scalar(Arc::new(MultiplyScalarUdf::new("a", "x", 1)));
        registry.register_scalar(Arc::new(MultiplyScalarUdf::new("m", "x", 1)));
        let names = registry.scalar_names();
        assert_eq!(names, vec!["a", "m", "z"]);
    }

    #[test]
    fn udf_registry_remove_scalar_returns_registration() {
        let mut registry = UdfRegistry::new();
        registry.register_scalar(Arc::new(MultiplyScalarUdf::new("double", "x", 2)));

        let removed = registry
            .remove_scalar("double")
            .expect("registered scalar should be returned");

        assert_eq!(removed.name(), "double");
        assert!(registry.get_scalar("double").is_none());
    }

    #[test]
    fn udf_registry_multiple_aggregates_sorted() {
        let mut registry = UdfRegistry::new();
        registry.register_aggregate(Arc::new(SumAggUdf::new()));
        // Register with different name by using a wrapper (reuse SumAggUdf)
        let names = registry.aggregate_names();
        assert_eq!(names, vec!["sum_agg"]);
    }

    #[test]
    fn udf_registry_multiple_tables_sorted() {
        let mut registry = UdfRegistry::new();
        registry.register_table(Arc::new(ConstantTableUdf::new(1)));
        let names = registry.table_names();
        assert_eq!(names, vec!["constant_table"]);
    }

    #[test]
    fn aggregate_empty_batch_finalize() {
        let udf = SumAggUdf::new();
        let state = AggState::default();
        let result = udf.finalize(state).unwrap();
        assert!(matches!(result, ScalarValue::Int64(0)));
    }

    #[test]
    fn aggregate_single_value() {
        let udf = SumAggUdf::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let array = Int64Array::from(vec![42_i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let mut state = AggState::default();
        udf.accumulate(&mut state, &batch).unwrap();
        let result = udf.finalize(state).unwrap();
        assert!(matches!(result, ScalarValue::Int64(42)));
    }

    #[test]
    fn aggregate_negative_values() {
        let udf = SumAggUdf::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let array = Int64Array::from(vec![-10_i64, -20, -30]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let mut state = AggState::default();
        udf.accumulate(&mut state, &batch).unwrap();
        let result = udf.finalize(state).unwrap();
        assert!(matches!(result, ScalarValue::Int64(-60)));
    }

    #[test]
    fn aggregate_mixed_positive_negative() {
        let udf = SumAggUdf::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let array = Int64Array::from(vec![-5_i64, 10, -3, 8]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let mut state = AggState::default();
        udf.accumulate(&mut state, &batch).unwrap();
        let result = udf.finalize(state).unwrap();
        assert!(matches!(result, ScalarValue::Int64(10)));
    }

    #[test]
    fn aggregate_multiple_accumulations() {
        let udf = SumAggUdf::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let b1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![3_i64, 4]))],
        )
        .unwrap();
        let mut state = AggState::default();
        udf.accumulate(&mut state, &b1).unwrap();
        udf.accumulate(&mut state, &b2).unwrap();
        let result = udf.finalize(state).unwrap();
        assert!(matches!(result, ScalarValue::Int64(10)));
    }

    #[test]
    fn aggregate_wrong_type_in_batch() {
        let udf = SumAggUdf::new();
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let array = arrow::array::StringArray::from(vec!["hello"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let mut state = AggState::default();
        let err = udf.accumulate(&mut state, &batch).unwrap_err();
        assert!(matches!(err, UdfError::InvalidArgument { .. }));
    }

    #[test]
    fn aggregate_name_and_schemas() {
        let udf = SumAggUdf::new();
        assert_eq!(udf.name(), "sum_agg");
        assert_eq!(udf.input_schema().fields().len(), 1);
        assert_eq!(udf.output_field().name(), "sum");
    }

    #[test]
    fn table_udf_name_and_schema() {
        let udf = ConstantTableUdf::new(99);
        assert_eq!(udf.name(), "constant_table");
        assert_eq!(udf.output_schema().fields().len(), 1);
        assert_eq!(udf.output_schema().field(0).name(), "constant");
    }

    #[test]
    fn table_udf_ignores_args() {
        let udf = ConstantTableUdf::new(7);
        let args = vec![
            ScalarValue::Int64(1),
            ScalarValue::Utf8("hello".into()),
            ScalarValue::Boolean(true),
        ];
        let batch = udf.call(&args).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 7);
    }

    #[test]
    fn scalar_value_variants() {
        let null = ScalarValue::Null;
        let int = ScalarValue::Int64(42);
        let float = ScalarValue::Float64(3.15);
        let utf8 = ScalarValue::Utf8("hello".into());
        let bool = ScalarValue::Boolean(true);
        let bytes = ScalarValue::Bytes(vec![1, 2, 3]);

        assert!(format!("{:?}", null).contains("Null"));
        assert!(format!("{:?}", int).contains("42"));
        assert!(format!("{:?}", float).contains("3.15"));
        assert!(format!("{:?}", utf8).contains("hello"));
        assert!(format!("{:?}", bool).contains("true"));
        assert!(format!("{:?}", bytes).contains("Bytes"));
    }

    #[test]
    fn scalar_value_clone() {
        let v = ScalarValue::Utf8("test".into());
        let c = v.clone();
        assert!(matches!(c, ScalarValue::Utf8(s) if s == "test"));
    }

    #[test]
    fn agg_state_default_is_empty() {
        let s = AggState::default();
        assert!(s.data.is_empty());
    }

    #[test]
    fn agg_state_debug() {
        let s = AggState {
            data: vec![1, 2, 3],
        };
        let debug = format!("{:?}", s);
        assert!(debug.contains("1, 2, 3"));
    }

    #[test]
    fn udf_error_is_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(UdfError::Arrow("test".into()));
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn arrow_error_conversion() {
        let arrow_err = arrow::error::ArrowError::InvalidArgumentError("bad".into());
        let udf_err: UdfError = arrow_err.into();
        assert!(matches!(udf_err, UdfError::Arrow(_)));
        assert!(udf_err.to_string().contains("bad"));
    }

    #[test]
    fn multiply_scalar_large_batch() {
        let udf = MultiplyScalarUdf::new("big", "x", 7);
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let values: Vec<i64> = (0..10000).collect();
        let array = Int64Array::from(values);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let result = udf.call(&batch).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.len(), 10000);
        assert_eq!(arr.value(0), 0);
        assert_eq!(arr.value(1), 7);
        assert_eq!(arr.value(9999), 9999 * 7);
    }

    #[test]
    fn registry_new_is_empty() {
        let registry = UdfRegistry::new();
        assert!(registry.scalar_names().is_empty());
        assert!(registry.aggregate_names().is_empty());
        assert!(registry.table_names().is_empty());
    }

    #[test]
    fn registry_default_is_empty() {
        let registry = UdfRegistry::default();
        assert!(registry.scalar_names().is_empty());
    }

    #[test]
    fn aggregate_merge_symmetric() {
        let udf = SumAggUdf::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let b1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![10_i64]))],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![20_i64]))],
        )
        .unwrap();

        let mut s1 = AggState::default();
        let mut s2 = AggState::default();
        udf.accumulate(&mut s1, &b1).unwrap();
        udf.accumulate(&mut s2, &b2).unwrap();

        let m12 = udf.merge(s1.clone(), s2.clone()).unwrap();
        let m21 = udf.merge(s2, s1).unwrap();

        let r12 = udf.finalize(m12).unwrap();
        let r21 = udf.finalize(m21).unwrap();

        assert!(matches!(r12, ScalarValue::Int64(30)));
        assert!(matches!(r21, ScalarValue::Int64(30)));
    }
}

// ============================================================================
// UDF Resource Limiting + Sandbox Hooks
// ============================================================================

/// Resource limits for UDF execution.
///
/// **Memory limits (M6 limitation):** The memory check uses a conservative proxy
/// of input and output batch size in bytes. This is not a true heap limit and does
/// not catch UDFs that allocate large intermediate structures during execution.
/// A production implementation would use a custom allocator or cgroup limits.
///
/// **Time limits:** Currently checked post-hoc after the UDF returns. A preemptive
/// timeout using `tokio::time::timeout` or `std::thread` with a join timeout is
/// planned for R9+ to prevent resource exhaustion from hung UDFs.
#[derive(Clone, Debug, Default)]
pub struct ResourceLimits {
    pub max_memory_bytes: Option<u64>,
    pub max_execution_time_ms: Option<u64>,
}

/// Real trait for sandboxed UDF execution with enforcement.
pub trait SandboxedUdfExecutor: Send + Sync {
    fn execute_with_limits(
        &self,
        udf: &dyn ScalarUdf,
        batch: &RecordBatch,
        limits: &ResourceLimits,
    ) -> Result<ArrayRef, UdfError>;
}

/// Concrete real implementation that enforces limits (using timeout for time).
pub struct DefaultSandboxedExecutor;

impl SandboxedUdfExecutor for DefaultSandboxedExecutor {
    fn execute_with_limits(
        &self,
        udf: &dyn ScalarUdf,
        batch: &RecordBatch,
        limits: &ResourceLimits,
    ) -> Result<ArrayRef, UdfError> {
        if krishiv_common::profile_forbids_native_scalar_udfs(
            krishiv_common::resolve_durability_profile(),
        ) {
            return Err(UdfError::Execution {
                message: String::from(
                    "native UDF execution runs with full process privileges; under durable \
                     profiles use LANGUAGE sql UDFs or set KRISHIV_ALLOW_FULL_PRIVILEGE_UDFS=1",
                ),
            });
        }
        let start = std::time::Instant::now();

        // Catch panics in user-supplied UDF bodies so a buggy or hostile UDF
        // cannot take down the DataFusion query plan (and the calling
        // process). `UdfError::Panic` exists precisely for this case; the
        // previous implementation let panics propagate and crash the worker.
        // `AssertUnwindSafe` is sound here because we only re-throw through
        // the typed error — we never observe the panic payload.
        let result =
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| udf.call(batch))) {
                Ok(Ok(array)) => array,
                Ok(Err(error)) => return Err(error),
                Err(payload) => {
                    let message = krishiv_common::panic_payload_to_string(&*payload);
                    return Err(UdfError::Panic(format!(
                        "UDF '{}' panicked during execution: {}",
                        udf.name(),
                        message
                    )));
                }
            };

        if let Some(max_ms) = limits.max_execution_time_ms
            && start.elapsed().as_millis() as u64 > max_ms
        {
            return Err(UdfError::Execution {
                message: format!("UDF exceeded time limit of {} ms", max_ms),
            });
        }

        // Real memory enforcement (conservative proxy using input batch size in bytes).
        // This is the live path; a production implementation would use a custom allocator
        // or cgroups limit. The check prevents obviously oversized work from proceeding.
        if let Some(max_bytes) = limits.max_memory_bytes {
            let approx_bytes: usize = batch
                .columns()
                .iter()
                .map(|c| c.get_array_memory_size())
                .sum();
            if approx_bytes as u64 > max_bytes {
                return Err(UdfError::Execution {
                    message: format!(
                        "UDF input exceeded memory limit of {} bytes (approx {} bytes)",
                        max_bytes, approx_bytes
                    ),
                });
            }

            // Also check output size: UDFs can materialize large intermediate structures.
            let output_size: usize = result.get_array_memory_size();
            if output_size as u64 > max_bytes {
                return Err(UdfError::Execution {
                    message: format!(
                        "UDF output exceeded memory limit of {} bytes (approx {} bytes)",
                        max_bytes, output_size
                    ),
                });
            }
        }

        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Focused test for memory limit enforcement (Track E)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod memory_enforcement_tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// A deliberately heavy scalar UDF for testing memory limits.
    /// It simply returns the input column but forces materialization of a large
    /// intermediate structure in a real implementation. For the test we use a
    /// normal UDF but feed it an input whose Arrow size exceeds the tiny limit.
    #[derive(Debug)]
    struct IdentityHeavyUdf {
        name: String,
        schema: Schema,
    }

    impl IdentityHeavyUdf {
        fn new() -> Self {
            let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
            Self {
                name: "identity_heavy".to_string(),
                schema,
            }
        }
    }

    impl ScalarUdf for IdentityHeavyUdf {
        fn name(&self) -> &str {
            &self.name
        }
        fn input_schema(&self) -> &Schema {
            &self.schema
        }
        fn output_field(&self) -> &Field {
            self.schema.field(0)
        }
        fn call(&self, batch: &RecordBatch) -> Result<ArrayRef, UdfError> {
            // Return the first column (simple identity for test purposes).
            Ok(batch.column(0).clone())
        }
    }

    #[test]
    fn default_sandboxed_executor_enforces_memory_limit() {
        let mut registry = UdfRegistry::new();
        let udf = Arc::new(IdentityHeavyUdf::new());
        registry.register_scalar(udf.clone());

        // Create a small but non-trivial batch whose Arrow size we can exceed with a tiny limit.
        let col = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)])),
            vec![Arc::new(col)],
        )
        .unwrap();

        let executor = DefaultSandboxedExecutor;

        // With a very tight limit (1 byte) the enforcement must fire.
        let limits = ResourceLimits {
            max_memory_bytes: Some(1),
            max_execution_time_ms: None,
        };

        let err = registry
            .execute_scalar_with_limits("identity_heavy", &batch, &limits, &executor)
            .unwrap_err();

        match err {
            UdfError::Execution { message } => {
                assert!(
                    message.contains("exceeded memory limit"),
                    "expected memory limit error, got: {}",
                    message
                );
            }
            other => panic!("expected Execution error, got {:?}", other),
        }
    }

    #[derive(Debug)]
    struct PanickingUdf;

    impl ScalarUdf for PanickingUdf {
        fn name(&self) -> &str {
            "panicking_udf"
        }
        fn input_schema(&self) -> &Schema {
            static SCHEMA: std::sync::OnceLock<Schema> = std::sync::OnceLock::new();
            SCHEMA.get_or_init(|| Schema::new(vec![Field::new("x", DataType::Int64, true)]))
        }
        fn output_field(&self) -> &Field {
            static FIELD: std::sync::OnceLock<Field> = std::sync::OnceLock::new();
            FIELD.get_or_init(|| Field::new("x", DataType::Int64, true))
        }
        fn call(&self, _batch: &RecordBatch) -> Result<ArrayRef, UdfError> {
            panic!("deliberate test panic: kaboom");
        }
    }

    #[test]
    fn default_sandboxed_executor_catches_udf_panic() {
        let mut registry = UdfRegistry::new();
        registry.register_scalar(Arc::new(PanickingUdf));
        let executor = DefaultSandboxedExecutor;
        let limits = ResourceLimits::default();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let err = registry
            .execute_scalar_with_limits("panicking_udf", &batch, &limits, &executor)
            .unwrap_err();

        match err {
            UdfError::Panic(message) => {
                assert!(
                    message.contains("panicking_udf"),
                    "udf name in error: {message}"
                );
                assert!(
                    message.contains("kaboom"),
                    "panic message in error: {message}"
                );
            }
            other => panic!("expected Panic error, got {other:?}"),
        }
    }

    #[test]
    fn panic_message_extracts_str_payload() {
        let payload: Box<dyn std::any::Any + Send> = Box::new("static str payload");
        assert_eq!(
            krishiv_common::panic_payload_to_string(&*payload),
            "static str payload"
        );
    }

    #[test]
    fn panic_message_extracts_string_payload() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(String::from("owned payload"));
        assert_eq!(
            krishiv_common::panic_payload_to_string(&*payload),
            "owned payload"
        );
    }

    #[test]
    fn panic_message_falls_back_for_unknown_payloads() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(42u32);
        assert_eq!(
            krishiv_common::panic_payload_to_string(&*payload),
            "non-string panic payload"
        );
    }
}
