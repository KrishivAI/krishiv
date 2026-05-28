#![forbid(unsafe_code)]

//! User-defined function (UDF) extension framework for Krishiv.
//!
//! Provides stable Rust contracts for scalar UDFs, aggregate UDAFs, and
//! table-valued UDTFs, along with a runtime registry.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, PrimitiveArray};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use arrow::record_batch::RecordBatch;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can occur during UDF execution.
#[derive(Debug)]
pub enum UdfError {
    /// An error originating from the Arrow library.
    Arrow(String),
    /// A general execution error.
    Execution { message: String },
    /// A panic was caught during UDF execution.
    Panic(String),
    /// An invalid argument was supplied to the UDF.
    InvalidArgument { message: String },
}

impl fmt::Display for UdfError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UdfError::Arrow(msg) => write!(f, "Arrow error: {msg}"),
            UdfError::Execution { message } => write!(f, "Execution error: {message}"),
            UdfError::Panic(msg) => write!(f, "Panic: {msg}"),
            UdfError::InvalidArgument { message } => {
                write!(f, "Invalid argument: {message}")
            }
        }
    }
}

impl std::error::Error for UdfError {}

impl From<arrow::error::ArrowError> for UdfError {
    fn from(e: arrow::error::ArrowError) -> Self {
        UdfError::Arrow(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Scalar UDF trait
// ---------------------------------------------------------------------------

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

    /// Execute the UDF over `batch`, returning one value per row.
    fn call(&self, batch: &RecordBatch) -> Result<ArrayRef, UdfError>;
}

// ---------------------------------------------------------------------------
// Aggregate UDF types and trait
// ---------------------------------------------------------------------------

/// Opaque serialised accumulator state owned by an aggregate UDF.
#[derive(Debug, Default)]
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
// Registry
// ---------------------------------------------------------------------------

/// Runtime registry that maps names to registered UDFs.
#[derive(Debug, Default)]
pub struct UdfRegistry {
    scalars: HashMap<String, Arc<dyn ScalarUdf>>,
    aggregates: HashMap<String, Arc<dyn AggregateUdf>>,
    tables: HashMap<String, Arc<dyn TableUdf>>,
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
        let result: PrimitiveArray<Int64Type> =
            int_array.iter().map(|v| v.map(|x| x * factor)).collect();

        Ok(Arc::new(result))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

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

            let mut current: i64 = if state.data.len() == 8 {
                i64::from_le_bytes(state.data[..8].try_into().unwrap())
            } else {
                0
            };

            for v in col.iter().flatten() {
                current += v;
            }
            state.data = current.to_le_bytes().to_vec();
            Ok(())
        }

        fn finalize(&self, state: AggState) -> Result<ScalarValue, UdfError> {
            if state.data.len() == 8 {
                let v = i64::from_le_bytes(state.data[..8].try_into().unwrap());
                Ok(ScalarValue::Int64(v))
            } else {
                Ok(ScalarValue::Int64(0))
            }
        }

        fn merge(&self, a: AggState, b: AggState) -> Result<AggState, UdfError> {
            let va: i64 = if a.data.len() == 8 {
                i64::from_le_bytes(a.data[..8].try_into().unwrap())
            } else {
                0
            };
            let vb: i64 = if b.data.len() == 8 {
                i64::from_le_bytes(b.data[..8].try_into().unwrap())
            } else {
                0
            };
            Ok(AggState {
                data: (va + vb).to_le_bytes().to_vec(),
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
        let distributed_result = udf
            .finalize(merged_state)
            .expect("finalize merged state");

        // -----------------------------------------------------------------
        // Reference path: accumulate all rows in a single pass.
        // -----------------------------------------------------------------
        let all_values = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![
                1_i64, 2, 3, 4, 5, 6, 7,
            ]))],
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
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vals))],
            )
            .expect("valid batch")
        };

        let mut s1 = AggState::default();
        let mut s2 = AggState::default();
        let mut s3 = AggState::default();

        udf.accumulate(&mut s1, &make_batch(vec![100])).expect("acc p1");
        udf.accumulate(&mut s2, &make_batch(vec![200, 300])).expect("acc p2");
        udf.accumulate(&mut s3, &make_batch(vec![400, 500, 600])).expect("acc p3");

        // Merge left-to-right: ((s1 merge s2) merge s3)
        let m12 = udf.merge(s1, s2).expect("merge s1+s2");
        let m123 = udf.merge(m12, s3).expect("merge (s1+s2)+s3");

        let result = udf.finalize(m123).expect("finalize three-partition merge");

        assert!(
            matches!(result, ScalarValue::Int64(2100)),
            "three-partition merge must yield 2100, got {result:?}",
        );
    }
}
