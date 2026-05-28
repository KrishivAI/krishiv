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

/// Register every scalar UDF in `registry` with the DataFusion session context.
pub fn sync_scalar_udfs(
    ctx: &datafusion::prelude::SessionContext,
    registry: &krishiv_udf::UdfRegistry,
) -> Result<(), DataFusionError> {
    for name in registry.scalar_names() {
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

        let df_udf = create_udf(
            &udf_name,
            input_types,
            return_type,
            Volatility::Immutable,
            Arc::new(move |args: &[ColumnarValue]| {
                let batch = columnar_values_to_record_batch(&input_schema, args)?;
                let array = udf
                    .call(&batch)
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
    registry: &krishiv_udf::UdfRegistry,
) -> Result<(), DataFusionError> {
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
                    state: krishiv_udf::AggState::default(),
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

/// DataFusion Accumulator bridge that delegates to a [`krishiv_udf::AggregateUdf`].
#[derive(Debug)]
struct KrishivAggregateAccumulator {
    udf: Arc<dyn krishiv_udf::AggregateUdf>,
    state: krishiv_udf::AggState,
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
        let data = arrow::array::Array::to_data(states[0].as_ref());
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
            let other = krishiv_udf::AggState {
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
            Ok(i32::from_ne_bytes(arr) as usize)
        }
        8 => {
            let arr: [u8; 8] = buf[pos..pos + 8].try_into().map_err(|_| {
                DataFusionError::Execution("merge_batch: invalid i64 offset".into())
            })?;
            Ok(i64::from_ne_bytes(arr) as usize)
        }
        _ => Err(DataFusionError::Execution(format!(
            "merge_batch: unsupported offset width {width}"
        ))),
    }
}

fn krishiv_scalar_to_datafusion(
    value: &krishiv_udf::ScalarValue,
) -> datafusion::error::Result<datafusion::scalar::ScalarValue> {
    use datafusion::scalar::ScalarValue as DfScalar;
    match value {
        krishiv_udf::ScalarValue::Null => Ok(DfScalar::Null),
        krishiv_udf::ScalarValue::Int64(v) => Ok(DfScalar::Int64(Some(*v))),
        krishiv_udf::ScalarValue::Float64(v) => Ok(DfScalar::Float64(Some(*v))),
        krishiv_udf::ScalarValue::Utf8(v) => Ok(DfScalar::Utf8(Some(v.clone()))),
        krishiv_udf::ScalarValue::Boolean(v) => Ok(DfScalar::Boolean(Some(*v))),
        krishiv_udf::ScalarValue::Bytes(v) => Ok(DfScalar::Binary(Some(v.clone()))),
    }
}

/// Register table UDFs from `registry` with DataFusion (P1-21).
pub fn sync_table_udfs(
    ctx: &datafusion::prelude::SessionContext,
    registry: &krishiv_udf::UdfRegistry,
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
    inner: Arc<dyn krishiv_udf::TableUdf>,
    schema: arrow::datatypes::Schema,
}

impl TableFunctionImpl for KrishivTableFunctionImpl {
    fn call(
        &self,
        _args: &[datafusion::logical_expr::Expr],
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let batch = self
            .inner
            .call(&[])
            .map_err(|e| DataFusionError::External(e.to_string().into()))?;
        let table = MemTable::try_new(Arc::new(self.schema.clone()), vec![vec![batch]])?;
        Ok(Arc::new(table))
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
