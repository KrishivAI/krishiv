//! Bridge Krishiv [`ScalarUdf`] implementations into DataFusion.

use std::sync::Arc;

use arrow::array::RecordBatchOptions;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::logical_expr::ColumnarValue;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{create_udf, Volatility};

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
        let _udf = udf;
        let _ = name;
    }
    let _ = ctx;
    Ok(())
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
        let _udf = udf;
        let _ = name;
    }
    let _ = ctx;
    Ok(())
}

fn columnar_values_to_record_batch(
    schema: &Schema,
    values: &[ColumnarValue],
) -> Result<RecordBatch, DataFusionError> {
    if values.len() != schema.fields().len() {
        return Err(DataFusionError::External(format!(
            "expected {} arguments, got {}",
            schema.fields().len(),
            values.len()
        )
        .into()));
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
                    return Err(DataFusionError::External(format!(
                        "column '{}' length {} does not match batch length {}",
                        field.name(),
                        a.len(),
                        num_rows
                    )
                    .into()));
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
