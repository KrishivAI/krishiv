//! Spark date part UDFs (year/month/day) for TPC-H compatibility.

use datafusion::arrow::array::{ArrayRef, Date32Array, Int32Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

fn civil_from_days(z: i32) -> (i32, i32, i32) {
    let z = z + 719468;
    let era = (if z < 0 { z - 146096 } else { z }) / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let y = y + if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

pub fn register_spark_date_udfs(
    ctx: &SessionContext,
) -> Result<(), datafusion::error::DataFusionError> {
    for (name, part) in [("year", 0i32), ("month", 1), ("day", 2), ("quarter", 1)] {
        let part_idx = part;
        let udf = create_udf(
            name,
            vec![DataType::Date32],
            DataType::Int32,
            Volatility::Immutable,
            Arc::new(move |args| {
                let arr = match &args[0] {
                    ColumnarValue::Array(a) => a.clone(),
                    ColumnarValue::Scalar(s) => s.to_array()?,
                };
                let dates = arr.as_any().downcast_ref::<Date32Array>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "{name}: expected Date32 array"
                    ))
                })?;
                let out: Int32Array = dates
                    .iter()
                    .map(|d| {
                        d.map(|v| {
                            let (y, m, day) = civil_from_days(v);
                            match part_idx {
                                0 => y,
                                1 => m,
                                2 => day,
                                _ => (m - 1) / 3 + 1,
                            }
                        })
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
            }),
        );
        ctx.register_udf(udf);
    }
    Ok(())
}
