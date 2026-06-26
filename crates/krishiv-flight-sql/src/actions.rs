use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use tonic::Status;

/// Error type for Krishiv DoAction handlers.
pub(crate) enum KrishivActionError {
    Status(Status),
    Other(String),
}

impl From<Status> for KrishivActionError {
    fn from(s: Status) -> Self {
        Self::Status(s)
    }
}

// Prepared statement parameter binding helpers.

/// Count the highest `$N` positional placeholder index in `sql`.
pub(crate) fn count_sql_params(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut max = 0usize;
    let mut i = 0;
    while i < bytes.len() {
        if bytes.get(i) == Some(&b'$') {
            i += 1;
            let start = i;
            while bytes.get(i).is_some_and(|b| b.is_ascii_digit()) {
                i += 1;
            }
            if i > start
                && let Ok(n) = sql[start..i].parse::<usize>()
                && n > max
            {
                max = n;
            }
        } else {
            i += 1;
        }
    }
    max
}

/// Build a parameter schema with `n` nullable `Utf8` fields named `p1 … pN`.
pub(crate) fn build_param_schema(n: usize) -> Schema {
    let fields: Vec<arrow::datatypes::Field> = (1..=n)
        .map(|i| {
            arrow::datatypes::Field::new(format!("p{i}"), arrow::datatypes::DataType::Utf8, true)
        })
        .collect();
    Schema::new(fields)
}

/// Serialize a schema as an Arrow IPC stream (schema-only, no record batches).
pub(crate) fn schema_to_ipc_bytes(schema: &Schema) -> Result<Vec<u8>, Status> {
    let mut buf = Vec::new();
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, schema)
        .map_err(|e| Status::internal(format!("ipc schema encode: {e}")))?;
    writer
        .finish()
        .map_err(|e| Status::internal(format!("ipc schema finish: {e}")))?;
    Ok(buf)
}

/// Substitute `$N` placeholders in `sql` with SQL literal values from `batch`
/// row 0.
///
/// The scan is single-pass over the original SQL text: each `$N` is replaced
/// with the literal for column `N` (1-indexed) and the substituted text is
/// copied verbatim, never re-scanned. This avoids two classes of bug that the
/// previous reverse `str::replace` approach had:
///
/// * a parameter value containing `$N` text would be re-substituted; and
/// * `$100` would be partially matched by the `$1` / `$10` replacements when
///   fewer than 100 parameters were bound.
///
/// `$` not followed by a digit, or `$N` where `N` is `0` or greater than the
/// number of columns, is left untouched. `$` and ASCII digits are single-byte
/// ASCII and can never appear inside a multi-byte UTF-8 sequence, so scanning
/// the bytes is UTF-8 safe; verbatim text is copied via `&str` slices taken at
/// valid character boundaries.
pub(crate) fn substitute_sql_params(sql: &str, batch: &RecordBatch) -> String {
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::DataType;

    fn col_literal(array: &dyn arrow::array::Array, row: usize) -> String {
        if array.is_null(row) {
            return "NULL".to_string();
        }
        match array.data_type() {
            DataType::Boolean => array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(|a| if a.value(row) { "TRUE" } else { "FALSE" }.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            DataType::Int8 => array
                .as_any()
                .downcast_ref::<Int8Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            DataType::Int16 => array
                .as_any()
                .downcast_ref::<Int16Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            DataType::Int32 => array
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            DataType::Int64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            DataType::UInt8 => array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            DataType::UInt16 => array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            DataType::UInt32 => array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            DataType::UInt64 => array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            DataType::Float32 => array
                .as_any()
                .downcast_ref::<Float32Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            DataType::Float64 => array
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            DataType::Utf8 => array
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|a| format!("'{}'", a.value(row).replace('\'', "''")))
                .unwrap_or_else(|| "NULL".to_string()),
            DataType::LargeUtf8 => array
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .map(|a| format!("'{}'", a.value(row).replace('\'', "''")))
                .unwrap_or_else(|| "NULL".to_string()),
            _ => "NULL".to_string(),
        }
    }

    let ncols = batch.num_columns();
    if ncols == 0 || batch.num_rows() == 0 {
        return sql.to_string();
    }

    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 64);
    let mut text_start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes.get(i) == Some(&b'$') {
            let digit_start = i + 1;
            let mut j = digit_start;
            while bytes.get(j).is_some_and(|b| b.is_ascii_digit()) {
                j += 1;
            }
            if j > digit_start
                && let Ok(n) = sql[digit_start..j].parse::<usize>()
                && (1..=ncols).contains(&n)
            {
                if text_start < i {
                    out.push_str(&sql[text_start..i]);
                }
                out.push_str(&col_literal(batch.column(n - 1).as_ref(), 0));
                i = j;
                text_start = j;
                continue;
            }
        }
        i += 1;
    }
    if text_start < bytes.len() {
        out.push_str(&sql[text_start..]);
    }
    out
}

pub(crate) fn encode_batches_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>, KrishivActionError> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches.first().map(|b| b.schema()).unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));
    let mut buf = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| KrishivActionError::Other(format!("ipc encode: {e}")))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| KrishivActionError::Other(format!("ipc write: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| KrishivActionError::Other(format!("ipc finish: {e}")))?;
    }
    Ok(buf)
}
