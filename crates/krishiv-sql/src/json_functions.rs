//! Spark-reference JSON scalar functions (Phase 60 SQL surface completeness).
//!
//! Closes the verified-missing JSON family gap for the batch SQL front door:
//!
//! - `get_json_object(json, path)` — extract a value by a Spark-style JSONPath
//!   (`$`, `.field`, `['field']`, `[index]`). Returns the leaf as text (a JSON
//!   string leaf is unquoted; objects/arrays are returned as compact JSON);
//!   `NULL` on invalid JSON, a missing path, or a JSON `null` leaf.
//! - `json_array_length(json)` — element count of a top-level JSON array, or
//!   `NULL` when the input is not a valid JSON array.
//!
//! The struct/schema-typed members of the family (`from_json`, `to_json`,
//! `json_tuple`) need schema-DDL parsing and struct/table-function machinery and
//! are tracked as the remaining Phase-60 JSON sub-items.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Builder, StringArray, StringBuilder};
use arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};
use datafusion::prelude::SessionContext;

/// Register the JSON scalar UDFs with the DataFusion session context.
pub fn register_json_functions(ctx: &SessionContext) -> Result<(), DataFusionError> {
    ctx.register_udf(make_get_json_object());
    ctx.register_udf(make_json_array_length());
    Ok(())
}

fn make_get_json_object() -> ScalarUDF {
    create_udf(
        "get_json_object",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let json = str_arg(&arrays, 0, "get_json_object")?;
            let path = str_arg(&arrays, 1, "get_json_object")?;
            let mut out = StringBuilder::new();
            for i in 0..json.len() {
                if json.is_null(i) || path.is_null(i) {
                    out.append_null();
                    continue;
                }
                match get_json_object_impl(json.value(i), path.value(i)) {
                    Some(s) => out.append_value(s),
                    None => out.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
        }),
    )
}

fn make_json_array_length() -> ScalarUDF {
    create_udf(
        "json_array_length",
        vec![DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let json = str_arg(&arrays, 0, "json_array_length")?;
            let mut out = Int64Builder::new();
            for i in 0..json.len() {
                if json.is_null(i) {
                    out.append_null();
                    continue;
                }
                match serde_json::from_str::<serde_json::Value>(json.value(i)) {
                    Ok(serde_json::Value::Array(a)) => out.append_value(a.len() as i64),
                    _ => out.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
        }),
    )
}

fn str_arg<'a>(
    arrays: &'a [ArrayRef],
    idx: usize,
    fname: &str,
) -> Result<&'a StringArray, DataFusionError> {
    arrays
        .get(idx)
        .and_then(|a| a.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| DataFusionError::Internal(format!("{fname}: argument {idx} must be Utf8")))
}

/// Extract a value from a JSON document by a Spark-style path and render the
/// leaf as text. Returns `None` (SQL NULL) on invalid JSON, a path that does
/// not resolve, or a JSON `null` leaf.
fn get_json_object_impl(json: &str, path: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(json).ok()?;
    let leaf = navigate(&value, path)?;
    render_leaf(leaf)
}

/// Navigate `value` by a Spark JSONPath: `$` root, `.field`, `['field']` /
/// `["field"]`, and `[index]`. Returns the referenced node or `None`.
fn navigate<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut chars = path.chars().peekable();
    if chars.next() != Some('$') {
        return None;
    }
    let mut cur = value;
    while let Some(&c) = chars.peek() {
        match c {
            '.' => {
                chars.next();
                let mut name = String::new();
                while let Some(&c2) = chars.peek() {
                    if c2 == '.' || c2 == '[' {
                        break;
                    }
                    name.push(c2);
                    chars.next();
                }
                if name.is_empty() {
                    return None;
                }
                cur = cur.get(&name)?;
            }
            '[' => {
                chars.next();
                match chars.peek() {
                    Some(&q) if q == '\'' || q == '"' => {
                        chars.next();
                        let mut name = String::new();
                        for c2 in chars.by_ref() {
                            if c2 == q {
                                break;
                            }
                            name.push(c2);
                        }
                        if chars.next() != Some(']') {
                            return None;
                        }
                        cur = cur.get(&name)?;
                    }
                    Some(_) => {
                        let mut idx = String::new();
                        while let Some(&c2) = chars.peek() {
                            if c2 == ']' {
                                break;
                            }
                            idx.push(c2);
                            chars.next();
                        }
                        if chars.next() != Some(']') {
                            return None;
                        }
                        let i: usize = idx.trim().parse().ok()?;
                        cur = cur.get(i)?;
                    }
                    None => return None,
                }
            }
            _ => return None,
        }
    }
    Some(cur)
}

fn render_leaf(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        // Objects and arrays are returned as their compact JSON text (Spark).
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_json_object_extracts_scalar_and_nested_and_indexed() {
        let doc = r#"{"a":{"b":42},"c":["x","y"],"s":"hi","n":null,"t":true}"#;
        assert_eq!(get_json_object_impl(doc, "$.a.b").as_deref(), Some("42"));
        assert_eq!(get_json_object_impl(doc, "$.s").as_deref(), Some("hi"));
        assert_eq!(get_json_object_impl(doc, "$['s']").as_deref(), Some("hi"));
        assert_eq!(get_json_object_impl(doc, "$.c[1]").as_deref(), Some("y"));
        assert_eq!(get_json_object_impl(doc, "$.t").as_deref(), Some("true"));
        // Object leaf → compact JSON text.
        assert_eq!(
            get_json_object_impl(doc, "$.a").as_deref(),
            Some(r#"{"b":42}"#)
        );
    }

    #[test]
    fn get_json_object_returns_null_for_missing_null_and_invalid() {
        let doc = r#"{"a":{"b":42},"n":null}"#;
        assert_eq!(get_json_object_impl(doc, "$.missing"), None);
        assert_eq!(get_json_object_impl(doc, "$.a.missing"), None);
        assert_eq!(get_json_object_impl(doc, "$.n"), None); // JSON null → SQL NULL
        assert_eq!(get_json_object_impl(doc, "no-dollar"), None);
        assert_eq!(get_json_object_impl("{not valid json", "$.a"), None);
    }

    #[test]
    fn json_navigate_root_and_array_root() {
        assert_eq!(
            get_json_object_impl(r#"[10,20,30]"#, "$[2]").as_deref(),
            Some("30")
        );
        assert_eq!(get_json_object_impl(r#"[10,20,30]"#, "$[9]"), None);
    }

    /// End-to-end: the functions are registered and callable through the real
    /// SQL front door (proves the `register_json_functions` wiring).
    #[tokio::test]
    async fn json_functions_usable_via_sql() {
        use arrow::array::{Int64Array, StringArray};

        let engine = crate::SqlEngine::new();
        let batches = engine
            .sql(
                "SELECT get_json_object('{\"a\":{\"b\":7}}', '$.a.b') AS v, \
                 json_array_length('[1,2,3,4]') AS n",
            )
            .await
            .expect("plan get_json_object query")
            .collect()
            .await
            .expect("collect");

        let batch = &batches[0];
        let v = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("v is Utf8");
        let n = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("n is Int64");
        assert_eq!(v.value(0), "7");
        assert_eq!(n.value(0), 4);
    }
}
