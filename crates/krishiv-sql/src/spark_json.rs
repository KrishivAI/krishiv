#![forbid(unsafe_code)]
//! Spark's schema-typed JSON functions: `to_json`, `from_json` and
//! `schema_of_json`.
//!
//! # `to_json(struct | array | map)`
//!
//! Writes compact JSON. As in Spark, a NULL struct field is left out of its
//! object, while a NULL array element or map value is written as `null`.
//! Dates are `yyyy-MM-dd`; timestamps are `yyyy-MM-dd'T'HH:mm:ss.SSS`, followed
//! by the offset (`Z` for UTC) when the column carries a time zone. Binary is
//! base64. `NaN` and the infinities, which JSON has no number for, are strings.
//!
//! # `from_json(json, schema)`
//!
//! `schema` is a constant in Spark's DDL form — a field list (`a INT, b
//! STRING`) or a type (`STRUCT<a: INT>`, `ARRAY<INT>`, `MAP<STRING, INT>`).
//! Parsing is permissive, matching Spark's default mode:
//!
//! - a NULL input is NULL;
//! - text that is not a JSON object, read with a struct schema, is a struct of
//!   NULLs (not a NULL struct);
//! - a field that is missing, or whose value does not fit its type, is NULL
//!   and the rest of the record is kept (Spark 3.4+'s partial results);
//! - a field in the JSON but not in the schema is ignored;
//! - a STRING field accepts any JSON value: a number, boolean, object or array
//!   arrives as its JSON text.
//!
//! An integer field does not take a fractional number, and no numeric field
//! takes a quoted one — both are NULL, as in Spark.
//!
//! # `schema_of_json(json)`
//!
//! The DDL of the type Spark would infer: integers are `BIGINT`, other numbers
//! `DOUBLE`, a `null` is `STRING`, object fields are listed in name order, and
//! array elements are merged to one type (`BIGINT` with `DOUBLE` is `DOUBLE`,
//! otherwise anything unlike is `STRING`). Text that is not JSON gives NULL.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, ListArray, MapArray, StringArray, StringBuilder,
    StructArray, new_null_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, TimeUnit};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::TimeZone;
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use serde_json::Value;

type DFResult<T> = Result<T, DataFusionError>;

/// Register `to_json`, `from_json` and `schema_of_json`.
pub fn register_spark_json_functions(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(ToJson {
        signature: Signature::any(1, Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::new_from_impl(FromJson {
        signature: Signature::any(2, Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::new_from_impl(SchemaOfJson {
        signature: Signature::any(1, Volatility::Immutable),
    }));
}

fn plan_error<T>(message: String) -> DFResult<T> {
    Err(DataFusionError::Plan(message))
}

// ── Spark DDL → Arrow type ───────────────────────────────────────────────────

/// Parse a Spark DDL schema: a field list or a single type.
pub(crate) fn parse_ddl_schema(ddl: &str) -> DFResult<DataType> {
    let tokens = ddl_tokens(ddl)?;
    // A single type that consumes the whole text…
    let mut as_type = DdlParser::new(&tokens);
    if let Ok(data_type) = as_type.data_type()
        && as_type.at_end()
    {
        return Ok(data_type);
    }
    // …otherwise a field list.
    let mut as_fields = DdlParser::new(&tokens);
    let fields = as_fields.fields(None)?;
    if !as_fields.at_end() {
        return plan_error(format!("schema `{ddl}`: unexpected text after the fields"));
    }
    Ok(DataType::Struct(fields))
}

#[derive(Debug, Clone, PartialEq)]
enum DdlToken {
    Word(String),
    Quoted(String),
    Text(String),
    Number(String),
    Symbol(char),
}

fn ddl_tokens(ddl: &str) -> DFResult<Vec<DdlToken>> {
    let mut tokens = Vec::new();
    let mut chars = ddl.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            c if c.is_whitespace() => {}
            '`' | '\'' | '"' => {
                let mut text = String::new();
                let mut closed = false;
                while let Some(next) = chars.next() {
                    if next == ch {
                        // A doubled quote is one literal quote.
                        if chars.peek() == Some(&ch) {
                            text.push(ch);
                            chars.next();
                            continue;
                        }
                        closed = true;
                        break;
                    }
                    text.push(next);
                }
                if !closed {
                    return plan_error(format!("schema `{ddl}`: unterminated quote"));
                }
                tokens.push(if ch == '`' {
                    DdlToken::Quoted(text)
                } else {
                    DdlToken::Text(text)
                });
            }
            c if c.is_ascii_digit() => {
                let mut number = String::from(c);
                while let Some(next) = chars.peek().copied().filter(char::is_ascii_digit) {
                    number.push(next);
                    chars.next();
                }
                tokens.push(DdlToken::Number(number));
            }
            c if c.is_alphanumeric() || c == '_' => {
                let mut word = String::from(c);
                while let Some(next) = chars
                    .peek()
                    .copied()
                    .filter(|n| n.is_alphanumeric() || *n == '_')
                {
                    word.push(next);
                    chars.next();
                }
                tokens.push(DdlToken::Word(word));
            }
            '<' | '>' | ',' | ':' | '(' | ')' => tokens.push(DdlToken::Symbol(ch)),
            other => {
                return plan_error(format!("schema `{ddl}`: unexpected character `{other}`"));
            }
        }
    }
    Ok(tokens)
}

struct DdlParser<'a> {
    tokens: &'a [DdlToken],
    position: usize,
}

impl<'a> DdlParser<'a> {
    fn new(tokens: &'a [DdlToken]) -> Self {
        Self {
            tokens,
            position: 0,
        }
    }

    fn at_end(&self) -> bool {
        self.position >= self.tokens.len()
    }

    fn peek(&self) -> Option<&'a DdlToken> {
        self.tokens.get(self.position)
    }

    fn next(&mut self) -> Option<&'a DdlToken> {
        let token = self.tokens.get(self.position);
        self.position += 1;
        token
    }

    fn eat_symbol(&mut self, symbol: char) -> bool {
        if self.peek() == Some(&DdlToken::Symbol(symbol)) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn expect_symbol(&mut self, symbol: char) -> DFResult<()> {
        if self.eat_symbol(symbol) {
            Ok(())
        } else {
            plan_error(format!("schema: expected `{symbol}`"))
        }
    }

    fn eat_word(&mut self, word: &str) -> bool {
        match self.peek() {
            Some(DdlToken::Word(found)) if found.eq_ignore_ascii_case(word) => {
                self.position += 1;
                true
            }
            _ => false,
        }
    }

    fn number(&mut self) -> DFResult<u8> {
        match self.next() {
            Some(DdlToken::Number(text)) => text
                .parse()
                .map_err(|_| DataFusionError::Plan(format!("schema: `{text}` is out of range"))),
            _ => plan_error(String::from("schema: expected a number")),
        }
    }

    /// `name [:] type [NOT NULL] [COMMENT '…']`, comma separated, up to
    /// `terminator` (or the end of the text).
    fn fields(&mut self, terminator: Option<char>) -> DFResult<Fields> {
        let mut fields = Vec::new();
        loop {
            if let Some(end) = terminator
                && self.peek() == Some(&DdlToken::Symbol(end))
            {
                break;
            }
            let name = match self.next() {
                Some(DdlToken::Word(name) | DdlToken::Quoted(name)) => name.clone(),
                _ => return plan_error(String::from("schema: expected a field name")),
            };
            self.eat_symbol(':');
            let data_type = self.data_type()?;
            if self.eat_word("NOT") && !self.eat_word("NULL") {
                return plan_error(String::from("schema: expected NULL after NOT"));
            }
            if self.eat_word("COMMENT") && !matches!(self.next(), Some(DdlToken::Text(_))) {
                return plan_error(String::from("schema: expected a string after COMMENT"));
            }
            // Parsing is permissive, so every field can come back NULL.
            fields.push(Field::new(name, data_type, true));
            if !self.eat_symbol(',') {
                break;
            }
        }
        if fields.is_empty() {
            return plan_error(String::from("schema: no fields"));
        }
        Ok(Fields::from(fields))
    }

    fn data_type(&mut self) -> DFResult<DataType> {
        let Some(DdlToken::Word(name)) = self.next() else {
            return plan_error(String::from("schema: expected a type name"));
        };
        let data_type = match name.to_ascii_lowercase().as_str() {
            "boolean" | "bool" => DataType::Boolean,
            "tinyint" | "byte" => DataType::Int8,
            "smallint" | "short" => DataType::Int16,
            "int" | "integer" => DataType::Int32,
            "bigint" | "long" => DataType::Int64,
            "float" | "real" => DataType::Float32,
            "double" => DataType::Float64,
            "string" => DataType::Utf8,
            "varchar" | "char" => {
                if self.eat_symbol('(') {
                    self.number()?;
                    self.expect_symbol(')')?;
                }
                DataType::Utf8
            }
            "binary" => DataType::Binary,
            "date" => DataType::Date32,
            "timestamp" | "timestamp_ntz" | "timestamp_ltz" => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            "decimal" | "dec" | "numeric" => {
                // Spark's default is DECIMAL(10, 0).
                let (mut precision, mut scale) = (10_u8, 0_u8);
                if self.eat_symbol('(') {
                    precision = self.number()?;
                    if self.eat_symbol(',') {
                        scale = self.number()?;
                    }
                    self.expect_symbol(')')?;
                }
                if precision == 0 || precision > 38 || scale > precision {
                    return plan_error(format!(
                        "schema: DECIMAL({precision}, {scale}) is not a valid type"
                    ));
                }
                DataType::Decimal128(precision, i8::try_from(scale).unwrap_or(0))
            }
            "array" => {
                self.expect_symbol('<')?;
                let element = self.data_type()?;
                self.expect_symbol('>')?;
                DataType::List(Arc::new(Field::new("item", element, true)))
            }
            "map" => {
                self.expect_symbol('<')?;
                let key = self.data_type()?;
                self.expect_symbol(',')?;
                let value = self.data_type()?;
                self.expect_symbol('>')?;
                let entries = Fields::from(vec![
                    Field::new("key", key, false),
                    Field::new("value", value, true),
                ]);
                DataType::Map(
                    Arc::new(Field::new("entries", DataType::Struct(entries), false)),
                    false,
                )
            }
            "struct" => {
                self.expect_symbol('<')?;
                let fields = self.fields(Some('>'))?;
                self.expect_symbol('>')?;
                DataType::Struct(fields)
            }
            other => return plan_error(format!("schema: unknown type `{other}`")),
        };
        Ok(data_type)
    }
}

// ── from_json ────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct FromJson {
    signature: Signature,
}

impl ScalarUDFImpl for FromJson {
    fn name(&self) -> &str {
        "from_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        plan_error(String::from(
            "from_json: the schema must be a string constant",
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        let schema = match args.scalar_arguments.get(1) {
            Some(Some(
                ScalarValue::Utf8(Some(ddl))
                | ScalarValue::LargeUtf8(Some(ddl))
                | ScalarValue::Utf8View(Some(ddl)),
            )) => parse_ddl_schema(ddl)?,
            _ => {
                return plan_error(String::from(
                    "from_json: the schema must be a string constant, e.g. 'a INT, b STRING'",
                ));
            }
        };
        Ok(Arc::new(Field::new(self.name(), schema, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let Some(json) = args.args.first() else {
            return plan_error(String::from("from_json needs a JSON string and a schema"));
        };
        let json = cast(&json.to_array(rows)?, &DataType::Utf8)?;
        let json = json.as_string::<i32>();
        let parsed: Vec<Option<Value>> = json
            .iter()
            .map(|text| text.and_then(|text| serde_json::from_str(text).ok()))
            .collect();
        let data_type = args.return_field.data_type();

        let array = match data_type {
            // Text that is present but is not an object is a struct of NULLs,
            // not a NULL struct; only a NULL input is NULL.
            DataType::Struct(fields) => {
                let empty = Value::Object(serde_json::Map::new());
                let values: Vec<Option<&Value>> = parsed
                    .iter()
                    .enumerate()
                    .map(|(row, value)| {
                        if json.is_null(row) {
                            None
                        } else {
                            Some(value.as_ref().filter(|v| v.is_object()).unwrap_or(&empty))
                        }
                    })
                    .collect();
                build_array(&values, &DataType::Struct(fields.clone()))?
            }
            other => {
                let values: Vec<Option<&Value>> = parsed.iter().map(Option::as_ref).collect();
                build_array(&values, other)?
            }
        };
        Ok(ColumnarValue::Array(array))
    }
}

/// Build an array of `data_type` from one JSON value per row. A value that
/// does not fit the type is NULL.
fn build_array(values: &[Option<&Value>], data_type: &DataType) -> DFResult<ArrayRef> {
    fn non_null<'a>(value: &Option<&'a Value>) -> Option<&'a Value> {
        value.filter(|v| !v.is_null())
    }
    macro_rules! integers {
        ($array:ty, $native:ty) => {
            Arc::new(<$array>::from_iter(values.iter().map(|value| {
                non_null(value)
                    .and_then(Value::as_i64)
                    .and_then(|number| <$native>::try_from(number).ok())
            })))
        };
    }
    let float = |value: &Value| match value {
        Value::Number(number) => number.as_f64(),
        // The three values JSON cannot write as numbers.
        Value::String(text) => match text.as_str() {
            "NaN" => Some(f64::NAN),
            "Infinity" | "+Infinity" | "Inf" | "+Inf" => Some(f64::INFINITY),
            "-Infinity" | "-Inf" => Some(f64::NEG_INFINITY),
            _ => None,
        },
        _ => None,
    };
    // Types Arrow already parses from text: hand it the text and let a value
    // it cannot read become NULL.
    let via_text = |text_of: &dyn Fn(&Value) -> Option<String>| -> DFResult<ArrayRef> {
        let text: StringArray = values
            .iter()
            .map(|value| non_null(value).and_then(text_of))
            .collect();
        Ok(cast(&text, data_type)?)
    };

    let array: ArrayRef = match data_type {
        DataType::Boolean => Arc::new(BooleanArray::from_iter(
            values
                .iter()
                .map(|value| non_null(value).and_then(Value::as_bool)),
        )),
        DataType::Int8 => integers!(Int8Array, i8),
        DataType::Int16 => integers!(Int16Array, i16),
        DataType::Int32 => integers!(Int32Array, i32),
        DataType::Int64 => integers!(Int64Array, i64),
        DataType::Float32 => Arc::new(Float32Array::from_iter(values.iter().map(|value| {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "FLOAT is the declared type; narrowing is what it asks for"
            )]
            non_null(value).and_then(float).map(|number| number as f32)
        }))),
        DataType::Float64 => Arc::new(Float64Array::from_iter(
            values.iter().map(|value| non_null(value).and_then(float)),
        )),
        DataType::Utf8 => Arc::new(StringArray::from_iter(values.iter().map(|value| {
            non_null(value).map(|value| match value {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            })
        }))),
        DataType::Binary => Arc::new(BinaryArray::from_iter(values.iter().map(|value| {
            non_null(value)
                .and_then(Value::as_str)
                .and_then(|text| BASE64.decode(text).ok())
        }))),
        DataType::Decimal128(_, _) => via_text(&|value| match value {
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })?,
        DataType::Date32 | DataType::Timestamp(_, _) => {
            via_text(&|value| value.as_str().map(str::to_owned))?
        }
        DataType::List(field) => {
            let mut children: Vec<Option<&Value>> = Vec::new();
            let mut lengths = Vec::with_capacity(values.len());
            let mut validity = Vec::with_capacity(values.len());
            for value in values {
                match non_null(value).and_then(Value::as_array) {
                    Some(elements) => {
                        children.extend(elements.iter().map(Some));
                        lengths.push(elements.len());
                        validity.push(true);
                    }
                    None => {
                        lengths.push(0);
                        validity.push(false);
                    }
                }
            }
            Arc::new(ListArray::try_new(
                Arc::clone(field),
                OffsetBuffer::from_lengths(lengths),
                build_array(&children, field.data_type())?,
                Some(NullBuffer::from(validity)),
            )?)
        }
        DataType::Struct(fields) => {
            let objects: Vec<Option<&serde_json::Map<String, Value>>> = values
                .iter()
                .map(|value| non_null(value).and_then(Value::as_object))
                .collect();
            let columns = fields
                .iter()
                .map(|field| {
                    let column: Vec<Option<&Value>> = objects
                        .iter()
                        .map(|object| object.and_then(|object| object.get(field.name())))
                        .collect();
                    build_array(&column, field.data_type())
                })
                .collect::<DFResult<Vec<_>>>()?;
            let validity: Vec<bool> = objects.iter().map(Option::is_some).collect();
            Arc::new(StructArray::try_new(
                fields.clone(),
                columns,
                Some(NullBuffer::from(validity)),
            )?)
        }
        DataType::Map(entries, sorted) => {
            let DataType::Struct(entry_fields) = entries.data_type() else {
                return plan_error(String::from("from_json: malformed MAP type"));
            };
            let [key_field, value_field] = entry_fields.iter().as_slice() else {
                return plan_error(String::from("from_json: malformed MAP type"));
            };
            let mut keys: Vec<Option<&str>> = Vec::new();
            let mut children: Vec<Option<&Value>> = Vec::new();
            let mut lengths = Vec::with_capacity(values.len());
            let mut validity = Vec::with_capacity(values.len());
            for value in values {
                match non_null(value).and_then(Value::as_object) {
                    Some(object) => {
                        for (key, value) in object {
                            keys.push(Some(key.as_str()));
                            children.push(Some(value));
                        }
                        lengths.push(object.len());
                        validity.push(true);
                    }
                    None => {
                        lengths.push(0);
                        validity.push(false);
                    }
                }
            }
            // JSON object keys are text; a typed key is read from that text.
            let keys = cast(&StringArray::from(keys), key_field.data_type())?;
            if keys.null_count() > 0 {
                return Err(DataFusionError::Execution(format!(
                    "from_json: a map key is not a valid {}",
                    key_field.data_type()
                )));
            }
            let struct_array = StructArray::try_new(
                entry_fields.clone(),
                vec![keys, build_array(&children, value_field.data_type())?],
                None,
            )?;
            Arc::new(MapArray::try_new(
                Arc::clone(entries),
                OffsetBuffer::from_lengths(lengths),
                struct_array,
                Some(NullBuffer::from(validity)),
                *sorted,
            )?)
        }
        DataType::Null => new_null_array(data_type, values.len()),
        other => {
            return plan_error(format!("from_json: unsupported type {other} in the schema"));
        }
    };
    Ok(array)
}

// ── to_json ──────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct ToJson {
    signature: Signature,
}

impl ScalarUDFImpl for ToJson {
    fn name(&self) -> &str {
        "to_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        match arg_types {
            [
                DataType::Struct(_)
                | DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Map(_, _),
            ] => Ok(DataType::Utf8),
            [other] => plan_error(format!(
                "to_json expects a struct, array or map, got {other}"
            )),
            _ => plan_error(String::from("to_json takes exactly one argument")),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let Some(input) = args.args.first() else {
            return plan_error(String::from("to_json takes exactly one argument"));
        };
        let input = input.to_array(rows)?;
        let mut out = StringBuilder::new();
        let mut text = String::new();
        for row in 0..input.len() {
            if input.is_null(row) {
                out.append_null();
                continue;
            }
            text.clear();
            write_json(input.as_ref(), row, &mut text)?;
            out.append_value(&text);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

fn write_string(text: &str, out: &mut String) -> DFResult<()> {
    let quoted = serde_json::to_string(text)
        .map_err(|e| DataFusionError::Execution(format!("to_json: {e}")))?;
    out.push_str(&quoted);
    Ok(())
}

/// Microseconds since the epoch of a timestamp in any unit.
fn timestamp_micros(array: &dyn Array, row: usize, unit: TimeUnit) -> i64 {
    use arrow::datatypes::{
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType,
    };
    match unit {
        TimeUnit::Second => array
            .as_primitive::<TimestampSecondType>()
            .value(row)
            .saturating_mul(1_000_000),
        TimeUnit::Millisecond => array
            .as_primitive::<TimestampMillisecondType>()
            .value(row)
            .saturating_mul(1_000),
        TimeUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().value(row),
        TimeUnit::Nanosecond => array
            .as_primitive::<TimestampNanosecondType>()
            .value(row)
            .div_euclid(1_000),
    }
}

/// Append the JSON for `array[row]`, which the caller knows is not NULL.
fn write_json(array: &dyn Array, row: usize, out: &mut String) -> DFResult<()> {
    let plain = |out: &mut String| -> DFResult<()> {
        let formatter = ArrayFormatter::try_new(array, &FormatOptions::default())?;
        out.push_str(&formatter.value(row).to_string());
        Ok(())
    };
    match array.data_type() {
        DataType::Boolean => out.push_str(if array.as_boolean().value(row) {
            "true"
        } else {
            "false"
        }),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => plain(out)?,
        DataType::Float32 | DataType::Float64 => {
            let value = match array.data_type() {
                DataType::Float32 => f64::from(
                    array
                        .as_primitive::<arrow::datatypes::Float32Type>()
                        .value(row),
                ),
                _ => array
                    .as_primitive::<arrow::datatypes::Float64Type>()
                    .value(row),
            };
            if value.is_nan() {
                out.push_str("\"NaN\"");
            } else if value.is_infinite() {
                out.push_str(if value > 0.0 {
                    "\"Infinity\""
                } else {
                    "\"-Infinity\""
                });
            } else {
                plain(out)?;
            }
        }
        DataType::Utf8 => write_string(array.as_string::<i32>().value(row), out)?,
        DataType::LargeUtf8 => write_string(array.as_string::<i64>().value(row), out)?,
        DataType::Utf8View => write_string(array.as_string_view().value(row), out)?,
        DataType::Binary => write_string(&BASE64.encode(array.as_binary::<i32>().value(row)), out)?,
        DataType::LargeBinary => {
            write_string(&BASE64.encode(array.as_binary::<i64>().value(row)), out)?;
        }
        DataType::BinaryView => {
            write_string(&BASE64.encode(array.as_binary_view().value(row)), out)?;
        }
        DataType::Date32 => {
            let days = array
                .as_primitive::<arrow::datatypes::Date32Type>()
                .value(row);
            let date = chrono::DateTime::from_timestamp(i64::from(days).saturating_mul(86_400), 0)
                .ok_or_else(|| {
                    DataFusionError::Execution(String::from("to_json: date out of range"))
                })?;
            write_string(&date.format("%Y-%m-%d").to_string(), out)?;
        }
        DataType::Timestamp(unit, zone) => {
            let micros = timestamp_micros(array, row, *unit);
            let utc = chrono::DateTime::from_timestamp_micros(micros).ok_or_else(|| {
                DataFusionError::Execution(String::from("to_json: timestamp out of range"))
            })?;
            let text = match zone {
                None => utc.format("%Y-%m-%dT%H:%M:%S%.3f").to_string(),
                Some(zone) => {
                    let zone: arrow::array::timezone::Tz = zone.parse()?;
                    let local = zone.from_utc_datetime(&utc.naive_utc());
                    let text = local.format("%Y-%m-%dT%H:%M:%S%.3f%:z").to_string();
                    match text.strip_suffix("+00:00") {
                        Some(stem) => format!("{stem}Z"),
                        None => text,
                    }
                }
            };
            write_string(&text, out)?;
        }
        DataType::List(_) => write_elements(array.as_list::<i32>().value(row).as_ref(), out)?,
        DataType::LargeList(_) => write_elements(array.as_list::<i64>().value(row).as_ref(), out)?,
        DataType::FixedSizeList(_, _) => {
            write_elements(array.as_fixed_size_list().value(row).as_ref(), out)?;
        }
        DataType::Struct(fields) => {
            let columns = array.as_struct().columns();
            out.push('{');
            let mut first = true;
            for (field, column) in fields.iter().zip(columns) {
                // Spark leaves a NULL field out of the object.
                if column.is_null(row) {
                    continue;
                }
                if !first {
                    out.push(',');
                }
                first = false;
                write_string(field.name(), out)?;
                out.push(':');
                write_json(column.as_ref(), row, out)?;
            }
            out.push('}');
        }
        DataType::Map(_, _) => {
            let entries = array.as_map().value(row);
            let [keys, values] = entries.columns() else {
                return plan_error(String::from("to_json: malformed map"));
            };
            let key_text = ArrayFormatter::try_new(keys.as_ref(), &FormatOptions::default())?;
            out.push('{');
            for entry in 0..entries.len() {
                if entry > 0 {
                    out.push(',');
                }
                write_string(&key_text.value(entry).to_string(), out)?;
                out.push(':');
                if values.is_null(entry) {
                    out.push_str("null");
                } else {
                    write_json(values.as_ref(), entry, out)?;
                }
            }
            out.push('}');
        }
        other => return plan_error(format!("to_json: unsupported type {other}")),
    }
    Ok(())
}

fn write_elements(elements: &dyn Array, out: &mut String) -> DFResult<()> {
    out.push('[');
    for index in 0..elements.len() {
        if index > 0 {
            out.push(',');
        }
        if elements.is_null(index) {
            out.push_str("null");
        } else {
            write_json(elements, index, out)?;
        }
    }
    out.push(']');
    Ok(())
}

// ── schema_of_json ───────────────────────────────────────────────────────────

/// The type Spark infers for a JSON value.
#[derive(Debug, Clone, PartialEq)]
enum Inferred {
    /// A JSON `null`, or an empty array's element: no evidence yet.
    Unknown,
    Boolean,
    BigInt,
    /// An integer too large for BIGINT.
    BigDecimal,
    Double,
    Text,
    Array(Box<Inferred>),
    Struct(BTreeMap<String, Inferred>),
}

fn infer(value: &Value) -> Inferred {
    match value {
        Value::Null => Inferred::Unknown,
        Value::Bool(_) => Inferred::Boolean,
        Value::Number(number) if number.is_i64() => Inferred::BigInt,
        Value::Number(number) if number.is_u64() => Inferred::BigDecimal,
        Value::Number(_) => Inferred::Double,
        Value::String(_) => Inferred::Text,
        Value::Array(elements) => Inferred::Array(Box::new(
            elements
                .iter()
                .map(infer)
                .fold(Inferred::Unknown, merge_inferred),
        )),
        Value::Object(object) => Inferred::Struct(
            object
                .iter()
                .map(|(name, value)| (name.clone(), infer(value)))
                .collect(),
        ),
    }
}

fn merge_inferred(left: Inferred, right: Inferred) -> Inferred {
    match (left, right) {
        (Inferred::Unknown, other) | (other, Inferred::Unknown) => other,
        (left, right) if left == right => left,
        (Inferred::BigInt | Inferred::BigDecimal, Inferred::Double)
        | (Inferred::Double, Inferred::BigInt | Inferred::BigDecimal) => Inferred::Double,
        (Inferred::BigInt, Inferred::BigDecimal) | (Inferred::BigDecimal, Inferred::BigInt) => {
            Inferred::BigDecimal
        }
        (Inferred::Array(left), Inferred::Array(right)) => {
            Inferred::Array(Box::new(merge_inferred(*left, *right)))
        }
        (Inferred::Struct(mut left), Inferred::Struct(right)) => {
            for (name, right_type) in right {
                let merged = match left.remove(&name) {
                    Some(left_type) => merge_inferred(left_type, right_type),
                    None => right_type,
                };
                left.insert(name, merged);
            }
            Inferred::Struct(left)
        }
        _ => Inferred::Text,
    }
}

fn inferred_ddl(inferred: &Inferred) -> String {
    match inferred {
        Inferred::Unknown | Inferred::Text => String::from("STRING"),
        Inferred::Boolean => String::from("BOOLEAN"),
        Inferred::BigInt => String::from("BIGINT"),
        Inferred::BigDecimal => String::from("DECIMAL(20,0)"),
        Inferred::Double => String::from("DOUBLE"),
        Inferred::Array(element) => format!("ARRAY<{}>", inferred_ddl(element)),
        Inferred::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|(name, field)| {
                    let plain = !name.is_empty()
                        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
                    let name = if plain {
                        name.clone()
                    } else {
                        format!("`{}`", name.replace('`', "``"))
                    };
                    format!("{name}: {}", inferred_ddl(field))
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("STRUCT<{fields}>")
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SchemaOfJson {
    signature: Signature,
}

impl ScalarUDFImpl for SchemaOfJson {
    fn name(&self) -> &str {
        "schema_of_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let Some(json) = args.args.first() else {
            return plan_error(String::from("schema_of_json takes exactly one argument"));
        };
        let json = cast(&json.to_array(rows)?, &DataType::Utf8)?;
        let schemas: StringArray = json
            .as_string::<i32>()
            .iter()
            .map(|text| {
                text.and_then(|text| serde_json::from_str::<Value>(text).ok())
                    .map(|value| inferred_ddl(&infer(&value)))
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(schemas)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::util::pretty::pretty_format_batches;

    /// One result cell, from a single-row single-column query.
    async fn cell(sql: &str) -> String {
        let batches = crate::SqlEngine::new()
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("plan `{sql}`: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("run `{sql}`: {e}"));
        let text = pretty_format_batches(&batches).expect("format").to_string();
        let row = text
            .lines()
            .nth(3)
            .unwrap_or_else(|| panic!("no row: {text}"));
        row.trim_matches('|').trim().to_string()
    }

    async fn error(sql: &str) -> String {
        let engine = crate::SqlEngine::new();
        match engine.sql(sql).await {
            Err(e) => e.to_string(),
            Ok(df) => df.collect().await.expect_err("must fail").to_string(),
        }
    }

    #[test]
    fn ddl_field_lists_and_types_parse() {
        let fields = |ddl: &str| match parse_ddl_schema(ddl).expect("parse") {
            DataType::Struct(fields) => fields
                .iter()
                .map(|f| format!("{}:{}", f.name(), f.data_type()))
                .collect::<Vec<_>>()
                .join(" | "),
            other => other.to_string(),
        };
        assert_eq!(fields("a INT, b STRING"), "a:Int32 | b:Utf8");
        assert_eq!(
            fields("a: bigint, `my col` DOUBLE"),
            "a:Int64 | my col:Float64"
        );
        assert_eq!(
            fields("price DECIMAL(10, 2) NOT NULL COMMENT 'net', d DATE"),
            "price:Decimal128(10, 2) | d:Date32"
        );
        assert_eq!(fields("STRUCT<a: INT, b: STRING>"), "a:Int32 | b:Utf8");
        // The colon-free struct form Spark also accepts.
        assert_eq!(fields("STRUCT<a INT, b STRING>"), "a:Int32 | b:Utf8");
        assert!(matches!(
            parse_ddl_schema("ARRAY<INT>").expect("parse"),
            DataType::List(_)
        ));
        assert!(matches!(
            parse_ddl_schema("MAP<STRING, ARRAY<BIGINT>>").expect("parse"),
            DataType::Map(_, _)
        ));
        // A nested type inside a field list.
        assert!(fields("tags ARRAY<STRING>, inner STRUCT<x: INT>").starts_with("tags:List"));
        for bad in ["", "a", "a NOPE", "STRUCT<a INT", "a INT,", "a DECIMAL(99)"] {
            assert!(parse_ddl_schema(bad).is_err(), "`{bad}` should not parse");
        }
    }

    #[tokio::test]
    async fn to_json_writes_structs_arrays_and_maps() {
        assert_eq!(
            cell("SELECT to_json(named_struct('a', 1, 'b', 'x')) AS j").await,
            r#"{"a":1,"b":"x"}"#
        );
        assert_eq!(cell("SELECT to_json([1, 2, 3]) AS j").await, "[1,2,3]");
        assert_eq!(
            cell("SELECT to_json(map(['k1', 'k2'], [1, 2])) AS j").await,
            r#"{"k1":1,"k2":2}"#
        );
        assert_eq!(
            cell(
                "SELECT to_json(named_struct('n', named_struct('xs', [1.5, 2.5]), 't', true)) AS j"
            )
            .await,
            r#"{"n":{"xs":[1.5,2.5]},"t":true}"#
        );
        // Text is escaped.
        assert_eq!(
            cell(r#"SELECT to_json(named_struct('s', 'a"b\c')) AS j"#).await,
            r#"{"s":"a\"b\\c"}"#
        );
    }

    #[tokio::test]
    async fn to_json_handles_nulls_dates_and_timestamps() {
        // A NULL field is omitted; a NULL element is `null`.
        assert_eq!(
            cell("SELECT to_json(named_struct('a', 1, 'b', CAST(NULL AS VARCHAR))) AS j").await,
            r#"{"a":1}"#
        );
        assert_eq!(
            cell("SELECT to_json(make_array(1, NULL, 3)) AS j").await,
            "[1,null,3]"
        );
        assert_eq!(
            cell("SELECT to_json(named_struct('d', DATE '2024-03-09')) AS j").await,
            r#"{"d":"2024-03-09"}"#
        );
        assert_eq!(
            cell("SELECT to_json(named_struct('t', TIMESTAMP '2024-03-09 10:11:12.345678')) AS j")
                .await,
            r#"{"t":"2024-03-09T10:11:12.345"}"#
        );
        assert_eq!(
            cell(
                "SELECT to_json(named_struct('t', arrow_cast(TIMESTAMP '2024-03-09 10:11:12', \
                 'Timestamp(Microsecond, Some(\"UTC\"))'))) AS j"
            )
            .await,
            r#"{"t":"2024-03-09T10:11:12.000Z"}"#
        );
        assert!(
            error("SELECT to_json(1)")
                .await
                .contains("struct, array or map")
        );
    }

    #[tokio::test]
    async fn from_json_reads_typed_fields() {
        assert_eq!(
            cell(r#"SELECT from_json('{"a":1,"b":"x"}', 'a INT, b STRING') AS s"#).await,
            "{a: 1, b: x}"
        );
        // Fields can then be used as ordinary columns.
        assert_eq!(
            cell(r#"SELECT from_json('{"a":41}', 'a INT')['a'] + 1 AS n"#).await,
            "42"
        );
        assert_eq!(
            cell(
                r#"SELECT from_json('{"d":"2024-03-09","p":12.5,"xs":[1,2]}', 'd DATE, p DECIMAL(6,2), xs ARRAY<BIGINT>') AS s"#
            )
            .await,
            "{d: 2024-03-09, p: 12.50, xs: [1, 2]}"
        );
        assert_eq!(
            cell(r#"SELECT from_json('[1, 2, 3]', 'ARRAY<INT>') AS xs"#).await,
            "[1, 2, 3]"
        );
        // Entries keep the document's order, as in Spark (serde_json
        // `preserve_order`, declared in the workspace Cargo.toml).
        assert_eq!(
            cell(r#"SELECT from_json('{"k":1,"j":2}', 'MAP<STRING, INT>') AS m"#).await,
            "{k: 1, j: 2}"
        );
        assert_eq!(
            cell(r#"SELECT from_json('{"o":{"x":7}}', 'o STRUCT<x: INT, y: STRING>') AS s"#).await,
            "{o: {x: 7, y: }}"
        );
    }

    #[tokio::test]
    async fn from_json_is_permissive() {
        // Missing and unknown fields.
        assert_eq!(
            cell(r#"SELECT from_json('{"a":1,"extra":true}', 'a INT, b STRING') AS s"#).await,
            "{a: 1, b: }"
        );
        // A value that does not fit is NULL; the rest of the record is kept.
        assert_eq!(
            cell(r#"SELECT from_json('{"a":"oops","b":"x"}', 'a INT, b STRING') AS s"#).await,
            "{a: , b: x}"
        );
        assert_eq!(
            cell(r#"SELECT from_json('{"a":1.5}', 'a INT') AS s"#).await,
            "{a: }"
        );
        assert_eq!(
            cell(r#"SELECT from_json('{"a":99999}', 'a TINYINT') AS s"#).await,
            "{a: }"
        );
        // A STRING field takes any value as its JSON text.
        assert_eq!(
            cell(r#"SELECT from_json('{"a":{"x":1},"b":5}', 'a STRING, b STRING') AS s"#).await,
            r#"{a: {"x":1}, b: 5}"#
        );
        // Not JSON at all: a struct of NULLs, not a NULL struct.
        assert_eq!(
            cell("SELECT from_json('not json', 'a INT') IS NULL AS is_null").await,
            "false"
        );
        assert_eq!(
            cell("SELECT from_json('not json', 'a INT') AS s").await,
            "{a: }"
        );
        // A NULL input is NULL.
        assert_eq!(
            cell("SELECT from_json(CAST(NULL AS VARCHAR), 'a INT') IS NULL AS is_null").await,
            "true"
        );
        assert!(
            error("SELECT from_json(j, j) FROM (VALUES ('{}')) AS t(j)")
                .await
                .contains("string constant")
        );
    }

    #[tokio::test]
    async fn from_json_and_to_json_round_trip_a_column() {
        let batches = crate::SqlEngine::new()
            .sql(
                "SELECT to_json(from_json(j, 'a INT, tags ARRAY<STRING>')) AS back \
                 FROM (VALUES ('{\"a\":1,\"tags\":[\"x\",\"y\"]}'), ('{\"a\":2}'), (NULL)) AS t(j)",
            )
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        let text = pretty_format_batches(&batches).expect("format").to_string();
        assert!(text.contains(r#"{"a":1,"tags":["x","y"]}"#), "{text}");
        assert!(text.contains(r#"{"a":2}"#), "{text}");
    }

    #[tokio::test]
    async fn schema_of_json_infers_sparks_types() {
        assert_eq!(
            cell(r#"SELECT schema_of_json('{"b":1,"a":"x"}') AS s"#).await,
            "STRUCT<a: STRING, b: BIGINT>"
        );
        assert_eq!(
            cell(r#"SELECT schema_of_json('[{"col":0}]') AS s"#).await,
            "ARRAY<STRUCT<col: BIGINT>>"
        );
        assert_eq!(
            cell(r#"SELECT schema_of_json('{"n":1.5,"t":true,"z":null,"e":[]}') AS s"#).await,
            "STRUCT<e: ARRAY<STRING>, n: DOUBLE, t: BOOLEAN, z: STRING>"
        );
        // Array elements are merged to one type.
        assert_eq!(
            cell(r#"SELECT schema_of_json('[1, 2.5]') AS s"#).await,
            "ARRAY<DOUBLE>"
        );
        assert_eq!(
            cell(r#"SELECT schema_of_json('[1, "a"]') AS s"#).await,
            "ARRAY<STRING>"
        );
        assert_eq!(
            cell(r#"SELECT schema_of_json('[{"a":1},{"b":"x"}]') AS s"#).await,
            "ARRAY<STRUCT<a: BIGINT, b: STRING>>"
        );
        assert_eq!(
            cell("SELECT schema_of_json('not json') IS NULL AS is_null").await,
            "true"
        );
    }

    /// The inferred schema is one `from_json` accepts.
    #[test]
    fn inferred_schemas_parse_back() {
        for json in [
            r#"{"a":1,"b":[1.5],"c":{"d":"x"},"e":null}"#,
            r#"[{"weird name":true}]"#,
            r#"{"big":18446744073709551615}"#,
        ] {
            let value: Value = serde_json::from_str(json).expect("json");
            let ddl = inferred_ddl(&infer(&value));
            assert!(parse_ddl_schema(&ddl).is_ok(), "{ddl}");
        }
    }
}
