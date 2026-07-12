//! Partitioned Iceberg write support (Phase 52, task #191).
//!
//! Three pieces shared by every write path that must be partition-spec
//! correct (durable CTAS landing, copy-on-write DML rewrites, compaction):
//!
//! 1. [`parse_partition_transform`] — parses one `PARTITIONED BY` item
//!    (`region`, `bucket(16, id)`, `truncate(4, sku)`, `day(ts)`, …) into a
//!    column name plus [`Transform`].
//! 2. [`build_unbound_partition_spec`] / [`transforms_from_metadata`] — map
//!    parsed transforms onto an Iceberg schema at CREATE time, and recover
//!    the transform list from an existing table's default spec so rewrite
//!    paths preserve partitioning.
//! 3. [`PartitionFanout`] — splits record batches into per-partition-value
//!    groups. Values are computed with iceberg-rust's own transform
//!    functions (`transform_literal`), so bucket hashing, truncation, and
//!    temporal projection match the Iceberg spec exactly; only the
//!    arrow-58 → [`Datum`] scalar conversion is ours (iceberg-rust 0.9 pins
//!    arrow 57, so its vectorized array path cannot accept workspace arrays).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
    StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use iceberg::spec::{
    Datum, Literal, PrimitiveLiteral, Schema, Struct, TableMetadata, Transform,
    UnboundPartitionSpec,
};
use iceberg::transform::{BoxedTransformFunction, create_transform_function};

use crate::lakehouse::LakehouseError;

/// One `PARTITIONED BY` entry: a source column and the Iceberg transform
/// applied to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionTransformField {
    /// Source column name exactly as it appears in the table schema.
    pub column: String,
    /// Iceberg partition transform.
    pub transform: Transform,
}

impl PartitionTransformField {
    /// Partition field name following the Iceberg reference-implementation
    /// convention (`region`, `id_bucket`, `sku_trunc`, `ts_day`, …).
    pub fn field_name(&self) -> String {
        match self.transform {
            Transform::Identity => self.column.clone(),
            Transform::Bucket(_) => format!("{}_bucket", self.column),
            Transform::Truncate(_) => format!("{}_trunc", self.column),
            Transform::Year => format!("{}_year", self.column),
            Transform::Month => format!("{}_month", self.column),
            Transform::Day => format!("{}_day", self.column),
            Transform::Hour => format!("{}_hour", self.column),
            _ => format!("{}_part", self.column),
        }
    }
}

fn strip_ident_quotes(s: &str) -> String {
    let t = s.trim();
    if t.len() >= 2 && t.starts_with('"') && t.ends_with('"') {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

/// Parse one `PARTITIONED BY` item.
///
/// Accepted shapes (case-insensitive function names, plural aliases match
/// Spark SQL): a bare column (identity), `identity(col)`, `bucket(n, col)`,
/// `truncate(w, col)`, `year(col)`, `month(col)`, `day(col)`, `hour(col)`.
pub fn parse_partition_transform(item: &str) -> Result<PartitionTransformField, LakehouseError> {
    let item = item.trim();
    let bad = |msg: String| LakehouseError::Iceberg(format!("PARTITIONED BY: {msg}"));
    let Some(open) = item.find('(') else {
        if item.is_empty() {
            return Err(bad("empty partition entry".to_string()));
        }
        return Ok(PartitionTransformField {
            column: strip_ident_quotes(item),
            transform: Transform::Identity,
        });
    };
    if !item.ends_with(')') {
        return Err(bad(format!("malformed transform `{item}`")));
    }
    let func = item[..open].trim().to_ascii_lowercase();
    let args: Vec<&str> = item[open + 1..item.len() - 1]
        .split(',')
        .map(str::trim)
        .collect();
    let one_col = |args: &[&str]| -> Result<String, LakehouseError> {
        match args {
            [col] if !col.is_empty() => Ok(strip_ident_quotes(col)),
            _ => Err(bad(format!("`{func}` takes exactly one column argument"))),
        }
    };
    let width_and_col = |args: &[&str]| -> Result<(u32, String), LakehouseError> {
        match args {
            [n, col] if !col.is_empty() => {
                let n: u32 =
                    n.parse().ok().filter(|v| *v > 0).ok_or_else(|| {
                        bad(format!("`{func}` needs a positive integer, got `{n}`"))
                    })?;
                Ok((n, strip_ident_quotes(col)))
            }
            _ => Err(bad(format!("`{func}` takes (n, column)"))),
        }
    };
    let (column, transform) = match func.as_str() {
        "identity" => (one_col(&args)?, Transform::Identity),
        "bucket" => {
            let (n, col) = width_and_col(&args)?;
            (col, Transform::Bucket(n))
        }
        "truncate" => {
            let (w, col) = width_and_col(&args)?;
            (col, Transform::Truncate(w))
        }
        "year" | "years" => (one_col(&args)?, Transform::Year),
        "month" | "months" => (one_col(&args)?, Transform::Month),
        "day" | "days" => (one_col(&args)?, Transform::Day),
        "hour" | "hours" => (one_col(&args)?, Transform::Hour),
        other => {
            return Err(bad(format!(
                "unsupported transform `{other}` (supported: identity, bucket, truncate, \
                 year, month, day, hour)"
            )));
        }
    };
    Ok(PartitionTransformField { column, transform })
}

/// Build the `UnboundPartitionSpec` for a new table from parsed transforms,
/// resolving source columns against the Iceberg schema created for it.
pub fn build_unbound_partition_spec(
    fields: &[PartitionTransformField],
    schema: &Schema,
) -> Result<UnboundPartitionSpec, LakehouseError> {
    let mut builder = UnboundPartitionSpec::builder();
    for field in fields {
        let source = schema.field_by_name(&field.column).ok_or_else(|| {
            LakehouseError::Iceberg(format!(
                "PARTITIONED BY column `{}` does not exist in the table schema",
                field.column
            ))
        })?;
        builder = builder
            .add_partition_field(source.id, field.field_name(), field.transform)
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
    }
    Ok(builder.build())
}

/// Recover the transform list from an existing table's default partition
/// spec, so rewrite paths (DML copy-on-write, compaction, replace) preserve
/// partitioning instead of silently recreating the table unpartitioned.
///
/// Returns an empty vector for unpartitioned tables.
pub fn transforms_from_metadata(
    metadata: &TableMetadata,
) -> Result<Vec<PartitionTransformField>, LakehouseError> {
    let schema = metadata.current_schema();
    metadata
        .default_partition_spec()
        .fields()
        .iter()
        .map(|f| {
            let source = schema.field_by_id(f.source_id).ok_or_else(|| {
                LakehouseError::Iceberg(format!(
                    "partition spec field `{}` references unknown source field id {}",
                    f.name, f.source_id
                ))
            })?;
            Ok(PartitionTransformField {
                column: source.name.clone(),
                transform: f.transform,
            })
        })
        .collect()
}

/// One partition-value group produced by [`PartitionFanout::split`].
#[derive(Debug)]
pub struct PartitionedBatch {
    /// Canonical grouping key (stable across batches for equal values).
    pub key: String,
    /// Hive-style relative path segment (`region=eu/ts_day=2026-07-12`), or
    /// empty for unpartitioned writes. Sanitized for object-store paths;
    /// only informational — the authoritative value is `partition`.
    pub path: String,
    /// Iceberg partition struct for `DataFile` descriptors.
    pub partition: Struct,
    /// The rows of the input batch that fall in this partition.
    pub batch: RecordBatch,
}

/// Splits arrow batches into per-partition-value groups per a transform list.
pub struct PartitionFanout {
    /// (column index, field name, transform function) per partition field.
    fields: Vec<(usize, String, BoxedTransformFunction)>,
}

impl PartitionFanout {
    /// Resolve partition columns against the arrow schema of the batches
    /// that will be written. Fails on unknown columns or transforms.
    pub fn try_new(
        arrow_schema: &arrow::datatypes::Schema,
        fields: &[PartitionTransformField],
    ) -> Result<Self, LakehouseError> {
        let fields = fields
            .iter()
            .map(|f| {
                let idx = arrow_schema.index_of(&f.column).map_err(|_| {
                    LakehouseError::Iceberg(format!(
                        "PARTITIONED BY column `{}` is missing from the result schema",
                        f.column
                    ))
                })?;
                let func = create_transform_function(&f.transform)
                    .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
                Ok((idx, f.field_name(), func))
            })
            .collect::<Result<Vec<_>, LakehouseError>>()?;
        Ok(Self { fields })
    }

    /// True when no partition fields are configured (passthrough writes).
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Split `batch` into per-partition-value sub-batches.
    ///
    /// An unpartitioned fanout returns the batch unchanged under an empty
    /// partition struct. Zero-row batches produce one empty group so
    /// schema-only writes keep flowing.
    pub fn split(&self, batch: &RecordBatch) -> Result<Vec<PartitionedBatch>, LakehouseError> {
        if self.fields.is_empty() || batch.num_rows() == 0 {
            // Zero-row batches carry no partition values; callers drop empty
            // groups before any file is written.
            return Ok(vec![PartitionedBatch {
                key: String::new(),
                path: String::new(),
                partition: Struct::empty(),
                batch: batch.clone(),
            }]);
        }

        // Compute the transformed partition value per field, per row.
        let mut per_field: Vec<Vec<Option<Datum>>> = Vec::with_capacity(self.fields.len());
        for (idx, _, func) in &self.fields {
            let array = batch.column(*idx);
            let mut out = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let value = match datum_at(array, row)? {
                    Some(datum) => func
                        .transform_literal(&datum)
                        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?,
                    None => None,
                };
                out.push(value);
            }
            per_field.push(out);
        }

        // Group row indices by canonical key.
        let mut groups: BTreeMap<String, (Vec<u64>, Vec<Option<Datum>>)> = BTreeMap::new();
        for row in 0..batch.num_rows() {
            let values: Vec<Option<Datum>> = per_field
                .iter()
                .map(|col| col.get(row).cloned().flatten())
                .collect();
            let key = values
                .iter()
                .map(|v| match v {
                    Some(d) => format!("{d}"),
                    None => "\u{0}null".to_string(),
                })
                .collect::<Vec<_>>()
                .join("\u{1}");
            groups
                .entry(key)
                .or_insert_with(|| (Vec::new(), values))
                .0
                .push(row as u64);
        }

        let mut out = Vec::with_capacity(groups.len());
        for (key, (rows, values)) in groups {
            let indices = arrow::array::UInt64Array::from(rows);
            let columns = batch
                .columns()
                .iter()
                .map(|c| arrow::compute::take(c, &indices, None))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
            let sub = RecordBatch::try_new(batch.schema(), columns)
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
            let path = self
                .fields
                .iter()
                .zip(values.iter())
                .map(|((_, name, _), v)| {
                    let raw = match v {
                        Some(d) => path_value(d),
                        None => "null".to_string(),
                    };
                    format!("{name}={}", sanitize_path_segment(&raw))
                })
                .collect::<Vec<_>>()
                .join("/");
            let partition = values
                .into_iter()
                .map(|v| v.map(|d| Literal::Primitive(d.literal().clone())))
                .collect::<Struct>();
            out.push(PartitionedBatch {
                key,
                path,
                partition,
                batch: sub,
            });
        }
        Ok(out)
    }
}

/// Render a partition datum for a hive path segment. Strings render raw —
/// `Datum`'s `Display` wraps them in double quotes, which would sanitize to
/// `_value_` — everything else uses its `Display` form.
fn path_value(datum: &Datum) -> String {
    match datum.literal() {
        PrimitiveLiteral::String(s) => s.clone(),
        _ => format!("{datum}"),
    }
}

/// Keep hive-style path segments object-store safe. The partition struct in
/// table metadata is authoritative; this only shapes file locations.
fn sanitize_path_segment(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':' | '+') {
                c
            } else {
                '_'
            }
        })
        .take(128)
        .collect()
}

/// Convert one arrow array element to an iceberg [`Datum`], mirroring the
/// type map of `arrow_schema_to_iceberg_schema` (hand-rolled because
/// iceberg-rust 0.9 pins arrow 57). Returns `Ok(None)` for nulls.
fn datum_at(array: &Arc<dyn Array>, row: usize) -> Result<Option<Datum>, LakehouseError> {
    if array.is_null(row) {
        return Ok(None);
    }
    macro_rules! prim {
        ($ty:ty, $ctor:expr) => {{
            let a = array
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| LakehouseError::Iceberg("array downcast failed".to_string()))?;
            #[allow(clippy::redundant_closure_call)]
            Ok(Some($ctor(a.value(row))))
        }};
    }
    match array.data_type() {
        DataType::Boolean => prim!(BooleanArray, Datum::bool),
        DataType::Int8 => prim!(Int8Array, |v: i8| Datum::int(i32::from(v))),
        DataType::Int16 => prim!(Int16Array, |v: i16| Datum::int(i32::from(v))),
        DataType::Int32 => prim!(Int32Array, Datum::int),
        DataType::Int64 => prim!(Int64Array, Datum::long),
        DataType::UInt8 => prim!(UInt8Array, |v: u8| Datum::long(i64::from(v))),
        DataType::UInt16 => prim!(UInt16Array, |v: u16| Datum::long(i64::from(v))),
        DataType::UInt32 => prim!(UInt32Array, |v: u32| Datum::long(i64::from(v))),
        DataType::Float32 => prim!(Float32Array, Datum::float),
        DataType::Float64 => prim!(Float64Array, Datum::double),
        DataType::Utf8 => prim!(StringArray, |v: &str| Datum::string(v)),
        DataType::LargeUtf8 => prim!(LargeStringArray, |v: &str| Datum::string(v)),
        DataType::Utf8View => prim!(StringViewArray, |v: &str| Datum::string(v)),
        DataType::Date32 => prim!(Date32Array, Datum::date),
        DataType::Date64 => prim!(Date64Array, |v: i64| Datum::date((v / 86_400_000) as i32)),
        DataType::Timestamp(unit, tz) => {
            let ts_err = || LakehouseError::Iceberg("timestamp downcast failed".to_string());
            let any = array.as_any();
            let micros = match unit {
                TimeUnit::Second => {
                    any.downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(ts_err)?
                        .value(row)
                        * 1_000_000
                }
                TimeUnit::Millisecond => {
                    any.downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(ts_err)?
                        .value(row)
                        * 1_000
                }
                TimeUnit::Microsecond => any
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(ts_err)?
                    .value(row),
                TimeUnit::Nanosecond => {
                    any.downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(ts_err)?
                        .value(row)
                        / 1_000
                }
            };
            Ok(Some(if tz.is_some() {
                Datum::timestamptz_micros(micros)
            } else {
                Datum::timestamp_micros(micros)
            }))
        }
        other => Err(LakehouseError::Iceberg(format!(
            "partition column type {other} is not supported for partitioned writes; \
             cast it in the SELECT"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema as ArrowSchema};
    use iceberg::spec::{NestedField, PrimitiveType, Type};

    fn arrow_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ])
    }

    fn batch(ids: Vec<i64>, regions: Vec<Option<&str>>, ts: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(arrow_schema()),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(regions)),
                Arc::new(TimestampMicrosecondArray::from(ts)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn parses_bare_column_and_transforms() {
        assert_eq!(
            parse_partition_transform("region").unwrap(),
            PartitionTransformField {
                column: "region".to_string(),
                transform: Transform::Identity
            }
        );
        assert_eq!(
            parse_partition_transform("bucket(16, id)").unwrap(),
            PartitionTransformField {
                column: "id".to_string(),
                transform: Transform::Bucket(16)
            }
        );
        assert_eq!(
            parse_partition_transform("TRUNCATE(4, \"sku\")").unwrap(),
            PartitionTransformField {
                column: "sku".to_string(),
                transform: Transform::Truncate(4)
            }
        );
        assert_eq!(
            parse_partition_transform("days(ts)").unwrap().transform,
            Transform::Day
        );
        assert!(parse_partition_transform("bucket(0, id)").is_err());
        assert!(parse_partition_transform("median(id)").is_err());
        assert!(parse_partition_transform("").is_err());
    }

    #[test]
    fn spec_builder_resolves_source_ids_and_names() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "region",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();
        let spec = build_unbound_partition_spec(
            &[
                parse_partition_transform("region").unwrap(),
                parse_partition_transform("bucket(8, id)").unwrap(),
            ],
            &schema,
        )
        .unwrap();
        let fields = spec.fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].source_id, 2);
        assert_eq!(fields[0].name, "region");
        assert_eq!(fields[1].source_id, 1);
        assert_eq!(fields[1].name, "id_bucket");
        assert!(
            build_unbound_partition_spec(&[parse_partition_transform("missing").unwrap()], &schema)
                .is_err()
        );
    }

    #[test]
    fn fanout_groups_by_identity_value_including_nulls() {
        let fanout = PartitionFanout::try_new(
            &arrow_schema(),
            &[parse_partition_transform("region").unwrap()],
        )
        .unwrap();
        let split = fanout
            .split(&batch(
                vec![1, 2, 3, 4],
                vec![Some("eu"), Some("us"), Some("eu"), None],
                vec![0, 0, 0, 0],
            ))
            .unwrap();
        assert_eq!(split.len(), 3);
        let total: usize = split.iter().map(|s| s.batch.num_rows()).sum();
        assert_eq!(total, 4);
        let eu = split.iter().find(|s| s.path == "region=eu").unwrap();
        assert_eq!(eu.batch.num_rows(), 2);
        let null_group = split.iter().find(|s| s.path == "region=null").unwrap();
        assert_eq!(null_group.batch.num_rows(), 1);
        assert!(null_group.partition.fields()[0].is_none());
    }

    #[test]
    fn fanout_bucket_matches_iceberg_reference_value() {
        // Iceberg spec appendix: murmur3(34L) = 2017239379; 2017239379 % 4 = 3.
        let fanout = PartitionFanout::try_new(
            &arrow_schema(),
            &[parse_partition_transform("bucket(4, id)").unwrap()],
        )
        .unwrap();
        let split = fanout
            .split(&batch(vec![34], vec![Some("eu")], vec![0]))
            .unwrap();
        assert_eq!(split.len(), 1);
        assert_eq!(split[0].path, "id_bucket=3");
    }

    #[test]
    fn fanout_day_transform_groups_by_calendar_day() {
        let fanout = PartitionFanout::try_new(
            &arrow_schema(),
            &[parse_partition_transform("day(ts)").unwrap()],
        )
        .unwrap();
        let day = 86_400_000_000i64; // one day in micros
        let split = fanout
            .split(&batch(
                vec![1, 2, 3],
                vec![Some("eu"), Some("eu"), Some("eu")],
                vec![0, 1_000_000, day + 1],
            ))
            .unwrap();
        assert_eq!(split.len(), 2, "two distinct days");
        assert_eq!(split[0].batch.num_rows() + split[1].batch.num_rows(), 3);
    }

    #[test]
    fn empty_fanout_passes_batch_through_unpartitioned() {
        let fanout = PartitionFanout::try_new(&arrow_schema(), &[]).unwrap();
        let split = fanout
            .split(&batch(vec![1], vec![Some("eu")], vec![0]))
            .unwrap();
        assert_eq!(split.len(), 1);
        assert!(split[0].path.is_empty());
        assert_eq!(split[0].partition, Struct::empty());
    }
}
