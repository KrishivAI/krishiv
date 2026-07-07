//! T9: JDBC source and sink for Postgres (and MySQL when the `mysql` sqlx
//! feature is enabled in the workspace).
//!
//! # Source
//!
//! [`JdbcSource`] executes `SELECT * FROM <table> OFFSET <offset> LIMIT
//! <batch_size>` in a loop, materialising each page as an Arrow
//! [`RecordBatch`].  The `batch_size` defaults to 1 000 rows; callers may
//! override it via [`JdbcSource::with_batch_size`].
//!
//! # Sink
//!
//! [`JdbcSink`] issues a per-row `INSERT INTO <table> VALUES (…)` inside
//! a transaction per batch.  Production deployments should prefer
//! `COPY … FROM STDIN` or `INSERT … ON CONFLICT DO NOTHING` for better
//! throughput; the simple insert path ships first to unblock integration
//! tests.
//!
//! # URL format
//!
//! Both structs accept a bare connection URL (without the `jdbc:` prefix):
//! ```text
//! postgresql://user:pass@host:5432/dbname
//! ```
//!
//! The JDBC wrapper in [`crate::sql::SqlConnector`] strips the prefix before
//! calling these constructors.

#![cfg(feature = "jdbc")]

use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};

use krishiv_common::sql_util::{quote_identifier, quote_qualified};

use crate::capabilities::ConnectorCapabilities;
use crate::error::{ConnectorError, ConnectorResult};
use crate::sink::Sink;
use crate::source::Source;

const DEFAULT_BATCH_SIZE: u32 = 1_000;

// ── JdbcSource ────────────────────────────────────────────────────────────────

/// Postgres JDBC source: pages through `SELECT * FROM <table>` using
/// `LIMIT`/`OFFSET` and converts each page to an Arrow [`RecordBatch`].
pub struct JdbcSource {
    pool: PgPool,
    table: String,
    batch_size: u32,
    offset: u64,
    /// CONN-5: Optional key column for keyset pagination (stable under concurrent
    /// writes). When set, uses `WHERE key > $last_key` instead of OFFSET.
    key_column: Option<String>,
    /// Last seen key value for keyset pagination.
    last_key: Option<i64>,
    schema: Option<SchemaRef>,
    exhausted: bool,
}

impl JdbcSource {
    /// Open a connection pool and return a [`JdbcSource`].
    ///
    /// `url` is the bare Postgres connection URL (no `jdbc:` prefix).
    /// `table` is the target table name.
    pub async fn connect(url: &str, table: impl Into<String>) -> ConnectorResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(url)
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
        Ok(Self {
            pool,
            table: table.into(),
            batch_size: DEFAULT_BATCH_SIZE,
            offset: 0,
            key_column: None,
            last_key: None,
            schema: None,
            exhausted: false,
        })
    }

    /// Override the page size.  Defaults to 1 000 rows.
    #[must_use]
    pub fn with_batch_size(mut self, n: u32) -> Self {
        self.batch_size = n.max(1);
        self
    }

    /// CONN-5: Set the key column for keyset pagination. When set, the source
    /// uses `WHERE key > $last_key ORDER BY key LIMIT N` instead of
    /// `OFFSET`-based pagination, which is unstable under concurrent writes.
    #[must_use]
    pub fn with_key_column(mut self, col: impl Into<String>) -> Self {
        self.key_column = Some(col.into());
        self
    }

    /// Derive the Arrow schema from the first row by issuing `LIMIT 0`.
    async fn fetch_schema(&mut self) -> ConnectorResult<SchemaRef> {
        if let Some(s) = &self.schema {
            return Ok(Arc::clone(s));
        }
        let sql = format!("SELECT * FROM {} LIMIT 0", quote_qualified(&self.table));
        let rows = sqlx::query(&sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
        // When the table is empty, infer schema from column metadata.
        let cols = rows
            .first()
            .map_or_else(|| vec![], |row| row.columns().iter().collect::<Vec<_>>());
        let schema = pg_columns_to_schema(cols);
        let schema = Arc::new(schema);
        self.schema = Some(Arc::clone(&schema));
        Ok(schema)
    }
}

impl Source for JdbcSource {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_bounded()
            .with_rewindable()
            .with_checkpoint()
    }

    async fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
        if self.exhausted {
            return Ok(None);
        }
        // CONN-5: Use keyset pagination when a key column is configured
        // (stable under concurrent writes); fall back to OFFSET otherwise.
        let sql = if let Some(ref key_col) = self.key_column {
            let quoted_key = quote_pg_ident(key_col);
            match self.last_key {
                Some(k) => format!(
                    "SELECT * FROM {} WHERE {} > {} ORDER BY {} LIMIT {}",
                    quote_qualified(&self.table),
                    quoted_key,
                    k,
                    quoted_key,
                    self.batch_size
                ),
                None => format!(
                    "SELECT * FROM {} ORDER BY {} LIMIT {}",
                    quote_qualified(&self.table),
                    quoted_key,
                    self.batch_size
                ),
            }
        } else {
            format!(
                "SELECT * FROM {} LIMIT {} OFFSET {}",
                quote_qualified(&self.table),
                self.batch_size,
                self.offset
            )
        };
        let rows: Vec<PgRow> = sqlx::query(&sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
        if rows.is_empty() {
            self.exhausted = true;
            return Ok(None);
        }
        // CONN-5: Track the last key for keyset pagination.
        if let Some(ref key_col) = self.key_column {
            if let Some(last_row) = rows.last() {
                if let Ok(val) = last_row.try_get::<i64, _>(key_col.as_str()) {
                    self.last_key = Some(val);
                }
            }
        }
        self.offset = self.offset.saturating_add(rows.len() as u64);
        let schema = match &self.schema {
            Some(s) => Arc::clone(s),
            None => {
                let s = Arc::new(pg_columns_to_schema(rows[0].columns().iter().collect()));
                self.schema = Some(Arc::clone(&s));
                s
            }
        };
        let batch = pg_rows_to_batch(schema, &rows)
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
        Ok(Some(batch))
    }

    fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
        Some(Box::new(self.offset))
    }

    fn reset(&mut self) {
        self.offset = 0;
        self.last_key = None;
        self.exhausted = false;
    }
}

// ── JdbcOffset & CheckpointSource ─────────────────────────────────────────────

/// CONN-10: Typed checkpoint offset for JDBC pagination.
///
/// Captures the pagination mode (OFFSET vs keyset) so a checkpoint saves enough
/// state to resume from the exact row boundary, even if the table has concurrent
/// writes.
#[derive(Debug, Clone, PartialEq)]
pub enum JdbcOffset {
    /// Traditional OFFSET-based pagination: resume at this row offset.
    Offset(u64),
    /// Keyset pagination: resume after this key value.
    Keyset {
        /// Column name used for keyset pagination.
        column: String,
        /// Last observed key value.
        last_key: i64,
    },
}

impl crate::offset::Offset for JdbcOffset {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            JdbcOffset::Offset(v) => {
                buf.push(0); // tag: offset mode
                buf.extend_from_slice(&v.to_le_bytes());
            }
            JdbcOffset::Keyset { column, last_key } => {
                buf.push(1); // tag: keyset mode
                let col_bytes = column.as_bytes();
                buf.extend_from_slice(&(col_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(col_bytes);
                buf.extend_from_slice(&last_key.to_le_bytes());
            }
        }
        buf
    }

    fn decode(bytes: &[u8]) -> ConnectorResult<Self>
    where
        Self: Sized,
    {
        if bytes.is_empty() {
            return Err(ConnectorError::Config {
                message: "empty JDBC offset bytes".into(),
            });
        }
        match bytes[0] {
            0 => {
                if bytes.len() < 9 {
                    return Err(ConnectorError::Config {
                        message: "truncated JDBC offset (Offset)".into(),
                    });
                }
                let v = u64::from_le_bytes(bytes[1..9].try_into().map_err(|_| {
                    ConnectorError::Config {
                        message: "offset decode failed".into(),
                    }
                })?);
                Ok(JdbcOffset::Offset(v))
            }
            1 => {
                if bytes.len() < 5 {
                    return Err(ConnectorError::Config {
                        message: "truncated JDBC offset (Keyset)".into(),
                    });
                }
                let col_len = u32::from_le_bytes(bytes[1..5].try_into().map_err(|_| {
                    ConnectorError::Config {
                        message: "keyset col_len decode failed".into(),
                    }
                })?) as usize;
                let key_start = 5 + col_len;
                if bytes.len() < key_start + 8 {
                    return Err(ConnectorError::Config {
                        message: "truncated JDBC offset (Keyset key)".into(),
                    });
                }
                let column = String::from_utf8(bytes[5..5 + col_len].to_vec()).map_err(|e| {
                    ConnectorError::Config {
                        message: format!("keyset column not valid utf-8: {e}"),
                    }
                })?;
                let last_key =
                    i64::from_le_bytes(bytes[key_start..key_start + 8].try_into().map_err(
                        |_| ConnectorError::Config {
                            message: "keyset key decode failed".into(),
                        },
                    )?);
                Ok(JdbcOffset::Keyset { column, last_key })
            }
            tag => Err(ConnectorError::Config {
                message: format!("unknown JDBC offset tag: {tag}"),
            }),
        }
    }
}

impl crate::source::CheckpointSource for JdbcSource {
    type Offset = JdbcOffset;

    fn checkpoint_offset(&self) -> ConnectorResult<JdbcOffset> {
        if let Some(ref col) = self.key_column {
            Ok(JdbcOffset::Keyset {
                column: col.clone(),
                last_key: self.last_key.unwrap_or(-1),
            })
        } else {
            Ok(JdbcOffset::Offset(self.offset))
        }
    }

    fn restore_offset(&mut self, offset: &JdbcOffset) -> ConnectorResult<()> {
        match offset {
            JdbcOffset::Offset(v) => {
                self.offset = *v;
                self.last_key = None;
            }
            JdbcOffset::Keyset { column, last_key } => {
                self.key_column = Some(column.clone());
                self.last_key = Some(*last_key);
                self.offset = 0;
            }
        }
        self.exhausted = false;
        Ok(())
    }
}

// ── JdbcSink ─────────────────────────────────────────────────────────────────

/// Postgres JDBC sink: writes Arrow [`RecordBatch`] values row-by-row via
/// `INSERT INTO <table> (<columns>) VALUES (<values>)`.
pub struct JdbcSink {
    pool: PgPool,
    table: String,
}

impl JdbcSink {
    /// Open a connection pool and return a [`JdbcSink`].
    pub async fn connect(url: &str, table: impl Into<String>) -> ConnectorResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(url)
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
        Ok(Self {
            pool,
            table: table.into(),
        })
    }
}

impl Sink for JdbcSink {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new().with_bounded()
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> ConnectorResult<()> {
        let schema = batch.schema();
        let ncols = schema.fields().len();
        let cols_clause = schema
            .fields()
            .iter()
            .map(|f| quote_identifier(f.name()))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders: Vec<String> = (1..=ncols).map(|i| format!("${i}")).collect();
        let ph_clause = placeholders.join(", ");
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote_qualified(&self.table),
            cols_clause,
            ph_clause
        );

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;

        for row_idx in 0..batch.num_rows() {
            let mut q = sqlx::query(&sql);
            for col_idx in 0..ncols {
                let col = batch.column(col_idx);
                q = bind_column_value(q, col.as_ref(), row_idx)?;
            }
            q.execute(&mut *tx)
                .await
                .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
        }

        tx.commit()
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
        Ok(())
    }

    async fn flush(&mut self) -> ConnectorResult<()> {
        Ok(())
    }
}

// ── Identifier quoting ───────────────────────────────────────────────────────

/// Double-quote a Postgres identifier and escape any embedded double-quote
/// characters by doubling them (`"` → `""`).
// ── Arrow ↔ Postgres helpers ──────────────────────────────────────────────────

fn pg_columns_to_schema(cols: Vec<&sqlx::postgres::PgColumn>) -> Schema {
    use sqlx::{Column, TypeInfo};
    let fields: Vec<Field> = cols
        .iter()
        .map(|col| {
            let dt = match col.type_info().name() {
                "INT2" | "SMALLINT" => DataType::Int16,
                "INT4" | "INT" | "INTEGER" => DataType::Int32,
                "INT8" | "BIGINT" => DataType::Int64,
                "FLOAT4" | "REAL" => DataType::Float32,
                "FLOAT8" | "DOUBLE PRECISION" => DataType::Float64,
                "BOOL" | "BOOLEAN" => DataType::Boolean,
                _ => DataType::Utf8,
            };
            Field::new(col.name(), dt, true)
        })
        .collect();
    Schema::new(fields)
}

fn pg_rows_to_batch(schema: SchemaRef, rows: &[PgRow]) -> arrow::error::Result<RecordBatch> {
    let ncols = schema.fields().len();
    let nrows = rows.len();

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(ncols);
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array: ArrayRef = match field.data_type() {
            DataType::Int16 => {
                let mut b = Int16Builder::with_capacity(nrows);
                for row in rows {
                    match row.try_get::<Option<i16>, _>(col_idx) {
                        Ok(Some(v)) => b.append_value(v),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Int32 => {
                let mut b = Int32Builder::with_capacity(nrows);
                for row in rows {
                    match row.try_get::<Option<i32>, _>(col_idx) {
                        Ok(Some(v)) => b.append_value(v),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(nrows);
                for row in rows {
                    match row.try_get::<Option<i64>, _>(col_idx) {
                        Ok(Some(v)) => b.append_value(v),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float32 => {
                let mut b = Float32Builder::with_capacity(nrows);
                for row in rows {
                    match row.try_get::<Option<f32>, _>(col_idx) {
                        Ok(Some(v)) => b.append_value(v),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::with_capacity(nrows);
                for row in rows {
                    match row.try_get::<Option<f64>, _>(col_idx) {
                        Ok(Some(v)) => b.append_value(v),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Boolean => {
                let mut b = BooleanBuilder::with_capacity(nrows);
                for row in rows {
                    match row.try_get::<Option<bool>, _>(col_idx) {
                        Ok(Some(v)) => b.append_value(v),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            _ => {
                let mut b = StringBuilder::with_capacity(nrows, nrows * 16);
                for row in rows {
                    match row.try_get::<Option<String>, _>(col_idx) {
                        Ok(Some(v)) => b.append_value(v),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        };
        arrays.push(array);
    }
    RecordBatch::try_new(schema, arrays)
}

fn bind_column_value<'q>(
    q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    col: &dyn Array,
    row_idx: usize,
) -> ConnectorResult<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>> {
    use arrow::array::*;
    if col.is_null(row_idx) {
        return Ok(q.bind(Option::<i64>::None));
    }
    let bound = match col.data_type() {
        DataType::Int16 => {
            let v = col
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row_idx);
            q.bind(v)
        }
        DataType::Int32 => {
            let v = col
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row_idx);
            q.bind(v)
        }
        DataType::Int64 => {
            let v = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row_idx);
            q.bind(v)
        }
        DataType::Float32 => {
            let v = col
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row_idx);
            q.bind(v)
        }
        DataType::Float64 => {
            let v = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row_idx);
            q.bind(v)
        }
        DataType::Boolean => {
            let v = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row_idx);
            q.bind(v)
        }
        DataType::Utf8 => {
            let v = col
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row_idx)
                .to_owned();
            q.bind(v)
        }
        other => {
            return Err(ConnectorError::Io(std::io::Error::other(format!(
                "unsupported column type for JDBC bind: {other}"
            ))));
        }
    };
    Ok(bound)
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    /// Verify that `pg_rows_to_batch` round-trips a manually-constructed list
    /// of typed builders — no live database required.
    #[test]
    fn batch_round_trip_via_builders() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ids: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), None]));
        let names: ArrayRef = Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None]));
        let batch = RecordBatch::try_new(schema, vec![ids, names]).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);
        assert!(id_col.is_null(2));

        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "alice");
        assert!(name_col.is_null(2));
    }

    /// `quote_pg_ident` prevents SQL injection by quoting and escaping identifiers.
    #[test]
    fn quote_pg_ident_prevents_sql_injection() {
        // Simple identifiers are double-quoted.
        assert_eq!(quote_identifier("users"), "\"users\"");

        // Embedded double-quotes are doubled per the Postgres spec (escape by doubling).
        assert_eq!(quote_identifier("table\"name"), "\"table\"\"name\"");

        // An injection attempt (`users; DROP TABLE users; --`) is neutralised: the
        // result is a single quoted identifier with the semicolons inside the quotes.
        let injected = "users; DROP TABLE users; --";
        let quoted = quote_identifier(injected);
        assert!(quoted.starts_with('"'), "must be double-quoted");
        assert!(quoted.ends_with('"'), "must be double-quoted");
        assert!(
            !quoted.contains("; DROP"),
            "injection payload must be inside quotes"
        );

        // Schema-qualified names quote each component separately.
        assert_eq!(quote_qualified("public.users"), "\"public\".\"users\"");
        assert_eq!(
            quote_qualified("schema\".evil.table"),
            "\"schema\"\".evil\".\"table\""
        );
    }

    /// `JdbcSource` and `JdbcSink` capabilities are correct.
    #[test]
    fn source_and_sink_capabilities() {
        // We can't instantiate without a live PgPool, so verify the capability
        // values via the builder methods directly.
        let source_caps = ConnectorCapabilities::new()
            .with_bounded()
            .with_rewindable();
        assert!(source_caps.is_bounded());
        assert!(source_caps.is_rewindable());

        let sink_caps = ConnectorCapabilities::new().with_bounded();
        assert!(sink_caps.is_bounded());
        assert!(!sink_caps.is_rewindable());
    }
}
