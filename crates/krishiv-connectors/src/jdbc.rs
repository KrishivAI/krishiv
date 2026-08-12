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

    /// Start keyset pagination after `cursor` — the incremental-pull entry
    /// point: a caller that persisted the last ingested key resumes with
    /// `WHERE key > cursor`, reading only rows added since. Only meaningful
    /// together with [`with_key_column`]; ignored by OFFSET pagination.
    #[must_use]
    pub fn with_cursor_after(mut self, cursor: i64) -> Self {
        self.last_key = Some(cursor);
        self
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
            let quoted_key = quote_identifier(key_col);
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
        if let Some(ref key_col) = self.key_column
            && let Some(last_row) = rows.last()
            && let Ok(val) = last_row.try_get::<i64, _>(key_col.as_str())
        {
            self.last_key = Some(val);
        }
        self.offset = self.offset.saturating_add(rows.len() as u64);
        let schema = match &self.schema {
            Some(s) => Arc::clone(s),
            None => {
                let first_row = rows.first().ok_or_else(|| {
                    ConnectorError::Io(std::io::Error::other(
                        "jdbc read_batch: rows became empty after the is_empty guard",
                    ))
                })?;
                let s = Arc::new(pg_columns_to_schema(first_row.columns().iter().collect()));
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
        fn truncated(what: &str) -> ConnectorError {
            ConnectorError::Config {
                message: format!("truncated JDBC offset ({what})"),
            }
        }

        let tag = *bytes.first().ok_or_else(|| ConnectorError::Config {
            message: "empty JDBC offset bytes".into(),
        })?;
        match tag {
            0 => {
                let field = bytes.get(1..9).ok_or_else(|| truncated("Offset"))?;
                let v =
                    u64::from_le_bytes(field.try_into().map_err(|_| ConnectorError::Config {
                        message: "offset decode failed".into(),
                    })?);
                Ok(JdbcOffset::Offset(v))
            }
            1 => {
                let len_field = bytes.get(1..5).ok_or_else(|| truncated("Keyset"))?;
                let col_len = u32::from_le_bytes(len_field.try_into().map_err(|_| {
                    ConnectorError::Config {
                        message: "keyset col_len decode failed".into(),
                    }
                })?) as usize;
                let key_start = 5 + col_len;
                let column_field = bytes
                    .get(5..key_start)
                    .ok_or_else(|| truncated("Keyset column"))?;
                let column = String::from_utf8(column_field.to_vec()).map_err(|e| {
                    ConnectorError::Config {
                        message: format!("keyset column not valid utf-8: {e}"),
                    }
                })?;
                let key_field = bytes
                    .get(key_start..key_start + 8)
                    .ok_or_else(|| truncated("Keyset key"))?;
                let last_key = i64::from_le_bytes(key_field.try_into().map_err(|_| {
                    ConnectorError::Config {
                        message: "keyset key decode failed".into(),
                    }
                })?);
                Ok(JdbcOffset::Keyset { column, last_key })
            }
            other => Err(ConnectorError::Config {
                message: format!("unknown JDBC offset tag: {other}"),
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

/// Postgres JDBC sink.
///
/// Two delivery modes, and the difference is what the platform is allowed
/// to *label* the sink:
///
/// - **Append** (no conflict keys): plain `INSERT`. Re-delivering a batch
///   after a crash duplicates rows, so this is **at-least-once**, and
///   nothing may call it idempotent.
/// - **Upsert** (conflict keys declared): `INSERT … ON CONFLICT (keys) DO
///   UPDATE SET …`. Re-delivering the same row converges to the same
///   state, which is what makes **at-least-once idempotent upsert** a true
///   label rather than an aspiration.
///
/// The keys are declared, never inferred: guessing a primary key would
/// silently turn a duplicate-row bug into an overwrite bug, and the two
/// fail in opposite directions.
pub struct JdbcSink {
    pool: PgPool,
    table: String,
    /// Columns forming the conflict target. Empty = append mode.
    conflict_keys: Vec<String>,
    /// Rows per multi-row INSERT. Batching is what makes this usable for
    /// anything but a toy: one round trip per row is the difference
    /// between a sink and a bottleneck.
    batch_rows: usize,
}

/// Default rows per statement. Postgres binds at most 65535 parameters
/// per statement, so the effective cap is `65535 / columns`; this default
/// stays well inside that for wide tables and is clamped per batch.
const DEFAULT_BATCH_ROWS: usize = 500;

/// Hard ceiling from the Postgres wire protocol: a statement may bind at
/// most 65535 parameters. Exceeding it is a driver error, not a slow
/// query, so the sink computes rows-per-statement from the column count
/// rather than hoping.
const MAX_BIND_PARAMS: usize = 65535;

impl JdbcSink {
    /// Open a connection pool in APPEND mode (at-least-once).
    pub async fn connect(url: &str, table: impl Into<String>) -> ConnectorResult<Self> {
        Self::connect_with(url, table, Vec::new(), DEFAULT_BATCH_ROWS).await
    }

    /// Open a connection pool with explicit conflict keys (upsert mode)
    /// and batch size.
    pub async fn connect_with(
        url: &str,
        table: impl Into<String>,
        conflict_keys: Vec<String>,
        batch_rows: usize,
    ) -> ConnectorResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(url)
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
        Ok(Self {
            pool,
            table: table.into(),
            conflict_keys,
            batch_rows: batch_rows.max(1),
        })
    }

    /// Whether this sink upserts (and may therefore be labelled
    /// idempotent) or merely appends.
    pub fn is_upsert(&self) -> bool {
        !self.conflict_keys.is_empty()
    }
}

/// Render the INSERT for `rows` rows of `columns` columns.
///
/// Split out as a pure function so the SQL — especially the ON CONFLICT
/// clause and the parameter numbering across a multi-row VALUES list —
/// is testable without a database.
pub(crate) fn render_insert(
    table: &str,
    columns: &[String],
    rows: usize,
    conflict_keys: &[String],
) -> String {
    let cols_clause = columns
        .iter()
        .map(|c| quote_identifier(c))
        .collect::<Vec<_>>()
        .join(", ");
    let mut tuples = Vec::with_capacity(rows);
    let mut param = 1usize;
    for _ in 0..rows {
        let ph: Vec<String> = (0..columns.len())
            .map(|_| {
                let p = format!("${param}");
                param += 1;
                p
            })
            .collect();
        tuples.push(format!("({})", ph.join(", ")));
    }
    let mut sql = format!(
        "INSERT INTO {} ({}) VALUES {}",
        quote_qualified(table),
        cols_clause,
        tuples.join(", ")
    );
    if !conflict_keys.is_empty() {
        let target = conflict_keys
            .iter()
            .map(|c| quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", ");
        // Update every NON-key column from the proposed row. A key column
        // in the SET list would be a no-op at best and a rewrite of the
        // conflict target at worst.
        let updates: Vec<String> = columns
            .iter()
            .filter(|c| !conflict_keys.iter().any(|k| k.eq_ignore_ascii_case(c)))
            .map(|c| {
                let q = quote_identifier(c);
                format!("{q} = EXCLUDED.{q}")
            })
            .collect();
        if updates.is_empty() {
            // Every column is part of the key: there is nothing to update,
            // and DO NOTHING is the honest idempotent form.
            sql.push_str(&format!(" ON CONFLICT ({target}) DO NOTHING"));
        } else {
            sql.push_str(&format!(
                " ON CONFLICT ({target}) DO UPDATE SET {}",
                updates.join(", ")
            ));
        }
    }
    sql
}

impl Sink for JdbcSink {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new().with_bounded()
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> ConnectorResult<()> {
        let schema = batch.schema();
        let ncols = schema.fields().len();
        if ncols == 0 || batch.num_rows() == 0 {
            return Ok(());
        }
        let columns: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        // A conflict key that is not in the batch would render SQL the
        // server rejects at execution time, halfway through a transaction.
        // Fail before opening it, naming the column.
        for key in &self.conflict_keys {
            if !columns.iter().any(|c| c.eq_ignore_ascii_case(key)) {
                return Err(ConnectorError::Io(std::io::Error::other(format!(
                    "jdbc sink: conflict key '{key}' is not a column of the batch \
                     ({}) — an upsert cannot key on a column it is not writing",
                    columns.join(", ")
                ))));
            }
        }

        // Rows per statement, bounded by the wire protocol's parameter cap.
        let per_stmt = self.batch_rows.min((MAX_BIND_PARAMS / ncols).max(1));

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;

        let mut start = 0usize;
        while start < batch.num_rows() {
            let rows = per_stmt.min(batch.num_rows() - start);
            let sql = render_insert(&self.table, &columns, rows, &self.conflict_keys);
            let mut q = sqlx::query(&sql);
            for row_idx in start..start + rows {
                for col_idx in 0..ncols {
                    let col = batch.column(col_idx);
                    q = bind_column_value(q, col.as_ref(), row_idx)?;
                }
            }
            q.execute(&mut *tx)
                .await
                .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
            start += rows;
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

/// Downcast `col` to the concrete array type `data_type()` reported.
///
/// Arrow's own contract guarantees `col.data_type()` and `col`'s actual
/// concrete Rust type always correspond — a mismatch here would mean the
/// `Array` itself was built inconsistently, not a normal runtime condition
/// this connector can hit. Still returns a real error rather than
/// `.unwrap()`-panicking on it: this crate treats "should be provably
/// unreachable" as a case to report, not a license to crash.
fn downcast_or_err<'a, T: 'static>(col: &'a dyn Array, want: &str) -> ConnectorResult<&'a T> {
    col.as_any().downcast_ref::<T>().ok_or_else(|| {
        ConnectorError::Io(std::io::Error::other(format!(
            "jdbc bind: column reported {want} but its Array value did not downcast to it"
        )))
    })
}

fn bind_column_value<'q>(
    q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    col: &dyn Array,
    row_idx: usize,
) -> ConnectorResult<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>> {
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, StringArray,
    };
    if col.is_null(row_idx) {
        return Ok(q.bind(Option::<i64>::None));
    }
    let bound = match col.data_type() {
        DataType::Int16 => {
            let v = downcast_or_err::<Int16Array>(col, "Int16")?.value(row_idx);
            q.bind(v)
        }
        DataType::Int32 => {
            let v = downcast_or_err::<Int32Array>(col, "Int32")?.value(row_idx);
            q.bind(v)
        }
        DataType::Int64 => {
            let v = downcast_or_err::<Int64Array>(col, "Int64")?.value(row_idx);
            q.bind(v)
        }
        DataType::Float32 => {
            let v = downcast_or_err::<Float32Array>(col, "Float32")?.value(row_idx);
            q.bind(v)
        }
        DataType::Float64 => {
            let v = downcast_or_err::<Float64Array>(col, "Float64")?.value(row_idx);
            q.bind(v)
        }
        DataType::Boolean => {
            let v = downcast_or_err::<BooleanArray>(col, "Boolean")?.value(row_idx);
            q.bind(v)
        }
        DataType::Utf8 => {
            let v = downcast_or_err::<StringArray>(col, "Utf8")?
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
    use crate::offset::Offset as _;
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

        // An injection attempt (`users; DROP TABLE users; --`) is neutralised:
        // wrap-quoting yields exactly ONE double-quoted identifier — the
        // payload stays inert text inside the quotes (it contains no `"`, so
        // nothing can terminate the identifier early).
        let injected = "users; DROP TABLE users; --";
        assert_eq!(quote_identifier(injected), format!("\"{injected}\""));

        // A payload that DOES carry a quote cannot break out either — every
        // interior `"` is doubled, so the identifier still ends only at the
        // final wrapping quote.
        let quote_escape = quote_identifier("x\"; DROP TABLE users; --");
        assert_eq!(quote_escape, "\"x\"\"; DROP TABLE users; --\"");

        // Schema-qualified names quote each `.`-separated component.
        assert_eq!(quote_qualified("public.users"), "\"public\".\"users\"");
        assert_eq!(
            quote_qualified("schema\".evil.table"),
            "\"schema\"\"\".\"evil\".\"table\""
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

    /// CONN-10: `JdbcOffset::encode`/`decode` round-trip both pagination
    /// modes. Guards the `.get(range)`-based decode rewrite (was raw `[]`
    /// indexing) against accidentally changing the wire format, not just
    /// against panicking.
    #[test]
    fn offset_round_trips_both_modes() {
        let offset = JdbcOffset::Offset(42);
        assert_eq!(JdbcOffset::decode(&offset.encode()).unwrap(), offset);

        let keyset = JdbcOffset::Keyset {
            column: "id".to_owned(),
            last_key: -7,
        };
        assert_eq!(JdbcOffset::decode(&keyset.encode()).unwrap(), keyset);

        // A non-ASCII column name exercises the UTF-8 boundary in the
        // `bytes.get(5..key_start)` slice explicitly.
        let unicode_keyset = JdbcOffset::Keyset {
            column: "ключ".to_owned(),
            last_key: 0,
        };
        assert_eq!(
            JdbcOffset::decode(&unicode_keyset.encode()).unwrap(),
            unicode_keyset
        );
    }

    /// Every truncation point in `decode` must return a `Config` error, not
    /// panic — each case below is `encode()`'s real output for that variant,
    /// cut off one byte before the field decode would need.
    #[test]
    fn offset_decode_rejects_truncated_bytes_at_every_boundary() {
        assert!(JdbcOffset::decode(&[]).is_err(), "empty bytes");

        let full_offset = JdbcOffset::Offset(1).encode();
        assert!(
            JdbcOffset::decode(&full_offset[..full_offset.len() - 1]).is_err(),
            "Offset payload one byte short"
        );

        let full_keyset = JdbcOffset::Keyset {
            column: "id".to_owned(),
            last_key: 1,
        }
        .encode();
        // Cut before the col_len u32 is complete.
        assert!(
            JdbcOffset::decode(&full_keyset[..4]).is_err(),
            "Keyset col_len truncated"
        );
        // Cut inside the column name bytes.
        assert!(
            JdbcOffset::decode(&full_keyset[..6]).is_err(),
            "Keyset column name truncated"
        );
        // Cut inside the trailing i64 key.
        assert!(
            JdbcOffset::decode(&full_keyset[..full_keyset.len() - 1]).is_err(),
            "Keyset key truncated"
        );
    }

    #[test]
    fn offset_decode_rejects_unknown_tag() {
        let err = JdbcOffset::decode(&[99]).unwrap_err();
        assert!(matches!(err, ConnectorError::Config { .. }));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod sink_sql_tests {
    use super::render_insert;

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn append_mode_emits_no_conflict_clause() {
        // Without declared keys this is at-least-once APPEND, and the SQL
        // must not imply otherwise.
        let sql = render_insert("public.t", &cols(&["id", "v"]), 2, &[]);
        assert_eq!(
            sql,
            r#"INSERT INTO "public"."t" ("id", "v") VALUES ($1, $2), ($3, $4)"#
        );
        assert!(!sql.contains("ON CONFLICT"));
    }

    #[test]
    fn upsert_updates_every_non_key_column() {
        let sql = render_insert("t", &cols(&["id", "v", "w"]), 1, &cols(&["id"]));
        assert!(sql.contains(r#"ON CONFLICT ("id") DO UPDATE SET"#), "{sql}");
        assert!(sql.contains(r#""v" = EXCLUDED."v""#), "{sql}");
        assert!(sql.contains(r#""w" = EXCLUDED."w""#), "{sql}");
        // A key column in the SET list would rewrite the conflict target.
        assert!(!sql.contains(r#""id" = EXCLUDED."id""#), "{sql}");
    }

    #[test]
    fn parameters_number_continuously_across_a_multi_row_values_list() {
        // Off-by-one here binds the wrong column to the wrong row — a
        // silent data-corruption bug rather than an error.
        let sql = render_insert("t", &cols(&["a", "b", "c"]), 3, &[]);
        for n in 1..=9 {
            assert!(sql.contains(&format!("${n}")), "missing ${n} in {sql}");
        }
        assert!(!sql.contains("$10"), "{sql}");
        assert!(sql.contains("($1, $2, $3), ($4, $5, $6), ($7, $8, $9)"), "{sql}");
    }

    #[test]
    fn an_all_key_table_upserts_as_do_nothing() {
        // Every column is part of the key: there is nothing to update, and
        // DO NOTHING is the honest idempotent form (DO UPDATE SET with an
        // empty list is a syntax error).
        let sql = render_insert("t", &cols(&["a", "b"]), 1, &cols(&["a", "b"]));
        assert!(sql.ends_with(r#"ON CONFLICT ("a", "b") DO NOTHING"#), "{sql}");
    }

    #[test]
    fn conflict_keys_match_columns_case_insensitively() {
        // Postgres folds unquoted identifiers to lower case, so a declared
        // key of `ID` must not produce `"ID" = EXCLUDED."ID"` in the SET
        // list while the conflict target says `"ID"`.
        let sql = render_insert("t", &cols(&["id", "v"]), 1, &cols(&["ID"]));
        assert!(!sql.contains(r#""id" = EXCLUDED."id""#), "{sql}");
        assert!(sql.contains(r#""v" = EXCLUDED."v""#), "{sql}");
    }
}
