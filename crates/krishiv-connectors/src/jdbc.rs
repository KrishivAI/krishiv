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
    ///
    /// The column must be `BIGINT` (int8); `read_batch` errors if the last
    /// row's key cannot be read as an `i64`.
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
        // CONN-5: Track the last key for keyset pagination. The key column
        // MUST be BIGINT (int8): a decode failure here means every page
        // would re-issue the same WHERE clause and loop on the first page
        // forever, so it is an error rather than a silent skip.
        if let Some(ref key_col) = self.key_column
            && let Some(last_row) = rows.last()
        {
            let val = last_row.try_get::<i64, _>(key_col.as_str()).map_err(|e| {
                ConnectorError::Config {
                    message: format!(
                        "jdbc keyset pagination: key column '{key_col}' must be BIGINT \
                         (int8) — could not read it as i64: {e}"
                    ),
                }
            })?;
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
        /// Last observed key value; `None` when no row has been read yet,
        /// so a restore returns to the no-`WHERE` first page rather than
        /// inventing a sentinel key that would skip real rows.
        last_key: Option<i64>,
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
                // Tag 1 = keyset with a key (wire-compatible with pre-Option
                // checkpoints); tag 2 = keyset before any read (no key).
                match last_key {
                    Some(k) => {
                        buf.push(1);
                        let col_bytes = column.as_bytes();
                        buf.extend_from_slice(&(col_bytes.len() as u32).to_le_bytes());
                        buf.extend_from_slice(col_bytes);
                        buf.extend_from_slice(&k.to_le_bytes());
                    }
                    None => {
                        buf.push(2);
                        let col_bytes = column.as_bytes();
                        buf.extend_from_slice(&(col_bytes.len() as u32).to_le_bytes());
                        buf.extend_from_slice(col_bytes);
                    }
                }
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
                Ok(JdbcOffset::Keyset {
                    column,
                    last_key: Some(last_key),
                })
            }
            2 => {
                let len_field = bytes.get(1..5).ok_or_else(|| truncated("Keyset"))?;
                let col_len = u32::from_le_bytes(len_field.try_into().map_err(|_| {
                    ConnectorError::Config {
                        message: "keyset col_len decode failed".into(),
                    }
                })?) as usize;
                let column_field = bytes
                    .get(5..5 + col_len)
                    .ok_or_else(|| truncated("Keyset column"))?;
                let column = String::from_utf8(column_field.to_vec()).map_err(|e| {
                    ConnectorError::Config {
                        message: format!("keyset column not valid utf-8: {e}"),
                    }
                })?;
                Ok(JdbcOffset::Keyset {
                    column,
                    last_key: None,
                })
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
                last_key: self.last_key,
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
                // `None` = the checkpoint was taken before any read, so the
                // restore returns to the fresh no-WHERE first page.
                self.last_key = *last_key;
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
    // Resolve each declared key to the column's ACTUAL spelling.
    // `quote_identifier` always double-quotes and never folds, so a
    // declared `ID` against a column `id` would render ON CONFLICT ("ID")
    // and fail with 42703 INSIDE the transaction — past the pre-flight
    // guard that matches case-insensitively and exists precisely to fail
    // before the transaction opens.
    let conflict_keys: Vec<String> = conflict_keys
        .iter()
        .map(|k| {
            columns
                .iter()
                .find(|c| c.eq_ignore_ascii_case(k))
                .cloned()
                .unwrap_or_else(|| k.clone())
        })
        .collect();
    let conflict_keys = conflict_keys.as_slice();
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
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
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

        // In UPSERT mode, a statement may not touch the same conflict key
        // twice: Postgres raises 21000 "ON CONFLICT DO UPDATE command
        // cannot affect row a second time" and rolls back the ENTIRE
        // batch, including the duplicate-free rows. Worse, whether two
        // duplicate rows land in the same statement depends on upstream
        // batch sizes, so identical logical input would pass or fail
        // non-deterministically.
        //
        // Deduplicate by conflict key across the whole batch, keeping the
        // LAST occurrence: that is what a sequence of individual upserts
        // would converge to, so batching changes throughput and not
        // semantics.
        let row_order: Vec<usize> = if self.conflict_keys.is_empty() {
            (0..batch.num_rows()).collect()
        } else {
            let key_indices: Vec<usize> = self
                .conflict_keys
                .iter()
                .filter_map(|k| columns.iter().position(|c| c.eq_ignore_ascii_case(k)))
                .collect();
            let mut last_for_key: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            for row_idx in 0..batch.num_rows() {
                let key = composite_row_key(&batch, &key_indices, row_idx);
                last_for_key.insert(key, row_idx);
            }
            let mut kept: Vec<usize> = last_for_key.into_values().collect();
            kept.sort_unstable();
            kept
        };

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;

        for chunk in row_order.chunks(per_stmt) {
            let sql = render_insert(&self.table, &columns, chunk.len(), &self.conflict_keys);
            let mut q = sqlx::query(&sql);
            for &row_idx in chunk {
                for col_idx in 0..ncols {
                    let col = batch.column(col_idx);
                    q = bind_column_value(q, col.as_ref(), row_idx)?;
                }
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
        // A NULL must still be bound at the COLUMN's type. sqlx's
        // `Encode for Option<T>` reports `T::type_info()` even for `None`,
        // so binding Option::<i64>::None declares OID 20 (int8) — and
        // since render_insert emits bare `$n` with no casts, Postgres
        // rejects the whole batch the moment any bool/text/float column
        // holds a NULL. Bind the right None per type instead.
        return Ok(match col.data_type() {
            DataType::Boolean => q.bind(Option::<bool>::None),
            DataType::Int16 => q.bind(Option::<i16>::None),
            DataType::Int32 => q.bind(Option::<i32>::None),
            DataType::Int64 => q.bind(Option::<i64>::None),
            DataType::Float32 => q.bind(Option::<f32>::None),
            DataType::Float64 => q.bind(Option::<f64>::None),
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                q.bind(Option::<String>::None)
            }
            // Unsupported types error on the non-null path below; a NULL
            // of such a type is bound as text, which Postgres accepts for
            // any column since an untyped NULL literal is polymorphic.
            _ => q.bind(Option::<String>::None),
        });
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
        // DataFusion 54 produces Utf8View for string literals and many
        // string kernels, so a sink that only handled Utf8 could not
        // write the engine's own default string type — `INSERT INTO …
        // SELECT 'a'` failed at bind time. LargeUtf8 is here for the same
        // reason: these are representations of the same value, and the
        // wire protocol takes text either way.
        DataType::Utf8View => {
            let v = downcast_or_err::<arrow::array::StringViewArray>(col, "Utf8View")?
                .value(row_idx)
                .to_owned();
            q.bind(v)
        }
        DataType::LargeUtf8 => {
            let v = downcast_or_err::<arrow::array::LargeStringArray>(col, "LargeUtf8")?
                .value(row_idx)
                .to_owned();
            q.bind(v)
        }
        // Temporal types are deliberately NOT bound here: sqlx needs its
        // chrono/time feature for them, and enabling that workspace-wide
        // is a wider change than this leg justifies. The fall-through
        // below names the type, so a date column fails loudly at the
        // first write rather than silently binding as text.
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

    /// `pg_rows_to_batch` with zero rows still honours the schema — the only
    /// input constructible without a live `PgRow`.
    #[test]
    fn pg_rows_to_batch_empty_rows_yield_empty_batch_with_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = pg_rows_to_batch(Arc::clone(&schema), &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), schema);
    }

    /// Manually-built Arrow arrays round-trip through `RecordBatch` — a
    /// builder-level sanity check; `pg_rows_to_batch` over real rows needs a
    /// live database and is covered by integration tests.
    #[test]
    fn arrow_batch_manual_construction_round_trips() {
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

    /// Capability-builder flags compose as the JDBC connectors expect. This
    /// does NOT exercise `JdbcSource::capabilities()` / `JdbcSink::
    /// capabilities()` themselves — those need a live `PgPool` to construct
    /// the connector and are covered by integration tests.
    #[test]
    fn capability_builder_flags_compose_for_source_and_sink_shapes() {
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
            last_key: Some(-7),
        };
        assert_eq!(JdbcOffset::decode(&keyset.encode()).unwrap(), keyset);

        // A non-ASCII column name exercises the UTF-8 boundary in the
        // `bytes.get(5..key_start)` slice explicitly.
        let unicode_keyset = JdbcOffset::Keyset {
            column: "ключ".to_owned(),
            last_key: Some(0),
        };
        assert_eq!(
            JdbcOffset::decode(&unicode_keyset.encode()).unwrap(),
            unicode_keyset
        );
    }

    /// A keyset checkpoint taken BEFORE any read carries `last_key: None`
    /// and must round-trip as such — restoring it puts the source back in
    /// the fresh no-`WHERE` state instead of skipping rows `<=` a sentinel.
    #[test]
    fn keyset_offset_before_any_read_round_trips_as_none() {
        let fresh = JdbcOffset::Keyset {
            column: "id".to_owned(),
            last_key: None,
        };
        let decoded = JdbcOffset::decode(&fresh.encode()).unwrap();
        assert_eq!(decoded, fresh);
        // The None encoding must be DISTINCT from any concrete key.
        let sentinel = JdbcOffset::Keyset {
            column: "id".to_owned(),
            last_key: Some(-1),
        };
        assert_ne!(fresh.encode(), sentinel.encode());
        assert_eq!(
            JdbcOffset::decode(&sentinel.encode()).unwrap(),
            sentinel,
            "Some(-1) must stay a real key, not collapse into None"
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
            last_key: Some(1),
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
        assert!(
            sql.contains("($1, $2, $3), ($4, $5, $6), ($7, $8, $9)"),
            "{sql}"
        );
    }

    #[test]
    fn an_all_key_table_upserts_as_do_nothing() {
        // Every column is part of the key: there is nothing to update, and
        // DO NOTHING is the honest idempotent form (DO UPDATE SET with an
        // empty list is a syntax error).
        let sql = render_insert("t", &cols(&["a", "b"]), 1, &cols(&["a", "b"]));
        assert!(
            sql.ends_with(r#"ON CONFLICT ("a", "b") DO NOTHING"#),
            "{sql}"
        );
    }

    #[test]
    fn a_declared_key_renders_the_columns_actual_spelling() {
        // Regression (adversarial review, 2026-08-12): quote_identifier
        // never folds, so ON CONFLICT ("ID") against column `id` failed
        // with 42703 INSIDE the transaction — past the pre-flight guard
        // that exists to fail before it opens.
        let sql = render_insert("t", &cols(&["id", "v"]), 1, &cols(&["ID"]));
        assert!(
            sql.contains(r#"ON CONFLICT ("id")"#),
            "must fold to the real column: {sql}"
        );
        assert!(!sql.contains(r#"ON CONFLICT ("ID")"#), "{sql}");
    }

    #[test]
    fn composite_dedup_key_cannot_be_forged_across_column_boundaries() {
        use super::composite_row_key;
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        // Two DIFFERENT key tuples whose naive `\u{1f}`-joined renderings
        // are identical: ("a\u{1f}b", "c") vs ("a", "b\u{1f}c").
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Utf8, false),
            Field::new("k2", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a\u{1f}b", "a"])),
                Arc::new(StringArray::from(vec!["c", "b\u{1f}c"])),
            ],
        )
        .unwrap();
        let key0 = composite_row_key(&batch, &[0, 1], 0);
        let key1 = composite_row_key(&batch, &[0, 1], 1);
        assert_ne!(
            key0, key1,
            "distinct key tuples must not collapse into one dedup key"
        );

        // Equal tuples still dedup to the same key.
        let dup = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["x", "x"])),
                Arc::new(StringArray::from(vec!["y", "y"])),
            ],
        )
        .unwrap();
        assert_eq!(
            composite_row_key(&dup, &[0, 1], 0),
            composite_row_key(&dup, &[0, 1], 1)
        );
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

/// Join the per-column dedup keys for one row into a single map key.
///
/// Each component is length-prefixed (`<len>:<bytes>`) rather than joined
/// with a bare separator: a separator that can also appear INSIDE a string
/// value would let two different key tuples render identically (e.g.
/// `("a<sep>b", "c")` vs `("a", "b<sep>c")`), silently collapsing distinct
/// rows into one upsert.
fn composite_row_key(batch: &RecordBatch, key_indices: &[usize], row_idx: usize) -> String {
    let mut key = String::new();
    for &col_idx in key_indices {
        let part = cell_key(batch.column(col_idx).as_ref(), row_idx);
        key.push_str(&part.len().to_string());
        key.push(':');
        key.push_str(&part);
    }
    key
}

/// A row's value for one column rendered as a dedup key. Only used to
/// collapse duplicate conflict targets within a batch, so it needs to be
/// stable and collision-free for equal values — not human-readable.
fn cell_key(col: &dyn Array, row_idx: usize) -> String {
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        LargeStringArray, StringArray, StringViewArray,
    };
    if col.is_null(row_idx) {
        return "\u{0}NULL".into();
    }
    macro_rules! as_str {
        ($t:ty) => {
            col.as_any()
                .downcast_ref::<$t>()
                .map(|a| a.value(row_idx).to_string())
        };
    }
    let rendered = match col.data_type() {
        DataType::Boolean => as_str!(BooleanArray),
        DataType::Int16 => as_str!(Int16Array),
        DataType::Int32 => as_str!(Int32Array),
        DataType::Int64 => as_str!(Int64Array),
        DataType::Float32 => as_str!(Float32Array),
        DataType::Float64 => as_str!(Float64Array),
        DataType::Utf8 => as_str!(StringArray),
        DataType::Utf8View => as_str!(StringViewArray),
        DataType::LargeUtf8 => as_str!(LargeStringArray),
        _ => None,
    };
    // An unrenderable key type falls back to the row index, which makes
    // the row unique to itself — no dedup, but never a WRONG collapse.
    rendered.unwrap_or_else(|| format!("\u{0}row{row_idx}"))
}
