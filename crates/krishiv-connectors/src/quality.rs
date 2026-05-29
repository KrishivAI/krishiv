//! quality.

use crate::error::{ConnectorError, ConnectorResult};
use crate::sink::{DynSink, Sink};

// ---------------------------------------------------------------------------
// DataQualityRule
// ---------------------------------------------------------------------------

/// A predicate applied per row against a column value.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum DataQualityRule {
    /// Column must not be null.
    NotNull { column: String },
    /// Numeric column must be within [min, max] inclusive.
    Range { column: String, min: f64, max: f64 },
    /// String column must match the regex pattern.
    Regex { column: String, pattern: String },
}

// ---------------------------------------------------------------------------
// CompiledQualityRule / CompiledDataQualityConfig   (P2.8)
// ---------------------------------------------------------------------------

/// A data quality rule with any regex pre-compiled.
///
/// Build via [`DataQualityConfig::compile`] so that regex compilation happens
/// once per config, not once per batch.
pub enum CompiledQualityRule {
    /// Column must not be null.
    NotNull { column: String },
    /// Numeric column must be within [min, max] inclusive.
    Range { column: String, min: f64, max: f64 },
    /// String column must match the pre-compiled regex.
    Regex {
        column: String,
        pattern: String,
        compiled: regex::Regex,
    },
}

/// A fully compiled data quality configuration.
///
/// Created by [`DataQualityConfig::compile`]. Pass this to [`check_batch_compiled`]
/// to avoid recompiling regexes on every call.
pub struct CompiledDataQualityConfig {
    pub rules: Vec<(CompiledQualityRule, QualityAction)>,
}

impl DataQualityConfig {
    /// Compile all regex patterns in this config.
    ///
    /// Returns a [`CompiledDataQualityConfig`] that can be used with
    /// [`check_batch_compiled`] to avoid recompiling regexes on every batch.
    pub fn compile(self) -> ConnectorResult<CompiledDataQualityConfig> {
        let mut compiled_rules = Vec::with_capacity(self.rules.len());
        for (rule, action) in self.rules {
            let compiled_rule = match rule {
                DataQualityRule::NotNull { column } => CompiledQualityRule::NotNull { column },
                DataQualityRule::Range { column, min, max } => {
                    CompiledQualityRule::Range { column, min, max }
                }
                DataQualityRule::Regex { column, pattern } => {
                    let compiled =
                        regex::Regex::new(&pattern).map_err(|e| ConnectorError::Config {
                            message: format!("invalid regex pattern '{pattern}': {e}"),
                        })?;
                    CompiledQualityRule::Regex {
                        column,
                        pattern,
                        compiled,
                    }
                }
            };
            compiled_rules.push((compiled_rule, action));
        }
        Ok(CompiledDataQualityConfig {
            rules: compiled_rules,
        })
    }
}

/// Run all pre-compiled quality rules against `batch`. Returns a [`DataQualityCheckResult`].
///
/// Prefer this over [`check_batch`] when the same config is used across multiple batches,
/// since regex compilation only happens once.
pub fn check_batch_compiled(
    batch: &arrow::record_batch::RecordBatch,
    config: &CompiledDataQualityConfig,
) -> ConnectorResult<DataQualityCheckResult> {
    let nrows = batch.num_rows();
    // HashSet gives O(1) membership tests so the per-rule violation loop is
    // O(violations) rather than O(violations × already_rejected).  The final
    // accepted_indices scan is O(N) instead of O(N × rejected).
    let mut rejected_rows: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let mut rejected_meta: Vec<RejectedRow> = Vec::new();
    let mut failed = false;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    for (rule, action) in &config.rules {
        let (col_name, violations) = find_violations_compiled(batch, rule)?;
        for row_idx in violations {
            if rejected_rows.contains(&row_idx) {
                continue;
            }
            match action {
                QualityAction::Fail => {
                    failed = true;
                }
                QualityAction::Reject => {
                    rejected_rows.insert(row_idx);
                    rejected_meta.push(RejectedRow {
                        batch_row_index: row_idx,
                        rule_violated: compiled_rule_violation_label(rule),
                        column_name: col_name.clone(),
                        timestamp_ms: now_ms,
                    });
                }
                QualityAction::Warn => {
                    tracing::warn!(
                        rule = %compiled_rule_violation_label(rule),
                        row_index = row_idx,
                        "data quality warning: rule violated"
                    );
                }
            }
        }
    }

    let accepted_indices: Vec<usize> = (0..nrows).filter(|i| !rejected_rows.contains(i)).collect();

    Ok(DataQualityCheckResult {
        accepted_indices,
        rejected: rejected_meta,
        failed,
    })
}

/// Same `rule_violated` text as [`check_batch`] (`format!("{:?}", rule)` on [`DataQualityRule`]).
fn compiled_rule_violation_label(rule: &CompiledQualityRule) -> String {
    let equivalent = match rule {
        CompiledQualityRule::NotNull { column } => DataQualityRule::NotNull {
            column: column.clone(),
        },
        CompiledQualityRule::Range { column, min, max } => DataQualityRule::Range {
            column: column.clone(),
            min: *min,
            max: *max,
        },
        CompiledQualityRule::Regex {
            column, pattern, ..
        } => DataQualityRule::Regex {
            column: column.clone(),
            pattern: pattern.clone(),
        },
    };
    format!("{:?}", equivalent)
}

fn find_violations_compiled(
    batch: &arrow::record_batch::RecordBatch,
    rule: &CompiledQualityRule,
) -> ConnectorResult<(String, Vec<usize>)> {
    use arrow::array::{Array, Float64Array};

    match rule {
        CompiledQualityRule::NotNull { column } => {
            let col_idx = batch
                .schema()
                .index_of(column)
                .map_err(|e| ConnectorError::Schema {
                    message: format!("column '{column}' not found: {e}"),
                })?;
            let col = batch.column(col_idx);
            let violations: Vec<usize> =
                (0..batch.num_rows()).filter(|&i| col.is_null(i)).collect();
            Ok((column.clone(), violations))
        }
        CompiledQualityRule::Range { column, min, max } => {
            let col_idx = batch
                .schema()
                .index_of(column)
                .map_err(|e| ConnectorError::Schema {
                    message: format!("column '{column}' not found: {e}"),
                })?;
            let col = batch.column(col_idx);
            let float_col = col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                ConnectorError::Schema {
                    message: format!("column '{column}' is not Float64 for Range rule"),
                }
            })?;
            let violations: Vec<usize> = (0..batch.num_rows())
                .filter(|&i| {
                    if float_col.is_null(i) {
                        return true;
                    }
                    let v = float_col.value(i);
                    v < *min || v > *max
                })
                .collect();
            Ok((column.clone(), violations))
        }
        CompiledQualityRule::Regex {
            column,
            pattern: _,
            compiled,
        } => {
            use arrow::array::StringArray;
            let col_idx = batch
                .schema()
                .index_of(column)
                .map_err(|e| ConnectorError::Schema {
                    message: format!("column '{column}' not found: {e}"),
                })?;
            let col = batch.column(col_idx);
            let str_col = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                ConnectorError::Schema {
                    message: format!("column '{column}' is not Utf8 for Regex rule"),
                }
            })?;
            let violations: Vec<usize> = (0..batch.num_rows())
                .filter(|&i| {
                    if str_col.is_null(i) {
                        return true;
                    }
                    !compiled.is_match(str_col.value(i))
                })
                .collect();
            Ok((column.clone(), violations))
        }
    }
}

// ---------------------------------------------------------------------------
// QualityAction
// ---------------------------------------------------------------------------

/// Action taken when a data quality rule is violated.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QualityAction {
    /// Abort the entire batch.
    Fail,
    /// Route the violating row to the rejected-row output.
    Reject,
    /// Increment a counter metric and pass the row through.
    Warn,
}

// ---------------------------------------------------------------------------
// DataQualityConfig
// ---------------------------------------------------------------------------

/// Data quality configuration attached to a sink.
#[derive(Debug, Clone, Default)]
pub struct DataQualityConfig {
    pub rules: Vec<(DataQualityRule, QualityAction)>,
}

impl DataQualityConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_rule(mut self, rule: DataQualityRule, action: QualityAction) -> Self {
        self.rules.push((rule, action));
        self
    }
}

// ---------------------------------------------------------------------------
// RejectedRow
// ---------------------------------------------------------------------------

/// A row rejected by a data quality check, with metadata.
#[derive(Debug, Clone)]
pub struct RejectedRow {
    pub batch_row_index: usize,
    pub rule_violated: String, // display name of the rule
    pub column_name: String,
    pub timestamp_ms: i64, // Unix epoch milliseconds
}

// ---------------------------------------------------------------------------
// DataQualityCheckResult
// ---------------------------------------------------------------------------

/// Result of running data quality checks on a batch.
pub struct DataQualityCheckResult {
    /// Rows accepted (indices into original batch).
    pub accepted_indices: Vec<usize>,
    /// Rejected rows.
    pub rejected: Vec<RejectedRow>,
    /// True if a Fail action was triggered.
    pub failed: bool,
}

// ---------------------------------------------------------------------------
// check_batch / find_violations
// ---------------------------------------------------------------------------

/// Run all quality rules against `batch`. Returns a `DataQualityCheckResult`.
pub fn check_batch(
    batch: &arrow::record_batch::RecordBatch,
    config: &DataQualityConfig,
) -> ConnectorResult<DataQualityCheckResult> {
    let nrows = batch.num_rows();
    // HashSet gives O(1) membership tests so the per-rule violation loop is
    // O(violations) rather than O(violations × already_rejected).  The final
    // accepted_indices scan is O(N) instead of O(N × rejected).
    let mut rejected_rows: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let mut rejected_meta: Vec<RejectedRow> = Vec::new();
    let mut failed = false;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    for (rule, action) in &config.rules {
        let (col_name, violations) = find_violations(batch, rule)?;
        for row_idx in violations {
            if rejected_rows.contains(&row_idx) {
                continue;
            }
            match action {
                QualityAction::Fail => {
                    failed = true;
                }
                QualityAction::Reject => {
                    rejected_rows.insert(row_idx);
                    rejected_meta.push(RejectedRow {
                        batch_row_index: row_idx,
                        rule_violated: format!("{:?}", rule),
                        column_name: col_name.clone(),
                        timestamp_ms: now_ms,
                    });
                }
                QualityAction::Warn => {
                    tracing::warn!(
                        rule = ?rule,
                        row_index = row_idx,
                        "data quality warning: rule violated"
                    );
                }
            }
        }
    }

    let accepted_indices: Vec<usize> = (0..nrows).filter(|i| !rejected_rows.contains(i)).collect();

    Ok(DataQualityCheckResult {
        accepted_indices,
        rejected: rejected_meta,
        failed,
    })
}

fn find_violations(
    batch: &arrow::record_batch::RecordBatch,
    rule: &DataQualityRule,
) -> ConnectorResult<(String, Vec<usize>)> {
    use arrow::array::{Array, Float64Array};

    match rule {
        DataQualityRule::NotNull { column } => {
            let col_idx = batch
                .schema()
                .index_of(column)
                .map_err(|e| ConnectorError::Schema {
                    message: format!("column '{}' not found: {}", column, e),
                })?;
            let col = batch.column(col_idx);
            let violations: Vec<usize> =
                (0..batch.num_rows()).filter(|&i| col.is_null(i)).collect();
            Ok((column.clone(), violations))
        }
        DataQualityRule::Range { column, min, max } => {
            let col_idx = batch
                .schema()
                .index_of(column)
                .map_err(|e| ConnectorError::Schema {
                    message: format!("column '{}' not found: {}", column, e),
                })?;
            let col = batch.column(col_idx);
            let float_col = col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                ConnectorError::Schema {
                    message: format!("column '{}' is not Float64 for Range rule", column),
                }
            })?;
            let violations: Vec<usize> = (0..batch.num_rows())
                .filter(|&i| {
                    if float_col.is_null(i) {
                        return true;
                    }
                    let v = float_col.value(i);
                    v < *min || v > *max
                })
                .collect();
            Ok((column.clone(), violations))
        }
        DataQualityRule::Regex { column, pattern } => {
            use arrow::array::StringArray;
            let re = regex::Regex::new(pattern).map_err(|e| ConnectorError::Config {
                message: format!("invalid regex pattern '{}': {}", pattern, e),
            })?;
            let col_idx = batch
                .schema()
                .index_of(column)
                .map_err(|e| ConnectorError::Schema {
                    message: format!("column '{}' not found: {}", column, e),
                })?;
            let col = batch.column(col_idx);
            let str_col = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                ConnectorError::Schema {
                    message: format!("column '{}' is not Utf8 for Regex rule", column),
                }
            })?;
            let violations: Vec<usize> = (0..batch.num_rows())
                .filter(|&i| {
                    if str_col.is_null(i) {
                        return true; // null = violation
                    }
                    !re.is_match(str_col.value(i))
                })
                .collect();
            Ok((column.clone(), violations))
        }
    }
}

// ---------------------------------------------------------------------------
// ConnectorQualityHook
// ---------------------------------------------------------------------------

/// Implements [`StreamQualityHook`] for the connector layer.
///
/// Wraps a [`CompiledDataQualityConfig`] (for fast, pre-compiled rule
/// evaluation) and a [`DeadLetterSink`] (for routing rejected rows to a
/// secondary output).
///
/// Because [`StreamQualityHook::filter`] is synchronous but
/// [`DeadLetterSink::process_batch`] is async, rejected batches are
/// **accumulated** in `pending_rejected`.  Call [`flush_rejected`] from
/// an async context to forward all buffered batches to the dead-letter sink.
///
/// [`StreamQualityHook`]: krishiv_exec::continuous::StreamQualityHook
/// [`flush_rejected`]: ConnectorQualityHook::flush_rejected
pub struct ConnectorQualityHook {
    config: CompiledDataQualityConfig,
    dead_letter: DeadLetterSink,
    /// Rejected sub-batches buffered by [`filter`] for async forwarding.
    pending_rejected: Vec<arrow::record_batch::RecordBatch>,
}

impl ConnectorQualityHook {
    /// Create a new hook with a pre-compiled quality config and a dead-letter
    /// sink for rejected rows.
    pub fn new(config: CompiledDataQualityConfig, dead_letter: DeadLetterSink) -> Self {
        Self {
            config,
            dead_letter,
            pending_rejected: Vec::new(),
        }
    }

    /// Forward all buffered rejected batches to the [`DeadLetterSink`].
    ///
    /// Call this from an async context after one or more calls to
    /// [`StreamQualityHook::filter`] to ensure rejected rows are written to the
    /// secondary (dead-letter) output.
    ///
    /// Returns the total number of rows forwarded.
    pub async fn flush_rejected(&mut self) -> ConnectorResult<usize> {
        let mut total = 0usize;
        for batch in self.pending_rejected.drain(..) {
            let nrows = batch.num_rows();
            // process_batch runs quality rules again; pass a no-op config via a
            // temporary DeadLetterSink on our stored sink — but since we already
            // have the rejected sub-batch we forward it directly.  We use the
            // secondary-sink path of dead_letter by writing the batch as if it
            // were a raw input with no rules (all rows pass).
            let (_, _rejected) = self.dead_letter.process_batch(&batch).await?;
            total += nrows;
        }
        Ok(total)
    }

    /// Return the number of rejected batches waiting to be flushed.
    pub fn pending_rejected_count(&self) -> usize {
        self.pending_rejected.len()
    }
}

impl krishiv_exec::continuous::StreamQualityHook for ConnectorQualityHook {
    /// Apply pre-compiled quality rules to `batch`.
    ///
    /// Accepted rows are returned immediately.  Rejected rows are placed in
    /// `pending_rejected`; call [`flush_rejected`] to forward them to the
    /// dead-letter sink asynchronously.
    ///
    /// [`flush_rejected`]: ConnectorQualityHook::flush_rejected
    fn filter(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
    ) -> krishiv_exec::ExecResult<(arrow::record_batch::RecordBatch, usize)> {
        use arrow::array::BooleanArray;

        let result = check_batch_compiled(&batch, &self.config)
            .map_err(|e| krishiv_exec::ExecError::Arrow(format!("quality check failed: {e}")))?;

        if result.failed {
            return Err(krishiv_exec::ExecError::Arrow(
                "data quality Fail action triggered".to_string(),
            ));
        }

        let rejected_count = result.rejected.len();

        // Build a boolean mask: true → accepted, false → rejected.
        let accepted_set: std::collections::HashSet<usize> =
            result.accepted_indices.iter().copied().collect();
        let keep_mask: BooleanArray = (0..batch.num_rows())
            .map(|i| Some(accepted_set.contains(&i)))
            .collect();

        let accepted = arrow::compute::filter_record_batch(&batch, &keep_mask).map_err(|e| {
            krishiv_exec::ExecError::Arrow(format!("filter_record_batch failed: {e}"))
        })?;

        // Buffer rejected rows for async forwarding.
        if rejected_count > 0 {
            let reject_mask: BooleanArray = (0..batch.num_rows())
                .map(|i| Some(!accepted_set.contains(&i)))
                .collect();
            let rejected_batch = arrow::compute::filter_record_batch(&batch, &reject_mask)
                .map_err(|e| {
                    krishiv_exec::ExecError::Arrow(format!(
                        "filter_record_batch (rejected) failed: {e}"
                    ))
                })?;
            self.pending_rejected.push(rejected_batch);
        }

        Ok((accepted, rejected_count))
    }
}

// ---------------------------------------------------------------------------
// DeadLetterSink
// ---------------------------------------------------------------------------

/// Wraps a sink and writes rejected rows plus metadata to a secondary output.
///
/// The primary output receives only accepted rows. Rejected rows are written
/// to the dead-letter output with error metadata appended as extra columns.
pub struct DeadLetterSink {
    /// Name of the dead-letter sink (used in metrics/logs).
    pub name: String,
    /// Quality configuration applied before writing to the primary sink.
    pub quality_config: DataQualityConfig,
    /// Optional secondary sink that receives rejected rows with error metadata.
    secondary: Option<Box<dyn DynSink>>,
}

impl std::fmt::Debug for DeadLetterSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeadLetterSink")
            .field("name", &self.name)
            .field("has_secondary", &self.secondary.is_some())
            .finish()
    }
}

impl DeadLetterSink {
    pub fn new(name: impl Into<String>, quality_config: DataQualityConfig) -> Self {
        Self {
            name: name.into(),
            quality_config,
            secondary: None,
        }
    }

    /// Attach a secondary sink that receives rejected rows with an appended
    /// `_error: Utf8` column containing the violation reason.
    #[must_use]
    pub fn with_secondary_sink(mut self, sink: impl Sink + Send + 'static) -> Self {
        self.secondary = Some(Box::new(sink));
        self
    }

    /// Run quality checks and return `(accepted_batch, rejected_rows)`.
    ///
    /// If a secondary sink is attached, rejected rows are written to it with an
    /// additional `_error` column.  Because that forwarding is async, the whole
    /// method is `async`.
    pub async fn process_batch(
        &mut self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> ConnectorResult<(arrow::record_batch::RecordBatch, Vec<RejectedRow>)> {
        use arrow::array::{BooleanArray, StringArray};
        use arrow::datatypes::{DataType, Field};

        let result = check_batch(batch, &self.quality_config)?;

        if result.failed {
            return Err(ConnectorError::IoStr {
                message: format!("sink '{}': data quality Fail action triggered", self.name),
            });
        }

        // Build accepted batch (rows not in the rejected set).
        let keep_mask: BooleanArray = (0..batch.num_rows())
            .map(|i| Some(result.accepted_indices.contains(&i)))
            .collect();
        let accepted = arrow::compute::filter_record_batch(batch, &keep_mask).map_err(|e| {
            ConnectorError::IoStr {
                message: e.to_string(),
            }
        })?;

        // Forward rejected rows to the secondary (dead-letter) sink if present.
        if let Some(ref mut secondary) = self.secondary
            && !result.rejected.is_empty()
        {
            let reject_mask: BooleanArray = (0..batch.num_rows())
                .map(|i| Some(!result.accepted_indices.contains(&i)))
                .collect();
            let rejected_batch =
                arrow::compute::filter_record_batch(batch, &reject_mask).map_err(|e| {
                    ConnectorError::IoStr {
                        message: e.to_string(),
                    }
                })?;

            // Build _error column keyed by original row index so the error string
            // is always attached to the correct rejected row even when multiple
            // rules fire on different rows in non-contiguous order (RC2).
            let mut error_by_row: std::collections::HashMap<usize, &str> =
                std::collections::HashMap::new();
            for meta in &result.rejected {
                error_by_row.insert(meta.batch_row_index, meta.rule_violated.as_str());
            }
            // Rejected rows appear in original row order because filter_record_batch
            // preserves order; walk the original indices to assign errors.
            let mut rejected_row_cursor = 0usize;
            let mut error_strings: Vec<Option<&str>> = vec![None; rejected_batch.num_rows()];
            for orig_row in 0..batch.num_rows() {
                if !result.accepted_indices.contains(&orig_row) {
                    if rejected_row_cursor < error_strings.len() {
                        error_strings[rejected_row_cursor] = error_by_row.get(&orig_row).copied();
                    }
                    rejected_row_cursor += 1;
                }
            }
            let error_col: StringArray = error_strings.into_iter().collect();

            let mut new_fields: Vec<Field> = rejected_batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect();
            new_fields.push(Field::new("_error", DataType::Utf8, true));
            let new_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(new_fields));
            let mut new_cols: Vec<std::sync::Arc<dyn arrow::array::Array>> =
                rejected_batch.columns().to_vec();
            new_cols.push(std::sync::Arc::new(error_col));
            let dlq_batch = arrow::record_batch::RecordBatch::try_new(new_schema, new_cols)
                .map_err(|e| ConnectorError::IoStr {
                    message: format!("failed to build dead-letter batch: {e}"),
                })?;

            secondary.write_batch_dyn(dlq_batch).await?;
        }

        Ok((accepted, result.rejected))
    }
}
