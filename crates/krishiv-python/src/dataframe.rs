//! `DataFrame` batch SQL results.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::errors::map_krishiv_error;
use crate::expression::PyColumn;
use crate::query_result::PyQueryResult;

#[pyclass(name = "DataFrame")]
pub struct PyDataFrame {
    pub(crate) inner: krishiv_api::DataFrame,
}

#[pymethods]
impl PyDataFrame {
    /// Collect and return a [`QueryResult`] with Arrow batches.
    pub fn collect(&self, py: Python<'_>) -> PyResult<PyQueryResult> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .collect()
                .map(PyQueryResult::new)
                .map_err(map_krishiv_error)
        })
    }

    /// Collect and return a pretty-printed ASCII table.
    pub fn collect_pretty(&self, py: Python<'_>) -> PyResult<String> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .collect()
                .and_then(|r| r.pretty())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Alias for collect() — returns Arrow batches.
    pub fn collect_batches(&self, py: Python<'_>) -> PyResult<PyQueryResult> {
        self.collect(py)
    }

    pub fn collect_async(&self, py: Python<'_>) -> PyResult<PyQueryResult> {
        let inner = self.inner.clone();
        py.detach(move || {
            crate::session::block_on_async(inner.collect_async())
                .map(PyQueryResult::new)
                .map_err(map_krishiv_error)
        })
    }

    pub fn execute_stream_async(&self, py: Python<'_>) -> PyResult<PyDataFrameStream> {
        let inner = self.inner.clone();
        let stream = py
            .detach(move || {
                crate::session::block_on_async(async move {
                    inner.execute_stream_async().await.map_err(|e| {
                        krishiv_api::KrishivError::Runtime {
                            message: e.to_string(),
                        }
                    })
                })
            })
            .map_err(map_krishiv_error)?;
        Ok(PyDataFrameStream {
            stream: std::sync::Arc::new(tokio::sync::Mutex::new(stream)),
        })
    }

    /// Print up to `n` rows as an ASCII table to stdout.
    #[pyo3(signature = (n=20))]
    pub fn show(&self, py: Python<'_>, n: usize) -> PyResult<()> {
        self.collect(py)?.show(n)
    }

    pub fn explain(&self, py: Python<'_>) -> PyResult<String> {
        let inner = self.inner.clone();
        py.detach(move || inner.explain().map_err(map_krishiv_error))
    }

    pub fn explain_logical(&self) -> String {
        self.inner.explain_logical()
    }

    #[pyo3(signature = (mode="physical"))]
    pub fn explain_mode(&self, py: Python<'_>, mode: &str) -> PyResult<String> {
        let mode = match mode {
            "logical" => krishiv_api::ExplainMode::Logical,
            "physical" => krishiv_api::ExplainMode::Physical,
            "analyze" => krishiv_api::ExplainMode::Analyze,
            other => {
                return Err(PyRuntimeError::new_err(format!(
                    "unknown explain mode '{other}'; expected logical, physical, or analyze"
                )));
            }
        };
        let inner = self.inner.clone();
        py.detach(move || inner.explain_with(mode).map_err(map_krishiv_error))
    }

    pub fn select(&self, columns: Vec<String>) -> PyResult<Self> {
        let refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
        self.inner
            .select(&refs)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    /// Select raw SQL expressions. This is a preview compatibility escape hatch.
    pub fn select_exprs(&self, expressions: Vec<String>) -> PyResult<Self> {
        let expressions = expressions
            .into_iter()
            .map(krishiv_api::Expr::raw)
            .collect::<Vec<_>>();
        self.inner
            .select_exprs(&expressions)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn select_columns(&self, expressions: Vec<PyColumn>) -> PyResult<Self> {
        let expressions = expressions
            .into_iter()
            .map(|column| column.inner)
            .collect::<Vec<_>>();
        self.inner
            .select_exprs(&expressions)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn filter_column(&self, predicate: PyColumn) -> PyResult<Self> {
        self.inner
            .filter_expr(predicate.inner)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn filter(&self, predicate: String) -> PyResult<Self> {
        self.inner
            .filter(&predicate)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn limit(&self, n: usize) -> PyResult<Self> {
        self.inner
            .limit(n)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn boundedness(&self) -> &'static str {
        match self.inner.boundedness() {
            krishiv_api::Boundedness::Bounded => "bounded",
            krishiv_api::Boundedness::Unbounded => "unbounded",
        }
    }

    pub fn is_bounded(&self) -> bool {
        self.inner.is_bounded()
    }

    #[pyo3(signature = (columns=Vec::new()))]
    pub fn drop_nulls(&self, columns: Vec<String>) -> PyResult<Self> {
        let columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
        self.inner
            .drop_nulls(&columns)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn sample(&self, fraction: f64) -> PyResult<Self> {
        self.inner
            .sample(fraction)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn distinct(&self) -> PyResult<Self> {
        self.inner
            .distinct()
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    #[pyo3(signature = (columns, descending=None))]
    pub fn sort(&self, columns: Vec<String>, descending: Option<Vec<bool>>) -> PyResult<Self> {
        let descending = descending.unwrap_or_else(|| vec![false; columns.len()]);
        let refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
        self.inner
            .sort(&refs, &descending)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn drop_columns(&self, columns: Vec<String>) -> PyResult<Self> {
        let refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
        self.inner
            .drop(&refs)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn rename(&self, old: String, new: String) -> PyResult<Self> {
        self.inner
            .rename(&old, &new)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn with_column(&self, name: String, expression: String) -> PyResult<Self> {
        self.inner
            .with_column(&name, &expression)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn fill_null(&self, column: String, value: String) -> PyResult<Self> {
        self.inner
            .fill_null(&column, &value)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    /// Group by raw SQL expressions. This is a preview compatibility escape hatch.
    pub fn group_by(&self, expressions: Vec<String>) -> PyGroupedDataFrame {
        PyGroupedDataFrame {
            dataframe: self.inner.clone(),
            group_exprs: expressions
                .into_iter()
                .map(krishiv_api::Expr::raw)
                .collect(),
        }
    }

    pub fn group_by_columns(&self, expressions: Vec<PyColumn>) -> PyGroupedDataFrame {
        PyGroupedDataFrame {
            dataframe: self.inner.clone(),
            group_exprs: expressions.into_iter().map(|column| column.inner).collect(),
        }
    }

    pub fn union(&self, right: &PyDataFrame) -> PyResult<Self> {
        self.inner
            .union(&right.inner)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn union_distinct(&self, right: &PyDataFrame) -> PyResult<Self> {
        self.inner
            .union_distinct(&right.inner)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn intersect(&self, right: &PyDataFrame) -> PyResult<Self> {
        self.inner
            .intersect(&right.inner)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn intersect_distinct(&self, right: &PyDataFrame) -> PyResult<Self> {
        self.inner
            .intersect_distinct(&right.inner)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn except_all(&self, right: &PyDataFrame) -> PyResult<Self> {
        self.inner
            .except(&right.inner)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn except_distinct(&self, right: &PyDataFrame) -> PyResult<Self> {
        self.inner
            .except_distinct(&right.inner)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    /// Alias for `except_all` that avoids the Python `except` keyword conflict.
    pub fn except_(&self, right: &PyDataFrame) -> PyResult<Self> {
        self.except_all(right)
    }

    pub fn pivot(
        &self,
        groups: Vec<PyColumn>,
        pivot_column: PyColumn,
        aggregate: PyColumn,
        values: Vec<(PyColumn, String)>,
    ) -> PyResult<Self> {
        let groups = groups
            .into_iter()
            .map(|column| column.inner)
            .collect::<Vec<_>>();
        let values = values
            .into_iter()
            .map(|(column, alias)| match column.inner.into_node() {
                krishiv_plan::expression::Expr::Literal { value } => {
                    Ok(krishiv_api::PivotValue::new(value, alias))
                }
                _ => Err(PyRuntimeError::new_err(
                    "pivot values must be created with lit()",
                )),
            })
            .collect::<PyResult<Vec<_>>>()?;
        self.inner
            .pivot(&groups, pivot_column.inner, aggregate.inner, &values)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn unpivot(
        &self,
        columns: Vec<String>,
        name_column: String,
        value_column: String,
    ) -> PyResult<Self> {
        let columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
        self.inner
            .unpivot(&columns, &name_column, &value_column)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    #[pyo3(signature = (path, format, *, mode = "error", partition_by = Vec::new(), max_rows_per_file = None))]
    pub fn write_file(
        &self,
        py: Python<'_>,
        path: String,
        format: String,
        mode: &str,
        partition_by: Vec<String>,
        max_rows_per_file: Option<usize>,
    ) -> PyResult<()> {
        let inner = self.inner.clone();
        let mode = match mode {
            "error" | "error_if_exists" => krishiv_api::WriteMode::ErrorIfExists,
            "append" => krishiv_api::WriteMode::Append,
            "overwrite" => krishiv_api::WriteMode::Overwrite,
            "ignore" => krishiv_api::WriteMode::Ignore,
            "dynamic_overwrite" => krishiv_api::WriteMode::DynamicOverwrite,
            other => {
                return Err(PyRuntimeError::new_err(format!(
                    "unsupported write mode '{other}'"
                )));
            }
        };
        let format = match format.to_ascii_lowercase().as_str() {
            "parquet" => krishiv_api::DataFormat::Parquet,
            "csv" => krishiv_api::DataFormat::Csv,
            "json" | "ndjson" => krishiv_api::DataFormat::Json,
            other => {
                return Err(PyRuntimeError::new_err(format!(
                    "unsupported format '{other}'"
                )));
            }
        };
        py.detach(move || {
            inner
                .write()
                .file_options(krishiv_api::FileWriteOptions {
                    format,
                    mode,
                    layout: krishiv_api::FileLayout {
                        partition_by,
                        max_rows_per_file,
                        ..krishiv_api::FileLayout::default()
                    },
                    schema_evolution: krishiv_api::SchemaEvolutionMode::Strict,
                })
                .and_then(|writer| writer.save(&path))
                .map_err(map_krishiv_error)
        })
    }

    pub fn write_parquet(&self, py: Python<'_>, path: String) -> PyResult<()> {
        let inner = self.inner.clone();
        py.detach(move || inner.write_parquet(&path).map_err(map_krishiv_error))
    }

    pub fn write_csv(&self, py: Python<'_>, path: String) -> PyResult<()> {
        let inner = self.inner.clone();
        py.detach(move || inner.write_csv(&path).map_err(map_krishiv_error))
    }

    pub fn write_json(&self, py: Python<'_>, path: String) -> PyResult<()> {
        let inner = self.inner.clone();
        py.detach(move || inner.write_json(&path).map_err(map_krishiv_error))
    }

    /// Write to Parquet with typed options.
    ///
    /// `compression` accepts: "snappy", "gzip", "lz4", "zstd", "brotli", "uncompressed".
    /// `max_row_group_size` sets the maximum rows per row-group.
    #[pyo3(signature = (path, *, compression=None, max_row_group_size=None))]
    pub fn write_parquet_with_options(
        &self,
        py: Python<'_>,
        path: String,
        compression: Option<String>,
        max_row_group_size: Option<usize>,
    ) -> PyResult<()> {
        let opts = krishiv_sql::ParquetWriterOptions {
            compression,
            max_row_group_size,
        };
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .write_parquet_with_options(&path, &opts)
                .map_err(map_krishiv_error)
        })
    }

    /// Write to CSV with typed options.
    ///
    /// `delimiter` is a single character; defaults to comma.
    /// `has_header` controls whether a header row is emitted.
    #[pyo3(signature = (path, *, delimiter=None, has_header=None))]
    pub fn write_csv_with_options(
        &self,
        py: Python<'_>,
        path: String,
        delimiter: Option<String>,
        has_header: Option<bool>,
    ) -> PyResult<()> {
        let delimiter_char: Option<char> = match delimiter {
            Some(ref s) => {
                let mut chars = s.chars();
                let c = chars.next().ok_or_else(|| {
                    PyRuntimeError::new_err("delimiter must be a non-empty string")
                })?;
                if chars.next().is_some() {
                    return Err(PyRuntimeError::new_err(
                        "delimiter must be a single character",
                    ));
                }
                Some(c)
            }
            None => None,
        };
        let opts = krishiv_sql::CsvWriterOptions {
            delimiter: delimiter_char,
            has_header,
        };
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .write_csv_with_options(&path, &opts)
                .map_err(map_krishiv_error)
        })
    }

    /// Materialise this DataFrame into an in-memory table and return a new
    /// DataFrame backed by it. Equivalent to `persist()`.
    pub fn cache(&self, py: Python<'_>) -> PyResult<Self> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .cache()
                .map(|inner| Self { inner })
                .map_err(map_krishiv_error)
        })
    }

    /// Alias for `cache()`.
    pub fn persist(&self, py: Python<'_>) -> PyResult<Self> {
        self.cache(py)
    }

    /// Drop the in-memory table created by `cache()` / `persist()`.
    /// A no-op if this DataFrame was not previously cached.
    pub fn unpersist(&self) -> PyResult<()> {
        self.inner.unpersist().map_err(map_krishiv_error)
    }

    /// Register this DataFrame as a temporary SQL view named `name`.
    ///
    /// Subsequent `session.sql("SELECT * FROM <name>")` calls resolve against
    /// this DataFrame's query.
    pub fn create_or_replace_temp_view(&self, name: String) -> PyResult<()> {
        self.inner
            .create_or_replace_temp_view(&name)
            .map_err(map_krishiv_error)
    }

    pub fn num_rows(&self, py: Python<'_>) -> PyResult<usize> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .collect()
                .map(|r| r.row_count())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Create a :class:`DataStreamWriter` for writing this DataFrame as a streaming sink.
    ///
    /// ## Example
    ///
    /// ```python
    /// writer = df.write_stream()
    /// writer.output_mode("append")
    /// writer.trigger("processing_time", 1000)
    /// query = writer.start()
    /// query.await_termination(timeout_ms=30_000)
    /// ```
    pub fn write_stream(&self) -> crate::streaming::PyDataStreamWriter {
        crate::streaming::PyDataStreamWriter::new(self.inner.clone())
    }

    /// Convert this DataFrame into a :class:`StreamingDataFrame` builder.
    pub fn to_streaming(&self) -> crate::streaming_dataframe::PyStreamingDataFrame {
        crate::streaming_dataframe::PyStreamingDataFrame::new(self.inner.clone())
    }

    /// Return the schema as a list of `(column_name, arrow_type)` pairs.
    ///
    /// Example::
    ///
    ///     df.schema()  # => [("id", "Int64"), ("name", "Utf8")]
    pub fn schema(&self) -> PyResult<Vec<(String, String)>> {
        self.inner
            .schema()
            .map(|s| {
                s.fields()
                    .iter()
                    .map(|f| (f.name().clone(), f.data_type().to_string()))
                    .collect()
            })
            .map_err(map_krishiv_error)
    }

    /// Return column names.
    pub fn columns(&self) -> PyResult<Vec<String>> {
        self.inner
            .schema()
            .map(|s| s.fields().iter().map(|f| f.name().clone()).collect())
            .map_err(map_krishiv_error)
    }

    /// Compute summary statistics (count, mean, stddev, min, max per column).
    pub fn describe(&self, py: Python<'_>) -> PyResult<Self> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .describe()
                .map(|inner| Self { inner })
                .map_err(map_krishiv_error)
        })
    }

    /// Join this DataFrame with `right`.
    ///
    /// `on` — list of column names to join on (same name on both sides).
    /// `how` — join type: ``"inner"`` (default), ``"left"``, ``"right"``,
    ///   ``"full"``, ``"left_semi"``, ``"right_semi"``, ``"left_anti"``,
    ///   ``"right_anti"``.
    #[pyo3(signature = (right, on, *, how = "inner"))]
    pub fn join(&self, right: &PyDataFrame, on: Vec<String>, how: &str) -> PyResult<Self> {
        let left_on: Vec<&str> = on.iter().map(String::as_str).collect();
        let right_on: Vec<&str> = on.iter().map(String::as_str).collect();
        self.inner
            .join(&right.inner, how, &left_on, &right_on)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    /// Join using separate left and right key column lists.
    ///
    /// Supports the same ``how`` values as :meth:`join`.
    #[pyo3(signature = (right, left_on, right_on, *, how = "inner"))]
    pub fn join_on(
        &self,
        right: &PyDataFrame,
        left_on: Vec<String>,
        right_on: Vec<String>,
        how: &str,
    ) -> PyResult<Self> {
        let left_refs: Vec<&str> = left_on.iter().map(String::as_str).collect();
        let right_refs: Vec<&str> = right_on.iter().map(String::as_str).collect();
        self.inner
            .join(&right.inner, how, &left_refs, &right_refs)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    /// Repartition the DataFrame into `num_partitions` partitions.
    ///
    /// `key_columns` — optional list of columns to hash-partition by.
    /// When empty, round-robin partitioning is used.
    #[pyo3(signature = (num_partitions, key_columns = Vec::new()))]
    pub fn repartition(&self, num_partitions: u32, key_columns: Vec<String>) -> Self {
        let refs: Vec<&str> = key_columns.iter().map(String::as_str).collect();
        Self {
            inner: self.inner.clone().repartition(num_partitions, &refs),
        }
    }

    /// Collect and return both the result and execution stats.
    ///
    /// Returns ``(QueryResult, {"output_rows": int, "cpu_nanos": int})``.
    pub fn collect_with_stats(
        &self,
        py: Python<'_>,
    ) -> PyResult<(PyQueryResult, Py<pyo3::types::PyDict>)> {
        let inner = self.inner.clone();
        let (result, stats) = py.detach(move || {
            inner
                .collect_with_stats()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })?;
        let stats_dict = pyo3::types::PyDict::new(py);
        stats_dict.set_item("output_rows", stats.output_rows)?;
        stats_dict.set_item("cpu_nanos", stats.cpu_nanos)?;
        Ok((PyQueryResult::new(result), stats_dict.unbind()))
    }

    /// Sort ascending by `columns` (alias for ``sort`` with all-ascending order).
    pub fn order_by(&self, columns: Vec<String>) -> PyResult<Self> {
        let descending = vec![false; columns.len()];
        let refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
        self.inner
            .sort(&refs, &descending)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    /// Assign an alias to this DataFrame (for use in SQL references).
    pub fn alias(&self, name: String) -> PyResult<Self> {
        self.inner
            .alias(&name)
            .map(|inner| Self { inner })
            .map_err(map_krishiv_error)
    }

    pub fn __repr__(&self) -> String {
        format!("DataFrame(plan={})", self.inner.explain_logical())
    }
}

#[pyclass(name = "GroupedDataFrame")]
pub struct PyGroupedDataFrame {
    dataframe: krishiv_api::DataFrame,
    group_exprs: Vec<krishiv_api::Expr>,
}

#[pymethods]
impl PyGroupedDataFrame {
    pub fn agg(&self, expressions: Vec<String>) -> PyResult<PyDataFrame> {
        let groups = self.group_exprs.clone();
        let aggregates = expressions
            .into_iter()
            .map(krishiv_api::Expr::raw)
            .collect::<Vec<_>>();
        self.dataframe
            .group_by(&groups)
            .agg(&aggregates)
            .map(|inner| PyDataFrame { inner })
            .map_err(map_krishiv_error)
    }

    pub fn agg_columns(&self, expressions: Vec<PyColumn>) -> PyResult<PyDataFrame> {
        let aggregates = expressions
            .into_iter()
            .map(|column| column.inner)
            .collect::<Vec<_>>();
        self.dataframe
            .group_by(&self.group_exprs)
            .agg(&aggregates)
            .map(|inner| PyDataFrame { inner })
            .map_err(map_krishiv_error)
    }

    pub fn cube(&self, groups: Vec<PyColumn>, aggregates: Vec<PyColumn>) -> PyResult<PyDataFrame> {
        let grouping = krishiv_api::GroupingSpec::Cube(
            groups.into_iter().map(|column| column.inner).collect(),
        );
        let aggregates = aggregates
            .into_iter()
            .map(|column| column.inner)
            .collect::<Vec<_>>();
        self.dataframe
            .group_by(&self.group_exprs)
            .agg_grouping(grouping, &aggregates)
            .map(|inner| PyDataFrame { inner })
            .map_err(map_krishiv_error)
    }

    pub fn rollup(
        &self,
        groups: Vec<PyColumn>,
        aggregates: Vec<PyColumn>,
    ) -> PyResult<PyDataFrame> {
        let grouping = krishiv_api::GroupingSpec::Rollup(
            groups.into_iter().map(|column| column.inner).collect(),
        );
        let aggregates = aggregates
            .into_iter()
            .map(|column| column.inner)
            .collect::<Vec<_>>();
        self.dataframe
            .group_by(&self.group_exprs)
            .agg_grouping(grouping, &aggregates)
            .map(|inner| PyDataFrame { inner })
            .map_err(map_krishiv_error)
    }

    /// Apply explicit `GROUPING SETS` — each inner list defines one grouping set.
    ///
    /// ## Example
    /// ```python
    /// df.group_by(["region"]).agg_grouping_sets(
    ///     sets=[[col("region"), col("product")], [col("region")]],
    ///     aggregates=[sum("amount").alias("total")],
    /// )
    /// ```
    pub fn agg_grouping_sets(
        &self,
        sets: Vec<Vec<PyColumn>>,
        aggregates: Vec<PyColumn>,
    ) -> PyResult<PyDataFrame> {
        let grouping = krishiv_api::GroupingSpec::Sets(
            sets.into_iter()
                .map(|set| set.into_iter().map(|c| c.inner).collect())
                .collect(),
        );
        let aggregates = aggregates.into_iter().map(|c| c.inner).collect::<Vec<_>>();
        self.dataframe
            .group_by(&self.group_exprs)
            .agg_grouping(grouping, &aggregates)
            .map(|inner| PyDataFrame { inner })
            .map_err(map_krishiv_error)
    }

    pub fn count(&self) -> PyResult<PyDataFrame> {
        let groups = self.group_exprs.clone();
        self.dataframe
            .group_by(&groups)
            .count()
            .map(|inner| PyDataFrame { inner })
            .map_err(map_krishiv_error)
    }
}

#[pyclass(name = "DataFrameStream")]
pub struct PyDataFrameStream {
    pub(crate) stream: std::sync::Arc<tokio::sync::Mutex<krishiv_api::KrishivStream>>,
}

impl PyDataFrameStream {
    pub(crate) fn from_stream(stream: krishiv_api::KrishivStream) -> Self {
        Self {
            stream: std::sync::Arc::new(tokio::sync::Mutex::new(stream)),
        }
    }
}

#[pymethods]
impl PyDataFrameStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// NOTE: this method blocks a runtime worker per call via
    /// `block_on_async`. For high-throughput streams consider using
    /// `tokio::select!` with a timeout on the underlying stream future
    /// so that `__anext__` does not stall the event loop indefinitely
    /// when the stream has no data ready.
    fn __anext__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let stream = self.stream.clone();
        let next_item = py
            .detach(move || {
                crate::session::block_on_async(async move {
                    use futures::StreamExt;
                    let mut stream = stream.lock().await;
                    Ok::<_, krishiv_api::KrishivError>(stream.next().await)
                })
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        match next_item {
            Some(Ok(batch)) => Ok(Some(
                crate::batch::PyBatch::from_record_batch(batch)
                    .into_pyobject(py)?
                    .into_any()
                    .unbind(),
            )),
            Some(Err(e)) => Err(PyRuntimeError::new_err(e.to_string())),
            None => Err(pyo3::exceptions::PyStopAsyncIteration::new_err("")),
        }
    }
}

#[cfg(test)]
mod phase_c_tests {
    use super::*;

    #[test]
    fn canonical_python_dataframe_matches_rust_relational_results() {
        let session = krishiv_api::Session::builder().build().unwrap();
        let left = PyDataFrame {
            inner: session.sql("SELECT 1 AS id UNION ALL SELECT 2").unwrap(),
        };
        let right = PyDataFrame {
            inner: session.sql("SELECT 2 AS id").unwrap(),
        };
        assert!(left.is_bounded());
        let python = left
            .intersect_distinct(&right)
            .unwrap()
            .inner
            .collect()
            .unwrap();
        let rust = session
            .sql("SELECT 1 AS id UNION ALL SELECT 2")
            .unwrap()
            .intersect_distinct(&session.sql("SELECT 2 AS id").unwrap())
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(python.row_count(), rust.row_count());
    }
}
