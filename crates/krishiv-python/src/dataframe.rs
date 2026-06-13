//! `DataFrame` batch SQL results.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::errors::map_krishiv_error;
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

    pub fn group_by(&self, expressions: Vec<String>) -> PyGroupedDataFrame {
        PyGroupedDataFrame {
            dataframe: self.inner.clone(),
            group_exprs: expressions,
        }
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
        py.detach(move || inner.cache().map(|inner| Self { inner }).map_err(map_krishiv_error))
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

    pub fn __repr__(&self) -> String {
        format!("DataFrame(plan={})", self.inner.explain_logical())
    }
}

#[pyclass(name = "GroupedDataFrame")]
pub struct PyGroupedDataFrame {
    dataframe: krishiv_api::DataFrame,
    group_exprs: Vec<String>,
}

#[pymethods]
impl PyGroupedDataFrame {
    pub fn agg(&self, expressions: Vec<String>) -> PyResult<PyDataFrame> {
        let groups = self
            .group_exprs
            .iter()
            .cloned()
            .map(krishiv_api::Expr::raw)
            .collect::<Vec<_>>();
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

    pub fn count(&self) -> PyResult<PyDataFrame> {
        let groups = self
            .group_exprs
            .iter()
            .cloned()
            .map(krishiv_api::Expr::raw)
            .collect::<Vec<_>>();
        self.dataframe
            .group_by(&groups)
            .count()
            .map(|inner| PyDataFrame { inner })
            .map_err(map_krishiv_error)
    }
}

#[pyclass(name = "DataFrameStream")]
pub struct PyDataFrameStream {
    stream: std::sync::Arc<tokio::sync::Mutex<krishiv_api::KrishivStream>>,
}

#[pymethods]
impl PyDataFrameStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

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
