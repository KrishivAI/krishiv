//! Python bindings for [`krishiv_api::streaming_dataframe::StreamingDataFrame`].

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::dataframe::PyDataFrameStream;
use crate::errors::map_krishiv_error;

#[pyclass(name = "StreamingDataFrame")]
pub struct PyStreamingDataFrame {
    inner: krishiv_api::streaming_dataframe::StreamingDataFrame,
}

impl PyStreamingDataFrame {
    pub fn new(df: krishiv_api::DataFrame) -> Self {
        Self { inner: df.stream() }
    }

    /// Borrow the underlying engine `StreamingDataFrame` (e.g. to read its
    /// execution spec when submitting a continuous job).
    pub(crate) fn engine(&self) -> &krishiv_api::streaming_dataframe::StreamingDataFrame {
        &self.inner
    }
}

#[pymethods]
impl PyStreamingDataFrame {
    pub fn with_event_time(&self, column: String) -> Self {
        Self {
            inner: self.inner.clone().with_event_time(column),
        }
    }

    pub fn key_by(&self, column: String) -> Self {
        Self {
            inner: self.inner.clone().key_by(column),
        }
    }

    pub fn tumbling_window(&self, window_size_ms: u64) -> Self {
        Self {
            inner: self.inner.clone().tumbling_window(window_size_ms),
        }
    }

    pub fn sliding_window(&self, window_size_ms: u64, slide_ms: u64) -> Self {
        Self {
            inner: self.inner.clone().sliding_window(window_size_ms, slide_ms),
        }
    }

    pub fn session_window(&self, gap_ms: u64) -> Self {
        Self {
            inner: self.inner.clone().session_window(gap_ms),
        }
    }

    /// Set the windowed aggregations, e.g. ``agg(total=ks.agg.sum("amount"),
    /// n=ks.agg.count())``. Without this, a windowed ``collect()`` defaults to
    /// ``COUNT(*)`` as the single ``count`` column.
    #[pyo3(signature = (**kwargs))]
    pub fn agg(&self, kwargs: Option<&Bound<'_, pyo3::types::PyDict>>) -> PyResult<Self> {
        let descriptors = crate::agg::descriptors_from_kwargs(kwargs)?;
        if descriptors.is_empty() {
            return Err(PyRuntimeError::new_err(
                "agg() requires at least one named aggregation, \
                 e.g. agg(total=ks.agg.sum('amount'))",
            ));
        }
        let exprs: Vec<krishiv_dataflow::AggExpr> = descriptors
            .iter()
            .map(crate::stream_exec::agg_descriptor_to_expr)
            .collect();
        Ok(Self {
            inner: self.inner.clone().agg(exprs),
        })
    }

    pub fn with_watermark_lag(&self, lag_ms: u64) -> Self {
        Self {
            inner: self.inner.clone().with_watermark_lag(lag_ms),
        }
    }

    /// Per-key state TTL in milliseconds (expired keys are evicted).
    pub fn with_state_ttl(&self, ttl_ms: u64) -> Self {
        Self {
            inner: self.inner.clone().with_state_ttl(Some(ttl_ms)),
        }
    }

    /// Add a per-source watermark lag (source_id -> lag_ms) for multi-source
    /// watermark reconciliation (effective watermark = min across sources).
    pub fn with_source_watermark(&self, source_id: String, lag_ms: u64) -> Self {
        Self {
            inner: self.inner.clone().with_source_watermark(source_id, lag_ms),
        }
    }

    /// Column identifying which source each row came from (required with
    /// per-source watermark lags).
    pub fn with_source_id_column(&self, column: String) -> Self {
        Self {
            inner: self.inner.clone().with_source_id_column(column),
        }
    }

    pub fn with_side_output(&self, name: String, lateness_threshold_ms: u64) -> Self {
        Self {
            inner: self
                .inner
                .clone()
                .with_side_output(name, lateness_threshold_ms),
        }
    }

    #[pyo3(signature = (*, subset))]
    pub fn drop_duplicates(&self, subset: Vec<String>) -> Self {
        Self {
            inner: self.inner.clone().drop_duplicates(subset),
        }
    }

    /// Like :meth:`drop_duplicates`, but keeps the seen-keys set in **durable,
    /// state-backed** storage (RocksDB) so deduplication survives across an
    /// unbounded stream (and restarts) instead of an in-memory set.
    #[pyo3(signature = (*, subset))]
    pub fn drop_duplicates_with_state(&self, subset: Vec<String>) -> Self {
        Self {
            inner: self.inner.clone().drop_duplicates_with_state(subset),
        }
    }

    // ── Stateless transforms (before windowing) — Spark's "same DataFrame API
    // for batch and streaming". Delegate to the underlying DataFrame. ──
    pub fn select(&self, columns: Vec<String>) -> PyResult<Self> {
        let cols: Vec<&str> = columns.iter().map(String::as_str).collect();
        Ok(Self {
            inner: self.inner.clone().select(&cols).map_err(map_krishiv_error)?,
        })
    }

    pub fn filter(&self, predicate: String) -> PyResult<Self> {
        Ok(Self {
            inner: self
                .inner
                .clone()
                .filter(&predicate)
                .map_err(map_krishiv_error)?,
        })
    }

    pub fn with_column(&self, name: String, expr: String) -> PyResult<Self> {
        Ok(Self {
            inner: self
                .inner
                .clone()
                .with_column(&name, &expr)
                .map_err(map_krishiv_error)?,
        })
    }

    pub fn drop_columns(&self, columns: Vec<String>) -> PyResult<Self> {
        let cols: Vec<&str> = columns.iter().map(String::as_str).collect();
        Ok(Self {
            inner: self
                .inner
                .clone()
                .drop_columns(&cols)
                .map_err(map_krishiv_error)?,
        })
    }

    /// Flink-style `transformWithState` — the single low-level escape hatch.
    ///
    /// `func` is a handler object with `on_event(key, batch, row, ctx)` and
    /// `on_timer(key, fire_time_ms, ctx)`; inside them it may use ValueState /
    /// ListState / MapState / ReducingState / AggregatingState and register
    /// event/processing-time timers via `ctx`. Requires `key_by(...)`. Returns
    /// the stream of emitted rows (bypasses window()+agg()).
    pub fn transform_with_state(
        &self,
        py: Python<'_>,
        func: Py<PyAny>,
    ) -> PyResult<PyDataFrameStream> {
        let bridge = crate::process_api::bridge_from_func(py, &func)?;
        let inner = self.inner.clone();
        let out_stream = py
            .detach(move || {
                crate::session::block_on_async(async move {
                    inner.transform_with_state(Box::new(bridge)).await
                })
            })
            .map_err(map_krishiv_error)?;
        Ok(PyDataFrameStream::from_stream(out_stream))
    }

    /// Collect this windowed streaming DataFrame as a bounded result. On a
    /// distributed session the windowed aggregation runs DISTRIBUTED on the
    /// cluster (via the coordinator); embedded sessions run it in-process.
    pub fn collect(&self, py: Python<'_>) -> PyResult<Vec<crate::batch::PyBatch>> {
        let inner = self.inner.clone();
        let batches = py
            .detach(move || {
                crate::session::block_on_async(async move { inner.collect_bounded().await })
            })
            .map_err(map_krishiv_error)?;
        Ok(batches
            .into_iter()
            .map(crate::batch::PyBatch::from_record_batch)
            .collect())
    }

    /// Connect this streaming DataFrame with `other` for dual-stream
    /// `CoProcessFunction` processing (Flink connected streams). `func` is a
    /// handler with `on_stream1`/`on_stream2`/`on_timer`; both streams are keyed
    /// by `key_column`. Returns a stream of the batches the handler emits.
    pub fn co_process(
        &self,
        py: Python<'_>,
        other: &PyStreamingDataFrame,
        key_column: String,
        func: Py<PyAny>,
    ) -> PyResult<PyDataFrameStream> {
        let bridge = crate::stream_bridges::co_bridge_from_func(py, &func)?;
        let left_df = self.inner.source_df();
        let right_df = other.inner.source_df();
        let out = py.detach(move || -> PyResult<Vec<arrow::record_batch::RecordBatch>> {
            let left = crate::session::block_on_async(async move { left_df.collect_async().await })
                .map_err(map_krishiv_error)?
                .into_batches();
            let right =
                crate::session::block_on_async(async move { right_df.collect_async().await })
                    .map_err(map_krishiv_error)?
                    .into_batches();
            let mut ex = krishiv_api::CoProcessExecutor::new(&key_column, Box::new(bridge));
            let err = |e: krishiv_dataflow::ExecError| PyRuntimeError::new_err(e.to_string());
            let mut emitted = Vec::new();
            for b in &left {
                emitted.extend(ex.process_stream1(b, 0).map_err(err)?);
            }
            for b in &right {
                emitted.extend(ex.process_stream2(b, 0).map_err(err)?);
            }
            emitted.extend(ex.fire_timers(i64::MAX).map_err(err)?);
            Ok(emitted)
        })?;
        Ok(PyDataFrameStream::from_stream(Box::pin(
            futures::stream::iter(out.into_iter().map(Ok::<_, String>)),
        )))
    }

    /// Process this (keyed) streaming DataFrame against a `broadcast` streaming
    /// DataFrame with a `BroadcastProcessFunction` (Flink broadcast state).
    /// `func` has `on_keyed_event`/`on_broadcast_event`; `key_column` shards the
    /// keyed state. Returns a stream of the emitted batches.
    pub fn broadcast_process(
        &self,
        py: Python<'_>,
        broadcast: &PyStreamingDataFrame,
        key_column: String,
        func: Py<PyAny>,
    ) -> PyResult<PyDataFrameStream> {
        let bridge = crate::stream_bridges::broadcast_bridge_from_func(py, &func)?;
        let keyed_df = self.inner.source_df();
        let broadcast_df = broadcast.inner.source_df();
        let out = py.detach(move || -> PyResult<Vec<arrow::record_batch::RecordBatch>> {
            let bcast =
                crate::session::block_on_async(async move { broadcast_df.collect_async().await })
                    .map_err(map_krishiv_error)?
                    .into_batches();
            let keyed =
                crate::session::block_on_async(async move { keyed_df.collect_async().await })
                    .map_err(map_krishiv_error)?
                    .into_batches();
            let mut ex = krishiv_api::BroadcastProcessExecutor::new(&key_column, Box::new(bridge));
            let err = |e: krishiv_dataflow::ExecError| PyRuntimeError::new_err(e.to_string());
            let mut emitted = Vec::new();
            for b in &bcast {
                emitted.extend(ex.process_broadcast_batch(b, 0).map_err(err)?);
            }
            for b in &keyed {
                emitted.extend(ex.process_keyed_batch(b, 0).map_err(err)?);
            }
            Ok(emitted)
        })?;
        Ok(PyDataFrameStream::from_stream(Box::pin(
            futures::stream::iter(out.into_iter().map(Ok::<_, String>)),
        )))
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
        Ok(PyDataFrameStream::from_stream(stream))
    }

    /// Run the pipeline and return ``(main_stream, late_stream)``: records that
    /// fall past the side-output lateness threshold are routed to
    /// ``late_stream`` instead of being dropped. Requires
    /// :meth:`with_side_output`. Poll both streams concurrently.
    pub fn execute_stream_with_side_output_async(
        &self,
        py: Python<'_>,
    ) -> PyResult<(PyDataFrameStream, PyDataFrameStream)> {
        let inner = self.inner.clone();
        let (main, late) = py
            .detach(move || {
                crate::session::block_on_async(async move {
                    let streams = inner.execute_stream_with_side_output_async().await?;
                    let (main, side) = streams.into_parts();
                    Ok::<_, krishiv_api::KrishivError>((main, side.into_stream()))
                })
            })
            .map_err(map_krishiv_error)?;
        Ok((
            PyDataFrameStream::from_stream(main),
            PyDataFrameStream::from_stream(late),
        ))
    }

    pub fn write_stream(&self) -> PyResult<crate::streaming::PyDataStreamWriter> {
        // Rebuild from the streaming builder's underlying DataFrame via a fresh stream().
        Err(PyRuntimeError::new_err(
            "use DataFrame.write_stream() for structured streaming sinks; \
             StreamingDataFrame.execute_stream_async() runs the pipeline",
        ))
    }
}

#[pyfunction]
#[pyo3(signature = (left, right, left_time_col, right_time_col, left_key_col, right_key_col, lower_bound_ms, upper_bound_ms))]
pub fn interval_join(
    left: Vec<crate::batch::PyBatch>,
    right: Vec<crate::batch::PyBatch>,
    left_time_col: String,
    right_time_col: String,
    left_key_col: String,
    right_key_col: String,
    lower_bound_ms: i64,
    upper_bound_ms: i64,
) -> PyResult<Vec<(crate::batch::PyBatch, crate::batch::PyBatch)>> {
    let left_batches: Vec<_> = left.into_iter().map(|b| b.record_batch().clone()).collect();
    let right_batches: Vec<_> = right
        .into_iter()
        .map(|b| b.record_batch().clone())
        .collect();
    let pairs = krishiv_api::streaming_dataframe::StreamingDataFrame::stream_stream_join(
        &left_batches,
        &right_batches,
        &left_time_col,
        &right_time_col,
        &left_key_col,
        &right_key_col,
        lower_bound_ms,
        upper_bound_ms,
    )
    .map_err(map_krishiv_error)?;
    Ok(pairs
        .into_iter()
        .map(|(l, r)| {
            (
                crate::batch::PyBatch::from_record_batch(l.as_ref().clone()),
                crate::batch::PyBatch::from_record_batch(r.as_ref().clone()),
            )
        })
        .collect())
}

#[pyfunction]
#[pyo3(signature = (stream_batches, table_snapshots, stream_time_col, version_col, lookback_ms, *, inner_join = false))]
pub fn stream_table_join(
    stream_batches: Vec<crate::batch::PyBatch>,
    table_snapshots: Vec<crate::batch::PyBatch>,
    stream_time_col: String,
    version_col: String,
    lookback_ms: i64,
    inner_join: bool,
) -> PyResult<Vec<(crate::batch::PyBatch, Option<crate::batch::PyBatch>)>> {
    let stream: Vec<_> = stream_batches
        .into_iter()
        .map(|b| b.record_batch().clone())
        .collect();
    let table: Vec<_> = table_snapshots
        .into_iter()
        .map(|b| b.record_batch().clone())
        .collect();
    let pairs = krishiv_api::streaming_dataframe::StreamingDataFrame::stream_table_join(
        &stream,
        &table,
        &stream_time_col,
        &version_col,
        lookback_ms,
        inner_join,
    )
    .map_err(map_krishiv_error)?;
    Ok(pairs
        .into_iter()
        .map(|(stream_batch, table_batch)| {
            (
                crate::batch::PyBatch::from_record_batch(stream_batch),
                table_batch.map(|batch| crate::batch::PyBatch::from_record_batch(batch)),
            )
        })
        .collect())
}

#[pyfunction]
#[pyo3(signature = (stream_batches, table_snapshots, stream_time_col, version_col, lookback_ms, *, inner_join = false, join_keys = None))]
pub fn temporal_join(
    stream_batches: Vec<crate::batch::PyBatch>,
    table_snapshots: Vec<crate::batch::PyBatch>,
    stream_time_col: String,
    version_col: String,
    lookback_ms: i64,
    inner_join: bool,
    join_keys: Option<Vec<String>>,
) -> PyResult<Vec<(crate::batch::PyBatch, Option<crate::batch::PyBatch>)>> {
    use krishiv_dataflow::temporal_join::TemporalJoinSpec;

    let stream: Vec<_> = stream_batches
        .into_iter()
        .map(|b| b.record_batch().clone())
        .collect();
    let table: Vec<_> = table_snapshots
        .into_iter()
        .map(|b| b.record_batch().clone())
        .collect();
    let spec = TemporalJoinSpec {
        stream_time_col,
        join_keys: join_keys.unwrap_or_default(),
        inner_join,
    };
    let pairs = krishiv_api::streaming_dataframe::temporal_join(
        &stream,
        &table,
        &spec,
        &version_col,
        lookback_ms,
    )
    .map_err(map_krishiv_error)?;
    Ok(pairs
        .into_iter()
        .map(|(stream_batch, table_batch)| {
            (
                crate::batch::PyBatch::from_record_batch(stream_batch),
                table_batch.map(|batch| crate::batch::PyBatch::from_record_batch(batch)),
            )
        })
        .collect())
}

#[pyfunction]
#[pyo3(signature = (left, right, left_time_col, right_time_col, left_key_col, right_key_col, lower_bound_ms, upper_bound_ms))]
pub fn stream_stream_join(
    left: Vec<crate::batch::PyBatch>,
    right: Vec<crate::batch::PyBatch>,
    left_time_col: String,
    right_time_col: String,
    left_key_col: String,
    right_key_col: String,
    lower_bound_ms: i64,
    upper_bound_ms: i64,
) -> PyResult<Vec<(crate::batch::PyBatch, crate::batch::PyBatch)>> {
    let left_batches: Vec<_> = left.into_iter().map(|b| b.record_batch().clone()).collect();
    let right_batches: Vec<_> = right
        .into_iter()
        .map(|b| b.record_batch().clone())
        .collect();
    let pairs = krishiv_api::streaming_dataframe::StreamingDataFrame::stream_stream_join(
        &left_batches,
        &right_batches,
        &left_time_col,
        &right_time_col,
        &left_key_col,
        &right_key_col,
        lower_bound_ms,
        upper_bound_ms,
    )
    .map_err(map_krishiv_error)?;
    Ok(pairs
        .into_iter()
        .map(|(l, r)| {
            (
                crate::batch::PyBatch::from_record_batch(l.as_ref().clone()),
                crate::batch::PyBatch::from_record_batch(r.as_ref().clone()),
            )
        })
        .collect())
}

#[pyclass(name = "DataStreamReader")]
pub struct PyDataStreamReader {
    session: krishiv_api::Session,
}

impl PyDataStreamReader {
    pub fn new(session: krishiv_api::Session) -> Self {
        Self { session }
    }
}

#[pymethods]
impl PyDataStreamReader {
    pub fn file_stream(&self, path: String) -> PyResult<crate::dataframe::PyDataFrame> {
        self.session
            .read_stream()
            .file_stream(path)
            .map(|df| crate::dataframe::PyDataFrame { inner: df })
            .map_err(map_krishiv_error)
    }

    /// Read a Kafka topic as a streaming source, returning a :class:`DataFrame`
    /// that can be turned into a :class:`StreamingDataFrame` via
    /// ``.to_streaming()`` (or fed directly into streaming SQL).
    ///
    /// ``schema`` is an optional Krishiv schema class describing the message
    /// payload; when omitted the topic is exposed as a single ``value`` string
    /// column. Requires the ``kafka`` feature (``pip install krishiv[kafka]``).
    #[pyo3(signature = (brokers, topic, group_id = "krishiv".to_string(), schema = None))]
    pub fn kafka(
        &self,
        #[allow(unused_variables)] py: Python<'_>,
        brokers: String,
        topic: String,
        group_id: String,
        schema: Option<Py<pyo3::types::PyType>>,
    ) -> PyResult<crate::dataframe::PyDataFrame> {
        #[cfg(not(feature = "kafka"))]
        {
            let _ = (brokers, topic, group_id, schema);
            Err(crate::errors::ConnectorError::new_err(
                "Kafka support requires the `kafka` feature (pip install krishiv[kafka])",
            ))
        }
        #[cfg(feature = "kafka")]
        {
            use arrow::datatypes::{DataType, Field, Schema};
            use std::sync::Arc;
            let arrow_schema: arrow::datatypes::SchemaRef = if let Some(cls) = schema {
                crate::schema::PySchema::arrow_schema_from_class(cls.bind(py))?
            } else {
                Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
            };
            let sanitized: String = topic
                .chars()
                .map(|c| if c.is_alphanumeric() { c } else { '_' })
                .collect();
            let name = format!("kafka_{sanitized}");
            // rdkafka eagerly spawns a poll thread when the consumer is created,
            // which requires an active Tokio reactor. Enter the persistent global
            // runtime so the consumer's background task lives on it (not on a
            // transient one that would be torn down after this call returns).
            let _rt_guard = crate::RUNTIME.enter();
            self.session
                .register_kafka_source(&name, arrow_schema, brokers, topic, group_id)
                .map_err(map_krishiv_error)?;
            let df = self
                .session
                .sql(format!("SELECT * FROM {name}"))
                .map_err(map_krishiv_error)?;
            Ok(crate::dataframe::PyDataFrame { inner: df })
        }
    }
}
