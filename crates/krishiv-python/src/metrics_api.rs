//! Python bindings for `krishiv-metrics` — Prometheus render and counter snapshots.

use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Render all current Krishiv metrics as a Prometheus-format text string.
///
/// The returned string can be exposed on an HTTP ``/metrics`` endpoint.
///
/// Example::
///
///     import krishiv
///     text = krishiv.metrics.render_prometheus()
///     # e.g. expose via a simple HTTP server
///     print(text[:200])
#[pyfunction]
pub fn render_prometheus(py: Python<'_>) -> PyResult<String> {
    py.detach(|| Ok(krishiv_metrics::global_metrics().render_prometheus()))
}

/// Return a snapshot of the current global Krishiv metrics as a plain dict.
///
/// Keys: ``tasks_submitted``, ``tasks_succeeded``, ``tasks_failed``,
/// ``executor_lost``, ``shuffle_bytes_written``, ``job_queue_depth``,
/// ``watermark_entry_count``, ``state_key_entry_count``,
/// ``spill_bytes_total``, ``spill_files_total``.
/// Per-job labeled counters (checkpoint epochs, task attempts, etc.) are
/// exposed only via :py:func:`render_prometheus`.
///
/// Example::
///
///     snap = krishiv.metrics.snapshot()
///     print(snap["tasks_succeeded"])
#[pyfunction]
pub fn snapshot(py: Python<'_>) -> PyResult<Py<PyDict>> {
    let m = krishiv_metrics::global_metrics();
    let dict = PyDict::new(py);
    dict.set_item("tasks_submitted", m.tasks_submitted())?;
    dict.set_item("tasks_succeeded", m.tasks_succeeded())?;
    dict.set_item("tasks_failed", m.tasks_failed())?;
    dict.set_item("executor_lost", m.executor_lost())?;
    dict.set_item("shuffle_bytes_written", m.shuffle_bytes_written())?;
    dict.set_item("job_queue_depth", m.job_queue_depth())?;
    dict.set_item("watermark_entry_count", m.watermark_entry_count())?;
    dict.set_item("state_key_entry_count", m.state_key_entry_count())?;
    dict.set_item("spill_bytes_total", m.spill_bytes_total())?;
    dict.set_item("spill_files_total", m.spill_files_total())?;
    Ok(dict.unbind())
}

pub fn register_metrics_module(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let m = PyModule::new(py, "metrics")?;
    m.add_function(wrap_pyfunction!(render_prometheus, &m)?)?;
    m.add_function(wrap_pyfunction!(snapshot, &m)?)?;
    parent.add_submodule(&m)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("krishiv.metrics", &m)?;
    Ok(())
}
