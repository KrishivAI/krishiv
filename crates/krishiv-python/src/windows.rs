//! Window spec factories (`ks.windows.*`).

use pyo3::prelude::*;

use crate::errors::SchemaError;
use crate::pipeline::{WindowDescriptor, WindowKind};

/// Window configuration for `KeyedStream.window(spec)`.
#[pyclass(name = "WindowSpec")]
#[derive(Clone)]
pub struct PyWindowSpec {
    kind: WindowKind,
    size_ms: u64,
    slide_ms: Option<u64>,
    gap_ms: Option<u64>,
}

impl PyWindowSpec {
    pub fn into_descriptor(self) -> WindowDescriptor {
        WindowDescriptor {
            kind: self.kind,
            size_ms: self.size_ms,
            slide_ms: self.slide_ms,
            gap_ms: self.gap_ms,
        }
    }
}

#[pyfunction]
#[pyo3(name = "tumbling")]
fn windows_tumbling(size_ms: u64) -> PyWindowSpec {
    PyWindowSpec {
        kind: WindowKind::Tumbling,
        size_ms,
        slide_ms: None,
        gap_ms: None,
    }
}

#[pyfunction]
#[pyo3(name = "sliding")]
fn windows_sliding(size_ms: u64, slide_ms: u64) -> PyWindowSpec {
    PyWindowSpec {
        kind: WindowKind::Sliding,
        size_ms,
        slide_ms: Some(slide_ms),
        gap_ms: None,
    }
}

#[pyfunction]
#[pyo3(name = "session")]
fn windows_session(gap_ms: u64) -> PyWindowSpec {
    PyWindowSpec {
        kind: WindowKind::Session,
        size_ms: gap_ms,
        slide_ms: None,
        gap_ms: Some(gap_ms),
    }
}

pub fn register_windows_module(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let windows = PyModule::new(py, "windows")?;
    windows.add_function(wrap_pyfunction!(windows_tumbling, &windows)?)?;
    windows.add_function(wrap_pyfunction!(windows_sliding, &windows)?)?;
    windows.add_function(wrap_pyfunction!(windows_session, &windows)?)?;
    windows.add_class::<PyWindowSpec>()?;
    parent.add_submodule(&windows)?;
    Ok(())
}

pub fn ensure_watermark_before_window(
    watermark_column: &str,
    max_lateness_ms: u64,
) -> PyResult<()> {
    if watermark_column.is_empty() || max_lateness_ms == 0 {
        return Err(SchemaError::new_err(
            "call with_watermark(column, max_lateness_ms) before window()",
        ));
    }
    Ok(())
}
