//! Python exception types for Krishiv.

use pyo3::prelude::*;

pyo3::create_exception!(krishiv, KrishivError, pyo3::exceptions::PyException);
pyo3::create_exception!(krishiv, QueryError, KrishivError);
pyo3::create_exception!(krishiv, SchemaError, KrishivError);
pyo3::create_exception!(krishiv, ConnectorError, KrishivError);
pyo3::create_exception!(krishiv, CheckpointError, KrishivError);
pyo3::create_exception!(krishiv, AuthorizationError, KrishivError);
pyo3::create_exception!(krishiv, ModeError, KrishivError);
pyo3::create_exception!(krishiv, UdfError, KrishivError);

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("KrishivError", py.get_type::<KrishivError>())?;
    m.add("QueryError", py.get_type::<QueryError>())?;
    m.add("SchemaError", py.get_type::<SchemaError>())?;
    m.add("ConnectorError", py.get_type::<ConnectorError>())?;
    m.add("CheckpointError", py.get_type::<CheckpointError>())?;
    m.add("AuthorizationError", py.get_type::<AuthorizationError>())?;
    m.add("ModeError", py.get_type::<ModeError>())?;
    m.add("UdfError", py.get_type::<UdfError>())?;
    Ok(())
}
