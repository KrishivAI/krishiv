//! Two-phase Parquet sink driver.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use crate::capabilities::ConnectorCapabilities;
use crate::config::ConnectorConfig;
use crate::error::ConnectorResult;
use crate::registry::descriptor::ConnectorDescriptor;
use crate::registry::driver::{OpenedTwoPhaseSink, TwoPhaseSinkDriver};
use crate::registry::kind::{ConnectorKind, ConnectorRole};
use crate::two_phase::LocalParquetTwoPhaseCommitSink;

pub struct LocalParquetTwoPhaseSinkDriver;

impl TwoPhaseSinkDriver for LocalParquetTwoPhaseSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::TwoPhaseParquet,
            ConnectorRole::TwoPhaseSink,
            ConnectorCapabilities::new().with_two_phase_commit(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        let _ = PathBuf::from(config.required("base_dir")?);
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<OpenedTwoPhaseSink>> + Send + 'a>> {
        Box::pin(async move {
            let base_dir = PathBuf::from(config.required("base_dir")?);
            Ok(OpenedTwoPhaseSink::LocalParquet(
                LocalParquetTwoPhaseCommitSink::new(base_dir),
            ))
        })
    }
}
