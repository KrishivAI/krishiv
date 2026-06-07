//! Object-store (S3-compatible) source and sink drivers.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;

use crate::capabilities::ConnectorCapabilities;
use crate::config::ConnectorConfig;
use crate::error::{ConnectorError, ConnectorResult};
use crate::registry::descriptor::ConnectorDescriptor;
use crate::registry::driver::{SinkDriver, SourceDriver};
use crate::registry::kind::{ConnectorKind, ConnectorRole};
use crate::s3::{S3Sink, S3Source};
use crate::sink::DynSink;
use crate::source::DynSource;

fn local_object_store(config: &ConnectorConfig) -> ConnectorResult<Arc<dyn ObjectStore>> {
    let base = config
        .get("base_path")
        .ok_or_else(|| ConnectorError::Config {
            message: "S3 connector requires property 'base_path' for local object-store roots"
                .into(),
        })?;
    let root = PathBuf::from(base);
    std::fs::create_dir_all(&root).map_err(ConnectorError::Io)?;
    Ok(Arc::new(LocalFileSystem::new_with_prefix(root).map_err(|e| {
        ConnectorError::ObjectStore {
            message: format!("failed to open local object store at '{base}': {e}"),
            status: None,
        }
    })?))
}

fn object_path(config: &ConnectorConfig) -> ConnectorResult<ObjectPath> {
    let key = config.required("object_path")?;
    ObjectPath::parse(key).map_err(|e| ConnectorError::Config {
        message: format!("invalid object_path '{key}': {e}"),
    })
}

pub struct S3SourceDriver;

impl SourceDriver for S3SourceDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::S3,
            ConnectorRole::Source,
            ConnectorCapabilities::new()
                .with_bounded()
                .with_rewindable()
                .with_checkpoint(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        let _ = local_object_store(config)?;
        let _ = object_path(config)?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSource>>> + Send + 'a>> {
        Box::pin(async move {
            let store = local_object_store(config)?;
            let path = object_path(config)?;
            let source = S3Source::open(store, path).await?;
            Ok(Box::new(source) as Box<dyn DynSource>)
        })
    }
}

pub struct S3SinkDriver;

impl SinkDriver for S3SinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor::new(
            ConnectorKind::S3,
            ConnectorRole::Sink,
            ConnectorCapabilities::new()
                .with_bounded()
                .with_idempotent(),
        )
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        let _ = local_object_store(config)?;
        let _ = object_path(config)?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Box<dyn DynSink>>> + Send + 'a>> {
        Box::pin(async move {
            let store = local_object_store(config)?;
            let path = object_path(config)?;
            let sink = S3Sink::new(store, path);
            Ok(Box::new(sink) as Box<dyn DynSink>)
        })
    }
}
