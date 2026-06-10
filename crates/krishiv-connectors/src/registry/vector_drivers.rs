//! Vector sink drivers integrated with [`ConnectorRegistry`].

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::capabilities::ConnectorCapabilities;
use crate::config::ConnectorConfig;
use crate::error::{ConnectorError, ConnectorResult};
use crate::registry::SharedVectorSinkDriver;
use crate::registry::descriptor::ConnectorDescriptor;
use crate::registry::driver::VectorSinkDriver;
use crate::registry::kind::{ConnectorKind, ConnectorRole};
use crate::registry::ConnectorRegistry;
use crate::vector::VectorSinkRegistry;
use crate::vector::config::VectorSinkConfig;
use crate::vector::traits::{VectorSink, VectorSinkError};

fn map_vector_error(error: VectorSinkError) -> ConnectorError {
    ConnectorError::Config {
        message: error.to_string(),
    }
}

fn vector_config_from_connector(config: &ConnectorConfig) -> ConnectorResult<VectorSinkConfig> {
    let kind = ConnectorKind::parse(&config.kind)?;
    match kind {
        ConnectorKind::MemoryVector => Ok(VectorSinkConfig::Memory),
        ConnectorKind::LanceDb => Ok(VectorSinkConfig::LanceDb {
            uri: config.required("uri")?.to_string(),
            table: config.required("table")?.to_string(),
            vector_dim: config.required("vector_dim")?.parse().map_err(|_| {
                ConnectorError::Config {
                    message: "vector_dim must be a positive integer".into(),
                }
            })?,
        }),
        ConnectorKind::Weaviate => Ok(VectorSinkConfig::Weaviate {
            base_url: config.required("base_url")?.to_string(),
            class_name: config.required("class_name")?.to_string(),
            api_key: config.get("api_key").map(str::to_string),
        }),
        ConnectorKind::Pinecone => Ok(VectorSinkConfig::Pinecone {
            host: config.required("host")?.to_string(),
            api_key: config.required("api_key")?.to_string(),
            namespace: config.get("namespace").map(str::to_string),
        }),
        #[cfg(feature = "qdrant")]
        ConnectorKind::Qdrant => Ok(VectorSinkConfig::Qdrant {
            url: config.required("url")?.to_string(),
            collection: config.required("collection")?.to_string(),
            vector_size: config.required("vector_size")?.parse().map_err(|_| {
                ConnectorError::Config {
                    message: "vector_size must be an integer".into(),
                }
            })?,
            create_collection_if_missing: config
                .get("create_collection_if_missing")
                .map(|value| value == "true")
                .unwrap_or(true),
        }),
        #[cfg(feature = "pgvector")]
        ConnectorKind::Pgvector => Ok(VectorSinkConfig::Pgvector {
            database_url: config.required("database_url")?.to_string(),
            table: config.required("table")?.to_string(),
            vector_dim: config.required("vector_dim")?.parse().map_err(|_| {
                ConnectorError::Config {
                    message: "vector_dim must be a positive integer".into(),
                }
            })?,
        }),
        other => Err(ConnectorError::Config {
            message: format!("kind '{}' is not a vector sink", other.as_str()),
        }),
    }
}

struct GenericVectorSinkDriver {
    descriptor: ConnectorDescriptor,
}

impl GenericVectorSinkDriver {
    fn new(kind: ConnectorKind) -> Self {
        Self {
            descriptor: ConnectorDescriptor::new(
                kind,
                ConnectorRole::VectorSink,
                ConnectorCapabilities::new().with_unbounded(),
            ),
        }
    }
}

impl VectorSinkDriver for GenericVectorSinkDriver {
    fn descriptor(&self) -> ConnectorDescriptor {
        self.descriptor.clone()
    }

    fn validate(&self, config: &ConnectorConfig) -> ConnectorResult<()> {
        let _ = vector_config_from_connector(config)?;
        Ok(())
    }

    fn open<'a>(
        &'a self,
        config: &'a ConnectorConfig,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<Arc<dyn VectorSink>>> + Send + 'a>> {
        Box::pin(async move {
            let vector_config = vector_config_from_connector(config)?;
            VectorSinkRegistry::from_config(&vector_config)
                .await
                .map_err(map_vector_error)
        })
    }
}

pub(crate) fn register_builtin_vector_drivers(registry: &mut ConnectorRegistry) {
    registry.register_vector_sink(Arc::new(GenericVectorSinkDriver::new(
        ConnectorKind::MemoryVector,
    )) as SharedVectorSinkDriver);
    registry.register_vector_sink(
        Arc::new(GenericVectorSinkDriver::new(ConnectorKind::LanceDb)) as SharedVectorSinkDriver,
    );
    registry.register_vector_sink(
        Arc::new(GenericVectorSinkDriver::new(ConnectorKind::Weaviate)) as SharedVectorSinkDriver,
    );
    registry.register_vector_sink(
        Arc::new(GenericVectorSinkDriver::new(ConnectorKind::Pinecone)) as SharedVectorSinkDriver,
    );
    #[cfg(feature = "qdrant")]
    registry.register_vector_sink(
        Arc::new(GenericVectorSinkDriver::new(ConnectorKind::Qdrant)) as SharedVectorSinkDriver,
    );
    #[cfg(feature = "pgvector")]
    registry.register_vector_sink(
        Arc::new(GenericVectorSinkDriver::new(ConnectorKind::Pgvector)) as SharedVectorSinkDriver,
    );
}
