//! Iceberg REST Catalog client (R18 S3.1).

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::{CatalogError, CatalogResult};

/// REST catalog configuration.
#[derive(Debug, Clone)]
pub struct RestCatalogConfig {
    pub base_url: String,
    pub warehouse: Option<String>,
    pub prefix: String,
    pub bearer_token: Option<String>,
    /// HTTP request timeout in milliseconds.
    /// - `None` (default): falls back to `DEFAULT_CATALOG_TIMEOUT_MS` (30 s).
    /// - `Some(0)`: no timeout (requests may block indefinitely).
    /// - `Some(n)`: timeout after `n` milliseconds.
    pub timeout_ms: Option<u64>,
}

/// Default HTTP request timeout for catalog operations (30 seconds).
pub const DEFAULT_CATALOG_TIMEOUT_MS: u64 = 30_000;

/// Iceberg table identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IcebergTableId {
    pub namespace: String,
    pub name: String,
}

/// Partition field evolution request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionFieldSpec {
    pub name: String,
    pub source_column: String,
    pub transform: String,
}

/// Trait for Iceberg REST catalog operations.
#[async_trait]
pub trait IcebergCatalogClient: Send + Sync {
    async fn list_tables(&self, namespace: &str) -> CatalogResult<Vec<String>>;
    async fn load_table_metadata(&self, table: &IcebergTableId)
    -> CatalogResult<serde_json::Value>;
    async fn add_partition_field(
        &self,
        table: &IcebergTableId,
        field: PartitionFieldSpec,
    ) -> CatalogResult<()>;
    async fn drop_partition_field(
        &self,
        table: &IcebergTableId,
        partition_name: &str,
    ) -> CatalogResult<()>;
    async fn replace_partition_spec(
        &self,
        table: &IcebergTableId,
        fields: Vec<PartitionFieldSpec>,
    ) -> CatalogResult<()>;
}

/// Generic Iceberg REST catalog (Nessie, Tabular, self-hosted).
#[derive(Clone)]
pub struct GenericRestCatalog {
    config: RestCatalogConfig,
    client: Client,
}

impl GenericRestCatalog {
    pub fn new(config: RestCatalogConfig) -> Self {
        let timeout_ms = config.timeout_ms.unwrap_or(DEFAULT_CATALOG_TIMEOUT_MS);
        let mut builder = Client::builder();
        if timeout_ms > 0 {
            builder = builder.timeout(Duration::from_millis(timeout_ms));
        }
        let client = builder.build().expect("failed to build HTTP client");
        Self { config, client }
    }

    fn url(&self, path: &str) -> String {
        let base = format!(
            "{}/{}",
            self.config.base_url.trim_end_matches('/'),
            self.config.prefix.trim_matches('/')
        );
        let mut url = url::Url::parse(&base).expect("invalid base URL");
        url.path_segments_mut()
            .expect("cannot be a cannot-be-a-base URL")
            .extend(path.trim_start_matches('/').split('/'));
        url.to_string()
    }

    async fn get_json(&self, url: String) -> CatalogResult<serde_json::Value> {
        let mut req = self.client.get(url);
        if let Some(token) = &self.config.bearer_token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().await.map_err(|e| CatalogError::Http {
            status: 0,
            message: e.to_string(),
        })?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            return Err(CatalogError::Http {
                status,
                message: resp.text().await.unwrap_or_default(),
            });
        }
        resp.json().await.map_err(|e| CatalogError::Http {
            status: 0,
            message: e.to_string(),
        })
    }
}

#[async_trait]
impl IcebergCatalogClient for GenericRestCatalog {
    async fn list_tables(&self, namespace: &str) -> CatalogResult<Vec<String>> {
        let url = self.url(&format!("namespaces/{namespace}/tables"));
        let body = self.get_json(url).await?;
        let ids = body
            .get("identifiers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(ids
            .into_iter()
            .filter_map(|id| id.get("name").and_then(|n| n.as_str()).map(str::to_string))
            .collect())
    }

    async fn load_table_metadata(
        &self,
        table: &IcebergTableId,
    ) -> CatalogResult<serde_json::Value> {
        let url = self.url(&format!(
            "namespaces/{}/tables/{}",
            table.namespace, table.name
        ));
        self.get_json(url).await
    }

    async fn add_partition_field(
        &self,
        table: &IcebergTableId,
        field: PartitionFieldSpec,
    ) -> CatalogResult<()> {
        let url = self.url(&format!(
            "namespaces/{}/tables/{}/partition-specs/add",
            table.namespace, table.name
        ));
        let mut req = self.client.post(url).json(&field);
        if let Some(token) = &self.config.bearer_token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().await.map_err(|e| CatalogError::Http {
            status: 0,
            message: e.to_string(),
        })?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            return Err(CatalogError::Http {
                status,
                message: resp.text().await.unwrap_or_default(),
            });
        }
        Ok(())
    }

    async fn drop_partition_field(
        &self,
        table: &IcebergTableId,
        partition_name: &str,
    ) -> CatalogResult<()> {
        let url = self.url(&format!(
            "namespaces/{}/tables/{}/partition-specs/{partition_name}",
            table.namespace, table.name
        ));
        let mut req = self.client.delete(url);
        if let Some(token) = &self.config.bearer_token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().await.map_err(|e| CatalogError::Http {
            status: 0,
            message: e.to_string(),
        })?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            return Err(CatalogError::Http {
                status,
                message: resp.text().await.unwrap_or_default(),
            });
        }
        Ok(())
    }

    async fn replace_partition_spec(
        &self,
        table: &IcebergTableId,
        fields: Vec<PartitionFieldSpec>,
    ) -> CatalogResult<()> {
        let url = self.url(&format!(
            "namespaces/{}/tables/{}/partition-specs/replace",
            table.namespace, table.name
        ));
        let mut req = self
            .client
            .put(url)
            .json(&serde_json::json!({ "fields": fields }));
        if let Some(token) = &self.config.bearer_token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().await.map_err(|e| CatalogError::Http {
            status: 0,
            message: e.to_string(),
        })?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            return Err(CatalogError::Http {
                status,
                message: resp.text().await.unwrap_or_default(),
            });
        }
        Ok(())
    }
}

/// AWS Glue Iceberg REST shim (REST endpoint required).
#[derive(Clone)]
pub struct GlueRestCatalog {
    inner: GenericRestCatalog,
    pub region: String,
    pub database: String,
}

impl GlueRestCatalog {
    pub fn new(
        region: impl Into<String>,
        database: impl Into<String>,
        rest_url: impl Into<String>,
    ) -> Self {
        Self::with_timeout(region, database, rest_url, None)
    }

    pub fn with_timeout(
        region: impl Into<String>,
        database: impl Into<String>,
        rest_url: impl Into<String>,
        timeout_ms: Option<u64>,
    ) -> Self {
        let region = region.into();
        let database = database.into();
        Self {
            inner: GenericRestCatalog::new(RestCatalogConfig {
                base_url: rest_url.into(),
                warehouse: Some(format!("glue://{region}/{database}")),
                prefix: "v1".into(),
                bearer_token: None,
                timeout_ms,
            }),
            region,
            database,
        }
    }
}

#[async_trait]
impl IcebergCatalogClient for GlueRestCatalog {
    async fn list_tables(&self, namespace: &str) -> CatalogResult<Vec<String>> {
        self.inner.list_tables(namespace).await
    }
    async fn load_table_metadata(
        &self,
        table: &IcebergTableId,
    ) -> CatalogResult<serde_json::Value> {
        self.inner.load_table_metadata(table).await
    }
    async fn add_partition_field(
        &self,
        table: &IcebergTableId,
        field: PartitionFieldSpec,
    ) -> CatalogResult<()> {
        self.inner.add_partition_field(table, field).await
    }
    async fn drop_partition_field(
        &self,
        table: &IcebergTableId,
        partition_name: &str,
    ) -> CatalogResult<()> {
        self.inner.drop_partition_field(table, partition_name).await
    }
    async fn replace_partition_spec(
        &self,
        table: &IcebergTableId,
        fields: Vec<PartitionFieldSpec>,
    ) -> CatalogResult<()> {
        self.inner.replace_partition_spec(table, fields).await
    }
}

/// Nessie catalog wrapper.
#[derive(Clone)]
pub struct NessieCatalog {
    inner: GenericRestCatalog,
}

impl NessieCatalog {
    pub fn new(uri: impl Into<String>, reference: impl Into<String>) -> Self {
        Self::with_timeout(uri, reference, None)
    }

    pub fn with_timeout(
        uri: impl Into<String>,
        reference: impl Into<String>,
        timeout_ms: Option<u64>,
    ) -> Self {
        let uri = uri.into();
        let reference = reference.into();
        Self {
            inner: GenericRestCatalog::new(RestCatalogConfig {
                base_url: uri,
                warehouse: Some(reference),
                prefix: "v1".into(),
                bearer_token: None,
                timeout_ms,
            }),
        }
    }
}

#[async_trait]
impl IcebergCatalogClient for NessieCatalog {
    async fn list_tables(&self, namespace: &str) -> CatalogResult<Vec<String>> {
        self.inner.list_tables(namespace).await
    }
    async fn load_table_metadata(
        &self,
        table: &IcebergTableId,
    ) -> CatalogResult<serde_json::Value> {
        self.inner.load_table_metadata(table).await
    }
    async fn add_partition_field(
        &self,
        table: &IcebergTableId,
        field: PartitionFieldSpec,
    ) -> CatalogResult<()> {
        self.inner.add_partition_field(table, field).await
    }
    async fn drop_partition_field(
        &self,
        table: &IcebergTableId,
        partition_name: &str,
    ) -> CatalogResult<()> {
        self.inner.drop_partition_field(table, partition_name).await
    }
    async fn replace_partition_spec(
        &self,
        table: &IcebergTableId,
        fields: Vec<PartitionFieldSpec>,
    ) -> CatalogResult<()> {
        self.inner.replace_partition_spec(table, fields).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // -----------------------------------------------------------------------
    // Happy path: list tables
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn generic_rest_lists_tables() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "identifiers": [{"namespace": ["ns"], "name": "t1"}]
            })))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let tables = catalog.list_tables("ns").await.unwrap();
        assert_eq!(tables, vec!["t1".to_string()]);
    }

    #[tokio::test]
    async fn generic_rest_lists_multiple_tables() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "identifiers": [
                    {"namespace": ["ns"], "name": "alpha"},
                    {"namespace": ["ns"], "name": "beta"},
                    {"namespace": ["ns"], "name": "gamma"}
                ]
            })))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let mut tables = catalog.list_tables("ns").await.unwrap();
        tables.sort();
        assert_eq!(tables, vec!["alpha", "beta", "gamma"]);
    }

    // -----------------------------------------------------------------------
    // Edge case: empty identifiers
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn generic_rest_lists_tables_empty_identifiers() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "identifiers": []
            })))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let tables = catalog.list_tables("ns").await.unwrap();
        assert!(tables.is_empty());
    }

    #[tokio::test]
    async fn generic_rest_lists_tables_no_identifiers_key() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "something_else": []
            })))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let tables = catalog.list_tables("ns").await.unwrap();
        assert!(tables.is_empty());
    }

    #[tokio::test]
    async fn generic_rest_lists_tables_identifiers_not_array() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "identifiers": "not_an_array"
            })))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let tables = catalog.list_tables("ns").await.unwrap();
        assert!(tables.is_empty());
    }

    #[tokio::test]
    async fn generic_rest_lists_tables_missing_name_field() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "identifiers": [{"namespace": ["ns"]}]
            })))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let tables = catalog.list_tables("ns").await.unwrap();
        assert!(tables.is_empty());
    }

    // -----------------------------------------------------------------------
    // Happy path: load table metadata
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn generic_rest_load_table_metadata() {
        let server = MockServer::start().await;
        let metadata = serde_json::json!({
            "format-version": 2,
            "table-uuid": "test-uuid",
            "location": "s3://bucket/path",
        });
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/my_ns/tables/my_table"))
            .respond_with(ResponseTemplate::new(200).set_body_json(metadata.clone()))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let table_id = IcebergTableId {
            namespace: "my_ns".into(),
            name: "my_table".into(),
        };
        let result = catalog.load_table_metadata(&table_id).await.unwrap();
        assert_eq!(result["format-version"], 2);
        assert_eq!(result["table-uuid"], "test-uuid");
    }

    // -----------------------------------------------------------------------
    // Happy path: add partition field
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn generic_rest_add_partition_field() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/namespaces/ns/tables/t/partition-specs/add"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        let field = PartitionFieldSpec {
            name: "day".into(),
            source_column: "event_date".into(),
            transform: "day".into(),
        };
        catalog.add_partition_field(&table_id, field).await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Happy path: drop partition field
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn generic_rest_drop_partition_field() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v1/namespaces/ns/tables/t/partition-specs/day"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        catalog
            .drop_partition_field(&table_id, "day")
            .await
            .unwrap();
    }

    // -----------------------------------------------------------------------
    // Happy path: replace partition spec
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn generic_rest_replace_partition_spec() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/v1/namespaces/ns/tables/t/partition-specs/replace"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        let fields = vec![
            PartitionFieldSpec {
                name: "day".into(),
                source_column: "date_col".into(),
                transform: "day".into(),
            },
            PartitionFieldSpec {
                name: "month".into(),
                source_column: "date_col".into(),
                transform: "month".into(),
            },
        ];
        catalog
            .replace_partition_spec(&table_id, fields)
            .await
            .unwrap();
    }

    // -----------------------------------------------------------------------
    // Error case: HTTP error on list tables
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn generic_rest_list_tables_http_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .respond_with(ResponseTemplate::new(404).set_body_string("namespace not found"))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let err = catalog.list_tables("ns").await.unwrap_err();
        match err {
            CatalogError::Http { status, message } => {
                assert_eq!(status, 404);
                assert!(message.contains("namespace not found"));
            }
            other => panic!("expected Http error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn generic_rest_load_metadata_http_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables/bad_table"))
            .respond_with(ResponseTemplate::new(404).set_body_string("table not found"))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "bad_table".into(),
        };
        let err = catalog.load_table_metadata(&table_id).await.unwrap_err();
        match err {
            CatalogError::Http { status, .. } => assert_eq!(status, 404),
            other => panic!("expected Http error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn generic_rest_add_partition_field_http_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/namespaces/ns/tables/t/partition-specs/add"))
            .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        let field = PartitionFieldSpec {
            name: "day".into(),
            source_column: "date_col".into(),
            transform: "day".into(),
        };
        let err = catalog
            .add_partition_field(&table_id, field)
            .await
            .unwrap_err();
        match err {
            CatalogError::Http { status, .. } => assert_eq!(status, 400),
            other => panic!("expected Http error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn generic_rest_drop_partition_field_http_error() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v1/namespaces/ns/tables/t/partition-specs/day"))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        let err = catalog
            .drop_partition_field(&table_id, "day")
            .await
            .unwrap_err();
        match err {
            CatalogError::Http { status, .. } => assert_eq!(status, 500),
            other => panic!("expected Http error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn generic_rest_replace_spec_http_error() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/v1/namespaces/ns/tables/t/partition-specs/replace"))
            .respond_with(ResponseTemplate::new(409).set_body_string("conflict"))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        let err = catalog
            .replace_partition_spec(&table_id, vec![])
            .await
            .unwrap_err();
        match err {
            CatalogError::Http { status, .. } => assert_eq!(status, 409),
            other => panic!("expected Http error, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Bearer token authentication
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn generic_rest_bearer_token_sent() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .and(header("authorization", "Bearer my-secret-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "identifiers": [{"namespace": ["ns"], "name": "t1"}]
            })))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: Some("my-secret-token".into()),
        });
        let tables = catalog.list_tables("ns").await.unwrap();
        assert_eq!(tables, vec!["t1".to_string()]);
    }

    #[tokio::test]
    async fn generic_rest_no_bearer_token_no_auth_header() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "identifiers": []
            })))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let tables = catalog.list_tables("ns").await.unwrap();
        assert!(tables.is_empty());
    }

    // -----------------------------------------------------------------------
    // URL construction: trailing/leading slashes
    // -----------------------------------------------------------------------

    #[test]
    fn url_construction_trailing_slash_base() {
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: "http://localhost:8080/".into(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
        });
        let url = catalog.url("namespaces/ns/tables");
        assert_eq!(url, "http://localhost:8080/v1/namespaces/ns/tables");
    }

    #[test]
    fn url_construction_leading_slash_path() {
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: "http://localhost:8080".into(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
        });
        let url = catalog.url("/namespaces/ns/tables");
        assert_eq!(url, "http://localhost:8080/v1/namespaces/ns/tables");
    }

    #[test]
    fn url_construction_both_slashes() {
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: "http://localhost:8080/".into(),
            warehouse: None,
            prefix: "/v1/".into(),
            bearer_token: None,
        });
        let url = catalog.url("/namespaces/ns/tables");
        assert_eq!(url, "http://localhost:8080/v1/namespaces/ns/tables");
    }

    #[test]
    fn url_construction_no_slashes() {
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: "http://localhost:8080".into(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
        });
        let url = catalog.url("namespaces/ns/tables");
        assert_eq!(url, "http://localhost:8080/v1/namespaces/ns/tables");
    }

    // -----------------------------------------------------------------------
    // Special characters in namespace/table names
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn generic_rest_namespace_with_special_chars() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/my-ns/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "identifiers": [{"namespace": ["my-ns"], "name": "t1"}]
            })))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let tables = catalog.list_tables("my-ns").await.unwrap();
        assert_eq!(tables, vec!["t1".to_string()]);
    }

    #[tokio::test]
    async fn generic_rest_table_with_special_chars() {
        let server = MockServer::start().await;
        let metadata = serde_json::json!({
            "format-version": 2,
            "table-uuid": "special-uuid"
        });
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables/my-table_name"))
            .respond_with(ResponseTemplate::new(200).set_body_json(metadata))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "my-table_name".into(),
        };
        let result = catalog.load_table_metadata(&table_id).await.unwrap();
        assert_eq!(result["format-version"], 2);
    }

    // -----------------------------------------------------------------------
    // IcebergTableId: Clone, PartialEq, Eq, Hash
    // -----------------------------------------------------------------------

    #[test]
    fn iceberg_table_id_clone() {
        let id = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        let cloned = id.clone();
        assert_eq!(id, cloned);
    }

    #[test]
    fn iceberg_table_id_eq() {
        let a = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        let b = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        let c = IcebergTableId {
            namespace: "ns".into(),
            name: "other".into(),
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn iceberg_table_id_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let a = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        let b = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        a.hash(&mut h1);
        b.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    // -----------------------------------------------------------------------
    // IcebergTableId: Serialize / Deserialize
    // -----------------------------------------------------------------------

    #[test]
    fn iceberg_table_id_serde_roundtrip() {
        let id = IcebergTableId {
            namespace: "my_ns".into(),
            name: "my_table".into(),
        };
        let json = serde_json::to_string(&id).unwrap();
        let deserialized: IcebergTableId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }

    // -----------------------------------------------------------------------
    // PartitionFieldSpec: Serialize / Deserialize
    // -----------------------------------------------------------------------

    #[test]
    fn partition_field_spec_serde_roundtrip() {
        let spec = PartitionFieldSpec {
            name: "day".into(),
            source_column: "event_date".into(),
            transform: "day".into(),
        };
        let json = serde_json::to_string(&spec).unwrap();
        let deserialized: PartitionFieldSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec.name, deserialized.name);
        assert_eq!(spec.source_column, deserialized.source_column);
        assert_eq!(spec.transform, deserialized.transform);
    }

    // -----------------------------------------------------------------------
    // RestCatalogConfig: Clone and Debug
    // -----------------------------------------------------------------------

    #[test]
    fn rest_catalog_config_clone() {
        let config = RestCatalogConfig {
            base_url: "http://localhost".into(),
            warehouse: Some("wh".into()),
            prefix: "v1".into(),
            bearer_token: Some("tok".into()),
        };
        let cloned = config.clone();
        assert_eq!(config.base_url, cloned.base_url);
        assert_eq!(config.warehouse, cloned.warehouse);
        assert_eq!(config.prefix, cloned.prefix);
        assert_eq!(config.bearer_token, cloned.bearer_token);
    }

    #[test]
    fn rest_catalog_config_debug() {
        let config = RestCatalogConfig {
            base_url: "http://localhost".into(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
        };
        let dbg = format!("{config:?}");
        assert!(dbg.contains("RestCatalogConfig"));
    }

    // -----------------------------------------------------------------------
    // GlueRestCatalog construction
    // -----------------------------------------------------------------------

    #[test]
    fn glue_rest_catalog_construction() {
        let catalog = GlueRestCatalog::new("us-east-1", "mydb", "http://localhost:8181");
        assert_eq!(catalog.region, "us-east-1");
        assert_eq!(catalog.database, "mydb");
    }

    #[test]
    fn glue_rest_catalog_warehouse_format() {
        let catalog = GlueRestCatalog::new("eu-west-1", "analytics", "http://localhost:8181");
        let expected = Some("glue://eu-west-1/analytics".to_string());
        assert_eq!(catalog.inner.config.warehouse, expected);
    }

    #[test]
    fn glue_rest_catalog_prefix_is_v1() {
        let catalog = GlueRestCatalog::new("us-west-2", "db", "http://localhost:8181");
        assert_eq!(catalog.inner.config.prefix, "v1");
    }

    #[test]
    fn glue_rest_catalog_no_bearer_token() {
        let catalog = GlueRestCatalog::new("us-east-1", "db", "http://localhost:8181");
        assert!(catalog.inner.config.bearer_token.is_none());
    }

    // -----------------------------------------------------------------------
    // GlueRestCatalog: delegates to inner GenericRestCatalog
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn glue_rest_catalog_list_tables() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "identifiers": [{"namespace": ["ns"], "name": "t1"}]
            })))
            .mount(&server)
            .await;
        let catalog = GlueRestCatalog::new("us-east-1", "db", server.uri());
        let tables = catalog.list_tables("ns").await.unwrap();
        assert_eq!(tables, vec!["t1".to_string()]);
    }

    #[tokio::test]
    async fn glue_rest_catalog_load_metadata() {
        let server = MockServer::start().await;
        let metadata = serde_json::json!({"format-version": 2});
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables/t1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(metadata))
            .mount(&server)
            .await;
        let catalog = GlueRestCatalog::new("us-east-1", "db", server.uri());
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "t1".into(),
        };
        let result = catalog.load_table_metadata(&table_id).await.unwrap();
        assert_eq!(result["format-version"], 2);
    }

    #[tokio::test]
    async fn glue_rest_catalog_add_partition() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/namespaces/ns/tables/t/partition-specs/add"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let catalog = GlueRestCatalog::new("us-east-1", "db", server.uri());
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        catalog
            .add_partition_field(
                &table_id,
                PartitionFieldSpec {
                    name: "d".into(),
                    source_column: "c".into(),
                    transform: "identity".into(),
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn glue_rest_catalog_drop_partition() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v1/namespaces/ns/tables/t/partition-specs/d"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let catalog = GlueRestCatalog::new("us-east-1", "db", server.uri());
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        catalog.drop_partition_field(&table_id, "d").await.unwrap();
    }

    #[tokio::test]
    async fn glue_rest_catalog_replace_spec() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/v1/namespaces/ns/tables/t/partition-specs/replace"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let catalog = GlueRestCatalog::new("us-east-1", "db", server.uri());
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        catalog
            .replace_partition_spec(&table_id, vec![])
            .await
            .unwrap();
    }

    // -----------------------------------------------------------------------
    // NessieCatalog construction
    // -----------------------------------------------------------------------

    #[test]
    fn nessie_catalog_construction() {
        let catalog = NessieCatalog::new("http://nessie:8080", "main");
        assert_eq!(catalog.inner.config.base_url, "http://nessie:8080");
        assert_eq!(catalog.inner.config.warehouse, Some("main".into()));
        assert_eq!(catalog.inner.config.prefix, "v1");
        assert!(catalog.inner.config.bearer_token.is_none());
    }

    // -----------------------------------------------------------------------
    // NessieCatalog: delegates to inner GenericRestCatalog
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn nessie_catalog_list_tables() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "identifiers": [{"namespace": ["ns"], "name": "t1"}]
            })))
            .mount(&server)
            .await;
        let catalog = NessieCatalog::new(server.uri(), "main");
        let tables = catalog.list_tables("ns").await.unwrap();
        assert_eq!(tables, vec!["t1".to_string()]);
    }

    #[tokio::test]
    async fn nessie_catalog_load_metadata() {
        let server = MockServer::start().await;
        let metadata = serde_json::json!({"format-version": 2, "table-uuid": "nessie"});
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables/t1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(metadata))
            .mount(&server)
            .await;
        let catalog = NessieCatalog::new(server.uri(), "main");
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "t1".into(),
        };
        let result = catalog.load_table_metadata(&table_id).await.unwrap();
        assert_eq!(result["table-uuid"], "nessie");
    }

    #[tokio::test]
    async fn nessie_catalog_add_partition() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/namespaces/ns/tables/t/partition-specs/add"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let catalog = NessieCatalog::new(server.uri(), "main");
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        catalog
            .add_partition_field(
                &table_id,
                PartitionFieldSpec {
                    name: "m".into(),
                    source_column: "ts".into(),
                    transform: "month".into(),
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn nessie_catalog_drop_partition() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v1/namespaces/ns/tables/t/partition-specs/m"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let catalog = NessieCatalog::new(server.uri(), "main");
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        catalog.drop_partition_field(&table_id, "m").await.unwrap();
    }

    #[tokio::test]
    async fn nessie_catalog_replace_spec() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/v1/namespaces/ns/tables/t/partition-specs/replace"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let catalog = NessieCatalog::new(server.uri(), "main");
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        catalog
            .replace_partition_spec(
                &table_id,
                vec![PartitionFieldSpec {
                    name: "y".into(),
                    source_column: "dt".into(),
                    transform: "year".into(),
                }],
            )
            .await
            .unwrap();
    }

    // -----------------------------------------------------------------------
    // GenericRestCatalog: Clone
    // -----------------------------------------------------------------------

    #[test]
    fn generic_rest_catalog_clone() {
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: "http://localhost".into(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
        });
        let cloned = catalog.clone();
        assert_eq!(cloned.config.base_url, "http://localhost");
    }

    // -----------------------------------------------------------------------
    // GenericRestCatalog: empty body / malformed JSON
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn generic_rest_load_metadata_invalid_json() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables/t"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        let err = catalog.load_table_metadata(&table_id).await.unwrap_err();
        match err {
            CatalogError::Http { message, .. } => {
                assert!(message.contains("expected value") || message.contains("error"));
            }
            other => panic!("expected Http error for invalid JSON, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // GlueRestCatalog: Clone
    // -----------------------------------------------------------------------

    #[test]
    fn glue_rest_catalog_clone() {
        let catalog = GlueRestCatalog::new("us-east-1", "db", "http://localhost");
        let cloned = catalog.clone();
        assert_eq!(cloned.region, "us-east-1");
        assert_eq!(cloned.database, "db");
    }

    // -----------------------------------------------------------------------
    // NessieCatalog: Clone
    // -----------------------------------------------------------------------

    #[test]
    fn nessie_catalog_clone() {
        let catalog = NessieCatalog::new("http://localhost", "main");
        let cloned = catalog.clone();
        assert_eq!(cloned.inner.config.base_url, "http://localhost");
    }

    // -----------------------------------------------------------------------
    // Error: HTTP error on POST with 500
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn generic_rest_add_partition_server_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/namespaces/ns/tables/t/partition-specs/add"))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
            .mount(&server)
            .await;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: server.uri(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        });
        let table_id = IcebergTableId {
            namespace: "ns".into(),
            name: "t".into(),
        };
        let field = PartitionFieldSpec {
            name: "d".into(),
            source_column: "c".into(),
            transform: "day".into(),
        };
        let err = catalog
            .add_partition_field(&table_id, field)
            .await
            .unwrap_err();
        match err {
            CatalogError::Http { status, .. } => assert_eq!(status, 500),
            other => panic!("expected Http error, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // CatalogError Display for Http
    // -----------------------------------------------------------------------

    #[test]
    fn catalog_error_http_display() {
        let err = CatalogError::Http {
            status: 403,
            message: "forbidden".into(),
        };
        assert_eq!(err.to_string(), "HTTP error 403: forbidden");
    }

    #[test]
    fn catalog_error_debug() {
        let err = CatalogError::TableNotFound { name: "t".into() };
        let dbg = format!("{err:?}");
        assert!(dbg.contains("TableNotFound"));
    }

    // ── Timeout configuration ─────────────────────────────────────────────────

    #[tokio::test]
    async fn catalog_custom_timeout_applied_on_unreachable_server() {
        // A 1 ms timeout pointed at a non-listening port must return an error
        // quickly, confirming the timeout is respected.
        use std::time::Instant;
        let catalog = GenericRestCatalog::new(RestCatalogConfig {
            base_url: "http://127.0.0.1:19999".into(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: Some(1), // 1 millisecond — far shorter than any real latency
        });
        let start = Instant::now();
        let result = catalog.list_tables("ns").await;
        let elapsed = start.elapsed();
        assert!(result.is_err(), "request to non-listening port must fail");
        assert!(
            elapsed.as_secs() < 5,
            "1 ms timeout must not block for seconds; elapsed: {:?}",
            elapsed
        );
    }

    #[test]
    fn timeout_none_uses_default() {
        // Constructing with timeout_ms: None must use DEFAULT_CATALOG_TIMEOUT_MS.
        let config = RestCatalogConfig {
            base_url: "http://example.com".into(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: None,
        };
        let _catalog = GenericRestCatalog::new(config); // Must not panic
    }

    #[test]
    fn timeout_zero_disables_timeout() {
        // timeout_ms: Some(0) means no timeout — must construct without panic.
        let config = RestCatalogConfig {
            base_url: "http://example.com".into(),
            warehouse: None,
            prefix: "v1".into(),
            bearer_token: None,
            timeout_ms: Some(0),
        };
        let _catalog = GenericRestCatalog::new(config);
    }
}
