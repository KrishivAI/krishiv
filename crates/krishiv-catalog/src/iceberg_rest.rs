//! Iceberg REST Catalog client (R18 S3.1).

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::{CatalogError, CatalogResult};

/// REST catalog configuration.
#[derive(Debug, Clone)]
pub struct RestCatalogConfig {
    pub base_url: String,
    pub warehouse: Option<String>,
    pub prefix: String,
    pub bearer_token: Option<String>,
}

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
        Self {
            config,
            client: Client::new(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!(
            "{}/{}/{}",
            self.config.base_url.trim_end_matches('/'),
            self.config.prefix.trim_matches('/'),
            path.trim_start_matches('/')
        )
    }

    async fn get_json(&self, url: String) -> CatalogResult<serde_json::Value> {
        let mut req = self.client.get(url);
        if let Some(token) = &self.config.bearer_token {
            req = req.bearer_auth(token);
        }
        let resp = req.send().await.map_err(|e| CatalogError::InvalidSchema {
            message: e.to_string(),
        })?;
        if !resp.status().is_success() {
            return Err(CatalogError::InvalidSchema {
                message: resp.text().await.unwrap_or_default(),
            });
        }
        resp.json().await.map_err(|e| CatalogError::InvalidSchema {
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
        let resp = req.send().await.map_err(|e| CatalogError::InvalidSchema {
            message: e.to_string(),
        })?;
        if !resp.status().is_success() {
            return Err(CatalogError::InvalidSchema {
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
        let resp = req.send().await.map_err(|e| CatalogError::InvalidSchema {
            message: e.to_string(),
        })?;
        if !resp.status().is_success() {
            return Err(CatalogError::InvalidSchema {
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
        let resp = req.send().await.map_err(|e| CatalogError::InvalidSchema {
            message: e.to_string(),
        })?;
        if !resp.status().is_success() {
            return Err(CatalogError::InvalidSchema {
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
        let region = region.into();
        let database = database.into();
        Self {
            inner: GenericRestCatalog::new(RestCatalogConfig {
                base_url: rest_url.into(),
                warehouse: Some(format!("glue://{region}/{database}")),
                prefix: "v1".into(),
                bearer_token: None,
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
        let uri = uri.into();
        let reference = reference.into();
        Self {
            inner: GenericRestCatalog::new(RestCatalogConfig {
                base_url: uri,
                warehouse: Some(reference),
                prefix: "v1".into(),
                bearer_token: None,
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
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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
        });
        let tables = catalog.list_tables("ns").await.unwrap();
        assert_eq!(tables, vec!["t1".to_string()]);
    }
}
