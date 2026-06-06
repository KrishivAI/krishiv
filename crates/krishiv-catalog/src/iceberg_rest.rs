//! Production-oriented client for the Apache Iceberg REST Catalog API.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::header::{ACCEPT, USER_AGENT};
use reqwest::{Client, Method, RequestBuilder, Response, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::sync::OnceCell;
use url::Url;
use uuid::Uuid;

use crate::{CatalogError, CatalogResult};

const API_VERSION_SEGMENT: &str = "v1";
const DEFAULT_CATALOG_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_PAGE_SIZE: u32 = 1_000;
const MAX_PAGE_SIZE: u32 = 10_000;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const ERROR_BODY_LIMIT_BYTES: usize = 64 * 1024;
const MAX_LIST_PAGES: usize = 10_000;
const MAX_LISTED_TABLES: usize = 1_000_000;
const MAX_IDENTIFIER_BYTES: usize = 1_024;
const DEFAULT_NAMESPACE_SEPARATOR: &str = "%1F";
const USER_AGENT_VALUE: &str = concat!("krishiv-catalog/", env!("CARGO_PKG_VERSION"));

/// REST catalog configuration.
///
/// Construction validates the endpoint and all resource ceilings. Credentials
/// are deliberately omitted from `Debug`.
#[derive(Clone)]
pub struct RestCatalogConfig {
    base_url: Url,
    warehouse: Option<String>,
    catalog_prefix: Option<String>,
    bearer_token: Option<String>,
    timeout: Duration,
    page_size: u32,
    max_response_bytes: usize,
}

impl RestCatalogConfig {
    /// Create a validated configuration for an Iceberg REST catalog.
    pub fn new(base_url: impl AsRef<str>) -> CatalogResult<Self> {
        let base_url =
            Url::parse(base_url.as_ref()).map_err(|error| CatalogError::InvalidConfiguration {
                message: format!("invalid catalog URL: {error}"),
            })?;
        validate_base_url(&base_url)?;

        Ok(Self {
            base_url,
            warehouse: None,
            catalog_prefix: None,
            bearer_token: None,
            timeout: DEFAULT_CATALOG_TIMEOUT,
            page_size: DEFAULT_PAGE_SIZE,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
        })
    }

    /// Select the warehouse passed to the mandatory `/v1/config` request.
    pub fn with_warehouse(mut self, warehouse: impl Into<String>) -> CatalogResult<Self> {
        let warehouse =
            validate_non_blank("warehouse", warehouse.into(), MAX_IDENTIFIER_BYTES * 4)?;
        self.warehouse = Some(warehouse);
        Ok(self)
    }

    /// Supply a client-side catalog prefix.
    ///
    /// Server defaults are applied first and server overrides are applied last,
    /// as required by the Iceberg REST configuration contract.
    pub fn with_catalog_prefix(mut self, prefix: impl Into<String>) -> CatalogResult<Self> {
        let prefix = prefix.into();
        split_catalog_prefix(&prefix)?;
        self.catalog_prefix = Some(prefix);
        Ok(self)
    }

    /// Attach a bearer token to configuration and catalog requests.
    pub fn with_bearer_token(mut self, token: impl Into<String>) -> CatalogResult<Self> {
        self.bearer_token = Some(validate_non_blank(
            "bearer token",
            token.into(),
            MAX_IDENTIFIER_BYTES * 16,
        )?);
        Ok(self)
    }

    /// Set the end-to-end request timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> CatalogResult<Self> {
        if timeout.is_zero() {
            return Err(CatalogError::InvalidConfiguration {
                message: "catalog request timeout must be positive".to_string(),
            });
        }
        self.timeout = timeout;
        Ok(self)
    }

    /// Set the requested list page size.
    pub fn with_page_size(mut self, page_size: u32) -> CatalogResult<Self> {
        if !(1..=MAX_PAGE_SIZE).contains(&page_size) {
            return Err(CatalogError::InvalidConfiguration {
                message: format!(
                    "catalog page size must be between 1 and {MAX_PAGE_SIZE}, got {page_size}"
                ),
            });
        }
        self.page_size = page_size;
        Ok(self)
    }

    /// Set the maximum successful response body retained in memory.
    pub fn with_max_response_bytes(mut self, limit: usize) -> CatalogResult<Self> {
        if limit == 0 {
            return Err(CatalogError::InvalidConfiguration {
                message: "catalog response limit must be positive".to_string(),
            });
        }
        self.max_response_bytes = limit;
        Ok(self)
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    pub fn warehouse(&self) -> Option<&str> {
        self.warehouse.as_deref()
    }

    pub fn catalog_prefix(&self) -> Option<&str> {
        self.catalog_prefix.as_deref()
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    pub fn max_response_bytes(&self) -> usize {
        self.max_response_bytes
    }
}

impl fmt::Debug for RestCatalogConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestCatalogConfig")
            .field("base_url", &self.base_url)
            .field("has_warehouse", &self.warehouse.is_some())
            .field("catalog_prefix", &self.catalog_prefix)
            .field("has_bearer_token", &self.bearer_token.is_some())
            .field("timeout", &self.timeout)
            .field("page_size", &self.page_size)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish()
    }
}

/// Validated Iceberg table identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IcebergTableId {
    namespace: String,
    name: String,
}

impl IcebergTableId {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> CatalogResult<Self> {
        Ok(Self {
            namespace: validate_non_blank(
                "table namespace",
                namespace.into(),
                MAX_IDENTIFIER_BYTES,
            )?,
            name: validate_non_blank("table name", name.into(), MAX_IDENTIFIER_BYTES)?,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn display_name(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }
}

/// Validated load-table response.
#[derive(Clone, PartialEq)]
pub struct LoadedIcebergTable {
    metadata_location: String,
    metadata: serde_json::Value,
    config: BTreeMap<String, String>,
}

impl LoadedIcebergTable {
    pub fn metadata_location(&self) -> &str {
        &self.metadata_location
    }

    pub fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }

    pub fn into_metadata(self) -> serde_json::Value {
        self.metadata
    }

    /// Per-table configuration returned by the catalog.
    ///
    /// This map may contain credentials and must not be logged.
    pub fn config(&self) -> &BTreeMap<String, String> {
        &self.config
    }
}

impl fmt::Debug for LoadedIcebergTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let format_version = self
            .metadata
            .get("format-version")
            .and_then(serde_json::Value::as_u64);
        let table_uuid = self
            .metadata
            .get("table-uuid")
            .and_then(serde_json::Value::as_str);
        formatter
            .debug_struct("LoadedIcebergTable")
            .field("has_metadata_location", &true)
            .field("format_version", &format_version)
            .field("table_uuid", &table_uuid)
            .field("config_keys", &self.config.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Read-only operations implemented by the Iceberg REST catalog client.
///
/// Table mutations are intentionally absent until Krishiv has a typed commit
/// request model with Iceberg requirements and updates.
#[async_trait]
pub trait IcebergCatalogClient: Send + Sync {
    async fn list_tables(&self, namespace: &str) -> CatalogResult<Vec<String>>;

    async fn load_table(&self, table: &IcebergTableId) -> CatalogResult<LoadedIcebergTable>;

    async fn load_table_metadata(
        &self,
        table: &IcebergTableId,
    ) -> CatalogResult<serde_json::Value> {
        Ok(self.load_table(table).await?.into_metadata())
    }
}

#[derive(Debug)]
struct ResolvedCatalogConfig {
    prefix_segments: Vec<String>,
    namespace_separator: String,
    endpoints: Option<HashSet<String>>,
}

/// Generic Apache Iceberg REST catalog.
#[derive(Clone)]
pub struct GenericRestCatalog {
    config: RestCatalogConfig,
    client: Client,
    resolved: Arc<OnceCell<ResolvedCatalogConfig>>,
}

impl GenericRestCatalog {
    /// Build a catalog with Krishiv's bounded default HTTP client.
    pub fn new(config: RestCatalogConfig) -> CatalogResult<Self> {
        let connect_timeout = config.timeout.min(DEFAULT_CONNECT_TIMEOUT);
        let client = Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(config.timeout)
            .user_agent(USER_AGENT_VALUE)
            .build()
            .map_err(|error| CatalogError::InvalidConfiguration {
                message: format!("failed to build catalog HTTP client: {error}"),
            })?;
        Self::with_http_client(config, client)
    }

    /// Build a catalog with a caller-configured HTTP client.
    ///
    /// This supports custom trust roots, proxies, and authentication headers.
    /// The validated per-request timeout and response ceiling still apply.
    pub fn with_http_client(config: RestCatalogConfig, client: Client) -> CatalogResult<Self> {
        validate_base_url(&config.base_url)?;
        Ok(Self {
            config,
            client,
            resolved: Arc::new(OnceCell::new()),
        })
    }

    async fn resolved_config(&self) -> CatalogResult<&ResolvedCatalogConfig> {
        self.resolved
            .get_or_try_init(|| async { self.fetch_catalog_config().await })
            .await
    }

    async fn fetch_catalog_config(&self) -> CatalogResult<ResolvedCatalogConfig> {
        let mut url = append_url_segments(&self.config.base_url, [API_VERSION_SEGMENT, "config"])?;
        if let Some(warehouse) = self.config.warehouse() {
            url.query_pairs_mut().append_pair("warehouse", warehouse);
        }

        let response: CatalogConfigResponse = self
            .execute_json(
                self.request(Method::GET, url),
                "fetch catalog configuration",
                NotFoundKind::None,
            )
            .await?;

        let prefix = response
            .overrides
            .get("prefix")
            .map(String::as_str)
            .or(self.config.catalog_prefix())
            .or_else(|| response.defaults.get("prefix").map(String::as_str))
            .unwrap_or_default();
        let prefix_segments = split_catalog_prefix(prefix)?;
        let namespace_separator = response
            .overrides
            .get("namespace-separator")
            .or_else(|| response.defaults.get("namespace-separator"))
            .map(String::as_str)
            .unwrap_or(DEFAULT_NAMESPACE_SEPARATOR);
        let namespace_separator = decode_namespace_separator(namespace_separator)?;

        let endpoints = response
            .endpoints
            .map(validate_advertised_endpoints)
            .transpose()?;

        Ok(ResolvedCatalogConfig {
            prefix_segments,
            namespace_separator,
            endpoints,
        })
    }

    fn request(&self, method: Method, url: Url) -> RequestBuilder {
        let mut request = self
            .client
            .request(method, url)
            .timeout(self.config.timeout)
            .header(ACCEPT, "application/json")
            .header(USER_AGENT, USER_AGENT_VALUE);
        if let Some(token) = &self.config.bearer_token {
            request = request.bearer_auth(token);
        }
        request
    }

    fn catalog_url(&self, resolved: &ResolvedCatalogConfig, suffix: &[&str]) -> CatalogResult<Url> {
        let mut segments = Vec::with_capacity(1 + resolved.prefix_segments.len() + suffix.len());
        segments.push(API_VERSION_SEGMENT);
        segments.extend(resolved.prefix_segments.iter().map(String::as_str));
        segments.extend_from_slice(suffix);
        append_url_segments(&self.config.base_url, segments)
    }

    async fn execute_json<T>(
        &self,
        request: RequestBuilder,
        operation: &'static str,
        not_found: NotFoundKind,
    ) -> CatalogResult<T>
    where
        T: DeserializeOwned,
    {
        let response = request
            .send()
            .await
            .map_err(|error| CatalogError::Transport {
                operation: operation.to_string(),
                message: error.to_string(),
            })?;
        let status = response.status();

        if !status.is_success() {
            let body = read_error_body(response, operation).await;
            return match (status, not_found) {
                (StatusCode::NOT_FOUND, NotFoundKind::Namespace(name)) => {
                    Err(CatalogError::SchemaNotFound { name })
                }
                (StatusCode::NOT_FOUND, NotFoundKind::Table(name)) => {
                    Err(CatalogError::TableNotFound { name })
                }
                _ => Err(CatalogError::Http {
                    status: status.as_u16(),
                    message: describe_error_body(&body),
                }),
            };
        }

        let body = read_bounded_body(response, self.config.max_response_bytes, operation).await?;
        serde_json::from_slice(&body).map_err(|error| CatalogError::InvalidResponse {
            operation: operation.to_string(),
            message: format!(
                "response is not valid JSON at line {}, column {}: {error}",
                error.line(),
                error.column()
            ),
        })
    }

    fn require_endpoint(
        resolved: &ResolvedCatalogConfig,
        endpoint: RequiredEndpoint,
    ) -> CatalogResult<()> {
        let Some(endpoints) = &resolved.endpoints else {
            return Ok(());
        };
        if endpoint.matches(endpoints, &resolved.prefix_segments) {
            Ok(())
        } else {
            Err(CatalogError::UnsupportedOperation {
                operation: endpoint.operation().to_string(),
            })
        }
    }
}

impl fmt::Debug for GenericRestCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GenericRestCatalog")
            .field("config", &self.config)
            .field("configuration_resolved", &self.resolved.get().is_some())
            .finish()
    }
}

#[async_trait]
impl IcebergCatalogClient for GenericRestCatalog {
    async fn list_tables(&self, namespace: &str) -> CatalogResult<Vec<String>> {
        let namespace = validate_non_blank(
            "table namespace",
            namespace.to_string(),
            MAX_IDENTIFIER_BYTES,
        )?;
        let resolved = self.resolved_config().await?;
        Self::require_endpoint(resolved, RequiredEndpoint::ListTables)?;

        let mut table_names = Vec::new();
        let mut identifiers = HashSet::new();
        let mut seen_page_tokens = HashSet::new();
        let mut page_token = String::new();

        for _ in 0..MAX_LIST_PAGES {
            let mut url =
                self.catalog_url(resolved, &["namespaces", namespace.as_str(), "tables"])?;
            url.query_pairs_mut()
                .append_pair("pageToken", &page_token)
                .append_pair("pageSize", &self.config.page_size.to_string());

            let page: ListTablesResponse = self
                .execute_json(
                    self.request(Method::GET, url),
                    "list catalog tables",
                    NotFoundKind::Namespace(namespace.clone()),
                )
                .await?;

            for identifier in page.identifiers {
                validate_identifier_response(
                    &identifier,
                    &namespace,
                    &resolved.namespace_separator,
                )?;
                let key = (identifier.namespace, identifier.name.clone());
                if !identifiers.insert(key) {
                    return Err(CatalogError::InvalidResponse {
                        operation: "list catalog tables".to_string(),
                        message: format!(
                            "server returned duplicate table identifier '{}'",
                            identifier.name
                        ),
                    });
                }
                if table_names.len() == MAX_LISTED_TABLES {
                    return Err(CatalogError::InvalidResponse {
                        operation: "list catalog tables".to_string(),
                        message: format!(
                            "listing exceeds the maximum of {MAX_LISTED_TABLES} tables"
                        ),
                    });
                }
                table_names.push(identifier.name);
            }

            let Some(next_page_token) = page.next_page_token else {
                return Ok(table_names);
            };
            if next_page_token.is_empty() {
                return Err(CatalogError::InvalidResponse {
                    operation: "list catalog tables".to_string(),
                    message: "server returned an empty next-page-token".to_string(),
                });
            }
            if !seen_page_tokens.insert(next_page_token.clone()) {
                return Err(CatalogError::InvalidResponse {
                    operation: "list catalog tables".to_string(),
                    message: "server repeated a pagination token".to_string(),
                });
            }
            page_token = next_page_token;
        }

        Err(CatalogError::InvalidResponse {
            operation: "list catalog tables".to_string(),
            message: format!("listing exceeded the maximum of {MAX_LIST_PAGES} pages"),
        })
    }

    async fn load_table(&self, table: &IcebergTableId) -> CatalogResult<LoadedIcebergTable> {
        let resolved = self.resolved_config().await?;
        Self::require_endpoint(resolved, RequiredEndpoint::LoadTable)?;
        let url = self.catalog_url(
            resolved,
            &["namespaces", table.namespace(), "tables", table.name()],
        )?;
        let response: LoadTableResponse = self
            .execute_json(
                self.request(Method::GET, url),
                "load catalog table",
                NotFoundKind::Table(table.display_name()),
            )
            .await?;
        validate_loaded_table(response)
    }
}

#[derive(Debug, Deserialize)]
struct CatalogConfigResponse {
    defaults: BTreeMap<String, String>,
    overrides: BTreeMap<String, String>,
    #[serde(default)]
    endpoints: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct ListTablesResponse {
    identifiers: Vec<TableIdentifierResponse>,
    #[serde(rename = "next-page-token", default)]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TableIdentifierResponse {
    namespace: Vec<String>,
    name: String,
}

#[derive(Debug, Deserialize)]
struct LoadTableResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    metadata: serde_json::Value,
    #[serde(default)]
    config: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy)]
enum RequiredEndpoint {
    ListTables,
    LoadTable,
}

impl RequiredEndpoint {
    fn operation(self) -> &'static str {
        match self {
            Self::ListTables => "listing Iceberg tables",
            Self::LoadTable => "loading an Iceberg table",
        }
    }

    fn template(self) -> &'static str {
        match self {
            Self::ListTables => "GET /v1/{prefix}/namespaces/{namespace}/tables",
            Self::LoadTable => "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        }
    }

    fn matches(self, endpoints: &HashSet<String>, prefix_segments: &[String]) -> bool {
        if endpoints.contains(self.template()) {
            return true;
        }

        let prefix = prefix_segments.join("/");
        let concrete = match self {
            Self::ListTables if prefix.is_empty() => {
                "GET /v1/namespaces/{namespace}/tables".to_string()
            }
            Self::LoadTable if prefix.is_empty() => {
                "GET /v1/namespaces/{namespace}/tables/{table}".to_string()
            }
            Self::ListTables => {
                format!("GET /v1/{prefix}/namespaces/{{namespace}}/tables")
            }
            Self::LoadTable => {
                format!("GET /v1/{prefix}/namespaces/{{namespace}}/tables/{{table}}")
            }
        };
        endpoints.contains(&concrete)
    }
}

enum NotFoundKind {
    None,
    Namespace(String),
    Table(String),
}

fn validate_base_url(url: &Url) -> CatalogResult<()> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(CatalogError::InvalidConfiguration {
            message: format!(
                "catalog URL scheme must be http or https, got '{}'",
                url.scheme()
            ),
        });
    }
    if url.host_str().is_none() {
        return Err(CatalogError::InvalidConfiguration {
            message: "catalog URL must include a host".to_string(),
        });
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(CatalogError::InvalidConfiguration {
            message: "catalog URL must not contain embedded credentials".to_string(),
        });
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(CatalogError::InvalidConfiguration {
            message: "catalog URL must not contain a query string or fragment".to_string(),
        });
    }
    if url.cannot_be_a_base() {
        return Err(CatalogError::InvalidConfiguration {
            message: "catalog URL cannot be used as a hierarchical base URL".to_string(),
        });
    }
    Ok(())
}

fn validate_non_blank(label: &str, value: String, max_bytes: usize) -> CatalogResult<String> {
    if value.trim().is_empty() {
        return Err(CatalogError::InvalidConfiguration {
            message: format!("{label} must not be blank"),
        });
    }
    if value.trim() != value {
        return Err(CatalogError::InvalidConfiguration {
            message: format!("{label} must not have leading or trailing whitespace"),
        });
    }
    if value.contains('\0') {
        return Err(CatalogError::InvalidConfiguration {
            message: format!("{label} must not contain NUL"),
        });
    }
    if value.len() > max_bytes {
        return Err(CatalogError::InvalidConfiguration {
            message: format!("{label} exceeds the maximum of {max_bytes} bytes"),
        });
    }
    Ok(value)
}

fn split_catalog_prefix(prefix: &str) -> CatalogResult<Vec<String>> {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        return Ok(Vec::new());
    }

    prefix
        .split('/')
        .map(|segment| {
            if segment.is_empty() || matches!(segment, "." | "..") {
                return Err(CatalogError::InvalidConfiguration {
                    message: format!("catalog prefix contains invalid segment '{segment}'"),
                });
            }
            validate_non_blank(
                "catalog prefix segment",
                segment.to_string(),
                MAX_IDENTIFIER_BYTES,
            )
        })
        .collect()
}

fn validate_advertised_endpoints(endpoints: Vec<String>) -> CatalogResult<HashSet<String>> {
    let mut validated = HashSet::with_capacity(endpoints.len());
    for endpoint in endpoints {
        let endpoint = validate_non_blank("advertised endpoint", endpoint, 4_096)?;
        if !validated.insert(endpoint.clone()) {
            return Err(CatalogError::InvalidResponse {
                operation: "fetch catalog configuration".to_string(),
                message: format!("server advertised endpoint '{endpoint}' more than once"),
            });
        }
    }
    Ok(validated)
}

fn decode_namespace_separator(encoded: &str) -> CatalogResult<String> {
    let query = format!("separator={encoded}");
    let decoded = url::form_urlencoded::parse(query.as_bytes())
        .next()
        .map(|(_, value)| value.into_owned())
        .ok_or_else(|| CatalogError::InvalidResponse {
            operation: "fetch catalog configuration".to_string(),
            message: "namespace-separator could not be decoded".to_string(),
        })?;
    if decoded.is_empty() || decoded.contains('\0') || decoded.len() > 16 {
        return Err(CatalogError::InvalidResponse {
            operation: "fetch catalog configuration".to_string(),
            message: "namespace-separator must decode to between 1 and 16 non-NUL bytes"
                .to_string(),
        });
    }
    Ok(decoded)
}

fn append_url_segments<I, S>(base_url: &Url, segments: I) -> CatalogResult<Url>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut url = base_url.clone();
    let mut path = url
        .path_segments_mut()
        .map_err(|_| CatalogError::InvalidConfiguration {
            message: "catalog URL cannot accept path segments".to_string(),
        })?;
    path.pop_if_empty();
    for segment in segments {
        path.push(segment.as_ref());
    }
    drop(path);
    Ok(url)
}

async fn read_bounded_body(
    mut response: Response,
    limit: usize,
    operation: &str,
) -> CatalogResult<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(CatalogError::ResponseTooLarge {
            operation: operation.to_string(),
            limit_bytes: limit,
        });
    }

    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| CatalogError::Transport {
            operation: operation.to_string(),
            message: format!("failed while reading response body: {error}"),
        })?
    {
        let next_len =
            body.len()
                .checked_add(chunk.len())
                .ok_or_else(|| CatalogError::ResponseTooLarge {
                    operation: operation.to_string(),
                    limit_bytes: limit,
                })?;
        if next_len > limit {
            return Err(CatalogError::ResponseTooLarge {
                operation: operation.to_string(),
                limit_bytes: limit,
            });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn read_error_body(mut response: Response, operation: &str) -> Vec<u8> {
    let mut body = Vec::new();
    while body.len() < ERROR_BODY_LIMIT_BYTES {
        let chunk = match response.chunk().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => return body,
            Err(error) => {
                let fallback = format!("failed to read error response during {operation}: {error}");
                return fallback.into_bytes();
            }
        };
        let remaining = ERROR_BODY_LIMIT_BYTES - body.len();
        if chunk.len() > remaining {
            body.extend_from_slice(&chunk[..remaining]);
            body.extend_from_slice(b" [truncated]");
            return body;
        }
        body.extend_from_slice(&chunk);
    }
    body
}

fn describe_error_body(body: &[u8]) -> String {
    #[derive(Deserialize)]
    struct ErrorEnvelope {
        error: ErrorDetail,
    }
    #[derive(Deserialize)]
    struct ErrorDetail {
        message: String,
        #[serde(rename = "type")]
        error_type: Option<String>,
        code: Option<u16>,
    }

    if let Ok(envelope) = serde_json::from_slice::<ErrorEnvelope>(body) {
        let mut message = envelope.error.message;
        if let Some(error_type) = envelope.error.error_type {
            message = format!("{error_type}: {message}");
        }
        if let Some(code) = envelope.error.code {
            message = format!("{message} (catalog code {code})");
        }
        return message;
    }

    let text = String::from_utf8_lossy(body).trim().to_string();
    if text.is_empty() {
        "catalog returned an empty error response".to_string()
    } else {
        text
    }
}

fn validate_identifier_response(
    identifier: &TableIdentifierResponse,
    requested_namespace: &str,
    namespace_separator: &str,
) -> CatalogResult<()> {
    if identifier.namespace.is_empty() {
        return Err(CatalogError::InvalidResponse {
            operation: "list catalog tables".to_string(),
            message: format!(
                "table identifier '{}' has an empty namespace",
                identifier.name
            ),
        });
    }
    for namespace_part in &identifier.namespace {
        validate_response_string("list catalog tables", "namespace component", namespace_part)?;
    }
    let response_namespace = identifier.namespace.join(namespace_separator);
    if response_namespace != requested_namespace {
        return Err(CatalogError::InvalidResponse {
            operation: "list catalog tables".to_string(),
            message: format!(
                "server returned table '{}' from namespace '{}' while listing '{}'",
                identifier.name, response_namespace, requested_namespace
            ),
        });
    }
    validate_response_string("list catalog tables", "table name", &identifier.name)
}

fn validate_loaded_table(response: LoadTableResponse) -> CatalogResult<LoadedIcebergTable> {
    validate_response_string(
        "load catalog table",
        "metadata-location",
        &response.metadata_location,
    )?;
    validate_absolute_uri(
        "load catalog table",
        "metadata-location",
        &response.metadata_location,
    )?;
    for key in response.config.keys() {
        validate_response_string("load catalog table", "table config key", key)?;
    }

    let metadata = response
        .metadata
        .as_object()
        .ok_or_else(|| CatalogError::InvalidResponse {
            operation: "load catalog table".to_string(),
            message: "metadata must be a JSON object".to_string(),
        })?;
    let format_version = metadata
        .get("format-version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| CatalogError::InvalidResponse {
            operation: "load catalog table".to_string(),
            message: "metadata format-version must be an integer".to_string(),
        })?;
    if !(1..=3).contains(&format_version) {
        return Err(CatalogError::InvalidResponse {
            operation: "load catalog table".to_string(),
            message: format!("unsupported Iceberg format-version {format_version}"),
        });
    }

    let table_uuid = metadata
        .get("table-uuid")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| CatalogError::InvalidResponse {
            operation: "load catalog table".to_string(),
            message: "metadata table-uuid must be a string".to_string(),
        })?;
    Uuid::parse_str(table_uuid).map_err(|error| CatalogError::InvalidResponse {
        operation: "load catalog table".to_string(),
        message: format!("metadata table-uuid is invalid: {error}"),
    })?;

    if let Some(location) = metadata.get("location") {
        let location = location
            .as_str()
            .ok_or_else(|| CatalogError::InvalidResponse {
                operation: "load catalog table".to_string(),
                message: "metadata location must be a string".to_string(),
            })?;
        validate_response_string("load catalog table", "metadata location", location)?;
        validate_absolute_uri("load catalog table", "metadata location", location)?;
    }

    Ok(LoadedIcebergTable {
        metadata_location: response.metadata_location,
        metadata: response.metadata,
        config: response.config,
    })
}

fn validate_response_string(operation: &str, label: &str, value: &str) -> CatalogResult<()> {
    if value.trim().is_empty() {
        return Err(CatalogError::InvalidResponse {
            operation: operation.to_string(),
            message: format!("{label} must not be blank"),
        });
    }
    if value.contains('\0') {
        return Err(CatalogError::InvalidResponse {
            operation: operation.to_string(),
            message: format!("{label} must not contain NUL"),
        });
    }
    if value.len() > MAX_IDENTIFIER_BYTES * 16 {
        return Err(CatalogError::InvalidResponse {
            operation: operation.to_string(),
            message: format!("{label} is unreasonably large"),
        });
    }
    Ok(())
}

fn validate_absolute_uri(operation: &str, label: &str, value: &str) -> CatalogResult<()> {
    let uri = Url::parse(value).map_err(|error| CatalogError::InvalidResponse {
        operation: operation.to_string(),
        message: format!("{label} must be an absolute URI: {error}"),
    })?;
    if uri.cannot_be_a_base() {
        return Err(CatalogError::InvalidResponse {
            operation: operation.to_string(),
            message: format!("{label} must be a hierarchical URI"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use reqwest::Client;
    use serde_json::json;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    async fn mount_config(server: &MockServer, body: serde_json::Value) {
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(server)
            .await;
    }

    fn test_catalog(server: &MockServer) -> GenericRestCatalog {
        GenericRestCatalog::new(RestCatalogConfig::new(server.uri()).unwrap()).unwrap()
    }

    fn valid_load_response() -> serde_json::Value {
        json!({
            "metadata-location": "s3://warehouse/db/table/metadata/v1.json",
            "metadata": {
                "format-version": 2,
                "table-uuid": "4d9d09d7-927d-47f6-9063-06e62f070a3b",
                "location": "s3://warehouse/db/table"
            },
            "config": {
                "token": "table-secret"
            }
        })
    }

    #[test]
    fn configuration_rejects_unsafe_or_unbounded_values() {
        assert!(RestCatalogConfig::new("file:///tmp/catalog").is_err());
        assert!(RestCatalogConfig::new("https://user:secret@example.com").is_err());
        assert!(RestCatalogConfig::new("https://example.com?token=secret").is_err());
        assert!(
            RestCatalogConfig::new("https://example.com")
                .unwrap()
                .with_timeout(Duration::ZERO)
                .is_err()
        );
        assert!(
            RestCatalogConfig::new("https://example.com")
                .unwrap()
                .with_page_size(0)
                .is_err()
        );
        assert!(
            RestCatalogConfig::new("https://example.com")
                .unwrap()
                .with_max_response_bytes(0)
                .is_err()
        );
    }

    #[test]
    fn configuration_debug_redacts_bearer_token() {
        let config = RestCatalogConfig::new("https://example.com")
            .unwrap()
            .with_bearer_token("top-secret")
            .unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("top-secret"));
        assert!(debug.contains("has_bearer_token: true"));
    }

    #[test]
    fn table_identifier_is_validated() {
        assert!(IcebergTableId::new("", "events").is_err());
        assert!(IcebergTableId::new("analytics", " ").is_err());
        assert!(IcebergTableId::new(" analytics", "events").is_err());
        let table = IcebergTableId::new("analytics", "events").unwrap();
        assert_eq!(table.namespace(), "analytics");
        assert_eq!(table.name(), "events");
    }

    #[test]
    fn url_segments_are_encoded_and_base_path_is_preserved() {
        let base = Url::parse("https://example.com/catalog/api/").unwrap();
        let url = append_url_segments(&base, ["v1", "namespaces", "a/b", "tables"]).unwrap();
        assert_eq!(
            url.as_str(),
            "https://example.com/catalog/api/v1/namespaces/a%2Fb/tables"
        );
    }

    #[tokio::test]
    async fn config_negotiation_applies_warehouse_auth_and_server_prefix_override() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .and(query_param("warehouse", "s3://warehouse"))
            .and(header("authorization", "Bearer catalog-secret"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "defaults": { "prefix": "default" },
                "overrides": { "prefix": "tenant/prod" }
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/tenant/prod/namespaces/analytics/tables"))
            .and(query_param("pageToken", ""))
            .and(query_param("pageSize", "1000"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "identifiers": [
                    { "namespace": ["analytics"], "name": "events" }
                ]
            })))
            .mount(&server)
            .await;

        let config = RestCatalogConfig::new(server.uri())
            .unwrap()
            .with_warehouse("s3://warehouse")
            .unwrap()
            .with_catalog_prefix("client")
            .unwrap()
            .with_bearer_token("catalog-secret")
            .unwrap();
        let catalog = GenericRestCatalog::new(config).unwrap();

        assert_eq!(
            catalog.list_tables("analytics").await.unwrap(),
            vec!["events"]
        );
    }

    #[tokio::test]
    async fn list_tables_follows_pagination_and_negotiates_once() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "defaults": {},
                "overrides": {}
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .and(query_param("pageToken", ""))
            .and(query_param("pageSize", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "identifiers": [
                    { "namespace": ["ns"], "name": "a" },
                    { "namespace": ["ns"], "name": "b" }
                ],
                "next-page-token": "page-2"
            })))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .and(query_param("pageToken", "page-2"))
            .and(query_param("pageSize", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "identifiers": [
                    { "namespace": ["ns"], "name": "c" }
                ],
                "next-page-token": null
            })))
            .expect(2)
            .mount(&server)
            .await;

        let config = RestCatalogConfig::new(server.uri())
            .unwrap()
            .with_page_size(2)
            .unwrap();
        let catalog = GenericRestCatalog::new(config).unwrap();

        assert_eq!(
            catalog.list_tables("ns").await.unwrap(),
            vec!["a", "b", "c"]
        );
        assert_eq!(
            catalog.list_tables("ns").await.unwrap(),
            vec!["a", "b", "c"]
        );
    }

    #[tokio::test]
    async fn list_tables_rejects_repeated_page_token() {
        let server = MockServer::start().await;
        mount_config(&server, json!({ "defaults": {}, "overrides": {} })).await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "identifiers": [],
                "next-page-token": "same"
            })))
            .mount(&server)
            .await;

        let error = test_catalog(&server).list_tables("ns").await.unwrap_err();
        assert!(matches!(error, CatalogError::InvalidResponse { .. }));
        assert!(error.to_string().contains("repeated a pagination token"));
    }

    #[tokio::test]
    async fn list_tables_rejects_malformed_and_duplicate_identifiers() {
        let server = MockServer::start().await;
        mount_config(&server, json!({ "defaults": {}, "overrides": {} })).await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "identifiers": [
                    { "namespace": ["ns"], "name": "events" },
                    { "namespace": ["ns"], "name": "events" }
                ]
            })))
            .mount(&server)
            .await;

        let error = test_catalog(&server).list_tables("ns").await.unwrap_err();
        assert!(error.to_string().contains("duplicate table identifier"));
    }

    #[tokio::test]
    async fn list_tables_validates_multipart_namespace_with_server_separator() {
        let server = MockServer::start().await;
        mount_config(
            &server,
            json!({
                "defaults": {},
                "overrides": { "namespace-separator": "%2E" }
            }),
        )
        .await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/accounting.tax/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "identifiers": [
                    { "namespace": ["accounting", "tax"], "name": "payments" }
                ]
            })))
            .mount(&server)
            .await;

        assert_eq!(
            test_catalog(&server)
                .list_tables("accounting.tax")
                .await
                .unwrap(),
            vec!["payments"]
        );
    }

    #[tokio::test]
    async fn list_tables_rejects_identifier_from_another_namespace() {
        let server = MockServer::start().await;
        mount_config(&server, json!({ "defaults": {}, "overrides": {} })).await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "identifiers": [
                    { "namespace": ["other"], "name": "events" }
                ]
            })))
            .mount(&server)
            .await;

        let error = test_catalog(&server).list_tables("ns").await.unwrap_err();
        assert!(error.to_string().contains("while listing 'ns'"));
    }

    #[tokio::test]
    async fn list_tables_rejects_missing_identifiers() {
        let server = MockServer::start().await;
        mount_config(&server, json!({ "defaults": {}, "overrides": {} })).await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&server)
            .await;

        let error = test_catalog(&server).list_tables("ns").await.unwrap_err();
        assert!(matches!(error, CatalogError::InvalidResponse { .. }));
    }

    #[tokio::test]
    async fn advertised_capabilities_are_enforced() {
        let server = MockServer::start().await;
        mount_config(
            &server,
            json!({
                "defaults": {},
                "overrides": {},
                "endpoints": [
                    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"
                ]
            }),
        )
        .await;

        let error = test_catalog(&server).list_tables("ns").await.unwrap_err();
        assert!(matches!(error, CatalogError::UnsupportedOperation { .. }));
    }

    #[tokio::test]
    async fn list_not_found_maps_to_schema_not_found() {
        let server = MockServer::start().await;
        mount_config(&server, json!({ "defaults": {}, "overrides": {} })).await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/missing/tables"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "error": {
                    "message": "namespace is missing",
                    "type": "NoSuchNamespaceException",
                    "code": 404
                }
            })))
            .mount(&server)
            .await;

        let error = test_catalog(&server)
            .list_tables("missing")
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            CatalogError::SchemaNotFound { name } if name == "missing"
        ));
    }

    #[tokio::test]
    async fn load_table_returns_validated_envelope_and_redacts_config_debug() {
        let server = MockServer::start().await;
        mount_config(&server, json!({ "defaults": {}, "overrides": {} })).await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables/events"))
            .respond_with(ResponseTemplate::new(200).set_body_json(valid_load_response()))
            .mount(&server)
            .await;

        let table = IcebergTableId::new("ns", "events").unwrap();
        let loaded = test_catalog(&server).load_table(&table).await.unwrap();
        assert_eq!(
            loaded.metadata_location(),
            "s3://warehouse/db/table/metadata/v1.json"
        );
        assert_eq!(loaded.metadata()["format-version"], 2);
        assert_eq!(loaded.config().get("token").unwrap(), "table-secret");
        assert!(!format!("{loaded:?}").contains("table-secret"));
        assert!(!format!("{loaded:?}").contains("s3://warehouse"));
    }

    #[tokio::test]
    async fn load_table_metadata_compatibility_helper_returns_only_metadata() {
        let server = MockServer::start().await;
        mount_config(&server, json!({ "defaults": {}, "overrides": {} })).await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables/events"))
            .respond_with(ResponseTemplate::new(200).set_body_json(valid_load_response()))
            .mount(&server)
            .await;

        let metadata = test_catalog(&server)
            .load_table_metadata(&IcebergTableId::new("ns", "events").unwrap())
            .await
            .unwrap();
        assert_eq!(metadata["format-version"], 2);
        assert!(metadata.get("metadata-location").is_none());
    }

    #[tokio::test]
    async fn load_table_rejects_invalid_metadata_contract() {
        let server = MockServer::start().await;
        mount_config(&server, json!({ "defaults": {}, "overrides": {} })).await;
        let mut response = valid_load_response();
        response["metadata"]["table-uuid"] = json!("not-a-uuid");
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables/events"))
            .respond_with(ResponseTemplate::new(200).set_body_json(response))
            .mount(&server)
            .await;

        let error = test_catalog(&server)
            .load_table(&IcebergTableId::new("ns", "events").unwrap())
            .await
            .unwrap_err();
        assert!(matches!(error, CatalogError::InvalidResponse { .. }));
        assert!(error.to_string().contains("table-uuid is invalid"));
    }

    #[tokio::test]
    async fn load_table_rejects_relative_metadata_location() {
        let server = MockServer::start().await;
        mount_config(&server, json!({ "defaults": {}, "overrides": {} })).await;
        let mut response = valid_load_response();
        response["metadata-location"] = json!("metadata/v1.json");
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables/events"))
            .respond_with(ResponseTemplate::new(200).set_body_json(response))
            .mount(&server)
            .await;

        let error = test_catalog(&server)
            .load_table(&IcebergTableId::new("ns", "events").unwrap())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("must be an absolute URI"));
    }

    #[tokio::test]
    async fn load_table_not_found_maps_to_table_not_found() {
        let server = MockServer::start().await;
        mount_config(&server, json!({ "defaults": {}, "overrides": {} })).await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables/missing"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let error = test_catalog(&server)
            .load_table(&IcebergTableId::new("ns", "missing").unwrap())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            CatalogError::TableNotFound { name } if name == "ns.missing"
        ));
    }

    #[tokio::test]
    async fn response_body_limit_is_enforced_without_content_length_reliance() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(
                    json!({
                        "defaults": {},
                        "overrides": {},
                        "padding": "x".repeat(2_000)
                    })
                    .to_string(),
                ),
            )
            .mount(&server)
            .await;

        let config = RestCatalogConfig::new(server.uri())
            .unwrap()
            .with_max_response_bytes(128)
            .unwrap();
        let error = GenericRestCatalog::new(config)
            .unwrap()
            .list_tables("ns")
            .await
            .unwrap_err();
        assert!(matches!(error, CatalogError::ResponseTooLarge { .. }));
    }

    #[tokio::test]
    async fn iceberg_error_envelope_preserves_type_message_and_code() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({
                "error": {
                    "message": "catalog unavailable",
                    "type": "ServiceUnavailableException",
                    "code": 503
                }
            })))
            .mount(&server)
            .await;

        let error = test_catalog(&server).list_tables("ns").await.unwrap_err();
        assert!(matches!(error, CatalogError::Http { status: 503, .. }));
        let message = error.to_string();
        assert!(message.contains("ServiceUnavailableException"));
        assert!(message.contains("catalog unavailable"));
        assert!(message.contains("catalog code 503"));
    }

    #[tokio::test]
    async fn custom_http_client_is_supported() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .and(header("x-catalog-tenant", "tenant-a"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "defaults": {},
                "overrides": {}
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/namespaces/ns/tables"))
            .and(header("x-catalog-tenant", "tenant-a"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "identifiers": []
            })))
            .mount(&server)
            .await;

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-catalog-tenant", "tenant-a".parse().unwrap());
        let client = Client::builder().default_headers(headers).build().unwrap();
        let catalog = GenericRestCatalog::with_http_client(
            RestCatalogConfig::new(server.uri()).unwrap(),
            client,
        )
        .unwrap();
        assert!(catalog.list_tables("ns").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn request_timeout_is_reported_as_transport_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/config"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(100))
                    .set_body_json(json!({ "defaults": {}, "overrides": {} })),
            )
            .mount(&server)
            .await;

        let config = RestCatalogConfig::new(server.uri())
            .unwrap()
            .with_timeout(Duration::from_millis(10))
            .unwrap();
        let error = GenericRestCatalog::new(config)
            .unwrap()
            .list_tables("ns")
            .await
            .unwrap_err();
        assert!(matches!(error, CatalogError::Transport { .. }));
    }
}
