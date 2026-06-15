use std::collections::VecDeque;
use std::sync::{Arc, Mutex as StdMutex};

use dashmap::DashMap;
use reqwest::header::ACCEPT;
use reqwest::{Client, Url};
use tokio::sync::Mutex as AsyncMutex;

use super::{SchemaRegistryError, SchemaRegistryResult};

pub(crate) const MAX_REGISTRY_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_ERROR_BODY_CHARS: usize = 8 * 1024;
pub(crate) const MAX_CACHED_SCHEMAS: usize = 1_024;
pub(crate) const MAX_CACHED_SCHEMA_BYTES: usize = 32 * 1024 * 1024;

/// HTTP schema cache keyed by schema id.
#[derive(Clone)]
pub struct SchemaRegistryClient {
    base_url: Url,
    pub(crate) cache: Arc<DashMap<u32, String>>,
    pub(crate) avro_cache: Arc<DashMap<u32, Arc<apache_avro::Schema>>>,
    protobuf_cache: Arc<DashMap<u32, Arc<Vec<super::protobuf::ProtoField>>>>,
    pub(crate) cache_state: Arc<StdMutex<SchemaCacheState>>,
    pub(crate) fetch_locks: Arc<DashMap<u32, Arc<AsyncMutex<()>>>>,
    http: Client,
}

#[derive(Default)]
pub(crate) struct SchemaCacheState {
    order: VecDeque<u32>,
    pub(crate) total_bytes: usize,
}

struct FetchLockLease<'a> {
    locks: &'a DashMap<u32, Arc<AsyncMutex<()>>>,
    id: u32,
    lock: Arc<AsyncMutex<()>>,
}

impl Drop for FetchLockLease<'_> {
    fn drop(&mut self) {
        if let dashmap::mapref::entry::Entry::Occupied(entry) = self.locks.entry(self.id)
            && Arc::ptr_eq(entry.get(), &self.lock)
            && Arc::strong_count(entry.get()) == 2
        {
            entry.remove();
        }
    }
}

impl SchemaRegistryClient {
    /// Create a registry client with bounded request timeouts.
    pub fn new(url: impl AsRef<str>) -> SchemaRegistryResult<Self> {
        let http = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(10))
            .user_agent(concat!(
                "krishiv-connectors-schema-registry/",
                env!("CARGO_PKG_VERSION")
            ))
            .build()
            .map_err(|error| SchemaRegistryError::InvalidConfiguration {
                field: "http_client",
                message: error.to_string(),
            })?;
        Self::with_http_client(url, http)
    }

    /// Create a registry client with a caller-configured HTTP client.
    ///
    /// This supports custom authentication headers, private certificate roots,
    /// proxies, and timeout policies without embedding secrets in registry URLs.
    pub fn with_http_client(url: impl AsRef<str>, http: Client) -> SchemaRegistryResult<Self> {
        let base_url = Self::parse_base_url(url.as_ref())?;
        Ok(Self {
            base_url,
            cache: Arc::new(DashMap::new()),
            avro_cache: Arc::new(DashMap::new()),
            protobuf_cache: Arc::new(DashMap::new()),
            cache_state: Arc::new(StdMutex::new(SchemaCacheState::default())),
            fetch_locks: Arc::new(DashMap::new()),
            http,
        })
    }

    pub async fn fetch_schema(&self, id: u32) -> SchemaRegistryResult<String> {
        if let Some(hit) = self.cached_schema(id) {
            return Ok(hit);
        }

        let lease = FetchLockLease {
            locks: &self.fetch_locks,
            id,
            lock: self
                .fetch_locks
                .entry(id)
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone(),
        };
        let guard = lease.lock.lock().await;
        let result = if let Some(hit) = self.cached_schema(id) {
            Ok(hit)
        } else {
            self.fetch_schema_uncached(id).await
        };
        drop(guard);
        result
    }

    async fn fetch_schema_uncached(&self, id: u32) -> SchemaRegistryResult<String> {
        let url = self.schema_url(id)?;
        let resp = self
            .http
            .get(url)
            .header(
                ACCEPT,
                "application/vnd.schemaregistry.v1+json, application/json",
            )
            .send()
            .await
            .map_err(|error| SchemaRegistryError::Request {
                schema_id: id,
                message: error.to_string(),
            })?;
        let status = resp.status();
        let body = read_response_body(resp, id, status.as_u16()).await?;
        if !status.is_success() {
            let body = String::from_utf8_lossy(&body);
            let body = bounded_error_body(body.trim());
            return Err(SchemaRegistryError::HttpStatus {
                status: status.as_u16(),
                body,
            });
        }
        #[derive(serde::Deserialize)]
        struct Body {
            schema: String,
        }
        let body: Body = serde_json::from_slice(&body).map_err(|error| {
            SchemaRegistryError::InvalidResponse(format!(
                "schema id {id} response was not valid JSON: {error}"
            ))
        })?;
        if body.schema.trim().is_empty() {
            return Err(SchemaRegistryError::InvalidResponse(format!(
                "schema id {id} response contained a blank schema"
            )));
        }
        self.cache_schema(id, body.schema.clone());
        Ok(body.schema)
    }

    pub(crate) fn cached_schema(&self, id: u32) -> Option<String> {
        let mut state = self
            .cache_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let schema = self.cache.get(&id)?.clone();
        touch_cache_order(&mut state.order, id);
        Some(schema)
    }

    fn touch_cached_schema(&self, id: u32) {
        let mut state = self
            .cache_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.cache.contains_key(&id) {
            touch_cache_order(&mut state.order, id);
        }
    }

    pub(crate) fn cache_schema(&self, id: u32, schema: String) {
        let mut state = self
            .cache_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some((_, previous)) = self.cache.remove(&id) {
            state.total_bytes = state.total_bytes.saturating_sub(previous.len());
        }
        self.avro_cache.remove(&id);
        self.protobuf_cache.remove(&id);
        if let Some(index) = state.order.iter().position(|cached_id| *cached_id == id) {
            state.order.remove(index);
        }
        if schema.len() > MAX_CACHED_SCHEMA_BYTES {
            return;
        }
        while state.order.len() >= MAX_CACHED_SCHEMAS
            || state.total_bytes.saturating_add(schema.len()) > MAX_CACHED_SCHEMA_BYTES
        {
            if let Some(evicted_id) = state.order.pop_front() {
                if let Some((_, evicted)) = self.cache.remove(&evicted_id) {
                    state.total_bytes = state.total_bytes.saturating_sub(evicted.len());
                }
                self.avro_cache.remove(&evicted_id);
                self.protobuf_cache.remove(&evicted_id);
            } else {
                state.total_bytes = 0;
                break;
            }
        }
        state.total_bytes += schema.len();
        self.cache.insert(id, schema);
        state.order.push_back(id);
    }

    pub(crate) async fn fetch_avro_schema(
        &self,
        id: u32,
    ) -> SchemaRegistryResult<Arc<apache_avro::Schema>> {
        if let Some(cached) = self.avro_cache.get(&id) {
            let schema = cached.clone();
            drop(cached);
            self.touch_cached_schema(id);
            return Ok(schema);
        }
        let schema_str = self.fetch_schema(id).await?;
        if let Some(schema) = self.avro_cache.get(&id) {
            return Ok(schema.clone());
        }
        let schema = Arc::new(
            apache_avro::Schema::parse_str(&schema_str)
                .map_err(|error| SchemaRegistryError::Decode(error.to_string()))?,
        );
        self.cache_parsed_avro(id, schema.clone());
        Ok(schema)
    }

    pub(crate) async fn fetch_protobuf_schema(
        &self,
        id: u32,
    ) -> SchemaRegistryResult<Arc<Vec<super::protobuf::ProtoField>>> {
        if let Some(cached) = self.protobuf_cache.get(&id) {
            let schema = cached.clone();
            drop(cached);
            self.touch_cached_schema(id);
            return Ok(schema);
        }
        let schema_str = self.fetch_schema(id).await?;
        if let Some(schema) = self.protobuf_cache.get(&id) {
            return Ok(schema.clone());
        }
        let schema = Arc::new(super::protobuf::parse_proto_schema(&schema_str)?);
        self.cache_parsed_protobuf(id, schema.clone());
        Ok(schema)
    }

    pub(crate) fn cache_parsed_avro(&self, id: u32, schema: Arc<apache_avro::Schema>) {
        let _state = self
            .cache_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.cache.contains_key(&id) {
            self.avro_cache.insert(id, schema);
        }
    }

    fn cache_parsed_protobuf(&self, id: u32, schema: Arc<Vec<super::protobuf::ProtoField>>) {
        let _state = self
            .cache_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.cache.contains_key(&id) {
            self.protobuf_cache.insert(id, schema);
        }
    }

    pub(crate) fn parse_base_url(raw_url: &str) -> SchemaRegistryResult<Url> {
        let raw_url = raw_url.trim();
        if raw_url.is_empty() {
            return Err(SchemaRegistryError::InvalidConfiguration {
                field: "url",
                message: "must not be blank".into(),
            });
        }
        let mut url =
            Url::parse(raw_url).map_err(|error| SchemaRegistryError::InvalidConfiguration {
                field: "url",
                message: error.to_string(),
            })?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(SchemaRegistryError::InvalidConfiguration {
                field: "url",
                message: "scheme must be http or https".into(),
            });
        }
        if url.cannot_be_a_base() || url.host_str().is_none() {
            return Err(SchemaRegistryError::InvalidConfiguration {
                field: "url",
                message: "must be an absolute hierarchical URL with a host".into(),
            });
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(SchemaRegistryError::InvalidConfiguration {
                field: "url",
                message: "query parameters and fragments are not allowed".into(),
            });
        }
        let normalized_path = url.path().trim_end_matches('/').to_string();
        url.set_path(if normalized_path.is_empty() {
            "/"
        } else {
            &normalized_path
        });
        Ok(url)
    }

    fn schema_url(&self, id: u32) -> SchemaRegistryResult<Url> {
        let mut url = self.base_url.clone();
        let id = id.to_string();
        url.path_segments_mut()
            .map_err(|_| SchemaRegistryError::InvalidConfiguration {
                field: "url",
                message: "cannot append registry API path".into(),
            })?
            .pop_if_empty()
            .extend(["schemas", "ids", id.as_str()]);
        Ok(url)
    }
}

fn touch_cache_order(order: &mut VecDeque<u32>, id: u32) {
    if let Some(index) = order.iter().position(|cached_id| *cached_id == id) {
        order.remove(index);
    }
    order.push_back(id);
}

fn bounded_error_body(body: &str) -> String {
    if body.is_empty() {
        return "<empty response body>".into();
    }
    let mut chars = body.chars();
    let mut bounded = chars
        .by_ref()
        .take(MAX_ERROR_BODY_CHARS)
        .collect::<String>();
    if chars.next().is_some() {
        bounded.push_str("...");
    }
    bounded
}

async fn read_response_body(
    mut response: reqwest::Response,
    schema_id: u32,
    status: u16,
) -> SchemaRegistryResult<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) =
        response
            .chunk()
            .await
            .map_err(|error| SchemaRegistryError::Request {
                schema_id,
                message: error.to_string(),
            })?
    {
        let next_len =
            body.len()
                .checked_add(chunk.len())
                .ok_or(SchemaRegistryError::ResponseTooLarge {
                    schema_id,
                    status,
                    limit_bytes: MAX_REGISTRY_RESPONSE_BYTES,
                })?;
        if next_len > MAX_REGISTRY_RESPONSE_BYTES {
            return Err(SchemaRegistryError::ResponseTooLarge {
                schema_id,
                status,
                limit_bytes: MAX_REGISTRY_RESPONSE_BYTES,
            });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

