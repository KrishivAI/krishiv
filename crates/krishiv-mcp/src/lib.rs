#![forbid(unsafe_code)]

//! Model Context Protocol frontend for Krishiv.
//!
//! The MCP server is deliberately a frontend over [`krishiv_api::Session`].
//! Runtime mode, placement, scheduling, connector behavior, and durability stay
//! behind existing Krishiv crate APIs.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use krishiv_api::{
    Checkpointable, EngineKind, ExecutionMode, FeedableJob, Job, KrishivError, Session,
    SessionBuilder, SubmittedSqlJobStatus,
};
use krishiv_connectors::ConnectorConfig;
use krishiv_connectors::registry::{ConnectorRole, default_registry};
use krishiv_ivm::IncrementalViewSpec;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const DEFAULT_MCP_ADDR: &str = "127.0.0.1:8765";
const DEFAULT_MAX_ROWS: usize = 100;
const DEFAULT_TIMEOUT_MS: u64 = 30_000;

/// Errors from starting or serving the MCP frontend.
#[derive(Debug, thiserror::Error)]
pub enum McpServerError {
    #[error("invalid MCP config: {0}")]
    InvalidConfig(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Krishiv error: {0}")]
    Krishiv(#[from] KrishivError),
    #[error("invalid socket address: {0}")]
    AddrParse(#[from] std::net::AddrParseError),
}

type McpResult<T> = std::result::Result<T, McpServerError>;
type ToolResult<T> = std::result::Result<T, ToolError>;

#[derive(Debug, thiserror::Error)]
enum ToolError {
    #[error("missing required argument '{0}'")]
    MissingArgument(&'static str),
    #[error("argument '{name}' must be {expected}")]
    InvalidArgument {
        name: &'static str,
        expected: &'static str,
    },
    #[error("unsupported MCP tool operation: {0}")]
    Unsupported(String),
    #[error("{0}")]
    Runtime(String),
}

impl From<KrishivError> for ToolError {
    fn from(value: KrishivError) -> Self {
        Self::Runtime(value.to_string())
    }
}

/// MCP server runtime configuration.
#[derive(Debug, Clone)]
pub struct McpConfig {
    pub max_rows: usize,
    pub default_timeout_ms: u64,
    pub allow_write_sql: bool,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_MAX_ROWS,
            default_timeout_ms: DEFAULT_TIMEOUT_MS,
            allow_write_sql: false,
        }
    }
}

impl McpConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();
        if let Ok(raw) = std::env::var("KRISHIV_MCP_MAX_ROWS")
            && let Ok(value) = raw.trim().parse::<usize>()
        {
            config.max_rows = value;
        }
        if let Ok(raw) = std::env::var("KRISHIV_MCP_TIMEOUT_MS")
            && let Ok(value) = raw.trim().parse::<u64>()
        {
            config.default_timeout_ms = value;
        }
        if let Ok(raw) = std::env::var("KRISHIV_MCP_ALLOW_WRITE_SQL") {
            config.allow_write_sql = matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            );
        }
        config
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum McpTransport {
    Stdio,
    Http,
}

/// Run an MCP server configured from environment variables and CLI flags.
pub async fn run_mcp_from_env(args: &[String]) -> McpResult<()> {
    let mut transport = match std::env::var("KRISHIV_MCP_TRANSPORT")
        .unwrap_or_else(|_| "stdio".to_string())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "http" | "streamable-http" | "streamable_http" => McpTransport::Http,
        "stdio" => McpTransport::Stdio,
        other => {
            return Err(McpServerError::InvalidConfig(format!(
                "unknown KRISHIV_MCP_TRANSPORT '{other}'; expected stdio or http"
            )));
        }
    };
    let mut addr = std::env::var("KRISHIV_MCP_ADDR").unwrap_or_else(|_| DEFAULT_MCP_ADDR.into());

    let mut i = 0usize;
    while i < args.len() {
        let arg = args.get(i).map(String::as_str).unwrap_or_default();
        match arg {
            "--stdio" => {
                transport = McpTransport::Stdio;
                i += 1;
            }
            "--http" => {
                transport = McpTransport::Http;
                i += 1;
            }
            "--addr" => {
                let Some(value) = args.get(i + 1) else {
                    return Err(McpServerError::InvalidConfig(
                        "--addr requires a socket address".into(),
                    ));
                };
                addr = value.clone();
                i += 2;
            }
            "--transport" => {
                let Some(value) = args.get(i + 1) else {
                    return Err(McpServerError::InvalidConfig(
                        "--transport requires stdio or http".into(),
                    ));
                };
                transport = match value.trim().to_ascii_lowercase().as_str() {
                    "stdio" => McpTransport::Stdio,
                    "http" | "streamable-http" | "streamable_http" => McpTransport::Http,
                    other => {
                        return Err(McpServerError::InvalidConfig(format!(
                            "unknown transport '{other}'; expected stdio or http"
                        )));
                    }
                };
                i += 2;
            }
            other => {
                return Err(McpServerError::InvalidConfig(format!(
                    "unknown mcp argument '{other}'"
                )));
            }
        }
    }

    let mut builder = SessionBuilder::from_env()?;
    if let Ok(http_url) = std::env::var("KRISHIV_COORDINATOR_HTTP")
        && !http_url.trim().is_empty()
    {
        builder = builder.with_coordinator_http(http_url);
    }
    let server = Arc::new(KrishivMcpServer::new(
        builder.build()?,
        McpConfig::from_env(),
    ));
    match transport {
        McpTransport::Stdio => serve_stdio(server).await,
        McpTransport::Http => serve_http(server, addr.parse()?).await,
    }
}

/// Help text for `krishiv mcp`.
pub fn mcp_help() -> &'static str {
    "Model Context Protocol server for Krishiv.\n\
     \n\
     Usage:\n\
       krishiv mcp [--stdio]\n\
       krishiv mcp --http [--addr 127.0.0.1:8765]\n\
     \n\
     Env:\n\
       KRISHIV_MODE                       embedded | single-node | distributed | bare-metal | k8s\n\
       KRISHIV_COORDINATOR_URL            Flight/coordinator URL for single-node/distributed\n\
       KRISHIV_COORDINATOR_HTTP           HTTP management URL for jobs and IVM\n\
       KRISHIV_COORDINATOR_BEARER_TOKEN   Bearer token for protected coordinator HTTP APIs\n\
       KRISHIV_MCP_TRANSPORT              stdio | http (default stdio)\n\
       KRISHIV_MCP_ADDR                   HTTP bind address (default 127.0.0.1:8765)\n\
       KRISHIV_MCP_MAX_ROWS               max rows returned by SQL/sample tools (default 100)\n\
       KRISHIV_MCP_TIMEOUT_MS             default SQL timeout (default 30000)\n\
       KRISHIV_MCP_ALLOW_WRITE_SQL        allow execute_sql to run non-read-only SQL\n"
}

/// Mode-aware MCP server over a Krishiv session.
#[derive(Clone)]
pub struct KrishivMcpServer {
    session: Arc<Session>,
    config: McpConfig,
}

impl KrishivMcpServer {
    pub fn new(session: Session, config: McpConfig) -> Self {
        Self {
            session: Arc::new(session),
            config,
        }
    }

    /// Handle one JSON-RPC request or notification.
    pub async fn handle_json_rpc(&self, message: Value) -> Option<Value> {
        let id = message.get("id").cloned();
        let method = match message.get("method").and_then(Value::as_str) {
            Some(method) => method,
            None => {
                return Some(json_rpc_error(id, -32600, "invalid JSON-RPC request", None));
            }
        };

        if id.is_none() && method.starts_with("notifications/") {
            return None;
        }

        let params = message.get("params").cloned();
        let result = match method {
            "initialize" => Ok(self.initialize_result()),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({ "tools": self.tools() })),
            "tools/call" => Ok(self.handle_tool_call(params).await),
            "resources/list" => Ok(json!({ "resources": self.resources() })),
            "resources/read" => self.handle_resource_read(params).await,
            "prompts/list" => Ok(json!({ "prompts": self.prompts() })),
            "prompts/get" => self.handle_prompt_get(params),
            unknown => Err((-32601, format!("unknown MCP method '{unknown}'"))),
        };

        Some(match result {
            Ok(result) => json!({
                "jsonrpc": "2.0",
                "id": id.unwrap_or(Value::Null),
                "result": result
            }),
            Err((code, message)) => json_rpc_error(id, code, &message, None),
        })
    }

    fn initialize_result(&self) -> Value {
        json!({
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {
                "tools": { "listChanged": false },
                "resources": { "subscribe": false, "listChanged": false },
                "prompts": { "listChanged": false }
            },
            "serverInfo": {
                "name": "krishiv-mcp",
                "version": env!("CARGO_PKG_VERSION")
            },
            "instructions": "Use Krishiv MCP tools as a typed frontend over the configured Session. Runtime mode and deployment placement are selected by KRISHIV_MODE and coordinator environment variables."
        })
    }

    fn tools(&self) -> Vec<Value> {
        vec![
            tool(
                "krishiv_health",
                "Krishiv Health",
                "Return MCP server and Krishiv runtime health.",
                object_schema([], []),
            ),
            tool(
                "runtime_info",
                "Runtime Info",
                "Return execution mode, placement, coordinator endpoints, and MCP limits.",
                object_schema([], []),
            ),
            tool(
                "deployment_capabilities",
                "Deployment Capabilities",
                "Return mode-aware MCP execution and control-plane routing capabilities.",
                object_schema([], []),
            ),
            tool(
                "execute_sql",
                "Execute SQL",
                "Run a bounded SQL query and return a capped JSON table.",
                object_schema(
                    [
                        (
                            "query",
                            json!({ "type": "string", "description": "SQL query to execute" }),
                        ),
                        (
                            "limit",
                            json!({ "type": "integer", "minimum": 0, "description": "Maximum rows to return" }),
                        ),
                        (
                            "timeout_ms",
                            json!({ "type": "integer", "minimum": 1, "description": "Wall-clock timeout in milliseconds" }),
                        ),
                        (
                            "read_only",
                            json!({ "type": "boolean", "description": "Reject non-read-only SQL before execution" }),
                        ),
                    ],
                    ["query"],
                ),
            ),
            tool(
                "explain_sql",
                "Explain SQL",
                "Return logical, physical, or analyze plan text for a SQL query.",
                object_schema(
                    [
                        ("query", json!({ "type": "string" })),
                        (
                            "mode",
                            json!({ "type": "string", "enum": ["logical", "physical", "analyze"] }),
                        ),
                    ],
                    ["query"],
                ),
            ),
            tool(
                "list_catalogs",
                "List Catalogs",
                "List the catalogs/schemas visible through this MCP session.",
                object_schema([], []),
            ),
            tool(
                "list_tables",
                "List Tables",
                "List registered table names in the current session catalog.",
                object_schema([], []),
            ),
            tool(
                "describe_table",
                "Describe Table",
                "Return the Arrow/DataFusion schema for a table.",
                object_schema([("table", json!({ "type": "string" }))], ["table"]),
            ),
            tool(
                "sample_table",
                "Sample Table",
                "Return a capped sample from a table.",
                object_schema(
                    [
                        ("table", json!({ "type": "string" })),
                        ("limit", json!({ "type": "integer", "minimum": 0 })),
                    ],
                    ["table"],
                ),
            ),
            tool(
                "submit_sql_job",
                "Submit SQL Job",
                "Compile and submit a Krishiv SQL pipeline job; returns a job handle.",
                object_schema(
                    [(
                        "sql",
                        json!({ "type": "string", "description": "CREATE SOURCE/SINK pipeline SQL script" }),
                    )],
                    ["sql"],
                ),
            ),
            tool(
                "list_jobs",
                "List Jobs",
                "List local jobs or remote coordinator jobs, depending on deployment mode.",
                object_schema([], []),
            ),
            tool(
                "get_job_status",
                "Get Job Status",
                "Return status for a local or coordinator job.",
                object_schema([("job_id", json!({ "type": "string" }))], ["job_id"]),
            ),
            tool(
                "get_job_result",
                "Get Job Result",
                "Return stored job result metadata when the core API supports it.",
                object_schema(
                    [
                        ("job_id", json!({ "type": "string" })),
                        ("limit", json!({ "type": "integer", "minimum": 0 })),
                    ],
                    ["job_id"],
                ),
            ),
            tool(
                "cancel_job",
                "Cancel Job",
                "Cancel a job when the configured Krishiv control plane exposes cancellation.",
                object_schema([("job_id", json!({ "type": "string" }))], ["job_id"]),
            ),
            tool(
                "submit_streaming_pipeline",
                "Submit Streaming Pipeline",
                "Compile and submit a bounded run-once streaming pipeline SQL job.",
                object_schema([("sql", json!({ "type": "string" }))], ["sql"]),
            ),
            tool(
                "get_streaming_job_status",
                "Get Streaming Job Status",
                "Return continuous stream registration metadata or bounded streaming job status.",
                object_schema([("job_id", json!({ "type": "string" }))], ["job_id"]),
            ),
            tool(
                "list_continuous_streams",
                "List Continuous Streams",
                "List continuous stream jobs registered through the mode-aware Session streaming API.",
                object_schema([], []),
            ),
            tool(
                "create_continuous_stream",
                "Create Continuous Stream",
                "Compile windowed streaming SQL and register a continuous stream job across embedded, single-node, or distributed modes.",
                object_schema(
                    [
                        ("job_name", json!({ "type": "string" })),
                        ("query", json!({ "type": "string" })),
                    ],
                    ["job_name", "query"],
                ),
            ),
            tool(
                "feed_continuous_stream",
                "Feed Continuous Stream",
                "Run a SQL query and push its result batches into a registered continuous stream job.",
                object_schema(
                    [
                        ("job_name", json!({ "type": "string" })),
                        ("query", json!({ "type": "string" })),
                    ],
                    ["job_name", "query"],
                ),
            ),
            tool(
                "drain_continuous_stream",
                "Drain Continuous Stream",
                "Drain newly emitted batches from a registered continuous stream job.",
                object_schema(
                    [
                        ("job_name", json!({ "type": "string" })),
                        ("limit", json!({ "type": "integer", "minimum": 0 })),
                    ],
                    ["job_name"],
                ),
            ),
            tool(
                "checkpoint_continuous_stream",
                "Checkpoint Continuous Stream",
                "Export the latest continuous stream checkpoint bytes from the active runtime or coordinator control plane.",
                object_schema([("job_name", json!({ "type": "string" }))], ["job_name"]),
            ),
            tool(
                "restore_continuous_stream",
                "Restore Continuous Stream",
                "Restore a registered continuous stream from exported checkpoint bytes. Provide query to recreate the stream first when needed.",
                object_schema(
                    [
                        ("job_name", json!({ "type": "string" })),
                        ("checkpoint_base64", json!({ "type": "string" })),
                        ("query", json!({ "type": "string" })),
                    ],
                    ["job_name", "checkpoint_base64"],
                ),
            ),
            tool(
                "create_incremental_view",
                "Create Incremental View",
                "Create or update an IVM view in the mode-aware IVM job registry.",
                object_schema(
                    [
                        ("job_name", json!({ "type": "string" })),
                        ("view_name", json!({ "type": "string" })),
                        ("query", json!({ "type": "string" })),
                        ("materialized", json!({ "type": "boolean" })),
                    ],
                    ["job_name", "view_name", "query"],
                ),
            ),
            tool(
                "feed_incremental_view",
                "Feed Incremental View",
                "Run a SQL query and feed its result as a source snapshot into an IVM job.",
                object_schema(
                    [
                        ("job_name", json!({ "type": "string" })),
                        ("source", json!({ "type": "string" })),
                        ("query", json!({ "type": "string" })),
                    ],
                    ["job_name", "source", "query"],
                ),
            ),
            tool(
                "step_incremental_view",
                "Step Incremental View",
                "Run one IVM tick for a job.",
                object_schema([("job_name", json!({ "type": "string" }))], ["job_name"]),
            ),
            tool(
                "snapshot_incremental_view",
                "Snapshot Incremental View",
                "Read a materialized IVM view snapshot.",
                object_schema(
                    [
                        ("job_name", json!({ "type": "string" })),
                        ("view_name", json!({ "type": "string" })),
                        ("limit", json!({ "type": "integer", "minimum": 0 })),
                    ],
                    ["job_name", "view_name"],
                ),
            ),
            tool(
                "checkpoint_incremental_job",
                "Checkpoint Incremental Job",
                "Serialize an IVM job checkpoint through the mode-aware Session IVM API.",
                object_schema(
                    [
                        ("job_name", json!({ "type": "string" })),
                        (
                            "delta",
                            json!({ "type": "boolean", "description": "Return a delta checkpoint instead of a full checkpoint" }),
                        ),
                    ],
                    ["job_name"],
                ),
            ),
            tool(
                "restore_incremental_job",
                "Restore Incremental Job",
                "Restore an IVM job from base64 checkpoint bytes through the mode-aware Session IVM API.",
                object_schema(
                    [
                        ("job_name", json!({ "type": "string" })),
                        (
                            "checkpoint_base64",
                            json!({ "type": "string", "description": "Base64-encoded checkpoint bytes returned by checkpoint_incremental_job" }),
                        ),
                        (
                            "delta",
                            json!({ "type": "boolean", "description": "Apply a delta checkpoint instead of a full checkpoint" }),
                        ),
                    ],
                    ["job_name", "checkpoint_base64"],
                ),
            ),
            tool(
                "list_connectors",
                "List Connectors",
                "List built-in connector drivers and capability flags.",
                object_schema([], []),
            ),
            tool(
                "validate_connector_config",
                "Validate Connector Config",
                "Validate a connector config against the built-in registry without opening it.",
                object_schema(
                    [
                        (
                            "role",
                            json!({ "type": "string", "enum": ["source", "sink", "two_phase_sink"] }),
                        ),
                        ("name", json!({ "type": "string" })),
                        ("kind", json!({ "type": "string" })),
                        (
                            "properties",
                            json!({ "type": "object", "additionalProperties": { "type": "string" } }),
                        ),
                    ],
                    ["role", "name", "kind"],
                ),
            ),
            tool(
                "register_source",
                "Register Source",
                "Register a source in the current session when a direct Session API exists.",
                object_schema(
                    [
                        ("name", json!({ "type": "string" })),
                        ("kind", json!({ "type": "string" })),
                        (
                            "properties",
                            json!({ "type": "object", "additionalProperties": { "type": "string" } }),
                        ),
                    ],
                    ["name", "kind"],
                ),
            ),
            tool(
                "register_sink",
                "Register Sink",
                "Register a sink when a direct Session API exists.",
                object_schema(
                    [
                        ("name", json!({ "type": "string" })),
                        ("kind", json!({ "type": "string" })),
                        (
                            "properties",
                            json!({ "type": "object", "additionalProperties": { "type": "string" } }),
                        ),
                    ],
                    ["name", "kind"],
                ),
            ),
            tool(
                "list_executors",
                "List Executors",
                "Return executor information when exposed through the configured control plane.",
                object_schema([], []),
            ),
            tool(
                "get_metrics_summary",
                "Metrics Summary",
                "Return a compact runtime/job metrics summary available to this MCP process.",
                object_schema([("job_id", json!({ "type": "string" }))], []),
            ),
        ]
    }

    fn resources(&self) -> Vec<Value> {
        vec![
            resource(
                "krishiv://runtime",
                "runtime",
                "Krishiv runtime mode, placement, and MCP limits",
            ),
            resource(
                "krishiv://deployment/capabilities",
                "deployment-capabilities",
                "Mode-aware MCP execution and control-plane routing capabilities",
            ),
            resource(
                "krishiv://catalog/tables",
                "tables",
                "Session catalog table list",
            ),
            resource(
                "krishiv://jobs",
                "jobs",
                "Local or remote coordinator job status",
            ),
            resource(
                "krishiv://connectors",
                "connectors",
                "Built-in connector driver metadata",
            ),
        ]
    }

    fn prompts(&self) -> Vec<Value> {
        vec![
            json!({
                "name": "investigate_sql",
                "title": "Investigate SQL",
                "description": "Inspect tables, explain a query, then execute a capped result.",
                "arguments": [
                    { "name": "question", "description": "The analysis question", "required": true },
                    { "name": "tables", "description": "Relevant table names", "required": false }
                ]
            }),
            json!({
                "name": "build_streaming_pipeline",
                "title": "Build Streaming Pipeline",
                "description": "Draft a Krishiv streaming pipeline and validate connector capabilities.",
                "arguments": [
                    { "name": "source", "description": "Input source description", "required": true },
                    { "name": "sink", "description": "Output sink description", "required": true }
                ]
            }),
            json!({
                "name": "inspect_job",
                "title": "Inspect Job",
                "description": "Check job status, runtime placement, and available metrics.",
                "arguments": [
                    { "name": "job_id", "description": "Krishiv job id", "required": true }
                ]
            }),
        ]
    }

    async fn handle_tool_call(&self, params: Option<Value>) -> Value {
        let result = match parse_tool_call(params) {
            Ok((name, arguments)) => self.call_tool(&name, arguments).await,
            Err(error) => Err(error),
        };
        match result {
            Ok(value) => tool_result(value, false),
            Err(error) => tool_result(json!({ "error": error.to_string() }), true),
        }
    }

    async fn call_tool(&self, name: &str, arguments: Map<String, Value>) -> ToolResult<Value> {
        match name {
            "krishiv_health" => Ok(self.health()),
            "runtime_info" => Ok(self.runtime_info()),
            "deployment_capabilities" => Ok(self.deployment_capabilities()),
            "execute_sql" => self.tool_execute_sql(&arguments).await,
            "explain_sql" => self.tool_explain_sql(&arguments).await,
            "list_catalogs" => self.tool_list_catalogs(),
            "list_tables" => self.tool_list_tables(),
            "describe_table" => self.tool_describe_table(&arguments).await,
            "sample_table" => self.tool_sample_table(&arguments).await,
            "submit_sql_job" => self.tool_submit_sql_job(&arguments).await,
            "list_jobs" => self.tool_list_jobs().await,
            "get_job_status" => self.tool_get_job_status(&arguments).await,
            "get_job_result" => self.tool_get_job_result(&arguments).await,
            "cancel_job" => self.tool_cancel_job(&arguments).await,
            "submit_streaming_pipeline" => self.tool_submit_streaming_pipeline(&arguments).await,
            "get_streaming_job_status" => self.tool_get_streaming_job_status(&arguments).await,
            "list_continuous_streams" => self.tool_list_continuous_streams().await,
            "create_continuous_stream" => self.tool_create_continuous_stream(&arguments).await,
            "feed_continuous_stream" => self.tool_feed_continuous_stream(&arguments).await,
            "drain_continuous_stream" => self.tool_drain_continuous_stream(&arguments).await,
            "checkpoint_continuous_stream" => {
                self.tool_checkpoint_continuous_stream(&arguments).await
            }
            "restore_continuous_stream" => self.tool_restore_continuous_stream(&arguments).await,
            "create_incremental_view" => self.tool_create_incremental_view(&arguments).await,
            "feed_incremental_view" => self.tool_feed_incremental_view(&arguments).await,
            "step_incremental_view" => self.tool_step_incremental_view(&arguments).await,
            "snapshot_incremental_view" => self.tool_snapshot_incremental_view(&arguments).await,
            "checkpoint_incremental_job" => self.tool_checkpoint_incremental_job(&arguments).await,
            "restore_incremental_job" => self.tool_restore_incremental_job(&arguments).await,
            "list_connectors" => self.tool_list_connectors(),
            "validate_connector_config" => self.tool_validate_connector_config(&arguments),
            "register_source" => self.tool_register_source(&arguments).await,
            "register_sink" => self.tool_register_sink(&arguments),
            "list_executors" => self.tool_list_executors().await,
            "get_metrics_summary" => self.tool_get_metrics_summary(&arguments),
            other => Err(ToolError::Unsupported(format!("unknown tool '{other}'"))),
        }
    }

    async fn handle_resource_read(
        &self,
        params: Option<Value>,
    ) -> std::result::Result<Value, (i64, String)> {
        let Some(uri) = params
            .as_ref()
            .and_then(Value::as_object)
            .and_then(|obj| obj.get("uri"))
            .and_then(Value::as_str)
        else {
            return Err((-32602, "resources/read requires params.uri".into()));
        };
        let value = match uri {
            "krishiv://runtime" => self.runtime_info(),
            "krishiv://deployment/capabilities" => self.deployment_capabilities(),
            "krishiv://catalog/tables" => self.tool_list_tables().map_err(|e| {
                (
                    -32603,
                    format!("failed to list tables for resource read: {e}"),
                )
            })?,
            "krishiv://jobs" => self.tool_list_jobs().await.map_err(|e| {
                (
                    -32603,
                    format!("failed to list jobs for resource read: {e}"),
                )
            })?,
            "krishiv://connectors" => self.tool_list_connectors().map_err(|e| {
                (
                    -32603,
                    format!("failed to list connectors for resource read: {e}"),
                )
            })?,
            other => return Err((-32602, format!("unknown resource URI '{other}'"))),
        };
        Ok(json!({
            "contents": [{
                "uri": uri,
                "mimeType": "application/json",
                "text": value_to_pretty_json(&value)
            }]
        }))
    }

    fn handle_prompt_get(
        &self,
        params: Option<Value>,
    ) -> std::result::Result<Value, (i64, String)> {
        let Some(name) = params
            .as_ref()
            .and_then(Value::as_object)
            .and_then(|obj| obj.get("name"))
            .and_then(Value::as_str)
        else {
            return Err((-32602, "prompts/get requires params.name".into()));
        };
        let args = params
            .as_ref()
            .and_then(Value::as_object)
            .and_then(|obj| obj.get("arguments"))
            .and_then(Value::as_object);
        let get_arg = |key: &str| -> String {
            args.and_then(|obj| obj.get(key))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };
        let text = match name {
            "investigate_sql" => format!(
                "Use krishiv_health, list_tables, describe_table, explain_sql, then execute_sql with a bounded limit. Question: {}. Tables: {}.",
                get_arg("question"),
                get_arg("tables")
            ),
            "build_streaming_pipeline" => format!(
                "Use list_connectors and validate_connector_config, then draft CREATE SOURCE / CREATE SINK SQL for source [{}] and sink [{}]. Submit with submit_streaming_pipeline only after validation.",
                get_arg("source"),
                get_arg("sink")
            ),
            "inspect_job" => format!(
                "Use get_job_status, get_metrics_summary, runtime_info, and list_executors for job_id [{}].",
                get_arg("job_id")
            ),
            other => return Err((-32602, format!("unknown prompt '{other}'"))),
        };
        Ok(json!({
            "description": format!("Krishiv prompt template: {name}"),
            "messages": [{
                "role": "user",
                "content": { "type": "text", "text": text }
            }]
        }))
    }

    fn health(&self) -> Value {
        let runtime = self.session.execution_runtime();
        json!({
            "status": "ok",
            "server": "krishiv-mcp",
            "mcp_protocol_version": MCP_PROTOCOL_VERSION,
            "krishiv_version": env!("CARGO_PKG_VERSION"),
            "mode": format!("{:?}", self.session.mode()),
            "deployment_target": format!("{:?}", self.session.deployment_target()),
            "runtime_mode": format!("{:?}", runtime.mode()),
            "placement": format!("{:?}", runtime.placement()),
            "uses_remote_execution": runtime.uses_remote_execution(),
        })
    }

    fn runtime_info(&self) -> Value {
        let runtime = self.session.execution_runtime();
        json!({
            "mode": format!("{:?}", self.session.mode()),
            "deployment_target": format!("{:?}", self.session.deployment_target()),
            "runtime_mode": format!("{:?}", runtime.mode()),
            "placement": format!("{:?}", runtime.placement()),
            "uses_remote_execution": runtime.uses_remote_execution(),
            "flight_url": runtime.flight_url(),
            "coordinator_http_url": self.session.coordinator_http_url(),
            "coordinator_grpc_url": self.session.coordinator_grpc_url().or(runtime.coordinator_grpc_url()),
            "capabilities": self.deployment_capabilities(),
            "mcp": {
                "max_rows": self.config.max_rows,
                "default_timeout_ms": self.config.default_timeout_ms,
                "allow_write_sql": self.config.allow_write_sql,
                "protocol_version": MCP_PROTOCOL_VERSION,
            }
        })
    }

    fn deployment_capabilities(&self) -> Value {
        let runtime = self.session.execution_runtime();
        let coordinator_http_url = self.session.coordinator_http_url();
        let coordinator_control_plane = coordinator_http_url.is_some();
        json!({
            "mode": format!("{:?}", self.session.mode()),
            "deployment_target": format!("{:?}", self.session.deployment_target()),
            "runtime_mode": format!("{:?}", runtime.mode()),
            "placement": format!("{:?}", runtime.placement()),
            "uses_remote_execution": runtime.uses_remote_execution(),
            "coordinator": {
                "http_url": coordinator_http_url,
                "grpc_url": self.session.coordinator_grpc_url().or(runtime.coordinator_grpc_url()),
                "flight_url": runtime.flight_url(),
                "control_plane_configured": coordinator_control_plane,
            },
            "execution": {
                "execute_sql": if runtime.uses_remote_execution() { "runtime_remote" } else { "session_local" },
                "submit_sql_job": "session_background_over_configured_runtime",
                "submit_streaming_pipeline": "session_submit_over_configured_runtime",
                "continuous_streams": if self.session.mode() == ExecutionMode::Distributed {
                    "coordinator_continuous_stream_api"
                } else {
                    "session_continuous_stream_registry"
                },
                "incremental_views": if self.session.mode() == ExecutionMode::Distributed {
                    "coordinator_ivm"
                } else {
                    "session_ivm_registry"
                },
                "incremental_checkpoints": if self.session.mode() == ExecutionMode::Distributed {
                    "coordinator_ivm_checkpoint_api"
                } else {
                    "session_ivm_registry_checkpoint_api"
                },
            },
            "control_plane_tools": {
                "list_jobs": {
                    "coordinator_first": coordinator_control_plane,
                    "local_fallback": self.session.mode() != ExecutionMode::Distributed,
                },
                "get_job_status": {
                    "coordinator_first": coordinator_control_plane,
                    "local_submitted_job_fallback": true,
                },
                "get_job_result": {
                    "coordinator_batch_sql": coordinator_control_plane,
                    "local_submitted_job_metadata": true,
                },
                "cancel_job": {
                    "coordinator_first": coordinator_control_plane,
                    "local_submitted_job_fallback": true,
                },
                "list_executors": {
                    "coordinator_first": coordinator_control_plane,
                    "local_runtime_fallback": self.session.mode() != ExecutionMode::Distributed,
                },
            },
            "fallback_policy": if self.session.mode() == ExecutionMode::Distributed {
                "distributed mode treats coordinator failures as terminal unless a local submitted-job handle exists"
            } else if coordinator_control_plane {
                "coordinator-backed tools try the coordinator first, then report local session state with the coordinator error"
            } else {
                "local session APIs only"
            },
        })
    }

    fn has_coordinator_control_plane(&self) -> bool {
        self.session.coordinator_http_url().is_some()
    }

    async fn tool_execute_sql(&self, arguments: &Map<String, Value>) -> ToolResult<Value> {
        let query = required_string(arguments, "query")?;
        let read_only =
            optional_bool(arguments, "read_only")?.unwrap_or(!self.config.allow_write_sql);
        if read_only && !looks_read_only_sql(query) {
            return Err(ToolError::Unsupported(
                "execute_sql rejected non-read-only SQL; use explicit job/pipeline tools or set read_only=false with KRISHIV_MCP_ALLOW_WRITE_SQL=1".into(),
            ));
        }
        if !read_only && !self.config.allow_write_sql {
            return Err(ToolError::Unsupported(
                "write SQL through execute_sql is disabled; set KRISHIV_MCP_ALLOW_WRITE_SQL=1"
                    .into(),
            ));
        }
        let limit = capped_limit(optional_usize(arguments, "limit")?, self.config.max_rows);
        let timeout_ms =
            optional_u64(arguments, "timeout_ms")?.unwrap_or(self.config.default_timeout_ms);
        let execution_query = limited_query_for_sql(query, limit);
        let result = self
            .session
            .sql_with_timeout(execution_query, timeout_ms)
            .await?
            .collect_async()
            .await?;
        query_result_to_json(result.into_batches(), limit)
    }

    async fn tool_explain_sql(&self, arguments: &Map<String, Value>) -> ToolResult<Value> {
        let query = required_string(arguments, "query")?;
        let mode = optional_string(arguments, "mode")?.unwrap_or_else(|| "physical".into());
        let dataframe = self.session.sql_async(query).await?;
        let explain = match mode.trim().to_ascii_lowercase().as_str() {
            "logical" => dataframe.explain_logical(),
            "physical" => dataframe.explain_async().await?,
            "analyze" => {
                let explain = dataframe.explain_async().await?;
                let result = dataframe.collect_async().await?;
                format!(
                    "{explain}\n\nExecution statistics:\n  output_rows={}\n  result_rows={}",
                    result.row_count(),
                    result.row_count()
                )
            }
            other => {
                return Err(ToolError::InvalidArgument {
                    name: "mode",
                    expected: "logical, physical, or analyze",
                }
                .with_detail(other));
            }
        };
        Ok(json!({
            "mode": mode,
            "explain": explain,
            "runtime": self.runtime_info(),
        }))
    }

    fn tool_list_catalogs(&self) -> ToolResult<Value> {
        Ok(json!({
            "catalogs": [{
                "name": "session",
                "schemas": ["default"],
                "scope": "current Krishiv Session"
            }]
        }))
    }

    fn tool_list_tables(&self) -> ToolResult<Value> {
        Ok(json!({
            "tables": self.session.list_tables()?,
        }))
    }

    async fn tool_describe_table(&self, arguments: &Map<String, Value>) -> ToolResult<Value> {
        let table = required_string(arguments, "table")?;
        let sql = format!("SELECT * FROM {} LIMIT 0", quote_identifier(table));
        let schema = self.session.sql_async(sql).await?.schema()?;
        Ok(json!({
            "table": table,
            "schema": schema_to_json(&schema),
        }))
    }

    async fn tool_sample_table(&self, arguments: &Map<String, Value>) -> ToolResult<Value> {
        let table = required_string(arguments, "table")?;
        let limit = capped_limit(optional_usize(arguments, "limit")?, self.config.max_rows);
        let sql = format!("SELECT * FROM {} LIMIT {limit}", quote_identifier(table));
        let result = self.session.sql_async(sql).await?.collect_async().await?;
        query_result_to_json(result.into_batches(), limit)
    }

    async fn tool_submit_sql_job(&self, arguments: &Map<String, Value>) -> ToolResult<Value> {
        let sql = required_string(arguments, "sql")?;
        let status = self.session.submit_sql_background(sql)?;
        Ok(json!({
            "job_id": status.job_id(),
            "name": status.name(),
            "engine": status.engine().as_str(),
            "status": status.state().as_str(),
            "scope": "session_background",
        }))
    }

    async fn tool_list_jobs(&self) -> ToolResult<Value> {
        let mut coordinator_error = None;
        if self.has_coordinator_control_plane() {
            match self.session.list_jobs_remote().await {
                Ok(jobs) => {
                    return Ok(json!({
                        "scope": "coordinator",
                        "jobs": serde_json::to_value(jobs).map_err(|e| ToolError::Runtime(e.to_string()))?,
                    }));
                }
                Err(error) if self.session.mode() == ExecutionMode::Distributed => {
                    return Err(ToolError::from(error));
                }
                Err(error) => {
                    coordinator_error = Some(error.to_string());
                    tracing::debug!(error = %error, "remote job list failed; returning local registry");
                }
            }
        }
        let jobs = self
            .session
            .jobs()
            .into_iter()
            .map(|job| {
                json!({
                    "job_id": job.id().to_string(),
                    "name": job.name(),
                    "state": job.state().to_string(),
                })
            })
            .collect::<Vec<_>>();
        let submitted_sql_jobs = self
            .session
            .submitted_sql_jobs()
            .into_iter()
            .map(submitted_sql_job_to_json)
            .collect::<Vec<_>>();
        Ok(json!({
            "scope": "local_session",
            "coordinator_error": coordinator_error,
            "jobs": jobs,
            "submitted_sql_jobs": submitted_sql_jobs,
        }))
    }

    async fn tool_get_job_status(&self, arguments: &Map<String, Value>) -> ToolResult<Value> {
        let job_id = required_string(arguments, "job_id")?;
        let mut coordinator_status = None;
        if self.has_coordinator_control_plane() {
            match self.session.get_job_remote(job_id).await {
                Ok(Some(view)) => {
                    return Ok(json!({
                        "scope": "coordinator",
                        "job": serde_json::to_value(view).map_err(|e| ToolError::Runtime(e.to_string()))?,
                    }));
                }
                Ok(None) => {
                    coordinator_status = Some(json!({
                        "found": false,
                    }));
                }
                Err(error)
                    if self.session.mode() == ExecutionMode::Distributed
                        && self.session.submitted_sql_job_status(job_id).is_none() =>
                {
                    return Err(ToolError::from(error));
                }
                Err(error) => {
                    coordinator_status = Some(json!({
                        "error": error.to_string(),
                    }));
                }
            }
        }
        if let Some(status) = self.session.submitted_sql_job_status(job_id) {
            return Ok(json!({
                "scope": "local_session",
                "coordinator_status": coordinator_status,
                "job": submitted_sql_job_to_json(status),
            }));
        }
        let job =
            self.session.jobs().into_iter().find(|candidate| {
                candidate.id().to_string() == job_id || candidate.name() == job_id
            });
        match job {
            Some(job) => Ok(json!({
                "scope": "local_session",
                "coordinator_status": coordinator_status,
                "job": {
                    "job_id": job.id().to_string(),
                    "name": job.name(),
                    "state": job.state().to_string(),
                }
            })),
            None => Ok(json!({
                "scope": "local_session",
                "job_id": job_id,
                "state": "unknown",
                "coordinator_status": coordinator_status,
            })),
        }
    }

    async fn tool_get_job_result(&self, arguments: &Map<String, Value>) -> ToolResult<Value> {
        let job_id = required_string(arguments, "job_id")?;
        let limit = capped_limit(optional_usize(arguments, "limit")?, self.config.max_rows);
        let mut coordinator_error = None;
        if self.has_coordinator_control_plane() {
            match self.session.get_job_result_remote(job_id).await {
                Ok(result) => return coordinator_job_result_to_json(result, limit),
                Err(error)
                    if self.session.mode() == ExecutionMode::Distributed
                        && self.session.submitted_sql_job_status(job_id).is_none() =>
                {
                    return Err(ToolError::from(error));
                }
                Err(error) => {
                    coordinator_error = Some(error.to_string());
                }
            }
        }
        if let Some(status) = self.session.submitted_sql_job_status(job_id) {
            let mut result = submitted_sql_job_result_to_json(status);
            if let Some(obj) = result.as_object_mut() {
                obj.insert("coordinator_error".into(), json!(coordinator_error));
            }
            return Ok(result);
        }
        if let Some(error) = coordinator_error {
            return Err(ToolError::Unsupported(format!(
                "job result materialization for '{job_id}' is not available in this session; use execute_sql for local direct result queries: {error}"
            )));
        }
        match self.session.get_job_result_remote(job_id).await {
            Ok(result) => coordinator_job_result_to_json(result, limit),
            Err(error) => Err(ToolError::Unsupported(format!(
                "job result materialization for '{job_id}' is not available in this session; use execute_sql for local direct result queries: {error}"
            ))),
        }
    }

    async fn tool_cancel_job(&self, arguments: &Map<String, Value>) -> ToolResult<Value> {
        let job_id = required_string(arguments, "job_id")?;
        let mut coordinator_error = None;
        if self.has_coordinator_control_plane() {
            match self.session.cancel_job_remote(job_id).await {
                Ok(()) => {
                    return Ok(json!({
                        "cancelled": true,
                        "job_id": job_id,
                        "scope": "coordinator",
                    }));
                }
                Err(error) if self.session.submitted_sql_job_status(job_id).is_some() => {
                    tracing::debug!(error = %error, job_id, "remote cancel failed; trying local submitted job registry");
                    coordinator_error = Some(error.to_string());
                }
                Err(error) if self.session.mode() == ExecutionMode::Distributed => {
                    return Err(ToolError::from(error));
                }
                Err(error) => {
                    coordinator_error = Some(error.to_string());
                }
            }
        }

        if self.session.submitted_sql_job_status(job_id).is_some() {
            let status = self.session.cancel_submitted_sql_job(job_id)?;
            return Ok(json!({
                "cancelled": status.state() == krishiv_api::SubmittedSqlJobState::Cancelled,
                "job_id": job_id,
                "scope": "local_session",
                "coordinator_error": coordinator_error,
                "job": submitted_sql_job_to_json(status),
            }));
        }

        if let Some(error) = coordinator_error {
            return Err(ToolError::Runtime(error));
        }
        self.session.cancel_job_remote(job_id).await?;
        Ok(json!({
            "cancelled": true,
            "job_id": job_id,
            "scope": "coordinator",
        }))
    }

    async fn tool_submit_streaming_pipeline(
        &self,
        arguments: &Map<String, Value>,
    ) -> ToolResult<Value> {
        let sql = required_string(arguments, "sql")?;
        let compiled = self.session.compile_sql_job(sql)?;
        if compiled.engine != EngineKind::Streaming {
            return Err(ToolError::Unsupported(format!(
                "compiled SQL job '{}' is {}, not streaming",
                compiled.name, compiled.engine
            )));
        }
        let handle = self.session.submit(compiled).await?;
        Ok(json!({
            "job_id": handle.job_id().to_string(),
            "status": format!("{:?}", handle.status()),
        }))
    }

    async fn tool_get_streaming_job_status(
        &self,
        arguments: &Map<String, Value>,
    ) -> ToolResult<Value> {
        let job_id = required_string(arguments, "job_id")?;
        if let Some(stream) = self.session.continuous_stream_status(job_id).await? {
            return Ok(json!({
                "scope": "continuous_stream_registry",
                "job_id": stream.job_id(),
                "streaming": continuous_stream_status_to_json(&stream),
            }));
        }
        let mut status = self.tool_get_job_status(arguments).await?;
        if let Some(obj) = status.as_object_mut() {
            obj.insert(
                "streaming".into(),
                json!({
                    "registered": false,
                    "mode": format!("{:?}", self.session.mode()),
                    "checkpoint": "bounded streaming jobs use submit()/job status; continuous streams are exposed via create/feed/drain/list_continuous_streams",
                }),
            );
        }
        Ok(status)
    }

    async fn tool_list_continuous_streams(&self) -> ToolResult<Value> {
        let streams = self
            .session
            .list_continuous_stream_statuses()
            .await?
            .into_iter()
            .map(|stream| continuous_stream_status_to_json(&stream))
            .collect::<Vec<_>>();
        Ok(json!({
            "scope": "continuous_stream_registry",
            "streams": streams,
        }))
    }

    async fn tool_create_continuous_stream(
        &self,
        arguments: &Map<String, Value>,
    ) -> ToolResult<Value> {
        let job_name = required_string(arguments, "job_name")?;
        let query = required_string(arguments, "query")?;
        let stream = self
            .session
            .register_stream_job_sql(job_name, query)
            .await?;
        Ok(stream_job_to_json(stream))
    }

    async fn tool_feed_continuous_stream(
        &self,
        arguments: &Map<String, Value>,
    ) -> ToolResult<Value> {
        let job_name = required_string(arguments, "job_name")?;
        let query = required_string(arguments, "query")?;
        let batches = self
            .session
            .sql_async(query)
            .await?
            .collect_async()
            .await?
            .into_batches();
        let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        self.session.push_stream_job_input(job_name, batches)?;
        let stream = self.session.registered_stream_job(job_name);
        Ok(json!({
            "job_id": job_name,
            "source": stream.as_ref().and_then(|job| job.source()),
            "fed_rows": row_count,
        }))
    }

    async fn tool_drain_continuous_stream(
        &self,
        arguments: &Map<String, Value>,
    ) -> ToolResult<Value> {
        let job_name = required_string(arguments, "job_name")?;
        let limit = capped_limit(optional_usize(arguments, "limit")?, self.config.max_rows);
        let batches = self.session.poll_stream_job(job_name).await?;
        let stream = self.session.registered_stream_job(job_name);
        let mut result = query_result_to_json(batches, limit)?;
        if let Some(obj) = result.as_object_mut() {
            obj.insert("job_id".into(), json!(job_name));
            obj.insert(
                "source".into(),
                json!(stream.as_ref().and_then(|job| job.source())),
            );
        }
        Ok(result)
    }

    async fn tool_checkpoint_continuous_stream(
        &self,
        arguments: &Map<String, Value>,
    ) -> ToolResult<Value> {
        let job_name = required_string(arguments, "job_name")?;
        let checkpoint = self.session.checkpoint_continuous_stream(job_name).await?;
        Ok(continuous_stream_checkpoint_to_json(&checkpoint))
    }

    async fn tool_restore_continuous_stream(
        &self,
        arguments: &Map<String, Value>,
    ) -> ToolResult<Value> {
        let job_name = required_string(arguments, "job_name")?;
        let checkpoint_base64 = required_string(arguments, "checkpoint_base64")?;
        let snapshot_bytes = BASE64.decode(checkpoint_base64.as_bytes()).map_err(|_| {
            ToolError::InvalidArgument {
                name: "checkpoint_base64",
                expected: "valid base64-encoded checkpoint bytes",
            }
        })?;
        if self
            .session
            .continuous_stream_status(job_name)
            .await?
            .is_none()
            && let Some(query) = optional_string(arguments, "query")?
        {
            self.session
                .register_stream_job_sql(job_name, &query)
                .await?;
        }
        let status = self
            .session
            .restore_continuous_stream(job_name, &snapshot_bytes)
            .await?;
        Ok(continuous_stream_status_to_json(&status))
    }

    async fn tool_create_incremental_view(
        &self,
        arguments: &Map<String, Value>,
    ) -> ToolResult<Value> {
        let job_name = required_string(arguments, "job_name")?;
        let view_name = required_string(arguments, "view_name")?;
        let query = required_string(arguments, "query")?;
        let materialized = optional_bool(arguments, "materialized")?.unwrap_or(true);
        let schema = self.session.sql_async(query).await?.schema()?;
        let spec = IncrementalViewSpec {
            name: view_name.to_string(),
            body_sql: query.to_string(),
            output_schema: schema,
            is_materialized: materialized,
            is_recursive: false,
            lateness: Vec::new(),
        };
        let job = self.session.ivm(job_name).await?;
        job.register_view(spec).await?;
        Ok(json!({
            "job_id": job.job_id(),
            "view": view_name,
            "materialized": materialized,
        }))
    }

    async fn tool_feed_incremental_view(
        &self,
        arguments: &Map<String, Value>,
    ) -> ToolResult<Value> {
        let job_name = required_string(arguments, "job_name")?;
        let source = required_string(arguments, "source")?;
        let query = required_string(arguments, "query")?;
        let batches = self
            .session
            .sql_async(query)
            .await?
            .collect_async()
            .await?
            .into_batches();
        let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        let job = self.session.ivm(job_name).await?;
        job.feed_snapshot(source, &batches).await?;
        Ok(json!({
            "job_id": job.job_id(),
            "source": source,
            "fed_rows": row_count,
        }))
    }

    async fn tool_step_incremental_view(
        &self,
        arguments: &Map<String, Value>,
    ) -> ToolResult<Value> {
        let job_name = required_string(arguments, "job_name")?;
        let job = self.session.ivm(job_name).await?;
        let report = job.step().await?;
        Ok(json!({
            "job_id": job.job_id(),
            "tick": report.tick,
            "active_views": report.active_views,
            "total_output_rows": report.total_output_rows,
            "degraded_views": report.degraded_views,
            "errored_views": report.errored_views.into_iter().map(|error| {
                json!({
                    "view": error.view,
                    "kind": format!("{:?}", error.kind),
                    "message": error.message,
                })
            }).collect::<Vec<_>>(),
        }))
    }

    async fn tool_snapshot_incremental_view(
        &self,
        arguments: &Map<String, Value>,
    ) -> ToolResult<Value> {
        let job_name = required_string(arguments, "job_name")?;
        let view_name = required_string(arguments, "view_name")?;
        let limit = capped_limit(optional_usize(arguments, "limit")?, self.config.max_rows);
        let job = self.session.ivm(job_name).await?;
        match job.snapshot(view_name).await? {
            Some(batch) => query_result_to_json(vec![batch], limit),
            None => Ok(json!({
                "job_id": job.job_id(),
                "view": view_name,
                "rows": [],
                "row_count": 0,
            })),
        }
    }

    async fn tool_checkpoint_incremental_job(
        &self,
        arguments: &Map<String, Value>,
    ) -> ToolResult<Value> {
        let job_name = required_string(arguments, "job_name")?;
        let delta = optional_bool(arguments, "delta")?.unwrap_or(false);
        let job = self.session.ivm(job_name).await?;
        let bytes = if delta {
            job.enable_delta_checkpoints()?;
            job.checkpoint_delta().await?
        } else {
            job.checkpoint().await?
        };
        Ok(json!({
            "job_id": job.job_id(),
            "checkpoint_type": if delta { "delta" } else { "full" },
            "encoding": "base64",
            "byte_len": bytes.len(),
            "checkpoint_base64": BASE64.encode(&bytes),
        }))
    }

    async fn tool_restore_incremental_job(
        &self,
        arguments: &Map<String, Value>,
    ) -> ToolResult<Value> {
        let job_name = required_string(arguments, "job_name")?;
        let delta = optional_bool(arguments, "delta")?.unwrap_or(false);
        let checkpoint_base64 =
            required_string_alias(arguments, "checkpoint_base64", "checkpoint")?;
        let bytes = decode_checkpoint_base64(checkpoint_base64)?;
        let job = self.session.ivm(job_name).await?;
        if delta {
            job.restore_delta(&bytes).await?;
        } else {
            job.restore(&bytes).await?;
        }
        Ok(json!({
            "job_id": job.job_id(),
            "restored": true,
            "checkpoint_type": if delta { "delta" } else { "full" },
            "encoding": "base64",
            "byte_len": bytes.len(),
        }))
    }

    fn tool_list_connectors(&self) -> ToolResult<Value> {
        let connectors = default_registry()
            .descriptors()
            .into_iter()
            .map(|descriptor| {
                json!({
                    "kind": descriptor.kind.as_str(),
                    "role": connector_role_str(descriptor.role),
                    "maturity": descriptor.maturity.as_str(),
                    "capabilities": connector_capabilities_json(&descriptor.default_capabilities),
                    "sql_job_execution": connector_sql_job_execution(
                        descriptor.kind.as_str(),
                        descriptor.role,
                    ),
                })
            })
            .collect::<Vec<_>>();
        let registered_sinks = self
            .session
            .registered_sink_configs()
            .into_iter()
            .map(|config| {
                json!({
                    "name": config.name,
                    "kind": config.kind,
                    "role": "sink",
                    "sql_job_execution": connector_sql_job_execution(
                        config.kind.as_str(),
                        ConnectorRole::Sink,
                    ),
                })
            })
            .collect::<Vec<_>>();
        let registered_sources = self
            .session
            .registered_source_configs()
            .into_iter()
            .map(|config| {
                json!({
                    "name": config.name,
                    "kind": config.kind,
                    "role": "source",
                    "sql_job_execution": connector_sql_job_execution(
                        config.kind.as_str(),
                        ConnectorRole::Source,
                    ),
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "connectors": connectors,
            "registered_sources": registered_sources,
            "registered_sinks": registered_sinks,
        }))
    }

    fn tool_validate_connector_config(&self, arguments: &Map<String, Value>) -> ToolResult<Value> {
        let role = required_string(arguments, "role")?;
        let config = connector_config_from_args(arguments)?;
        let registry = default_registry();
        let result = match parse_connector_role(role)? {
            ConnectorRole::Source => registry.validate_source(&config),
            ConnectorRole::Sink => registry.validate_sink(&config),
            ConnectorRole::TwoPhaseSink => registry.validate_two_phase_sink(&config),
            // A vector sink is a member of the sink family; validate it as one
            // until the registry exposes a dedicated vector-sink validator.
            ConnectorRole::VectorSink => registry.validate_sink(&config),
        };
        match result {
            Ok(()) => {
                Ok(json!({ "valid": true, "name": config.name, "kind": config.kind, "role": role }))
            }
            Err(error) => Ok(
                json!({ "valid": false, "name": config.name, "kind": config.kind, "role": role, "error": error.to_string() }),
            ),
        }
    }

    async fn tool_register_source(&self, arguments: &Map<String, Value>) -> ToolResult<Value> {
        let name = required_string(arguments, "name")?;
        let kind = required_string(arguments, "kind")?;
        let properties = optional_object(arguments, "properties")?;
        let config = connector_config_from_args(arguments)?;
        match kind.trim().to_ascii_lowercase().as_str() {
            "parquet" => {
                let path = properties
                    .and_then(|obj| obj.get("path"))
                    .and_then(Value::as_str)
                    .ok_or(ToolError::MissingArgument("properties.path"))?;
                self.session
                    .register_parquet_async(name, Path::new(path))
                    .await?;
                self.session.register_source_config(config)?;
                Ok(json!({
                    "registered": true,
                    "name": name,
                    "kind": "parquet",
                    "path": path,
                    "scope": "session",
                }))
            }
            other => {
                self.session.register_source_config(config)?;
                Ok(json!({
                    "registered": true,
                    "name": name,
                    "kind": other,
                    "scope": "session",
                    "note": "registered as connector metadata; direct SQL table registration is currently parquet-only",
                }))
            }
        }
    }

    fn tool_register_sink(&self, arguments: &Map<String, Value>) -> ToolResult<Value> {
        let config = connector_config_from_args(arguments)?;
        self.session.register_sink_config(config.clone())?;
        Ok(json!({
            "registered": true,
            "name": config.name,
            "kind": config.kind,
            "scope": "session",
        }))
    }

    async fn tool_list_executors(&self) -> ToolResult<Value> {
        match self.session.list_executors_remote().await {
            Ok(executors) => Ok(json!({
                "scope": "coordinator",
                "executors": serde_json::to_value(executors)
                    .map_err(|e| ToolError::Runtime(e.to_string()))?,
            })),
            Err(error) if self.session.mode() != ExecutionMode::Distributed => Ok(json!({
                "scope": "local_runtime",
                "executors": [self.local_runtime_executor()],
                "note": format!("coordinator executor list unavailable: {error}"),
            })),
            Err(error) => Err(ToolError::Runtime(error.to_string())),
        }
    }

    fn local_runtime_executor(&self) -> Value {
        let runtime = self.session.execution_runtime();
        json!({
            "executor_id": "local-in-process",
            "host": "local",
            "slots": std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(1),
            "state": "Running",
            "runtime_mode": format!("{:?}", runtime.mode()),
            "placement": format!("{:?}", runtime.placement()),
            "uses_remote_execution": runtime.uses_remote_execution(),
        })
    }

    fn tool_get_metrics_summary(&self, arguments: &Map<String, Value>) -> ToolResult<Value> {
        let job_id = optional_string(arguments, "job_id")?;
        let local_jobs = self.session.jobs();
        Ok(json!({
            "job_id": job_id,
            "runtime": self.runtime_info(),
            "local_job_count": local_jobs.len(),
            "local_jobs_by_state": local_jobs.into_iter().fold(BTreeMap::<String, usize>::new(), |mut acc, job| {
                *acc.entry(job.state().to_string()).or_insert(0) += 1;
                acc
            }),
            "note": "Detailed runtime metrics stay in the metrics/telemetry subsystem; this MCP tool exposes the mode-aware summary currently available through Session."
        }))
    }
}

async fn serve_stdio(server: Arc<KrishivMcpServer>) -> McpResult<()> {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut lines = BufReader::new(stdin).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let message: Value = serde_json::from_str(&line)?;
        if let Some(response) = server.handle_json_rpc(message).await {
            let mut serialized = serde_json::to_vec(&response)?;
            serialized.push(b'\n');
            stdout.write_all(&serialized).await?;
            stdout.flush().await?;
        }
    }
    Ok(())
}

async fn serve_http(server: Arc<KrishivMcpServer>, addr: SocketAddr) -> McpResult<()> {
    let router = Router::new()
        .route("/healthz", get(http_health))
        .route("/mcp", post(http_mcp))
        .with_state(server);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "Krishiv MCP HTTP server listening");
    axum::serve(listener, router).await?;
    Ok(())
}

async fn http_health(State(server): State<Arc<KrishivMcpServer>>) -> impl IntoResponse {
    Json(server.health())
}

async fn http_mcp(
    State(server): State<Arc<KrishivMcpServer>>,
    Json(message): Json<Value>,
) -> impl IntoResponse {
    match server.handle_json_rpc(message).await {
        Some(response) => Json(response).into_response(),
        None => StatusCode::ACCEPTED.into_response(),
    }
}

fn parse_tool_call(params: Option<Value>) -> ToolResult<(String, Map<String, Value>)> {
    let Some(params) = params else {
        return Err(ToolError::InvalidArgument {
            name: "params",
            expected: "object with name and arguments",
        });
    };
    let Some(object) = params.as_object() else {
        return Err(ToolError::InvalidArgument {
            name: "params",
            expected: "object",
        });
    };
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or(ToolError::MissingArgument("name"))?
        .to_string();
    let arguments = object
        .get("arguments")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    Ok((name, arguments))
}

fn required_string<'a>(object: &'a Map<String, Value>, name: &'static str) -> ToolResult<&'a str> {
    object
        .get(name)
        .and_then(Value::as_str)
        .ok_or(ToolError::MissingArgument(name))
}

fn required_string_alias<'a>(
    object: &'a Map<String, Value>,
    name: &'static str,
    alias: &'static str,
) -> ToolResult<&'a str> {
    match object.get(name) {
        Some(Value::String(value)) => Ok(value),
        Some(Value::Null) | None => match object.get(alias) {
            Some(Value::String(value)) => Ok(value),
            Some(Value::Null) | None => Err(ToolError::MissingArgument(name)),
            Some(_) => Err(ToolError::InvalidArgument {
                name: alias,
                expected: "a string",
            }),
        },
        Some(_) => Err(ToolError::InvalidArgument {
            name,
            expected: "a string",
        }),
    }
}

fn optional_string(object: &Map<String, Value>, name: &'static str) -> ToolResult<Option<String>> {
    match object.get(name) {
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(ToolError::InvalidArgument {
            name,
            expected: "a string",
        }),
    }
}

fn optional_bool(object: &Map<String, Value>, name: &'static str) -> ToolResult<Option<bool>> {
    match object.get(name) {
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(ToolError::InvalidArgument {
            name,
            expected: "a boolean",
        }),
    }
}

fn optional_usize(object: &Map<String, Value>, name: &'static str) -> ToolResult<Option<usize>> {
    match object.get(name) {
        Some(Value::Number(value)) => value
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .map(Some)
            .ok_or(ToolError::InvalidArgument {
                name,
                expected: "a non-negative integer",
            }),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(ToolError::InvalidArgument {
            name,
            expected: "a non-negative integer",
        }),
    }
}

fn optional_u64(object: &Map<String, Value>, name: &'static str) -> ToolResult<Option<u64>> {
    match object.get(name) {
        Some(Value::Number(value)) => value.as_u64().map(Some).ok_or(ToolError::InvalidArgument {
            name,
            expected: "a non-negative integer",
        }),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(ToolError::InvalidArgument {
            name,
            expected: "a non-negative integer",
        }),
    }
}

fn optional_object<'a>(
    object: &'a Map<String, Value>,
    name: &'static str,
) -> ToolResult<Option<&'a Map<String, Value>>> {
    match object.get(name) {
        Some(Value::Object(value)) => Ok(Some(value)),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(ToolError::InvalidArgument {
            name,
            expected: "an object",
        }),
    }
}

fn capped_limit(limit: Option<usize>, max_rows: usize) -> usize {
    limit.unwrap_or(max_rows).min(max_rows)
}

fn decode_checkpoint_base64(raw: &str) -> ToolResult<Vec<u8>> {
    BASE64.decode(raw.trim()).map_err(|e| {
        ToolError::InvalidArgument {
            name: "checkpoint_base64",
            expected: "base64-encoded checkpoint bytes",
        }
        .with_detail(&e.to_string())
    })
}

fn looks_read_only_sql(query: &str) -> bool {
    let trimmed = query.trim_start();
    let first = trimmed
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    matches!(
        first.as_str(),
        "select" | "with" | "show" | "describe" | "explain"
    )
}

fn limited_query_for_sql(query: &str, limit: usize) -> String {
    let trimmed = query.trim();
    let without_semicolon = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    let first = without_semicolon
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if matches!(first.as_str(), "select" | "with") {
        format!("SELECT * FROM ({without_semicolon}) AS __krishiv_mcp_result LIMIT {limit}")
    } else {
        without_semicolon.to_string()
    }
}

fn query_result_to_json(batches: Vec<RecordBatch>, limit: usize) -> ToolResult<Value> {
    let schema = batches
        .iter()
        .find(|batch| batch.num_columns() > 0 || batch.num_rows() > 0)
        .map(RecordBatch::schema);
    let total_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    let mut rows = Vec::new();
    for batch in &batches {
        append_batch_rows(batch, limit, &mut rows)?;
        if rows.len() >= limit {
            break;
        }
    }
    Ok(json!({
        "row_count": total_rows,
        "returned_rows": rows.len(),
        "truncated": total_rows > rows.len(),
        "schema": schema.as_ref().map(schema_to_json),
        "rows": rows,
    }))
}

fn coordinator_job_result_to_json(
    result: krishiv_api::CoordinatorBatchSqlJobResult,
    limit: usize,
) -> ToolResult<Value> {
    match result {
        krishiv_api::CoordinatorBatchSqlJobResult::Succeeded { job_id, batches } => {
            let mut result = query_result_to_json(batches, limit)?;
            if let Some(obj) = result.as_object_mut() {
                obj.insert("job_id".into(), json!(job_id));
                obj.insert("state".into(), json!("Succeeded"));
                obj.insert("scope".into(), json!("coordinator_batch_sql"));
            }
            Ok(result)
        }
        krishiv_api::CoordinatorBatchSqlJobResult::Pending { job_id, state } => Ok(json!({
            "scope": "coordinator_batch_sql",
            "job_id": job_id,
            "state": state,
            "result_available": false,
        })),
        krishiv_api::CoordinatorBatchSqlJobResult::Failed { job_id, error } => Ok(json!({
            "scope": "coordinator_batch_sql",
            "job_id": job_id,
            "state": "Failed",
            "result_available": false,
            "error": error,
        })),
        krishiv_api::CoordinatorBatchSqlJobResult::Cancelled { job_id, error } => Ok(json!({
            "scope": "coordinator_batch_sql",
            "job_id": job_id,
            "state": "Cancelled",
            "result_available": false,
            "error": error,
        })),
    }
}

fn submitted_sql_job_to_json(status: SubmittedSqlJobStatus) -> Value {
    json!({
        "job_id": status.job_id(),
        "name": status.name(),
        "engine": status.engine().as_str(),
        "state": status.state().as_str(),
        "error": status.error(),
    })
}

fn submitted_sql_job_result_to_json(status: SubmittedSqlJobStatus) -> Value {
    json!({
        "scope": "local_submitted_sql",
        "job": submitted_sql_job_to_json(status.clone()),
        "job_id": status.job_id(),
        "state": status.state().as_str(),
        "result_available": status.state() == krishiv_api::SubmittedSqlJobState::Succeeded,
        "materialized_rows": false,
        "rows": [],
        "row_count": 0,
        "note": "submit_sql_job runs a SQL pipeline into its declared sink; use execute_sql for direct local row materialization.",
    })
}

fn append_batch_rows(batch: &RecordBatch, limit: usize, rows: &mut Vec<Value>) -> ToolResult<()> {
    let schema = batch.schema();
    for row_index in 0..batch.num_rows() {
        if rows.len() >= limit {
            return Ok(());
        }
        let mut row = Map::new();
        for (column_index, field) in schema.fields().iter().enumerate() {
            let Some(column) = batch.columns().get(column_index) else {
                continue;
            };
            let value = if column.is_null(row_index) {
                Value::Null
            } else {
                Value::String(
                    arrow::util::display::array_value_to_string(column.as_ref(), row_index)
                        .map_err(|e| ToolError::Runtime(e.to_string()))?,
                )
            };
            row.insert(field.name().clone(), value);
        }
        rows.push(Value::Object(row));
    }
    Ok(())
}

fn schema_to_json(schema: &SchemaRef) -> Value {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            json!({
                "name": field.name(),
                "data_type": field.data_type().to_string(),
                "nullable": field.is_nullable(),
            })
        })
        .collect::<Vec<_>>();
    json!({ "fields": fields })
}

fn stream_job_to_json(job: krishiv_api::RegisteredContinuousStreamJob) -> Value {
    json!({
        "job_id": job.name(),
        "source": job.source(),
        "spec": stream_spec_to_json(job.spec()),
    })
}

fn continuous_stream_status_to_json(status: &krishiv_api::ContinuousStreamStatus) -> Value {
    json!({
        "registered": true,
        "job_id": status.job_id(),
        "state": status.state(),
        "source": status.source(),
        "spec": stream_spec_to_json(status.spec()),
        "uses_remote_execution": status.uses_remote_execution(),
        "task_count": status.task_count(),
        "assigned_task_count": status.assigned_task_count(),
        "running_task_count": status.running_task_count(),
        "succeeded_task_count": status.succeeded_task_count(),
        "failed_task_count": status.failed_task_count(),
        "pending_input_batches": status.pending_input_batches(),
        "last_watermark_ms": status.last_watermark_ms(),
        "persisted_watermark_ms": status.persisted_watermark_ms(),
        "snapshot_available": status.snapshot_available(),
        "cycle_in_flight": status.cycle_in_flight(),
    })
}

fn continuous_stream_checkpoint_to_json(
    checkpoint: &krishiv_api::ContinuousStreamCheckpoint,
) -> Value {
    json!({
        "job_id": checkpoint.job_id(),
        "source": checkpoint.source(),
        "spec": stream_spec_to_json(checkpoint.spec()),
        "snapshot_available": checkpoint.snapshot_available(),
        "checkpoint_base64": checkpoint.snapshot_bytes().map(|bytes| BASE64.encode(bytes)),
        "byte_len": checkpoint.snapshot_bytes().map(|bytes| bytes.len()).unwrap_or(0),
        "watermark_ms": checkpoint.watermark_ms(),
        "uses_remote_execution": checkpoint.uses_remote_execution(),
    })
}

fn stream_spec_to_json(spec: &krishiv_api::LocalWindowExecutionSpec) -> Value {
    json!({
        "key_column": spec.key_column.clone(),
        "key_column_type": spec.key_column_type.clone(),
        "event_time_column": spec.event_time_column.clone(),
        "watermark_lag_ms": spec.watermark_lag_ms,
        "window_kind": stream_window_kind_to_json(&spec.window_kind),
        "window_size_ms": spec.window_size_ms,
        "aggregates": spec.agg_exprs.iter().map(|agg| {
            json!({
                "function": format!("{:?}", agg.function),
                "input_column": agg.input_column.clone(),
                "output_column": agg.output_column.clone(),
            })
        }).collect::<Vec<_>>(),
        "state_ttl_ms": spec.state_ttl_ms,
        "allowed_lateness_ms": spec.allowed_lateness_ms,
        "source_watermark_lags": spec.source_watermark_lags.clone(),
        "source_id_column": spec.source_id_column.clone(),
        "window_timezone": spec.window_timezone.clone(),
    })
}

fn stream_window_kind_to_json(kind: &krishiv_api::LocalWindowKind) -> Value {
    match kind {
        krishiv_api::LocalWindowKind::Tumbling => json!({ "kind": "tumbling" }),
        krishiv_api::LocalWindowKind::Sliding { slide_ms } => {
            json!({ "kind": "sliding", "slide_ms": slide_ms })
        }
        krishiv_api::LocalWindowKind::Session { gap_ms } => {
            json!({ "kind": "session", "gap_ms": gap_ms })
        }
        krishiv_api::LocalWindowKind::Count { size, slide } => {
            json!({ "kind": "count", "size": size, "slide": slide })
        }
    }
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn connector_config_from_args(arguments: &Map<String, Value>) -> ToolResult<ConnectorConfig> {
    let name = required_string(arguments, "name")?;
    let kind = required_string(arguments, "kind")?;
    let mut config = ConnectorConfig::new(name, kind);
    if let Some(properties) = optional_object(arguments, "properties")? {
        for (key, value) in properties {
            let Some(value) = value.as_str() else {
                return Err(ToolError::InvalidArgument {
                    name: "properties",
                    expected: "an object of string values",
                });
            };
            config = config.with_property(key, value);
        }
    }
    Ok(config)
}

fn parse_connector_role(role: &str) -> ToolResult<ConnectorRole> {
    match role.trim().to_ascii_lowercase().as_str() {
        "source" => Ok(ConnectorRole::Source),
        "sink" => Ok(ConnectorRole::Sink),
        "two_phase_sink" | "two-phase-sink" | "two_phase" => Ok(ConnectorRole::TwoPhaseSink),
        "vector_sink" | "vector-sink" => Ok(ConnectorRole::VectorSink),
        _ => Err(ToolError::InvalidArgument {
            name: "role",
            expected: "source, sink, or two_phase_sink",
        }),
    }
}

fn connector_role_str(role: ConnectorRole) -> &'static str {
    match role {
        ConnectorRole::Source => "source",
        ConnectorRole::Sink => "sink",
        ConnectorRole::TwoPhaseSink => "two_phase_sink",
        ConnectorRole::VectorSink => "vector_sink",
    }
}

fn connector_capabilities_json(capabilities: &krishiv_connectors::ConnectorCapabilities) -> Value {
    json!({
        "bounded": capabilities.is_bounded(),
        "unbounded": capabilities.is_unbounded(),
        "rewindable": capabilities.is_rewindable(),
        "transactional": capabilities.is_transactional(),
        "idempotent": capabilities.is_idempotent(),
        "checkpoint": capabilities.is_checkpoint_capable(),
        "two_phase_commit": capabilities.is_two_phase_commit_capable(),
        "delivery_guarantee": format!("{:?}", capabilities.delivery_guarantee()),
    })
}

fn connector_sql_job_execution(kind: &str, role: ConnectorRole) -> Value {
    let kind = kind.trim().to_ascii_lowercase();
    let executable = match role {
        ConnectorRole::Source => matches!(
            kind.as_str(),
            "parquet" | "parquet-directory" | "csv" | "json" | "ndjson" | "s3" | "s3-prefix"
        ),
        ConnectorRole::Sink => {
            matches!(kind.as_str(), "parquet" | "csv" | "json" | "ndjson" | "s3")
        }
        ConnectorRole::TwoPhaseSink => false,
        // Vector sinks are not driven through the batch SQL-job execution path.
        ConnectorRole::VectorSink => false,
    };
    let modes = match (role, kind.as_str()) {
        (ConnectorRole::Source, "parquet") => json!(["embedded", "single-node", "distributed"]),
        (ConnectorRole::Source, _) if executable => {
            json!(["embedded", "single-node", "distributed"])
        }
        (ConnectorRole::Sink, _) if executable => {
            json!(["embedded", "single-node", "distributed"])
        }
        _ => json!([]),
    };
    let note = match (role, kind.as_str(), executable) {
        (ConnectorRole::Source, "parquet", true) => {
            "distributed batch jobs register parquet paths with the coordinator"
        }
        (ConnectorRole::Source, _, true) => {
            "bounded non-parquet sources are drained through local connector I/O, spilled to temporary parquet, and shipped to the configured remote runtime for distributed batch computation"
        }
        (ConnectorRole::Sink, _, true) => {
            "sinks are opened by the submitting process after the query result stream is produced"
        }
        _ => {
            "validated registry metadata only until the engine runtime has a matching source/sink provider"
        }
    };
    json!({
        "supported": executable,
        "modes": modes,
        "note": note,
    })
}

fn tool(name: &str, title: &str, description: &str, input_schema: Value) -> Value {
    json!({
        "name": name,
        "title": title,
        "description": description,
        "inputSchema": input_schema,
    })
}

fn resource(uri: &str, name: &str, description: &str) -> Value {
    json!({
        "uri": uri,
        "name": name,
        "description": description,
        "mimeType": "application/json",
    })
}

fn object_schema<const P: usize, const R: usize>(
    properties: [(&str, Value); P],
    required: [&str; R],
) -> Value {
    let props = properties
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect::<Map<_, _>>();
    let required = required.into_iter().map(str::to_string).collect::<Vec<_>>();
    json!({
        "type": "object",
        "properties": props,
        "required": required,
    })
}

fn tool_result(value: Value, is_error: bool) -> Value {
    json!({
        "content": [{
            "type": "text",
            "text": value_to_pretty_json(&value),
        }],
        "structuredContent": value,
        "isError": is_error,
    })
}

fn value_to_pretty_json(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn json_rpc_error(id: Option<Value>, code: i64, message: &str, data: Option<Value>) -> Value {
    let mut error = Map::new();
    error.insert("code".into(), json!(code));
    error.insert("message".into(), json!(message));
    if let Some(data) = data {
        error.insert("data".into(), data);
    }
    json!({
        "jsonrpc": "2.0",
        "id": id.unwrap_or(Value::Null),
        "error": Value::Object(error),
    })
}

trait ToolErrorDetail {
    fn with_detail(self, detail: &str) -> Self;
}

impl ToolErrorDetail for ToolError {
    fn with_detail(self, detail: &str) -> Self {
        match self {
            Self::InvalidArgument { name, expected } => Self::Runtime(format!(
                "argument '{name}' must be {expected}; got '{detail}'"
            )),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{KrishivMcpServer, McpConfig, coordinator_job_result_to_json};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_api::Session;
    use parquet::arrow::ArrowWriter;
    use serde_json::{Value, json};
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn one_row_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "answer",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42]))])
            .expect("record batch")
    }

    fn stream_events_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "b"])) as _,
                Arc::new(Int64Array::from(vec![1_000, 2_000, 13_000])) as _,
            ],
        )
        .expect("record batch")
    }

    fn write_one_row_parquet(path: &std::path::Path) -> Result<(), String> {
        let batch = one_row_batch();
        let file = File::create(path).map_err(|e| e.to_string())?;
        let mut writer =
            ArrowWriter::try_new(file, batch.schema(), None).map_err(|e| e.to_string())?;
        writer.write(&batch).map_err(|e| e.to_string())?;
        writer.close().map_err(|e| e.to_string())?;
        Ok(())
    }

    fn write_stream_events_parquet(path: &std::path::Path) -> Result<(), String> {
        let batch = stream_events_batch();
        let file = File::create(path).map_err(|e| e.to_string())?;
        let mut writer =
            ArrowWriter::try_new(file, batch.schema(), None).map_err(|e| e.to_string())?;
        writer.write(&batch).map_err(|e| e.to_string())?;
        writer.close().map_err(|e| e.to_string())?;
        Ok(())
    }

    #[tokio::test]
    async fn tools_list_includes_runtime_and_sql_tools() -> Result<(), String> {
        let server = KrishivMcpServer::new(
            Session::builder().build().map_err(|e| e.to_string())?,
            McpConfig::default(),
        );
        let response = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list"
            }))
            .await
            .ok_or_else(|| "expected response".to_string())?;
        let tools = response
            .pointer("/result/tools")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("tools/list response missing tools: {response}"))?;
        let names = tools
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert!(names.contains(&"krishiv_health"));
        assert!(names.contains(&"deployment_capabilities"));
        assert!(names.contains(&"execute_sql"));
        assert!(names.contains(&"create_incremental_view"));
        assert!(names.contains(&"checkpoint_incremental_job"));
        assert!(names.contains(&"restore_incremental_job"));
        assert!(names.contains(&"create_continuous_stream"));
        assert!(names.contains(&"feed_continuous_stream"));
        assert!(names.contains(&"drain_continuous_stream"));
        assert!(names.contains(&"checkpoint_continuous_stream"));
        assert!(names.contains(&"restore_continuous_stream"));
        Ok(())
    }

    #[tokio::test]
    async fn deployment_capabilities_report_single_node_control_plane() -> Result<(), String> {
        let session = Session::builder()
            .with_local_cluster("http://coord:50051")
            .with_coordinator_http("http://coord:2002")
            .build()
            .map_err(|e| e.to_string())?;
        let server = KrishivMcpServer::new(session, McpConfig::default());
        let response = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "deployment_capabilities",
                    "arguments": {}
                }
            }))
            .await
            .ok_or_else(|| "expected response".to_string())?;

        assert_eq!(
            response.pointer("/result/structuredContent/coordinator/control_plane_configured"),
            Some(&json!(true))
        );
        assert_eq!(
            response.pointer(
                "/result/structuredContent/control_plane_tools/list_jobs/coordinator_first"
            ),
            Some(&json!(true))
        );
        assert_eq!(
            response
                .pointer("/result/structuredContent/control_plane_tools/list_jobs/local_fallback"),
            Some(&json!(true))
        );
        Ok(())
    }

    #[tokio::test]
    async fn execute_sql_returns_structured_rows() -> Result<(), String> {
        let server = KrishivMcpServer::new(
            Session::builder().build().map_err(|e| e.to_string())?,
            McpConfig::default(),
        );
        let response = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "execute_sql",
                    "arguments": {
                        "query": "SELECT 42 AS answer",
                        "limit": 10
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected response".to_string())?;
        let is_error = response
            .pointer("/result/isError")
            .and_then(Value::as_bool)
            .ok_or_else(|| format!("tools/call response missing isError: {response}"))?;
        assert!(!is_error, "execute_sql returned error: {response}");
        let returned_rows = response
            .pointer("/result/structuredContent/returned_rows")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("execute_sql missing returned_rows: {response}"))?;
        assert_eq!(returned_rows, 1);
        Ok(())
    }

    #[tokio::test]
    async fn register_sink_stores_validated_session_sink() -> Result<(), String> {
        let server = KrishivMcpServer::new(
            Session::builder().build().map_err(|e| e.to_string())?,
            McpConfig::default(),
        );
        let response = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "register_sink",
                    "arguments": {
                        "name": "out",
                        "kind": "parquet",
                        "properties": {
                            "path": "/tmp/out.parquet"
                        }
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected register response".to_string())?;
        let is_error = response
            .pointer("/result/isError")
            .and_then(Value::as_bool)
            .ok_or_else(|| format!("register_sink response missing isError: {response}"))?;
        assert!(!is_error, "register_sink returned error: {response}");

        let list = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "list_connectors",
                    "arguments": {}
                }
            }))
            .await
            .ok_or_else(|| "expected list response".to_string())?;
        assert_eq!(
            list.pointer("/result/structuredContent/registered_sinks/0/name"),
            Some(&json!("out"))
        );
        assert_eq!(
            list.pointer("/result/structuredContent/registered_sinks/0/kind"),
            Some(&json!("parquet"))
        );
        assert_eq!(
            list.pointer(
                "/result/structuredContent/registered_sinks/0/sql_job_execution/supported"
            ),
            Some(&json!(true))
        );
        Ok(())
    }

    #[tokio::test]
    async fn list_connectors_marks_csv_sources_distributed_executable() -> Result<(), String> {
        let server = KrishivMcpServer::new(
            Session::builder().build().map_err(|e| e.to_string())?,
            McpConfig::default(),
        );
        let response = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "list_connectors",
                    "arguments": {}
                }
            }))
            .await
            .ok_or_else(|| "expected list response".to_string())?;
        let connectors = response
            .pointer("/result/structuredContent/connectors")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("list_connectors missing connectors: {response}"))?;
        let csv_source = connectors
            .iter()
            .find(|connector| {
                connector.get("kind").and_then(Value::as_str) == Some("csv")
                    && connector.get("role").and_then(Value::as_str) == Some("source")
            })
            .ok_or_else(|| format!("csv source connector not listed: {response}"))?;
        assert_eq!(
            csv_source.pointer("/sql_job_execution/supported"),
            Some(&json!(true))
        );
        let modes = csv_source
            .pointer("/sql_job_execution/modes")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("csv source missing execution modes: {csv_source}"))?;
        assert!(modes.contains(&json!("distributed")));
        Ok(())
    }

    #[test]
    fn get_job_result_json_includes_coordinator_rows() -> Result<(), String> {
        let response = coordinator_job_result_to_json(
            krishiv_api::CoordinatorBatchSqlJobResult::Succeeded {
                job_id: "job-result".to_owned(),
                batches: vec![one_row_batch()],
            },
            10,
        )
        .map_err(|e| e.to_string())?;
        assert_eq!(response.pointer("/state"), Some(&json!("Succeeded")));
        assert_eq!(response.pointer("/rows/0/answer"), Some(&json!("42")));
        Ok(())
    }

    #[tokio::test]
    async fn get_job_result_uses_local_submitted_job_registry() -> Result<(), String> {
        let server = KrishivMcpServer::new(
            Session::builder().build().map_err(|e| e.to_string())?,
            McpConfig::default(),
        );
        let dir = tempdir().map_err(|e| e.to_string())?;
        let input = dir.path().join("input.parquet");
        write_one_row_parquet(&input)?;
        let sql = format!(
            "CREATE SOURCE orders FROM parquet(path='{}'); \
             CREATE SOURCE bad AS SELECT missing_column FROM orders; \
             CREATE SINK out FROM bad INTO parquet(path='{}');",
            input.display(),
            dir.path().join("out.parquet").display()
        );

        let submit = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "submit_sql_job",
                    "arguments": { "sql": sql }
                }
            }))
            .await
            .ok_or_else(|| "expected submit response".to_string())?;
        assert_eq!(
            submit.pointer("/result/structuredContent/scope"),
            Some(&json!("session_background"))
        );

        for _ in 0..100 {
            let result = server
                .handle_json_rpc(json!({
                    "jsonrpc": "2.0",
                    "id": 6,
                    "method": "tools/call",
                    "params": {
                        "name": "get_job_result",
                        "arguments": { "job_id": "out" }
                    }
                }))
                .await
                .ok_or_else(|| "expected result response".to_string())?;
            assert_eq!(
                result.pointer("/result/structuredContent/scope"),
                Some(&json!("local_submitted_sql"))
            );
            if result.pointer("/result/structuredContent/state") == Some(&json!("failed")) {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Err("local submitted SQL job did not fail within timeout".into())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancel_job_uses_local_submitted_job_registry() -> Result<(), String> {
        let server = KrishivMcpServer::new(
            Session::builder().build().map_err(|e| e.to_string())?,
            McpConfig::default(),
        );
        let dir = tempdir().map_err(|e| e.to_string())?;
        let sql = format!(
            "CREATE SOURCE orders FROM parquet(path='{}'); \
             CREATE SINK out FROM orders INTO parquet(path='{}');",
            dir.path().join("missing.parquet").display(),
            dir.path().join("out.parquet").display()
        );

        server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "submit_sql_job",
                    "arguments": { "sql": sql }
                }
            }))
            .await
            .ok_or_else(|| "expected submit response".to_string())?;
        let cancel = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 8,
                "method": "tools/call",
                "params": {
                    "name": "cancel_job",
                    "arguments": { "job_id": "out" }
                }
            }))
            .await
            .ok_or_else(|| "expected cancel response".to_string())?;

        assert_eq!(
            cancel.pointer("/result/structuredContent/scope"),
            Some(&json!("local_session"))
        );
        assert_eq!(
            cancel.pointer("/result/structuredContent/job/state"),
            Some(&json!("cancelled"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn submit_sql_job_resolves_registered_source_and_sink_names() -> Result<(), String> {
        let server = KrishivMcpServer::new(
            Session::builder().build().map_err(|e| e.to_string())?,
            McpConfig::default(),
        );
        let dir = tempdir().map_err(|e| e.to_string())?;
        let input = dir.path().join("input.parquet");
        let output = dir.path().join("output.parquet");
        write_one_row_parquet(&input)?;

        let register_source = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/call",
                "params": {
                    "name": "register_source",
                    "arguments": {
                        "name": "orders_input",
                        "kind": "parquet",
                        "properties": { "path": input.display().to_string() }
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected source registration response".to_string())?;
        assert_eq!(
            register_source.pointer("/result/isError"),
            Some(&json!(false))
        );
        let register_sink = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 10,
                "method": "tools/call",
                "params": {
                    "name": "register_sink",
                    "arguments": {
                        "name": "orders_output",
                        "kind": "parquet",
                        "properties": { "path": output.display().to_string() }
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected sink registration response".to_string())?;
        assert_eq!(
            register_sink.pointer("/result/isError"),
            Some(&json!(false))
        );

        let sql = "
            CREATE SOURCE orders FROM orders_input;
            CREATE SINK out FROM orders INTO orders_output;
        ";
        let submit = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 11,
                "method": "tools/call",
                "params": {
                    "name": "submit_sql_job",
                    "arguments": { "sql": sql }
                }
            }))
            .await
            .ok_or_else(|| "expected submit response".to_string())?;
        assert_eq!(submit.pointer("/result/isError"), Some(&json!(false)));

        for _ in 0..100 {
            let result = server
                .handle_json_rpc(json!({
                    "jsonrpc": "2.0",
                    "id": 12,
                    "method": "tools/call",
                    "params": {
                        "name": "get_job_result",
                        "arguments": { "job_id": "out" }
                    }
                }))
                .await
                .ok_or_else(|| "expected result response".to_string())?;
            if result.pointer("/result/structuredContent/state") == Some(&json!("succeeded")) {
                assert!(output.exists(), "registered parquet sink should be written");
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Err("registered connector SQL job did not succeed within timeout".into())
    }

    #[tokio::test]
    async fn continuous_stream_tools_register_feed_and_drain() -> Result<(), String> {
        let server = KrishivMcpServer::new(
            Session::builder().build().map_err(|e| e.to_string())?,
            McpConfig::default(),
        );
        let dir = tempdir().map_err(|e| e.to_string())?;
        let input = dir.path().join("events.parquet");
        write_stream_events_parquet(&input)?;

        let register_source = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 20,
                "method": "tools/call",
                "params": {
                    "name": "register_source",
                    "arguments": {
                        "name": "events_input",
                        "kind": "parquet",
                        "properties": { "path": input.display().to_string() }
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected source registration response".to_string())?;
        assert_eq!(
            register_source.pointer("/result/isError"),
            Some(&json!(false))
        );

        let create = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 21,
                "method": "tools/call",
                "params": {
                    "name": "create_continuous_stream",
                    "arguments": {
                        "job_name": "windowed_events",
                        "query": "SELECT user_id, COUNT(*) AS count \
                                  FROM TUMBLE(TABLE events_input, DESCRIPTOR(ts), 10000) \
                                  GROUP BY user_id, window_start, window_end"
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected create stream response".to_string())?;
        assert_eq!(create.pointer("/result/isError"), Some(&json!(false)));
        assert_eq!(
            create.pointer("/result/structuredContent/source"),
            Some(&json!("events_input"))
        );
        assert_eq!(
            create.pointer("/result/structuredContent/spec/window_kind/kind"),
            Some(&json!("tumbling"))
        );

        let status = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 22,
                "method": "tools/call",
                "params": {
                    "name": "get_streaming_job_status",
                    "arguments": { "job_id": "windowed_events" }
                }
            }))
            .await
            .ok_or_else(|| "expected stream status response".to_string())?;
        assert_eq!(
            status.pointer("/result/structuredContent/streaming/registered"),
            Some(&json!(true))
        );

        let feed = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 23,
                "method": "tools/call",
                "params": {
                    "name": "feed_continuous_stream",
                    "arguments": {
                        "job_name": "windowed_events",
                        "query": "SELECT user_id, ts FROM events_input"
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected feed stream response".to_string())?;
        assert_eq!(feed.pointer("/result/isError"), Some(&json!(false)));
        assert_eq!(
            feed.pointer("/result/structuredContent/fed_rows"),
            Some(&json!(3))
        );

        let drain = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 24,
                "method": "tools/call",
                "params": {
                    "name": "drain_continuous_stream",
                    "arguments": {
                        "job_name": "windowed_events",
                        "limit": 10
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected drain stream response".to_string())?;
        assert_eq!(
            drain.pointer("/result/isError"),
            Some(&json!(false)),
            "drain response: {drain}"
        );
        let row_count = drain
            .pointer("/result/structuredContent/row_count")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("drain missing row_count: {drain}"))?;
        assert!(row_count > 0, "drain should produce window output: {drain}");

        let checkpoint = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 25,
                "method": "tools/call",
                "params": {
                    "name": "checkpoint_continuous_stream",
                    "arguments": { "job_name": "windowed_events" }
                }
            }))
            .await
            .ok_or_else(|| "expected checkpoint stream response".to_string())?;
        assert_eq!(
            checkpoint.pointer("/result/isError"),
            Some(&json!(false)),
            "checkpoint response: {checkpoint}"
        );
        let checkpoint_len = checkpoint
            .pointer("/result/structuredContent/byte_len")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("checkpoint missing byte_len: {checkpoint}"))?;
        assert!(
            checkpoint_len > 0,
            "checkpoint should export bytes after at least one drain: {checkpoint}"
        );

        let list = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 26,
                "method": "tools/call",
                "params": {
                    "name": "list_continuous_streams",
                    "arguments": {}
                }
            }))
            .await
            .ok_or_else(|| "expected list streams response".to_string())?;
        assert_eq!(
            list.pointer("/result/structuredContent/streams/0/job_id"),
            Some(&json!("windowed_events"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn continuous_stream_restore_round_trips_checkpoint_base64() -> Result<(), String> {
        let server = KrishivMcpServer::new(
            Session::builder().build().map_err(|e| e.to_string())?,
            McpConfig::default(),
        );
        let dir = tempdir().map_err(|e| e.to_string())?;
        let input = dir.path().join("events.parquet");
        write_stream_events_parquet(&input)?;

        server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 30,
                "method": "tools/call",
                "params": {
                    "name": "register_source",
                    "arguments": {
                        "name": "events_input",
                        "kind": "parquet",
                        "properties": { "path": input.display().to_string() }
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected source registration response".to_string())?;
        server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 31,
                "method": "tools/call",
                "params": {
                    "name": "create_continuous_stream",
                    "arguments": {
                        "job_name": "restore_stream",
                        "query": "SELECT user_id, COUNT(*) AS count \
                                  FROM TUMBLE(TABLE events_input, DESCRIPTOR(ts), 10000) \
                                  GROUP BY user_id, window_start, window_end"
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected create response".to_string())?;

        for id in [32_u64, 34_u64] {
            let feed = server
                .handle_json_rpc(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": "tools/call",
                    "params": {
                        "name": "feed_continuous_stream",
                        "arguments": {
                            "job_name": "restore_stream",
                            "query": "SELECT user_id, ts FROM events_input"
                        }
                    }
                }))
                .await
                .ok_or_else(|| "expected feed response".to_string())?;
            assert_eq!(feed.pointer("/result/isError"), Some(&json!(false)));

            let drain = server
                .handle_json_rpc(json!({
                    "jsonrpc": "2.0",
                    "id": id + 1,
                    "method": "tools/call",
                    "params": {
                        "name": "drain_continuous_stream",
                        "arguments": {
                            "job_name": "restore_stream",
                            "limit": 10
                        }
                    }
                }))
                .await
                .ok_or_else(|| "expected drain response".to_string())?;
            assert_eq!(drain.pointer("/result/isError"), Some(&json!(false)));
        }

        let checkpoint1 = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 36,
                "method": "tools/call",
                "params": {
                    "name": "checkpoint_continuous_stream",
                    "arguments": { "job_name": "restore_stream" }
                }
            }))
            .await
            .ok_or_else(|| "expected checkpoint response".to_string())?;
        let checkpoint1_base64 = checkpoint1
            .pointer("/result/structuredContent/checkpoint_base64")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("checkpoint missing base64: {checkpoint1}"))?
            .to_string();

        let feed_again = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 37,
                "method": "tools/call",
                "params": {
                    "name": "feed_continuous_stream",
                    "arguments": {
                        "job_name": "restore_stream",
                        "query": "SELECT user_id, ts FROM events_input"
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected feed response".to_string())?;
        assert_eq!(feed_again.pointer("/result/isError"), Some(&json!(false)));
        let drain_again = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 38,
                "method": "tools/call",
                "params": {
                    "name": "drain_continuous_stream",
                    "arguments": {
                        "job_name": "restore_stream",
                        "limit": 10
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected drain response".to_string())?;
        assert_eq!(drain_again.pointer("/result/isError"), Some(&json!(false)));

        let checkpoint2 = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 39,
                "method": "tools/call",
                "params": {
                    "name": "checkpoint_continuous_stream",
                    "arguments": { "job_name": "restore_stream" }
                }
            }))
            .await
            .ok_or_else(|| "expected second checkpoint response".to_string())?;
        let checkpoint2_base64 = checkpoint2
            .pointer("/result/structuredContent/checkpoint_base64")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("checkpoint missing base64: {checkpoint2}"))?;
        assert_ne!(checkpoint1_base64, checkpoint2_base64);

        let restore = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 40,
                "method": "tools/call",
                "params": {
                    "name": "restore_continuous_stream",
                    "arguments": {
                        "job_name": "restore_stream",
                        "checkpoint_base64": checkpoint1_base64
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected restore response".to_string())?;
        assert_eq!(restore.pointer("/result/isError"), Some(&json!(false)));

        let checkpoint3 = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 41,
                "method": "tools/call",
                "params": {
                    "name": "checkpoint_continuous_stream",
                    "arguments": { "job_name": "restore_stream" }
                }
            }))
            .await
            .ok_or_else(|| "expected third checkpoint response".to_string())?;
        assert_eq!(
            checkpoint3.pointer("/result/structuredContent/checkpoint_base64"),
            Some(&json!(checkpoint1_base64))
        );
        Ok(())
    }

    #[tokio::test]
    async fn incremental_job_checkpoint_restore_round_trips_base64() -> Result<(), String> {
        let server = KrishivMcpServer::new(
            Session::builder().build().map_err(|e| e.to_string())?,
            McpConfig::default(),
        );
        let dir = tempdir().map_err(|e| e.to_string())?;
        let input = dir.path().join("orders.parquet");
        write_one_row_parquet(&input)?;

        server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 13,
                "method": "tools/call",
                "params": {
                    "name": "register_source",
                    "arguments": {
                        "name": "orders",
                        "kind": "parquet",
                        "properties": { "path": input.display().to_string() }
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected source registration response".to_string())?;
        server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 14,
                "method": "tools/call",
                "params": {
                    "name": "create_incremental_view",
                    "arguments": {
                        "job_name": "checkpoint_job",
                        "view_name": "answers",
                        "query": "SELECT answer FROM orders"
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected create view response".to_string())?;
        server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 15,
                "method": "tools/call",
                "params": {
                    "name": "feed_incremental_view",
                    "arguments": {
                        "job_name": "checkpoint_job",
                        "source": "orders",
                        "query": "SELECT answer FROM orders"
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected feed response".to_string())?;
        server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 16,
                "method": "tools/call",
                "params": {
                    "name": "step_incremental_view",
                    "arguments": { "job_name": "checkpoint_job" }
                }
            }))
            .await
            .ok_or_else(|| "expected step response".to_string())?;

        let checkpoint = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 17,
                "method": "tools/call",
                "params": {
                    "name": "checkpoint_incremental_job",
                    "arguments": { "job_name": "checkpoint_job" }
                }
            }))
            .await
            .ok_or_else(|| "expected checkpoint response".to_string())?;
        assert_eq!(checkpoint.pointer("/result/isError"), Some(&json!(false)));
        assert_eq!(
            checkpoint.pointer("/result/structuredContent/checkpoint_type"),
            Some(&json!("full"))
        );
        let checkpoint_base64 = checkpoint
            .pointer("/result/structuredContent/checkpoint_base64")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("checkpoint missing base64 payload: {checkpoint}"))?;
        let byte_len = checkpoint
            .pointer("/result/structuredContent/byte_len")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("checkpoint missing byte length: {checkpoint}"))?;
        assert!(byte_len > 0);

        assert!(server.session.reset_ivm_job("checkpoint_job"));
        server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 18,
                "method": "tools/call",
                "params": {
                    "name": "create_incremental_view",
                    "arguments": {
                        "job_name": "checkpoint_job",
                        "view_name": "answers",
                        "query": "SELECT answer FROM orders"
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected recreate view response".to_string())?;

        let restore = server
            .handle_json_rpc(json!({
                "jsonrpc": "2.0",
                "id": 19,
                "method": "tools/call",
                "params": {
                    "name": "restore_incremental_job",
                    "arguments": {
                        "job_name": "checkpoint_job",
                        "checkpoint_base64": checkpoint_base64
                    }
                }
            }))
            .await
            .ok_or_else(|| "expected restore response".to_string())?;
        assert_eq!(restore.pointer("/result/isError"), Some(&json!(false)));
        assert_eq!(
            restore.pointer("/result/structuredContent/restored"),
            Some(&json!(true))
        );
        assert_eq!(
            restore.pointer("/result/structuredContent/byte_len"),
            Some(&json!(byte_len))
        );
        Ok(())
    }
}
