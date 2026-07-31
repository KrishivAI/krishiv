//! Shared SQL / explain query parsing and session execution paths.

// Deliberate sync-over-async boundary module (Phase 51 async contract):
// block_on here bridges a synchronous public surface to the async core.
#![allow(clippy::disallowed_methods)]

use std::path::PathBuf;
use std::sync::Arc;

use krishiv_api::{DataFrame, ExecutionMode, KrishivError, Session};
use krishiv_common::async_util::block_on;
use krishiv_common::sql_util::split_sql_statements;
use krishiv_plan::governance::{AllowAllPolicyHook, AuthProvider, StaticApiKeyAuthProvider};

use crate::cli::CliResponse;

/// How to execute a SQL statement from the CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryExecution {
    #[default]
    /// `Session::sql` / `sql_async`
    Default,
    /// `Session::execute_local`
    Local,
    /// `Session::execute_remote`
    Remote,
}

/// How `krishiv sql` renders a result set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// Bordered ASCII table — readable, but decoration a program has to parse
    /// around.
    #[default]
    Table,
    /// Newline-delimited JSON, one object per row. What a benchmark harness or
    /// any downstream program should consume: the rows, and nothing else.
    Json,
}

/// A `--parquet` registration, plus anything `--primary-key` declared for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetTableArg {
    pub name: String,
    pub path: PathBuf,
    /// Columns that jointly form the table's primary key, if declared.
    ///
    /// Informational and unverified, exactly as Spark/Databricks `RELY`.
    /// Parquet carries no key, so declaring one is the only way the optimizer
    /// can learn that a column determines the others — which is what enables
    /// group-by pruning and the late-materialisation join-back. A key that is
    /// not really unique and non-null gives *wrong answers*.
    pub primary_key: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct QueryCommand {
    pub query: String,
    /// Result rendering; `--format json` selects NDJSON.
    pub format: OutputFormat,
    pub mode: ExecutionMode,
    pub parquet_tables: Vec<ParquetTableArg>,
    pub execution: QueryExecution,
    pub api_key: Option<String>,
    /// Per-query session timeout, parsed from the CLI but not yet wired to
    /// the session builder. PR #XXX will plumb this through.
    #[expect(
        dead_code,
        reason = "parsed from --query-timeout; wired to session in planned PR"
    )]
    pub timeout_secs: Option<u64>,
    /// Remote coordinator (Flight) URL from the global `-c/--coordinator` flag.
    /// Takes precedence over `KRISHIV_COORDINATOR`; `None` ⇒ env fallback.
    pub coordinator_url: Option<String>,
    /// `explain --analyze`: execute the query and report per-operator runtime
    /// metrics instead of only the plan. A plan cannot answer why a query is
    /// slow when the interesting operator is a dynamic filter, which is empty
    /// until execution populates it.
    pub analyze: bool,
}

pub fn sql_help() -> String {
    String::from(
        "Run a SQL query.\n\
         \n\
         Usage:\n\
           krishiv sql --query <SQL> [OPTIONS]\n\
         \n\
         Options:\n\
           -q, --query <SQL>           SQL statement or semicolon-separated statements (required)\n\
           --mode <embedded|single-node|distributed>  Execution mode (default: embedded)\n\
           --local                     Use Session::execute_local\n\
           --remote                    Use Session::execute_remote (requires coordinator)\n\
           --timeout <SECS>            Timeout in seconds for remote queries (default: 30)\n\
           --api-key <KEY>             Policy-enforced sql_as (requires KRISHIV_API_KEYS)\n\
           --parquet <table=path>      Register a Parquet table (repeatable)\n\
           --primary-key <table=cols>  Declare a table's primary key, comma-separated\n\
                                       (repeatable; informational and UNVERIFIED — a key\n\
                                        that is not unique and non-null gives wrong answers)\n\
           --format <table|json>       Result format (default: table; json = NDJSON rows)\n\
           -h, --help                  Show help\n\
         \n\
         Multi-statement: separate statements with semicolons. Only the last\n\
         statement's result is printed; earlier statements execute for side effects.\n\
         \n\
         Examples:\n\
           krishiv sql --query \"select 1 as value\"\n\
           krishiv sql --local --mode single-node --query \"select 1\"\n\
           krishiv sql --remote -c http://127.0.0.1:50051 --query \"select 1\"\n\
           krishiv sql --query \"CREATE TABLE t (id INT); INSERT INTO t VALUES (1); SELECT * FROM t\"\n\
           krishiv sql --api-key dev-key --query \"select * from people\"\n",
    )
}

pub fn explain_help() -> String {
    String::from(
        "Show logical and physical plan information.\n\
         \n\
         Usage:\n\
           krishiv explain --query <SQL> [OPTIONS]\n\
         \n\
         Options:\n\
           Same as `krishiv sql` (--local, --remote, --api-key, --mode, --parquet).\n\
         \n\
         For continuous window jobs see `krishiv stream submit --help`.\n",
    )
}

pub fn parse_query_command(args: &[&str]) -> Result<QueryCommand, String> {
    let mut query = None;
    let mut mode = ExecutionMode::Embedded;
    let mut parquet_tables: Vec<ParquetTableArg> = Vec::new();
    let mut primary_keys: Vec<(String, Vec<String>)> = Vec::new();
    let mut format = OutputFormat::default();
    let mut analyze = false;
    let mut execution = QueryExecution::Default;
    let mut api_key = None;
    let mut timeout_secs = None;
    let mut idx = 0;
    while idx < args.len() {
        let Some(&arg) = args.get(idx) else {
            break;
        };
        match arg {
            "--query" | "-q" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --query"))?;
                query = Some((*value).to_owned());
            }
            "--mode" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --mode"))?;
                mode = parse_mode(value)?;
            }
            "--parquet" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --parquet"))?;
                parquet_tables.push(parse_parquet_spec(value)?);
            }
            "--primary-key" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --primary-key"))?;
                primary_keys.push(parse_primary_key_spec(value)?);
            }
            "--analyze" => {
                analyze = true;
            }
            "--format" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --format"))?;
                format = match *value {
                    "table" => OutputFormat::Table,
                    "json" => OutputFormat::Json,
                    other => {
                        return Err(format!(
                            "--format must be 'table' or 'json', got '{other}'"
                        ));
                    }
                };
            }
            "--local" => {
                if execution == QueryExecution::Remote {
                    return Err(String::from("--local and --remote are mutually exclusive"));
                }
                execution = QueryExecution::Local;
            }
            "--remote" => {
                if execution == QueryExecution::Local {
                    return Err(String::from("--local and --remote are mutually exclusive"));
                }
                execution = QueryExecution::Remote;
            }
            "--api-key" => {
                idx += 1;
                api_key = Some(
                    args.get(idx)
                        .ok_or_else(|| String::from("missing value for --api-key"))?
                        .to_string(),
                );
            }
            "--timeout" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --timeout"))?;
                timeout_secs = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| format!("invalid timeout value: {value}"))?,
                );
            }
            "--help" | "-h" => return Err(String::from("help requested")),
            unknown => return Err(format!("unknown option: {unknown}")),
        }
        idx += 1;
    }
    if api_key.is_some() && execution != QueryExecution::Default {
        return Err(String::from(
            "--api-key cannot be combined with --local or --remote",
        ));
    }
    let query = query.ok_or_else(|| String::from("missing required --query <SQL>"))?;
    if query.trim().is_empty() {
        return Err(String::from("query cannot be empty"));
    }
    // A key naming a table that was never registered is a typo, and a silent
    // one: the declaration would simply not apply and the only symptom would be
    // "the optimization mysteriously does nothing".
    for (table, columns) in primary_keys {
        match parquet_tables.iter_mut().find(|t| t.name == table) {
            Some(entry) => entry.primary_key = columns,
            None => {
                return Err(format!(
                    "--primary-key names table '{table}', which no --parquet registered"
                ));
            }
        }
    }
    Ok(QueryCommand {
        query,
        format,
        mode,
        parquet_tables,
        execution,
        api_key,
        timeout_secs,
        coordinator_url: None,
        analyze,
    })
}

pub fn build_session(command: &QueryCommand) -> Result<Session, String> {
    let mut builder = Session::builder().with_execution_mode(command.mode);
    // The explicit `-c/--coordinator` flag wins over the env var
    // (`KRISHIV_COORDINATOR_URL`, or its deprecated aliases).
    let coordinator = command
        .coordinator_url
        .clone()
        .or_else(krishiv_common::coordinator_url_env)
        .filter(|url| !url.trim().is_empty());
    if command.mode == ExecutionMode::SingleNode
        && let Some(url) = coordinator.clone()
    {
        builder = builder.with_local_cluster(url);
    }
    if (command.mode == ExecutionMode::Distributed || command.execution == QueryExecution::Remote)
        && let Some(url) = coordinator.clone()
    {
        builder = builder.with_coordinator(url);
        if command.execution == QueryExecution::Remote {
            builder = builder.with_remote_execution(true);
        }
    }
    if command.api_key.is_some() {
        let auth = auth_from_env()?;
        builder = builder
            .with_auth(auth)
            .with_policy(Arc::new(AllowAllPolicyHook));
    }
    let session = builder.build().map_err(|e| e.to_string())?;
    for table in &command.parquet_tables {
        if !table.path.exists() {
            return Err(format!(
                "DataFusion error: parquet file not found: {}",
                table.path.display()
            ));
        }
        session
            .register_parquet_with_primary_key(&table.name, &table.path, &table.primary_key)
            .map_err(|e| e.to_string())?;
    }
    Ok(session)
}

pub fn run_sql(command: &QueryCommand) -> CliResponse {
    let session = match build_session(command) {
        Ok(session) => session,
        Err(message) => return CliResponse::err(format!("{message}\n"), 1),
    };
    let statements = split_sql_statements(&command.query);
    if statements.is_empty() {
        return CliResponse::err(String::from("query is empty\n"), 1);
    }
    let last_idx = statements.len() - 1;
    let mut last_output = String::new();
    for (i, stmt) in statements.iter().enumerate() {
        let mut stmt_cmd = command.clone();
        stmt_cmd.query = stmt.clone();
        match block_on(async {
            let df = query_dataframe(&session, &stmt_cmd).await?;
            if i == last_idx {
                let result = df.collect_async().await?;
                match stmt_cmd.format {
                    OutputFormat::Table => result.pretty(),
                    OutputFormat::Json => result.to_json_lines(),
                }
            } else {
                // Non-final statements: execute for side effects, discard result.
                let _ = df.collect_async().await?;
                Ok(String::new())
            }
        }) {
            Ok(output) => {
                if i == last_idx {
                    last_output = output;
                }
            }
            Err(error) => {
                return CliResponse::err(format!("statement {} failed: {error}\n", i + 1), 1);
            }
        }
    }
    CliResponse::ok(format!("{last_output}\n"))
}

pub fn run_explain(command: &QueryCommand) -> CliResponse {
    let session = match build_session(command) {
        Ok(session) => session,
        Err(message) => return CliResponse::err(format!("{message}\n"), 1),
    };
    match block_on(async {
        if command.analyze {
            let df = analyze_dataframe(&session, command).await?;
            df.explain_analyze_async().await
        } else {
            let df = query_dataframe(&session, command).await?;
            df.explain_async().await
        }
    }) {
        Ok(output) => CliResponse::ok(format!("{output}\n")),
        Err(error) => CliResponse::err(format!("{error}\n"), 1),
    }
}

async fn query_dataframe(
    session: &Session,
    command: &QueryCommand,
) -> Result<DataFrame, KrishivError> {
    if let Some(api_key) = &command.api_key {
        return session.sql_as_async(api_key, &command.query).await;
    }
    match command.execution {
        QueryExecution::Default => session.sql_async(&command.query).await,
        QueryExecution::Local => session.execute_local_async(&command.query).await,
        QueryExecution::Remote => session.execute_remote_async(&command.query).await,
    }
}

/// Build the handle `--analyze` needs, which is not the same handle the other
/// paths want.
///
/// `execute_local_async` runs the query and hands back its results; the planned
/// DataFrame it went through is gone by the time we see the handle, so
/// `explain_analyze_async` found `sql_dataframe: None` and failed with
/// "needs a locally-planned query" for every `explain --analyze --local`
/// invocation. Routing `--local` through the same SQL planning path as the
/// default gives an equivalent local execution *and* keeps the plan, which is
/// the whole point of ANALYZE.
///
/// `--remote` genuinely cannot work: the fragments run on executors and their
/// metrics live there, so say that rather than fail with a plan-shaped error.
async fn analyze_dataframe(
    session: &Session,
    command: &QueryCommand,
) -> Result<DataFrame, KrishivError> {
    if matches!(command.execution, QueryExecution::Remote) {
        return Err(KrishivError::Runtime {
            message: String::from(
                "EXPLAIN ANALYZE reports metrics from the process that ran the plan, so it \
                 cannot be combined with --remote: a distributed plan's metrics live on the \
                 executors that ran its fragments. Re-run without --remote to analyze locally.",
            ),
        });
    }
    if let Some(api_key) = &command.api_key {
        return session.sql_as_async(api_key, &command.query).await;
    }
    session.sql_async(&command.query).await
}

fn auth_from_env() -> Result<Arc<dyn AuthProvider>, String> {
    let raw = std::env::var("KRISHIV_API_KEYS").map_err(|_| {
        String::from("KRISHIV_API_KEYS is required for --api-key (format: key1=user,key2=svc)")
    })?;
    let mut map = std::collections::HashMap::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (key, subject) = part
            .split_once('=')
            .ok_or_else(|| format!("invalid KRISHIV_API_KEYS entry: {part}"))?;
        map.insert(key.trim().to_string(), subject.trim().to_string());
    }
    if map.is_empty() {
        return Err(String::from("KRISHIV_API_KEYS must list at least one key"));
    }
    Ok(Arc::new(StaticApiKeyAuthProvider::new(map)))
}

fn parse_mode(value: &str) -> Result<ExecutionMode, String> {
    match value {
        "embedded" => Ok(ExecutionMode::Embedded),
        "single-node" => Ok(ExecutionMode::SingleNode),
        "distributed" => Ok(ExecutionMode::Distributed),
        other => Err(format!("unsupported mode: {other}")),
    }
}

fn parse_parquet_spec(value: &str) -> Result<ParquetTableArg, String> {
    let (table, path) = value
        .split_once('=')
        .ok_or_else(|| String::from("--parquet must use table=path"))?;
    if table.trim().is_empty() {
        return Err(String::from("parquet table name cannot be empty"));
    }
    if path.trim().is_empty() {
        return Err(String::from("parquet path cannot be empty"));
    }
    Ok(ParquetTableArg {
        name: table.to_owned(),
        path: PathBuf::from(path),
        primary_key: Vec::new(),
    })
}

/// Parse `--primary-key table=col[,col…]`.
///
/// A separate flag rather than a suffix on `--parquet`, because a path may
/// itself contain almost any separator (`s3://bucket/x:y`), and a delimiter
/// that is ambiguous in a path is one that silently truncates it.
fn parse_primary_key_spec(value: &str) -> Result<(String, Vec<String>), String> {
    let (table, columns) = value
        .split_once('=')
        .ok_or_else(|| String::from("--primary-key must use table=col[,col...]"))?;
    if table.trim().is_empty() {
        return Err(String::from("primary-key table name cannot be empty"));
    }
    let columns: Vec<String> = columns
        .split(',')
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .map(str::to_owned)
        .collect();
    if columns.is_empty() {
        return Err(format!("--primary-key for '{table}' names no columns"));
    }
    Ok((table.trim().to_owned(), columns))
}
