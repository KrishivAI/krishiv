//! Shared SQL / explain query parsing and session execution paths.

use std::path::PathBuf;
use std::sync::Arc;

use krishiv_api::{DataFrame, ExecutionMode, KrishivError, Session};
use krishiv_common::async_util::block_on;
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

#[derive(Debug, Clone)]
pub struct QueryCommand {
    pub query: String,
    pub mode: ExecutionMode,
    pub parquet_tables: Vec<(String, PathBuf)>,
    pub execution: QueryExecution,
    pub api_key: Option<String>,
    #[allow(dead_code)] // parsed; wired to session timeout in a follow-up
    pub timeout_secs: Option<u64>,
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
    let mut parquet_tables = Vec::new();
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
    Ok(QueryCommand {
        query,
        mode,
        parquet_tables,
        execution,
        api_key,
        timeout_secs,
    })
}

pub fn build_session(command: &QueryCommand) -> Result<Session, String> {
    let mut builder = Session::builder().with_execution_mode(command.mode);
    if command.mode == ExecutionMode::SingleNode
        && let Ok(url) = std::env::var("KRISHIV_COORDINATOR")
        && !url.trim().is_empty()
    {
        builder = builder.with_local_cluster(url);
    }
    if (command.mode == ExecutionMode::Distributed || command.execution == QueryExecution::Remote)
        && let Ok(url) = std::env::var("KRISHIV_COORDINATOR")
        && !url.trim().is_empty()
    {
        builder = builder.with_coordinator(url.clone());
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
    for (table, path) in &command.parquet_tables {
        if !path.exists() {
            return Err(format!(
                "DataFusion error: parquet file not found: {}",
                path.display()
            ));
        }
        session
            .register_parquet(table, path)
            .map_err(|e| e.to_string())?;
    }
    Ok(session)
}

pub fn run_sql(command: &QueryCommand) -> CliResponse {
    let session = match build_session(command) {
        Ok(session) => session,
        Err(message) => return CliResponse::err(format!("{message}\n"), 1),
    };
    let statements = split_statements(&command.query);
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
                result.pretty()
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
        let df = query_dataframe(&session, command).await?;
        df.explain_async().await
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

fn parse_parquet_spec(value: &str) -> Result<(String, PathBuf), String> {
    let (table, path) = value
        .split_once('=')
        .ok_or_else(|| String::from("--parquet must use table=path"))?;
    if table.trim().is_empty() {
        return Err(String::from("parquet table name cannot be empty"));
    }
    if path.trim().is_empty() {
        return Err(String::from("parquet path cannot be empty"));
    }
    Ok((table.to_owned(), PathBuf::from(path)))
}

/// Split a SQL string into statements on `;`, stripping `--` comment lines.
fn split_statements(sql: &str) -> Vec<String> {
    let stripped: String = sql
        .lines()
        .map(|l| {
            if l.trim_start().starts_with("--") {
                ""
            } else {
                l
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    stripped
        .split(';')
        .map(|s| s.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|s| !s.is_empty())
        .collect()
}
