#![forbid(unsafe_code)]

//! Command-line shell for Krishiv R1.

use std::path::PathBuf;

use krishiv_api::{ExecutionMode, Session};

/// CLI response used by `main` and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliResponse {
    /// Standard output.
    pub stdout: String,
    /// Standard error.
    pub stderr: String,
    /// Process exit code.
    pub exit_code: i32,
}

impl CliResponse {
    fn ok(stdout: impl Into<String>) -> Self {
        Self {
            stdout: stdout.into(),
            stderr: String::new(),
            exit_code: 0,
        }
    }

    fn err(stderr: impl Into<String>, exit_code: i32) -> Self {
        Self {
            stdout: String::new(),
            stderr: stderr.into(),
            exit_code,
        }
    }
}

#[derive(Debug, Clone)]
struct QueryCommand {
    query: String,
    mode: ExecutionMode,
    parquet_tables: Vec<(String, PathBuf)>,
}

/// Dispatch CLI arguments after the binary name.
pub fn dispatch(args: &[&str]) -> CliResponse {
    match args {
        [] | ["--help"] | ["-h"] | ["help"] => CliResponse::ok(main_help()),
        ["sql"] | ["sql", "--help"] | ["sql", "-h"] => CliResponse::ok(sql_help()),
        ["explain"] | ["explain", "--help"] | ["explain", "-h"] => CliResponse::ok(explain_help()),
        ["jobs", "--help"] | ["jobs", "-h"] => CliResponse::ok(jobs_help()),
        ["help", "sql"] => CliResponse::ok(sql_help()),
        ["help", "explain"] => CliResponse::ok(explain_help()),
        ["help", "jobs"] => CliResponse::ok(jobs_help()),
        ["sql", rest @ ..] => run_sql(rest),
        ["explain", rest @ ..] => run_explain(rest),
        ["jobs", rest @ ..] => run_jobs(rest),
        [unknown, ..] => {
            CliResponse::err(format!("unknown command: {unknown}\n\n{}", main_help()), 2)
        }
    }
}

/// Top-level help text.
pub fn main_help() -> String {
    String::from(
        "Krishiv hybrid compute framework\n\
         \n\
         Usage:\n\
           krishiv <COMMAND>\n\
         \n\
         Commands:\n\
           sql       Run a local SQL query\n\
           explain   Show logical/physical plan information\n\
           jobs      List local jobs for this process\n\
           help      Show help for a command\n\
         \n\
         Options:\n\
           -h, --help  Show help\n",
    )
}

/// Help text for `krishiv sql`.
pub fn sql_help() -> String {
    String::from(
        "Run a local SQL query.\n\
         \n\
         Usage:\n\
           krishiv sql --query <SQL> [--parquet <table=path>] [--mode <embedded|single-node>]\n\
         \n\
         Examples:\n\
           krishiv sql --query \"select 1 as value\"\n\
           krishiv sql --parquet people=./people.parquet --query \"select count(*) from people\"\n",
    )
}

/// Help text for `krishiv explain`.
pub fn explain_help() -> String {
    String::from(
        "Show logical and physical plan information.\n\
         \n\
         Usage:\n\
           krishiv explain --query <SQL> [--parquet <table=path>] [--mode <embedded|single-node>]\n\
         \n\
         Examples:\n\
           krishiv explain --query \"select 1 as value\"\n\
           krishiv explain --parquet people=./people.parquet --query \"select * from people\"\n",
    )
}

/// Help text for `krishiv jobs`.
pub fn jobs_help() -> String {
    String::from(
        "List local jobs for this process.\n\
         \n\
         Usage:\n\
           krishiv jobs\n\
         \n\
         R1 note:\n\
           Jobs are local to the current process. Persistent job history starts later.\n",
    )
}

fn run_sql(args: &[&str]) -> CliResponse {
    let command = match parse_query_command(args) {
        Ok(command) => command,
        Err(message) => return CliResponse::err(format!("{message}\n\n{}", sql_help()), 2),
    };

    let session = match build_session(&command) {
        Ok(session) => session,
        Err(message) => return CliResponse::err(format!("{message}\n"), 1),
    };

    match session
        .sql(&command.query)
        .and_then(|dataframe| dataframe.collect())
        .and_then(|result| result.pretty())
    {
        Ok(output) => CliResponse::ok(format!("{output}\n")),
        Err(error) => CliResponse::err(format!("{error}\n"), 1),
    }
}

fn run_explain(args: &[&str]) -> CliResponse {
    let command = match parse_query_command(args) {
        Ok(command) => command,
        Err(message) => return CliResponse::err(format!("{message}\n\n{}", explain_help()), 2),
    };

    let session = match build_session(&command) {
        Ok(session) => session,
        Err(message) => return CliResponse::err(format!("{message}\n"), 1),
    };

    match session
        .sql(&command.query)
        .and_then(|dataframe| dataframe.explain())
    {
        Ok(output) => CliResponse::ok(format!("{output}\n")),
        Err(error) => CliResponse::err(format!("{error}\n"), 1),
    }
}

fn run_jobs(args: &[&str]) -> CliResponse {
    if !args.is_empty() {
        return CliResponse::err(
            format!("unexpected arguments for jobs\n\n{}", jobs_help()),
            2,
        );
    }

    let session = match Session::builder().build() {
        Ok(session) => session,
        Err(error) => return CliResponse::err(format!("{error}\n"), 1),
    };
    let jobs = session.jobs();

    if jobs.is_empty() {
        return CliResponse::ok("No local jobs in this process.\n");
    }

    let mut output = String::from("ID\tSTATE\tNAME\n");
    for job in jobs {
        output.push_str(&format!(
            "{}\t{}\t{}\n",
            job.id().as_str(),
            job.state(),
            job.name()
        ));
    }

    CliResponse::ok(output)
}

fn build_session(command: &QueryCommand) -> Result<Session, String> {
    let session = Session::builder()
        .with_execution_mode(command.mode)
        .build()
        .map_err(|error| error.to_string())?;

    for (table, path) in &command.parquet_tables {
        session
            .register_parquet(table, path)
            .map_err(|error| error.to_string())?;
    }

    Ok(session)
}

fn parse_query_command(args: &[&str]) -> Result<QueryCommand, String> {
    let mut query = None;
    let mut mode = ExecutionMode::Embedded;
    let mut parquet_tables = Vec::new();
    let mut idx = 0;

    while idx < args.len() {
        match args[idx] {
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
            "--help" | "-h" => {
                return Err(String::from("help requested"));
            }
            unknown => return Err(format!("unknown option: {unknown}")),
        }
        idx += 1;
    }

    let query = query.ok_or_else(|| String::from("missing required --query <SQL>"))?;
    if query.trim().is_empty() {
        return Err(String::from("query cannot be empty"));
    }

    Ok(QueryCommand {
        query,
        mode,
        parquet_tables,
    })
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

#[cfg(test)]
mod tests {
    use super::dispatch;

    #[test]
    fn top_level_help_lists_commands() {
        let response = dispatch(&["--help"]);

        assert_eq!(response.exit_code, 0);
        assert!(response.stdout.contains("Commands:"));
        assert!(response.stdout.contains("sql"));
        assert!(response.stdout.contains("explain"));
        assert!(response.stdout.contains("jobs"));
    }

    #[test]
    fn subcommand_help_is_available() {
        let response = dispatch(&["help", "explain"]);

        assert_eq!(response.exit_code, 0);
        assert!(response.stdout.contains("krishiv explain"));
    }

    #[test]
    fn unknown_command_fails() {
        let response = dispatch(&["bogus"]);

        assert_eq!(response.exit_code, 2);
        assert!(response.stderr.contains("unknown command: bogus"));
    }

    #[test]
    fn sql_command_executes_literal_query() {
        let response = dispatch(&["sql", "--query", "select 1 as value"]);

        assert_eq!(response.exit_code, 0, "{}", response.stderr);
        assert!(response.stdout.contains("value"));
        assert!(response.stdout.contains("1"));
    }

    #[test]
    fn explain_command_returns_plan() {
        let response = dispatch(&["explain", "--query", "select 1 as value"]);

        assert_eq!(response.exit_code, 0, "{}", response.stderr);
        assert!(response.stdout.contains("logical_plan"));
        assert!(response.stdout.contains("physical_plan"));
    }

    #[test]
    fn jobs_command_reports_empty_local_process() {
        let response = dispatch(&["jobs"]);

        assert_eq!(response.exit_code, 0);
        assert!(response.stdout.contains("No local jobs"));
    }
}
