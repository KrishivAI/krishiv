//! CLI dispatch for the `krishiv` binary.

use std::path::PathBuf;

use krishiv_api::{ExecutionMode, Session};
use krishiv_checkpoint::{LocalFsCheckpointStorage, list_valid_epochs, read_epoch_metadata};
use krishiv_proto::{
    CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState, JobId,
    JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec,
};
use krishiv_scheduler::{Coordinator, JobDetailSnapshot, JobSnapshot};

pub use crate::remote_client::RemoteCoordinatorClient;

// ── Coordinator mode ──────────────────────────────────────────────────────────

/// Whether CLI commands address a local in-process coordinator or a remote one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorMode {
    /// Default: use the in-process local coordinator.
    Local,
    /// Connect to a remote coordinator at the given URL.
    ///
    /// Set via `--coordinator <URL>` / `-c <URL>`, or the
    /// `KRISHIV_COORDINATOR` environment variable (flag takes precedence).
    Remote(String),
}

impl CoordinatorMode {
    /// Resolve the coordinator mode from CLI args and/or environment.
    ///
    /// Strips `--coordinator <URL>` / `-c <URL>` from `args` (returning the
    /// remaining tokens) and returns the resolved `CoordinatorMode`.
    /// The environment variable `KRISHIV_COORDINATOR` is used as a fallback
    /// when no flag is present.
    pub(crate) fn from_args_and_env<'a>(args: &'a [&'a str]) -> (CoordinatorMode, Vec<&'a str>) {
        let mut remaining: Vec<&str> = Vec::new();
        let mut coordinator_url: Option<String> = None;
        let mut i = 0;
        while i < args.len() {
            match args[i] {
                "--coordinator" | "-c" if i + 1 < args.len() => {
                    coordinator_url = Some(args[i + 1].to_owned());
                    i += 2;
                }
                _ => {
                    remaining.push(args[i]);
                    i += 1;
                }
            }
        }

        // Env var fallback when the flag was not provided.
        if coordinator_url.is_none() {
            coordinator_url = std::env::var("KRISHIV_COORDINATOR").ok();
        }

        let mode = match coordinator_url {
            Some(url) if !url.trim().is_empty() => CoordinatorMode::Remote(url),
            _ => CoordinatorMode::Local,
        };
        (mode, remaining)
    }
}

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

#[derive(Debug, Clone)]
struct SubmitCommand {
    job_id: String,
    name: String,
    kind: JobKind,
    tasks: usize,
    executor_id: String,
    launch: bool,
}

/// Dispatch CLI arguments after the binary name.
pub fn dispatch(args: &[&str]) -> CliResponse {
    match args {
        [] | ["--help"] | ["-h"] | ["help"] => CliResponse::ok(main_help()),
        ["sql"] | ["sql", "--help"] | ["sql", "-h"] => CliResponse::ok(sql_help()),
        ["explain"] | ["explain", "--help"] | ["explain", "-h"] => CliResponse::ok(explain_help()),
        ["submit"] | ["submit", "--help"] | ["submit", "-h"] => CliResponse::ok(submit_help()),
        ["jobs", "--help"] | ["jobs", "-h"] => CliResponse::ok(jobs_help()),
        ["state"] | ["state", "--help"] | ["state", "-h"] => CliResponse::ok(state_help()),
        ["savepoint", "--help"] | ["savepoint", "-h"] => CliResponse::ok(savepoint_help()),
        ["restore", "--help"] | ["restore", "-h"] => CliResponse::ok(restore_help()),
        ["checkpoints", "--help"] | ["checkpoints", "-h"] => CliResponse::ok(checkpoints_help()),
        ["help", "sql"] => CliResponse::ok(sql_help()),
        ["help", "explain"] => CliResponse::ok(explain_help()),
        ["help", "submit"] => CliResponse::ok(submit_help()),
        ["help", "jobs"] => CliResponse::ok(jobs_help()),
        ["help", "state"] => CliResponse::ok(state_help()),
        ["help", "savepoint"] => CliResponse::ok(savepoint_help()),
        ["help", "restore"] => CliResponse::ok(restore_help()),
        ["help", "checkpoints"] => CliResponse::ok(checkpoints_help()),
        ["sql", rest @ ..] => run_sql(rest),
        ["explain", rest @ ..] => run_explain(rest),
        ["submit", rest @ ..] => run_submit(rest),
        ["jobs", rest @ ..] => run_jobs(rest),
        ["state", rest @ ..] => run_state(rest),
        ["savepoint", rest @ ..] => run_savepoint(rest),
        ["restore", rest @ ..] => run_restore(rest),
        ["checkpoints", rest @ ..] => run_checkpoints(rest),
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
           sql          Run a local SQL query\n\
           explain      Show logical/physical plan information\n\
           submit       Submit a distributed job to the R2 local scheduler\n\
           jobs         List local jobs for this process\n\
           state        Inspect streaming operator state metadata (R5.2)\n\
           savepoint    Trigger a savepoint on a running streaming job (R6)\n\
           restore      Restore a streaming job from a checkpoint or savepoint (R6)\n\
           checkpoints  List checkpoints for a streaming job (R6)\n\
           help         Show help for a command\n\
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

/// Help text for `krishiv submit`.
pub fn submit_help() -> String {
    String::from(
        "Submit a distributed job to the R2 local scheduler.\n\
         \n\
         Usage:\n\
           krishiv submit [--job-id <ID>] [--name <NAME>] [--kind <batch|streaming>] [--tasks <N>] [--executor <ID>] [--launch]\n\
         \n\
         Examples:\n\
           krishiv submit --job-id job-1 --name demo --tasks 2\n\
           krishiv submit --kind streaming --tasks 1 --launch\n\
         \n\
         R2 note:\n\
           This command uses the in-process scheduler skeleton. Kubernetes submission starts in a later R2 slice.\n",
    )
}

/// Help text for `krishiv jobs`.
pub fn jobs_help() -> String {
    String::from(
        "List local jobs for this process.\n\
         \n\
         Usage:\n\
           krishiv jobs [--distributed]\n\
         \n\
         R1 note:\n\
           Jobs are local to the current process. Persistent job history starts later.\n\
         \n\
         R2 note:\n\
           --distributed shows the R2 scheduler status shape for this process.\n",
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

fn run_submit(args: &[&str]) -> CliResponse {
    let command = match parse_submit_command(args) {
        Ok(command) => command,
        Err(message) => return CliResponse::err(format!("{message}\n\n{}", submit_help()), 2),
    };

    match submit_to_local_scheduler(&command) {
        Ok(output) => CliResponse::ok(output),
        Err(error) => CliResponse::err(format!("{error}\n"), 1),
    }
}

fn run_jobs(args: &[&str]) -> CliResponse {
    if args == ["--distributed"] {
        let coordinator = match active_local_coordinator() {
            Ok(coordinator) => coordinator,
            Err(error) => return CliResponse::err(format!("{error}\n"), 1),
        };
        return CliResponse::ok(render_distributed_jobs(&coordinator.job_snapshots()));
    }

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

/// Help text for `krishiv state`.
pub fn state_help() -> String {
    String::from(
        "Inspect streaming operator state metadata (read-only).\n\
         \n\
         Usage:\n\
           krishiv state inspect --job <JOB_ID> --operator <OPERATOR_ID>\n\
         \n\
         Subcommands:\n\
           inspect   List namespaces and key counts for a streaming operator\n\
         \n\
         Options:\n\
           --job <JOB_ID>          Job ID of the streaming job\n\
           --operator <OPERATOR_ID>  Operator ID within the job\n\
           -h, --help              Show help\n\
         \n\
         Note: State inspection is read-only.  It reports namespace and key-count\n\
         metadata but never exposes raw value bytes and never mutates live state.\n",
    )
}

fn run_state(args: &[&str]) -> CliResponse {
    match args {
        ["inspect", rest @ ..] => run_state_inspect(rest),
        _ => CliResponse::err(format!("unknown state subcommand\n\n{}", state_help()), 2),
    }
}

fn run_state_inspect(args: &[&str]) -> CliResponse {
    let mut job_id: Option<&str> = None;
    let mut operator_id: Option<&str> = None;
    let mut storage_path: Option<&str> = None;
    let mut i = 0;
    while i < args.len() {
        match args[i] {
            "--job" if i + 1 < args.len() => {
                job_id = Some(args[i + 1]);
                i += 2;
            }
            "--operator" if i + 1 < args.len() => {
                operator_id = Some(args[i + 1]);
                i += 2;
            }
            "--storage-path" if i + 1 < args.len() => {
                storage_path = Some(args[i + 1]);
                i += 2;
            }
            other => {
                return CliResponse::err(
                    format!("unexpected argument '{other}'\n\n{}", state_help()),
                    2,
                );
            }
        }
    }
    let Some(job_id) = job_id else {
        return CliResponse::err(format!("--job is required\n\n{}", state_help()), 2);
    };
    let Some(operator_id) = operator_id else {
        return CliResponse::err(format!("--operator is required\n\n{}", state_help()), 2);
    };
    let path = storage_path.unwrap_or("./krishiv-checkpoints");
    let storage = match LocalFsCheckpointStorage::new(path) {
        Ok(s) => s,
        Err(e) => return CliResponse::err(format!("storage error: {e}\n"), 1),
    };
    let epochs = match list_valid_epochs(&storage, job_id) {
        Ok(e) => e,
        Err(e) => return CliResponse::err(format!("checkpoint error: {e}\n"), 1),
    };
    let Some(&latest_epoch) = epochs.last() else {
        return CliResponse::ok(format!(
            "No checkpoints found for job {job_id} in {path}.\n\
             State inspection requires at least one committed checkpoint.\n"
        ));
    };
    let meta = match read_epoch_metadata(&storage, job_id, latest_epoch) {
        Ok(Some(m)) => m,
        Ok(None) => {
            return CliResponse::ok(format!(
                "No metadata for epoch {latest_epoch} of job {job_id}.\n"
            ))
        }
        Err(e) => return CliResponse::err(format!("metadata read error: {e}\n"), 1),
    };
    let snapshots: Vec<_> = meta
        .operator_snapshots
        .iter()
        .filter(|s| s.operator_id == operator_id)
        .collect();
    if snapshots.is_empty() {
        return CliResponse::ok(format!(
            "No state snapshots found for operator {operator_id} in epoch {latest_epoch}.\n"
        ));
    }
    let mut out = format!(
        "State snapshot(s) for operator {operator_id} (job {job_id}, epoch {latest_epoch}):\n\n\
         TASK\tSNAPSHOT PATH\n"
    );
    for s in &snapshots {
        out.push_str(&format!("{}\t{}\n", s.task_id, s.snapshot_path));
    }
    CliResponse::ok(out)
}

fn submit_to_local_scheduler(command: &SubmitCommand) -> Result<String, String> {
    let mut coordinator = active_local_coordinator()?;
    let executor_id =
        ExecutorId::try_new(command.executor_id.clone()).map_err(|error| error.to_string())?;
    let slots = command.tasks.max(1);
    coordinator
        .register_executor(ExecutorDescriptor::new(
            executor_id.clone(),
            "local-r2-executor",
            slots,
        ))
        .map_err(|error| error.to_string())?;
    coordinator
        .executor_heartbeat(ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy))
        .map_err(|error| error.to_string())?;

    let job = build_submit_job(command)?;
    let job_id = job.job_id().clone();
    coordinator
        .submit_job(job)
        .map_err(|error| error.to_string())?;
    if command.launch {
        coordinator
            .launch_assigned_tasks(&job_id)
            .map_err(|error| error.to_string())?;
    }

    let detail = coordinator
        .job_detail_snapshot(&job_id)
        .map_err(|error| error.to_string())?;
    Ok(render_submit_result(
        command.kind,
        &detail,
        &coordinator.executor_snapshots(),
    ))
}

fn active_local_coordinator() -> Result<Coordinator, String> {
    let coordinator_id =
        CoordinatorId::try_new("coord-local").map_err(|error| error.to_string())?;
    Ok(Coordinator::active(coordinator_id))
}

fn build_submit_job(command: &SubmitCommand) -> Result<JobSpec, String> {
    let job_id = JobId::try_new(command.job_id.clone()).map_err(|error| error.to_string())?;
    let mut stage = StageSpec::new(
        StageId::try_new("stage-1").map_err(|error| error.to_string())?,
        "r2-cli-stage",
    );

    for idx in 1..=command.tasks {
        let task_id = TaskId::try_new(format!("task-{idx}")).map_err(|error| error.to_string())?;
        stage = stage.with_task(TaskSpec::new(task_id, format!("r2 cli task {idx}")));
    }

    Ok(JobSpec::new(job_id, command.name.clone(), command.kind).with_stage(stage))
}

fn render_submit_result(
    kind: JobKind,
    detail: &JobDetailSnapshot,
    executors: &[krishiv_scheduler::ExecutorRecord],
) -> String {
    let mut output = format!("Submitted distributed {kind} job through the R2 local scheduler.\n");
    output.push_str(&render_distributed_jobs(&[detail.job().clone()]));

    output.push_str("\nSTAGE\tSTATE\tTASKS\n");
    for stage in detail.stages() {
        output.push_str(&format!(
            "{}\t{}\t{}\n",
            stage.stage_id(),
            stage.state(),
            stage.task_count()
        ));
    }

    output.push_str("\nTASK\tSTAGE\tSTATE\tEXECUTOR\tATTEMPT\n");
    for stage in detail.stages() {
        for task in stage.tasks() {
            let executor = task
                .assigned_executor()
                .map(ToString::to_string)
                .unwrap_or_else(|| String::from("-"));
            output.push_str(&format!(
                "{}\t{}\t{}\t{}\t{}\n",
                task.task_id(),
                stage.stage_id(),
                task.state(),
                executor,
                task.attempt()
            ));
        }
    }

    output.push_str("\nEXECUTOR\tSTATE\tSLOTS\tHOST\n");
    for executor in executors {
        output.push_str(&format!(
            "{}\t{}\t{}\t{}\n",
            executor.executor_id(),
            executor.state(),
            executor.descriptor().slots(),
            executor.descriptor().host()
        ));
    }

    output
}

fn render_distributed_jobs(jobs: &[JobSnapshot]) -> String {
    if jobs.is_empty() {
        return String::from("No distributed jobs in this process.\n");
    }

    let mut output =
        String::from("JOB\tSTATE\tSTAGES\tTASKS\tASSIGNED\tRUNNING\tSUCCEEDED\tFAILED\n");
    for job in jobs {
        output.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            job.job_id(),
            job.state(),
            job.stage_count(),
            job.task_count(),
            job.assigned_task_count(),
            job.running_task_count(),
            job.succeeded_task_count(),
            job.failed_task_count()
        ));
    }

    output
}

fn build_session(command: &QueryCommand) -> Result<Session, String> {
    let session = Session::builder()
        .with_execution_mode(command.mode)
        .build()
        .map_err(|error| error.to_string())?;

    for (table, path) in &command.parquet_tables {
        // DataFusion 53+ silently returns empty results for missing files;
        // validate existence here so users get a clear error.
        if !path.exists() {
            return Err(format!(
                "DataFusion error: parquet file not found: {}",
                path.display()
            ));
        }
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

fn parse_submit_command(args: &[&str]) -> Result<SubmitCommand, String> {
    let mut job_id = String::from("job-1");
    let mut name = String::from("demo-distributed-job");
    let mut kind = JobKind::Batch;
    let mut tasks = 1;
    let mut executor_id = String::from("exec-local-1");
    let mut launch = false;
    let mut idx = 0;

    while idx < args.len() {
        match args[idx] {
            "--job-id" => {
                idx += 1;
                job_id = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --job-id"))?
                    .to_string();
            }
            "--name" => {
                idx += 1;
                name = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --name"))?
                    .to_string();
            }
            "--kind" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --kind"))?;
                kind = parse_job_kind(value)?;
            }
            "--tasks" => {
                idx += 1;
                let value = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --tasks"))?;
                tasks = parse_positive_usize(value, "--tasks")?;
            }
            "--executor" => {
                idx += 1;
                executor_id = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --executor"))?
                    .to_string();
            }
            "--launch" => {
                launch = true;
            }
            "--help" | "-h" => {
                return Err(String::from("help requested"));
            }
            unknown => return Err(format!("unknown option: {unknown}")),
        }
        idx += 1;
    }

    if job_id.trim().is_empty() {
        return Err(String::from("job id cannot be empty"));
    }
    if name.trim().is_empty() {
        return Err(String::from("job name cannot be empty"));
    }
    if executor_id.trim().is_empty() {
        return Err(String::from("executor id cannot be empty"));
    }

    Ok(SubmitCommand {
        job_id,
        name,
        kind,
        tasks,
        executor_id,
        launch,
    })
}

fn parse_job_kind(value: &str) -> Result<JobKind, String> {
    match value {
        "batch" => Ok(JobKind::Batch),
        "streaming" => Ok(JobKind::Streaming),
        other => Err(format!("unsupported job kind: {other}")),
    }
}

fn parse_positive_usize(value: &str, flag: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("{flag} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{flag} must be greater than zero"));
    }
    Ok(parsed)
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

// ── R6: savepoint ─────────────────────────────────────────────────────────────

/// Help text for `krishiv savepoint`.
pub fn savepoint_help() -> String {
    String::from(
        "Trigger a savepoint on a running streaming job.\n\
         \n\
         Usage:\n\
           krishiv savepoint --job <JOB_ID> [--label <LABEL>]\n\
         \n\
         Options:\n\
           --job <JOB_ID>   Job ID of the streaming job (required)\n\
           --label <LABEL>  Human-readable label for this savepoint (optional)\n\
           -h, --help       Show help\n\
         \n\
         Note: The savepoint is written to the checkpoint storage path configured\n\
         for the job.  Use `krishiv checkpoints list --job <JOB_ID>` to confirm\n\
         completion, then `krishiv restore` to resume from the savepoint.\n",
    )
}

fn run_savepoint(args: &[&str]) -> CliResponse {
    let mut job_id: Option<&str> = None;
    let mut label: Option<&str> = None;
    let mut i = 0;
    while i < args.len() {
        match args[i] {
            "--job" if i + 1 < args.len() => {
                job_id = Some(args[i + 1]);
                i += 2;
            }
            "--label" if i + 1 < args.len() => {
                label = Some(args[i + 1]);
                i += 2;
            }
            other => {
                return CliResponse::err(
                    format!("unexpected argument '{other}'\n\n{}", savepoint_help()),
                    2,
                );
            }
        }
    }
    let Some(job_id_str) = job_id else {
        return CliResponse::err(format!("--job is required\n\n{}", savepoint_help()), 2);
    };
    let job_id = match JobId::try_new(job_id_str) {
        Ok(id) => id,
        Err(e) => return CliResponse::err(format!("invalid job id: {e}\n"), 2),
    };
    let label_opt = label.map(String::from);
    let mut coordinator = match active_local_coordinator() {
        Ok(c) => c,
        Err(e) => return CliResponse::err(format!("{e}\n"), 1),
    };
    match coordinator.trigger_checkpoint_for_job(&job_id) {
        Ok(_) => {
            let label_display = label_opt.as_deref().unwrap_or("(none)");
            CliResponse::ok(format!(
                "Savepoint initiated\nJob:   {job_id}\nLabel: {label_display}\n\
                 Note: in local mode the coordinator holds no running jobs.\n\
                 For distributed jobs, use a remote coordinator (--coordinator planned for R12).\n"
            ))
        }
        Err(e) => CliResponse::err(
            format!(
                "Savepoint failed for job {job_id}: {e}\n\
                 Ensure the job is a running streaming job with checkpoint_interval_ms set.\n\
                 For distributed jobs, connect to the coordinator (--coordinator planned for R12).\n"
            ),
            1,
        ),
    }
}

// ── R6: restore ───────────────────────────────────────────────────────────────

/// Help text for `krishiv restore`.
pub fn restore_help() -> String {
    String::from(
        "Restore a streaming job from a checkpoint or savepoint.\n\
         \n\
         Usage:\n\
           krishiv restore --job <JOB_ID> --epoch <N> [--storage-path <PATH>]\n\
         \n\
         Options:\n\
           --job <JOB_ID>          Job ID of the streaming job (required)\n\
           --epoch <N>             Checkpoint epoch to restore from (required)\n\
           --storage-path <PATH>   Checkpoint storage base path (optional, uses job default)\n\
           -h, --help              Show help\n\
         \n\
         Note: The job must have been stopped (via savepoint) before restore.\n\
         Restoring to a different parallelism than the checkpoint is not supported in R6.\n",
    )
}

fn run_restore(args: &[&str]) -> CliResponse {
    let mut job_id: Option<&str> = None;
    let mut epoch: Option<&str> = None;
    let mut storage_path: Option<&str> = None;
    let mut i = 0;
    while i < args.len() {
        match args[i] {
            "--job" if i + 1 < args.len() => {
                job_id = Some(args[i + 1]);
                i += 2;
            }
            "--epoch" if i + 1 < args.len() => {
                epoch = Some(args[i + 1]);
                i += 2;
            }
            "--storage-path" if i + 1 < args.len() => {
                storage_path = Some(args[i + 1]);
                i += 2;
            }
            other => {
                return CliResponse::err(
                    format!("unexpected argument '{other}'\n\n{}", restore_help()),
                    2,
                );
            }
        }
    }
    let Some(job_id) = job_id else {
        return CliResponse::err(format!("--job is required\n\n{}", restore_help()), 2);
    };
    let Some(epoch) = epoch else {
        return CliResponse::err(format!("--epoch is required\n\n{}", restore_help()), 2);
    };
    let epoch_num: u64 = match epoch.parse() {
        Ok(n) => n,
        Err(_) => {
            return CliResponse::err(
                format!(
                    "--epoch must be a non-negative integer; got '{epoch}'\n\n{}",
                    restore_help()
                ),
                2,
            );
        }
    };
    let path = storage_path.unwrap_or("./krishiv-checkpoints");
    let storage = match LocalFsCheckpointStorage::new(path) {
        Ok(s) => s,
        Err(e) => return CliResponse::err(format!("storage error: {e}\n"), 1),
    };
    let meta = match read_epoch_metadata(&storage, job_id, epoch_num) {
        Ok(Some(m)) => m,
        Ok(None) => {
            return CliResponse::err(
                format!("epoch {epoch_num} not found for job {job_id} in {path}\n"),
                1,
            )
        }
        Err(e) => return CliResponse::err(format!("checkpoint read error: {e}\n"), 1),
    };
    let snapshot_count = meta.operator_snapshots.len();
    let source_count = meta.source_offsets.len();
    let kind = if meta.is_savepoint { "savepoint" } else { "checkpoint" };
    let label = meta
        .savepoint_label
        .as_deref()
        .map(|l| format!(" ({l})"))
        .unwrap_or_default();
    CliResponse::ok(format!(
        "Restore plan\n\
         Job:              {job_id}\n\
         Epoch:            {epoch_num}\n\
         Type:             {kind}{label}\n\
         Storage:          {path}\n\
         Source partitions:{source_count}\n\
         Operator snapshots:{snapshot_count}\n\
         Fencing token:    {ft}\n\
         \n\
         To apply: signal the running coordinator to restore from epoch {epoch_num}.\n\
         Remote coordinator restore (--coordinator) planned for R12.\n",
        ft = meta.fencing_token,
    ))
}

// ── R6: checkpoints ───────────────────────────────────────────────────────────

/// Help text for `krishiv checkpoints`.
pub fn checkpoints_help() -> String {
    String::from(
        "List and inspect checkpoints for a streaming job.\n\
         \n\
         Usage:\n\
           krishiv checkpoints list --job <JOB_ID>\n\
         \n\
         Subcommands:\n\
           list   List all valid checkpoint epochs for a job\n\
         \n\
         Options:\n\
           --job <JOB_ID>  Job ID of the streaming job (required)\n\
           -h, --help      Show help\n",
    )
}

fn run_checkpoints(args: &[&str]) -> CliResponse {
    match args {
        ["list", rest @ ..] => run_checkpoints_list(rest),
        _ => CliResponse::err(
            format!("unknown checkpoints subcommand\n\n{}", checkpoints_help()),
            2,
        ),
    }
}

fn run_checkpoints_list(args: &[&str]) -> CliResponse {
    let mut job_id: Option<&str> = None;
    let mut storage_path: Option<&str> = None;
    let mut i = 0;
    while i < args.len() {
        match args[i] {
            "--job" if i + 1 < args.len() => {
                job_id = Some(args[i + 1]);
                i += 2;
            }
            "--storage-path" if i + 1 < args.len() => {
                storage_path = Some(args[i + 1]);
                i += 2;
            }
            other => {
                return CliResponse::err(
                    format!("unexpected argument '{other}'\n\n{}", checkpoints_help()),
                    2,
                );
            }
        }
    }
    let Some(job_id) = job_id else {
        return CliResponse::err(format!("--job is required\n\n{}", checkpoints_help()), 2);
    };
    let path = storage_path.unwrap_or("./krishiv-checkpoints");
    let storage = match LocalFsCheckpointStorage::new(path) {
        Ok(s) => s,
        Err(e) => return CliResponse::err(format!("storage error: {e}\n"), 1),
    };
    let epochs = match list_valid_epochs(&storage, job_id) {
        Ok(e) => e,
        Err(e) => return CliResponse::err(format!("checkpoint error: {e}\n"), 1),
    };
    if epochs.is_empty() {
        return CliResponse::ok(format!(
            "No checkpoints found for job {job_id} in {path}\n"
        ));
    }
    let mut out = format!(
        "Checkpoints for job {job_id} in {path}\n\nEPOCH\tTYPE\tLABEL\n"
    );
    for &epoch in &epochs {
        let (kind, label) = match read_epoch_metadata(&storage, job_id, epoch) {
            Ok(Some(meta)) => {
                let k = if meta.is_savepoint { "savepoint" } else { "checkpoint" };
                let l = meta.savepoint_label.unwrap_or_default();
                (k, l)
            }
            _ => ("unknown", String::new()),
        };
        out.push_str(&format!("{epoch}\t{kind}\t{label}\n"));
    }
    CliResponse::ok(out)
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
        assert!(response.stdout.contains("submit"));
        assert!(response.stdout.contains("jobs"));
    }

    #[test]
    fn subcommand_help_is_available() {
        let response = dispatch(&["help", "explain"]);

        assert_eq!(response.exit_code, 0);
        assert!(response.stdout.contains("krishiv explain"));
    }

    #[test]
    fn submit_help_is_available() {
        let response = dispatch(&["help", "submit"]);

        assert_eq!(response.exit_code, 0);
        assert!(response.stdout.contains("krishiv submit"));
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

    #[test]
    fn jobs_command_reports_empty_distributed_process() {
        let response = dispatch(&["jobs", "--distributed"]);

        assert_eq!(response.exit_code, 0);
        assert!(response.stdout.contains("No distributed jobs"));
    }

    #[test]
    fn submit_command_uses_r2_scheduler_status_shape() {
        let response = dispatch(&[
            "submit", "--job-id", "job-demo", "--name", "demo", "--tasks", "2", "--launch",
        ]);

        assert_eq!(response.exit_code, 0, "{}", response.stderr);
        assert!(response.stdout.contains("Submitted distributed batch job"));
        assert!(response.stdout.contains("JOB\tSTATE\tSTAGES\tTASKS"));
        assert!(response.stdout.contains("job-demo\trunning\t1\t2"));
        assert!(
            response
                .stdout
                .contains("TASK\tSTAGE\tSTATE\tEXECUTOR\tATTEMPT")
        );
        assert!(
            response
                .stdout
                .contains("task-1\tstage-1\trunning\texec-local-1\t1")
        );
        assert!(response.stdout.contains("EXECUTOR\tSTATE\tSLOTS\tHOST"));
    }

    #[test]
    fn submit_command_rejects_zero_tasks() {
        let response = dispatch(&["submit", "--tasks", "0"]);

        assert_eq!(response.exit_code, 2);
        assert!(
            response
                .stderr
                .contains("--tasks must be greater than zero")
        );
    }

    #[test]
    fn state_help_command_exits_zero() {
        let response = dispatch(&["state", "--help"]);
        assert_eq!(response.exit_code, 0);
        assert!(response.stdout.contains("inspect"));
        assert!(response.stdout.contains("read-only"));
    }

    #[test]
    fn state_inspect_requires_job_and_operator() {
        let response = dispatch(&["state", "inspect"]);
        assert_eq!(response.exit_code, 2);
        assert!(response.stderr.contains("--job is required"));
    }

    #[test]
    fn state_inspect_with_no_checkpoints_reports_none_found() {
        let response = dispatch(&[
            "state",
            "inspect",
            "--job",
            "job-123",
            "--operator",
            "tumbling-1",
            "--storage-path",
            "./krishiv-checkpoints",
        ]);
        assert_eq!(response.exit_code, 0, "{:?}", response);
        assert!(response.stdout.contains("job-123"));
        assert!(
            response.stdout.contains("No checkpoints found")
                || response.stdout.contains("No state snapshots found")
                || response.stdout.contains("No metadata"),
            "expected informative message, got: {:?}",
            response.stdout
        );
    }

    #[test]
    fn state_unknown_subcommand_exits_nonzero() {
        let response = dispatch(&["state", "unknown"]);
        assert_eq!(response.exit_code, 2);
        assert!(response.stderr.contains("unknown state subcommand"));
    }

    #[test]
    fn savepoint_help_command_exits_zero() {
        let response = dispatch(&["savepoint", "--help"]);
        assert_eq!(response.exit_code, 0);
        assert!(response.stdout.contains("savepoint"));
    }

    #[test]
    fn savepoint_requires_job_flag() {
        let response = dispatch(&["savepoint"]);
        assert_eq!(response.exit_code, 2);
        assert!(response.stderr.contains("--job is required"));
    }

    #[test]
    fn savepoint_local_mode_fails_with_no_running_job() {
        let response = dispatch(&["savepoint", "--job", "job-1"]);
        assert_eq!(response.exit_code, 1, "{:?}", response);
        assert!(response.stderr.contains("job-1"));
        assert!(
            response.stderr.contains("Savepoint failed") || response.stderr.contains("Savepoint initiated"),
            "expected savepoint result, got: {:?}",
            response.stderr
        );
    }

    #[test]
    fn savepoint_with_label_includes_job_in_output() {
        let response = dispatch(&["savepoint", "--job", "job-2", "--label", "pre-upgrade"]);
        assert_eq!(response.exit_code, 1, "{:?}", response);
        assert!(response.stderr.contains("job-2"));
    }

    #[test]
    fn restore_help_command_exits_zero() {
        let response = dispatch(&["restore", "--help"]);
        assert_eq!(response.exit_code, 0);
        assert!(response.stdout.contains("restore"));
    }

    #[test]
    fn restore_requires_job_and_epoch() {
        let response = dispatch(&["restore", "--job", "job-1"]);
        assert_eq!(response.exit_code, 2);
        assert!(response.stderr.contains("--epoch is required"));

        let response = dispatch(&["restore", "--epoch", "3"]);
        assert_eq!(response.exit_code, 2);
        assert!(response.stderr.contains("--job is required"));
    }

    #[test]
    fn restore_with_missing_epoch_returns_error() {
        let response = dispatch(&[
            "restore",
            "--job",
            "job-1",
            "--epoch",
            "3",
            "--storage-path",
            "./krishiv-checkpoints",
        ]);
        assert_eq!(response.exit_code, 1, "{:?}", response);
        assert!(
            response.stderr.contains("job-1") || response.stderr.contains("epoch"),
            "expected epoch/job in error, got: {:?}",
            response.stderr
        );
    }

    #[test]
    fn restore_rejects_non_numeric_epoch() {
        let response = dispatch(&["restore", "--job", "job-1", "--epoch", "latest"]);
        assert_eq!(response.exit_code, 2);
        assert!(response.stderr.contains("non-negative integer"));
    }

    #[test]
    fn checkpoints_help_exits_zero() {
        let response = dispatch(&["checkpoints", "--help"]);
        assert_eq!(response.exit_code, 0);
        assert!(response.stdout.contains("checkpoints"));
    }

    #[test]
    fn checkpoints_list_requires_job() {
        let response = dispatch(&["checkpoints", "list"]);
        assert_eq!(response.exit_code, 2);
        assert!(response.stderr.contains("--job is required"));
    }

    #[test]
    fn checkpoints_list_returns_skeleton() {
        let response = dispatch(&["checkpoints", "list", "--job", "job-1"]);
        assert_eq!(response.exit_code, 0, "{}", response.stderr);
        assert!(response.stdout.contains("No checkpoints found"));
        assert!(response.stdout.contains("job-1"));
    }

    #[test]
    fn checkpoints_unknown_subcommand_exits_nonzero() {
        let response = dispatch(&["checkpoints", "delete"]);
        assert_eq!(response.exit_code, 2);
        assert!(response.stderr.contains("unknown checkpoints subcommand"));
    }
}
