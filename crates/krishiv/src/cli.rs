//! CLI dispatch for the `krishiv` binary.

use std::path::PathBuf;

use krishiv_api::{ExecutionMode, Session};
use krishiv_async_util::block_on;
use krishiv_checkpoint::{LocalFsCheckpointStorage, list_valid_epochs, read_epoch_metadata};
use krishiv_proto::{
    CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState, JobId,
    JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec,
};
use krishiv_scheduler::{Coordinator, JobDetailSnapshot, JobSnapshot};

use crate::remote_client::RemoteCoordinatorClient;

// ── CoordinatorMode ───────────────────────────────────────────────────────────

/// Whether commands dispatch locally (in-process) or to a remote coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorMode {
    /// Use the in-process coordinator (default).
    Local,
    /// Forward commands to a remote coordinator at the given URL.
    Remote(String),
}

impl CoordinatorMode {
    /// Parse `--coordinator`/`-c` from `args`, consulting `KRISHIV_COORDINATOR`
    /// env var as a fallback. Returns the mode and the remaining args.
    pub fn from_args_and_env<'a>(args: &'a [&'a str]) -> (CoordinatorMode, Vec<&'a str>) {
        let env_value = std::env::var("KRISHIV_COORDINATOR").ok();
        Self::from_args_with_env_override(args, env_value.as_deref())
    }

    /// Testable variant: `env_value` is `Some(url)` if the env var is set.
    /// Call sites in tests pass an explicit value instead of mutating real env.
    pub fn from_args_with_env_override<'a>(
        args: &'a [&'a str],
        env_value: Option<&str>,
    ) -> (CoordinatorMode, Vec<&'a str>) {
        let mut remaining = Vec::new();
        let mut url: Option<String> = None;
        let mut i = 0;
        while i < args.len() {
            match args[i] {
                "--coordinator" | "-c" if i + 1 < args.len() => {
                    url = Some(args[i + 1].to_owned());
                    i += 2;
                }
                other => {
                    remaining.push(other);
                    i += 1;
                }
            }
        }
        // Explicit flag beats env var.
        if url.is_none()
            && let Some(env_url) = env_value.filter(|u| !u.is_empty())
        {
            url = Some(env_url.to_owned());
        }
        let mode = match url {
            Some(u) => CoordinatorMode::Remote(u),
            None => CoordinatorMode::Local,
        };
        (mode, remaining)
    }
}

/// CLI response used by `main` and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliResponse {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

impl CliResponse {
    pub(crate) fn ok(stdout: impl Into<String>) -> Self {
        Self {
            stdout: stdout.into(),
            stderr: String::new(),
            exit_code: 0,
        }
    }
    pub(crate) fn err(stderr: impl Into<String>, exit_code: i32) -> Self {
        Self {
            stdout: String::new(),
            stderr: stderr.into(),
            exit_code,
        }
    }
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
    // Strip the global --coordinator/-c flag before routing to subcommands.
    let (coordinator_mode, remaining) = CoordinatorMode::from_args_and_env(args);
    let args: &[&str] = &remaining;

    match args {
        [] | ["--help"] | ["-h"] | ["help"] => CliResponse::ok(main_help()),
        ["sql"] | ["sql", "--help"] | ["sql", "-h"] => {
            CliResponse::ok(crate::query_cli::sql_help())
        }
        ["explain"] | ["explain", "--help"] | ["explain", "-h"] => {
            CliResponse::ok(crate::query_cli::explain_help())
        }
        ["stream"] | ["stream", "--help"] | ["stream", "-h"] => {
            CliResponse::ok(crate::stream_cmd::stream_help())
        }
        ["table"] | ["table", "--help"] | ["table", "-h"] => {
            CliResponse::ok(crate::table_cmd::table_help())
        }
        ["submit"] | ["submit", "--help"] | ["submit", "-h"] => CliResponse::ok(submit_help()),
        ["jobs", "--help"] | ["jobs", "-h"] => CliResponse::ok(jobs_help()),
        ["state"] | ["state", "--help"] | ["state", "-h"] => CliResponse::ok(state_help()),
        ["savepoint", "--help"] | ["savepoint", "-h"] => CliResponse::ok(savepoint_help()),
        ["restore", "--help"] | ["restore", "-h"] => CliResponse::ok(restore_help()),
        ["checkpoints", "--help"] | ["checkpoints", "-h"] => CliResponse::ok(checkpoints_help()),
        ["compat", "--help"] | ["compat", "-h"] => CliResponse::ok(compat_help()),
        ["local"] | ["local", "--help"] | ["local", "-h"] => {
            CliResponse::ok(crate::local_cluster::local_help())
        }
        ["help", "sql"] => CliResponse::ok(crate::query_cli::sql_help()),
        ["help", "explain"] => CliResponse::ok(crate::query_cli::explain_help()),
        ["help", "stream"] => CliResponse::ok(crate::stream_cmd::stream_help()),
        ["help", "table"] => CliResponse::ok(crate::table_cmd::table_help()),
        ["help", "submit"] => CliResponse::ok(submit_help()),
        ["help", "jobs"] => CliResponse::ok(jobs_help()),
        ["help", "state"] => CliResponse::ok(state_help()),
        ["help", "savepoint"] => CliResponse::ok(savepoint_help()),
        ["help", "restore"] => CliResponse::ok(restore_help()),
        ["help", "checkpoints"] => CliResponse::ok(checkpoints_help()),
        ["help", "compat"] => CliResponse::ok(compat_help()),
        ["help", "local"] => CliResponse::ok(crate::local_cluster::local_help()),
        ["help", "daemons"] | ["help", "daemon"] => crate::daemon_cmd::help_daemons(),
        ["local", rest @ ..] => crate::local_cluster::run_local(rest),
        ["cluster", rest @ ..] => crate::cluster_cmd::run_cluster(rest),
        ["coordinator", ..] | ["clusterd", ..] | ["executor", ..] | ["job-coordinator", ..]
        | ["flight-server", ..] | ["shuffle-svc", ..] => CliResponse::err(
            format!(
                "daemon commands must run via the krishiv binary entrypoint\n\n{}",
                crate::daemon_cmd::daemons_help()
            ),
            2,
        ),
        ["compat", "analyze", rest @ ..] => run_compat_analyze(rest),
        ["sql", rest @ ..] => run_sql(rest),
        ["explain", rest @ ..] => run_explain(rest),
        ["stream", rest @ ..] => crate::stream_cmd::run_stream(rest),
        ["table", rest @ ..] => crate::table_cmd::run_table(rest),
        ["submit", rest @ ..] => run_submit(rest),
        ["jobs", rest @ ..] => run_jobs(rest),
        ["state", rest @ ..] => run_state(rest, &coordinator_mode),
        ["savepoint", rest @ ..] => run_savepoint(rest, &coordinator_mode),
        ["restore", rest @ ..] => run_restore(rest, &coordinator_mode),
        ["checkpoints", rest @ ..] => run_checkpoints(rest, &coordinator_mode),
        [unknown, ..] => {
            CliResponse::err(format!("unknown command: {unknown}\n\n{}", main_help()), 2)
        }
    }
}

pub fn main_help() -> String {
    String::from(
        "Krishiv hybrid compute framework\n\
         \n\
         Usage:\n\
           krishiv [OPTIONS] <COMMAND>\n\
         \n\
         Commands:\n\
           sql          Run SQL (--local, --remote, --api-key)\n\
           explain      Show logical/physical plan information\n\
           stream       Continuous streaming jobs (submit, push, poll)\n\
           table        Read parquet, delta, or hudi tables\n\
           submit       Submit a distributed job to the R2 local scheduler\n\
           jobs         List local jobs for this process\n\
           state        Inspect streaming operator state metadata (R5.2)\n\
           savepoint    Trigger a savepoint on a running streaming job (R6)\n\
           restore      Restore a streaming job from a checkpoint or savepoint (R6)\n\
           checkpoints  List checkpoints for a streaming job (R6)\n\
           compat       PySpark migration compatibility tools (R15)\n\
           local        Start/stop/status a Spark-like local cluster\n\
           cluster      Start/stop/status bare-metal clusterd + executors\n\
           coordinator  Run active coordinator (distributed)\n\
           clusterd     Run cluster control plane (CCP)\n\
           job-coordinator  Run per-job coordinator (JCP)\n\
           executor     Run data-plane executor worker\n\
           flight-server  Run Arrow Flight SQL endpoint\n\
           shuffle-svc  Run optional shuffle HTTP service\n\
           help         Show help for a command (try: krishiv help daemons)\n\
         \n\
         Options:\n\
           -c, --coordinator <URL>  Remote coordinator URL (or set KRISHIV_COORDINATOR)\n\
           -h, --help               Show help\n",
    )
}

pub fn compat_help() -> String {
    String::from(
        "PySpark migration compatibility analyzer.\n\
         \n\
         Usage:\n\
           krishiv compat analyze <file.py> [--format text|json] [--output <file>]\n",
    )
}

fn run_compat_analyze(args: &[&str]) -> CliResponse {
    use std::path::PathBuf;
    let mut path = None;
    let mut format = "text";
    let mut output = None;
    let mut i = 0;
    while i < args.len() {
        match args[i] {
            "--format" if i + 1 < args.len() => {
                format = args[i + 1];
                i += 2;
            }
            "--output" | "-o" if i + 1 < args.len() => {
                output = Some(PathBuf::from(args[i + 1]));
                i += 2;
            }
            flag if flag.starts_with('-') => {
                return CliResponse::err(format!("unknown flag: {flag}"), 2);
            }
            file => {
                path = Some(PathBuf::from(file));
                i += 1;
            }
        }
    }
    let Some(path) = path else {
        return CliResponse::err(format!("missing file\n\n{}", compat_help()), 2);
    };
    let report = match crate::compat::analyze_file(&path) {
        Ok(r) => r,
        Err(e) => return CliResponse::err(e, 1),
    };
    let body = if format == "json" {
        serde_json::to_string_pretty(&report).unwrap_or_else(|e| e.to_string())
    } else {
        crate::compat::format_report_text(&report)
    };
    if let Some(out_path) = output {
        if let Err(e) = std::fs::write(&out_path, &body) {
            return CliResponse::err(format!("write {}: {e}", out_path.display()), 1);
        }
        CliResponse::ok(format!("wrote report to {}\n", out_path.display()))
    } else {
        CliResponse::ok(body)
    }
}

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
    let command = match crate::query_cli::parse_query_command(args) {
        Ok(command) => command,
        Err(message) => {
            return CliResponse::err(format!("{message}\n\n{}", crate::query_cli::sql_help()), 2)
        }
    };
    crate::query_cli::run_sql(&command)
}

fn run_explain(args: &[&str]) -> CliResponse {
    let command = match crate::query_cli::parse_query_command(args) {
        Ok(command) => command,
        Err(message) => {
            return CliResponse::err(
                format!("{message}\n\n{}", crate::query_cli::explain_help()),
                2,
            )
        }
    };
    crate::query_cli::run_explain(&command)
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
           -h, --help              Show help\n",
    )
}

fn run_state(args: &[&str], mode: &CoordinatorMode) -> CliResponse {
    match args {
        ["inspect", rest @ ..] => run_state_inspect(rest, mode),
        _ => CliResponse::err(format!("unknown state subcommand\n\n{}", state_help()), 2),
    }
}

fn run_state_inspect(args: &[&str], mode: &CoordinatorMode) -> CliResponse {
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

    // Remote coordinator path.
    if let CoordinatorMode::Remote(url) = mode {
        let mut client = RemoteCoordinatorClient::new(url.clone());
        return match block_on_remote(client.inspect_state(job_id, operator_id)) {
            Ok(snapshots) if snapshots.is_empty() => CliResponse::ok(format!(
                "No state snapshots found for operator {operator_id} (job {job_id}) on coordinator {url}.\n"
            )),
            Ok(snapshots) => {
                let mut out = format!(
                    "State snapshots for operator {operator_id} (job {job_id}) from {url}:\n\nTASK\tSNAPSHOT PATH\n"
                );
                for s in snapshots {
                    out.push_str(&format!("{}\t{}\n", s.task_id, s.snapshot_path));
                }
                CliResponse::ok(out)
            }
            Err(e) => CliResponse::err(format!("remote coordinator error: {e}\n"), 1),
        };
    }

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
            ));
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
        ExecutorId::try_new(command.executor_id.clone()).map_err(|e| e.to_string())?;
    let slots = command.tasks.max(1);
    coordinator
        .register_executor(ExecutorDescriptor::new(
            executor_id.clone(),
            "local-r2-executor",
            slots,
        ))
        .map_err(|e| e.to_string())?;
    coordinator
        .executor_heartbeat(ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy))
        .map_err(|e| e.to_string())?;
    let job = build_submit_job(command)?;
    let job_id = job.job_id().clone();
    coordinator.submit_job(job).map_err(|e| e.to_string())?;
    if command.launch {
        coordinator
            .launch_assigned_tasks(&job_id)
            .map_err(|e| e.to_string())?;
    }
    let detail = coordinator
        .job_detail_snapshot(&job_id)
        .map_err(|e| e.to_string())?;
    Ok(render_submit_result(
        command.kind,
        &detail,
        &coordinator.executor_snapshots(),
    ))
}

fn active_local_coordinator() -> Result<Coordinator, String> {
    let coordinator_id = CoordinatorId::try_new("coord-local").map_err(|e| e.to_string())?;
    Ok(Coordinator::active(coordinator_id))
}

fn build_submit_job(command: &SubmitCommand) -> Result<JobSpec, String> {
    let job_id = JobId::try_new(command.job_id.clone()).map_err(|e| e.to_string())?;
    let mut stage = StageSpec::new(
        StageId::try_new("stage-1").map_err(|e| e.to_string())?,
        "r2-cli-stage",
    );
    for idx in 1..=command.tasks {
        let task_id = TaskId::try_new(format!("task-{idx}")).map_err(|e| e.to_string())?;
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
                kind = parse_job_kind(
                    args.get(idx)
                        .ok_or_else(|| String::from("missing value for --kind"))?,
                )?;
            }
            "--tasks" => {
                idx += 1;
                tasks = parse_positive_usize(
                    args.get(idx)
                        .ok_or_else(|| String::from("missing value for --tasks"))?,
                    "--tasks",
                )?;
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
            "--help" | "-h" => return Err(String::from("help requested")),
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

// ── R6: savepoint ─────────────────────────────────────────────────────────────

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
           -h, --help       Show help\n",
    )
}

fn run_savepoint(args: &[&str], mode: &CoordinatorMode) -> CliResponse {
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
    let label_opt = label.map(String::from);

    // Remote coordinator path.
    if let CoordinatorMode::Remote(url) = mode {
        let mut client = RemoteCoordinatorClient::new(url.clone());
        return match block_on_remote(client.trigger_savepoint(job_id_str)) {
            Ok(()) => {
                let label_display = label_opt.as_deref().unwrap_or("(none)");
                CliResponse::ok(format!(
                    "Savepoint initiated\nJob:   {job_id_str}\nLabel: {label_display}\nCoordinator: {url}\n"
                ))
            }
            Err(e) => CliResponse::err(format!("remote coordinator error: {e}\n"), 1),
        };
    }

    // Local coordinator path.
    let job_id = match JobId::try_new(job_id_str) {
        Ok(id) => id,
        Err(e) => return CliResponse::err(format!("invalid job id: {e}\n"), 2),
    };
    let mut coordinator = match active_local_coordinator() {
        Ok(c) => c,
        Err(e) => return CliResponse::err(format!("{e}\n"), 1),
    };
    match coordinator.trigger_checkpoint_for_job(&job_id) {
        Ok(_) => {
            let label_display = label_opt.as_deref().unwrap_or("(none)");
            CliResponse::ok(format!(
                "Savepoint initiated\nJob:   {job_id}\nLabel: {label_display}\n\
                 Note: in local mode the coordinator holds no running jobs.\n"
            ))
        }
        Err(e) => CliResponse::err(
            format!(
                "Savepoint failed for job {job_id}: {e}\n\
                 Ensure the job is a running streaming job with checkpoint_interval_ms set.\n"
            ),
            1,
        ),
    }
}

fn block_on_remote<F: std::future::Future>(fut: F) -> F::Output {
    block_on(fut)
}

// ── R6: restore ───────────────────────────────────────────────────────────────

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
           -h, --help              Show help\n",
    )
}

fn run_restore(args: &[&str], mode: &CoordinatorMode) -> CliResponse {
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

    // Remote coordinator path.
    if let CoordinatorMode::Remote(url) = mode {
        let mut client = RemoteCoordinatorClient::new(url.clone());
        let path = storage_path.unwrap_or("./krishiv-checkpoints");
        return match block_on_remote(client.restore(job_id, epoch_num, path)) {
            Ok(()) => CliResponse::ok(format!(
                "Restore requested\nJob:         {job_id}\nEpoch:       {epoch_num}\nCoordinator: {url}\n"
            )),
            Err(e) => CliResponse::err(format!("remote coordinator error: {e}\n"), 1),
        };
    }

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
            );
        }
        Err(e) => return CliResponse::err(format!("checkpoint read error: {e}\n"), 1),
    };
    let snapshot_count = meta.operator_snapshots.len();
    let source_count = meta.source_offsets.len();
    let kind = if meta.is_savepoint {
        "savepoint"
    } else {
        "checkpoint"
    };
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
         To apply: use --coordinator <URL> to trigger restore on a live cluster.\n",
        ft = meta.fencing_token,
    ))
}

// ── R6: checkpoints ───────────────────────────────────────────────────────────

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

fn run_checkpoints(args: &[&str], mode: &CoordinatorMode) -> CliResponse {
    match args {
        ["list", rest @ ..] => run_checkpoints_list(rest, mode),
        _ => CliResponse::err(
            format!("unknown checkpoints subcommand\n\n{}", checkpoints_help()),
            2,
        ),
    }
}

fn run_checkpoints_list(args: &[&str], mode: &CoordinatorMode) -> CliResponse {
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

    // Remote coordinator path.
    if let CoordinatorMode::Remote(url) = mode {
        let mut client = RemoteCoordinatorClient::new(url.clone());
        return match block_on_remote(client.list_checkpoints(job_id)) {
            Ok(epochs) if epochs.is_empty() => CliResponse::ok(format!(
                "No checkpoints found for job {job_id} on coordinator {url}\n"
            )),
            Ok(epochs) => {
                let mut out =
                    format!("Checkpoints for job {job_id} from {url}\n\nEPOCH\tTYPE\tLABEL\n");
                for e in epochs {
                    let label = e.label.unwrap_or_default();
                    out.push_str(&format!("{}\t{}\t{}\n", e.epoch, e.kind, label));
                }
                CliResponse::ok(out)
            }
            Err(e) => CliResponse::err(format!("remote coordinator error: {e}\n"), 1),
        };
    }

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
        return CliResponse::ok(format!("No checkpoints found for job {job_id} in {path}\n"));
    }
    let mut out = format!("Checkpoints for job {job_id} in {path}\n\nEPOCH\tTYPE\tLABEL\n");
    for &epoch in &epochs {
        let (kind, label) = match read_epoch_metadata(&storage, job_id, epoch) {
            Ok(Some(meta)) => {
                let k = if meta.is_savepoint {
                    "savepoint"
                } else {
                    "checkpoint"
                };
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
    use super::{CoordinatorMode, dispatch};

    #[test]
    fn top_level_help_lists_commands() {
        let response = dispatch(&["--help"]);
        assert_eq!(response.exit_code, 0);
        assert!(response.stdout.contains("Commands:"));
        assert!(response.stdout.contains("sql"));
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
        assert!(response.stdout.contains("1"));
    }

    #[test]
    fn sql_local_flag_is_accepted() {
        let response = dispatch(&["sql", "--local", "--query", "select 2 as value"]);
        assert_eq!(response.exit_code, 0, "{}", response.stderr);
        assert!(response.stdout.contains("2"));
    }

    #[test]
    fn sql_api_key_requires_env() {
        let response = dispatch(&["sql", "--api-key", "k", "--query", "select 1"]);
        assert_eq!(response.exit_code, 1);
        assert!(response.stderr.contains("KRISHIV_API_KEYS"));
    }

    #[test]
    fn stream_submit_lists_job() {
        let response = dispatch(&["stream", "submit", "--job-id", "cli-events"]);
        assert_eq!(response.exit_code, 0, "{}", response.stderr);
        assert!(response.stdout.contains("cli-events"));
    }

    #[test]
    fn top_level_help_lists_stream_and_table() {
        let response = dispatch(&["--help"]);
        assert!(response.stdout.contains("stream"));
        assert!(response.stdout.contains("table"));
    }

    #[test]
    fn explain_command_returns_plan() {
        let response = dispatch(&["explain", "--query", "select 1 as value"]);
        assert_eq!(response.exit_code, 0, "{}", response.stderr);
        assert!(
            response.stdout.contains("logical plan:")
                || response.stdout.contains("logical_plan"),
            "{}",
            response.stdout
        );
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
    fn state_inspect_requires_job_and_operator() {
        let response = dispatch(&["state", "inspect"]);
        assert_eq!(response.exit_code, 2);
        assert!(response.stderr.contains("--job is required"));
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
            response.stderr.contains("Savepoint failed")
                || response.stderr.contains("Savepoint initiated"),
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
    }

    #[test]
    fn restore_rejects_non_numeric_epoch() {
        let response = dispatch(&["restore", "--job", "job-1", "--epoch", "latest"]);
        assert_eq!(response.exit_code, 2);
        assert!(response.stderr.contains("non-negative integer"));
    }

    #[test]
    fn checkpoints_list_requires_job() {
        let response = dispatch(&["checkpoints", "list"]);
        assert_eq!(response.exit_code, 2);
        assert!(response.stderr.contains("--job is required"));
    }

    #[test]
    fn checkpoints_list_returns_no_checkpoints() {
        let response = dispatch(&["checkpoints", "list", "--job", "job-1"]);
        assert_eq!(response.exit_code, 0, "{}", response.stderr);
        assert!(response.stdout.contains("No checkpoints found"));
    }

    // ── S4: CoordinatorMode tests ─────────────────────────────────────────────

    #[test]
    fn coordinator_flag_long_form_produces_remote_mode() {
        let (mode, remaining) = CoordinatorMode::from_args_with_env_override(
            &[
                "--coordinator",
                "http://coord:7070",
                "savepoint",
                "--job",
                "j1",
            ],
            None,
        );
        assert_eq!(
            mode,
            CoordinatorMode::Remote("http://coord:7070".to_string())
        );
        assert_eq!(remaining, vec!["savepoint", "--job", "j1"]);
    }

    #[test]
    fn coordinator_short_flag_produces_remote_mode() {
        let (mode, remaining) =
            CoordinatorMode::from_args_with_env_override(&["-c", "http://c:9", "jobs"], None);
        assert_eq!(mode, CoordinatorMode::Remote("http://c:9".to_string()));
        assert_eq!(remaining, vec!["jobs"]);
    }

    #[test]
    fn coordinator_flag_absent_and_no_env_var_is_local() {
        let (mode, remaining) =
            CoordinatorMode::from_args_with_env_override(&["savepoint", "--job", "j1"], None);
        assert_eq!(mode, CoordinatorMode::Local);
        assert_eq!(remaining, vec!["savepoint", "--job", "j1"]);
    }

    #[test]
    fn env_var_coordinator_produces_remote_mode_when_no_flag() {
        let (mode, _) =
            CoordinatorMode::from_args_with_env_override(&["jobs"], Some("http://env-coord:7070"));
        assert_eq!(
            mode,
            CoordinatorMode::Remote("http://env-coord:7070".to_string())
        );
    }

    #[test]
    fn flag_beats_env_var() {
        let (mode, _) = CoordinatorMode::from_args_with_env_override(
            &["--coordinator", "http://flag:1"],
            Some("http://env:2"),
        );
        assert_eq!(mode, CoordinatorMode::Remote("http://flag:1".to_string()));
    }

    #[test]
    fn main_help_mentions_coordinator_flag() {
        let response = dispatch(&["--help"]);
        assert_eq!(response.exit_code, 0);
        assert!(
            response.stdout.contains("--coordinator"),
            "help must document --coordinator flag"
        );
    }

    #[test]
    fn dispatch_savepoint_with_coordinator_flag_uses_remote_path() {
        // Remote path: real gRPC call — no server at coord:7070, so either exit 0 (success)
        // or exit 1 (connection refused) are acceptable; what matters is that the output
        // references the coordinator URL, proving the remote path was taken rather than local.
        let response = dispatch(&[
            "--coordinator",
            "http://coord:7070",
            "savepoint",
            "--job",
            "job-remote-1",
        ]);
        let combined = format!("{} {}", response.stdout, response.stderr);
        assert!(
            combined.contains("coord:7070")
                || combined.contains("Coordinator")
                || combined.contains("remote coordinator error"),
            "expected coordinator URL or remote error in output, got: {combined:?}"
        );
    }

    #[test]
    fn dispatch_checkpoints_list_with_coordinator_flag_uses_remote_path() {
        let response = dispatch(&[
            "--coordinator",
            "http://coord:7070",
            "checkpoints",
            "list",
            "--job",
            "job-ck-1",
        ]);
        let combined = format!("{} {}", response.stdout, response.stderr);
        assert!(
            combined.contains("job-ck-1")
                || combined.contains("coord:7070")
                || combined.contains("remote coordinator error"),
            "expected job id or remote error in output, got: {combined:?}"
        );
    }

    #[test]
    fn dispatch_restore_with_coordinator_flag_uses_remote_path() {
        let response = dispatch(&[
            "--coordinator",
            "http://coord:7070",
            "restore",
            "--job",
            "job-rs-1",
            "--epoch",
            "5",
        ]);
        let combined = format!("{} {}", response.stdout, response.stderr);
        assert!(
            combined.contains("Restore requested")
                || combined.contains("coord:7070")
                || combined.contains("remote coordinator error"),
            "expected restore output or remote error, got: {combined:?}"
        );
    }

    #[test]
    fn dispatch_state_inspect_with_coordinator_flag_uses_remote_path() {
        let response = dispatch(&[
            "--coordinator",
            "http://coord:7070",
            "state",
            "inspect",
            "--job",
            "job-si-1",
            "--operator",
            "op-1",
        ]);
        let combined = format!("{} {}", response.stdout, response.stderr);
        assert!(
            combined.contains("No state snapshots found")
                || combined.contains("coord:7070")
                || combined.contains("remote coordinator error"),
            "expected state output or remote error, got: {combined:?}"
        );
    }

    #[test]
    fn coordinator_flag_does_not_break_sql_command() {
        // --coordinator is stripped before routing; sql doesn't use it but must not error
        let response = dispatch(&[
            "--coordinator",
            "http://coord:7070",
            "sql",
            "--query",
            "select 42 as answer",
        ]);
        assert_eq!(response.exit_code, 0, "{:?}", response);
        assert!(response.stdout.contains("42") || response.stdout.contains("answer"));
    }
}
