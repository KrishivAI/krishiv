#![forbid(unsafe_code)]

//! Per-job coordinator process (JCP) for bare-metal / shared-metadata deployments.
//!
//! Shares a durable metadata store with the cluster control plane and runs
//! orchestration loops scoped to a single [`krishiv_proto::JobId`].

use std::env;
use std::error::Error;

use krishiv_proto::{JobId, JobKind, JobSpec};
use krishiv_scheduler::{CoordinatorDaemonConfig, JobCoordinator, build_shared_coordinator};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct JobSpecEnv {
    job_id: String,
    name: String,
    mode: String,
    tasks: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut job_id = None;
    let mut metadata_path = None;
    let mut help = false;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--job-id" => job_id = Some(args.next().ok_or("missing --job-id")?),
            "--metadata-path" => metadata_path = Some(args.next().ok_or("missing --metadata-path")?),
            "--help" | "-h" => help = true,
            unknown => return Err(format!("unknown option: {unknown}").into()),
        }
    }
    if help {
        println!(
            "Usage: krishiv-job-coordinator --job-id <ID> --metadata-path <PATH>\n\
             Optional env KRISHIV_JOB_SPEC_JSON for first-time job submit.\n"
        );
        return Ok(());
    }
    let job_id_str = job_id.ok_or("--job-id is required")?;
    let metadata_path = metadata_path.ok_or("--metadata-path is required")?;
    let job_id = JobId::try_new(&job_id_str).map_err(|e| format!("invalid job id: {e}"))?;

    let config = CoordinatorDaemonConfig {
        coordinator_id: format!("jcp-{job_id_str}"),
        grpc_addr: "127.0.0.1:0".parse().unwrap(),
        http_addr: None,
        shuffle_dir: None,
        metadata_backend: Some(String::from("json")),
        metadata_path: Some(metadata_path.into()),
        help: false,
    };

    let shared = build_shared_coordinator(&config)?;

    if let Ok(spec_json) = env::var("KRISHIV_JOB_SPEC_JSON") {
        let env_spec: JobSpecEnv = serde_json::from_str(&spec_json)
            .map_err(|e| format!("KRISHIV_JOB_SPEC_JSON: {e}"))?;
        let kind = match env_spec.mode.as_str() {
            "streaming" => JobKind::Streaming,
            _ => JobKind::Batch,
        };
        let submit_id = JobId::try_new(&env_spec.job_id)
            .map_err(|e| format!("job spec job_id: {e}"))?;
        if submit_id != job_id {
            return Err("KRISHIV_JOB_SPEC_JSON job_id must match --job-id".into());
        }
        let mut coord = shared.write().map_err(|_| "lock poisoned")?;
        if coord.job_snapshot(&job_id).is_err() {
            use krishiv_proto::{StageId, StageSpec, TaskId, TaskSpec};
            let stage_id = StageId::try_new("stage-1").map_err(|e| e.to_string())?;
            let mut stage = StageSpec::new(stage_id, format!("{}-stage", env_spec.name));
            for i in 1..=env_spec.tasks.max(1) {
                let task_id =
                    TaskId::try_new(format!("task-{i}")).map_err(|e| e.to_string())?;
                stage = stage.with_task(TaskSpec::new(task_id, format!("task-{i}")));
            }
            let spec = JobSpec::new(job_id.clone(), env_spec.name, kind).with_stage(stage);
            coord.submit_job(spec).map_err(|e| e.to_string())?;
        }
    }

    let jcp = JobCoordinator::new(job_id.clone(), shared.clone());
    jcp.spawn_job_orchestration_loops();

    println!("Krishiv job coordinator running for job {job_id}");
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        if let Ok(mut coord) = shared.write() {
            let _ = coord.sync_job_from_metadata_store(&job_id);
        }
    }
}
