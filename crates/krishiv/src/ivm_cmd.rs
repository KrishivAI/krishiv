//! `krishiv ivm` — incremental view maintenance (delta-batch) jobs.
//!
//! `ivm run` builds a [`CompiledJob`] with a CDC source, an incremental view
//! query, and a sink, then dispatches it through `Session::submit`. Without a
//! coordinator the view is maintained embedded in-process; with
//! `--coordinator/-c` it is maintained on the remote coordinator (distributed),
//! which is the path to exercise the cluster's distributed incremental engine.

// Deliberate sync-over-async boundary module (Phase 51 async contract):
// block_on here bridges a synchronous public surface to the async core.
#![allow(clippy::disallowed_methods)]

use krishiv_api::{CompiledJob, Session, SessionBuilder, SinkSpec, SourceSpec};
use krishiv_common::async_util::block_on;

use crate::cli::{CliResponse, CoordinatorMode};

pub fn run_ivm(args: &[&str], coordinator: &CoordinatorMode) -> CliResponse {
    match args {
        [] | ["--help"] | ["-h"] => CliResponse::ok(format!("{}\n", ivm_help())),
        ["run", "--help"] | ["run", "-h"] => CliResponse::ok(format!("{}\n", ivm_help())),
        ["run", rest @ ..] => run_ivm_job(rest, coordinator),
        [unknown, ..] => CliResponse::err(
            format!("unknown ivm subcommand: {unknown}\n\n{}", ivm_help()),
            2,
        ),
    }
}

pub fn ivm_help() -> &'static str {
    "Incremental view maintenance (delta-batch) jobs.\n\
     \n\
     Usage:\n\
       krishiv ivm run --job-id <ID> --sql <QUERY> --source <name>=<path> --sink <path> [OPTIONS]\n\
     \n\
     Options:\n\
       --job-id <ID>            View/job name (required)\n\
       --sql <QUERY>            The view's SQL over the source table(s) (required)\n\
       --source <name>=<path>   A CDC source table (repeatable; at least one required)\n\
       --sink <path>            Output file for the net materialized view (required)\n\
       --source-format <fmt>    parquet|csv|json for sources (default: csv)\n\
       --sink-format <fmt>      parquet|csv|json for the sink (default: json)\n\
     \n\
     With --coordinator/-c the view is maintained on the remote coordinator\n\
     (distributed); otherwise it runs embedded in-process.\n\
     \n\
     Example:\n\
       krishiv -c http://coordinator:2002 ivm run --job-id sales \\\n\
         --sql \"SELECT k, SUM(v) AS total FROM t GROUP BY k\" \\\n\
         --source t=./changes.csv --sink ./agg.ndjson\n"
}

struct IvmRunSpec {
    job_id: String,
    sql: String,
    sources: Vec<(String, String)>,
    sink: String,
    source_format: String,
    sink_format: String,
}

fn parse_ivm_run(args: &[&str]) -> Result<IvmRunSpec, String> {
    let mut job_id = None;
    let mut sql = None;
    let mut sources: Vec<(String, String)> = Vec::new();
    let mut sink = None;
    let mut source_format = String::from("csv");
    let mut sink_format = String::from("json");
    let mut idx = 0;
    while idx < args.len() {
        let Some(&arg) = args.get(idx) else {
            break;
        };
        match arg {
            "--job-id" => {
                idx += 1;
                job_id = Some(value_at(args, idx, "--job-id")?);
            }
            "--sql" => {
                idx += 1;
                sql = Some(value_at(args, idx, "--sql")?);
            }
            "--source" => {
                idx += 1;
                let raw = value_at(args, idx, "--source")?;
                let (name, path) = raw
                    .split_once('=')
                    .ok_or_else(|| format!("--source must be <name>=<path>, got '{raw}'"))?;
                sources.push((name.to_string(), path.to_string()));
            }
            "--sink" => {
                idx += 1;
                sink = Some(value_at(args, idx, "--sink")?);
            }
            "--source-format" => {
                idx += 1;
                source_format = value_at(args, idx, "--source-format")?;
            }
            "--sink-format" => {
                idx += 1;
                sink_format = value_at(args, idx, "--sink-format")?;
            }
            unknown => return Err(format!("unknown option: {unknown}")),
        }
        idx += 1;
    }
    if sources.is_empty() {
        return Err(String::from(
            "at least one --source <name>=<path> is required",
        ));
    }
    Ok(IvmRunSpec {
        job_id: job_id.ok_or_else(|| String::from("missing required --job-id"))?,
        sql: sql.ok_or_else(|| String::from("missing required --sql"))?,
        sources,
        sink: sink.ok_or_else(|| String::from("missing required --sink"))?,
        source_format,
        sink_format,
    })
}

fn value_at(args: &[&str], idx: usize, flag: &str) -> Result<String, String> {
    args.get(idx)
        .map(|v| (*v).to_string())
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn ivm_session(coordinator: &CoordinatorMode) -> Result<Session, krishiv_api::KrishivError> {
    match coordinator {
        // The incremental engine maintains the view over the coordinator's HTTP
        // management API (`/api/v1/ivm/*`); point `-c` at that HTTP endpoint.
        CoordinatorMode::Remote(url) => SessionBuilder::new()
            .with_coordinator(url.clone())
            .with_coordinator_http(url.clone())
            .with_remote_execution(true)
            .build(),
        CoordinatorMode::Local => SessionBuilder::new().build(),
    }
}

fn run_ivm_job(args: &[&str], coordinator: &CoordinatorMode) -> CliResponse {
    let spec = match parse_ivm_run(args) {
        Ok(s) => s,
        Err(e) => return CliResponse::err(format!("{e}\n\n{}", ivm_help()), 2),
    };
    let session = match ivm_session(coordinator) {
        Ok(s) => s,
        Err(e) => return CliResponse::err(format!("{e}\n"), 1),
    };

    let sources: Vec<SourceSpec> = spec
        .sources
        .iter()
        .map(|(name, path)| SourceSpec::cdc(name, &spec.source_format, path))
        .collect();
    let sinks = vec![SinkSpec::new("out", &spec.sink_format, &spec.sink)];
    let job = CompiledJob::new(&spec.job_id, &spec.sql, sources, sinks, false);

    match block_on(session.submit(job)) {
        Ok(handle) => CliResponse::ok(format!(
            "Submitted incremental job {} ({:?}); net view written to {}\n",
            spec.job_id,
            handle.status(),
            spec.sink
        )),
        Err(e) => CliResponse::err(format!("{e}\n"), 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_requires_source_sql_sink() {
        assert!(parse_ivm_run(&["--job-id", "j"]).is_err());
        let spec = parse_ivm_run(&[
            "--job-id",
            "j",
            "--sql",
            "SELECT * FROM t",
            "--source",
            "t=./a.csv",
            "--sink",
            "./out.json",
        ])
        .expect("valid args parse");
        assert_eq!(spec.job_id, "j");
        assert_eq!(spec.sources, vec![("t".to_string(), "./a.csv".to_string())]);
        assert_eq!(spec.sink, "./out.json");
    }

    #[test]
    fn ivm_run_embedded_materializes_view() {
        // Embedded path (no coordinator): the `ivm run` command maintains the view
        // in-process and writes the net table — the same submit() path used for the
        // distributed engine, exercised without a cluster.
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("kv.csv");
        let output = dir.path().join("agg.ndjson");
        std::fs::write(&input, "k,v\na,1\nb,2\na,3\n").unwrap();

        let resp = run_ivm(
            &[
                "run",
                "--job-id",
                "agg",
                "--sql",
                "SELECT k, SUM(v) AS total FROM t GROUP BY k",
                "--source",
                &format!("t={}", input.to_str().unwrap()),
                "--sink",
                output.to_str().unwrap(),
            ],
            &CoordinatorMode::Local,
        );
        assert_eq!(resp.exit_code, 0, "stderr: {}", resp.stderr);

        let written = std::fs::read_to_string(&output).unwrap();
        assert!(written.contains("\"total\":4"), "a=4: {written}");
        assert!(written.contains("\"total\":2"), "b=2: {written}");
    }
}
