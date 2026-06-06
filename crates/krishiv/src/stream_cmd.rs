//! `krishiv stream` — continuous streaming jobs (submit / push / poll).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use krishiv_api::{
    InProcessCluster, LocalWindowExecutionSpec, LocalWindowKind, QueryResult, Session,
    SessionBuilder,
};
use krishiv_common::async_util::block_on;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::cli::CliResponse;

pub fn stream_help() -> String {
    String::from(
        "Continuous streaming window jobs.\n\
         \n\
         Note: In this release, stream commands run in an in-process cluster.\n\
         Jobs are not persisted between CLI invocations — state is lost when\n\
         this process exits. Use --coordinator <URL> for durable job management.\n\
         \n\
         Usage:\n\
           krishiv stream submit --job-id <ID> [WINDOW OPTIONS]\n\
           krishiv stream push --job-id <ID> --parquet <path>\n\
           krishiv stream poll --job-id <ID>\n\
         \n\
         Window options (submit):\n\
           --key-column <COL>           Grouping key (default: user_id)\n\
           --event-time-column <COL>    Event-time column (default: ts)\n\
           --watermark-lag-ms <MS>      Watermark lag (default: 0)\n\
           --window <tumbling|sliding|session>  Window type (default: tumbling)\n\
           --window-size-ms <MS>        Window size (default: 60000)\n\
           --slide-ms <MS>              Slide interval (sliding windows)\n\
           --session-gap-ms <MS>        Session gap (session windows)\n\
         \n\
         Examples:\n\
           krishiv stream submit --job-id events --window sliding --slide-ms 30000\n\
           krishiv stream push --job-id events --parquet ./batch.parquet\n\
           krishiv stream poll --job-id events\n",
    )
}

pub fn run_stream(args: &[&str]) -> CliResponse {
    match args {
        [] | ["--help"] | ["-h"] => CliResponse::ok(format!("{}\n", stream_help())),
        ["submit", "--help"] | ["submit", "-h"] => {
            CliResponse::ok(format!("{}\n", stream_submit_help()))
        }
        ["submit", rest @ ..] => run_stream_submit(rest),
        ["push", rest @ ..] => run_stream_push(rest),
        ["poll", rest @ ..] => run_stream_poll(rest),
        [unknown, ..] => CliResponse::err(
            format!("unknown stream subcommand: {unknown}\n\n{}", stream_help()),
            2,
        ),
    }
}

fn stream_submit_help() -> &'static str {
    "Submit a continuous streaming window job.\n\
     \n\
     Usage: krishiv stream submit --job-id <ID> [OPTIONS]\n"
}

fn run_stream_submit(args: &[&str]) -> CliResponse {
    let spec = match parse_stream_submit(args) {
        Ok(s) => s,
        Err(e) => return CliResponse::err(format!("{e}\n\n{}", stream_submit_help()), 2),
    };
    let session = match stream_session() {
        Ok(s) => s,
        Err(e) => return CliResponse::err(format!("{e}\n"), 1),
    };
    eprintln!(
        "[local-mode] Stream job running in-process — state will be lost when this process exits."
    );
    match session.submit_stream_job(&spec.job_id, spec.window_spec) {
        Ok(id) => CliResponse::ok(format!(
            "Submitted continuous stream job {id} (window: {})\n",
            spec.window_label
        )),
        Err(e) => CliResponse::err(format!("{e}\n"), 1),
    }
}

fn run_stream_push(args: &[&str]) -> CliResponse {
    let (job_id, path) = match parse_job_and_parquet(args) {
        Ok(v) => v,
        Err(e) => return CliResponse::err(format!("{e}\n\n{}", stream_help()), 2),
    };
    let batches = match read_parquet_batches(&path) {
        Ok(b) => b,
        Err(e) => return CliResponse::err(format!("{e}\n"), 1),
    };
    let session = match stream_session() {
        Ok(s) => s,
        Err(e) => return CliResponse::err(format!("{e}\n"), 1),
    };
    match session.push_stream_job_input(&job_id, batches) {
        Ok(()) => CliResponse::ok(format!(
            "Pushed input to stream job {job_id} from {}\n",
            path.display()
        )),
        Err(e) => CliResponse::err(format!("{e}\n"), 1),
    }
}

fn run_stream_poll(args: &[&str]) -> CliResponse {
    let job_id = match parse_job_id_only(args) {
        Ok(id) => id,
        Err(e) => return CliResponse::err(format!("{e}\n\n{}", stream_help()), 2),
    };
    let session = match stream_session() {
        Ok(s) => s,
        Err(e) => return CliResponse::err(format!("{e}\n"), 1),
    };
    match block_on(session.poll_stream_job(&job_id)) {
        Ok(batches) => {
            let result = QueryResult::new(batches);
            match result.pretty() {
                Ok(table) => CliResponse::ok(format!(
                    "Polled stream job {job_id} ({} rows)\n{table}\n",
                    result.row_count()
                )),
                Err(e) => CliResponse::err(format!("{e}\n"), 1),
            }
        }
        Err(e) => CliResponse::err(format!("{e}\n"), 1),
    }
}

struct StreamSubmitSpec {
    job_id: String,
    window_spec: LocalWindowExecutionSpec,
    window_label: String,
}

fn parse_stream_submit(args: &[&str]) -> Result<StreamSubmitSpec, String> {
    let mut job_id = None;
    let mut key_column = String::from("user_id");
    let mut event_time_column = String::from("ts");
    let mut watermark_lag_ms = 0u64;
    let mut window = String::from("tumbling");
    let mut window_size_ms = 60_000u64;
    let mut slide_ms = 30_000u64;
    let mut session_gap_ms = 5_000u64;
    let mut idx = 0;
    while idx < args.len() {
        match args[idx] {
            "--job-id" => {
                idx += 1;
                job_id = Some(
                    args.get(idx)
                        .ok_or_else(|| String::from("missing value for --job-id"))?
                        .to_string(),
                );
            }
            "--key-column" => {
                idx += 1;
                key_column = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --key-column"))?
                    .to_string();
            }
            "--event-time-column" => {
                idx += 1;
                event_time_column = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --event-time-column"))?
                    .to_string();
            }
            "--watermark-lag-ms" => {
                idx += 1;
                watermark_lag_ms = parse_u64_arg(args.get(idx).copied(), "--watermark-lag-ms")?;
            }
            "--window" => {
                idx += 1;
                window = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --window"))?
                    .to_string();
            }
            "--window-size-ms" => {
                idx += 1;
                window_size_ms = parse_u64_arg(args.get(idx).copied(), "--window-size-ms")?;
            }
            "--slide-ms" => {
                idx += 1;
                slide_ms = parse_u64_arg(args.get(idx).copied(), "--slide-ms")?;
            }
            "--session-gap-ms" => {
                idx += 1;
                session_gap_ms = parse_u64_arg(args.get(idx).copied(), "--session-gap-ms")?;
            }
            "--help" | "-h" => return Err(String::from("help requested")),
            unknown => return Err(format!("unknown option: {unknown}")),
        }
        idx += 1;
    }
    let job_id = job_id.ok_or_else(|| String::from("missing required --job-id"))?;
    let (window_kind, window_label) = match window.as_str() {
        "tumbling" => (
            LocalWindowKind::Tumbling,
            format!("tumbling/{window_size_ms}ms"),
        ),
        "sliding" => (
            LocalWindowKind::Sliding { slide_ms },
            format!("sliding/{window_size_ms}ms slide {slide_ms}ms"),
        ),
        "session" => (
            LocalWindowKind::Session {
                gap_ms: session_gap_ms,
            },
            format!("session/gap {session_gap_ms}ms"),
        ),
        other => return Err(format!("unsupported --window: {other}")),
    };
    Ok(StreamSubmitSpec {
        job_id,
        window_label,
        window_spec: LocalWindowExecutionSpec {
                key_column_type: String::from("utf8"),
            key_column,
            event_time_column,
            watermark_lag_ms,
            window_kind,
            window_size_ms,
            agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
        },
    })
}

fn parse_job_and_parquet(args: &[&str]) -> Result<(String, PathBuf), String> {
    let mut job_id = None;
    let mut path = None;
    let mut idx = 0;
    while idx < args.len() {
        match args[idx] {
            "--job-id" => {
                idx += 1;
                job_id = Some(
                    args.get(idx)
                        .ok_or_else(|| String::from("missing value for --job-id"))?
                        .to_string(),
                );
            }
            "--parquet" => {
                idx += 1;
                path =
                    Some(PathBuf::from(args.get(idx).ok_or_else(|| {
                        String::from("missing value for --parquet")
                    })?));
            }
            unknown => return Err(format!("unknown option: {unknown}")),
        }
        idx += 1;
    }
    let job_id = job_id.ok_or_else(|| String::from("missing required --job-id"))?;
    let path = path.ok_or_else(|| String::from("missing required --parquet <path>"))?;
    Ok((job_id, path))
}

fn parse_job_id_only(args: &[&str]) -> Result<String, String> {
    let mut job_id = None;
    let mut idx = 0;
    while idx < args.len() {
        match args[idx] {
            "--job-id" => {
                idx += 1;
                job_id = Some(
                    args.get(idx)
                        .ok_or_else(|| String::from("missing value for --job-id"))?
                        .to_string(),
                );
            }
            unknown => return Err(format!("unknown option: {unknown}")),
        }
        idx += 1;
    }
    job_id.ok_or_else(|| String::from("missing required --job-id"))
}

fn parse_u64_arg(value: Option<&str>, flag: &str) -> Result<u64, String> {
    let value = value.ok_or_else(|| format!("missing value for {flag}"))?;
    value
        .parse::<u64>()
        .map_err(|_| format!("{flag} must be a non-negative integer"))
}

fn shared_stream_cluster() -> Arc<InProcessCluster> {
    static CLUSTER: OnceLock<Arc<InProcessCluster>> = OnceLock::new();
    Arc::clone(CLUSTER.get_or_init(|| {
        Arc::new(InProcessCluster::new().expect("in-process cluster for krishiv stream CLI"))
    }))
}

fn stream_session() -> Result<Session, krishiv_api::KrishivError> {
    SessionBuilder::new()
        .with_in_process_cluster(shared_stream_cluster())
        .build()
}

fn read_parquet_batches(path: &PathBuf) -> Result<Vec<krishiv_api::RecordBatch>, String> {
    let file =
        std::fs::File::open(path).map_err(|e| format!("failed to open {}: {e}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("parquet read error: {e}"))?
        .build()
        .map_err(|e| format!("parquet reader error: {e}"))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("parquet batch error: {e}"))
}

#[cfg(test)]
mod tests {
    use super::parse_stream_submit;

    #[test]
    fn parses_sliding_window_submit() {
        let spec = parse_stream_submit(&[
            "--job-id",
            "events",
            "--window",
            "sliding",
            "--slide-ms",
            "1000",
        ])
        .unwrap();
        assert_eq!(spec.job_id, "events");
        assert!(matches!(
            spec.window_spec.window_kind,
            krishiv_api::LocalWindowKind::Sliding { slide_ms: 1000 }
        ));
    }
}
