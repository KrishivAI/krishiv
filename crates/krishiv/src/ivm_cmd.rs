//! `krishiv ivm` — incremental view maintenance (delta-batch) jobs.
//!
//! `ivm run` builds a [`CompiledJob`] with a CDC source, an incremental view
//! query, and a sink, then dispatches it through `Session::submit`.
//!
//! The placement is chosen by `--mode`, defaulting to `embedded` without a
//! coordinator and `distributed` with one. The coordinator exposes **two**
//! endpoints on **different ports** — Arrow Flight (data plane, the global
//! `--coordinator/-c` flag, as in `krishiv sql`) and HTTP management, where
//! `/api/v1/ivm/*` lives (`--coordinator-http`). Distributed IVM talks to the
//! HTTP one. `ivm run` used to set both from the single `-c` string, which made
//! one of the two wrong in every deployment where they differ; it now requires
//! them to be named separately.

// Deliberate sync-over-async boundary module (Phase 51 async contract):
// block_on here bridges a synchronous public surface to the async core.
#![allow(clippy::disallowed_methods)]

use krishiv_api::{CompiledJob, Session, SessionBuilder, SinkSpec, SourceSpec};
use krishiv_common::async_util::block_on;

use crate::cli::{CliResponse, CoordinatorMode};

/// Environment fallback for the coordinator's HTTP management base.
const COORDINATOR_HTTP_ENV: &str = "KRISHIV_COORDINATOR_HTTP";

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
    // One string literal per line, not `\n\` continuations. A backslash at the
    // end of a Rust string literal eats the newline *and* every leading space
    // on the next source line, so the previous form rendered `krishiv ivm
    // --help` flush-left with none of the column alignment it is written for.
    concat!(
        "Incremental view maintenance (delta-batch) jobs.\n",
        "\n",
        "Usage:\n",
        "  krishiv ivm run --job-id <ID> --sql <QUERY> --source <name>=<path> --sink <path> [OPTIONS]\n",
        "\n",
        "Options:\n",
        "  --job-id <ID>            View/job name (required)\n",
        "  --sql <QUERY>            The view's SQL over the source table(s) (required)\n",
        "  --source <name>=<path>   A CDC source table. At least one is required;\n",
        "                           repeatable, and names must be unique.\n",
        "  --sink <path>            Output file for the net materialized view (required)\n",
        "  --source-format <fmt>    parquet|csv|json for sources (default: csv)\n",
        "  --sink-format <fmt>      Sink format (default: json). parquet, csv, json and\n",
        "                           ndjson are written as one local file. Any other\n",
        "                           value is handed to the connector registry (delta,\n",
        "                           iceberg, hudi, and whatever else this build's\n",
        "                           features register); those write directories or\n",
        "                           remote endpoints, which this command cannot read\n",
        "                           back, so it reports no size for them.\n",
        "  --mode <MODE>            embedded|single-node|distributed\n",
        "                           (default: distributed with --coordinator, else embedded)\n",
        "  --coordinator-http <URL> The coordinator's HTTP management base, where\n",
        "                           /api/v1/ivm/* lives. Required in distributed mode;\n",
        "                           falls back to $KRISHIV_COORDINATOR_HTTP. Rejected in\n",
        "                           the other two modes, which never read it.\n",
        "  --checkpoint-dir <DIR>   Single-node only; rejected in the other two modes.\n",
        "                           Accepted and inert: it sets the session's checkpoint\n",
        "                           root and that directory is created, but the\n",
        "                           incremental engine neither writes checkpoints into\n",
        "                           it nor restores from it, so nothing is stored there\n",
        "                           and every rerun starts from zero.\n",
        "\n",
        "Modes:\n",
        "  embedded      Maintain the view in this process. No coordinator.\n",
        "  single-node   Also maintains the view in this process, over the same\n",
        "                connector sources and sinks and with the same output. The\n",
        "                only differences today are that a transient engine error is\n",
        "                retried up to 3 times (embedded does not retry) and that\n",
        "                --checkpoint-dir's directory is created. --coordinator is\n",
        "                required and recorded on the session, but this path never\n",
        "                dials it: an unreachable URL still completes the run.\n",
        "  distributed   Maintain the view on the remote coordinator. Requires both\n",
        "                --coordinator (Flight) and --coordinator-http (management);\n",
        "                these are different ports.\n",
        "\n",
        "Ports:\n",
        "  --coordinator/-c is the Arrow Flight endpoint (2003 for `krishiv local`)\n",
        "  and --coordinator-http is the HTTP management endpoint (2002).\n",
        "\n",
        "Example:\n",
        "  krishiv -c http://coordinator:2003 ivm run --job-id sales \\\n",
        "    --coordinator-http http://coordinator:2002 \\\n",
        "    --sql \"SELECT k, SUM(v) AS total FROM t GROUP BY k\" \\\n",
        "    --source t=./changes.csv --sink ./agg.ndjson\n",
    )
}

/// Which placement `ivm run` should maintain the view in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IvmMode {
    Embedded,
    SingleNode,
    Distributed,
}

impl IvmMode {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "embedded" => Ok(Self::Embedded),
            "single-node" | "singlenode" | "single_node" => Ok(Self::SingleNode),
            "distributed" => Ok(Self::Distributed),
            other => Err(format!(
                "unknown --mode '{other}'; expected embedded|single-node|distributed"
            )),
        }
    }

    /// The `--mode` spelling, so a rejection can name the mode it resolved to.
    fn as_str(self) -> &'static str {
        match self {
            Self::Embedded => "embedded",
            Self::SingleNode => "single-node",
            Self::Distributed => "distributed",
        }
    }
}

#[derive(Debug)]
struct IvmRunSpec {
    job_id: String,
    sql: String,
    sources: Vec<(String, String)>,
    sink: String,
    source_format: String,
    sink_format: String,
    /// `None` means "derive from whether a coordinator was given".
    mode: Option<IvmMode>,
    coordinator_http: Option<String>,
    checkpoint_dir: Option<String>,
}

fn parse_ivm_run(args: &[&str]) -> Result<IvmRunSpec, String> {
    let mut job_id = None;
    let mut sql = None;
    let mut sources: Vec<(String, String)> = Vec::new();
    let mut sink = None;
    let mut source_format = String::from("csv");
    let mut sink_format = String::from("json");
    let mut mode = None;
    let mut coordinator_http = None;
    let mut checkpoint_dir = None;
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
                // Two `--source t=...` flags mean two CDC readers feeding one
                // view table. The engine would happily interleave both into the
                // same relation and the second path's rows would appear with no
                // way to tell them apart; a repeated name is a mistake, not a
                // merge request.
                if sources.iter().any(|(existing, _)| existing == name) {
                    return Err(format!(
                        "duplicate --source name '{name}': each source table must be named once"
                    ));
                }
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
            "--mode" => {
                idx += 1;
                mode = Some(IvmMode::parse(&value_at(args, idx, "--mode")?)?);
            }
            "--coordinator-http" => {
                idx += 1;
                coordinator_http = Some(value_at(args, idx, "--coordinator-http")?);
            }
            "--checkpoint-dir" => {
                idx += 1;
                checkpoint_dir = Some(value_at(args, idx, "--checkpoint-dir")?);
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
        mode,
        coordinator_http,
        checkpoint_dir,
    })
}

fn value_at(args: &[&str], idx: usize, flag: &str) -> Result<String, String> {
    args.get(idx)
        .map(|v| (*v).to_string())
        .ok_or_else(|| format!("missing value for {flag}"))
}

/// Resolve the placement, rejecting the combinations that used to be silently
/// reinterpreted.
fn resolve_mode(spec: &IvmRunSpec, coordinator: &CoordinatorMode) -> Result<IvmMode, String> {
    let has_coordinator = matches!(coordinator, CoordinatorMode::Remote(_));
    match (spec.mode, has_coordinator) {
        (Some(IvmMode::Embedded), true) => Err(String::from(
            "--mode embedded runs the view in this process, but a coordinator is set \
             (--coordinator/-c or KRISHIV_COORDINATOR_URL). Drop one of the two.",
        )),
        (Some(IvmMode::SingleNode), false) => Err(String::from(
            "--mode single-node needs the daemon's Arrow Flight URL; pass --coordinator/-c <URL>",
        )),
        (Some(IvmMode::Distributed), false) => Err(String::from(
            "--mode distributed needs the coordinator's Arrow Flight URL; \
             pass --coordinator/-c <URL>",
        )),
        (Some(mode), _) => Ok(mode),
        (None, true) => Ok(IvmMode::Distributed),
        (None, false) => Ok(IvmMode::Embedded),
    }
}

/// The coordinator's HTTP management base for distributed IVM.
///
/// This is deliberately not defaulted to the Flight URL. `Session::ivm` falls
/// back to the Flight URL when no HTTP base is set, which is right for a
/// single-port local setup and wrong — silently, with a connection error that
/// names the wrong port — for every real deployment. Naming it is cheap; being
/// pointed at the wrong port for a whole session is not.
fn resolve_coordinator_http(spec: &IvmRunSpec, flight_url: &str) -> Result<String, String> {
    let env_value = std::env::var(COORDINATOR_HTTP_ENV).ok();
    resolve_coordinator_http_with_env(spec, flight_url, env_value.as_deref())
}

/// Testable variant: `env_value` is `Some(url)` if the env var is set.
///
/// Call sites in tests pass an explicit value instead of reading the real
/// environment — the same split as
/// [`CoordinatorMode::from_args_with_env_override`], and for a sharper reason
/// here: this repo *exports* `KRISHIV_COORDINATOR_HTTP` itself (`local_cluster`
/// when it spawns the flight server, `krishiv-mcp` when it builds a session), so
/// a test that consulted the process environment would pass or skip depending on
/// which shell it ran in.
fn resolve_coordinator_http_with_env(
    spec: &IvmRunSpec,
    flight_url: &str,
    env_value: Option<&str>,
) -> Result<String, String> {
    if let Some(url) = &spec.coordinator_http {
        return Ok(url.clone());
    }
    if let Some(url) = env_value.filter(|u| !u.trim().is_empty()) {
        return Ok(url.to_string());
    }
    Err(format!(
        "distributed IVM is maintained over the coordinator's HTTP management API \
         (/api/v1/ivm/*), which is a different port from the Arrow Flight endpoint \
         given as --coordinator ({flight_url}). Pass --coordinator-http <URL> or set \
         {COORDINATOR_HTTP_ENV}."
    ))
}

/// Reject the flags that only one placement reads.
///
/// `--checkpoint-dir` was applied only in the single-node arm of [`ivm_session`]
/// and `--coordinator-http` read only in the distributed one. Outside those
/// modes each was parsed, stored on the spec, and then dropped without a word —
/// the same silent reinterpretation `resolve_mode` already refuses for `--mode
/// embedded` next to `-c`, so it gets the same answer.
///
/// Only the explicit flag is rejected. `$KRISHIV_COORDINATOR_HTTP` is ambient
/// (this repo exports it around its own local cluster), so its mere presence
/// must not turn an embedded run into an error.
fn reject_flags_outside_their_mode(spec: &IvmRunSpec, mode: IvmMode) -> Result<(), String> {
    if spec.checkpoint_dir.is_some() && mode != IvmMode::SingleNode {
        return Err(format!(
            "--checkpoint-dir is read only by --mode single-node, but this run resolved to \
             --mode {}. Drop the flag, or pass --mode single-node with --coordinator/-c.",
            mode.as_str()
        ));
    }
    if spec.coordinator_http.is_some() && mode != IvmMode::Distributed {
        return Err(format!(
            "--coordinator-http is read only by --mode distributed, but this run resolved to \
             --mode {}. Drop the flag, or pass --mode distributed.",
            mode.as_str()
        ));
    }
    Ok(())
}

/// Why building the session failed, and therefore which exit code to use.
///
/// `ivm_session` used to flatten both into one `Result<Session, String>`, and
/// `run_ivm_job` mapped the lot to exit code 2 — the *usage* code. A coordinator
/// URL the transport refused therefore reported itself to callers and CI as a
/// mistake in the command line.
#[derive(Debug)]
enum SessionError {
    /// The flags are wrong: exit 2, the same code the argument parser returns.
    Usage(String),
    /// The flags were fine and building the session failed: exit 1.
    Runtime(String),
}

impl SessionError {
    fn exit_code(&self) -> i32 {
        match self {
            Self::Usage(_) => 2,
            Self::Runtime(_) => 1,
        }
    }

    fn message(&self) -> &str {
        match self {
            Self::Usage(m) | Self::Runtime(m) => m,
        }
    }
}

fn ivm_session(spec: &IvmRunSpec, coordinator: &CoordinatorMode) -> Result<Session, SessionError> {
    let mode = resolve_mode(spec, coordinator).map_err(SessionError::Usage)?;
    reject_flags_outside_their_mode(spec, mode).map_err(SessionError::Usage)?;
    let flight_url = match coordinator {
        CoordinatorMode::Remote(url) => Some(url.clone()),
        CoordinatorMode::Local => None,
    };
    let mut builder = SessionBuilder::new();
    match (mode, flight_url) {
        (IvmMode::Embedded, _) => {}
        (IvmMode::SingleNode, Some(url)) => {
            // `with_local_cluster` is the one call that means SingleNode *with*
            // a Flight endpoint; `with_coordinator` would flip the mode to
            // Distributed underneath us.
            builder = builder.with_local_cluster(url);
            if let Some(dir) = &spec.checkpoint_dir {
                // This reaches `Session::checkpoint_dir` and from there
                // `durable_engine_runtime`, which roots a `DurableCheckpointService`
                // at `dir` (creating it). `IncrementalEngine::run` reads neither
                // `rt.checkpoint` nor `rt.state_dir` — only the streaming loop does —
                // and `ivm run` always compiles `SourceSpec::cdc`, i.e. always the
                // incremental engine. So the directory appears and stays empty. The
                // wiring is kept so the flag starts working the day the engine reads
                // its checkpoints (register row INT-F13); the help text says plainly
                // that today it does not.
                builder = builder.with_config("checkpoint_dir", dir.clone());
            }
        }
        (IvmMode::Distributed, Some(url)) => {
            let http = resolve_coordinator_http(spec, &url).map_err(SessionError::Usage)?;
            // `with_coordinator` is what switches the session to Distributed;
            // the management base is set separately because it is a separate
            // port, and `with_remote_execution` is the placement that mode
            // requires.
            builder = builder
                .with_coordinator(url)
                .with_coordinator_http(http)
                .with_remote_execution(true);
        }
        // `resolve_mode` already rejected single-node/distributed without one.
        (IvmMode::SingleNode | IvmMode::Distributed, None) => {
            return Err(SessionError::Usage(String::from(
                "single-node and distributed modes need --coordinator/-c <flight-url>",
            )));
        }
    }
    builder
        .build()
        .map_err(|e| SessionError::Runtime(e.to_string()))
}

fn run_ivm_job(args: &[&str], coordinator: &CoordinatorMode) -> CliResponse {
    let spec = match parse_ivm_run(args) {
        Ok(s) => s,
        Err(e) => return CliResponse::err(format!("{e}\n\n{}", ivm_help()), 2),
    };
    let session = match ivm_session(&spec, coordinator) {
        Ok(s) => s,
        Err(e) => return CliResponse::err(format!("{}\n", e.message()), e.exit_code()),
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
            "Submitted incremental job {} ({:?}); {}\n",
            spec.job_id,
            handle.status(),
            describe_sink_output(&spec.sink, &spec.sink_format),
        )),
        Err(e) => CliResponse::err(format!("{e}\n"), 1),
    }
}

/// The `--sink-format` values `ivm run` writes as a single local file.
///
/// `ConnectorSinkProvider` routes exactly these four to a file writer at the
/// `--sink` path; every other kind goes to the connector registry, which writes
/// a *directory* (delta, iceberg, hudi) or a remote endpoint (kafka, jdbc,
/// elasticsearch — whichever the build's features register). Only for the four
/// below does `std::fs::metadata(--sink)` describe the run's output.
const LOCAL_FILE_SINK_FORMATS: [&str; 4] = ["parquet", "csv", "json", "ndjson"];

/// Say what actually landed in the sink.
///
/// This line used to read "net view written to <path>" unconditionally — a view
/// that matched no rows printed it too, next to a sink file that was either
/// absent or zero bytes. Report what the filesystem says instead of what the
/// command hoped for.
///
/// The file's size is the only signal available here: `JobHandle` carries a
/// status and no row count, so the row count is not something this command can
/// state. A container format writes a header even for zero rows, so a parquet
/// sink reports its byte count rather than "no rows" — which is exactly as much
/// as is actually known.
///
/// `format` gates all of that. `--sink-format` is not restricted to the local
/// file kinds, and stat-ing the `--sink` path for the others gave answers that
/// were simply false: a successful `--sink-format delta` run reported the
/// directory entry's size ("80 bytes") as if it were the view, and any sink that
/// writes off-box would have failed the `metadata` call and printed "the view
/// produced no rows" after writing every one of them. For those kinds this says
/// only what it knows: the connector accepted the output.
fn describe_sink_output(sink: &str, format: &str) -> String {
    if !LOCAL_FILE_SINK_FORMATS.contains(&format) {
        return format!(
            "the {format} sink accepted the view; its output is not a local file this \
             command can stat, so no size is reported for {sink}"
        );
    }
    match std::fs::metadata(sink) {
        Ok(meta) if meta.len() > 0 => {
            format!("net view written to {sink} ({} bytes)", meta.len())
        }
        Ok(_) => format!("the view produced no rows, so {sink} is empty"),
        Err(_) => format!("the view produced no rows, so nothing was written to {sink}"),
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
        assert_eq!(spec.mode, None);
    }

    /// API-E6: two `--source` flags naming the same table were accepted, and the
    /// second one's rows were fed into the same relation with no sign of it.
    #[test]
    fn duplicate_source_names_are_rejected() {
        let err = parse_ivm_run(&[
            "--job-id",
            "j",
            "--sql",
            "SELECT * FROM t",
            "--source",
            "t=./a.csv",
            "--source",
            "t=./b.csv",
            "--sink",
            "./out.json",
        ])
        .expect_err("a repeated source name must be rejected");
        assert!(
            err.contains("duplicate --source name 't'"),
            "message must name the offending source: {err}"
        );
    }

    /// Distinct names still compose into a multi-source (join) view.
    #[test]
    fn distinct_source_names_are_accepted() {
        let spec = parse_ivm_run(&[
            "--job-id",
            "j",
            "--sql",
            "SELECT * FROM t JOIN u USING (k)",
            "--source",
            "t=./a.csv",
            "--source",
            "u=./b.csv",
            "--sink",
            "./out.json",
        ])
        .expect("two distinctly named sources parse");
        assert_eq!(spec.sources.len(), 2);
    }

    fn minimal_args() -> Vec<&'static str> {
        vec![
            "--job-id",
            "j",
            "--sql",
            "SELECT * FROM t",
            "--source",
            "t=./a.csv",
            "--sink",
            "./out.json",
        ]
    }

    /// API-E1: `--mode` reaches all three placements. Without it the mode is
    /// derived from whether a coordinator was given, which is what the CLI did
    /// before — single-node was simply unreachable.
    #[test]
    fn mode_flag_parses_all_three_placements() {
        for (raw, expected) in [
            ("embedded", IvmMode::Embedded),
            ("single-node", IvmMode::SingleNode),
            ("distributed", IvmMode::Distributed),
        ] {
            let mut args = minimal_args();
            args.push("--mode");
            args.push(raw);
            let spec = parse_ivm_run(&args).expect("mode parses");
            assert_eq!(spec.mode, Some(expected), "--mode {raw}");
        }
        let mut args = minimal_args();
        args.push("--mode");
        args.push("cluster");
        assert!(parse_ivm_run(&args).is_err(), "an unknown mode must error");
    }

    #[test]
    fn single_node_is_reachable_and_needs_a_flight_url() {
        let mut args = minimal_args();
        args.extend(["--mode", "single-node"]);
        let spec = parse_ivm_run(&args).expect("parses");

        let err = resolve_mode(&spec, &CoordinatorMode::Local)
            .expect_err("single-node without a coordinator must be rejected");
        assert!(err.contains("--coordinator"), "{err}");

        let mode = resolve_mode(&spec, &CoordinatorMode::Remote("http://c:2003".into()))
            .expect("single-node with a coordinator resolves");
        assert_eq!(mode, IvmMode::SingleNode);
    }

    #[test]
    fn mode_defaults_follow_the_coordinator_flag() {
        let spec = parse_ivm_run(&minimal_args()).expect("parses");
        assert_eq!(
            resolve_mode(&spec, &CoordinatorMode::Local).expect("default"),
            IvmMode::Embedded
        );
        assert_eq!(
            resolve_mode(&spec, &CoordinatorMode::Remote("http://c:2003".into())).expect("default"),
            IvmMode::Distributed
        );
    }

    /// `--mode embedded` next to a coordinator used to mean "ignore the
    /// coordinator". Now it is a conflict the user has to resolve.
    #[test]
    fn embedded_mode_with_a_coordinator_is_rejected() {
        let mut args = minimal_args();
        args.extend(["--mode", "embedded"]);
        let spec = parse_ivm_run(&args).expect("parses");
        let err = resolve_mode(&spec, &CoordinatorMode::Remote("http://c:2003".into()))
            .expect_err("embedded + coordinator must be rejected");
        assert!(err.contains("Drop one of the two"), "{err}");
    }

    /// API-E8: `-c` used to be copied into BOTH `with_coordinator` (Flight) and
    /// `with_coordinator_http` (management), which are different ports — so one
    /// of the two was always wrong. The HTTP base must now be named.
    ///
    /// The env value is passed explicitly rather than read from the process.
    /// This test used to `return` early when `KRISHIV_COORDINATOR_HTTP` was set,
    /// which made it a no-op in exactly the environments this repo creates for
    /// itself (`local_cluster` exports it for the flight server, `krishiv-mcp`
    /// for its session) — a test that reported success without asserting
    /// anything.
    #[test]
    fn distributed_requires_an_explicit_http_base() {
        let spec = parse_ivm_run(&minimal_args()).expect("parses");
        let err = resolve_coordinator_http_with_env(&spec, "http://coordinator:2003", None)
            .expect_err("a distributed run must not invent the management URL");
        assert!(
            err.contains("--coordinator-http") && err.contains("different port"),
            "the error must explain the two endpoints: {err}"
        );
    }

    /// The `$KRISHIV_COORDINATOR_HTTP` fallback branch had no test at all.
    #[test]
    fn env_supplies_the_http_base_when_the_flag_is_absent() {
        let spec = parse_ivm_run(&minimal_args()).expect("parses");
        assert_eq!(
            resolve_coordinator_http_with_env(
                &spec,
                "http://coordinator:2003",
                Some("http://from-env:2002"),
            )
            .expect("the env value is the fallback"),
            "http://from-env:2002"
        );
        // A blank or whitespace-only export is not a URL; it must not be
        // accepted as one and then fail later as a connection error.
        for blank in ["", "   "] {
            assert!(
                resolve_coordinator_http_with_env(&spec, "http://coordinator:2003", Some(blank))
                    .is_err(),
                "a blank env value must not resolve: {blank:?}"
            );
        }
    }

    /// The explicit flag beats the environment.
    #[test]
    fn the_http_flag_beats_the_env_fallback() {
        let mut args = minimal_args();
        args.extend(["--coordinator-http", "http://from-flag:2002"]);
        let spec = parse_ivm_run(&args).expect("parses");
        assert_eq!(
            resolve_coordinator_http_with_env(
                &spec,
                "http://coordinator:2003",
                Some("http://from-env:2002"),
            )
            .expect("resolves"),
            "http://from-flag:2002"
        );
    }

    #[test]
    fn explicit_http_base_is_used_verbatim() {
        let mut args = minimal_args();
        args.extend(["--coordinator-http", "http://coordinator:2002"]);
        let spec = parse_ivm_run(&args).expect("parses");
        assert_eq!(
            resolve_coordinator_http(&spec, "http://coordinator:2003").expect("resolves"),
            "http://coordinator:2002"
        );
    }

    /// The session `ivm run` builds for a distributed job must keep the two
    /// endpoints apart. Before the fix both came from the one `-c` string.
    #[test]
    fn distributed_session_keeps_flight_and_http_apart() {
        use krishiv_api::ExecutionMode;
        let mut args = minimal_args();
        args.extend(["--coordinator-http", "http://coordinator:2002"]);
        let spec = parse_ivm_run(&args).expect("parses");
        let session = ivm_session(
            &spec,
            &CoordinatorMode::Remote("http://coordinator:2003".into()),
        )
        .expect("session builds");
        assert_eq!(session.mode(), ExecutionMode::Distributed);
        assert_eq!(
            session.coordinator_http_url(),
            Some("http://coordinator:2002"),
            "the management base must be the HTTP one, not the Flight URL"
        );
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
        assert!(
            resp.stdout.contains("net view written to"),
            "a view with rows reports the file: {}",
            resp.stdout
        );

        let written = std::fs::read_to_string(&output).unwrap();
        assert!(written.contains("\"total\":4"), "a=4: {written}");
        assert!(written.contains("\"total\":2"), "b=2: {written}");
    }

    /// API-E7: the success line used to claim "net view written to <path>" even
    /// when the view matched nothing and the sink file was left empty.
    #[test]
    fn empty_view_does_not_claim_a_written_file() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("kv.csv");
        let output = dir.path().join("agg.ndjson");
        std::fs::write(&input, "k,v\na,1\nb,2\na,3\n").unwrap();

        let resp = run_ivm(
            &[
                "run",
                "--job-id",
                "empty",
                "--sql",
                "SELECT k, SUM(v) AS total FROM t WHERE v > 1000 GROUP BY k",
                "--source",
                &format!("t={}", input.to_str().unwrap()),
                "--sink",
                output.to_str().unwrap(),
            ],
            &CoordinatorMode::Local,
        );
        assert_eq!(resp.exit_code, 0, "stderr: {}", resp.stderr);
        let bytes = std::fs::metadata(&output).map(|m| m.len()).unwrap_or(0);
        assert_eq!(
            bytes, 0,
            "the view matched nothing, so the sink stays empty"
        );
        assert!(
            resp.stdout.contains("produced no rows"),
            "an empty view must not claim a written view: {}",
            resp.stdout
        );
        assert!(
            !resp.stdout.contains("net view written to"),
            "an empty view must not claim a written view: {}",
            resp.stdout
        );
    }

    // ── Help text ────────────────────────────────────────────────────────────

    /// The multi-line help used Rust's `\n\` line continuation, which eats the
    /// newline *and* every leading space on the next source line: the source was
    /// aligned into columns and `krishiv ivm --help` rendered flush-left.
    /// Asserted on the rendered string, which is what the user sees.
    #[test]
    fn help_renders_with_its_indentation() {
        let help = ivm_help();
        assert!(
            help.contains("\nUsage:\n  krishiv ivm run "),
            "the usage line must be indented under its heading:\n{help}"
        );
        assert!(
            help.contains("\n  --job-id <ID>            View/job name (required)\n"),
            "the option table must keep its column alignment:\n{help}"
        );
        assert!(
            help.contains("\nModes:\n  embedded      "),
            "the mode list must be indented under its heading:\n{help}"
        );
    }

    /// The rewritten `--source` line kept "names must be unique" and dropped the
    /// still-enforced "at least one is required" rule that `parse_ivm_run`
    /// applies. Both are enforced, so both are stated.
    #[test]
    fn help_states_both_source_rules_that_are_enforced() {
        let help = ivm_help();
        assert!(
            help.contains("At least one is required"),
            "help must state the rule parse_ivm_run enforces:\n{help}"
        );
        assert!(
            help.contains("names must be unique"),
            "help must state the uniqueness rule:\n{help}"
        );
        // Both claims are claims about this code, so check the code still makes
        // them true rather than trusting the sentence.
        assert!(
            parse_ivm_run(&["--job-id", "j", "--sql", "SELECT 1", "--sink", "./o"]).is_err(),
            "no --source must still be rejected"
        );
    }

    /// The example pointed `-c` at 50051, which is this project's port for
    /// nothing: `-c` is the Arrow Flight endpoint, and `krishiv local` puts
    /// Flight on 2003 and HTTP management on 2002 (`local_cluster.rs`).
    #[test]
    fn help_example_uses_this_projects_ports() {
        let help = ivm_help();
        assert!(
            !help.contains("50051"),
            "50051 is not a port this project serves:\n{help}"
        );
        assert!(
            help.contains("-c http://coordinator:2003"),
            "-c is the Flight endpoint (2003):\n{help}"
        );
        assert!(
            help.contains("--coordinator-http http://coordinator:2002"),
            "the management base is the HTTP endpoint (2002):\n{help}"
        );
    }

    /// `--checkpoint-dir` was documented as "Where single-node writes its
    /// on-disk checkpoints". It writes none: the value reaches
    /// `durable_engine_runtime`, but `IncrementalEngine::run` reads neither
    /// `rt.checkpoint` nor `rt.state_dir`, and `ivm run` always compiles a CDC
    /// source, so it is always the incremental engine. Verified by running the
    /// built binary: the directory is created and stays empty.
    #[test]
    fn help_does_not_claim_checkpoints_are_written() {
        let help = ivm_help();
        assert!(
            help.contains("Accepted and inert"),
            "the flag's help must say it is inert today:\n{help}"
        );
        assert!(
            !help.contains("Where single-node writes its on-disk checkpoints"),
            "the old claim must be gone:\n{help}"
        );
    }

    /// `--mode single-node` never dials the `--coordinator` URL it demands.
    /// Verified against the built binary with `-c http://192.0.2.1:9999`
    /// (TEST-NET-1, unroutable): the run completed in 0.12s.
    #[test]
    fn help_does_not_claim_single_node_reaches_a_daemon() {
        let help = ivm_help();
        assert!(
            help.contains("this path never\n                dials it"),
            "single-node's help must say the coordinator is not dialled:\n{help}"
        );
        assert!(
            !help.contains("against a local daemon"),
            "the old claim must be gone:\n{help}"
        );
    }

    // ── Flags outside the mode that reads them ───────────────────────────────

    /// `--checkpoint-dir` was applied only in the single-node arm and
    /// `--coordinator-http` read only in the distributed one; in any other mode
    /// both were parsed and then dropped in silence.
    #[test]
    fn flags_are_rejected_outside_the_mode_that_reads_them() {
        let mut with_ckpt = minimal_args();
        with_ckpt.extend(["--checkpoint-dir", "./x"]);
        let ckpt_spec = parse_ivm_run(&with_ckpt).expect("parses");
        for mode in [IvmMode::Embedded, IvmMode::Distributed] {
            let err = reject_flags_outside_their_mode(&ckpt_spec, mode)
                .expect_err("--checkpoint-dir must be rejected outside single-node");
            assert!(
                err.contains("--checkpoint-dir") && err.contains("single-node"),
                "the message must name the mode that accepts it: {err}"
            );
        }
        reject_flags_outside_their_mode(&ckpt_spec, IvmMode::SingleNode)
            .expect("single-node is the mode that reads --checkpoint-dir");

        let mut with_http = minimal_args();
        with_http.extend(["--coordinator-http", "http://coordinator:2002"]);
        let http_spec = parse_ivm_run(&with_http).expect("parses");
        for mode in [IvmMode::Embedded, IvmMode::SingleNode] {
            let err = reject_flags_outside_their_mode(&http_spec, mode)
                .expect_err("--coordinator-http must be rejected outside distributed");
            assert!(
                err.contains("--coordinator-http") && err.contains("distributed"),
                "the message must name the mode that accepts it: {err}"
            );
        }
        reject_flags_outside_their_mode(&http_spec, IvmMode::Distributed)
            .expect("distributed is the mode that reads --coordinator-http");
    }

    /// The ambient `$KRISHIV_COORDINATOR_HTTP` must not make an embedded run an
    /// error — this repo exports it around its own local cluster. Only the
    /// explicit flag is a mode conflict.
    #[test]
    fn the_env_http_base_does_not_make_an_embedded_run_a_conflict() {
        let spec = parse_ivm_run(&minimal_args()).expect("parses");
        reject_flags_outside_their_mode(&spec, IvmMode::Embedded)
            .expect("no explicit flag, so nothing to reject");
    }

    /// End to end: an embedded run that passes either flag exits 2 rather than
    /// completing and dropping it. `ivm run --mode embedded --checkpoint-dir ./x
    /// --coordinator-http http://bogus:1` used to exit 0.
    #[test]
    fn embedded_run_with_a_foreign_flag_exits_2() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("kv.csv");
        let output = dir.path().join("agg.ndjson");
        std::fs::write(&input, "k,v\na,1\n").unwrap();
        let source = format!("t={}", input.to_str().unwrap());

        for extra in [
            vec!["--checkpoint-dir", "./x"],
            vec!["--coordinator-http", "http://bogus:1"],
        ] {
            let mut args = vec![
                "run",
                "--job-id",
                "emb",
                "--sql",
                "SELECT k, SUM(v) AS total FROM t GROUP BY k",
                "--source",
                &source,
                "--sink",
                output.to_str().unwrap(),
            ];
            args.extend(extra.iter().copied());
            let resp = run_ivm(&args, &CoordinatorMode::Local);
            assert_eq!(
                resp.exit_code, 2,
                "a flag no mode reads must not be silently dropped: {resp:?}"
            );
            assert!(
                resp.stderr.contains(extra[0]),
                "the rejection must name the flag: {}",
                resp.stderr
            );
        }
    }

    // ── Exit codes ───────────────────────────────────────────────────────────

    /// `ivm_session` flattened usage errors and `builder.build()` failures into
    /// one `Result<_, String>`, and every one of them exited 2 — the usage code.
    /// An empty `-c` reaches the transport (`coordinator URL must not be empty`)
    /// only after the flags have been accepted, so it is a runtime failure and
    /// must exit 1.
    #[test]
    fn a_runtime_failure_exits_1_and_a_usage_error_exits_2() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("kv.csv");
        let output = dir.path().join("agg.ndjson");
        std::fs::write(&input, "k,v\na,1\n").unwrap();
        let source = format!("t={}", input.to_str().unwrap());
        let base = |extra: Vec<&'static str>| {
            let mut args = vec![
                "run",
                "--job-id",
                "j",
                "--sql",
                "SELECT k, SUM(v) AS total FROM t GROUP BY k",
                "--source",
                source.as_str(),
                "--sink",
                output.to_str().unwrap(),
            ];
            args.extend(extra);
            args
        };

        // Flags accepted, session construction fails: exit 1.
        let runtime_failure = run_ivm(
            &base(vec!["--coordinator-http", "http://coordinator:2002"]),
            &CoordinatorMode::Remote(String::new()),
        );
        assert_eq!(
            runtime_failure.exit_code, 1,
            "a failure to build the session is not a usage error: {runtime_failure:?}"
        );

        // Flags themselves wrong: still exit 2.
        let usage_error = run_ivm(
            &base(vec!["--mode", "embedded"]),
            &CoordinatorMode::Remote("http://coordinator:2003".into()),
        );
        assert_eq!(
            usage_error.exit_code, 2,
            "a mode conflict is a usage error: {usage_error:?}"
        );
    }

    // ── Sink description ─────────────────────────────────────────────────────

    /// `--sink-format` is not restricted to the local file kinds. For a registry
    /// connector the `--sink` path is not a file this command can stat, so the
    /// old unconditional `fs::metadata` answered "the view produced no rows"
    /// after a run that wrote every one of them — and for the lakehouse kinds,
    /// which write a directory, it reported the directory entry's size as the
    /// view's.
    #[test]
    fn a_non_file_sink_format_is_not_described_from_the_filesystem() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nowhere");
        let missing = missing.to_str().unwrap();

        for format in ["kafka", "delta", "iceberg", "hudi", "jdbc", "elasticsearch"] {
            let described = describe_sink_output(missing, format);
            assert!(
                !described.contains("produced no rows"),
                "{format} output cannot be read back, so no row claim is available: {described}"
            );
            assert!(
                described.contains(format),
                "the description must name the connector: {described}"
            );
        }

        // The local file kinds keep the filesystem-backed answer.
        for format in ["json", "ndjson", "csv", "parquet"] {
            assert!(
                describe_sink_output(missing, format).contains("produced no rows"),
                "a missing local {format} sink still means no rows"
            );
        }
        let real = dir.path().join("out.ndjson");
        std::fs::write(&real, "{\"k\":\"a\"}\n").unwrap();
        assert!(
            describe_sink_output(real.to_str().unwrap(), "json").contains("net view written to"),
            "a non-empty local sink still reports its bytes"
        );
    }
}
