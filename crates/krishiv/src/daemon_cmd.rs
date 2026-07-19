//! Long-running daemon subcommands (`krishiv coordinator`, `krishiv executor`, …).

// Deliberate sync-over-async boundary module (Phase 51 async contract):
// block_on here bridges a synchronous public surface to the async core.
#![allow(clippy::disallowed_methods)]

use krishiv_common::async_util::block_on;
use krishiv_scheduler::{
    CoordinatorSidecarFn, SharedCoordinator, coordinator_daemon_help, job_coordinator_daemon_help,
    parse_coordinator_daemon_config, parse_job_coordinator_daemon_config, run_clusterd_daemon,
    run_job_coordinator_daemon, run_standalone_coordinator,
};

use crate::cli::CliResponse;

/// If `args` starts with a daemon subcommand, run it and return `Some(exit_code)`.
pub fn try_run_daemon(args: &[String]) -> Option<i32> {
    let sub = args.first()?.as_str();
    match sub {
        "coordinator" | "clusterd" | "executor" | "job-coordinator" | "flight-server"
        | "shuffle-svc" | "mcp" | "health" => {}
        _ => return None,
    }
    // Boot banner (flag-minimization plan): announce the compiled-in capability
    // set before the daemon starts, so a missing `kafka`/`cloud`/etc. is visible
    // in `kubectl logs` instead of surfacing as a first-job failure. Emitted via
    // eprintln because per-daemon tracing is not yet initialised at this point.
    if sub != "health" {
        eprintln!("krishiv capabilities: {}", crate::capabilities::summary());
    }
    let rest: Vec<String> = args.iter().skip(1).cloned().collect();
    let code = match sub {
        "coordinator" => run_coordinator(&rest),
        "clusterd" => run_clusterd(&rest),
        "executor" => run_executor(&rest),
        "job-coordinator" => run_job_coordinator(&rest),
        "flight-server" => run_flight_server(&rest),
        "shuffle-svc" => run_shuffle_svc(&rest),
        "mcp" => run_mcp(&rest),
        "health" => run_health_check(),
        other => {
            tracing::error!(subcommand = %other, "unexpected daemon subcommand after validation");
            2
        }
    };
    Some(code)
}

fn run_mcp(args: &[String]) -> i32 {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print!("{}", krishiv_mcp::mcp_help());
        return 0;
    }
    match block_on(krishiv_mcp::run_mcp_from_env(args)) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{e}");
            2
        }
    }
}

fn run_coordinator(args: &[String]) -> i32 {
    match parse_coordinator_daemon_config(args.iter().cloned()) {
        Ok(config) if config.help => {
            print!("{}", coordinator_daemon_help());
            0
        }
        Ok(config) => {
            let factory = build_ui_http_factory();
            let mut sidecars: Vec<CoordinatorSidecarFn> = Vec::new();
            if let Some(sidecar) = build_flight_sidecar(&config) {
                sidecars.push(sidecar);
            }
            // Stringify the error inside the awaited future: `Box<dyn Error>`
            // (krishiv-scheduler's return type) is not `Send`, but `block_on`
            // requires its future's output to be `Send`. Only the `Display`
            // text is used here, so converting before crossing that boundary
            // avoids changing krishiv-scheduler's error type.
            match block_on(async {
                run_standalone_coordinator(config, factory, sidecars)
                    .await
                    .map_err(|e| e.to_string())
            }) {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("{e}");
                    2
                }
            }
        }
        Err(e) => {
            eprintln!("{e}");
            2
        }
    }
}

fn run_clusterd(args: &[String]) -> i32 {
    match parse_coordinator_daemon_config(args.iter().cloned()) {
        Ok(config) if config.help => {
            print!("{}", coordinator_daemon_help());
            0
        }
        Ok(config) => {
            let factory = build_ui_http_factory();
            let mut sidecars: Vec<CoordinatorSidecarFn> = Vec::new();
            if let Some(sidecar) = build_flight_sidecar(&config) {
                sidecars.push(sidecar);
            }
            match block_on(async {
                run_clusterd_daemon(config, factory, sidecars)
                    .await
                    .map_err(|e| e.to_string())
            }) {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("{e}");
                    2
                }
            }
        }
        Err(e) => {
            eprintln!("{e}");
            2
        }
    }
}

#[cfg(feature = "ui")]
fn build_ui_http_factory() -> Option<Box<dyn FnOnce(SharedCoordinator) -> axum::Router<()> + Send>>
{
    // Runtime off-switch (approved surface ruling): KRISHIV_UI=off boots
    // the daemon without the embedded UI even in a ui-featured build.
    if std::env::var("KRISHIV_UI").is_ok_and(|v| v.eq_ignore_ascii_case("off")) {
        tracing::info!("embedded UI disabled by KRISHIV_UI=off");
        return None;
    }
    Some(Box::new(|shared: SharedCoordinator| {
        let engine = krishiv_sql::SqlEngine::new();
        let ui_state = krishiv_ui::UiState::from_shared_coordinator(shared).with_sql_engine(engine);
        krishiv_ui::embedded_router(ui_state)
    }))
}

/// Built without the `ui` feature (#216): the daemon serves no embedded UI.
#[cfg(not(feature = "ui"))]
fn build_ui_http_factory() -> Option<Box<dyn FnOnce(SharedCoordinator) -> axum::Router<()> + Send>>
{
    None
}

/// Build a Flight SQL sidecar factory when `config.flight_addr` is set.
///
/// The returned factory is passed to `spawn_coordinator_sidecars` and binds the
/// Flight SQL server co-located with the coordinator — no HTTP proxy hop.
#[cfg(feature = "flight-sql")]
fn build_flight_sidecar(
    config: &krishiv_scheduler::CoordinatorDaemonConfig,
) -> Option<CoordinatorSidecarFn> {
    let addr = config.flight_addr?;
    Some(Box::new(move |coordinator: SharedCoordinator| {
        Box::pin(async move {
            use krishiv_flight_sql::{FlightExecutionHost, run_flight_server_with_host};
            let host = FlightExecutionHost::with_coordinator(coordinator);
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!(
                        addr = %addr,
                        error = %e,
                        "Failed to bind co-located Flight SQL address"
                    );
                    return;
                }
            };
            tracing::info!(addr = %addr, "Krishiv Flight SQL co-located with coordinator");
            if let Err(e) = run_flight_server_with_host(host, listener).await {
                tracing::error!(
                    error = %e,
                    "Co-located Flight SQL server terminated unexpectedly"
                );
            }
        })
    }))
}

/// When flight-sql feature is disabled, no sidecar is built.
#[cfg(not(feature = "flight-sql"))]
fn build_flight_sidecar(
    _config: &krishiv_scheduler::CoordinatorDaemonConfig,
) -> Option<CoordinatorSidecarFn> {
    None
}

fn run_job_coordinator(args: &[String]) -> i32 {
    match parse_job_coordinator_daemon_config(args.iter().cloned()) {
        Ok(config) if config.help => {
            print!("{}", job_coordinator_daemon_help());
            0
        }
        Ok(config) => match block_on(async {
            run_job_coordinator_daemon(config)
                .await
                .map_err(|e| e.to_string())
        }) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("{e}");
                2
            }
        },
        Err(e) => {
            eprintln!("{e}");
            2
        }
    }
}

fn run_executor(args: &[String]) -> i32 {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print!("{}", krishiv_executor::cli::executor_cli_help());
        return 0;
    }
    match block_on(krishiv_executor::cli::run_executor_cli(
        args.iter().cloned(),
    )) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{e}");
            2
        }
    }
}

fn run_flight_server(args: &[String]) -> i32 {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print!("{}", flight_server_help());
        return 0;
    }
    #[cfg(not(feature = "flight-sql"))]
    {
        eprintln!("flight-server support requires building krishiv with feature `flight-sql`");
        return 2;
    }
    #[cfg(feature = "flight-sql")]
    match block_on(krishiv_flight_sql::run_flight_server_from_env()) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{e}");
            2
        }
    }
}

fn run_shuffle_svc(args: &[String]) -> i32 {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print!("{}", shuffle_svc_help());
        return 0;
    }
    #[cfg(not(feature = "shuffle"))]
    {
        eprintln!("shuffle-svc support requires building krishiv with feature `shuffle`");
        return 2;
    }
    #[cfg(feature = "shuffle")]
    match block_on(krishiv_shuffle::shuffle_svc::run_shuffle_svc_from_env()) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{e}");
            2
        }
    }
}

/// Lightweight TCP health probe used by Docker HEALTHCHECK.
///
/// Replaces the `curl` dependency (~5-8 MB) with a zero-dependency `std::net`
/// connect check. The port defaults to 2002 (the health/metrics HTTP listener)
/// and can be overridden via `KRISHIV_HEALTH_PORT`.
fn run_health_check() -> i32 {
    use std::net::TcpStream;
    use std::time::Duration;

    let port: u16 = std::env::var("KRISHIV_HEALTH_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2002);
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}")
        .parse()
        .unwrap_or_else(|_| "127.0.0.1:2002".parse().expect("hardcoded fallback"));
    match TcpStream::connect_timeout(&addr, Duration::from_secs(2)) {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("health check failed ({addr}): {e}");
            1
        }
    }
}

pub fn daemons_help() -> String {
    let mut help = String::from(
        "Krishiv daemon processes (also available as legacy binary names).\n\
         \n\
         Usage:\n\
           krishiv coordinator [OPTIONS]     Active coordinator (was krishiv-coordinator)\n\
           krishiv clusterd [OPTIONS]        Cluster control plane (was krishiv-clusterd)\n\
           krishiv job-coordinator [OPTS]    Per-job coordinator (was krishiv-job-coordinator)\n\
           krishiv executor [OPTIONS]        Data-plane worker (was krishiv-executor)\n",
    );
    #[cfg(feature = "flight-sql")]
    help.push_str(
        "           krishiv flight-server             Arrow Flight SQL (was krishiv-flight-server)\n",
    );
    #[cfg(not(feature = "flight-sql"))]
    help.push_str(
        "           krishiv flight-server             Arrow Flight SQL [disabled; build with feature `flight-sql`]\n",
    );
    #[cfg(feature = "shuffle")]
    help.push_str("           krishiv shuffle-svc               Optional shuffle HTTP service\n");
    #[cfg(not(feature = "shuffle"))]
    help.push_str(
        "           krishiv shuffle-svc               Optional shuffle HTTP service [disabled; build with feature `shuffle`]\n",
    );
    help.push_str("           krishiv mcp                       Model Context Protocol server\n");
    help.push_str(
        "\n\
         Legacy binaries remain install aliases; prefer `krishiv <subcommand>`.\n",
    );
    help
}

fn flight_server_help() -> &'static str {
    "Arrow Flight SQL server.\n\
     \n\
     Usage: krishiv flight-server\n\
     Env: KRISHIV_FLIGHT_ADDR (default 127.0.0.1:2003)\n"
}

fn shuffle_svc_help() -> &'static str {
    "Shuffle partition HTTP service.\n\
     \n\
     Usage: krishiv shuffle-svc\n\
     Env: KRISHIV_SHUFFLE_DIR, KRISHIV_SHUFFLE_ADDR (default 0.0.0.0:2004)\n"
}

/// CLI help snippet for main help (daemon section).
pub fn daemon_help_section() -> String {
    let mut help = String::from(
        "  coordinator       Run active coordinator (distributed control plane)\n\
         clusterd          Run cluster control plane daemon (CCP)\n\
         job-coordinator   Run per-job coordinator (JCP)\n\
         executor          Run data-plane executor worker\n",
    );
    #[cfg(feature = "flight-sql")]
    help.push_str("  flight-server     Run Arrow Flight SQL endpoint\n");
    #[cfg(not(feature = "flight-sql"))]
    help.push_str(
        "  flight-server     Arrow Flight SQL endpoint [disabled; build with feature `flight-sql`]\n",
    );
    #[cfg(feature = "shuffle")]
    help.push_str("  shuffle-svc       Run optional shuffle HTTP service\n");
    #[cfg(not(feature = "shuffle"))]
    help.push_str(
        "  shuffle-svc       Optional shuffle HTTP service [disabled; build with feature `shuffle`]\n",
    );
    help.push_str("  mcp               Run Model Context Protocol server\n");
    help
}

/// Dispatch `help daemons` output.
pub fn help_daemons() -> CliResponse {
    CliResponse::ok(format!("{}\n", daemons_help()))
}
