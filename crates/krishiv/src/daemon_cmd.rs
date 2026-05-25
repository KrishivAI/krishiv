//! Long-running daemon subcommands (`krishiv coordinator`, `krishiv executor`, …).

use krishiv_async_util::block_on;
use krishiv_scheduler::{
    coordinator_daemon_help, job_coordinator_daemon_help, parse_coordinator_daemon_config,
    parse_job_coordinator_daemon_config, run_clusterd_daemon, run_job_coordinator_daemon,
    run_standalone_coordinator,
};

use crate::cli::CliResponse;

/// If `args` starts with a daemon subcommand, run it and return `Some(exit_code)`.
pub fn try_run_daemon(args: &[String]) -> Option<i32> {
    let sub = args.first()?.as_str();
    match sub {
        "coordinator" | "clusterd" | "executor" | "job-coordinator" | "flight-server"
        | "shuffle-svc" => {}
        _ => return None,
    }
    let rest: Vec<String> = args.iter().skip(1).cloned().collect();
    let code = match sub {
        "coordinator" => run_coordinator(&rest),
        "clusterd" => run_clusterd(&rest),
        "executor" => run_executor(&rest),
        "job-coordinator" => run_job_coordinator(&rest),
        "flight-server" => run_flight_server(&rest),
        "shuffle-svc" => run_shuffle_svc(&rest),
        _ => unreachable!(),
    };
    Some(code)
}

fn run_coordinator(args: &[String]) -> i32 {
    match parse_coordinator_daemon_config(args.iter().cloned()) {
        Ok(config) if config.help => {
            print!("{}", coordinator_daemon_help());
            0
        }
        Ok(config) => match block_on(run_standalone_coordinator(config)) {
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

fn run_clusterd(args: &[String]) -> i32 {
    match parse_coordinator_daemon_config(args.iter().cloned()) {
        Ok(config) if config.help => {
            print!("{}", coordinator_daemon_help());
            0
        }
        Ok(config) => match block_on(run_clusterd_daemon(config)) {
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

fn run_job_coordinator(args: &[String]) -> i32 {
    match parse_job_coordinator_daemon_config(args.iter().cloned()) {
        Ok(config) if config.help => {
            print!("{}", job_coordinator_daemon_help());
            0
        }
        Ok(config) => match block_on(run_job_coordinator_daemon(config)) {
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
    match block_on(krishiv_shuffle::shuffle_svc::run_shuffle_svc_from_env()) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("{e}");
            2
        }
    }
}

pub fn daemons_help() -> String {
    format!(
        "Krishiv daemon processes (also available as legacy binary names).\n\
         \n\
         Usage:\n\
           krishiv coordinator [OPTIONS]     Active coordinator (was krishiv-coordinator)\n\
           krishiv clusterd [OPTIONS]        Cluster control plane (was krishiv-clusterd)\n\
           krishiv job-coordinator [OPTS]    Per-job coordinator (was krishiv-job-coordinator)\n\
           krishiv executor [OPTIONS]        Data-plane worker (was krishiv-executor)\n\
           krishiv flight-server             Arrow Flight SQL (was krishiv-flight-server)\n\
           krishiv shuffle-svc               Optional shuffle HTTP service\n\
         \n\
         Legacy binaries remain install aliases; prefer `krishiv <subcommand>`.\n"
    )
}

fn flight_server_help() -> &'static str {
    "Arrow Flight SQL server.\n\
     \n\
     Usage: krishiv flight-server\n\
     Env: KRISHIV_FLIGHT_ADDR (default 127.0.0.1:50051)\n"
}

fn shuffle_svc_help() -> &'static str {
    "Shuffle partition HTTP service.\n\
     \n\
     Usage: krishiv shuffle-svc\n\
     Env: KRISHIV_SHUFFLE_DIR, KRISHIV_SHUFFLE_ADDR (default 0.0.0.0:7072)\n"
}

/// CLI help snippet for main help (daemon section).
pub fn daemon_help_section() -> &'static str {
    "  coordinator       Run active coordinator (distributed control plane)\n\
     clusterd          Run cluster control plane daemon (CCP)\n\
     job-coordinator   Run per-job coordinator (JCP)\n\
     executor          Run data-plane executor worker\n\
     flight-server     Run Arrow Flight SQL endpoint\n\
     shuffle-svc       Run optional shuffle HTTP service\n"
}

/// Dispatch `help daemons` output.
pub fn help_daemons() -> CliResponse {
    CliResponse::ok(format!("{}\n", daemons_help()))
}
