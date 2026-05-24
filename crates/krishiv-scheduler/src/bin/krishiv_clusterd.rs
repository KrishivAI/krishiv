#![forbid(unsafe_code)]

//! Cluster control plane daemon for bare-metal / VM deployments (ADR-DIST-03).
//!
//! Equivalent to `krishiv-coordinator` but explicitly runs the
//! [`krishiv_scheduler::ClusterControlPlane`] leader loop.

use std::env;
use std::error::Error;
use std::sync::Arc;

use krishiv_proto::CoordinatorId;
use krishiv_scheduler::{
    ClusterControlPlane, CoordinatorDaemonConfig, build_shared_coordinator,
    run_cluster_control_plane, spawn_coordinator_sidecars,
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = parse_config(env::args().skip(1))?;
    if config.help {
        print!("{}", help());
        return Ok(());
    }

    let shared = build_shared_coordinator(&config)?;
    let coordinator_id = CoordinatorId::try_new(&config.coordinator_id)?;
    let ccp = Arc::new(ClusterControlPlane::from_shared(coordinator_id, shared.clone()));

    spawn_coordinator_sidecars(&shared, &config).await?;

    let listener = TcpListener::bind(config.grpc_addr).await?;
    println!(
        "Krishiv clusterd (CCP) {} gRPC listening on {}",
        config.coordinator_id,
        listener.local_addr()?
    );

    run_cluster_control_plane(ccp, listener).await
}

fn parse_config(args: impl IntoIterator<Item = String>) -> Result<CoordinatorDaemonConfig, Box<dyn Error>> {
    let mut config = CoordinatorDaemonConfig {
        coordinator_id: env::var("KRISHIV_COORDINATOR_ID")
            .unwrap_or_else(|_| String::from("clusterd-local")),
        grpc_addr: env::var("KRISHIV_GRPC_ADDR")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| "0.0.0.0:9090".parse().unwrap()),
        http_addr: env::var("KRISHIV_HTTP_ADDR")
            .ok()
            .and_then(|value| value.parse().ok()),
        shuffle_dir: env::var("KRISHIV_SHUFFLE_DIR").ok().map(std::path::PathBuf::from),
        metadata_backend: env::var("KRISHIV_METADATA_BACKEND").ok(),
        metadata_path: env::var("KRISHIV_METADATA_PATH").ok().map(std::path::PathBuf::from),
        help: false,
    };
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--coordinator-id" => config.coordinator_id = next_arg(&mut args, "--coordinator-id")?,
            "--grpc-addr" => {
                config.grpc_addr = next_arg(&mut args, "--grpc-addr")?
                    .parse()
                    .map_err(|_| "invalid --grpc-addr")?;
            }
            "--http-addr" => {
                config.http_addr = Some(
                    next_arg(&mut args, "--http-addr")?
                        .parse()
                        .map_err(|_| "invalid --http-addr")?,
                );
            }
            "--shuffle-dir" => config.shuffle_dir = Some(std::path::PathBuf::from(next_arg(
                &mut args,
                "--shuffle-dir",
            )?)),
            "--metadata-backend" => {
                config.metadata_backend = Some(next_arg(&mut args, "--metadata-backend")?);
            }
            "--metadata-path" => {
                config.metadata_path =
                    Some(std::path::PathBuf::from(next_arg(&mut args, "--metadata-path")?));
            }
            "--help" | "-h" => config.help = true,
            unknown => return Err(format!("unknown option: {unknown}").into()),
        }
    }
    Ok(config)
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, Box<dyn Error>> {
    args.next()
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| format!("missing value for {flag}").into())
}

fn help() -> &'static str {
    "Krishiv cluster control plane (krishiv-clusterd)\n\
     \n\
     Usage: krishiv-clusterd [OPTIONS]\n\
     \n\
     Options mirror krishiv-coordinator; see krishiv-coordinator --help.\n"
}
