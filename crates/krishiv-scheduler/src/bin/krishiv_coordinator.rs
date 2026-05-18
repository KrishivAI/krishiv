#![forbid(unsafe_code)]

//! Standalone `krishiv-coordinator` binary for bare-metal / VM deployments.
//!
//! For Kubernetes deployments use `krishiv-operator`, which embeds the
//! coordinator alongside the CRD reconciler. This binary is for bare metal
//! or VM environments where Kubernetes is not available.
//!
//! Usage:
//!   krishiv-coordinator [--coordinator-id <ID>] [--grpc-addr <HOST:PORT>]
//!
//! Options:
//!   --coordinator-id <ID>     Coordinator id, defaults to KRISHIV_COORDINATOR_ID or coord-local
//!   --grpc-addr <HOST:PORT>   gRPC listen address, defaults to KRISHIV_GRPC_ADDR or 0.0.0.0:9090
//!   -h, --help                Show help

use std::env;
use std::error::Error;
use std::net::SocketAddr;

use krishiv_proto::CoordinatorId;
use krishiv_scheduler::{
    Coordinator, SharedCoordinator, serve_coordinator_executor_grpc_with_listener,
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = CoordinatorCliConfig::parse(env::args().skip(1))?;
    if config.help {
        print!("{}", CoordinatorCliConfig::help());
        return Ok(());
    }

    let coordinator_id = CoordinatorId::try_new(&config.coordinator_id)
        .map_err(|error| format!("invalid coordinator id: {error}"))?;
    let coordinator = SharedCoordinator::new(Coordinator::active(coordinator_id));

    let listener = TcpListener::bind(config.grpc_addr).await?;
    println!(
        "Krishiv coordinator {} gRPC listening on {}",
        config.coordinator_id,
        listener.local_addr()?
    );

    serve_coordinator_executor_grpc_with_listener(listener, coordinator).await?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CoordinatorCliConfig {
    coordinator_id: String,
    grpc_addr: SocketAddr,
    help: bool,
}

impl CoordinatorCliConfig {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, Box<dyn Error>> {
        let mut config = Self {
            coordinator_id: env::var("KRISHIV_COORDINATOR_ID")
                .unwrap_or_else(|_| String::from("coord-local")),
            grpc_addr: env::var("KRISHIV_GRPC_ADDR")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or_else(|| "0.0.0.0:9090".parse().unwrap()),
            help: false,
        };
        let mut args = args.into_iter();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--coordinator-id" => {
                    config.coordinator_id = next_arg(&mut args, "--coordinator-id")?;
                }
                "--grpc-addr" => {
                    let value = next_arg(&mut args, "--grpc-addr")?;
                    config.grpc_addr = value
                        .parse()
                        .map_err(|_| format!("invalid socket address for --grpc-addr: {value}"))?;
                }
                "--help" | "-h" => config.help = true,
                unknown => {
                    return Err(format!("unknown option: {unknown}\n\n{}", Self::help()).into());
                }
            }
        }

        if config.coordinator_id.trim().is_empty() {
            return Err("coordinator id cannot be empty".into());
        }

        Ok(config)
    }

    fn help() -> &'static str {
        "Run the Krishiv coordinator for bare-metal / VM deployments.\n\
         \n\
         Usage:\n\
           krishiv-coordinator [OPTIONS]\n\
         \n\
         Options:\n\
           --coordinator-id <ID>     Coordinator id, defaults to KRISHIV_COORDINATOR_ID or coord-local\n\
           --grpc-addr <HOST:PORT>   gRPC listen address, defaults to KRISHIV_GRPC_ADDR or 0.0.0.0:9090\n\
           -h, --help                Show help\n"
    }
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, Box<dyn Error>> {
    args.next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing value for {flag}").into())
}

#[cfg(test)]
mod tests {
    use super::CoordinatorCliConfig;

    #[test]
    fn parses_defaults() {
        let config = CoordinatorCliConfig::parse([]).unwrap();
        assert_eq!(config.coordinator_id, "coord-local");
        assert_eq!(config.grpc_addr.port(), 9090);
        assert!(!config.help);
    }

    #[test]
    fn parses_explicit_flags() {
        let config = CoordinatorCliConfig::parse([
            String::from("--coordinator-id"),
            String::from("coord-prod"),
            String::from("--grpc-addr"),
            String::from("127.0.0.1:19090"),
        ])
        .unwrap();

        assert_eq!(config.coordinator_id, "coord-prod");
        assert_eq!(config.grpc_addr.to_string(), "127.0.0.1:19090");
    }

    #[test]
    fn parses_help_flag() {
        let config = CoordinatorCliConfig::parse([String::from("--help")]).unwrap();
        assert!(config.help);
    }

    #[test]
    fn rejects_unknown_flag() {
        let error = CoordinatorCliConfig::parse([String::from("--wat")]).unwrap_err();
        assert!(error.to_string().contains("unknown option"));
    }

    #[test]
    fn rejects_invalid_grpc_addr() {
        let error = CoordinatorCliConfig::parse([
            String::from("--grpc-addr"),
            String::from("not-a-socket-addr"),
        ])
        .unwrap_err();
        assert!(error.to_string().contains("invalid socket address"));
    }
}
