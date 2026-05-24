#![forbid(unsafe_code)]

use std::error::Error;
use std::net::SocketAddr;

use krishiv_operator::{
    BootstrapExecutor, K8sLeaseElection, KubernetesControllerConfig, KubernetesControllerRuntime,
    run_kubernetes_controller_runtime_with_client,
};
use krishiv_proto::CoordinatorId;
use krishiv_scheduler::serve_coordinator_executor_grpc_with_listener;
use krishiv_scheduler::{LeaderElection, SharedCoordinator};
use krishiv_ui::{UiState, serve};
use kube::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = OperatorCliConfig::parse(std::env::args().skip(1))?;
    if config.help {
        print!("{}", OperatorCliConfig::help());
        return Ok(());
    }

    let controller_config = config.clone().into_controller_config()?;
    let runtime = KubernetesControllerRuntime::new(&controller_config)?;
    println!(
        "Krishiv operator watching {} as coordinator {}",
        config.watch_target(),
        config.coordinator_id
    );

    let client = Client::try_default().await?;
    // P0-4: embedded coordinator must tick heartbeats and launch assigned tasks.
    runtime.coordinator().spawn_orchestration_loops();
    spawn_coordinator_leader_election(
        runtime.coordinator(),
        client.clone(),
        controller_config.namespace().map(str::to_string),
        controller_config.coordinator_id().as_str().to_string(),
    );
    if config.status_addr.is_some() || config.executor_grpc_addr.is_some() {
        run_controller_with_servers(
            client,
            controller_config,
            runtime,
            config.status_addr,
            config.executor_grpc_addr,
        )
        .await?;
    } else {
        run_kubernetes_controller_runtime_with_client(client, controller_config, runtime).await?;
    }
    Ok(())
}

async fn run_controller_with_servers(
    client: Client,
    controller_config: KubernetesControllerConfig,
    runtime: KubernetesControllerRuntime,
    status_addr: Option<SocketAddr>,
    executor_grpc_addr: Option<SocketAddr>,
) -> Result<(), Box<dyn Error>> {
    let status_listener = match status_addr {
        Some(addr) => {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            println!(
                "Krishiv operator status API listening on http://{}/ui",
                listener.local_addr()?
            );
            Some(listener)
        }
        None => None,
    };
    let grpc_listener = match executor_grpc_addr {
        Some(addr) => {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            println!(
                "Krishiv coordinator/executor gRPC listening on {}",
                listener.local_addr()?
            );
            Some(listener)
        }
        None => None,
    };

    match (status_listener, grpc_listener) {
        (Some(status_listener), Some(grpc_listener)) => {
            let status_state = UiState::from_shared_coordinator(runtime.coordinator());
            let grpc_coordinator = runtime.coordinator().clone();
            tokio::select! {
                result = run_kubernetes_controller_runtime_with_client(client, controller_config, runtime) => {
                    result?;
                }
                result = serve(status_listener, status_state) => {
                    result?;
                }
                result = serve_coordinator_executor_grpc_with_listener(grpc_listener, grpc_coordinator) => {
                    result?;
                }
            }
        }
        (Some(status_listener), None) => {
            let status_state = UiState::from_shared_coordinator(runtime.coordinator());
            tokio::select! {
                result = run_kubernetes_controller_runtime_with_client(client, controller_config, runtime) => {
                    result?;
                }
                result = serve(status_listener, status_state) => {
                    result?;
                }
            }
        }
        (None, Some(grpc_listener)) => {
            let grpc_coordinator = runtime.coordinator().clone();
            tokio::select! {
                result = run_kubernetes_controller_runtime_with_client(client, controller_config, runtime) => {
                    result?;
                }
                result = serve_coordinator_executor_grpc_with_listener(grpc_listener, grpc_coordinator) => {
                    result?;
                }
            }
        }
        (None, None) => {
            run_kubernetes_controller_runtime_with_client(client, controller_config, runtime)
                .await?;
        }
    }

    Ok(())
}

fn spawn_coordinator_leader_election(
    coordinator: SharedCoordinator,
    client: kube::Client,
    namespace: Option<String>,
    coordinator_id: String,
) {
    let ns = namespace.unwrap_or_else(|| "krishiv-system".to_string());
    let election = std::sync::Arc::new(
        K8sLeaseElection::new(&coordinator_id, &ns, &coordinator_id).with_kube_client(client),
    );
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            ticker.tick().await;
            if election.try_acquire().await {
                if let Ok(mut c) = coordinator.write() {
                    c.promote_to_active();
                }
            } else if election.is_leader() {
                let _ = election.renew().await;
            } else if let Ok(mut c) = coordinator.write() {
                c.demote_to_standby();
            }
        }
    });
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OperatorCliConfig {
    namespace: Option<String>,
    all_namespaces: bool,
    coordinator_id: String,
    label_selector: Option<String>,
    field_selector: Option<String>,
    bootstrap_executor_id: Option<String>,
    bootstrap_executor_host: String,
    bootstrap_executor_slots: Option<usize>,
    status_addr: Option<SocketAddr>,
    executor_grpc_addr: Option<SocketAddr>,
    help: bool,
}

impl OperatorCliConfig {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut config = Self {
            namespace: std::env::var("KRISHIV_NAMESPACE")
                .ok()
                .or_else(|| Some(String::from("krishiv-system"))),
            all_namespaces: false,
            coordinator_id: std::env::var("KRISHIV_COORDINATOR_ID")
                .unwrap_or_else(|_| String::from("coord-kubernetes")),
            label_selector: None,
            field_selector: None,
            bootstrap_executor_id: None,
            bootstrap_executor_host: String::from("operator-bootstrap-executor"),
            bootstrap_executor_slots: None,
            status_addr: None,
            executor_grpc_addr: None,
            help: false,
        };
        let mut args = args.into_iter();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--namespace" => {
                    config.namespace = Some(next_arg(&mut args, "--namespace")?);
                    config.all_namespaces = false;
                }
                "--all-namespaces" => {
                    config.namespace = None;
                    config.all_namespaces = true;
                }
                "--coordinator-id" => {
                    config.coordinator_id = next_arg(&mut args, "--coordinator-id")?;
                }
                "--label-selector" => {
                    config.label_selector = Some(next_arg(&mut args, "--label-selector")?);
                }
                "--field-selector" => {
                    config.field_selector = Some(next_arg(&mut args, "--field-selector")?);
                }
                "--bootstrap-executor-id" => {
                    config.bootstrap_executor_id =
                        Some(next_arg(&mut args, "--bootstrap-executor-id")?);
                }
                "--bootstrap-executor-host" => {
                    config.bootstrap_executor_host =
                        next_arg(&mut args, "--bootstrap-executor-host")?;
                }
                "--bootstrap-executor-slots" => {
                    let value = next_arg(&mut args, "--bootstrap-executor-slots")?;
                    let slots = value
                        .parse::<usize>()
                        .map_err(|_| String::from("--bootstrap-executor-slots must be a number"))?;
                    if slots == 0 {
                        return Err(String::from(
                            "--bootstrap-executor-slots must be greater than zero",
                        ));
                    }
                    config.bootstrap_executor_slots = Some(slots);
                }
                "--status-addr" => {
                    let value = next_arg(&mut args, "--status-addr")?;
                    config.status_addr = Some(
                        value
                            .parse()
                            .map_err(|_| format!("invalid socket address: {value}"))?,
                    );
                }
                "--executor-grpc-addr" => {
                    let value = next_arg(&mut args, "--executor-grpc-addr")?;
                    config.executor_grpc_addr = Some(
                        value
                            .parse()
                            .map_err(|_| format!("invalid socket address: {value}"))?,
                    );
                }
                "--help" | "-h" => config.help = true,
                unknown => return Err(format!("unknown option: {unknown}\n\n{}", Self::help())),
            }
        }

        if config.coordinator_id.trim().is_empty() {
            return Err(String::from("coordinator id cannot be empty"));
        }

        Ok(config)
    }

    fn into_controller_config(self) -> Result<KubernetesControllerConfig, String> {
        let coordinator_id =
            CoordinatorId::try_new(self.coordinator_id).map_err(|error| error.to_string())?;
        let mut config = if self.all_namespaces {
            KubernetesControllerConfig::all_namespaces(coordinator_id)
        } else {
            KubernetesControllerConfig::namespaced(
                self.namespace
                    .unwrap_or_else(|| String::from("krishiv-system")),
                coordinator_id,
            )
        };

        if let Some(selector) = self.label_selector {
            config = config.with_label_selector(selector);
        }
        if let Some(selector) = self.field_selector {
            config = config.with_field_selector(selector);
        }
        if let Some(slots) = self.bootstrap_executor_slots {
            let executor_id = self
                .bootstrap_executor_id
                .unwrap_or_else(|| String::from("exec-operator-bootstrap"));
            config = config.with_bootstrap_executor(BootstrapExecutor::new(
                executor_id,
                self.bootstrap_executor_host,
                slots,
            ));
        }

        Ok(config)
    }

    fn watch_target(&self) -> String {
        if self.all_namespaces {
            String::from("all namespaces")
        } else {
            format!(
                "namespace {}",
                self.namespace.as_deref().unwrap_or("krishiv-system")
            )
        }
    }

    fn help() -> &'static str {
        "Run the Krishiv R2 Kubernetes operator.\n\
         \n\
         Usage:\n\
           krishiv-operator [--namespace <NS>|--all-namespaces] [OPTIONS]\n\
         \n\
         Options:\n\
           --namespace <NS>                 Namespace to watch, defaults to KRISHIV_NAMESPACE or krishiv-system\n\
           --all-namespaces                 Watch KrishivJob resources in all namespaces\n\
           --coordinator-id <ID>            Coordinator id, defaults to KRISHIV_COORDINATOR_ID or coord-kubernetes\n\
           --label-selector <SELECTOR>      Optional Kubernetes label selector\n\
           --field-selector <SELECTOR>      Optional Kubernetes field selector\n\
           --bootstrap-executor-id <ID>     Optional bootstrap executor id\n\
           --bootstrap-executor-host <HOST> Optional bootstrap executor host label\n\
           --bootstrap-executor-slots <N>   Register a bootstrap executor with N slots\n\
           --status-addr <HOST:PORT>        Serve scheduler-backed status API/UI on this address\n\
           --executor-grpc-addr <HOST:PORT> Serve coordinator/executor gRPC on this address\n\
           -h, --help                       Show help\n"
    }
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing value for {flag}"))
}

#[cfg(test)]
mod tests {
    use super::OperatorCliConfig;

    #[test]
    fn parses_defaults() {
        let config = OperatorCliConfig::parse([]).unwrap();

        assert_eq!(config.namespace.as_deref(), Some("krishiv-system"));
        assert_eq!(config.coordinator_id, "coord-kubernetes");
        assert!(!config.all_namespaces);
    }

    #[test]
    fn parses_all_namespaces_and_bootstrap_executor() {
        let config = OperatorCliConfig::parse([
            String::from("--all-namespaces"),
            String::from("--coordinator-id"),
            String::from("coord-1"),
            String::from("--bootstrap-executor-slots"),
            String::from("2"),
        ])
        .unwrap();
        let controller = config.clone().into_controller_config().unwrap();

        assert!(config.all_namespaces);
        assert_eq!(controller.namespace(), None);
        assert_eq!(controller.coordinator_id().as_str(), "coord-1");
    }

    #[test]
    fn parses_status_addr() {
        let config =
            OperatorCliConfig::parse([String::from("--status-addr"), String::from("0.0.0.0:8080")])
                .unwrap();

        assert_eq!(config.status_addr.unwrap().to_string(), "0.0.0.0:8080");
    }

    #[test]
    fn parses_executor_grpc_addr() {
        let config = OperatorCliConfig::parse([
            String::from("--executor-grpc-addr"),
            String::from("0.0.0.0:9090"),
        ])
        .unwrap();

        assert_eq!(
            config.executor_grpc_addr.unwrap().to_string(),
            "0.0.0.0:9090"
        );
    }

    #[test]
    fn rejects_zero_bootstrap_slots() {
        let error = OperatorCliConfig::parse([
            String::from("--bootstrap-executor-slots"),
            String::from("0"),
        ])
        .unwrap_err();

        assert!(error.contains("greater than zero"));
    }
}
