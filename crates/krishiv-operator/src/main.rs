#![forbid(unsafe_code)]

use std::error::Error;
#[cfg(feature = "k8s")]
use std::net::SocketAddr;

#[cfg(feature = "k8s")]
use krishiv_operator::{
    BootstrapExecutor, K8sLeaseElection, KubernetesControllerConfig, KubernetesControllerRuntime,
    run_kubernetes_controller_runtime_with_client,
};
#[cfg(feature = "k8s")]
use krishiv_proto::CoordinatorId;
#[cfg(feature = "k8s")]
use krishiv_scheduler::serve_coordinator_executor_grpc_with_listener;
#[cfg(feature = "k8s")]
use krishiv_scheduler::{LeaderElection, SharedCoordinator};
#[cfg(feature = "k8s")]
use kube::Client;

#[cfg(feature = "k8s")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    install_rustls_crypto_provider();
    let config = OperatorCliConfig::parse(std::env::args().skip(1))?;
    if config.help {
        print!("{}", OperatorCliConfig::help());
        return Ok(());
    }
    let grpc_auth_configured =
        configure_grpc_auth_for_startup(config.executor_grpc_addr.is_some())?;
    let _auth_reload_task = if grpc_auth_configured {
        krishiv_scheduler::spawn_grpc_auth_reload_task_from_env()
    } else {
        None
    };

    let controller_config = config.clone().into_controller_config()?;
    let runtime = KubernetesControllerRuntime::new(&controller_config)?;
    println!(
        "Krishiv operator watching {} as coordinator {}",
        config.watch_target(),
        config.coordinator_id
    );

    let client = Client::try_default().await?;
    // A2/E3: do NOT eagerly start orchestration loops while still Standby.
    // Demote first so the lease loop can promote us atomically once acquired.
    runtime.coordinator().write().await.demote_to_standby();
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
            config.http_sidecar_addr,
        )
        .await?;
    } else {
        run_kubernetes_controller_runtime_with_client(client, controller_config, runtime).await?;
    }
    Ok(())
}

#[cfg(feature = "k8s")]
fn install_rustls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[cfg(feature = "k8s")]
fn configure_grpc_auth_for_startup(exposes_coordinator_grpc: bool) -> Result<bool, Box<dyn Error>> {
    if std::env::var("KRISHIV_ALLOW_ANONYMOUS")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        krishiv_scheduler::set_allow_anonymous().map_err(|error| error.to_string())?;
        return Ok(false);
    }
    if krishiv_scheduler::configure_grpc_auth_provider_from_env() {
        return Ok(true);
    }
    if exposes_coordinator_grpc {
        return Err(format!(
            "--executor-grpc-addr requires {} or {} unless KRISHIV_ALLOW_ANONYMOUS=true",
            krishiv_scheduler::COORDINATOR_BEARER_TOKEN_ENV,
            krishiv_scheduler::COORDINATOR_BEARER_TOKENS_ENV
        )
        .into());
    }
    Ok(false)
}

#[cfg(not(feature = "k8s"))]
fn main() -> Result<(), Box<dyn Error>> {
    let wants_help = std::env::args()
        .skip(1)
        .any(|arg| arg == "--help" || arg == "-h");
    if wants_help {
        print!("{}", disabled_help());
        return Ok(());
    }
    Err(String::from("krishiv-operator requires building with feature `k8s`").into())
}

#[cfg(feature = "k8s")]
async fn run_controller_with_servers(
    client: Client,
    controller_config: KubernetesControllerConfig,
    runtime: KubernetesControllerRuntime,
    status_addr: Option<SocketAddr>,
    executor_grpc_addr: Option<SocketAddr>,
    http_sidecar_addr: SocketAddr,
) -> Result<(), Box<dyn Error>> {
    #[cfg(not(feature = "ui"))]
    if status_addr.is_some() {
        return Err(String::from(
            "--status-addr requires building krishiv-operator with feature `ui`",
        )
        .into());
    }

    let status_listener = match status_addr {
        Some(addr) => {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            println!(
                "Krishiv UI/status HTTP listening on {}",
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

    let http_listener = tokio::net::TcpListener::bind(http_sidecar_addr).await?;
    let http_coordinator = runtime.coordinator().clone();
    let http_config = krishiv_scheduler::CoordinatorDaemonConfig::http_sidecar(
        krishiv_state::checkpoint::DurabilityProfile::DistributedDurable,
    );
    let http_router = krishiv_scheduler::coordinator_http_router(http_coordinator, &http_config);
    let coordinator = runtime.coordinator().clone();

    tokio::select! {
        result = run_kubernetes_controller_runtime_with_client(client, controller_config, runtime) => {
            result?;
        }
        result = axum::serve(http_listener, http_router) => {
            result.map_err(|e| Box::new(e) as Box<dyn Error>)?;
        }
        result = async {
            if let Some(l) = grpc_listener {
                serve_coordinator_executor_grpc_with_listener(l, coordinator.clone()).await
                    .map_err(|e| Box::new(e) as Box<dyn Error>)
            } else {
                let _ = std::future::pending::<()>().await;
                Ok::<(), Box<dyn Error>>(())
            }
        } => { result?; }
        result = async {
            #[cfg(all(feature = "k8s", feature = "ui"))]
            if let Some(l) = status_listener {
                serve_status(l, coordinator.clone()).await
                    .map_err(|e| Box::new(e) as Box<dyn Error>)
            } else {
                let _ = std::future::pending::<()>().await;
                Ok::<(), Box<dyn Error>>(())
            }
            #[cfg(not(all(feature = "k8s", feature = "ui")))]
            {
                let _ = std::future::pending::<()>().await;
                Ok::<(), Box<dyn Error>>(())
            }
        } => { result?; }
    }

    Ok(())
}

#[cfg(all(feature = "k8s", feature = "ui"))]
async fn serve_status(
    listener: tokio::net::TcpListener,
    coordinator: SharedCoordinator,
) -> std::io::Result<()> {
    let state = krishiv_ui::UiState::from_shared_coordinator(coordinator);
    krishiv_ui::serve(listener, state).await
}

#[cfg(all(feature = "k8s", not(feature = "ui")))]
async fn serve_status(
    _listener: tokio::net::TcpListener,
    _coordinator: SharedCoordinator,
) -> std::io::Result<()> {
    tracing::warn!("serve_status requires the 'ui' feature; the status HTTP endpoint is disabled");
    Ok(())
}

#[cfg(feature = "k8s")]
fn spawn_coordinator_leader_election(
    coordinator: SharedCoordinator,
    client: kube::Client,
    namespace: Option<String>,
    coordinator_id: String,
) {
    let ns = namespace.unwrap_or_else(|| "krishiv-system".to_string());
    // Holder identity must be unique per pod so that two operator replicas do
    // not share the same identity and inadvertently both believe they hold the
    // lease.  POD_NAME is the canonical source (set via the downward API);
    // HOSTNAME is the fallback for bare-metal / local runs.
    let holder_identity = std::env::var("POD_NAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| coordinator_id.clone());
    let election: std::sync::Arc<dyn LeaderElection + Send + Sync> = std::sync::Arc::new(
        K8sLeaseElection::new(&coordinator_id, &ns, &holder_identity).with_kube_client(client),
    );

    // E3: orchestration loops are tied to leadership.  We hold the handles
    // in this task so we can stop them on demotion.
    let mut orchestration_handles: Option<krishiv_scheduler::OrchestratorHandles> = None;

    tokio::spawn(async move {
        // Try to acquire synchronously before starting the periodic loop so
        // there is no window where two pods both think they are Active (A2).
        if election.try_acquire().await {
            let mut c = coordinator.write().await;
            c.promote_to_active();
            orchestration_handles = Some(coordinator.spawn_orchestration_loops());
        }

        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let was_leader = election.is_leader();
            let acquired = election.try_acquire().await;
            if acquired {
                coordinator.write().await.promote_to_active();
                if orchestration_handles.is_none() {
                    orchestration_handles = Some(coordinator.spawn_orchestration_loops());
                }
            } else if was_leader {
                let renewed = election.renew().await;
                if !renewed {
                    coordinator.write().await.demote_to_standby();
                    if let Some(handles) = orchestration_handles.take() {
                        handles.shutdown().await;
                    }
                }
            }
            // When !acquired && !was_leader we were already standby — no action needed.
        }
    });
}

#[cfg(not(feature = "k8s"))]
fn disabled_help() -> &'static str {
    "Krishiv operator binary.\n\
     \n\
     This build was compiled without Kubernetes support.\n\
     Rebuild with feature `k8s` or enable the default `cluster` feature.\n"
}

#[cfg(feature = "k8s")]
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
    /// Coordinator HTTP sidecar address (health-check / state API); defaults to 0.0.0.0:8080.
    http_sidecar_addr: SocketAddr,
    /// gRPC endpoint injected into executor pods (KRISHIV_COORDINATOR_ENDPOINT).
    coordinator_endpoint: String,
    help: bool,
}

#[cfg(feature = "k8s")]
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
            http_sidecar_addr: "0.0.0.0:8080"
                .parse()
                .expect("default sidecar addr is valid"),
            coordinator_endpoint: std::env::var("KRISHIV_COORDINATOR_ENDPOINT")
                .unwrap_or_else(|_| String::from("http://krishiv-coordinator:9090")),
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
                "--http-sidecar-addr" => {
                    let value = next_arg(&mut args, "--http-sidecar-addr")?;
                    config.http_sidecar_addr = value
                        .parse()
                        .map_err(|_| format!("invalid socket address: {value}"))?;
                }
                "--coordinator-endpoint" => {
                    config.coordinator_endpoint = next_arg(&mut args, "--coordinator-endpoint")?;
                }
                "--help" | "-h" => config.help = true,
                unknown => return Err(format!("unknown option: {unknown}\n\n{}", Self::help())),
            }
        }

        if config.coordinator_id.trim().is_empty() {
            return Err(String::from("coordinator id cannot be empty"));
        }
        if config.coordinator_endpoint.trim().is_empty() {
            return Err(String::from("coordinator endpoint cannot be empty"));
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
        config = config.with_coordinator_endpoint(self.coordinator_endpoint);

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
           --http-sidecar-addr <HOST:PORT>  Coordinator HTTP sidecar address (default: 0.0.0.0:8080)\n\
           --coordinator-endpoint <URL>     Coordinator gRPC endpoint for executor pods (default: KRISHIV_COORDINATOR_ENDPOINT or http://krishiv-coordinator:9090)\n\
           -h, --help                       Show help\n"
    }
}

#[cfg(feature = "k8s")]
fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing value for {flag}"))
}

#[cfg(all(test, feature = "k8s"))]
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
    fn parses_coordinator_endpoint() {
        let config = OperatorCliConfig::parse([
            String::from("--coordinator-endpoint"),
            String::from("http://coord.example:9090"),
        ])
        .unwrap();
        let controller = config.clone().into_controller_config().unwrap();

        assert_eq!(config.coordinator_endpoint, "http://coord.example:9090");
        assert_eq!(
            controller.coordinator_endpoint(),
            "http://coord.example:9090"
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
