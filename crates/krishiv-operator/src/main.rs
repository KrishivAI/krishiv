#![forbid(unsafe_code)]

use std::error::Error;

use krishiv_operator::{BootstrapExecutor, KubernetesControllerConfig, run_kubernetes_controller};
use krishiv_proto::CoordinatorId;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = OperatorCliConfig::parse(std::env::args().skip(1))?;
    if config.help {
        print!("{}", OperatorCliConfig::help());
        return Ok(());
    }

    println!(
        "Krishiv operator watching {} as coordinator {}",
        config.watch_target(),
        config.coordinator_id
    );
    run_kubernetes_controller(config.into_controller_config()?).await?;
    Ok(())
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
    fn rejects_zero_bootstrap_slots() {
        let error = OperatorCliConfig::parse([
            String::from("--bootstrap-executor-slots"),
            String::from("0"),
        ])
        .unwrap_err();

        assert!(error.contains("greater than zero"));
    }
}
