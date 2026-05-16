#![forbid(unsafe_code)]

use std::env;
use std::process;

use krishiv_executor::{ExecutorConfig, ExecutorRuntime};

fn main() {
    match run(env::args().skip(1)) {
        Ok(()) => {}
        Err(error) => {
            eprintln!("{error}");
            process::exit(2);
        }
    }
}

fn run(args: impl IntoIterator<Item = String>) -> Result<(), String> {
    let config = ExecutorCliConfig::parse(args)?;
    if config.help {
        print!("{}", ExecutorCliConfig::help());
        return Ok(());
    }

    let runtime = ExecutorRuntime::new(config.into_executor_config()?);
    let registration = runtime.registration_request();
    let heartbeat = runtime.heartbeat_request();

    println!("{}", runtime.startup_summary());
    println!(
        "registration version={} executor={} host={} slots={}",
        registration.version(),
        registration.descriptor().executor_id(),
        registration.descriptor().host(),
        registration.descriptor().slots()
    );
    println!(
        "heartbeat version={} executor={} lease_generation={} state={} running_attempts={}",
        heartbeat.version(),
        heartbeat.executor_id(),
        heartbeat.lease_generation(),
        heartbeat.state(),
        heartbeat.running_attempts().len()
    );
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExecutorCliConfig {
    executor_id: String,
    host: String,
    slots: usize,
    coordinator_endpoint: String,
    help: bool,
}

impl ExecutorCliConfig {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut config = Self {
            executor_id: env::var("KRISHIV_EXECUTOR_ID")
                .unwrap_or_else(|_| String::from("exec-local")),
            host: env::var("HOSTNAME").unwrap_or_else(|_| String::from("localhost")),
            slots: env::var("KRISHIV_TASK_SLOTS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1),
            coordinator_endpoint: env::var("KRISHIV_COORDINATOR_ENDPOINT")
                .unwrap_or_else(|_| String::from("http://127.0.0.1:8080")),
            help: false,
        };
        let mut args = args.into_iter();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--executor-id" => config.executor_id = next_arg(&mut args, "--executor-id")?,
                "--host" => config.host = next_arg(&mut args, "--host")?,
                "--slots" => {
                    let value = next_arg(&mut args, "--slots")?;
                    config.slots = value
                        .parse()
                        .map_err(|_| String::from("--slots must be a positive integer"))?;
                }
                "--coordinator" => {
                    config.coordinator_endpoint = next_arg(&mut args, "--coordinator")?;
                }
                "--help" | "-h" => config.help = true,
                unknown => return Err(format!("unknown option: {unknown}\n\n{}", Self::help())),
            }
        }

        Ok(config)
    }

    fn into_executor_config(self) -> Result<ExecutorConfig, String> {
        ExecutorConfig::new(
            self.executor_id,
            self.host,
            self.slots,
            self.coordinator_endpoint,
        )
        .map_err(|error| error.to_string())
    }

    fn help() -> &'static str {
        "Run the Krishiv R3.1 executor skeleton.\n\
         \n\
         Usage:\n\
           krishiv-executor [OPTIONS]\n\
         \n\
         Options:\n\
           --executor-id <ID>       Executor id, defaults to KRISHIV_EXECUTOR_ID or exec-local\n\
           --host <HOST>            Host or pod name, defaults to HOSTNAME or localhost\n\
           --slots <N>              Task slots, defaults to KRISHIV_TASK_SLOTS or 1\n\
           --coordinator <URL>      Coordinator endpoint, defaults to KRISHIV_COORDINATOR_ENDPOINT or http://127.0.0.1:8080\n\
           -h, --help               Show help\n"
    }
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing value for {flag}"))
}

#[cfg(test)]
mod tests {
    use super::ExecutorCliConfig;

    #[test]
    fn parses_explicit_config() {
        let config = ExecutorCliConfig::parse([
            String::from("--executor-id"),
            String::from("exec-1"),
            String::from("--host"),
            String::from("pod-a"),
            String::from("--slots"),
            String::from("2"),
            String::from("--coordinator"),
            String::from("http://coordinator"),
        ])
        .unwrap();

        assert_eq!(config.executor_id, "exec-1");
        assert_eq!(config.host, "pod-a");
        assert_eq!(config.slots, 2);
        assert_eq!(config.coordinator_endpoint, "http://coordinator");
    }

    #[test]
    fn rejects_unknown_option() {
        let error = ExecutorCliConfig::parse([String::from("--wat")]).unwrap_err();

        assert!(error.contains("unknown option"));
    }
}
