//! Spark-like local cluster lifecycle (`krishiv local start|stop|status`).

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

use crate::cli::CliResponse;
use crate::process_util::{spawn_krishiv_daemon, spawn_krishiv_daemon_with_env};

const DEFAULT_DATA_DIR: &str = ".krishiv/local";
const CONFIG_FILE: &str = "cluster.json";
const DEFAULT_HTTP_ADDR: &str = "127.0.0.1:18080";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalClusterConfig {
    pub coordinator_grpc: String,
    pub coordinator_http: String,
    pub coordinator_flight: String,
    #[serde(default = "default_ui_url")]
    pub ui_url: String,
    pub executor_id: String,
    pub data_dir: PathBuf,
    #[serde(default)]
    pub coordinator_pid: Option<u32>,
    #[serde(default)]
    pub executor_pid: Option<u32>,
    #[serde(default)]
    pub flight_pid: Option<u32>,
    #[serde(default)]
    pub ui_pid: Option<u32>,
}

fn default_ui_url() -> String {
    ui_url_from_http_addr(DEFAULT_HTTP_ADDR)
}

fn ui_url_from_http_addr(addr: &str) -> String {
    format!("http://{addr}/ui")
}

fn select_local_http_addr(preferred: Option<&str>) -> Result<String, String> {
    if let Some(addr) = preferred {
        return Ok(addr.to_string());
    }
    Ok(DEFAULT_HTTP_ADDR.to_string())
}

impl LocalClusterConfig {
    fn path(data_dir: &Path) -> PathBuf {
        data_dir.join(CONFIG_FILE)
    }

    pub fn load(data_dir: &Path) -> Result<Self, String> {
        let path = Self::path(data_dir);
        let raw = fs::read_to_string(&path)
            .map_err(|e| format!("no local cluster at {}: {e}", path.display()))?;
        serde_json::from_str(&raw).map_err(|e| format!("invalid cluster config: {e}"))
    }

    pub fn save(&self) -> Result<(), String> {
        fs::create_dir_all(&self.data_dir).map_err(|e| e.to_string())?;
        let path = Self::path(&self.data_dir);
        let raw = serde_json::to_string_pretty(self).map_err(|e| e.to_string())?;
        fs::write(path, raw).map_err(|e| e.to_string())
    }
}

pub fn local_help() -> String {
    String::from(
        "Manage a Spark-like local Krishiv cluster (coordinator + executor).\n\
         \n\
         Usage:\n\
           krishiv local start [--data-dir <DIR>] [--http-addr <HOST:PORT>]\n\
           krishiv local stop [--data-dir <DIR>]\n\
           krishiv local status [--data-dir <DIR>]\n\
         \n\
         After start, use:\n\
           export KRISHIV_COORDINATOR=http://127.0.0.1:50051\n\
           (flight-server uses KRISHIV_COORDINATOR_HTTP=http://127.0.0.1:18080 for executor-backed SQL)\n\
          krishiv sql --mode single-node --query 'SELECT 1'\n\
         \n\
         Web UI:\n\
           http://127.0.0.1:18080/ui by default, or use --http-addr <HOST:PORT>.\n",
    )
}

pub fn run_local(args: &[&str]) -> CliResponse {
    match args {
        [] | ["--help"] | ["-h"] => CliResponse::ok(format!("{}\n", local_help())),
        ["start", rest @ ..] => run_local_start(rest),
        ["stop", rest @ ..] => run_local_stop(rest),
        ["status", rest @ ..] => run_local_status(rest),
        [unknown, ..] => CliResponse::err(
            format!("unknown local subcommand: {unknown}\n\n{}", local_help()),
            2,
        ),
    }
}

fn parse_data_dir(args: &[&str]) -> Result<PathBuf, String> {
    let mut dir = PathBuf::from(
        std::env::var("KRISHIV_LOCAL_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string()),
    );
    let mut idx = 0;
    while idx < args.len() {
        match args[idx] {
            "--data-dir" => {
                idx += 1;
                dir = PathBuf::from(
                    args.get(idx)
                        .ok_or_else(|| String::from("missing value for --data-dir"))?,
                );
            }
            "--http-addr" => {
                idx += 1;
                let _ = args
                    .get(idx)
                    .ok_or_else(|| String::from("missing value for --http-addr"))?;
            }
            other => return Err(format!("unknown option: {other}")),
        }
        idx += 1;
    }
    Ok(dir)
}

fn parse_http_addr(args: &[&str]) -> Result<Option<String>, String> {
    let mut addr = std::env::var("KRISHIV_LOCAL_HTTP_ADDR").ok();
    let mut idx = 0;
    while idx < args.len() {
        if args[idx] == "--http-addr" {
            idx += 1;
            addr = Some(
                args.get(idx)
                    .ok_or_else(|| String::from("missing value for --http-addr"))?
                    .to_string(),
            );
        }
        idx += 1;
    }
    Ok(addr)
}

fn run_local_start(args: &[&str]) -> CliResponse {
    let data_dir = match parse_data_dir(args) {
        Ok(d) => d,
        Err(e) => return CliResponse::err(format!("{e}\n"), 2),
    };
    let requested_http_addr = match parse_http_addr(args) {
        Ok(addr) => addr,
        Err(e) => return CliResponse::err(format!("{e}\n"), 2),
    };
    if let Ok(mut existing) = LocalClusterConfig::load(&data_dir) {
        if existing.ui_url != default_ui_url() || existing.ui_pid.is_some() {
            existing.ui_url = default_ui_url();
            existing.ui_pid = None;
            if let Err(e) = existing.save() {
                return CliResponse::err(format!("{e}\n"), 1);
            }
        }
        return CliResponse::err(
            format!(
                "local cluster already running (config at {})\nUI: {}\n",
                LocalClusterConfig::path(&data_dir).display(),
                existing.ui_url
            ),
            1,
        );
    }

    fs::create_dir_all(&data_dir).ok();

    let meta_path = data_dir.join("coordinator-meta.json");
    let grpc_addr = "127.0.0.1:9090";
    let http_addr = match select_local_http_addr(requested_http_addr.as_deref()) {
        Ok(addr) => addr,
        Err(e) => return CliResponse::err(format!("{e}\n"), 1),
    };

    let meta = meta_path.to_str().unwrap_or("coordinator-meta.json");
    let coordinator_pid = match spawn_krishiv_daemon(
        "coordinator",
        &[
            "--grpc-addr",
            grpc_addr,
            "--http-addr",
            &http_addr,
            "--metadata-backend",
            "json",
            "--metadata-path",
            meta,
        ],
    ) {
        Ok(pid) => pid,
        Err(e) => {
            return CliResponse::err(
                format!(
                    "failed to spawn krishiv coordinator: {e}\n\
                     Build with: cargo build -p krishiv --bin krishiv\n"
                ),
                1,
            );
        }
    };

    std::thread::sleep(std::time::Duration::from_millis(500));

    let coordinator_http = format!("http://{http_addr}");
    let flight_pid = match spawn_krishiv_daemon_with_env(
        "flight-server",
        &[],
        &[
            ("KRISHIV_FLIGHT_ADDR", "127.0.0.1:50051"),
            ("KRISHIV_COORDINATOR_HTTP", coordinator_http.as_str()),
        ],
    ) {
        Ok(pid) => pid,
        Err(e) => {
            let _ = Command::new("kill")
                .arg(coordinator_pid.to_string())
                .status();
            return CliResponse::err(
                format!(
                    "failed to spawn krishiv flight-server: {e}\n\
                     Build with: cargo build -p krishiv --bin krishiv\n"
                ),
                1,
            );
        }
    };

    std::thread::sleep(std::time::Duration::from_millis(300));

    let executor_pid = match spawn_krishiv_daemon(
        "executor",
        &[
            "--connect",
            "--coordinator",
            &format!("http://{grpc_addr}"),
            "--executor-id",
            "local-exec-1",
        ],
    ) {
        Ok(pid) => pid,
        Err(err) => {
            let _ = Command::new("kill")
                .arg(coordinator_pid.to_string())
                .status();
            let _ = Command::new("kill").arg(flight_pid.to_string()).status();
            return CliResponse::err(
                format!(
                    "failed to spawn krishiv executor: {err}\n\
                     Build with: cargo build -p krishiv --bin krishiv\n"
                ),
                1,
            );
        }
    };

    let config = LocalClusterConfig {
        coordinator_grpc: format!("http://{grpc_addr}"),
        coordinator_http: format!("http://{}", http_addr),
        coordinator_flight: String::from("http://127.0.0.1:50051"),
        ui_url: ui_url_from_http_addr(&http_addr),
        executor_id: String::from("local-exec-1"),
        data_dir: data_dir.clone(),
        coordinator_pid: Some(coordinator_pid),
        executor_pid: Some(executor_pid),
        flight_pid: Some(flight_pid),
        ui_pid: None,
    };

    match config.save() {
        Ok(()) => CliResponse::ok(format!(
            "Started local Krishiv cluster.\n\
             gRPC:    {}\n\
             HTTP:    {}\n\
             Flight:  {}\n\
             UI:      {}\n\
             data:    {}\n\
             \n\
             export KRISHIV_COORDINATOR={}\n",
            config.coordinator_grpc,
            config.coordinator_http,
            config.coordinator_flight,
            config.ui_url,
            config.data_dir.display(),
            config.coordinator_flight,
        )),
        Err(e) => CliResponse::err(format!("{e}\n"), 1),
    }
}

fn run_local_stop(args: &[&str]) -> CliResponse {
    let data_dir = match parse_data_dir(args) {
        Ok(d) => d,
        Err(e) => return CliResponse::err(format!("{e}\n"), 2),
    };
    let config = match LocalClusterConfig::load(&data_dir) {
        Ok(c) => c,
        Err(e) => return CliResponse::err(format!("{e}\n"), 1),
    };
    if let Some(pid) = config.flight_pid {
        let _ = Command::new("kill").arg(pid.to_string()).status();
    }
    if let Some(pid) = config.ui_pid {
        let _ = Command::new("kill").arg(pid.to_string()).status();
    }
    if let Some(pid) = config.executor_pid {
        let _ = Command::new("kill").arg(pid.to_string()).status();
    }
    if let Some(pid) = config.coordinator_pid {
        let _ = Command::new("kill").arg(pid.to_string()).status();
    }
    let _ = fs::remove_file(LocalClusterConfig::path(&data_dir));
    CliResponse::ok("Stopped local Krishiv cluster.\n".to_string())
}

fn run_local_status(args: &[&str]) -> CliResponse {
    let data_dir = match parse_data_dir(args) {
        Ok(d) => d,
        Err(e) => return CliResponse::err(format!("{e}\n"), 2),
    };
    match LocalClusterConfig::load(&data_dir) {
        Ok(c) => CliResponse::ok(format!(
            "Local cluster: running\n\
             gRPC:    {}\n\
             HTTP:    {}\n\
             Flight:  {}\n\
             UI:      {}\n\
             coordinator pid: {:?}\n\
             executor pid: {:?}\n\
             flight pid: {:?}\n\
             ui pid: {:?}\n",
            c.coordinator_grpc,
            c.coordinator_http,
            c.coordinator_flight,
            c.ui_url,
            c.coordinator_pid,
            c.executor_pid,
            c.flight_pid,
            c.ui_pid
        )),
        Err(e) => CliResponse::err(format!("{e}\n"), 1),
    }
}
