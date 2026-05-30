//! Bare-metal / VM cluster lifecycle (`krishiv cluster start|stop|status|verify-network`).

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

use crate::cli::CliResponse;
use crate::process_util::spawn_krishiv_daemon;

const DEFAULT_CLUSTER_DIR: &str = ".krishiv/cluster";
const DEFAULT_HTTP_ADDR: &str = "127.0.0.1:18080";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub clusterd_grpc: String,
    #[serde(default = "default_ui_url")]
    pub ui_url: String,
    pub metadata_path: PathBuf,
    pub data_dir: PathBuf,
    #[serde(default)]
    pub clusterd_pid: Option<u32>,
    #[serde(default)]
    pub executor_pids: Vec<u32>,
    #[serde(default)]
    pub ui_pid: Option<u32>,
}

fn default_ui_url() -> String {
    ui_url_from_http_addr(DEFAULT_HTTP_ADDR)
}

fn ui_url_from_http_addr(addr: &str) -> String {
    format!("http://{addr}/ui")
}

fn select_cluster_http_addr(preferred: Option<&str>) -> Result<String, String> {
    if let Some(addr) = preferred {
        return Ok(addr.to_string());
    }
    Ok(DEFAULT_HTTP_ADDR.to_string())
}

impl ClusterConfig {
    fn path(data_dir: &Path) -> PathBuf {
        data_dir.join("cluster.json")
    }

    pub fn load(data_dir: &Path) -> Result<Self, String> {
        let raw = fs::read_to_string(Self::path(data_dir))
            .map_err(|e| format!("no cluster at {}: {e}", data_dir.display()))?;
        serde_json::from_str(&raw).map_err(|e| format!("invalid cluster config: {e}"))
    }

    pub fn save(&self) -> Result<(), String> {
        fs::create_dir_all(&self.data_dir).map_err(|e| e.to_string())?;
        let raw = serde_json::to_string_pretty(self).map_err(|e| e.to_string())?;
        fs::write(Self::path(&self.data_dir), raw).map_err(|e| e.to_string())
    }
}

pub fn cluster_help() -> String {
    String::from(
        "Manage a bare-metal Krishiv cluster (clusterd + executors).\n\
         \n\
         Usage:\n\
           krishiv cluster start [--data-dir <DIR>] [--executors <N>] [--http-addr <HOST:PORT>]\n\
           krishiv cluster stop [--data-dir <DIR>]\n\
           krishiv cluster status [--data-dir <DIR>]\n\
           krishiv cluster verify-network [--data-dir <DIR>]\n\
         \n\
         Web UI:\n\
           http://127.0.0.1:18080/ui by default, or use --http-addr <HOST:PORT>.\n",
    )
}

pub fn run_cluster(args: &[&str]) -> CliResponse {
    match args {
        [] | ["--help"] | ["-h"] => CliResponse::ok(format!("{}\n", cluster_help())),
        ["start", rest @ ..] => cluster_start(rest),
        ["stop", rest @ ..] => cluster_stop(rest),
        ["status", rest @ ..] => cluster_status(rest),
        ["verify-network", rest @ ..] => cluster_verify_network(rest),
        [unknown, ..] => CliResponse::err(
            format!(
                "unknown cluster subcommand: {unknown}\n\n{}",
                cluster_help()
            ),
            2,
        ),
    }
}

fn parse_data_dir(args: &[&str]) -> Result<(PathBuf, usize), String> {
    let mut dir = PathBuf::from(
        std::env::var("KRISHIV_CLUSTER_DATA_DIR")
            .unwrap_or_else(|_| DEFAULT_CLUSTER_DIR.to_string()),
    );
    let mut executors = 2usize;
    let mut i = 0;
    while i < args.len() {
        match args[i] {
            "--data-dir" => {
                i += 1;
                dir = PathBuf::from(args.get(i).ok_or("missing --data-dir value")?);
            }
            "--executors" => {
                i += 1;
                executors = args
                    .get(i)
                    .ok_or("missing --executors value")?
                    .parse()
                    .map_err(|_| String::from("--executors must be a positive integer"))?;
            }
            "--http-addr" => {
                i += 1;
                let _ = args.get(i).ok_or("missing --http-addr value")?;
            }
            other => return Err(format!("unknown option: {other}")),
        }
        i += 1;
    }
    Ok((dir, executors))
}

fn parse_http_addr(args: &[&str]) -> Result<Option<String>, String> {
    let mut addr = std::env::var("KRISHIV_CLUSTER_HTTP_ADDR").ok();
    let mut idx = 0;
    while idx < args.len() {
        if args[idx] == "--http-addr" {
            idx += 1;
            addr = Some(
                args.get(idx)
                    .ok_or_else(|| String::from("missing --http-addr value"))?
                    .to_string(),
            );
        }
        idx += 1;
    }
    Ok(addr)
}

fn executor_bind_addrs(idx: usize) -> (String, String) {
    let port = 50055u16 + idx as u16;
    let barrier_port = 50056u16 + idx as u16;
    (
        format!("127.0.0.1:{port}"),
        format!("127.0.0.1:{barrier_port}"),
    )
}

fn cluster_start(args: &[&str]) -> CliResponse {
    let (data_dir, executor_count) = match parse_data_dir(args) {
        Ok(v) => v,
        Err(e) => return CliResponse::err(e, 2),
    };
    let requested_http_addr = match parse_http_addr(args) {
        Ok(addr) => addr,
        Err(e) => return CliResponse::err(e, 2),
    };
    fs::create_dir_all(&data_dir).ok();
    let metadata_path = data_dir.join("metadata.json");
    let meta = metadata_path
        .to_str()
        .unwrap_or("/tmp/krishiv-metadata.json");
    let http_addr = match select_cluster_http_addr(requested_http_addr.as_deref()) {
        Ok(addr) => addr,
        Err(e) => return CliResponse::err(e, 1),
    };
    let clusterd_pid = match spawn_krishiv_daemon(
        "clusterd",
        &[
            "--grpc-addr",
            "127.0.0.1:9090",
            "--http-addr",
            &http_addr,
            "--metadata-backend",
            "json",
            "--metadata-path",
            meta,
        ],
    ) {
        Ok(pid) => pid,
        Err(e) => return CliResponse::err(format!("failed to spawn clusterd: {e}"), 1),
    };
    let mut executor_pids = Vec::new();
    for i in 0..executor_count {
        let (task_addr, barrier_addr) = executor_bind_addrs(i);
        let exec_id = format!("exec-{i}");
        if let Ok(pid) = spawn_krishiv_daemon(
            "executor",
            &[
                "--connect",
                "--executor-id",
                &exec_id,
                "--coordinator",
                "http://127.0.0.1:9090",
                "--task-grpc-addr",
                &task_addr,
                "--barrier-grpc-addr",
                &barrier_addr,
            ],
        ) {
            executor_pids.push(pid);
        }
    }
    let cfg = ClusterConfig {
        clusterd_grpc: String::from("http://127.0.0.1:9090"),
        ui_url: ui_url_from_http_addr(&http_addr),
        metadata_path,
        data_dir: data_dir.clone(),
        clusterd_pid: Some(clusterd_pid),
        executor_pids,
        ui_pid: None,
    };
    if let Err(e) = cfg.save() {
        return CliResponse::err(e, 1);
    }
    CliResponse::ok(format!(
        "Krishiv cluster started in {}\n  clusterd: http://127.0.0.1:9090\n  UI: {}\n  executors: {executor_count}\n  export KRISHIV_COORDINATOR=http://127.0.0.1:9090\n",
        data_dir.display(),
        cfg.ui_url
    ))
}

fn cluster_stop(args: &[&str]) -> CliResponse {
    let (data_dir, _) = match parse_data_dir(args) {
        Ok(v) => v,
        Err(e) => return CliResponse::err(e, 2),
    };
    if let Ok(cfg) = ClusterConfig::load(&data_dir) {
        if let Some(pid) = cfg.clusterd_pid {
            let _ = Command::new("kill").arg(pid.to_string()).status();
        }
        for pid in cfg.executor_pids {
            let _ = Command::new("kill").arg(pid.to_string()).status();
        }
        if let Some(pid) = cfg.ui_pid {
            let _ = Command::new("kill").arg(pid.to_string()).status();
        }
        let _ = fs::remove_file(ClusterConfig::path(&data_dir));
    }
    CliResponse::ok(format!("Krishiv cluster stopped ({})", data_dir.display()))
}

fn cluster_status(args: &[&str]) -> CliResponse {
    let (data_dir, _) = match parse_data_dir(args) {
        Ok(v) => v,
        Err(e) => return CliResponse::err(e, 2),
    };
    match ClusterConfig::load(&data_dir) {
        Ok(cfg) => CliResponse::ok(format!(
            "clusterd={} pid={:?}\nui={} pid={:?}\nexecutors={} pids={:?}\nmetadata={}\n",
            cfg.clusterd_grpc,
            cfg.clusterd_pid,
            cfg.ui_url,
            cfg.ui_pid,
            cfg.executor_pids.len(),
            cfg.executor_pids,
            cfg.metadata_path.display()
        )),
        Err(e) => CliResponse::err(e, 1),
    }
}

fn cluster_verify_network(args: &[&str]) -> CliResponse {
    let (data_dir, _) = match parse_data_dir(args) {
        Ok(v) => v,
        Err(e) => return CliResponse::err(e, 2),
    };
    let _ = data_dir;
    let ports = [9090u16, 18080, 50055, 50056, 50057, 50058];
    let mut lines = Vec::new();
    for port in ports {
        let ok = std::net::TcpListener::bind(("127.0.0.1", port)).is_ok();
        lines.push(format!(
            "port {port}: {}",
            if ok {
                "available (not in use)"
            } else {
                "in use (expected when cluster running)"
            }
        ));
    }
    CliResponse::ok(format!(
        "Network check (bind probe on localhost):\n{}\n",
        lines.join("\n")
    ))
}

#[cfg(test)]
mod tests {
    use super::executor_bind_addrs;

    #[test]
    fn executor_barrier_addr_uses_loopback_host() {
        let (task_addr, barrier_addr) = executor_bind_addrs(0);

        assert_eq!(task_addr, "127.0.0.1:50055");
        assert_eq!(barrier_addr, "127.0.0.1:50056");
    }
}
