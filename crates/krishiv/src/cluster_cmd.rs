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
    #[serde(default = "default_http_addr")]
    pub http_addr: String,
    pub metadata_path: PathBuf,
    pub data_dir: PathBuf,
    #[serde(default)]
    pub clusterd_pid: Option<u32>,
    #[serde(default)]
    pub executor_pids: Vec<u32>,
}

fn default_http_addr() -> String {
    DEFAULT_HTTP_ADDR.to_string()
}

impl ClusterConfig {
    fn path(data_dir: &Path) -> PathBuf {
        data_dir.join("cluster.json")
    }

    pub fn ui_url(&self) -> String {
        format!("http://{}/ui", self.http_addr)
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

/// Returns (task_port, barrier_port) for executor at `idx`.
/// Stride is 2 so adjacent executors never share a port:
///   idx=0 → (50055, 50056), idx=1 → (50057, 50058), …
fn executor_port_pair(idx: usize) -> (u16, u16) {
    let base = 50055u16 + (idx as u16) * 2;
    (base, base + 1)
}

fn executor_bind_addrs(idx: usize) -> (String, String) {
    let (task_port, barrier_port) = executor_port_pair(idx);
    (
        format!("127.0.0.1:{task_port}"),
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
    let http_addr = requested_http_addr.unwrap_or_else(|| DEFAULT_HTTP_ADDR.to_string());
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
        http_addr: http_addr.clone(),
        metadata_path,
        data_dir: data_dir.clone(),
        clusterd_pid: Some(clusterd_pid),
        executor_pids,
    };
    if let Err(e) = cfg.save() {
        return CliResponse::err(e, 1);
    }
    CliResponse::ok(format!(
        "Krishiv cluster started in {}\n  clusterd: http://127.0.0.1:9090\n  UI: {}\n  executors: {executor_count}\n  export KRISHIV_COORDINATOR=http://127.0.0.1:9090\n",
        data_dir.display(),
        cfg.ui_url()
    ))
}

fn cluster_stop(args: &[&str]) -> CliResponse {
    let (data_dir, _) = match parse_data_dir(args) {
        Ok(v) => v,
        Err(e) => return CliResponse::err(e, 2),
    };
    let mut warnings: Vec<String> = Vec::new();
    if let Ok(cfg) = ClusterConfig::load(&data_dir) {
        if let Some(pid) = cfg.clusterd_pid
            && let Err(error) = Command::new("kill").arg(pid.to_string()).status()
        {
            warnings.push(format!("kill clusterd pid {pid} failed: {error}"));
        }
        for pid in cfg.executor_pids {
            if let Err(error) = Command::new("kill").arg(pid.to_string()).status() {
                warnings.push(format!("kill executor pid {pid} failed: {error}"));
            }
        }
        if let Err(error) = fs::remove_file(ClusterConfig::path(&data_dir)) {
            warnings.push(format!(
                "remove cluster config {}: {error}",
                ClusterConfig::path(&data_dir).display()
            ));
        }
    }
    if warnings.is_empty() {
        CliResponse::ok(format!("Krishiv cluster stopped ({})", data_dir.display()))
    } else {
        CliResponse::ok(format!(
            "Krishiv cluster stopped ({}). Warnings:\n  - {}",
            data_dir.display(),
            warnings.join("\n  - "),
        ))
    }
}

fn cluster_status(args: &[&str]) -> CliResponse {
    let (data_dir, _) = match parse_data_dir(args) {
        Ok(v) => v,
        Err(e) => return CliResponse::err(e, 2),
    };
    match ClusterConfig::load(&data_dir) {
        Ok(cfg) => CliResponse::ok(format!(
            "clusterd={} pid={:?}\nui={}\nexecutors={} pids={:?}\nmetadata={}\n",
            cfg.clusterd_grpc,
            cfg.clusterd_pid,
            cfg.ui_url(),
            cfg.executor_pids.len(),
            cfg.executor_pids,
            cfg.metadata_path.display()
        )),
        Err(e) => CliResponse::err(e, 1),
    }
}

fn cluster_verify_network(args: &[&str]) -> CliResponse {
    let (data_dir, default_executor_count) = match parse_data_dir(args) {
        Ok(v) => v,
        Err(e) => return CliResponse::err(e, 2),
    };

    let (executor_count, http_addr) = match ClusterConfig::load(&data_dir) {
        Ok(cfg) => (cfg.executor_pids.len(), cfg.http_addr),
        Err(_) => (default_executor_count, DEFAULT_HTTP_ADDR.to_string()),
    };

    let http_port: u16 = http_addr
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse().ok())
        .unwrap_or(18080);

    let mut ports = vec![9090u16, http_port];
    for i in 0..executor_count {
        let (task, barrier) = executor_port_pair(i);
        ports.push(task);
        ports.push(barrier);
    }

    let lines: Vec<String> = ports
        .iter()
        .map(|&port| {
            let ok = std::net::TcpListener::bind(("127.0.0.1", port)).is_ok();
            format!(
                "port {port}: {}",
                if ok {
                    "available (not in use)"
                } else {
                    "in use (expected when cluster running)"
                }
            )
        })
        .collect();

    CliResponse::ok(format!(
        "Network check (bind probe on localhost):\n{}\n",
        lines.join("\n")
    ))
}

#[cfg(test)]
mod tests {
    use super::{executor_bind_addrs, executor_port_pair};

    #[test]
    fn executor_addrs_stride_2_no_collision() {
        let (t0, b0) = executor_bind_addrs(0);
        let (t1, b1) = executor_bind_addrs(1);

        assert_eq!(t0, "127.0.0.1:50055");
        assert_eq!(b0, "127.0.0.1:50056");
        assert_eq!(t1, "127.0.0.1:50057");
        assert_eq!(b1, "127.0.0.1:50058");

        // No port appears twice across the first two executors.
        let ports = [
            executor_port_pair(0).0,
            executor_port_pair(0).1,
            executor_port_pair(1).0,
            executor_port_pair(1).1,
        ];
        let unique: std::collections::HashSet<_> = ports.iter().collect();
        assert_eq!(unique.len(), ports.len(), "port collision detected");
    }
}
