//! Process helpers for spawning Krishiv daemons from the unified binary.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// Path to the current `krishiv` executable (falls back to `krishiv` on `PATH`).
pub fn krishiv_executable() -> OsString {
    std::env::current_exe()
        .map(Into::into)
        .unwrap_or_else(|_| OsString::from("krishiv"))
}

/// Build a command that runs a Krishiv daemon subcommand on the same binary.
pub fn krishiv_daemon_command(subcommand: &str, args: &[&str]) -> Command {
    let mut cmd = Command::new(krishiv_executable());
    cmd.arg(subcommand).args(args);
    cmd
}

/// Spawn a background daemon; returns child PID when successful.
pub fn spawn_krishiv_daemon(
    subcommand: &str,
    args: &[&str],
) -> Result<u32, String> {
    spawn_krishiv_daemon_with_env(subcommand, args, &[])
}

/// Spawn a daemon with extra environment variables `(key, value)`.
pub fn spawn_krishiv_daemon_with_env(
    subcommand: &str,
    args: &[&str],
    env: &[(&str, &str)],
) -> Result<u32, String> {
    let mut cmd = krishiv_daemon_command(subcommand, args);
    for (key, value) in env {
        cmd.env(key, value);
    }
    let child = cmd
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("failed to spawn krishiv {subcommand}: {e}"))?;
    Ok(child.id())
}

/// Resolve data directory for cluster commands.
pub fn cluster_data_dir(default: &str, override_path: Option<&Path>) -> PathBuf {
    override_path.map(PathBuf::from).unwrap_or_else(|| {
        PathBuf::from(
            std::env::var("KRISHIV_CLUSTER_DATA_DIR")
                .unwrap_or_else(|_| default.to_string()),
        )
    })
}
