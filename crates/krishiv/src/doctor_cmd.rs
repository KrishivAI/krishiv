//! `krishiv doctor` — resolve and print the effective deployment configuration.
//!
//! Krishiv is configured by the `KRISHIV_*` flags declared in
//! `krishiv_common::env_registry` (see `docs/reference/env-flags.md`) plus the
//! `--coordinator` flag. `doctor` is a **read-only** reporter: it resolves the
//! deployment mode and the knobs that matter for it, groups them, and flags
//! misconfigurations (e.g. a distributed mode with no coordinator URL) without
//! constructing a live session or touching the network.

use std::collections::BTreeMap;

use crate::cli::{CliResponse, CoordinatorMode};

pub fn run_doctor(args: &[&str], coordinator: &CoordinatorMode) -> CliResponse {
    match args {
        [] | ["--help"] | ["-h"] => {
            let report = render_report(&|k| std::env::var(k).ok(), coordinator);
            CliResponse::ok(report)
        }
        [unknown, ..] => CliResponse::err(
            format!("unknown doctor argument: {unknown}\n\n{}", doctor_help()),
            2,
        ),
    }
}

pub fn doctor_help() -> &'static str {
    "Print the effective Krishiv deployment configuration.\n\
     \n\
     Usage:\n\
       krishiv [-c <URL>] doctor\n\
     \n\
     Resolves KRISHIV_MODE and the environment knobs relevant to the selected\n\
     deployment, groups them, and reports misconfigurations. Read-only: it does\n\
     not start a session or contact a coordinator.\n"
}

/// Resolved deployment mode, normalised from `KRISHIV_MODE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Embedded,
    SingleNode,
    Distributed,
    BareMetal,
    K8s,
    Unknown,
}

impl Mode {
    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "embedded" => Mode::Embedded,
            "single-node" | "single_node" | "singlenode" => Mode::SingleNode,
            "distributed" => Mode::Distributed,
            "bare-metal" | "bare_metal" | "baremetal" => Mode::BareMetal,
            "k8s" | "kubernetes" => Mode::K8s,
            _ => Mode::Unknown,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Mode::Embedded => "embedded",
            Mode::SingleNode => "single-node",
            Mode::Distributed => "distributed",
            Mode::BareMetal => "bare-metal",
            Mode::K8s => "k8s",
            Mode::Unknown => "unknown",
        }
    }

    /// Whether this mode requires a remote coordinator endpoint.
    fn requires_coordinator(self) -> bool {
        matches!(self, Mode::Distributed | Mode::BareMetal | Mode::K8s)
    }
}

/// Render the report. `lookup` resolves an env var (injected for testability).
fn render_report(lookup: &dyn Fn(&str) -> Option<String>, coordinator: &CoordinatorMode) -> String {
    let get = |k: &str| lookup(k).filter(|v| !v.trim().is_empty());

    let mode = Mode::parse(&get("KRISHIV_MODE").unwrap_or_default());
    let coord_url = match coordinator {
        CoordinatorMode::Remote(u) => Some(u.clone()),
        CoordinatorMode::Local => {
            get("KRISHIV_COORDINATOR_URL").or_else(|| get("KRISHIV_COORDINATOR"))
        }
    };

    // Curated, grouped knobs. Order within a group is stable for readable diffs.
    let groups: &[(&str, &[&str])] = &[
        (
            "Topology",
            &[
                "KRISHIV_COORDINATOR_URL",
                "KRISHIV_COORDINATOR_HTTP",
                "KRISHIV_REMOTE_EXEC",
                "KRISHIV_FLIGHT_ADDR",
                "KRISHIV_GRPC_ADDR",
            ],
        ),
        (
            "Durability",
            &[
                "KRISHIV_DURABILITY_PROFILE",
                "KRISHIV_CHECKPOINT_STORAGE",
                "KRISHIV_CHECKPOINT_DIR",
                "KRISHIV_METADATA_BACKEND",
                "KRISHIV_METADATA_PATH",
            ],
        ),
        (
            "Shuffle",
            &[
                "KRISHIV_SHUFFLE_DIR",
                "KRISHIV_SHUFFLE_MEMORY_BYTES",
                "KRISHIV_SHUFFLE_SPILL_THRESHOLD_BYTES",
                "KRISHIV_SHUFFLE_PARTITIONS",
            ],
        ),
        (
            "Resources",
            &[
                "KRISHIV_TARGET_PARALLELISM",
                "KRISHIV_TASK_SLOTS",
                "KRISHIV_EXECUTOR_MEMORY_LIMIT_BYTES",
                "KRISHIV_QUERY_MEMORY_LIMIT_BYTES",
                "KRISHIV_BATCH_SIZE",
            ],
        ),
        (
            "Transport",
            &["KRISHIV_INLINE_IPC_MAX_BYTES", "KRISHIV_ETCD_ENDPOINTS"],
        ),
    ];

    let mut out = String::new();
    out.push_str("Krishiv deployment configuration\n");
    out.push_str("════════════════════════════════\n");
    out.push_str(&format!("Mode             {}\n", mode.label()));
    out.push_str(&format!(
        "Coordinator URL  {}\n",
        coord_url.clone().unwrap_or_else(|| "(none)".into())
    ));
    out.push('\n');

    for (title, keys) in groups {
        out.push_str(&format!("{title}\n"));
        // Collect into a map so we print "(default)" for unset knobs uniformly.
        let mut rows: BTreeMap<&str, String> = BTreeMap::new();
        for &key in *keys {
            rows.insert(key, get(key).unwrap_or_else(|| "(default)".into()));
        }
        for (key, val) in &rows {
            out.push_str(&format!("  {key:<40} {val}\n"));
        }
        out.push('\n');
    }

    // ── All set flags (registry-driven; covers every declared knob) ──────────
    let mut set_rows: Vec<String> = Vec::new();
    for spec in krishiv_common::env_registry::FLAGS {
        if let Some(val) = get(spec.name) {
            let shown = if matches!(spec.kind, krishiv_common::FlagKind::Secret) {
                "(set, redacted)".to_string()
            } else {
                val
            };
            set_rows.push(format!("  {:<40} {}\n", spec.name, shown));
        }
    }
    if !set_rows.is_empty() {
        out.push_str("All set KRISHIV_* flags (from the flag registry)\n");
        for row in &set_rows {
            out.push_str(row);
        }
        out.push('\n');
    }

    // ── Validation ────────────────────────────────────────────────────────────
    let mut warnings: Vec<String> = Vec::new();
    // Registry validation: unknown flags, type mismatches, deprecated aliases.
    for issue in krishiv_common::validate_env() {
        warnings.push(issue.to_string());
    }
    if mode == Mode::Unknown {
        warnings.push(format!(
            "KRISHIV_MODE='{}' is not recognized; valid: embedded, single-node, \
             distributed, bare-metal, k8s",
            get("KRISHIV_MODE").unwrap_or_default()
        ));
    }
    if mode.requires_coordinator() && coord_url.is_none() {
        warnings.push(format!(
            "mode '{}' requires a coordinator endpoint; set KRISHIV_COORDINATOR_URL or pass -c <URL>",
            mode.label()
        ));
    }
    if mode == Mode::Embedded && coord_url.is_some() {
        warnings.push(
            "embedded mode ignores the coordinator URL; set KRISHIV_MODE for remote execution"
                .to_string(),
        );
    }

    if warnings.is_empty() {
        out.push_str("✓ no configuration problems detected\n");
    } else {
        out.push_str("Warnings\n");
        for w in &warnings {
            out.push_str(&format!("  ⚠ {w}\n"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup(pairs: &[(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        let map: std::collections::HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |k: &str| map.get(k).cloned()
    }

    #[test]
    fn embedded_default_is_clean() {
        let report = render_report(&lookup(&[]), &CoordinatorMode::Local);
        assert!(report.contains("Mode             embedded"), "{report}");
        assert!(report.contains("✓ no configuration problems"), "{report}");
    }

    #[test]
    fn distributed_without_coordinator_warns() {
        let report = render_report(
            &lookup(&[("KRISHIV_MODE", "distributed")]),
            &CoordinatorMode::Local,
        );
        assert!(
            report.contains("requires a coordinator endpoint"),
            "{report}"
        );
    }

    #[test]
    fn coordinator_flag_satisfies_distributed() {
        let report = render_report(
            &lookup(&[("KRISHIV_MODE", "distributed")]),
            &CoordinatorMode::Remote("http://coord:2002".into()),
        );
        assert!(report.contains("http://coord:2002"), "{report}");
        assert!(report.contains("✓ no configuration problems"), "{report}");
    }

    #[test]
    fn unknown_mode_and_bad_cap_warn() {
        let report = render_report(
            &lookup(&[
                ("KRISHIV_MODE", "galactic"),
                ("KRISHIV_INLINE_IPC_MAX_BYTES", "huge"),
            ]),
            &CoordinatorMode::Local,
        );
        assert!(report.contains("not recognized"), "{report}");
        assert!(report.contains("not a positive integer"), "{report}");
    }

    #[test]
    fn embedded_with_coordinator_notes_ignored() {
        let report = render_report(
            &lookup(&[]),
            &CoordinatorMode::Remote("http://coord:2002".into()),
        );
        assert!(
            report.contains("embedded mode ignores the coordinator URL"),
            "{report}"
        );
    }
}
