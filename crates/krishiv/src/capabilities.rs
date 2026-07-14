//! Compile-time capability manifest (flag-minimization plan, item 1).
//!
//! A `krishiv` binary's capabilities are decided at build time by Cargo
//! features (e.g. `kafka` pulls in the Kafka connector, `cloud` pulls in the S3
//! object store). Historically this was invisible at runtime: an image built
//! without `kafka`/`cloud` would simply fail the first Kafka or S3 job with no
//! hint that the capability was compiled out. This module makes the built-in
//! capability set an inspectable fact — via `krishiv capabilities` and a
//! one-line startup banner — so operators see gaps by looking, not by failing.
//!
//! The `cfg!(feature = …)` checks are evaluated in the `krishiv` binary crate,
//! which is where the top-level capability features are declared and propagated
//! to the sub-crates, so the manifest reflects what the shipped binary can do.

/// One built-time capability and whether it is compiled into this binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capability {
    /// Cargo feature / capability name (e.g. `"kafka"`).
    pub name: &'static str,
    /// Whether the feature was enabled in this build.
    pub enabled: bool,
    /// One-line description of what the capability unlocks.
    pub description: &'static str,
}

/// All reported capabilities, in a stable display order.
///
/// These are the deployment-relevant optional capabilities — the ones whose
/// absence changes what jobs the binary can run. Pure API-surface features are
/// intentionally omitted; this is an operator-facing manifest, not a full
/// feature dump.
pub fn all() -> Vec<Capability> {
    vec![
        Capability {
            name: "kafka",
            enabled: cfg!(feature = "kafka"),
            description: "Kafka/Redpanda source + sink connector (librdkafka)",
        },
        Capability {
            name: "cloud",
            enabled: cfg!(feature = "cloud"),
            description: "object-store I/O for S3/GCS/Azure (MinIO-compatible)",
        },
        Capability {
            name: "iceberg",
            enabled: cfg!(feature = "iceberg"),
            description: "Apache Iceberg table format (read/write/streaming sink)",
        },
        Capability {
            name: "delta",
            enabled: cfg!(feature = "delta"),
            description: "Delta Lake table format",
        },
        Capability {
            name: "state",
            enabled: cfg!(feature = "state"),
            description: "durable keyed state backend (RocksDB)",
        },
        Capability {
            name: "flight-sql",
            enabled: cfg!(feature = "flight-sql"),
            description: "Arrow Flight SQL server (JDBC/ADBC/BI clients)",
        },
        Capability {
            name: "shuffle",
            enabled: cfg!(feature = "shuffle"),
            description: "distributed shuffle service (partition-parallel stages)",
        },
        Capability {
            name: "etcd",
            enabled: cfg!(feature = "etcd"),
            description: "etcd metadata + leader election (distributed-durable HA)",
        },
        Capability {
            name: "k8s",
            enabled: cfg!(feature = "k8s"),
            description: "Kubernetes operator (CRD reconciler)",
        },
        Capability {
            name: "jemalloc",
            enabled: cfg!(feature = "jemalloc"),
            description: "jemalloc global allocator",
        },
    ]
}

/// One-line capability summary for a startup log line, e.g.
/// `kafka=off cloud=off iceberg=on delta=off … jemalloc=on`.
pub fn summary() -> String {
    all()
        .iter()
        .map(|c| format!("{}={}", c.name, if c.enabled { "on" } else { "off" }))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Multi-line human report for `krishiv capabilities`.
pub fn report() -> String {
    let mut out = String::from("krishiv build capabilities:\n\n");
    for c in all() {
        out.push_str(&format!(
            "  {:<11} {:<3}  {}\n",
            c.name,
            if c.enabled { "ON" } else { "off" },
            c.description,
        ));
    }
    out.push_str(
        "\nCapabilities are compiled in via Cargo features. To change them, rebuild\n\
         with a different preset (e.g. `--features prod` for kafka+cloud+iceberg+\n\
         distributed). A capability shown `off` will fail any job that needs it.\n",
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_capabilities_have_stable_names_and_descriptions() {
        let caps = all();
        assert!(caps.iter().any(|c| c.name == "kafka"));
        assert!(caps.iter().any(|c| c.name == "cloud"));
        assert!(caps.iter().all(|c| !c.name.is_empty() && !c.description.is_empty()));
    }

    #[test]
    fn summary_reports_every_capability_as_on_or_off() {
        let s = summary();
        for c in all() {
            let token = format!("{}={}", c.name, if c.enabled { "on" } else { "off" });
            assert!(s.contains(&token), "summary missing {token}: {s}");
        }
    }

    #[test]
    fn iceberg_tracks_its_cfg() {
        // The manifest must reflect the real build: iceberg's reported state is
        // exactly its cfg. This guards against a hardcoded/stale manifest.
        let iceberg = all().into_iter().find(|c| c.name == "iceberg").unwrap();
        assert_eq!(iceberg.enabled, cfg!(feature = "iceberg"));
    }
}
