//! Phase 62 certified capability matrix.
//!
//! The GA gate's central artifact: a machine-readable, honest-by-construction
//! enumeration of what the engine actually certifies. Two dimensions the
//! project's launch claim rests on —
//!
//! 1. **Compute × topology** — {batch SQL, parallel streaming, IVM} ×
//!    {single-node, distributed}, each cell a certification status with the
//!    fault-loop / benchmark evidence linked.
//! 2. **Data-movement paths** — the end-to-end source→sink combinations, each
//!    with its strongest delivery guarantee and certification status.
//!
//! The invariant enforced by the tests below is the whole point: **no cell may
//! claim `Certified` without a non-empty evidence reference.** A partial launch
//! stays honest by construction — you cut cells (downgrade to Preview), never
//! the gate. `render_markdown` publishes the same data to
//! `docs/reference/certification-matrix.md`, regenerated with
//! `KRISHIV_BLESS_CERT_MATRIX=1 cargo test -p krishiv-connectors cert_matrix`.

use crate::capabilities::{ConnectorMaturity, DeliveryGuarantee};

/// A compute capability the engine offers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compute {
    /// Bounded / batch SQL query execution.
    BatchSql,
    /// Continuous windowed streaming (the `stream:rloop:` run loop).
    ParallelStreaming,
    /// Incremental view maintenance (DeltaBatch).
    Ivm,
}

impl Compute {
    /// Stable label for docs and status APIs.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BatchSql => "batch SQL",
            Self::ParallelStreaming => "parallel streaming",
            Self::Ivm => "IVM",
        }
    }
}

/// Deployment topology a capability is certified in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Topology {
    /// One process (embedded or single coordinator+executor).
    SingleNode,
    /// Coordinator with one or more separate executors.
    Distributed,
}

impl Topology {
    /// Stable label for docs and status APIs.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SingleNode => "single-node",
            Self::Distributed => "distributed",
        }
    }
}

/// One cell of the compute × topology certification matrix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityCell {
    pub compute: Compute,
    pub topology: Topology,
    pub status: ConnectorMaturity,
    /// Linked evidence (benchmark path, cert commit, chaos test name). Required
    /// non-empty for any `Certified` cell — enforced by test.
    pub evidence: &'static str,
    /// Honest caveat for a downgraded cell (why it is not Certified yet).
    pub notes: &'static str,
}

/// One certified end-to-end data-movement path (source → sink).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataPathCell {
    pub source: &'static str,
    pub sink: &'static str,
    /// Strongest delivery guarantee the path can provide (capability, not an
    /// unconditional claim — the effective guarantee is the weakest link).
    pub delivery: DeliveryGuarantee,
    pub status: ConnectorMaturity,
    pub evidence: &'static str,
}

/// The compute × topology certification matrix.
///
/// Statuses are grounded in committed evidence, not aspiration. The single
/// deliberate `Preview` (distributed IVM) is the honest call: its mid-tick
/// cancel is non-cancellable by design (#224) and it carries lighter chaos
/// coverage than the distributed batch/streaming paths.
pub fn capability_matrix() -> Vec<CapabilityCell> {
    use Compute::{BatchSql, Ivm, ParallelStreaming};
    use ConnectorMaturity::{Certified, Preview};
    use Topology::{Distributed, SingleNode};
    vec![
        CapabilityCell {
            compute: BatchSql,
            topology: SingleNode,
            status: Certified,
            evidence: "krishiv-conformance corpus + docs/reference/sql-grammar.md coverage number; \
                       Phase 51 TPC-H yardstick in docs/BENCHMARKING.md",
            notes: "",
        },
        CapabilityCell {
            compute: BatchSql,
            topology: Distributed,
            status: Certified,
            evidence: "krishiv-scheduler placement/failover/recovery chaos suite \
                       (sections/*.inc); live coordinator→executor dispatch proven on the \
                       3-node k3s cert cluster 2026-07-22 (job batch-sql-*, task Succeeded on v2-exec-a)",
            notes: "",
        },
        CapabilityCell {
            compute: ParallelStreaming,
            topology: SingleNode,
            status: Certified,
            evidence: "benchmarks/results.jsonl streaming_latency_{embedded,single_node}_p50 \
                       (both inside budget); run_loop_v2 tumbling/session/cancel tests",
            notes: "",
        },
        CapabilityCell {
            compute: ParallelStreaming,
            topology: Distributed,
            status: Certified,
            evidence: "run_loop_parallel_three_matches_parallel_one (Phase 55 exit gate: keyed \
                       exchange, parallelism-3 == parallelism-1); stream_exchange keyed-shuffle \
                       tests; Kafka→Iceberg exactly-once (G8)",
            notes: "",
        },
        CapabilityCell {
            compute: Ivm,
            topology: SingleNode,
            status: Certified,
            evidence: "benchmarks/results.jsonl ivm_tick_p50_at_10m_rows (64.6ms vs 2000ms budget); \
                       ivm_vs_full_recompute bench; krishiv-ivm flow + partitioned tests; live IVM \
                       job proven on the k3s cert cluster 2026-07-22",
            notes: "Certified for correctness and throughput, NOT for restart-resume. \
                    IVM-AUD-INT-F13: `IncrementalEngine::run` never touches the runtime's \
                    checkpoint service, so at single-node placement an incremental job restarts \
                    from zero and re-emits its whole changelog, exactly as it does embedded — \
                    the durable-checkpoint property that distinguishes single-node from \
                    embedded for the streaming engine does not exist for this one, and none of \
                    the evidence above restarts a job. Whether this row should be downgraded \
                    is an open decision recorded in docs/implementation/ivm-audit-register.md.",
        },
        CapabilityCell {
            compute: Ivm,
            topology: Distributed,
            status: Preview,
            evidence: "Phase 57 resident-IVM dispatch (submit_resident_ivm_step, O(Δ) wire); \
                       ivm_http dispatch-decision tests",
            notes: "Preview until a distributed-IVM chaos gate lands: an in-flight IVM tick is \
                    non-cancellable by design (#224, already-accepted deltas) and distributed \
                    executor-loss during a resident tick has lighter fault coverage than the \
                    batch/streaming paths. Also read this row narrowly: executor-resident IVM \
                    requires an UNPARTITIONED job, and a job whose first view is a routable \
                    single-column GROUP BY is auto-partitioned on any coordinator with more \
                    than one shard (IvmJobRegistry::register_view), while a partitioned job \
                    always computes centrally. The canonical GROUP BY workload therefore runs \
                    on the coordinator's in-process shards and never reaches an executor; only \
                    jobs pinned single (Session::view / to_incremental view-DAGs) or with a \
                    non-shardable first view take the distributed path at all \
                    (IVM-AUD-DIST-A1).",
        },
    ]
}

/// The certified end-to-end data-movement paths.
pub fn data_path_matrix() -> Vec<DataPathCell> {
    use ConnectorMaturity::{Certified, Preview};
    use DeliveryGuarantee::{AtLeastOnce, BestEffort, EffectivelyOnce, ExactlyOnce};
    vec![
        DataPathCell {
            source: "CDC / connector source (krishiv ivm run, submit of an Incremental job)",
            sink: "connector sink (per-tick consolidated changelog)",
            delivery: BestEffort,
            status: Preview,
            evidence: "IVM-AUD-INT-F4 / INT-F13. Listed to state the guarantee, not to claim \
                       one: `IncrementalEngine::run` (krishiv-engines/src/lib.rs) takes no \
                       checkpoints and rewinds no source, so a restart re-runs the job from \
                       zero and rewrites the whole changelog, and a crash mid-drain leaves \
                       whatever the sink had already flushed. `CompiledJob::delivery` is never \
                       read by any engine, so requesting a stronger contract has no effect",
        },
        DataPathCell {
            source: "IVM view output (coordinator /views/{view}/snap and /output)",
            sink: "pull-only — the caller polls; there is no sink",
            delivery: BestEffort,
            status: Preview,
            evidence: "IVM-AUD-INT-F4 / INT-F5. `/output` peeks a coalescing watch that holds \
                       one delta per view, so a consumer polling slower than /step loses every \
                       delta but the newest; since INT-F5 the loss is measurable from \
                       `published_rows_total` but it is not recoverable. `/snap` is a whole \
                       snapshot and loses nothing, at O(view) per poll",
        },
        DataPathCell {
            source: "Kafka",
            sink: "Iceberg",
            delivery: ExactlyOnce,
            status: Certified,
            evidence: "G8 kill-loop certified on prod 2026-07-10 (image g8-9dd1fdf); \
                       DUR-2 recover-commit suite (append+upsert across executor crash, idempotent)",
        },
        DataPathCell {
            source: "batch SQL / object-store files",
            sink: "object-store Parquet (staged, atomic publish)",
            delivery: EffectivelyOnce,
            status: Preview,
            evidence: "DUR-1 Committing-state demote/redrive (staged publish is idempotent, \
                       coordinator/mod.rs); sections/dur1.rs.inc regression tests",
        },
        DataPathCell {
            source: "batch SQL",
            sink: "Iceberg (durable CTAS)",
            delivery: EffectivelyOnce,
            status: Preview,
            evidence: "durable CTAS (#162); overwrite_commit atomic version-hint flip \
                       (temp+fsync+rename, CONN-3); connectors iceberg suite",
        },
        DataPathCell {
            source: "Kafka",
            sink: "Kafka (transactional)",
            delivery: ExactlyOnce,
            status: Preview,
            evidence: "two-phase transactional Kafka sink (transactional_kafka); \
                       barrier-aligned prepare/commit — Preview: no prod kill-loop cert yet",
        },
        DataPathCell {
            source: "batch SQL",
            sink: "any registered connector sink (registry-sink batch export)",
            delivery: AtLeastOnce,
            status: Preview,
            evidence: "#197 registry-sink output contract \
                       (executor fragment::batch::execute_registry_sink) — writes then \
                       flushes before reporting task success, so output is durable on \
                       success but a retried attempt re-delivers. Preview: reaches every \
                       registered sink driver (see the connector reachability matrix) but \
                       has no barrier-aligned commit, so it is not a checkpointed \
                       participant",
        },
    ]
}

/// Render both matrices as the committed reference document.
pub fn render_markdown() -> String {
    let mut out = String::new();
    out.push_str("# Engine certification matrix (Phase 62 GA gate)\n\n");
    out.push_str(
        "Generated from `krishiv-connectors::cert_matrix` — do not edit by hand.\n\
         Regenerate with:\n\
         `KRISHIV_BLESS_CERT_MATRIX=1 cargo test -p krishiv-connectors cert_matrix`\n\n\
         Status is grounded in committed evidence: **no cell claims `certified`\n\
         without a linked benchmark / chaos test / live cert.** A partial launch\n\
         stays honest by construction — cells are cut (downgraded to `preview`),\n\
         never the gate.\n\n",
    );

    out.push_str("## Compute × topology\n\n");
    out.push_str("| Compute | Topology | Status | Evidence | Notes |\n");
    out.push_str("|---|---|---|---|---|\n");
    for cell in capability_matrix() {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} |\n",
            cell.compute.as_str(),
            cell.topology.as_str(),
            cell.status,
            cell.evidence,
            cell.notes,
        ));
    }

    out.push_str("\n## Data-movement paths\n\n");
    out.push_str("| Source | Sink | Delivery | Status | Evidence |\n");
    out.push_str("|---|---|---|---|---|\n");
    for cell in data_path_matrix() {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} |\n",
            cell.source, cell.sink, cell.delivery, cell.status, cell.evidence,
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The honesty invariant of the whole gate: any cell that claims Certified
    /// MUST carry a non-empty evidence reference. This is what stops a future
    /// change from marking a cell certified on assertion.
    #[test]
    fn every_certified_cell_has_linked_evidence() {
        for cell in capability_matrix() {
            if cell.status == ConnectorMaturity::Certified {
                assert!(
                    !cell.evidence.trim().is_empty(),
                    "certified capability cell {}×{} has no evidence",
                    cell.compute.as_str(),
                    cell.topology.as_str()
                );
            }
        }
        for cell in data_path_matrix() {
            if cell.status == ConnectorMaturity::Certified {
                assert!(
                    !cell.evidence.trim().is_empty(),
                    "certified data path {}→{} has no evidence",
                    cell.source,
                    cell.sink
                );
            }
        }
    }

    /// A downgraded (non-Certified) cell must explain itself — either an
    /// evidence pointer to what IS proven or a note on what is missing — so a
    /// Preview is never a silent unknown.
    #[test]
    fn downgraded_cells_are_explained() {
        for cell in capability_matrix() {
            if cell.status != ConnectorMaturity::Certified {
                assert!(
                    !cell.evidence.trim().is_empty() || !cell.notes.trim().is_empty(),
                    "non-certified cell {}×{} explains neither what is proven nor what is missing",
                    cell.compute.as_str(),
                    cell.topology.as_str()
                );
            }
        }
    }

    /// Every compute×topology combination is present exactly once — the matrix
    /// can never silently drop a cell (which would read as "not offered" rather
    /// than an explicit status).
    #[test]
    fn matrix_is_complete_and_unique() {
        let computes = [Compute::BatchSql, Compute::ParallelStreaming, Compute::Ivm];
        let topologies = [Topology::SingleNode, Topology::Distributed];
        let cells = capability_matrix();
        assert_eq!(cells.len(), computes.len() * topologies.len());
        for compute in computes {
            for topology in topologies {
                let matches = cells
                    .iter()
                    .filter(|c| c.compute == compute && c.topology == topology)
                    .count();
                assert_eq!(
                    matches,
                    1,
                    "{}×{} must appear exactly once",
                    compute.as_str(),
                    topology.as_str()
                );
            }
        }
    }

    /// The committed reference doc must match the generated matrix. Regenerate
    /// with `KRISHIV_BLESS_CERT_MATRIX=1`.
    #[test]
    fn committed_matrix_doc_matches() {
        let doc_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root")
            .join("docs/reference/certification-matrix.md");
        let generated = render_markdown();
        if std::env::var("KRISHIV_BLESS_CERT_MATRIX").is_ok() {
            std::fs::create_dir_all(doc_path.parent().unwrap()).unwrap();
            std::fs::write(&doc_path, &generated).unwrap();
            return;
        }
        let committed = std::fs::read_to_string(&doc_path).unwrap_or_default();
        assert_eq!(
            committed, generated,
            "docs/reference/certification-matrix.md is out of date; regenerate with \
             KRISHIV_BLESS_CERT_MATRIX=1 cargo test -p krishiv-connectors cert_matrix"
        );
    }
}
