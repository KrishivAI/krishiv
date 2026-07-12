//! Cost-based optimization (CBO) infrastructure: stats registry and
//! NDV-aware cost model.
//!
//! See [`TableStatsRegistry`], [`TableCboStats`], and
//! [`CboCostModel`] for the user-facing types. The cost model is
//! plug-compatible with the static one in [`super::StaticCostModel`].

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::{LogicalPlan, NodeOp};

use super::CostModel;

/// Per-column statistics collected by `ANALYZE TABLE … FOR COLUMNS`.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct ColumnCboStats {
    pub name: String,
    /// Approximate number of distinct values (HLL-based).
    pub ndv: Option<u64>,
    /// Rendered minimum value (display form; type-erased on purpose).
    pub min: Option<String>,
    /// Rendered maximum value.
    pub max: Option<String>,
    pub null_count: Option<u64>,
}

/// Catalog statistics the cost model needs for one table.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct TableCboStats {
    pub table: String,
    pub row_count: Option<u64>,
    pub ndv: Option<u64>,
    pub avg_row_bytes: Option<u64>,
    /// Column-level stats (empty unless ANALYZE ran with FOR COLUMNS).
    #[serde(default)]
    pub columns: Vec<ColumnCboStats>,
}

impl TableCboStats {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            ..Default::default()
        }
    }

    #[must_use]
    pub fn with_row_count(mut self, n: u64) -> Self {
        self.row_count = Some(n);
        self
    }

    #[must_use]
    pub fn with_ndv(mut self, n: u64) -> Self {
        self.ndv = Some(n);
        self
    }

    #[must_use]
    pub fn with_avg_row_bytes(mut self, n: u64) -> Self {
        self.avg_row_bytes = Some(n);
        self
    }
}

#[derive(Debug, Default, Clone)]
pub struct TableStatsRegistry {
    inner: Arc<RwLock<HashMap<String, TableCboStats>>>,
}

/// Process-global table statistics registry (Phase 54).
///
/// Written by the SQL layer (`ANALYZE TABLE`, Iceberg CTAS/DML auto-stats)
/// and read by cost-based consumers — [`CboCostModel`] behind
/// `default_aqe_optimizer_with_stats` on the coordinator, and any
/// planning-time rule that wants row counts beyond the per-engine
/// row-count registry. Clones share the same underlying map.
pub fn global_table_stats() -> &'static TableStatsRegistry {
    static GLOBAL: std::sync::OnceLock<TableStatsRegistry> = std::sync::OnceLock::new();
    GLOBAL.get_or_init(TableStatsRegistry::new)
}

impl TableStatsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&self, stats: TableCboStats) -> Option<TableCboStats> {
        match self.inner.write() {
            Ok(mut g) => g.insert(stats.table.clone(), stats),
            Err(p) => p.into_inner().insert(stats.table.clone(), stats),
        }
    }

    pub fn get(&self, table: &str) -> Option<TableCboStats> {
        match self.inner.read() {
            Ok(g) => g.get(table).cloned(),
            Err(p) => p.into_inner().get(table).cloned(),
        }
    }

    pub fn remove(&self, table: &str) -> Option<TableCboStats> {
        match self.inner.write() {
            Ok(mut g) => g.remove(table),
            Err(p) => p.into_inner().remove(table),
        }
    }

    pub fn len(&self) -> usize {
        match self.inner.read() {
            Ok(g) => g.len(),
            Err(p) => p.into_inner().len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn entries(&self) -> Vec<(String, TableCboStats)> {
        match self.inner.read() {
            Ok(g) => g.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            Err(p) => p
                .into_inner()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }
}

/// NDV-aware cost model.
pub struct CboCostModel {
    pub registry: TableStatsRegistry,
}

impl std::fmt::Debug for CboCostModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CboCostModel")
            .field("registry_entries", &self.registry.len())
            .finish()
    }
}

impl Default for CboCostModel {
    fn default() -> Self {
        Self {
            registry: TableStatsRegistry::new(),
        }
    }
}

impl CostModel for CboCostModel {
    fn estimate(&self, plan: &LogicalPlan) -> super::Cost {
        const DEFAULT_ROWS: u64 = 10_000;
        let mut cpu_nanos: u64 = 0;
        let mut memory_bytes: u64 = 0;
        let mut network_bytes: u64 = 0;

        for node in plan.nodes() {
            let (rows, row_bytes) = match node.op() {
                Some(NodeOp::Scan { table, .. }) => match self.registry.get(table) {
                    Some(stats) => (
                        stats.row_count.unwrap_or(DEFAULT_ROWS),
                        stats.avg_row_bytes.unwrap_or(64),
                    ),
                    None => (node.estimated_rows().unwrap_or(DEFAULT_ROWS), 64),
                },
                _ => (node.estimated_rows().unwrap_or(DEFAULT_ROWS), 64),
            };

            match node.op() {
                Some(NodeOp::Scan { .. }) => {
                    cpu_nanos = cpu_nanos.saturating_add(rows.saturating_mul(10));
                    memory_bytes = memory_bytes.saturating_add(rows.saturating_mul(row_bytes));
                }
                Some(NodeOp::Filter { .. }) => {
                    cpu_nanos = cpu_nanos.saturating_add(rows.saturating_mul(5));
                }
                Some(NodeOp::Project { .. }) => {
                    cpu_nanos = cpu_nanos.saturating_add(rows.saturating_mul(2));
                }
                Some(NodeOp::Aggregate { group_keys, .. }) => {
                    let ndv_proxy = group_keys.len() as u64;
                    cpu_nanos = cpu_nanos
                        .saturating_add(rows.saturating_mul(50))
                        .saturating_add(ndv_proxy.saturating_mul(20));
                    memory_bytes = memory_bytes.saturating_add(rows.saturating_mul(200));
                }
                Some(NodeOp::Join { .. }) | Some(NodeOp::SortMergeJoin { .. }) => {
                    let ndv_cost = rows.saturating_mul(100);
                    cpu_nanos = cpu_nanos.saturating_add(ndv_cost);
                    memory_bytes = memory_bytes.saturating_add(rows.saturating_mul(100));
                }
                Some(NodeOp::Exchange { .. }) => {
                    cpu_nanos = cpu_nanos.saturating_add(rows.saturating_mul(20));
                    network_bytes = network_bytes.saturating_add(rows.saturating_mul(200));
                }
                _ => {
                    cpu_nanos = cpu_nanos.saturating_add(rows.saturating_mul(15));
                    memory_bytes = memory_bytes.saturating_add(rows.saturating_mul(64));
                }
            }
        }

        super::Cost {
            cpu_nanos,
            memory_bytes,
            network_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExecutionKind, PlanNode};

    #[test]
    fn empty_registry_estimate_is_zero() {
        let model = CboCostModel::default();
        let plan = LogicalPlan::new("p", ExecutionKind::Batch);
        let cost = model.estimate(&plan);
        assert_eq!(cost.cpu_nanos, 0);
        assert_eq!(cost.memory_bytes, 0);
        assert_eq!(cost.network_bytes, 0);
    }

    #[test]
    fn registry_round_trip() {
        let reg = TableStatsRegistry::new();
        reg.put(
            TableCboStats::new("orders")
                .with_row_count(1_000_000)
                .with_ndv(1_000_000)
                .with_avg_row_bytes(128),
        );
        let got = reg.get("orders").expect("present");
        assert_eq!(got.row_count, Some(1_000_000));
        assert_eq!(got.ndv, Some(1_000_000));
        assert_eq!(got.avg_row_bytes, Some(128));
        assert_eq!(reg.get("unknown"), None);
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn registry_remove_returns_previous_value() {
        let reg = TableStatsRegistry::new();
        reg.put(TableCboStats::new("t1").with_row_count(100));
        let prev = reg.remove("t1").expect("present");
        assert_eq!(prev.row_count, Some(100));
        assert!(reg.is_empty());
    }

    #[test]
    fn cbo_cost_model_scan_charges_avg_row_bytes() {
        let model = CboCostModel {
            registry: {
                let reg = TableStatsRegistry::new();
                reg.put(
                    TableCboStats::new("lineitem")
                        .with_row_count(6_000_000)
                        .with_avg_row_bytes(256),
                );
                reg
            },
        };
        let mut plan = LogicalPlan::new("q", ExecutionKind::Batch);
        plan.add_node(
            PlanNode::new("scan", "scan lineitem", ExecutionKind::Batch).with_op(NodeOp::Scan {
                table: "lineitem".to_string(),
                filters: vec![],
            }),
        );
        let cost = model.estimate(&plan);
        // Scan: 6_000_000 * 10 = 60M CPU ns; 6_000_000 * 256 = 1.5 GB.
        assert_eq!(cost.cpu_nanos, 60_000_000);
        assert_eq!(cost.memory_bytes, 1_536_000_000);
    }

    #[test]
    fn cbo_cost_model_aggregate_charges_group_key_proxy() {
        let model = CboCostModel::default();
        let mut plan = LogicalPlan::new("q", ExecutionKind::Batch);
        plan.add_node(
            PlanNode::new("agg", "aggregate", ExecutionKind::Batch).with_op(NodeOp::Aggregate {
                group_keys: vec!["region".to_string(), "year".to_string()],
            }),
        );
        let cost = model.estimate(&plan);
        // rows = 10_000 default; CPU = 10_000 * 50 + 2 * 20 = 500_040
        assert_eq!(cost.cpu_nanos, 500_040);
    }
}
