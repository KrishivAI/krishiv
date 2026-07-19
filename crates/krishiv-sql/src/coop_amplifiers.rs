//! Cooperative yielding for input-amplifying operators (#217).
//!
//! DataFusion's `EnsureCooperative` instruments LEAF streams only: budget
//! is consumed per batch a leaf produces. An operator that amplifies its
//! input — a cross or nested-loop join whose output is orders of magnitude
//! larger than its input, or an unnest — drains its tiny budget-aware
//! inputs in microseconds and then computes budget-free: a 5-way cross
//! join over five 100-row VALUES tables feeds an aggregate 10^10 rows
//! while consuming 5 units of budget, so its poll never yields and no
//! timeout, cancel watcher, or select! arm can ever run (measured: a 2 s
//! `tokio::time::timeout` armed around it did not fire in 7+ minutes).
//!
//! The fix is one wrapper: put a [`CooperativeExec`] on top of each
//! amplifier so budget is also consumed per OUTPUT batch. The stream then
//! returns `Pending` every ~128 batches (~1M rows), which is what makes
//! the executor's cancel watcher and every timeout real for this operator
//! class. `datafusion-proto` round-trips `CooperativeExec`, so distributed
//! fragment encoding is unaffected.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::joins::{CrossJoinExec, NestedLoopJoinExec};
use datafusion::physical_optimizer::PhysicalOptimizerRule;

/// Wraps input-amplifying operators in [`CooperativeExec`] so their output
/// participates in cooperative scheduling. See the module docs for why the
/// default leaf-only instrumentation is not enough.
#[derive(Debug, Default)]
pub struct CooperativeAmplifiers {}

impl CooperativeAmplifiers {
    pub fn new() -> Self {
        Self {}
    }
}

fn is_amplifier(plan: &dyn ExecutionPlan) -> bool {
    // `ExecutionPlan: Any` — upcast to downcast (DF 54 has no `as_any`).
    let any = plan as &dyn std::any::Any;
    any.downcast_ref::<CrossJoinExec>().is_some()
        || any.downcast_ref::<NestedLoopJoinExec>().is_some()
}

impl PhysicalOptimizerRule for CooperativeAmplifiers {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            if is_amplifier(node.as_ref()) {
                Ok(Transformed::yes(
                    Arc::new(CooperativeExec::new(node)) as Arc<dyn ExecutionPlan>
                ))
            } else {
                Ok(Transformed::no(node))
            }
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "CooperativeAmplifiers"
    }

    fn schema_check(&self) -> bool {
        // A CooperativeExec wrapper is schema-transparent.
        true
    }
}
