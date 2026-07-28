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
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::joins::{CrossJoinExec, NestedLoopJoinExec};
use datafusion::physical_plan::unnest::UnnestExec;

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
        // The module docs have always named unnest as a member of this class
        // and it was never actually matched: one row in, one row per list
        // element out, with no leaf of its own between it and the consumer.
        || any.downcast_ref::<UnnestExec>().is_some()
}

/// Is this node already a [`CooperativeExec`]?
fn is_cooperative(plan: &dyn ExecutionPlan) -> bool {
    (plan as &dyn std::any::Any)
        .downcast_ref::<CooperativeExec>()
        .is_some()
}

impl PhysicalOptimizerRule for CooperativeAmplifiers {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            if is_amplifier(node.as_ref()) {
                return Ok(Transformed::yes(
                    Arc::new(CooperativeExec::new(node)) as Arc<dyn ExecutionPlan>
                ));
            }
            // Collapse a doubled wrapper, so applying the rule to a plan it has
            // already run on is a no-op. `transform_up` visits children first:
            // an amplifier that was *already* wrapped gets a second wrapper
            // when we reach it, and the pre-existing one is then visited with
            // that as its child. Without this the plan would gain a layer per
            // pass, and each layer costs a poll indirection on every batch.
            if is_cooperative(node.as_ref())
                && node
                    .children()
                    .first()
                    .is_some_and(|child| is_cooperative(child.as_ref()))
                && let Some(child) = node.children().first()
            {
                return Ok(Transformed::yes(Arc::clone(child)));
            }
            Ok(Transformed::no(node))
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::displayable;

    fn leaf() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        MemorySourceConfig::try_new_exec(&[vec![]], schema, None).unwrap()
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        CooperativeAmplifiers::new()
            .optimize(plan, &ConfigOptions::default())
            .unwrap()
    }

    fn rendered(plan: &Arc<dyn ExecutionPlan>) -> String {
        format!("{}", displayable(plan.as_ref()).indent(false))
    }

    /// The operator class the rule exists for must actually be wrapped.
    #[test]
    fn a_cross_join_is_wrapped() {
        let join = Arc::new(CrossJoinExec::new(leaf(), leaf())) as Arc<dyn ExecutionPlan>;
        assert_eq!(rendered(&join).matches("Cooperative").count(), 0);
        let out = optimize(join);
        assert_eq!(
            rendered(&out).matches("Cooperative").count(),
            1,
            "cross join not wrapped:\n{}",
            rendered(&out)
        );
    }

    /// Running the rule on a plan it has already run on must change nothing.
    ///
    /// Without the collapse, `transform_up` adds a wrapper every pass: it
    /// visits the amplifier before the wrapper already above it, so the old
    /// wrapper simply ends up on top of the new one. Each layer is a poll
    /// indirection on every batch for the rest of the query.
    #[test]
    fn optimizing_twice_is_a_no_op() {
        let join = Arc::new(CrossJoinExec::new(leaf(), leaf())) as Arc<dyn ExecutionPlan>;
        let once = optimize(join);
        let twice = optimize(Arc::clone(&once));
        assert_eq!(
            rendered(&once),
            rendered(&twice),
            "the rule is not idempotent; a second pass added a layer"
        );
        assert_eq!(
            rendered(&twice).matches("Cooperative").count(),
            1,
            "expected exactly one wrapper:\n{}",
            rendered(&twice)
        );
    }

    /// A plan with nothing to amplify must come back untouched — the rule
    /// costs a poll indirection, so it should only be paid where it buys
    /// preemptibility.
    #[test]
    fn a_plan_without_an_amplifier_is_left_alone() {
        let plan = leaf();
        let out = optimize(Arc::clone(&plan));
        assert_eq!(rendered(&plan), rendered(&out));
        assert_eq!(rendered(&out).matches("Cooperative").count(), 0);
    }
}
