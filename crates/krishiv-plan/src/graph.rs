//! Structural validation and deterministic logical-to-physical lowering.

use std::collections::{HashMap, VecDeque};

use crate::{LogicalPlan, MAX_PLAN_NODES, PhysicalPlan, PlanError, PlanNode};

const PHYSICAL_NODE_PREFIX: &str = "physical:";

pub(crate) fn validate_plan(
    plan_type: &str,
    name: &str,
    nodes: &[PlanNode],
) -> Result<(), PlanError> {
    if name.trim().is_empty() {
        return Err(PlanError::Validation(format!(
            "{plan_type} plan name must not be blank"
        )));
    }
    if nodes.len() > MAX_PLAN_NODES {
        return Err(PlanError::Validation(format!(
            "{plan_type} plan '{name}' has {} nodes, exceeding the limit of {MAX_PLAN_NODES}",
            nodes.len()
        )));
    }

    let mut node_indexes = HashMap::with_capacity(nodes.len());
    for (index, node) in nodes.iter().enumerate() {
        if node.id().trim().is_empty() {
            return Err(PlanError::Validation(format!(
                "{plan_type} plan '{name}' contains a blank node id"
            )));
        }
        if node.id() != node.id().trim() {
            return Err(PlanError::Validation(format!(
                "{plan_type} plan '{name}' node id '{}' has leading or trailing whitespace",
                node.id()
            )));
        }
        if node_indexes.insert(node.id(), index).is_some() {
            return Err(PlanError::Validation(format!(
                "{plan_type} plan '{name}' contains duplicate node id '{}'",
                node.id()
            )));
        }
    }

    let mut indegrees = vec![0usize; nodes.len()];
    let mut dependents = vec![Vec::new(); nodes.len()];
    for (node_index, node) in nodes.iter().enumerate() {
        let mut seen_inputs = std::collections::HashSet::new();
        for input in node.inputs() {
            if !seen_inputs.insert(input.as_str()) {
                return Err(PlanError::Validation(format!(
                    "{plan_type} plan '{name}' node '{}' has duplicate input reference '{input}'",
                    node.id()
                )));
            }
            if input.trim().is_empty() {
                return Err(PlanError::Validation(format!(
                    "{plan_type} plan '{name}' node '{}' contains a blank input reference",
                    node.id()
                )));
            }
            if input != input.trim() {
                return Err(PlanError::Validation(format!(
                    "{plan_type} plan '{name}' node '{}' input reference '{input}' has leading or trailing whitespace",
                    node.id()
                )));
            }
            if input == node.id() {
                return Err(PlanError::Validation(format!(
                    "{plan_type} plan '{name}' node '{}' references itself",
                    node.id()
                )));
            }
            let Some(&input_index) = node_indexes.get(input.as_str()) else {
                return Err(PlanError::Validation(format!(
                    "{plan_type} plan '{name}' node '{}' references missing input '{input}'",
                    node.id()
                )));
            };
            indegrees[node_index] = indegrees[node_index].checked_add(1).ok_or_else(|| {
                PlanError::Validation(format!(
                    "{plan_type} plan '{name}' node '{}' has too many input edges",
                    node.id()
                ))
            })?;
            dependents[input_index].push(node_index);
        }
    }

    let mut ready = indegrees
        .iter()
        .enumerate()
        .filter_map(|(index, indegree)| (*indegree == 0).then_some(index))
        .collect::<VecDeque<_>>();
    let mut visited = 0usize;
    while let Some(index) = ready.pop_front() {
        visited += 1;
        for &dependent in &dependents[index] {
            indegrees[dependent] -= 1;
            if indegrees[dependent] == 0 {
                ready.push_back(dependent);
            }
        }
    }

    if visited != nodes.len() {
        let blocked_nodes = nodes
            .iter()
            .zip(indegrees)
            .filter_map(|(node, indegree)| (indegree > 0).then_some(node.id()))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(PlanError::Validation(format!(
            "{plan_type} plan '{name}' contains a cycle; nodes blocked from topological ordering: {blocked_nodes}"
        )));
    }

    Ok(())
}

/// Validate and lower a logical plan into a physical plan.
///
/// Node identifiers and all input references receive a stable `physical:`
/// prefix. Operator metadata, schemas, estimates, and partitioning annotations
/// are preserved exactly.
pub fn lower_to_physical(logical: &LogicalPlan) -> Result<PhysicalPlan, PlanError> {
    logical.validate()?;

    let mut physical = PhysicalPlan::new(logical.name(), logical.kind());
    for node in logical.nodes() {
        let mut physical_node = PlanNode::new(
            physical_node_id(node.id()),
            format!("physical {}", node.label()),
            node.kind(),
        )
        .with_inputs(node.inputs().iter().map(|input| physical_node_id(input)))
        .with_partitioning(node.partitioning().clone())
        .with_broadcast_eligible(node.broadcast_eligible())
        .with_estimated_rows(node.estimated_rows())
        .with_output_schema(node.output_schema().clone());
        if let Some(op) = node.op() {
            physical_node = physical_node.with_op(op.clone());
        }
        physical.add_node(physical_node);
    }
    physical.validate()?;
    Ok(physical)
}

fn physical_node_id(logical_node_id: &str) -> String {
    format!("{PHYSICAL_NODE_PREFIX}{logical_node_id}")
}

#[cfg(test)]
mod tests {
    use crate::{
        ExecutionKind, FieldType, LogicalPlan, NodeOp, Partitioning, PhysicalPlan, PlanNode,
        PlanSchema, SchemaField,
    };

    use super::lower_to_physical;

    #[test]
    fn lowering_rewrites_edges_and_preserves_annotations() {
        let schema = PlanSchema::new(vec![SchemaField::new("id", FieldType::Int64)]);
        let logical = LogicalPlan::new("orders", ExecutionKind::Batch)
            .with_node(
                PlanNode::new("scan", "scan orders", ExecutionKind::Batch)
                    .with_op(NodeOp::Scan {
                        table: "orders".to_string(),
                        filters: vec!["active = true".to_string()],
                    })
                    .with_partitioning(Partitioning::Hash {
                        keys: vec!["id".to_string()],
                        buckets: 8,
                    })
                    .with_broadcast_eligible(true)
                    .with_estimated_rows(Some(42))
                    .with_output_schema(schema.clone()),
            )
            .with_node(
                PlanNode::new("sink", "collect", ExecutionKind::Batch)
                    .with_inputs(["scan"])
                    .with_op(NodeOp::Sink {
                        format: "memory".to_string(),
                    }),
            );

        let physical = lower_to_physical(&logical).expect("lower");

        assert_eq!(physical.nodes()[0].id(), "physical:scan");
        assert_eq!(physical.nodes()[1].inputs(), &["physical:scan"]);
        assert_eq!(
            physical.nodes()[0].partitioning(),
            logical.nodes()[0].partitioning()
        );
        assert!(physical.nodes()[0].broadcast_eligible());
        assert_eq!(physical.nodes()[0].estimated_rows(), Some(42));
        assert_eq!(physical.nodes()[0].output_schema(), &schema);
        assert_eq!(physical.nodes()[0].op(), logical.nodes()[0].op());
        physical.validate().expect("valid physical graph");
    }

    #[test]
    fn validation_rejects_duplicate_node_ids() {
        let plan = LogicalPlan::new("duplicate", ExecutionKind::Batch)
            .with_node(PlanNode::new("scan", "first", ExecutionKind::Batch))
            .with_node(PlanNode::new("scan", "second", ExecutionKind::Batch));

        let error = plan.validate().expect_err("duplicate ids must fail");
        assert!(error.to_string().contains("duplicate node id 'scan'"));
    }

    #[test]
    fn validation_rejects_missing_input() {
        let plan = PhysicalPlan::new("missing", ExecutionKind::Batch).with_node(
            PlanNode::new("sink", "sink", ExecutionKind::Batch).with_inputs(["unknown"]),
        );

        let error = plan.validate().expect_err("missing input must fail");
        assert!(error.to_string().contains("missing input 'unknown'"));
    }

    #[test]
    fn validation_rejects_self_reference() {
        let plan = LogicalPlan::new("self", ExecutionKind::Batch)
            .with_node(PlanNode::new("loop", "loop", ExecutionKind::Batch).with_inputs(["loop"]));

        let error = plan.validate().expect_err("self reference must fail");
        assert!(error.to_string().contains("references itself"));
    }

    #[test]
    fn validation_rejects_multi_node_cycle() {
        let plan = LogicalPlan::new("cycle", ExecutionKind::Batch)
            .with_node(PlanNode::new("left", "left", ExecutionKind::Batch).with_inputs(["right"]))
            .with_node(PlanNode::new("right", "right", ExecutionKind::Batch).with_inputs(["left"]));

        let error = plan.validate().expect_err("cycle must fail");
        assert!(error.to_string().contains("contains a cycle"));
        assert!(error.to_string().contains("left"));
        assert!(error.to_string().contains("right"));
    }

    #[test]
    fn validation_accepts_forward_references_in_an_acyclic_graph() {
        let plan = LogicalPlan::new("forward", ExecutionKind::Batch)
            .with_node(PlanNode::new("sink", "sink", ExecutionKind::Batch).with_inputs(["source"]))
            .with_node(PlanNode::new("source", "source", ExecutionKind::Batch));

        plan.validate().expect("forward references are valid");
    }

    #[test]
    fn lowering_rejects_blank_plan_name_and_node_id() {
        let blank_name = LogicalPlan::new(" ", ExecutionKind::Batch);
        assert!(
            lower_to_physical(&blank_name)
                .expect_err("blank name must fail")
                .to_string()
                .contains("plan name must not be blank")
        );

        let blank_node = LogicalPlan::new("named", ExecutionKind::Batch).with_node(PlanNode::new(
            " ",
            "blank id",
            ExecutionKind::Batch,
        ));
        assert!(
            lower_to_physical(&blank_node)
                .expect_err("blank node id must fail")
                .to_string()
                .contains("blank node id")
        );
    }
}
