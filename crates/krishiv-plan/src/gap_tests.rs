use crate::{ExecutionKind, LogicalPlan, Partitioning, PhysicalPlan, PlanNode};

#[test]
fn set_partitioning_to_unpartitioned() {
    let mut node = PlanNode::new("n1", "label", ExecutionKind::Batch)
        .with_partitioning(Partitioning::Broadcast);
    assert_eq!(node.partitioning(), &Partitioning::Broadcast);
    node.set_partitioning(Partitioning::Unpartitioned);
    assert_eq!(node.partitioning(), &Partitioning::Unpartitioned);
}

#[test]
fn set_partitioning_to_hash() {
    let mut node = PlanNode::new("n1", "label", ExecutionKind::Batch);
    node.set_partitioning(Partitioning::Hash {
        keys: vec!["k1".to_string(), "k2".to_string()],
        buckets: 32,
    });
    assert_eq!(
        node.partitioning(),
        &Partitioning::Hash {
            keys: vec!["k1".to_string(), "k2".to_string()],
            buckets: 32,
        }
    );
}

#[test]
fn set_partitioning_to_round_robin() {
    let mut node = PlanNode::new("n1", "label", ExecutionKind::Batch);
    node.set_partitioning(Partitioning::RoundRobin { buckets: 8 });
    assert_eq!(
        node.partitioning(),
        &Partitioning::RoundRobin { buckets: 8 }
    );
}

#[test]
fn set_partitioning_to_broadcast() {
    let mut node = PlanNode::new("n1", "label", ExecutionKind::Batch);
    node.set_partitioning(Partitioning::Broadcast);
    assert_eq!(node.partitioning(), &Partitioning::Broadcast);
}

#[test]
fn set_partitioning_to_range() {
    let mut node = PlanNode::new("n1", "label", ExecutionKind::Batch);
    node.set_partitioning(Partitioning::Range {
        keys: vec![("col".to_string(), true)],
        boundaries: vec!["\"a\"".to_string(), "\"m\"".to_string()],
        buckets: 3,
    });
    assert_eq!(
        node.partitioning(),
        &Partitioning::Range {
            keys: vec![("col".to_string(), true)],
            boundaries: vec!["\"a\"".to_string(), "\"m\"".to_string()],
            buckets: 3,
        }
    );
}

#[test]
fn set_partitioning_overwrites_previous() {
    let mut node =
        PlanNode::new("n1", "label", ExecutionKind::Batch).with_partitioning(Partitioning::Hash {
            keys: vec!["a".to_string()],
            buckets: 4,
        });
    assert_eq!(
        node.partitioning(),
        &Partitioning::Hash {
            keys: vec!["a".to_string()],
            buckets: 4,
        }
    );
    node.set_partitioning(Partitioning::RoundRobin { buckets: 2 });
    assert_eq!(
        node.partitioning(),
        &Partitioning::RoundRobin { buckets: 2 }
    );
}

#[test]
fn logical_plan_shuffle_partitions_default_none() {
    let plan = LogicalPlan::new("q", ExecutionKind::Batch);
    assert_eq!(plan.shuffle_partitions(), None);
}

#[test]
fn logical_plan_with_shuffle_partitions_sets_value() {
    let plan = LogicalPlan::new("q", ExecutionKind::Batch).with_shuffle_partitions(Some(64));
    assert_eq!(plan.shuffle_partitions(), Some(64));
}

#[test]
fn logical_plan_with_shuffle_partitions_clears_value() {
    let plan = LogicalPlan::new("q", ExecutionKind::Batch)
        .with_shuffle_partitions(Some(64))
        .with_shuffle_partitions(None);
    assert_eq!(plan.shuffle_partitions(), None);
}

#[test]
fn logical_plan_with_shuffle_partitions_updates() {
    let plan = LogicalPlan::new("q", ExecutionKind::Batch)
        .with_shuffle_partitions(Some(16))
        .with_shuffle_partitions(Some(128));
    assert_eq!(plan.shuffle_partitions(), Some(128));
}

#[test]
fn physical_plan_coalesced_partition_count_default_none() {
    let plan = PhysicalPlan::new("p", ExecutionKind::Batch);
    assert_eq!(plan.coalesced_partition_count(), None);
}

#[test]
fn physical_plan_with_coalesced_partition_count_sets_value() {
    let plan = PhysicalPlan::new("p", ExecutionKind::Batch).with_coalesced_partition_count(4);
    assert_eq!(plan.coalesced_partition_count(), Some(4));
}

#[test]
fn physical_plan_with_coalesced_partition_count_overwrites() {
    let plan = PhysicalPlan::new("p", ExecutionKind::Batch)
        .with_coalesced_partition_count(8)
        .with_coalesced_partition_count(2);
    assert_eq!(plan.coalesced_partition_count(), Some(2));
}

#[test]
fn physical_plan_shuffle_partitions_default_none() {
    let plan = PhysicalPlan::new("p", ExecutionKind::Batch);
    assert_eq!(plan.shuffle_partitions(), None);
}

#[test]
fn physical_plan_with_shuffle_partitions_sets_value() {
    let plan = PhysicalPlan::new("p", ExecutionKind::Batch).with_shuffle_partitions(Some(256));
    assert_eq!(plan.shuffle_partitions(), Some(256));
}

#[test]
fn physical_plan_with_shuffle_partitions_clears_value() {
    let plan = PhysicalPlan::new("p", ExecutionKind::Batch)
        .with_shuffle_partitions(Some(256))
        .with_shuffle_partitions(None);
    assert_eq!(plan.shuffle_partitions(), None);
}

#[test]
fn physical_plan_with_shuffle_partitions_updates() {
    let plan = PhysicalPlan::new("p", ExecutionKind::Batch)
        .with_shuffle_partitions(Some(32))
        .with_shuffle_partitions(Some(512));
    assert_eq!(plan.shuffle_partitions(), Some(512));
}

#[test]
fn execution_kind_batch_display() {
    assert_eq!(ExecutionKind::Batch.to_string(), "batch");
}

#[test]
fn execution_kind_streaming_display() {
    assert_eq!(ExecutionKind::Streaming.to_string(), "streaming");
}

#[test]
fn execution_kind_delta_batch_display() {
    assert_eq!(ExecutionKind::DeltaBatch.to_string(), "delta-batch");
}

#[test]
fn partitioning_display_range_single_key_ascending() {
    let p = Partitioning::Range {
        keys: vec![("ts".to_string(), true)],
        boundaries: vec![],
        buckets: 1,
    };
    assert_eq!(p.to_string(), "range(ts ASC, buckets=1)");
}

#[test]
fn partitioning_display_range_single_key_descending() {
    let p = Partitioning::Range {
        keys: vec![("price".to_string(), false)],
        boundaries: vec![],
        buckets: 1,
    };
    assert_eq!(p.to_string(), "range(price DESC, buckets=1)");
}

#[test]
fn partitioning_display_range_multiple_keys() {
    let p = Partitioning::Range {
        keys: vec![("a".to_string(), true), ("b".to_string(), false)],
        boundaries: vec!["\"x\"".to_string()],
        buckets: 2,
    };
    assert_eq!(p.to_string(), "range(a ASC, b DESC, buckets=2)");
}

#[test]
fn partitioning_display_hash_single_key() {
    let p = Partitioning::Hash {
        keys: vec!["id".to_string()],
        buckets: 16,
    };
    assert_eq!(p.to_string(), "hash(id, buckets=16)");
}

#[test]
fn partitioning_display_round_robin() {
    let p = Partitioning::RoundRobin { buckets: 12 };
    assert_eq!(p.to_string(), "round-robin(buckets=12)");
}
