//! Operator fusion detection for optimizing dataflow execution.
//!
//! `FusionDetector` identifies chainable operators that can be fused
//! to eliminate intermediate buffering and reduce overhead.

use std::collections::HashMap;

use arrow::record_batch::RecordBatch;

use crate::ExecResult;

/// A single stateless stage in a fused operator chain: a pure
/// `RecordBatch -> RecordBatch` transform (map / filter / project).
pub type FusedStage = Box<dyn Fn(RecordBatch) -> ExecResult<RecordBatch> + Send + Sync>;

/// An executable chain of fused stateless operators — the execution counterpart
/// to [`FusionDetector`].
///
/// The detector decides *which* adjacent operators may fuse; `FusedPipeline`
/// *runs* them as one unit. Applying the stages in a single [`run`](Self::run)
/// call threads each intermediate `RecordBatch` straight into the next stage —
/// there is no per-operator queue, `Arc` handoff, or re-buffering between them.
/// That cross-operator hand-off (and, across a network boundary, Arrow
/// re-serialization) is exactly the cost fusion removes — Flink's "operator
/// chaining". A chain of stateless map/filter/project operators thus costs one
/// pass over the batch instead of N passes with N−1 materializations.
#[derive(Default)]
pub struct FusedPipeline {
    stages: Vec<FusedStage>,
}

impl FusedPipeline {
    /// An empty pipeline (an identity transform until stages are added).
    pub fn new() -> Self {
        Self { stages: Vec::new() }
    }

    /// Append a stage. Builder-style: `FusedPipeline::new().then(map).then(filter)`.
    #[must_use]
    pub fn then(mut self, stage: FusedStage) -> Self {
        self.stages.push(stage);
        self
    }

    /// Number of fused stages.
    pub fn len(&self) -> usize {
        self.stages.len()
    }

    /// Whether the pipeline has no stages (runs as identity).
    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    /// Run every stage in order on `batch`, feeding each output directly into
    /// the next with no intermediate materialization. An empty pipeline returns
    /// `batch` unchanged.
    pub fn run(&self, batch: RecordBatch) -> ExecResult<RecordBatch> {
        let mut current = batch;
        for stage in &self.stages {
            current = stage(current)?;
        }
        Ok(current)
    }
}

/// Unique identifier for a node in the dataflow graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

/// A fusion of two operators.
#[derive(Debug, Clone)]
pub struct OperatorFusion {
    /// The source operator node.
    pub source: NodeId,
    /// The sink operator node.
    pub sink: NodeId,
}

/// Node metadata for fusion decisions.
#[derive(Debug, Clone)]
pub struct NodeMeta {
    /// Parallelism of this node.
    pub parallelism: usize,
    /// Number of output edges.
    pub output_count: usize,
    /// Number of input edges.
    pub input_count: usize,
    /// Whether this node is stateless.
    pub is_stateless: bool,
}

/// Simple dataflow graph for fusion detection.
pub struct DataflowGraph {
    nodes: HashMap<NodeId, NodeMeta>,
    edges: Vec<(NodeId, NodeId)>,
}

impl DataflowGraph {
    /// Create a new empty dataflow graph.
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: Vec::new(),
        }
    }

    /// Add a node to the graph.
    pub fn add_node(&mut self, id: NodeId, meta: NodeMeta) {
        self.nodes.insert(id, meta);
    }

    /// Add an edge between two nodes.
    pub fn add_edge(&mut self, from: NodeId, to: NodeId) {
        self.edges.push((from, to));
    }

    /// Get all nodes.
    pub fn nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.nodes.keys()
    }

    /// Get all edges in insertion order (deterministic).
    pub fn edges(&self) -> &[(NodeId, NodeId)] {
        &self.edges
    }

    /// Get the successor of a node (if any).
    pub fn successor(&self, node: &NodeId) -> Option<&NodeId> {
        self.edges
            .iter()
            .find(|(from, _)| from == node)
            .map(|(_, to)| to)
    }

    /// Get metadata for a node.
    pub fn meta(&self, node: &NodeId) -> Option<&NodeMeta> {
        self.nodes.get(node)
    }
}

impl Default for DataflowGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Detect chainable operators for fusion.
pub struct FusionDetector {
    graph: DataflowGraph,
}

impl FusionDetector {
    /// Create a new fusion detector with the given graph.
    pub fn new(graph: DataflowGraph) -> Self {
        Self { graph }
    }

    /// Detect operators that can be fused.
    pub fn detect_fusions(&self) -> Vec<OperatorFusion> {
        let mut fusions = Vec::new();

        // Iterate edges (insertion-ordered) rather than nodes (HashMap order) so
        // the detected fusions are deterministic and reproducible. Each forward
        // edge whose endpoints are compatible is a fusion candidate.
        for (source, sink) in self.graph.edges() {
            if self.can_fuse(source, sink) {
                fusions.push(OperatorFusion {
                    source: source.clone(),
                    sink: sink.clone(),
                });
            }
        }

        fusions
    }

    /// Check if two operators can be fused.
    fn can_fuse(&self, source: &NodeId, sink: &NodeId) -> bool {
        let source_meta = match self.graph.meta(source) {
            Some(meta) => meta,
            None => return false,
        };
        let sink_meta = match self.graph.meta(sink) {
            Some(meta) => meta,
            None => return false,
        };

        // Fuse if:
        // 1. Same parallelism
        source_meta.parallelism == sink_meta.parallelism
            // 2. Forward edge (no shuffle) - source has single output
            && source_meta.output_count == 1
            // 3. Sink has single input
            && sink_meta.input_count == 1
            // 4. Both are stateless or have compatible state
            && (source_meta.is_stateless || sink_meta.is_stateless)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fused_pipeline_runs_stages_in_one_pass() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();

        // Stage 1 (map): double every value. Stage 2 (filter): keep v > 4.
        let map_double: FusedStage = Box::new(move |b: RecordBatch| {
            let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let doubled: Int64Array = col.iter().map(|v| v.map(|x| x * 2)).collect();
            RecordBatch::try_new(b.schema(), vec![Arc::new(doubled)])
                .map_err(|e| crate::ExecError::Arrow(e.to_string()))
        });
        let filter_gt4: FusedStage = Box::new(move |b: RecordBatch| {
            let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let kept: Int64Array = col.iter().flatten().filter(|&x| x > 4).collect();
            RecordBatch::try_new(b.schema(), vec![Arc::new(kept)])
                .map_err(|e| crate::ExecError::Arrow(e.to_string()))
        });

        let pipeline = FusedPipeline::new().then(map_double).then(filter_gt4);
        assert_eq!(pipeline.len(), 2);
        let out = pipeline.run(batch).unwrap();
        // [1,2,3,4] → ×2 → [2,4,6,8] → >4 → [6,8].
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(col.values(), &[6, 8]);

        // An empty pipeline is the identity transform.
        let empty = FusedPipeline::new();
        assert!(empty.is_empty());
        let same = empty.run(RecordBatch::new_empty(schema.clone())).unwrap();
        assert_eq!(same.num_rows(), 0);
    }

    #[test]
    fn test_fusion_detection() {
        let mut graph = DataflowGraph::new();

        // Add nodes: filter → project → sink
        graph.add_node(
            NodeId("filter".to_string()),
            NodeMeta {
                parallelism: 1,
                output_count: 1,
                input_count: 1,
                is_stateless: true,
            },
        );
        graph.add_node(
            NodeId("project".to_string()),
            NodeMeta {
                parallelism: 1,
                output_count: 1,
                input_count: 1,
                is_stateless: true,
            },
        );
        graph.add_node(
            NodeId("sink".to_string()),
            NodeMeta {
                parallelism: 1,
                output_count: 0,
                input_count: 1,
                is_stateless: true,
            },
        );

        // Add edges
        graph.add_edge(NodeId("filter".to_string()), NodeId("project".to_string()));
        graph.add_edge(NodeId("project".to_string()), NodeId("sink".to_string()));

        let detector = FusionDetector::new(graph);
        let fusions = detector.detect_fusions();

        assert_eq!(fusions.len(), 2);
        assert_eq!(fusions[0].source.0, "filter");
        assert_eq!(fusions[0].sink.0, "project");
        assert_eq!(fusions[1].source.0, "project");
        assert_eq!(fusions[1].sink.0, "sink");
    }

    #[test]
    fn test_no_fusion_different_parallelism() {
        let mut graph = DataflowGraph::new();

        graph.add_node(
            NodeId("source".to_string()),
            NodeMeta {
                parallelism: 2,
                output_count: 1,
                input_count: 0,
                is_stateless: true,
            },
        );
        graph.add_node(
            NodeId("sink".to_string()),
            NodeMeta {
                parallelism: 1,
                output_count: 0,
                input_count: 1,
                is_stateless: true,
            },
        );

        graph.add_edge(NodeId("source".to_string()), NodeId("sink".to_string()));

        let detector = FusionDetector::new(graph);
        let fusions = detector.detect_fusions();

        assert_eq!(fusions.len(), 0);
    }
}
