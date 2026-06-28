//! Operator fusion detection for optimizing dataflow execution.
//!
//! `FusionDetector` identifies chainable operators that can be fused
//! to eliminate intermediate buffering and reduce overhead.

use std::collections::HashMap;

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
