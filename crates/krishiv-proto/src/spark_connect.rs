#![forbid(unsafe_code)]

//! Apache Spark Connect wire types (Spark 3.5 proto subset).

#[allow(clippy::large_enum_variant, clippy::result_large_err)]
pub mod connect {
    tonic::include_proto!("spark.connect");
}

/// Declared support matrix for Spark Connect plan nodes (ADR-R15.1 Option B).
#[derive(Debug, Clone, Default)]
pub struct SparkConnectCompatMatrix {
    pub supported_relations: Vec<&'static str>,
    pub supported_expressions: Vec<&'static str>,
}

impl SparkConnectCompatMatrix {
    /// Default matrix for the Krishiv Spark Connect server (TPC-H + PySpark shim).
    pub fn krishiv_default() -> Self {
        Self {
            supported_relations: vec![
                "sql",
                "read",
                "filter",
                "project",
                "join",
                "set_op",
                "sort",
                "limit",
                "aggregate",
                "with_columns",
                "drop",
                "deduplicate",
                "local_relation",
                "subquery_alias",
            ],
            supported_expressions: vec![
                "literal",
                "unresolved_attribute",
                "alias",
                "cast",
                "unresolved_function",
                "sort_order",
            ],
        }
    }

    pub fn supports_relation(&self, name: &str) -> bool {
        self.supported_relations.contains(&name)
    }

    pub fn supports_expression(&self, name: &str) -> bool {
        self.supported_expressions.contains(&name)
    }
}
