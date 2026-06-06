#![forbid(unsafe_code)]

//! Catalog abstractions for Krishiv.
//!
//! This crate defines `TableProvider`, `CatalogProvider`, schema types, and
//! column statistics. An in-memory reference implementation is included.

pub mod iceberg_rest;
pub use iceberg_rest::{
    GenericRestCatalog, IcebergCatalogClient, IcebergTableId, LoadedIcebergTable, RestCatalogConfig,
};

use std::collections::BTreeMap;
use std::fmt;

// ---------------------------------------------------------------------------
// Error and Result
// ---------------------------------------------------------------------------

/// Errors produced by catalog operations.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// A requested table was not found in the catalog.
    #[error("table not found: '{name}'")]
    TableNotFound { name: String },
    /// Table already exists and `if_not_exists` was false.
    #[error("table already exists: '{name}'")]
    TableAlreadyExists { name: String },
    /// A requested schema was not found.
    #[error("schema not found: '{name}'")]
    SchemaNotFound { name: String },
    /// The provided schema is structurally invalid.
    #[error("invalid schema: {message}")]
    InvalidSchema { message: String },
    /// Remote catalog configuration is malformed or unsafe.
    #[error("invalid catalog configuration: {message}")]
    InvalidConfiguration { message: String },
    /// A remote catalog request could not be completed.
    #[error("catalog transport error during {operation}: {message}")]
    Transport { operation: String, message: String },
    /// An HTTP request to a remote catalog service failed.
    #[error("HTTP error {status}: {message}")]
    Http { status: u16, message: String },
    /// A successful remote response did not satisfy the catalog contract.
    #[error("invalid catalog response during {operation}: {message}")]
    InvalidResponse { operation: String, message: String },
    /// A remote response exceeded the configured memory ceiling.
    #[error("catalog response during {operation} exceeded {limit_bytes} bytes")]
    ResponseTooLarge {
        operation: String,
        limit_bytes: usize,
    },
    /// The server explicitly does not advertise a required endpoint.
    #[error("catalog server does not support {operation}")]
    UnsupportedOperation { operation: String },
}

/// Convenience result alias for catalog operations.
pub type CatalogResult<T> = Result<T, CatalogError>;

// ---------------------------------------------------------------------------
// FieldType
// ---------------------------------------------------------------------------

/// Logical field types supported by the Krishiv catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Boolean,
    Utf8,
    Binary,
    Timestamp,
    Date32,
    /// List of `item_type` elements.
    List(Box<FieldType>),
    /// Struct with named fields.
    Struct(Vec<CatalogField>),
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            FieldType::Int8 => "Int8",
            FieldType::Int16 => "Int16",
            FieldType::Int32 => "Int32",
            FieldType::Int64 => "Int64",
            FieldType::UInt8 => "UInt8",
            FieldType::UInt16 => "UInt16",
            FieldType::UInt32 => "UInt32",
            FieldType::UInt64 => "UInt64",
            FieldType::Float32 => "Float32",
            FieldType::Float64 => "Float64",
            FieldType::Boolean => "Boolean",
            FieldType::Utf8 => "Utf8",
            FieldType::Binary => "Binary",
            FieldType::Timestamp => "Timestamp",
            FieldType::Date32 => "Date32",
            FieldType::List(inner) => return write!(f, "List<{inner}>"),
            FieldType::Struct(fields) => {
                return write!(f, "Struct({} fields)", fields.len());
            }
        };
        f.write_str(s)
    }
}

impl FieldType {
    /// Convert this field type to the equivalent Arrow [`DataType`].
    ///
    /// [`DataType`]: arrow::datatypes::DataType
    pub fn to_arrow(&self) -> arrow::datatypes::DataType {
        use arrow::datatypes::DataType;
        use arrow::datatypes::TimeUnit;
        match self {
            FieldType::Int8 => DataType::Int8,
            FieldType::Int16 => DataType::Int16,
            FieldType::Int32 => DataType::Int32,
            FieldType::Int64 => DataType::Int64,
            FieldType::UInt8 => DataType::UInt8,
            FieldType::UInt16 => DataType::UInt16,
            FieldType::UInt32 => DataType::UInt32,
            FieldType::UInt64 => DataType::UInt64,
            FieldType::Float32 => DataType::Float32,
            FieldType::Float64 => DataType::Float64,
            FieldType::Boolean => DataType::Boolean,
            FieldType::Utf8 => DataType::Utf8,
            FieldType::Binary => DataType::Binary,
            FieldType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            FieldType::Date32 => DataType::Date32,
            FieldType::List(item) => DataType::List(std::sync::Arc::new(
                arrow::datatypes::Field::new("item", item.to_arrow(), true),
            )),
            FieldType::Struct(fields) => {
                let arrow_fields: arrow::datatypes::Fields = fields
                    .iter()
                    .map(|f| {
                        std::sync::Arc::new(arrow::datatypes::Field::new(
                            f.name(),
                            f.field_type().to_arrow(),
                            f.nullable(),
                        ))
                    })
                    .collect();
                DataType::Struct(arrow_fields)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// CatalogField
// ---------------------------------------------------------------------------

/// A single field in a catalog table schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogField {
    name: String,
    field_type: FieldType,
    nullable: bool,
}

impl CatalogField {
    /// Create a new catalog field.
    pub fn new(name: impl Into<String>, field_type: FieldType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            field_type,
            nullable,
        }
    }

    /// The field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The logical field type.
    pub fn field_type(&self) -> &FieldType {
        &self.field_type
    }

    /// Whether the field is nullable.
    pub fn nullable(&self) -> bool {
        self.nullable
    }

    /// Convert this field to an Arrow [`Field`].
    ///
    /// [`Field`]: arrow::datatypes::Field
    pub fn to_arrow_field(&self) -> arrow::datatypes::Field {
        arrow::datatypes::Field::new(
            self.name.as_str(),
            self.field_type.to_arrow(),
            self.nullable,
        )
    }
}

// ---------------------------------------------------------------------------
// TableSchema
// ---------------------------------------------------------------------------

/// The schema of a catalog table: an ordered list of fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    fields: Vec<CatalogField>,
}

impl TableSchema {
    /// Create a new schema from a list of fields.
    pub fn new(fields: Vec<CatalogField>) -> Self {
        Self { fields }
    }

    /// Create an empty schema with no fields.
    pub fn empty() -> Self {
        Self { fields: Vec::new() }
    }

    /// Convert to an Arrow [`Schema`].
    ///
    /// [`Schema`]: arrow::datatypes::Schema
    pub fn to_arrow_schema(&self) -> arrow::datatypes::Schema {
        let arrow_fields: Vec<arrow::datatypes::Field> = self
            .fields
            .iter()
            .map(CatalogField::to_arrow_field)
            .collect();
        arrow::datatypes::Schema::new(arrow_fields)
    }

    /// Return the number of fields in this schema.
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// Look up a field by name.
    pub fn get_field(&self, name: &str) -> Option<&CatalogField> {
        self.fields.iter().find(|f| f.name() == name)
    }
}

// ---------------------------------------------------------------------------
// ColumnStatistics
// ---------------------------------------------------------------------------

/// Optional statistics for a table column (or the whole table).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ColumnStatistics {
    /// Total number of rows in the table, if known.
    pub row_count: Option<u64>,
    /// Number of null values in the column, if known.
    pub null_count: Option<u64>,
    /// String representation of the minimum value, if known.
    pub min_value: Option<String>,
    /// String representation of the maximum value, if known.
    pub max_value: Option<String>,
}

impl ColumnStatistics {
    /// Create a new `ColumnStatistics` with all fields set to `None`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the row count.
    #[must_use]
    pub fn with_row_count(mut self, count: u64) -> Self {
        self.row_count = Some(count);
        self
    }

    /// Set the null count.
    #[must_use]
    pub fn with_null_count(mut self, count: u64) -> Self {
        self.null_count = Some(count);
        self
    }

    /// Set the minimum value.
    #[must_use]
    pub fn with_min(mut self, min: impl Into<String>) -> Self {
        self.min_value = Some(min.into());
        self
    }

    /// Set the maximum value.
    #[must_use]
    pub fn with_max(mut self, max: impl Into<String>) -> Self {
        self.max_value = Some(max.into());
        self
    }
}

// ---------------------------------------------------------------------------
// TableMetadata
// ---------------------------------------------------------------------------

/// Full metadata for a table: name, schema, and optional statistics.
#[derive(Debug, Clone)]
pub struct TableMetadata {
    name: String,
    schema: TableSchema,
    stats: Option<ColumnStatistics>,
}

impl TableMetadata {
    /// Create new table metadata with no statistics.
    pub fn new(name: impl Into<String>, schema: TableSchema) -> Self {
        Self {
            name: name.into(),
            schema,
            stats: None,
        }
    }

    /// Attach column statistics and return the updated metadata.
    #[must_use]
    pub fn with_stats(mut self, stats: ColumnStatistics) -> Self {
        self.stats = Some(stats);
        self
    }

    /// The table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The table schema.
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Optional column statistics.
    pub fn statistics(&self) -> Option<&ColumnStatistics> {
        self.stats.as_ref()
    }
}

// ---------------------------------------------------------------------------
// TableProvider trait
// ---------------------------------------------------------------------------

/// A resolved reference to a single table's metadata.
pub trait TableProvider {
    /// The table name.
    fn name(&self) -> &str;

    /// The table schema.
    fn schema(&self) -> &TableSchema;

    /// Optional column statistics.
    fn statistics(&self) -> Option<&ColumnStatistics>;
}

// ---------------------------------------------------------------------------
// CatalogProvider trait
// ---------------------------------------------------------------------------

/// A registry of tables that can be listed, looked up, and registered.
pub trait CatalogProvider {
    /// Return the names of all tables in the catalog.
    fn list_tables(&self) -> Vec<String>;

    /// Look up a table by name.
    fn get_table(&self, name: &str) -> CatalogResult<&dyn TableProvider>;

    /// Register a table in the catalog.
    ///
    /// Returns an error if the schema is structurally invalid or if
    /// implementation-specific constraints are violated.
    fn register_table(&mut self, metadata: TableMetadata) -> CatalogResult<()>;
}

// ---------------------------------------------------------------------------
// InMemoryCatalog
// ---------------------------------------------------------------------------

/// A `TableProvider` wrapper over `TableMetadata`.
struct TableMetadataProvider {
    metadata: TableMetadata,
}

impl TableProvider for TableMetadataProvider {
    fn name(&self) -> &str {
        self.metadata.name()
    }

    fn schema(&self) -> &TableSchema {
        self.metadata.schema()
    }

    fn statistics(&self) -> Option<&ColumnStatistics> {
        self.metadata.statistics()
    }
}

/// An in-memory catalog backed by a sorted map.
pub struct InMemoryCatalog {
    tables: BTreeMap<String, TableMetadataProvider>,
    /// Optional in-memory row data keyed by table name.
    table_data: BTreeMap<String, std::sync::Arc<Vec<arrow::record_batch::RecordBatch>>>,
}

impl InMemoryCatalog {
    /// Create a new, empty in-memory catalog.
    pub fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
            table_data: BTreeMap::new(),
        }
    }

    /// Register a table and attach in-memory Arrow batches for SQL scans (P0-9).
    pub fn register_table_with_batches(
        &mut self,
        metadata: TableMetadata,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> CatalogResult<()> {
        let name = metadata.name().to_owned();
        self.register_table(metadata)?;
        if !batches.is_empty() {
            self.table_data.insert(name, std::sync::Arc::new(batches));
        }
        Ok(())
    }

    /// Return stored batches for a registered table, if any.
    pub fn table_batches(
        &self,
        name: &str,
    ) -> Option<std::sync::Arc<Vec<arrow::record_batch::RecordBatch>>> {
        self.table_data.get(name).cloned()
    }
}

impl Default for InMemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogProvider for InMemoryCatalog {
    fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    fn get_table(&self, name: &str) -> CatalogResult<&dyn TableProvider> {
        self.tables
            .get(name)
            .map(|p| p as &dyn TableProvider)
            .ok_or_else(|| CatalogError::TableNotFound {
                name: name.to_string(),
            })
    }

    fn register_table(&mut self, metadata: TableMetadata) -> CatalogResult<()> {
        let name = metadata.name().to_string();
        if self.tables.contains_key(&name) {
            return Err(CatalogError::TableAlreadyExists { name });
        }
        self.tables.insert(name, TableMetadataProvider { metadata });
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SchemaRegistry
// ---------------------------------------------------------------------------

/// A registry that maps logical schema names to [`TableSchema`] definitions.
///
/// Used by connectors (e.g., Kafka Avro/Protobuf topics) to resolve the
/// Arrow schema for a data stream at runtime without hard-coding field lists.
pub trait SchemaRegistry: Send + Sync {
    /// Look up a schema by name.
    fn get_schema(&self, name: &str) -> CatalogResult<TableSchema>;
    /// Register a schema under a name, replacing any existing entry.
    fn register_schema(&mut self, name: impl Into<String>, schema: TableSchema);
    /// Return all registered schema names.
    fn schema_names(&self) -> Vec<String>;
}

/// An in-memory [`SchemaRegistry`] backed by a sorted map.
#[derive(Debug, Default)]
pub struct InMemorySchemaRegistry {
    schemas: BTreeMap<String, TableSchema>,
}

impl InMemorySchemaRegistry {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SchemaRegistry for InMemorySchemaRegistry {
    fn get_schema(&self, name: &str) -> CatalogResult<TableSchema> {
        self.schemas
            .get(name)
            .cloned()
            .ok_or_else(|| CatalogError::SchemaNotFound {
                name: name.to_string(),
            })
    }

    fn register_schema(&mut self, name: impl Into<String>, schema: TableSchema) {
        self.schemas.insert(name.into(), schema);
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// DataFusion catalog bridge
// ---------------------------------------------------------------------------

/// DataFusion integration: wraps [`InMemoryCatalog`] as DataFusion catalog
/// and schema providers so that Krishiv catalog tables can be used directly
/// inside a DataFusion [`SessionContext`].
///
/// [`SessionContext`]: datafusion::prelude::SessionContext
pub mod datafusion_bridge {
    use std::any::Any;
    use std::fmt;
    use std::sync::{Arc, RwLock};

    use datafusion::catalog::{CatalogProvider, SchemaProvider};
    use datafusion::datasource::MemTable;
    use datafusion::error::Result as DfResult;

    /// Bridges a Krishiv [`InMemoryCatalog`] into a DataFusion
    /// [`CatalogProvider`].
    ///
    /// The bridge exposes a single schema named `"public"` that mirrors
    /// the tables registered in the underlying [`InMemoryCatalog`].
    ///
    /// [`InMemoryCatalog`]: crate::InMemoryCatalog
    pub struct DataFusionCatalogBridge {
        catalog: Arc<RwLock<crate::InMemoryCatalog>>,
        schema_name: String,
        /// MemTable cache shared across the inner `DataFusionSchemaBridge`
        /// instances DataFusion requests. Avoids re-cloning the entire
        /// `Vec<RecordBatch>` payload (which can be hundreds of MB) on
        /// every DataFusion `table()` call. Keyed by table name; cleared
        /// on `invalidate(name)`.
        schema_cache: std::sync::Arc<dashmap::DashMap<String, Arc<MemTable>>>,
    }

    impl DataFusionCatalogBridge {
        /// Create a bridge from an [`InMemoryCatalog`] shared reference.
        ///
        /// [`InMemoryCatalog`]: crate::InMemoryCatalog
        pub fn new(catalog: Arc<RwLock<crate::InMemoryCatalog>>) -> Self {
            Self {
                catalog,
                schema_name: "public".to_string(),
                schema_cache: std::sync::Arc::new(dashmap::DashMap::new()),
            }
        }

        /// Invalidate the MemTable cache for `name`, forcing the next
        /// `table()` call to rebuild the cached `MemTable` from the
        /// catalog's batch store. Call this after `register_table_with_batches`
        /// mutates the underlying catalog so the bridge does not serve
        /// a stale `MemTable` referencing the previous batch payload.
        ///
        /// The `DashMap` cache is per-bridge and survives across DataFusion
        /// `table()` calls; without this invalidation hook a second
        /// `register_table_with_batches` for the same name would not be
        /// visible to the DataFusion query plan.
        pub fn invalidate(&self, name: &str) {
            self.schema_cache.remove(name);
        }
    }

    impl fmt::Debug for DataFusionCatalogBridge {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("DataFusionCatalogBridge")
                .field("schema_name", &self.schema_name)
                .finish()
        }
    }

    impl CatalogProvider for DataFusionCatalogBridge {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema_names(&self) -> Vec<String> {
            vec![self.schema_name.clone()]
        }

        fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
            if name == self.schema_name {
                Some(Arc::new(DataFusionSchemaBridge {
                    catalog: self.catalog.clone(),
                    cache: self.schema_cache.clone(),
                }))
            } else {
                None
            }
        }
    }

    // -----------------------------------------------------------------------

    struct DataFusionSchemaBridge {
        catalog: Arc<RwLock<crate::InMemoryCatalog>>,
        cache: std::sync::Arc<dashmap::DashMap<String, Arc<MemTable>>>,
    }

    impl fmt::Debug for DataFusionSchemaBridge {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("DataFusionSchemaBridge").finish()
        }
    }

    #[async_trait::async_trait]
    impl SchemaProvider for DataFusionSchemaBridge {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn table_names(&self) -> Vec<String> {
            let catalog = self.catalog.read().unwrap_or_else(|p| p.into_inner());
            use crate::CatalogProvider as KrishivCatalogProvider;
            catalog.list_tables()
        }

        async fn table(
            &self,
            name: &str,
        ) -> DfResult<Option<Arc<dyn datafusion::datasource::TableProvider>>> {
            // MemTable cache: if we built one for this table already, return
            // the cached Arc. The cache is invalidated explicitly via
            // `invalidate(name)` when the underlying catalog is mutated.
            if let Some(cached) = self.cache.get(name) {
                return Ok(Some(
                    cached.clone() as Arc<dyn datafusion::datasource::TableProvider>
                ));
            }
            let catalog = self.catalog.read().unwrap_or_else(|p| p.into_inner());
            use crate::CatalogProvider as KrishivCatalogProvider;
            match catalog.get_table(name) {
                Ok(table_provider) => {
                    let arrow_schema = Arc::new(table_provider.schema().to_arrow_schema());
                    let batches = catalog.table_batches(name);
                    let partitions = batches.map(|b| (*b).clone()).unwrap_or_default();
                    let mem = MemTable::try_new(arrow_schema, vec![partitions])?;
                    let mem_arc = Arc::new(mem);
                    self.cache.insert(name.to_string(), mem_arc.clone());
                    Ok(Some(
                        mem_arc as Arc<dyn datafusion::datasource::TableProvider>,
                    ))
                }
                Err(crate::CatalogError::TableNotFound { .. }) => Ok(None),
                Err(error) => Err(datafusion::error::DataFusionError::External(Box::new(
                    error,
                ))),
            }
        }

        fn table_exist(&self, name: &str) -> bool {
            let catalog = self.catalog.read().unwrap_or_else(|p| p.into_inner());
            use crate::CatalogProvider as KrishivCatalogProvider;
            catalog.list_tables().iter().any(|table| table == name)
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_schema() -> TableSchema {
        TableSchema::new(vec![
            CatalogField::new("id", FieldType::Int64, false),
            CatalogField::new("name", FieldType::Utf8, true),
        ])
    }

    #[test]
    fn in_memory_catalog_registers_and_retrieves_table() {
        let mut catalog = InMemoryCatalog::new();
        let meta = TableMetadata::new("users", make_schema());
        catalog.register_table(meta).unwrap();

        let table = catalog.get_table("users").unwrap();
        assert_eq!(table.name(), "users");
        assert_eq!(table.schema().field_count(), 2);
    }

    #[test]
    fn in_memory_catalog_lists_tables() {
        let mut catalog = InMemoryCatalog::new();
        catalog
            .register_table(TableMetadata::new("alpha", make_schema()))
            .unwrap();
        catalog
            .register_table(TableMetadata::new("beta", make_schema()))
            .unwrap();

        let mut tables = catalog.list_tables();
        tables.sort();
        assert_eq!(tables, vec!["alpha", "beta"]);
    }

    #[test]
    fn in_memory_catalog_returns_error_for_unknown_table() {
        let catalog = InMemoryCatalog::new();
        let err = catalog.get_table("nonexistent").err().unwrap();
        match err {
            CatalogError::TableNotFound { name } => {
                assert_eq!(name, "nonexistent");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn table_schema_converts_to_arrow_schema() {
        let schema = make_schema();
        let arrow_schema = schema.to_arrow_schema();

        assert_eq!(arrow_schema.fields().len(), 2);
        let id_field = arrow_schema.field_with_name("id").unwrap();
        assert_eq!(id_field.data_type(), &arrow::datatypes::DataType::Int64);
        assert!(!id_field.is_nullable());

        let name_field = arrow_schema.field_with_name("name").unwrap();
        assert_eq!(name_field.data_type(), &arrow::datatypes::DataType::Utf8);
        assert!(name_field.is_nullable());
    }

    // -----------------------------------------------------------------------
    // SchemaRegistry tests
    // -----------------------------------------------------------------------

    #[test]
    fn schema_registry_registers_and_retrieves() {
        let mut registry = InMemorySchemaRegistry::new();
        registry.register_schema("events", make_schema());
        let schema = registry.get_schema("events").unwrap();
        assert_eq!(schema.field_count(), 2);
    }

    #[test]
    fn schema_registry_returns_error_for_missing() {
        let registry = InMemorySchemaRegistry::new();
        let err = registry.get_schema("nonexistent").unwrap_err();
        match err {
            CatalogError::SchemaNotFound { name } => {
                assert_eq!(name, "nonexistent");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn schema_registry_lists_names() {
        let mut registry = InMemorySchemaRegistry::new();
        registry.register_schema("orders", make_schema());
        registry.register_schema("users", make_schema());
        let mut names = registry.schema_names();
        names.sort();
        assert_eq!(names, vec!["orders", "users"]);
    }

    // -----------------------------------------------------------------------
    // DataFusion bridge tests
    // -----------------------------------------------------------------------

    #[test]
    fn datafusion_bridge_schema_names_returns_public() {
        use datafusion::catalog::CatalogProvider as DfCatalogProvider;

        let catalog = std::sync::Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        let bridge = crate::datafusion_bridge::DataFusionCatalogBridge::new(catalog);
        let names = bridge.schema_names();
        assert_eq!(names, vec!["public"]);
    }

    #[test]
    fn datafusion_bridge_table_exist() {
        let catalog = std::sync::Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        {
            let mut cat = catalog.write().unwrap();
            cat.register_table(TableMetadata::new("orders", make_schema()))
                .unwrap();
        }
        let bridge = crate::datafusion_bridge::DataFusionCatalogBridge::new(catalog);
        let schema_provider = {
            use datafusion::catalog::CatalogProvider as DfCatalogProvider;
            bridge.schema("public").unwrap()
        };
        assert!(schema_provider.table_exist("orders"));
        assert!(!schema_provider.table_exist("nonexistent"));
    }

    #[tokio::test]
    async fn datafusion_bridge_memtable_cache_reuses_arc() {
        use std::sync::Arc;

        let catalog = Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        {
            let mut cat = catalog.write().unwrap();
            cat.register_table(TableMetadata::new("orders", make_schema()))
                .unwrap();
        }
        let bridge = crate::datafusion_bridge::DataFusionCatalogBridge::new(catalog);
        let schema_provider = {
            use datafusion::catalog::CatalogProvider as DfCatalogProvider;
            bridge.schema("public").unwrap()
        };
        let first = schema_provider.table("orders").await.unwrap().unwrap();
        let second = schema_provider.table("orders").await.unwrap().unwrap();
        // Cached: identical Arc pointer, no re-clone of batch payload.
        let cached = Arc::ptr_eq(&first, &second);
        assert!(cached, "expected cached MemTable Arc, got fresh allocation");
    }

    #[tokio::test]
    async fn datafusion_bridge_invalidate_forces_rebuild() {
        use std::sync::Arc;

        let catalog = Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        {
            let mut cat = catalog.write().unwrap();
            cat.register_table(TableMetadata::new("orders", make_schema()))
                .unwrap();
        }
        let bridge = crate::datafusion_bridge::DataFusionCatalogBridge::new(catalog);
        let schema_provider = {
            use datafusion::catalog::CatalogProvider as DfCatalogProvider;
            bridge.schema("public").unwrap()
        };
        let first = schema_provider.table("orders").await.unwrap().unwrap();
        bridge.invalidate("orders");
        let second = schema_provider.table("orders").await.unwrap().unwrap();
        // After invalidation, a new MemTable Arc is constructed.
        assert!(!Arc::ptr_eq(&first, &second));
        // A second call without further invalidation must hit the cache again.
        let third = schema_provider.table("orders").await.unwrap().unwrap();
        assert!(Arc::ptr_eq(&second, &third));
    }

    #[tokio::test]
    async fn catalog_scan_returns_registered_row_count() {
        use std::sync::Arc;

        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::prelude::SessionContext;

        let catalog = Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        let schema = TableSchema::new(vec![CatalogField::new("id", FieldType::Int64, false)]);
        let arrow_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let values: Vec<Option<i64>> = (0..10).map(Some).collect();
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(Int64Array::from(values))]).unwrap();
        catalog
            .write()
            .unwrap()
            .register_table_with_batches(TableMetadata::new("t", schema), vec![batch])
            .unwrap();

        let ctx = SessionContext::new();
        ctx.register_catalog(
            "krishiv",
            Arc::new(crate::datafusion_bridge::DataFusionCatalogBridge::new(
                catalog,
            )),
        );
        let df = ctx.sql("SELECT * FROM krishiv.public.t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 10);
    }

    #[test]
    fn datafusion_bridge_unknown_schema_returns_none() {
        use datafusion::catalog::CatalogProvider as DfCatalogProvider;

        let catalog = std::sync::Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        let bridge = crate::datafusion_bridge::DataFusionCatalogBridge::new(catalog);
        let result = bridge.schema("nonexistent");
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // CatalogError Display tests
    // -----------------------------------------------------------------------

    #[test]
    fn catalog_error_display_table_not_found() {
        let err = CatalogError::TableNotFound {
            name: "orders".to_string(),
        };
        assert_eq!(err.to_string(), "table not found: 'orders'");
    }

    #[test]
    fn catalog_error_display_table_already_exists() {
        let err = CatalogError::TableAlreadyExists {
            name: "users".to_string(),
        };
        assert_eq!(err.to_string(), "table already exists: 'users'");
    }

    #[test]
    fn catalog_error_display_schema_not_found() {
        let err = CatalogError::SchemaNotFound {
            name: "events".to_string(),
        };
        assert_eq!(err.to_string(), "schema not found: 'events'");
    }

    #[test]
    fn catalog_error_display_invalid_schema() {
        let err = CatalogError::InvalidSchema {
            message: "missing required field 'id'".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "invalid schema: missing required field 'id'"
        );
    }

    #[test]
    fn catalog_error_display_http() {
        let err = CatalogError::Http {
            status: 404,
            message: "not found".to_string(),
        };
        assert_eq!(err.to_string(), "HTTP error 404: not found");
    }

    #[test]
    fn catalog_error_is_std_error() {
        let err = CatalogError::TableNotFound {
            name: "t".to_string(),
        };
        let e: &dyn std::error::Error = &err;
        assert!(e.source().is_none());
    }

    // -----------------------------------------------------------------------
    // FieldType to_arrow tests
    // -----------------------------------------------------------------------

    #[test]
    fn field_type_to_arrow_int8() {
        assert_eq!(FieldType::Int8.to_arrow(), arrow::datatypes::DataType::Int8);
    }

    #[test]
    fn field_type_to_arrow_int16() {
        assert_eq!(
            FieldType::Int16.to_arrow(),
            arrow::datatypes::DataType::Int16
        );
    }

    #[test]
    fn field_type_to_arrow_int32() {
        assert_eq!(
            FieldType::Int32.to_arrow(),
            arrow::datatypes::DataType::Int32
        );
    }

    #[test]
    fn field_type_to_arrow_int64() {
        assert_eq!(
            FieldType::Int64.to_arrow(),
            arrow::datatypes::DataType::Int64
        );
    }

    #[test]
    fn field_type_to_arrow_uint8() {
        assert_eq!(
            FieldType::UInt8.to_arrow(),
            arrow::datatypes::DataType::UInt8
        );
    }

    #[test]
    fn field_type_to_arrow_uint16() {
        assert_eq!(
            FieldType::UInt16.to_arrow(),
            arrow::datatypes::DataType::UInt16
        );
    }

    #[test]
    fn field_type_to_arrow_uint32() {
        assert_eq!(
            FieldType::UInt32.to_arrow(),
            arrow::datatypes::DataType::UInt32
        );
    }

    #[test]
    fn field_type_to_arrow_uint64() {
        assert_eq!(
            FieldType::UInt64.to_arrow(),
            arrow::datatypes::DataType::UInt64
        );
    }

    #[test]
    fn field_type_to_arrow_float32() {
        assert_eq!(
            FieldType::Float32.to_arrow(),
            arrow::datatypes::DataType::Float32
        );
    }

    #[test]
    fn field_type_to_arrow_float64() {
        assert_eq!(
            FieldType::Float64.to_arrow(),
            arrow::datatypes::DataType::Float64
        );
    }

    #[test]
    fn field_type_to_arrow_boolean() {
        assert_eq!(
            FieldType::Boolean.to_arrow(),
            arrow::datatypes::DataType::Boolean
        );
    }

    #[test]
    fn field_type_to_arrow_utf8() {
        assert_eq!(FieldType::Utf8.to_arrow(), arrow::datatypes::DataType::Utf8);
    }

    #[test]
    fn field_type_to_arrow_binary() {
        assert_eq!(
            FieldType::Binary.to_arrow(),
            arrow::datatypes::DataType::Binary
        );
    }

    #[test]
    fn field_type_to_arrow_timestamp() {
        use arrow::datatypes::{DataType, TimeUnit};
        assert_eq!(
            FieldType::Timestamp.to_arrow(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn field_type_to_arrow_date32() {
        assert_eq!(
            FieldType::Date32.to_arrow(),
            arrow::datatypes::DataType::Date32
        );
    }

    #[test]
    fn field_type_to_arrow_list() {
        let list_type = FieldType::List(Box::new(FieldType::Utf8));
        match list_type.to_arrow() {
            arrow::datatypes::DataType::List(field) => {
                assert_eq!(field.name(), "item");
                assert_eq!(field.data_type(), &arrow::datatypes::DataType::Utf8);
                assert!(field.is_nullable());
            }
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn field_type_to_arrow_struct() {
        let struct_type = FieldType::Struct(vec![
            CatalogField::new("x", FieldType::Int32, false),
            CatalogField::new("y", FieldType::Utf8, true),
        ]);
        match struct_type.to_arrow() {
            arrow::datatypes::DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "x");
                assert_eq!(fields[0].data_type(), &arrow::datatypes::DataType::Int32);
                assert!(!fields[0].is_nullable());
                assert_eq!(fields[1].name(), "y");
                assert_eq!(fields[1].data_type(), &arrow::datatypes::DataType::Utf8);
                assert!(fields[1].is_nullable());
            }
            other => panic!("expected Struct, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // FieldType Display tests
    // -----------------------------------------------------------------------

    #[test]
    fn field_type_display_simple() {
        assert_eq!(FieldType::Int8.to_string(), "Int8");
        assert_eq!(FieldType::Int16.to_string(), "Int16");
        assert_eq!(FieldType::Int32.to_string(), "Int32");
        assert_eq!(FieldType::Int64.to_string(), "Int64");
        assert_eq!(FieldType::UInt8.to_string(), "UInt8");
        assert_eq!(FieldType::UInt16.to_string(), "UInt16");
        assert_eq!(FieldType::UInt32.to_string(), "UInt32");
        assert_eq!(FieldType::UInt64.to_string(), "UInt64");
        assert_eq!(FieldType::Float32.to_string(), "Float32");
        assert_eq!(FieldType::Float64.to_string(), "Float64");
        assert_eq!(FieldType::Boolean.to_string(), "Boolean");
        assert_eq!(FieldType::Utf8.to_string(), "Utf8");
        assert_eq!(FieldType::Binary.to_string(), "Binary");
        assert_eq!(FieldType::Timestamp.to_string(), "Timestamp");
        assert_eq!(FieldType::Date32.to_string(), "Date32");
    }

    #[test]
    fn field_type_display_list() {
        let list = FieldType::List(Box::new(FieldType::Int32));
        assert_eq!(list.to_string(), "List<Int32>");
    }

    #[test]
    fn field_type_display_struct() {
        let s = FieldType::Struct(vec![
            CatalogField::new("a", FieldType::Boolean, true),
            CatalogField::new("b", FieldType::Utf8, false),
        ]);
        assert_eq!(s.to_string(), "Struct(2 fields)");
    }

    // -----------------------------------------------------------------------
    // CatalogField tests
    // -----------------------------------------------------------------------

    #[test]
    fn catalog_field_accessors() {
        let f = CatalogField::new("col", FieldType::Float64, true);
        assert_eq!(f.name(), "col");
        assert_eq!(f.field_type(), &FieldType::Float64);
        assert!(f.nullable());
    }

    #[test]
    fn catalog_field_to_arrow_field() {
        let f = CatalogField::new("ts", FieldType::Timestamp, false);
        let arrow_f = f.to_arrow_field();
        assert_eq!(arrow_f.name(), "ts");
        use arrow::datatypes::{DataType, TimeUnit};
        assert_eq!(
            arrow_f.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert!(!arrow_f.is_nullable());
    }

    // -----------------------------------------------------------------------
    // ColumnStatistics tests
    // -----------------------------------------------------------------------

    #[test]
    fn column_statistics_new_defaults() {
        let stats = ColumnStatistics::new();
        assert!(stats.row_count.is_none());
        assert!(stats.null_count.is_none());
        assert!(stats.min_value.is_none());
        assert!(stats.max_value.is_none());
    }

    #[test]
    fn column_statistics_default_trait() {
        let stats = ColumnStatistics::default();
        assert_eq!(stats, ColumnStatistics::new());
    }

    #[test]
    fn column_statistics_builder_all_fields() {
        let stats = ColumnStatistics::new()
            .with_row_count(1_000_000)
            .with_null_count(42)
            .with_min("abc")
            .with_max("xyz");

        assert_eq!(stats.row_count, Some(1_000_000));
        assert_eq!(stats.null_count, Some(42));
        assert_eq!(stats.min_value.as_deref(), Some("abc"));
        assert_eq!(stats.max_value.as_deref(), Some("xyz"));
    }

    #[test]
    fn column_statistics_builder_partial() {
        let stats = ColumnStatistics::new().with_row_count(500);
        assert_eq!(stats.row_count, Some(500));
        assert!(stats.null_count.is_none());
        assert!(stats.min_value.is_none());
        assert!(stats.max_value.is_none());
    }

    #[test]
    fn column_statistics_into_string() {
        let stats = ColumnStatistics::new()
            .with_row_count(100)
            .with_null_count(5)
            .with_min("1")
            .with_max("99");
        let dbg = format!("{stats:?}");
        assert!(dbg.contains("row_count: Some(100)"));
        assert!(dbg.contains("null_count: Some(5)"));
        assert!(dbg.contains("min_value: Some(\"1\")"));
        assert!(dbg.contains("max_value: Some(\"99\")"));
    }

    #[test]
    fn column_statistics_builder_overwrites() {
        let stats = ColumnStatistics::new()
            .with_row_count(10)
            .with_row_count(20);
        assert_eq!(stats.row_count, Some(20));
    }

    #[test]
    fn column_statistics_eq() {
        let a = ColumnStatistics::new()
            .with_row_count(100)
            .with_null_count(5);
        let b = ColumnStatistics::new()
            .with_row_count(100)
            .with_null_count(5);
        let c = ColumnStatistics::new().with_row_count(99);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    // -----------------------------------------------------------------------
    // TableMetadata tests
    // -----------------------------------------------------------------------

    #[test]
    fn table_metadata_new_and_accessors() {
        let meta = TableMetadata::new("events", make_schema());
        assert_eq!(meta.name(), "events");
        assert_eq!(meta.schema().field_count(), 2);
        assert!(meta.statistics().is_none());
    }

    #[test]
    fn table_metadata_with_stats() {
        let stats = ColumnStatistics::new()
            .with_row_count(5000)
            .with_null_count(100);
        let meta = TableMetadata::new("clicks", make_schema()).with_stats(stats);
        assert_eq!(meta.name(), "clicks");
        let s = meta.statistics().unwrap();
        assert_eq!(s.row_count, Some(5000));
        assert_eq!(s.null_count, Some(100));
    }

    #[test]
    fn table_metadata_into_string() {
        let meta = TableMetadata::new("test_table", make_schema());
        let dbg = format!("{meta:?}");
        assert!(dbg.contains("name: \"test_table\""));
        assert!(dbg.contains("stats: None"));
    }

    // -----------------------------------------------------------------------
    // InMemorySchemaRegistry replace behavior
    // -----------------------------------------------------------------------

    #[test]
    fn schema_registry_register_replaces_existing() {
        let mut registry = InMemorySchemaRegistry::new();
        let schema_a = TableSchema::new(vec![CatalogField::new("a", FieldType::Int32, false)]);
        let schema_b = TableSchema::new(vec![CatalogField::new("b", FieldType::Utf8, true)]);

        registry.register_schema("my_schema", schema_a);
        registry.register_schema("my_schema", schema_b);

        let retrieved = registry.get_schema("my_schema").unwrap();
        assert_eq!(retrieved.field_count(), 1);
        assert_eq!(
            retrieved.get_field("b").unwrap().field_type(),
            &FieldType::Utf8
        );
        assert!(retrieved.get_field("a").is_none());
    }

    #[test]
    fn schema_registry_empty_names() {
        let registry = InMemorySchemaRegistry::new();
        assert!(registry.schema_names().is_empty());
    }

    // -----------------------------------------------------------------------
    // TableSchema accessors
    // -----------------------------------------------------------------------

    #[test]
    fn table_schema_empty() {
        let schema = TableSchema::empty();
        assert_eq!(schema.field_count(), 0);
        assert!(schema.get_field("anything").is_none());
    }

    #[test]
    fn table_schema_get_field_found() {
        let schema = make_schema();
        let field = schema.get_field("name").unwrap();
        assert_eq!(field.name(), "name");
        assert_eq!(field.field_type(), &FieldType::Utf8);
        assert!(field.nullable());
    }

    #[test]
    fn table_schema_get_field_not_found() {
        let schema = make_schema();
        assert!(schema.get_field("missing").is_none());
    }

    // -----------------------------------------------------------------------
    // InMemoryCatalog duplicate registration
    // -----------------------------------------------------------------------

    #[test]
    fn in_memory_catalog_duplicate_register_errors() {
        let mut catalog = InMemoryCatalog::new();
        catalog
            .register_table(TableMetadata::new("t", make_schema()))
            .unwrap();
        let err = catalog
            .register_table(TableMetadata::new("t", make_schema()))
            .unwrap_err();
        match err {
            CatalogError::TableAlreadyExists { name } => assert_eq!(name, "t"),
            other => panic!("expected TableAlreadyExists, got {other}"),
        }
    }

    // -----------------------------------------------------------------------
    // CatalogResult type alias
    // -----------------------------------------------------------------------

    #[test]
    fn catalog_result_ok() {
        let r: CatalogResult<i32> = Ok(42);
        assert_eq!(r.unwrap(), 42);
    }

    #[test]
    fn catalog_result_err() {
        let r: CatalogResult<()> = Err(CatalogError::TableNotFound {
            name: "x".to_string(),
        });
        assert!(r.is_err());
    }

    // -----------------------------------------------------------------------
    // Edge cases: empty catalog
    // -----------------------------------------------------------------------

    #[test]
    fn empty_catalog_list_tables_returns_empty() {
        let catalog = InMemoryCatalog::new();
        assert!(catalog.list_tables().is_empty());
    }

    #[test]
    fn empty_catalog_get_table_returns_not_found() {
        let catalog = InMemoryCatalog::new();
        let err = catalog.get_table("anything").err().unwrap();
        assert!(matches!(err, CatalogError::TableNotFound { .. }));
    }

    #[test]
    fn empty_schema_registry_get_returns_not_found() {
        let registry = InMemorySchemaRegistry::new();
        assert!(registry.get_schema("x").is_err());
    }

    #[test]
    fn empty_schema_schema_names_empty() {
        let registry = InMemorySchemaRegistry::new();
        assert!(registry.schema_names().is_empty());
    }

    // -----------------------------------------------------------------------
    // Edge cases: special characters in names
    // -----------------------------------------------------------------------

    #[test]
    fn table_name_with_special_characters() {
        let mut catalog = InMemoryCatalog::new();
        let meta = TableMetadata::new("table-with-dashes.dots_and_underscores", make_schema());
        catalog.register_table(meta).unwrap();
        let table = catalog
            .get_table("table-with-dashes.dots_and_underscores")
            .unwrap();
        assert_eq!(table.name(), "table-with-dashes.dots_and_underscores");
    }

    #[test]
    fn table_name_with_unicode() {
        let mut catalog = InMemoryCatalog::new();
        let meta = TableMetadata::new("用户_table", make_schema());
        catalog.register_table(meta).unwrap();
        let table = catalog.get_table("用户_table").unwrap();
        assert_eq!(table.name(), "用户_table");
    }

    #[test]
    fn table_name_with_spaces() {
        let mut catalog = InMemoryCatalog::new();
        let meta = TableMetadata::new("my table name", make_schema());
        catalog.register_table(meta).unwrap();
        let table = catalog.get_table("my table name").unwrap();
        assert_eq!(table.name(), "my table name");
    }

    #[test]
    fn schema_name_with_special_characters() {
        let mut registry = InMemorySchemaRegistry::new();
        let schema = TableSchema::new(vec![CatalogField::new("col", FieldType::Int32, true)]);
        registry.register_schema("schema-with-dashes", schema);
        let retrieved = registry.get_schema("schema-with-dashes").unwrap();
        assert_eq!(retrieved.field_count(), 1);
    }

    #[test]
    fn field_name_with_special_characters() {
        let f = CatalogField::new("field-with-dots_and@spaces", FieldType::Utf8, false);
        assert_eq!(f.name(), "field-with-dots_and@spaces");
        let arrow_f = f.to_arrow_field();
        assert_eq!(arrow_f.name(), "field-with-dots_and@spaces");
    }

    // -----------------------------------------------------------------------
    // Edge cases: duplicate registration and overwrite
    // -----------------------------------------------------------------------

    #[test]
    fn catalog_duplicate_different_table_errors() {
        let mut catalog = InMemoryCatalog::new();
        catalog
            .register_table(TableMetadata::new("t1", make_schema()))
            .unwrap();
        catalog
            .register_table(TableMetadata::new("t2", make_schema()))
            .unwrap();
        assert_eq!(catalog.list_tables().len(), 2);
    }

    #[test]
    fn schema_registry_overwrite_preserves_single_entry() {
        let mut registry = InMemorySchemaRegistry::new();
        registry.register_schema("s", TableSchema::empty());
        registry.register_schema("s", make_schema());
        assert_eq!(registry.schema_names().len(), 1);
        assert_eq!(registry.get_schema("s").unwrap().field_count(), 2);
    }

    // -----------------------------------------------------------------------
    // TableSchema: empty schema arrow conversion
    // -----------------------------------------------------------------------

    #[test]
    fn empty_schema_to_arrow() {
        let schema = TableSchema::empty();
        let arrow_schema = schema.to_arrow_schema();
        assert_eq!(arrow_schema.fields().len(), 0);
    }

    #[test]
    fn single_field_schema_to_arrow() {
        let schema = TableSchema::new(vec![CatalogField::new("only", FieldType::Float32, true)]);
        let arrow_schema = schema.to_arrow_schema();
        assert_eq!(arrow_schema.fields().len(), 1);
        let f = arrow_schema.field_with_name("only").unwrap();
        assert_eq!(f.data_type(), &arrow::datatypes::DataType::Float32);
        assert!(f.is_nullable());
    }

    // -----------------------------------------------------------------------
    // FieldType: nested types to_arrow
    // -----------------------------------------------------------------------

    #[test]
    fn field_type_list_of_list() {
        let inner = FieldType::List(Box::new(FieldType::Int32));
        let outer = FieldType::List(Box::new(inner));
        match outer.to_arrow() {
            arrow::datatypes::DataType::List(field) => match field.data_type() {
                arrow::datatypes::DataType::List(inner_field) => {
                    assert_eq!(inner_field.data_type(), &arrow::datatypes::DataType::Int32);
                }
                other => panic!("expected nested List, got {other:?}"),
            },
            other => panic!("expected outer List, got {other:?}"),
        }
    }

    #[test]
    fn field_type_struct_nested_in_struct() {
        let inner_struct =
            FieldType::Struct(vec![CatalogField::new("a", FieldType::Boolean, true)]);
        let outer_struct = FieldType::Struct(vec![
            CatalogField::new("nested", inner_struct, false),
            CatalogField::new("simple", FieldType::Utf8, true),
        ]);
        match outer_struct.to_arrow() {
            arrow::datatypes::DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                match fields[0].data_type() {
                    arrow::datatypes::DataType::Struct(inner_fields) => {
                        assert_eq!(inner_fields.len(), 1);
                        assert_eq!(inner_fields[0].name(), "a");
                    }
                    other => panic!("expected inner Struct, got {other:?}"),
                }
                assert_eq!(fields[1].data_type(), &arrow::datatypes::DataType::Utf8);
            }
            other => panic!("expected Struct, got {other:?}"),
        }
    }

    #[test]
    fn field_type_list_of_struct() {
        let list_type = FieldType::List(Box::new(FieldType::Struct(vec![
            CatalogField::new("x", FieldType::Int64, false),
            CatalogField::new("y", FieldType::Utf8, true),
        ])));
        match list_type.to_arrow() {
            arrow::datatypes::DataType::List(item_field) => match item_field.data_type() {
                arrow::datatypes::DataType::Struct(fields) => {
                    assert_eq!(fields.len(), 2);
                }
                other => panic!("expected inner Struct, got {other:?}"),
            },
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn field_type_empty_struct() {
        let empty_struct = FieldType::Struct(vec![]);
        match empty_struct.to_arrow() {
            arrow::datatypes::DataType::Struct(fields) => {
                assert_eq!(fields.len(), 0);
            }
            other => panic!("expected Struct, got {other:?}"),
        }
    }

    #[test]
    fn field_type_list_of_binary() {
        let list_type = FieldType::List(Box::new(FieldType::Binary));
        match list_type.to_arrow() {
            arrow::datatypes::DataType::List(field) => {
                assert_eq!(field.data_type(), &arrow::datatypes::DataType::Binary);
            }
            other => panic!("expected List, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // CatalogField: Clone and PartialEq
    // -----------------------------------------------------------------------

    #[test]
    fn catalog_field_clone_eq() {
        let f1 = CatalogField::new("col", FieldType::Int32, true);
        let f2 = f1.clone();
        assert_eq!(f1, f2);
    }

    #[test]
    fn catalog_field_ne_name() {
        let f1 = CatalogField::new("a", FieldType::Int32, true);
        let f2 = CatalogField::new("b", FieldType::Int32, true);
        assert_ne!(f1, f2);
    }

    #[test]
    fn catalog_field_ne_type() {
        let f1 = CatalogField::new("a", FieldType::Int32, true);
        let f2 = CatalogField::new("a", FieldType::Utf8, true);
        assert_ne!(f1, f2);
    }

    #[test]
    fn catalog_field_ne_nullable() {
        let f1 = CatalogField::new("a", FieldType::Int32, true);
        let f2 = CatalogField::new("a", FieldType::Int32, false);
        assert_ne!(f1, f2);
    }

    // -----------------------------------------------------------------------
    // TableSchema: Clone and PartialEq
    // -----------------------------------------------------------------------

    #[test]
    fn table_schema_clone_eq() {
        let s1 = make_schema();
        let s2 = s1.clone();
        assert_eq!(s1, s2);
    }

    #[test]
    fn table_schema_ne_different_fields() {
        let s1 = TableSchema::new(vec![CatalogField::new("a", FieldType::Int32, false)]);
        let s2 = TableSchema::new(vec![CatalogField::new("b", FieldType::Int32, false)]);
        assert_ne!(s1, s2);
    }

    // -----------------------------------------------------------------------
    // TableMetadata: Clone
    // -----------------------------------------------------------------------

    #[test]
    fn table_metadata_clone() {
        let meta = TableMetadata::new("t", make_schema())
            .with_stats(ColumnStatistics::new().with_row_count(100));
        let cloned = meta.clone();
        assert_eq!(cloned.name(), "t");
        assert_eq!(cloned.statistics().unwrap().row_count, Some(100));
    }

    // -----------------------------------------------------------------------
    // InMemoryCatalog: register_table_with_batches
    // -----------------------------------------------------------------------

    #[test]
    fn register_table_with_batches_stores_data() {
        let mut catalog = InMemoryCatalog::new();
        let schema = TableSchema::new(vec![CatalogField::new("id", FieldType::Int64, false)]);
        let arrow_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            arrow_schema,
            vec![std::sync::Arc::new(arrow::array::Int64Array::from(vec![
                1, 2, 3,
            ]))],
        )
        .unwrap();
        catalog
            .register_table_with_batches(TableMetadata::new("data", schema), vec![batch])
            .unwrap();
        assert!(catalog.table_batches("data").is_some());
        assert_eq!(catalog.table_batches("data").unwrap().len(), 1);
        assert_eq!(catalog.table_batches("data").unwrap()[0].num_rows(), 3);
    }

    #[test]
    fn register_table_with_empty_batches_no_data() {
        let mut catalog = InMemoryCatalog::new();
        let schema = TableSchema::new(vec![CatalogField::new("id", FieldType::Int32, false)]);
        catalog
            .register_table_with_batches(TableMetadata::new("empty", schema), vec![])
            .unwrap();
        assert!(catalog.table_batches("empty").is_none());
    }

    #[test]
    fn table_batches_nonexistent_table() {
        let catalog = InMemoryCatalog::new();
        assert!(catalog.table_batches("nope").is_none());
    }

    // -----------------------------------------------------------------------
    // InMemoryCatalog: Default
    // -----------------------------------------------------------------------

    #[test]
    fn in_memory_catalog_default() {
        let catalog = InMemoryCatalog::default();
        assert!(catalog.list_tables().is_empty());
    }

    // -----------------------------------------------------------------------
    // InMemorySchemaRegistry: Default
    // -----------------------------------------------------------------------

    #[test]
    fn in_memory_schema_registry_default() {
        let registry = InMemorySchemaRegistry::default();
        assert!(registry.schema_names().is_empty());
    }

    // -----------------------------------------------------------------------
    // DataFusion bridge: SQL with empty table
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn datafusion_bridge_empty_table_query() {
        use datafusion::prelude::SessionContext;

        let catalog = std::sync::Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        {
            let mut cat = catalog.write().unwrap();
            let schema = TableSchema::new(vec![CatalogField::new("id", FieldType::Int64, false)]);
            cat.register_table(TableMetadata::new("empty_table", schema))
                .unwrap();
        }
        let ctx = SessionContext::new();
        ctx.register_catalog(
            "krishiv",
            std::sync::Arc::new(crate::datafusion_bridge::DataFusionCatalogBridge::new(
                catalog,
            )),
        );
        let df = ctx
            .sql("SELECT * FROM krishiv.public.empty_table")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 0);
    }

    #[tokio::test]
    async fn datafusion_bridge_sql_filter() {
        use std::sync::Arc;

        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::prelude::SessionContext;

        let catalog = Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        let schema = TableSchema::new(vec![
            CatalogField::new("id", FieldType::Int64, false),
            CatalogField::new("val", FieldType::Int64, false),
        ]);
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        catalog
            .write()
            .unwrap()
            .register_table_with_batches(TableMetadata::new("nums", schema), vec![batch])
            .unwrap();

        let ctx = SessionContext::new();
        ctx.register_catalog(
            "krishiv",
            Arc::new(crate::datafusion_bridge::DataFusionCatalogBridge::new(
                catalog,
            )),
        );
        let df = ctx
            .sql("SELECT id FROM krishiv.public.nums WHERE val > 15")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
    }

    #[tokio::test]
    async fn datafusion_bridge_sql_count_aggregate() {
        use std::sync::Arc;

        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::prelude::SessionContext;

        let catalog = Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        let schema = TableSchema::new(vec![CatalogField::new("x", FieldType::Int32, false)]);
        let arrow_schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(arrow::array::Int32Array::from(vec![
                1, 2, 3, 4, 5,
            ]))],
        )
        .unwrap();
        catalog
            .write()
            .unwrap()
            .register_table_with_batches(TableMetadata::new("agg", schema), vec![batch])
            .unwrap();

        let ctx = SessionContext::new();
        ctx.register_catalog(
            "krishiv",
            Arc::new(crate::datafusion_bridge::DataFusionCatalogBridge::new(
                catalog,
            )),
        );
        let df = ctx
            .sql("SELECT COUNT(*) AS cnt FROM krishiv.public.agg")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    // -----------------------------------------------------------------------
    // DataFusion bridge: multiple tables
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn datafusion_bridge_multiple_tables() {
        use std::sync::Arc;

        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::prelude::SessionContext;

        let catalog = Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        let schema = TableSchema::new(vec![CatalogField::new("id", FieldType::Int64, false)]);
        let arrow_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        {
            let mut cat = catalog.write().unwrap();
            cat.register_table_with_batches(
                TableMetadata::new("t1", schema.clone()),
                vec![batch.clone()],
            )
            .unwrap();
            cat.register_table_with_batches(TableMetadata::new("t2", schema), vec![batch])
                .unwrap();
        }

        let ctx = SessionContext::new();
        ctx.register_catalog(
            "krishiv",
            Arc::new(crate::datafusion_bridge::DataFusionCatalogBridge::new(
                catalog,
            )),
        );
        let df1 = ctx.sql("SELECT * FROM krishiv.public.t1").await.unwrap();
        let batches1 = df1.collect().await.unwrap();
        assert_eq!(batches1.len(), 1);
        assert_eq!(batches1[0].num_rows(), 1);

        let df2 = ctx.sql("SELECT * FROM krishiv.public.t2").await.unwrap();
        let batches2 = df2.collect().await.unwrap();
        assert_eq!(batches2.len(), 1);
        assert_eq!(batches2[0].num_rows(), 1);
    }

    // -----------------------------------------------------------------------
    // DataFusion bridge: schema() returns None for non-public
    // -----------------------------------------------------------------------

    #[test]
    fn datafusion_bridge_custom_schema_name_returns_none() {
        use datafusion::catalog::CatalogProvider as DfCatalogProvider;

        let catalog = std::sync::Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        let bridge = crate::datafusion_bridge::DataFusionCatalogBridge::new(catalog);
        assert!(bridge.schema("custom").is_none());
        assert!(bridge.schema("public").is_some());
    }

    // -----------------------------------------------------------------------
    // DataFusion bridge: as_any returns self
    // -----------------------------------------------------------------------

    #[test]
    fn datafusion_bridge_as_any() {
        use datafusion::catalog::CatalogProvider as DfCatalogProvider;

        let catalog = std::sync::Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        let bridge = crate::datafusion_bridge::DataFusionCatalogBridge::new(catalog);
        assert!(
            bridge
                .as_any()
                .downcast_ref::<crate::datafusion_bridge::DataFusionCatalogBridge>()
                .is_some()
        );
    }

    // -----------------------------------------------------------------------
    // DataFusion bridge: schema_names returns single "public"
    // -----------------------------------------------------------------------

    #[test]
    fn datafusion_bridge_only_public_schema() {
        use datafusion::catalog::CatalogProvider as DfCatalogProvider;

        let catalog = std::sync::Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        let bridge = crate::datafusion_bridge::DataFusionCatalogBridge::new(catalog);
        let names = bridge.schema_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], "public");
    }

    // -----------------------------------------------------------------------
    // FieldType Display: empty struct and empty list
    // -----------------------------------------------------------------------

    #[test]
    fn field_type_display_empty_struct() {
        let s = FieldType::Struct(vec![]);
        assert_eq!(s.to_string(), "Struct(0 fields)");
    }

    #[test]
    fn field_type_display_nested_list() {
        let inner = FieldType::List(Box::new(FieldType::Int32));
        let outer = FieldType::List(Box::new(inner));
        assert_eq!(outer.to_string(), "List<List<Int32>>");
    }

    // -----------------------------------------------------------------------
    // ColumnStatistics: eq with None fields
    // -----------------------------------------------------------------------

    #[test]
    fn column_statistics_eq_all_none() {
        let a = ColumnStatistics::new();
        let b = ColumnStatistics::new();
        assert_eq!(a, b);
    }

    #[test]
    fn column_statistics_ne_different_min() {
        let a = ColumnStatistics::new().with_min("aaa");
        let b = ColumnStatistics::new().with_min("zzz");
        assert_ne!(a, b);
    }

    // -----------------------------------------------------------------------
    // TableSchema: get_field with multiple fields
    // -----------------------------------------------------------------------

    #[test]
    fn table_schema_get_field_last() {
        let schema = TableSchema::new(vec![
            CatalogField::new("a", FieldType::Int32, false),
            CatalogField::new("b", FieldType::Utf8, true),
            CatalogField::new("c", FieldType::Float64, false),
        ]);
        let field = schema.get_field("c").unwrap();
        assert_eq!(field.field_type(), &FieldType::Float64);
    }

    #[test]
    fn table_schema_get_field_middle() {
        let schema = TableSchema::new(vec![
            CatalogField::new("a", FieldType::Int32, false),
            CatalogField::new("b", FieldType::Utf8, true),
            CatalogField::new("c", FieldType::Float64, false),
        ]);
        let field = schema.get_field("b").unwrap();
        assert_eq!(field.field_type(), &FieldType::Utf8);
        assert!(field.nullable());
    }

    // -----------------------------------------------------------------------
    // CatalogError: all variants as std::error::Error
    // -----------------------------------------------------------------------

    #[test]
    fn all_catalog_errors_are_std_error() {
        let errors: Vec<CatalogError> = vec![
            CatalogError::TableNotFound { name: "a".into() },
            CatalogError::TableAlreadyExists { name: "b".into() },
            CatalogError::SchemaNotFound { name: "c".into() },
            CatalogError::InvalidSchema {
                message: "d".into(),
            },
            CatalogError::InvalidConfiguration {
                message: "bad URL".into(),
            },
            CatalogError::Transport {
                operation: "load table".into(),
                message: "timed out".into(),
            },
            CatalogError::Http {
                status: 500,
                message: "e".into(),
            },
            CatalogError::InvalidResponse {
                operation: "list tables".into(),
                message: "missing identifiers".into(),
            },
            CatalogError::ResponseTooLarge {
                operation: "load table".into(),
                limit_bytes: 1024,
            },
            CatalogError::UnsupportedOperation {
                operation: "committing a table".into(),
            },
        ];
        for err in errors {
            let e: &dyn std::error::Error = &err;
            let _ = e.to_string();
            assert!(e.source().is_none());
        }
    }

    // -----------------------------------------------------------------------
    // InMemoryCatalog: register many tables
    // -----------------------------------------------------------------------

    #[test]
    fn in_memory_catalog_many_tables() {
        let mut catalog = InMemoryCatalog::new();
        for i in 0..100 {
            catalog
                .register_table(TableMetadata::new(format!("table_{i:03}"), make_schema()))
                .unwrap();
        }
        assert_eq!(catalog.list_tables().len(), 100);
        assert!(catalog.get_table("table_000").is_ok());
        assert!(catalog.get_table("table_099").is_ok());
        assert!(catalog.get_table("table_100").is_err());
    }

    // -----------------------------------------------------------------------
    // DataFusion bridge: table() returns None for unknown
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn datafusion_bridge_table_unknown() {
        use datafusion::catalog::CatalogProvider as DfCatalogProvider;
        use std::sync::Arc;

        let catalog = Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        let bridge = crate::datafusion_bridge::DataFusionCatalogBridge::new(catalog);
        let schema_provider = bridge.schema("public").unwrap();
        let result = schema_provider.table("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // DataFusion bridge: table() returns Some for known table
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn datafusion_bridge_table_known() {
        use datafusion::catalog::CatalogProvider as DfCatalogProvider;
        use std::sync::Arc;

        let catalog = Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        {
            let mut cat = catalog.write().unwrap();
            cat.register_table(TableMetadata::new("mytable", make_schema()))
                .unwrap();
        }
        let bridge = crate::datafusion_bridge::DataFusionCatalogBridge::new(catalog);
        let schema_provider = bridge.schema("public").unwrap();
        let result = schema_provider.table("mytable").await.unwrap();
        assert!(result.is_some());
    }

    // -----------------------------------------------------------------------
    // DataFusion bridge: table_names lists registered tables
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn datafusion_bridge_table_names() {
        use datafusion::catalog::CatalogProvider as DfCatalogProvider;
        use std::sync::Arc;

        let catalog = Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        {
            let mut cat = catalog.write().unwrap();
            cat.register_table(TableMetadata::new("alpha", make_schema()))
                .unwrap();
            cat.register_table(TableMetadata::new("beta", make_schema()))
                .unwrap();
        }
        let bridge = crate::datafusion_bridge::DataFusionCatalogBridge::new(catalog);
        let schema_provider = bridge.schema("public").unwrap();
        let mut names = schema_provider.table_names();
        names.sort();
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    // -----------------------------------------------------------------------
    // DataFusion bridge: empty catalog table_names
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn datafusion_bridge_empty_table_names() {
        use datafusion::catalog::CatalogProvider as DfCatalogProvider;
        use std::sync::Arc;

        let catalog = Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        let bridge = crate::datafusion_bridge::DataFusionCatalogBridge::new(catalog);
        let schema_provider = bridge.schema("public").unwrap();
        let names = schema_provider.table_names();
        assert!(names.is_empty());
    }

    // -----------------------------------------------------------------------
    // DataFusion bridge: table_exist with multiple tables
    // -----------------------------------------------------------------------

    #[test]
    fn datafusion_bridge_table_exist_multiple() {
        use datafusion::catalog::CatalogProvider as DfCatalogProvider;

        let catalog = std::sync::Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        {
            let mut cat = catalog.write().unwrap();
            cat.register_table(TableMetadata::new("a", make_schema()))
                .unwrap();
            cat.register_table(TableMetadata::new("b", make_schema()))
                .unwrap();
        }
        let bridge = crate::datafusion_bridge::DataFusionCatalogBridge::new(catalog);
        let sp = bridge.schema("public").unwrap();
        assert!(sp.table_exist("a"));
        assert!(sp.table_exist("b"));
        assert!(!sp.table_exist("c"));
    }

    // -----------------------------------------------------------------------
    // DataFusion bridge: debug format
    // -----------------------------------------------------------------------

    #[test]
    fn datafusion_bridge_debug() {
        let catalog = std::sync::Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        let bridge = crate::datafusion_bridge::DataFusionCatalogBridge::new(catalog);
        let dbg = format!("{bridge:?}");
        assert!(dbg.contains("DataFusionCatalogBridge"));
    }

    // -----------------------------------------------------------------------
    // InMemoryCatalog: register_table_with_batches error on duplicate
    // -----------------------------------------------------------------------

    #[test]
    fn register_table_with_batches_duplicate_errors() {
        let mut catalog = InMemoryCatalog::new();
        let schema = TableSchema::new(vec![CatalogField::new("id", FieldType::Int32, false)]);
        let arrow_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            arrow_schema,
            vec![std::sync::Arc::new(arrow::array::Int32Array::from(vec![1]))],
        )
        .unwrap();
        catalog
            .register_table_with_batches(TableMetadata::new("t", schema.clone()), vec![batch])
            .unwrap();
        let err = catalog
            .register_table_with_batches(TableMetadata::new("t", schema), vec![])
            .unwrap_err();
        assert!(matches!(err, CatalogError::TableAlreadyExists { .. }));
    }

    // -----------------------------------------------------------------------
    // DataFusion bridge: SQL with multiple columns and types
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn datafusion_bridge_sql_multiple_columns() {
        use std::sync::Arc;

        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::prelude::SessionContext;

        let catalog = Arc::new(std::sync::RwLock::new(InMemoryCatalog::new()));
        let schema = TableSchema::new(vec![
            CatalogField::new("id", FieldType::Int32, false),
            CatalogField::new("name", FieldType::Utf8, true),
        ]);
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        catalog
            .write()
            .unwrap()
            .register_table_with_batches(TableMetadata::new("mixed", schema), vec![batch])
            .unwrap();

        let ctx = SessionContext::new();
        ctx.register_catalog(
            "krishiv",
            Arc::new(crate::datafusion_bridge::DataFusionCatalogBridge::new(
                catalog,
            )),
        );
        let df = ctx
            .sql("SELECT name FROM krishiv.public.mixed WHERE id > 1")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
    }

    // -----------------------------------------------------------------------
    // CatalogError: Display for all variants via format
    // -----------------------------------------------------------------------

    #[test]
    fn catalog_error_display_all_variants() {
        let cases: Vec<(CatalogError, &str)> = vec![
            (
                CatalogError::TableNotFound {
                    name: "x".to_string(),
                },
                "table not found: 'x'",
            ),
            (
                CatalogError::TableAlreadyExists {
                    name: "y".to_string(),
                },
                "table already exists: 'y'",
            ),
            (
                CatalogError::SchemaNotFound {
                    name: "z".to_string(),
                },
                "schema not found: 'z'",
            ),
            (
                CatalogError::InvalidSchema {
                    message: "bad".to_string(),
                },
                "invalid schema: bad",
            ),
            (
                CatalogError::Http {
                    status: 403,
                    message: "forbidden".to_string(),
                },
                "HTTP error 403: forbidden",
            ),
            (
                CatalogError::InvalidConfiguration {
                    message: "bad URL".to_string(),
                },
                "invalid catalog configuration: bad URL",
            ),
            (
                CatalogError::Transport {
                    operation: "list tables".to_string(),
                    message: "timed out".to_string(),
                },
                "catalog transport error during list tables: timed out",
            ),
            (
                CatalogError::InvalidResponse {
                    operation: "load table".to_string(),
                    message: "missing metadata".to_string(),
                },
                "invalid catalog response during load table: missing metadata",
            ),
            (
                CatalogError::ResponseTooLarge {
                    operation: "load table".to_string(),
                    limit_bytes: 4096,
                },
                "catalog response during load table exceeded 4096 bytes",
            ),
            (
                CatalogError::UnsupportedOperation {
                    operation: "committing a table".to_string(),
                },
                "catalog server does not support committing a table",
            ),
        ];
        for (err, expected) in cases {
            assert_eq!(err.to_string(), expected);
        }
    }
}
