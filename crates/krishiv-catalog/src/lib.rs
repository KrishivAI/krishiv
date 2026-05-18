#![forbid(unsafe_code)]

//! Catalog abstractions for Krishiv.
//!
//! This crate defines `TableProvider`, `CatalogProvider`, schema types, and
//! column statistics. An in-memory reference implementation is included.

use std::collections::BTreeMap;
use std::fmt;

// ---------------------------------------------------------------------------
// Error and Result
// ---------------------------------------------------------------------------

/// Errors produced by catalog operations.
#[derive(Debug)]
pub enum CatalogError {
    /// A requested table was not found in the catalog.
    TableNotFound { name: String },
    /// A requested schema was not found.
    SchemaNotFound { name: String },
    /// The provided schema is structurally invalid.
    InvalidSchema { message: String },
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CatalogError::TableNotFound { name } => {
                write!(f, "table not found: '{name}'")
            }
            CatalogError::SchemaNotFound { name } => {
                write!(f, "schema not found: '{name}'")
            }
            CatalogError::InvalidSchema { message } => {
                write!(f, "invalid schema: {message}")
            }
        }
    }
}

impl std::error::Error for CatalogError {}

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
    List,
    Struct,
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
            FieldType::List => "List",
            FieldType::Struct => "Struct",
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
            FieldType::List => {
                // A List without an explicit item type; use Int64 as a default.
                DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::Int64,
                    true,
                )))
            }
            FieldType::Struct => {
                // An empty Struct; callers with nested fields should build the
                // Arrow schema directly.
                DataType::Struct(arrow::datatypes::Fields::empty())
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
}

impl InMemoryCatalog {
    /// Create a new, empty in-memory catalog.
    pub fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
        }
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
        self.tables.insert(name, TableMetadataProvider { metadata });
        Ok(())
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
}
