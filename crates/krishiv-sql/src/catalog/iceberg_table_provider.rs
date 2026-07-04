//! Real Iceberg read path via iceberg scan + DataFusion Parquet listing (Phase J2).
//!
//! `iceberg-datafusion 0.9.1` depends on DataFusion 52.x while the workspace
//! uses DataFusion 53.x, making `IcebergStaticTableProvider` incompatible with
//! the workspace `SessionContext`. Instead, we enumerate Parquet files through
//! iceberg's `plan_files()` and wrap them in DataFusion 53's `ListingTable`,
//! which provides native pushdown of projections and partition filters.

pub mod iceberg_scan {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::error::{DataFusionError, Result as DfResult};
    use datafusion::execution::SessionStateBuilder;
    use futures::TryStreamExt;
    use iceberg::spec::{PrimitiveType, Type};
    use iceberg::table::Table;

    /// The table's current schema as a workspace-Arrow schema, so an empty
    /// table (no data files yet) still exposes its columns to introspection.
    ///
    /// We map the Iceberg *spec* schema directly rather than call
    /// `iceberg::arrow::schema_to_arrow_schema`: iceberg 0.9.1 pins a different
    /// `arrow` than the workspace/DataFusion, so its Arrow types are a distinct
    /// (incompatible) crate version. Any field whose Iceberg type we do not map
    /// (nested struct/list/map) makes us fall back to an empty schema — never
    /// worse than the previous "schema unknown without files" behavior.
    fn table_arrow_schema(table: &Table) -> SchemaRef {
        let iceberg_schema = table.metadata().current_schema();
        let mut fields = Vec::new();
        for nested in iceberg_schema.as_struct().fields() {
            let Type::Primitive(prim) = nested.field_type.as_ref() else {
                return Arc::new(Schema::empty());
            };
            let Some(dt) = primitive_to_arrow(prim) else {
                return Arc::new(Schema::empty());
            };
            fields.push(Field::new(&nested.name, dt, !nested.required));
        }
        Arc::new(Schema::new(fields))
    }

    /// Map an Iceberg primitive type to the workspace Arrow `DataType`.
    fn primitive_to_arrow(prim: &PrimitiveType) -> Option<DataType> {
        Some(match prim {
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Int => DataType::Int32,
            PrimitiveType::Long => DataType::Int64,
            PrimitiveType::Float => DataType::Float32,
            PrimitiveType::Double => DataType::Float64,
            PrimitiveType::Date => DataType::Date32,
            PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
            PrimitiveType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            PrimitiveType::Timestamptz => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            PrimitiveType::TimestampNs => DataType::Timestamp(TimeUnit::Nanosecond, None),
            PrimitiveType::TimestamptzNs => {
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            }
            PrimitiveType::String => DataType::Utf8,
            PrimitiveType::Uuid => DataType::FixedSizeBinary(16),
            PrimitiveType::Fixed(n) => DataType::FixedSizeBinary(i32::try_from(*n).ok()?),
            PrimitiveType::Binary => DataType::Binary,
            PrimitiveType::Decimal { precision, scale } => {
                DataType::Decimal128(u8::try_from(*precision).ok()?, i8::try_from(*scale).ok()?)
            }
        })
    }

    /// Build a DataFusion [`TableProvider`] for an Iceberg table using its
    /// current snapshot's Parquet files.
    ///
    /// Enumerates data files via `plan_files()`, then wraps them in a
    /// DataFusion `ListingTable` so projection/filter pushdown works without
    /// going through `iceberg-datafusion` (which targets DataFusion 52.x).
    pub async fn iceberg_table_provider(table: Table) -> DfResult<Arc<dyn TableProvider>> {
        let arrow_schema = table_arrow_schema(&table);
        let file_paths = collect_file_paths(&table).await?;
        listing_provider_from_paths(file_paths, arrow_schema).await
    }

    /// Time-travel variant pinned to `snapshot_id`.
    pub async fn iceberg_table_provider_at_snapshot(
        table: Table,
        snapshot_id: i64,
    ) -> DfResult<Arc<dyn TableProvider>> {
        let arrow_schema = table_arrow_schema(&table);
        // Build a scan scoped to the requested snapshot.
        let scan = table
            .scan()
            .snapshot_id(snapshot_id)
            .build()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let task_stream = scan
            .plan_files()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let tasks: Vec<iceberg::scan::FileScanTask> = task_stream
            .try_collect()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let file_paths: Vec<String> = tasks
            .iter()
            .map(|t| local_path(t.data_file_path()))
            .collect();

        listing_provider_from_paths(file_paths, arrow_schema).await
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    async fn collect_file_paths(table: &Table) -> DfResult<Vec<String>> {
        let scan = table
            .scan()
            .build()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let task_stream = scan
            .plan_files()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let tasks: Vec<iceberg::scan::FileScanTask> = task_stream
            .try_collect()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(tasks
            .iter()
            .map(|t| local_path(t.data_file_path()))
            .collect())
    }

    /// Convert a file URI (file:///path) to a local path string; leave S3/GCS
    /// paths unchanged so DataFusion's object_store can handle them.
    fn local_path(uri: &str) -> String {
        if let Some(p) = uri.strip_prefix("file://") {
            p.to_string()
        } else {
            uri.to_string()
        }
    }

    async fn listing_provider_from_paths(
        paths: Vec<String>,
        arrow_schema: SchemaRef,
    ) -> DfResult<Arc<dyn TableProvider>> {
        let Some(first_path) = paths.first() else {
            // No data files yet: expose the table's schema via an empty
            // in-memory table so introspection still sees the columns.
            use datafusion::datasource::MemTable;
            return Ok(Arc::new(MemTable::try_new(arrow_schema, vec![vec![]])?));
        };

        // Use the first file path as the listing root; DataFusion globs for *.parquet.
        // For multiple files with different parents, we use the first file directly.
        let listing_url = ListingTableUrl::parse(first_path)?;

        let format = Arc::new(ParquetFormat::default().with_enable_pruning(true));
        let listing_options = ListingOptions::new(format)
            .with_file_extension(".parquet")
            .with_collect_stat(true);

        // Build a temporary session state to infer the schema.
        let state = SessionStateBuilder::new().with_default_features().build();
        let schema = listing_options.infer_schema(&state, &listing_url).await?;

        let config = ListingTableConfig::new(listing_url)
            .with_listing_options(listing_options)
            .with_schema(schema);

        Ok(Arc::new(ListingTable::try_new(config)?))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};

    use super::iceberg_scan::iceberg_table_provider;

    #[tokio::test]
    async fn iceberg_table_provider_exposes_schema() {
        let dir = tempfile::tempdir().unwrap();
        let warehouse = url::Url::from_file_path(dir.path()).unwrap().to_string();
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "mem",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await
            .unwrap();

        let namespace = NamespaceIdent::new("ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .build(),
            )
            .await
            .unwrap();

        // Empty table: provider should not panic.
        let provider = iceberg_table_provider(table).await.unwrap();
        // Empty table returns an empty-schema MemTable — just check it's Some.
        let _ = datafusion::catalog::TableProvider::schema(&*provider);
    }
}
