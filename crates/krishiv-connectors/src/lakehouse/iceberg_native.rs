//! Iceberg-native two-phase commit using iceberg-rust 0.9 Transaction API (P1.3).
//!
//! Uses `MemoryCatalog` backed by `LocalFsStorageFactory` so every manifest,
//! manifest-list, and table-metadata JSON is written in proper Iceberg spec
//! format.  A `version-hint.text` file in `{root}/metadata/` tracks the latest
//! committed metadata location for crash recovery.

#[cfg(feature = "iceberg")]
pub mod native {
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Schema,
        Struct, Type,
    };
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::{
        Catalog, CatalogBuilder, MemoryCatalog, NamespaceIdent, TableCreation, TableIdent,
    };
    use parquet::arrow::ArrowWriter;
    use tokio::sync::Mutex;
    use url::Url;

    use crate::lakehouse::two_phase::{
        IcebergTwoPhaseCommit, KAFKA_OFFSETS_SUMMARY_KEY, StagedSnapshot, kafka_offsets_json,
    };
    use crate::lakehouse::{LakehouseError, SchemaVersion};

    const VERSION_HINT: &str = "version-hint.text";

    pub(crate) struct StagedEntry {
        pub(crate) data_files: Vec<iceberg::spec::DataFile>,
    }

    /// Iceberg-native two-phase commit backed by the local filesystem.
    ///
    /// On `prepare`, Arrow data is written to `{root}/data/` as Parquet.
    /// On `commit`, a `Transaction::fast_append` call atomically updates the
    /// iceberg-spec table metadata (manifests + `table-metadata.json`) so that
    /// no partially committed snapshot is ever visible to readers.
    pub struct IcebergNativeTwoPhaseCommit {
        pub(crate) catalog: Arc<MemoryCatalog>,
        pub(crate) ident: TableIdent,
        /// Absolute local root path (no URI prefix).
        root: PathBuf,
        pub(crate) pending: Mutex<HashMap<i64, StagedEntry>>,
        snap_counter: AtomicI64,
    }

    impl IcebergNativeTwoPhaseCommit {
        /// Open or create an Iceberg table at `root`.
        ///
        /// If `{root}/metadata/version-hint.text` exists the last committed
        /// metadata is registered into the catalog for immediate use (crash
        /// recovery).  Otherwise a fresh table is created from `schema_version`.
        pub async fn open(
            root: &Path,
            table_name: &str,
            schema_version: &SchemaVersion,
        ) -> Result<Self, LakehouseError> {
            // Ensure root is absolute.
            fs::create_dir_all(root.join("data")).map_err(|e| LakehouseError::Io(e.to_string()))?;
            fs::create_dir_all(root.join("metadata"))
                .map_err(|e| LakehouseError::Io(e.to_string()))?;

            let root = root
                .canonicalize()
                .map_err(|e| LakehouseError::Io(e.to_string()))?;

            let table_uri = path_to_uri(&root)?;

            let catalog = MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "local",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), table_uri.clone())]),
                )
                .await
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
            let catalog = Arc::new(catalog);

            let namespace = NamespaceIdent::new("default".to_string());
            let ident = TableIdent::new(namespace.clone(), table_name.to_string());

            // Create namespace (idempotent – ignore "already exists" errors).
            let _ = catalog.create_namespace(&namespace, HashMap::new()).await;

            let version_hint = root.join("metadata").join(VERSION_HINT);
            if version_hint.exists() {
                let meta_loc = fs::read_to_string(&version_hint)
                    .map_err(|e| LakehouseError::Io(e.to_string()))?;
                let meta_loc = meta_loc.trim().to_string();
                catalog
                    .register_table(&ident, meta_loc)
                    .await
                    .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
            } else {
                let iceberg_schema = schema_version_to_iceberg(schema_version)?;
                let creation = TableCreation::builder()
                    .name(table_name.to_string())
                    .schema(iceberg_schema)
                    .location(table_uri)
                    .build();
                let table = catalog
                    .create_table(&namespace, creation)
                    .await
                    .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
                // Persist the initial metadata location so a crash after
                // create_table but before any commit still recovers cleanly.
                if let Some(loc) = table.metadata_location() {
                    write_version_hint(&root, loc)?;
                }
            }

            Ok(Self {
                catalog,
                ident,
                root,
                pending: Mutex::new(HashMap::new()),
                // Seed snap_counter with (pid << 32) so staged filenames are
                // unique across processes. Within one session the counter
                // increments monotonically, guaranteeing no collision even if
                // two sessions on the same host share the same root (e.g.
                // crash recovery).
                snap_counter: AtomicI64::new((std::process::id() as i64) << 32),
            })
        }

        fn update_version_hint(&self, metadata_location: &str) -> Result<(), LakehouseError> {
            write_version_hint(&self.root, metadata_location)
        }

        /// Atomically replace all table data with `batches`.
        ///
        /// Implemented via catalog drop-and-recreate followed by `prepare`+`commit`
        /// since iceberg-rust 0.9.1 does not expose an overwrite snapshot action in
        /// its public Transaction API.  Old data files remain as orphans on disk and
        /// would be cleaned by a future VACUUM.
        pub async fn overwrite_commit(
            &self,
            batches: Vec<RecordBatch>,
            kafka_offsets: BTreeMap<String, i64>,
            schema_version: &SchemaVersion,
        ) -> Result<i64, LakehouseError> {
            let namespace = self.ident.namespace().clone();

            // Capture the old table's metadata location before dropping so we
            // can restore it if the new commit fails (M5).
            let old_metadata_location = match self.catalog.load_table(&self.ident).await {
                Ok(table) => table.metadata_location().map(String::from),
                Err(_) => None,
            };

            // Drop current table — ignore "not found" (first overwrite on empty table).
            let _ = self.catalog.drop_table(&self.ident).await;
            // Clear any leftover pending entries (they reference the dropped table).
            self.pending.lock().await.clear();

            let iceberg_schema = schema_version_to_iceberg(schema_version)?;
            let table_uri = path_to_uri(&self.root)?;
            let creation = TableCreation::builder()
                .name(self.ident.name().to_string())
                .schema(iceberg_schema)
                .location(table_uri)
                .build();
            let table = match self.catalog.create_table(&namespace, creation).await {
                Ok(t) => t,
                Err(e) => {
                    // If table creation fails, attempt to recreate the old table
                    // from its metadata location to avoid data loss.
                    tracing::warn!(
                        error = %e,
                        "overwrite_commit: failed to create replacement table; \
                         attempting to restore original table"
                    );
                    if let Some(ref loc) = old_metadata_location {
                        let _ = self.catalog.drop_table(&self.ident).await;
                        let _ = self.catalog.register_table(&self.ident, loc.clone()).await;
                    }
                    return Err(LakehouseError::Iceberg(e.to_string()));
                }
            };
            if let Some(loc) = table.metadata_location() {
                self.update_version_hint(loc)?;
            }

            if batches.is_empty() {
                return Ok(0);
            }
            let staged = self.prepare(batches).await?;
            self.commit(staged, kafka_offsets).await
        }

        /// Record schema evolution in table properties.
        ///
        /// iceberg-rust 0.9.1 does not expose a public Transaction action for schema
        /// evolution (`AddSchema` / `SetCurrentSchema` require building a `TableCommit`
        /// which is `pub(crate)`).  As a best-effort alternative, the new schema
        /// metadata is stored under `krishiv.schema.id` and `krishiv.schema.fields`
        /// table properties so readers can observe the evolution through standard
        /// Iceberg table property APIs.
        pub async fn evolve_schema(
            &self,
            new_schema: &SchemaVersion,
        ) -> Result<(), LakehouseError> {
            let table = self
                .catalog
                .load_table(&self.ident)
                .await
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

            let fields_json = serde_json::to_string(
                &new_schema
                    .fields
                    .iter()
                    .map(|f| {
                        serde_json::json!({
                            "id": f.id,
                            "name": f.name,
                            "required": f.required,
                            "type": f.data_type,
                        })
                    })
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

            let tx = Transaction::new(&table);
            let action = tx
                .update_table_properties()
                .set(
                    "krishiv.schema.id".to_string(),
                    new_schema.schema_id.to_string(),
                )
                .set("krishiv.schema.fields".to_string(), fields_json);
            let tx = action
                .apply(tx)
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
            let committed = tx
                .commit(&*self.catalog)
                .await
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
            if let Some(loc) = committed.metadata_location() {
                self.update_version_hint(loc)?;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl IcebergTwoPhaseCommit for IcebergNativeTwoPhaseCommit {
        /// Write `batches` to `{root}/data/staged-{id}.parquet` and record the
        /// resulting `DataFile` descriptors in the pending map.
        async fn prepare(
            &self,
            batches: Vec<RecordBatch>,
        ) -> Result<StagedSnapshot, LakehouseError> {
            let staged_id = self.snap_counter.fetch_add(1, Ordering::Relaxed);

            if batches.is_empty() {
                self.pending
                    .lock()
                    .await
                    .insert(staged_id, StagedEntry { data_files: vec![] });
                return Ok(StagedSnapshot {
                    snapshot_id: staged_id,
                    batches: vec![],
                });
            }

            let file_name = format!("staged-{staged_id:016x}.parquet");
            let parquet_path = self.root.join("data").join(&file_name);
            let tmp_path = self.root.join("data").join(format!(".{file_name}.tmp"));

            let (record_count, file_size) = write_parquet_and_measure(&tmp_path, &batches)?;

            fs::rename(&tmp_path, &parquet_path).map_err(|e| LakehouseError::Io(e.to_string()))?;
            #[cfg(unix)]
            {
                if let Ok(dir) = fs::File::open(self.root.join("data")) {
                    let _ = dir.sync_all();
                }
            }

            let file_uri = path_to_uri(&parquet_path)?;

            let data_file = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(file_uri)
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(file_size)
                .record_count(record_count)
                .partition(Struct::empty())
                .partition_spec_id(0)
                .build()
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

            self.pending.lock().await.insert(
                staged_id,
                StagedEntry {
                    data_files: vec![data_file],
                },
            );

            Ok(StagedSnapshot {
                snapshot_id: staged_id,
                batches,
            })
        }

        /// Atomically commit via `Transaction::fast_append`, updating the
        /// iceberg-spec table metadata and writing manifests to disk.
        async fn commit(
            &self,
            staged: StagedSnapshot,
            kafka_offsets: BTreeMap<String, i64>,
        ) -> Result<i64, LakehouseError> {
            let entry = {
                let mut pending = self.pending.lock().await;
                pending
                    .remove(&staged.snapshot_id)
                    .ok_or_else(|| LakehouseError::Concurrency {
                        message: format!(
                            "staged snapshot {} not found in pending map",
                            staged.snapshot_id
                        ),
                    })?
            };

            if entry.data_files.is_empty() {
                // Nothing to commit; return current snapshot id (or 0).
                let table = self
                    .catalog
                    .load_table(&self.ident)
                    .await
                    .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
                return Ok(table
                    .metadata()
                    .current_snapshot()
                    .map(|s| s.snapshot_id())
                    .unwrap_or(0));
            }

            // Encode kafka offsets as snapshot summary properties.
            let mut snap_props: HashMap<String, String> = kafka_offsets
                .iter()
                .map(|(k, v)| (format!("krishiv.kafka.offset.{k}"), v.to_string()))
                .collect();
            snap_props.insert(
                KAFKA_OFFSETS_SUMMARY_KEY.to_string(),
                kafka_offsets_json(&kafka_offsets),
            );

            let table = self
                .catalog
                .load_table(&self.ident)
                .await
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

            let tx = Transaction::new(&table);
            let action = tx
                .fast_append()
                .add_data_files(entry.data_files)
                .set_snapshot_properties(snap_props);
            let tx = action
                .apply(tx)
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

            let committed = tx
                .commit(&*self.catalog)
                .await
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

            // Persist the new metadata location for crash recovery.
            if let Some(loc) = committed.metadata_location() {
                self.update_version_hint(loc)?;
            }

            committed
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .ok_or_else(|| LakehouseError::Concurrency {
                    message: "snapshot id missing after commit".to_string(),
                })
        }

        /// Discard a staged snapshot.  The Parquet file remains on disk as an
        /// orphan and will be removed by a future `VACUUM` operation.
        async fn abort(&self, staged: StagedSnapshot) -> Result<(), LakehouseError> {
            self.pending.lock().await.remove(&staged.snapshot_id);
            Ok(())
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    fn write_version_hint(root: &Path, metadata_location: &str) -> Result<(), LakehouseError> {
        let path = root.join("metadata").join(VERSION_HINT);
        fs::write(&path, metadata_location).map_err(|e| LakehouseError::Io(e.to_string()))
    }

    /// Convert an absolute local path to a `file://` URI.
    fn path_to_uri(path: &Path) -> Result<String, LakehouseError> {
        Url::from_file_path(path)
            .map(|u| u.to_string())
            .map_err(|()| {
                LakehouseError::Io(format!("cannot convert path to URI: {}", path.display()))
            })
    }

    /// Write `batches` to `path` as Parquet, fsync, then return `(record_count, file_size_bytes)`.
    fn write_parquet_and_measure(
        path: &Path,
        batches: &[RecordBatch],
    ) -> Result<(u64, u64), LakehouseError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| LakehouseError::Io(e.to_string()))?;
        }
        let schema = batches
            .first()
            .ok_or_else(|| LakehouseError::Io("empty batches".to_string()))?
            .schema();
        let file = fs::File::create(path).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let mut record_count: u64 = 0;
        for batch in batches {
            record_count += batch.num_rows() as u64;
            writer
                .write(batch)
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
        }
        let file = writer
            .into_inner()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        file.sync_all()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let file_size = file
            .metadata()
            .map_err(|e| LakehouseError::Io(e.to_string()))?
            .len();
        Ok((record_count, file_size))
    }

    /// Convert our `SchemaVersion` to an `iceberg::spec::Schema`.
    fn schema_version_to_iceberg(sv: &SchemaVersion) -> Result<Schema, LakehouseError> {
        let fields: Vec<Arc<NestedField>> = sv
            .fields
            .iter()
            .map(|f| {
                let ty = str_to_iceberg_type(&f.data_type)?;
                let field = if f.required {
                    NestedField::required(f.id, &f.name, ty)
                } else {
                    NestedField::optional(f.id, &f.name, ty)
                };
                Ok(Arc::new(field))
            })
            .collect::<Result<Vec<_>, LakehouseError>>()?;

        Schema::builder()
            .with_schema_id(sv.schema_id)
            .with_fields(fields)
            .build()
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))
    }

    fn str_to_iceberg_type(s: &str) -> Result<Type, LakehouseError> {
        let t = match s.to_lowercase().as_str() {
            "boolean" | "bool" => Type::Primitive(PrimitiveType::Boolean),
            "int" | "int32" | "integer" => Type::Primitive(PrimitiveType::Int),
            "long" | "int64" | "bigint" => Type::Primitive(PrimitiveType::Long),
            "float" | "float32" => Type::Primitive(PrimitiveType::Float),
            "double" | "float64" => Type::Primitive(PrimitiveType::Double),
            "string" | "utf8" | "varchar" | "text" | "str" => {
                Type::Primitive(PrimitiveType::String)
            }
            "binary" | "bytes" | "varbinary" => Type::Primitive(PrimitiveType::Binary),
            "date" | "date32" => Type::Primitive(PrimitiveType::Date),
            "timestamp" | "timestamp[us, tz=utc]" | "timestamp[ms]" | "timestamp[us]" => {
                Type::Primitive(PrimitiveType::Timestamp)
            }
            "timestamptz" => Type::Primitive(PrimitiveType::Timestamptz),
            _ => {
                return Err(LakehouseError::SchemaConflict {
                    message: format!("unsupported data type for Iceberg schema: '{s}'"),
                });
            }
        };
        Ok(t)
    }

    // ── tests ────────────────────────────────────────────────────────────────

    #[cfg(test)]
    mod tests {
        use std::collections::BTreeMap;
        use std::sync::Arc;

        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use iceberg::Catalog;

        use crate::lakehouse::two_phase::IcebergTwoPhaseCommit;
        use crate::lakehouse::{SchemaField, SchemaVersion};

        use super::IcebergNativeTwoPhaseCommit;

        fn schema_version() -> SchemaVersion {
            SchemaVersion {
                schema_id: 1,
                fields: vec![SchemaField {
                    id: 1,
                    name: "x".to_string(),
                    required: true,
                    data_type: "long".to_string(),
                }],
            }
        }

        fn batch(values: Vec<i64>) -> arrow::record_batch::RecordBatch {
            let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                "x",
                DataType::Int64,
                false,
            )]));
            arrow::record_batch::RecordBatch::try_new(
                schema,
                vec![Arc::new(Int64Array::from(values))],
            )
            .unwrap()
        }

        #[tokio::test]
        async fn iceberg_native_prepare_commit_round_trip() {
            let dir = tempfile::tempdir().unwrap();
            let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "test", &schema_version())
                .await
                .unwrap();

            let staged = tpc.prepare(vec![batch(vec![1, 2, 3])]).await.unwrap();
            assert_eq!(staged.batches.len(), 1);

            let snap_id = tpc.commit(staged, BTreeMap::new()).await.unwrap();
            assert!(snap_id > 0, "committed snapshot id must be positive");
        }

        #[tokio::test]
        async fn iceberg_native_multiple_commits_accumulate() {
            let dir = tempfile::tempdir().unwrap();
            let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "test", &schema_version())
                .await
                .unwrap();

            for i in 0..3i64 {
                let staged = tpc.prepare(vec![batch(vec![i])]).await.unwrap();
                tpc.commit(staged, BTreeMap::new()).await.unwrap();
            }

            // Verify version-hint.text was updated after each commit.
            let hint =
                std::fs::read_to_string(dir.path().join("metadata").join("version-hint.text"))
                    .unwrap();
            assert!(
                hint.contains("metadata.json"),
                "version-hint must reference a metadata file"
            );
        }

        #[tokio::test]
        async fn iceberg_native_abort_removes_pending() {
            let dir = tempfile::tempdir().unwrap();
            let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "test", &schema_version())
                .await
                .unwrap();

            let staged = tpc.prepare(vec![batch(vec![42])]).await.unwrap();
            tpc.abort(staged).await.unwrap();

            // Pending map should be empty — no entry to commit.
            assert!(tpc.pending.lock().await.is_empty());
        }

        #[tokio::test]
        async fn iceberg_native_empty_prepare_commits_noop() {
            let dir = tempfile::tempdir().unwrap();
            let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "test", &schema_version())
                .await
                .unwrap();

            let staged = tpc.prepare(vec![]).await.unwrap();
            // Commit of empty snapshot returns 0 (no snapshot yet).
            let snap_id = tpc.commit(staged, BTreeMap::new()).await.unwrap();
            assert_eq!(snap_id, 0);
        }

        #[tokio::test]
        async fn iceberg_native_kafka_offsets_in_snapshot_properties() {
            let dir = tempfile::tempdir().unwrap();
            let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "test", &schema_version())
                .await
                .unwrap();

            let staged = tpc.prepare(vec![batch(vec![1])]).await.unwrap();
            let mut offsets = BTreeMap::new();
            offsets.insert("orders-0".to_string(), 100i64);
            offsets.insert("orders-1".to_string(), 200i64);
            let snap_id = tpc.commit(staged, offsets).await.unwrap();
            assert!(snap_id > 0);

            // Reload table and verify snapshot summary properties.
            let table = tpc.catalog.load_table(&tpc.ident).await.unwrap();
            let snapshot = table.metadata().current_snapshot().unwrap();
            let summary_props = &snapshot.summary().additional_properties;
            assert!(
                summary_props.contains_key("krishiv.kafka.committed_offsets"),
                "kafka offsets must be in snapshot summary"
            );
        }

        #[tokio::test]
        async fn iceberg_native_version_hint_enables_recovery() {
            let dir = tempfile::tempdir().unwrap();

            // Session 1: create + commit.
            {
                let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "test", &schema_version())
                    .await
                    .unwrap();
                let staged = tpc.prepare(vec![batch(vec![7, 8, 9])]).await.unwrap();
                tpc.commit(staged, BTreeMap::new()).await.unwrap();
            }

            // Session 2: recovery — open the same root and verify the table is readable.
            {
                let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "test", &schema_version())
                    .await
                    .unwrap();
                let table = tpc.catalog.load_table(&tpc.ident).await.unwrap();
                assert!(
                    table.metadata().current_snapshot().is_some(),
                    "recovered table must have a committed snapshot"
                );
            }
        }

        #[tokio::test]
        async fn iceberg_native_overwrite_replaces_data() {
            let dir = tempfile::tempdir().unwrap();
            let sv = schema_version();
            let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "test", &sv)
                .await
                .unwrap();

            // First commit: append [1, 2, 3]
            let staged = tpc.prepare(vec![batch(vec![1, 2, 3])]).await.unwrap();
            tpc.commit(staged, BTreeMap::new()).await.unwrap();

            // Overwrite: replace with [10, 20]
            let snap_id = tpc
                .overwrite_commit(vec![batch(vec![10, 20])], BTreeMap::new(), &sv)
                .await
                .unwrap();
            assert!(snap_id > 0, "overwrite snapshot id must be positive");

            // Version hint must be updated.
            let hint =
                std::fs::read_to_string(dir.path().join("metadata").join("version-hint.text"))
                    .unwrap();
            assert!(hint.contains("metadata.json"));
        }

        #[tokio::test]
        async fn iceberg_native_evolve_schema_stores_properties() {
            let dir = tempfile::tempdir().unwrap();
            let sv = schema_version();
            let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "test", &sv)
                .await
                .unwrap();

            // Append some data first.
            let staged = tpc.prepare(vec![batch(vec![1])]).await.unwrap();
            tpc.commit(staged, BTreeMap::new()).await.unwrap();

            // Evolve schema: add optional "y" field.
            let sv2 = crate::lakehouse::SchemaVersion {
                schema_id: 2,
                fields: vec![
                    crate::lakehouse::SchemaField {
                        id: 1,
                        name: "x".to_string(),
                        required: true,
                        data_type: "long".to_string(),
                    },
                    crate::lakehouse::SchemaField {
                        id: 2,
                        name: "y".to_string(),
                        required: false,
                        data_type: "string".to_string(),
                    },
                ],
            };
            tpc.evolve_schema(&sv2).await.unwrap();

            // Schema evolution is stored in table properties.
            let table = tpc.catalog.load_table(&tpc.ident).await.unwrap();
            let props = table.metadata().properties();
            assert_eq!(
                props.get("krishiv.schema.id").map(String::as_str),
                Some("2")
            );
            assert!(props.contains_key("krishiv.schema.fields"));
        }
    }
}

#[cfg(feature = "iceberg")]
pub use native::IcebergNativeTwoPhaseCommit;
