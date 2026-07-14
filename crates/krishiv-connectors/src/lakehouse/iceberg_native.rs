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
    use bytes::Bytes;
    // `Storage as _` brings the async read/write/exists methods into scope so the
    // object-store branch can drive `KrishivStorage` (staging, version-hint).
    use iceberg::io::{LocalFsStorageFactory, Storage as _, StorageFactory};
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

    use crate::lakehouse::object_store_io::{KrishivStorage, KrishivStorageFactory};
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
        /// Local root path (canonicalized) for a `file://` table. For an
        /// object-store table this holds the raw `s3://…` root as an opaque
        /// carrier only — it is never `canonicalize`d or `join`ed for local FS.
        root: PathBuf,
        /// `true` when the table root is an object-store URI (`s3://` / `s3a://`).
        /// Selects the object-store branch in staging / version-hint / reads.
        is_object_store: bool,
        /// URI form of the table root: `s3://bucket/prefix` (object store) or the
        /// `file://` URI of the local root. Staged files and the version-hint are
        /// addressed beneath it on the object-store branch.
        root_uri: String,
        /// Scheme-dispatching object-store bridge, exercised only on the
        /// object-store branch. `file://` I/O stays on `std::fs` to preserve the
        /// certified fsync/rename crash-atomicity.
        store: KrishivStorage,
        pub(crate) pending: Mutex<HashMap<i64, StagedEntry>>,
        snap_counter: AtomicI64,
    }

    /// `true` when `root` is an object-store URI the sink must address through
    /// [`KrishivStorage`] rather than the local filesystem (`s3://`/`s3a://`, or
    /// `memory://` for the deterministic in-process object store).
    fn is_object_store_root(root: &Path) -> bool {
        root.to_str().is_some_and(|s| {
            s.starts_with("s3://") || s.starts_with("s3a://") || s.starts_with("memory://")
        })
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
            let is_object_store = is_object_store_root(root);
            let store = KrishivStorage::default();

            // Path/URI setup differs by backend. Object stores are prefix-based:
            // no directories to create, no local path to canonicalize — the raw
            // `s3://…` string IS the warehouse URI (trailing slash trimmed so the
            // `{root}/data/…` / `{root}/metadata/…` joins are well-formed).
            let (root, table_uri): (PathBuf, String) = if is_object_store {
                let uri = root
                    .to_str()
                    .ok_or_else(|| LakehouseError::Io("non-utf8 object-store root".to_string()))?
                    .trim_end_matches('/')
                    .to_string();
                (root.to_path_buf(), uri)
            } else {
                fs::create_dir_all(root.join("data"))
                    .map_err(|e| LakehouseError::Io(e.to_string()))?;
                fs::create_dir_all(root.join("metadata"))
                    .map_err(|e| LakehouseError::Io(e.to_string()))?;
                let root = root
                    .canonicalize()
                    .map_err(|e| LakehouseError::Io(e.to_string()))?;
                let uri = path_to_uri(&root)?;
                (root, uri)
            };

            // The MemoryCatalog's FileIO (manifests, manifest-lists,
            // table-metadata.json) goes through `KrishivStorageFactory` for an
            // object-store root (which serves `s3://`) and `LocalFsStorageFactory`
            // for a local root (byte-identical to the certified path).
            let storage_factory: Arc<dyn StorageFactory> = if is_object_store {
                Arc::new(KrishivStorageFactory)
            } else {
                Arc::new(LocalFsStorageFactory)
            };
            let catalog = MemoryCatalogBuilder::default()
                .with_storage_factory(storage_factory)
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

            // Recovery: register the last committed metadata if a version-hint
            // exists, else create the table and persist the initial hint. The
            // hint lives at `{root}/metadata/version-hint.text` in both backends.
            let existing_hint: Option<String> = if is_object_store {
                let vh_uri = version_hint_uri(&table_uri);
                if store
                    .exists(&vh_uri)
                    .await
                    .map_err(|e| LakehouseError::Io(e.to_string()))?
                {
                    let bytes = store
                        .read(&vh_uri)
                        .await
                        .map_err(|e| LakehouseError::Io(e.to_string()))?;
                    Some(String::from_utf8_lossy(&bytes).trim().to_string())
                } else {
                    None
                }
            } else {
                let version_hint = root.join("metadata").join(VERSION_HINT);
                if version_hint.exists() {
                    let meta_loc = fs::read_to_string(&version_hint)
                        .map_err(|e| LakehouseError::Io(e.to_string()))?;
                    Some(meta_loc.trim().to_string())
                } else {
                    None
                }
            };

            if let Some(meta_loc) = existing_hint {
                catalog
                    .register_table(&ident, meta_loc)
                    .await
                    .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
            } else {
                let iceberg_schema = schema_version_to_iceberg(schema_version)?;
                let creation = TableCreation::builder()
                    .name(table_name.to_string())
                    .schema(iceberg_schema)
                    .location(table_uri.clone())
                    .build();
                let table = catalog
                    .create_table(&namespace, creation)
                    .await
                    .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
                // Persist the initial metadata location so a crash after
                // create_table but before any commit still recovers cleanly.
                if let Some(loc) = table.metadata_location() {
                    if is_object_store {
                        store
                            .write(&version_hint_uri(&table_uri), Bytes::from(loc.to_string()))
                            .await
                            .map_err(|e| LakehouseError::Io(e.to_string()))?;
                    } else {
                        write_version_hint(&root, loc)?;
                    }
                }
            }

            Ok(Self {
                catalog,
                ident,
                root,
                is_object_store,
                root_uri: table_uri,
                store,
                pending: Mutex::new(HashMap::new()),
                // Seed snap_counter with a per-instance nanosecond timestamp so
                // staged filenames are unique across processes AND across
                // instances of the same process — including after a restart
                // that reuses the PID (the old `pid << 32` seed collided there,
                // so a fresh sink could reuse an orphan's name and DUR-2
                // re-staging could clobber the file it just committed). The
                // counter still increments monotonically within an instance.
                snap_counter: AtomicI64::new(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or_else(|_| (std::process::id() as i64) << 32),
                ),
            })
        }

        fn update_version_hint(&self, metadata_location: &str) -> Result<(), LakehouseError> {
            if self.is_object_store {
                // Object stores `put` atomically — no torn-write window, so the
                // local temp+fsync+rename dance is unnecessary here.
                krishiv_common::async_util::block_on(self.store.write(
                    &version_hint_uri(&self.root_uri),
                    Bytes::from(metadata_location.to_string()),
                ))
                .map_err(|e| LakehouseError::Io(e.to_string()))
            } else {
                write_version_hint(&self.root, metadata_location)
            }
        }

        /// Durably stage `batches` as one Parquet file under `{root}/data/`
        /// and return its `DataFile` descriptor plus the staged path.
        ///
        /// Synchronous (plain file I/O) so checkpoint-aligned sinks can stage
        /// from blocking contexts. The staged file is invisible to readers
        /// until its `DataFile` is committed via [`Self::append_data_files`];
        /// an uncommitted staged file is an orphan cleaned by VACUUM.
        pub fn stage_parquet(
            &self,
            batches: &[RecordBatch],
        ) -> Result<(PathBuf, iceberg::spec::DataFile), LakehouseError> {
            let staged_id = self.snap_counter.fetch_add(1, Ordering::Relaxed);
            let file_name = format!("staged-{staged_id:016x}.parquet");

            let (staged_ref, file_uri, record_count, file_size) = if self.is_object_store {
                // Object-store staging: buffer the Parquet in memory then `put`
                // it as a single atomic object (no tmp+rename — object stores
                // have neither directories nor rename). The staged ref carried
                // through the pending map / DUR-2 sidecar is the `s3://…` URI.
                let file_uri = format!("{}/data/{}", self.root_uri, file_name);
                let (buf, record_count, file_size) = write_parquet_to_buf(batches)?;
                krishiv_common::async_util::block_on(self.store.write(&file_uri, Bytes::from(buf)))
                    .map_err(|e| LakehouseError::Io(e.to_string()))?;
                (PathBuf::from(&file_uri), file_uri, record_count, file_size)
            } else {
                let parquet_path = self.root.join("data").join(&file_name);
                let tmp_path = self.root.join("data").join(format!(".{file_name}.tmp"));

                let (record_count, file_size) = write_parquet_and_measure(&tmp_path, batches)?;

                fs::rename(&tmp_path, &parquet_path)
                    .map_err(|e| LakehouseError::Io(e.to_string()))?;
                #[cfg(unix)]
                {
                    if let Ok(dir) = fs::File::open(self.root.join("data")) {
                        let _ = dir.sync_all();
                    }
                }
                let file_uri = path_to_uri(&parquet_path)?;
                (parquet_path, file_uri, record_count, file_size)
            };

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

            Ok((staged_ref, data_file))
        }

        /// Atomically commit already-staged data files via
        /// `Transaction::fast_append`, embedding `kafka_offsets` in the
        /// snapshot summary. Retry-safe: the caller keeps the `DataFile`
        /// descriptors until this returns `Ok`, so a failed transaction can
        /// simply be retried with the same files.
        pub async fn append_data_files(
            &self,
            data_files: Vec<iceberg::spec::DataFile>,
            kafka_offsets: BTreeMap<String, i64>,
        ) -> Result<i64, LakehouseError> {
            if data_files.is_empty() {
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

            let committed =
                fast_append_via(&self.catalog, &self.ident, data_files, &kafka_offsets).await?;

            // Best-effort recovery hint; the commit above is already durable.
            if let Some(loc) = committed.metadata_location()
                && let Err(e) = self.update_version_hint(loc)
            {
                tracing::warn!(
                    table = %self.ident,
                    location = loc,
                    error = %e,
                    "version hint update failed after successful commit; \
                     hint file may be stale — data is durable in the catalog"
                );
            }

            committed
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .ok_or_else(|| LakehouseError::Concurrency {
                    message: "snapshot id missing after commit".to_string(),
                })
        }

        /// Read every data file of the current snapshot into Arrow batches.
        ///
        /// Returns an empty vec for an empty table. Used by the streaming
        /// sink's copy-on-write row-level path (upsert/delete).
        pub async fn read_all(&self) -> Result<Vec<RecordBatch>, LakehouseError> {
            use futures::TryStreamExt as _;

            let table = self
                .catalog
                .load_table(&self.ident)
                .await
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
            if table.metadata().current_snapshot().is_none() {
                return Ok(vec![]);
            }
            let scan = table
                .scan()
                .build()
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
            let tasks: Vec<iceberg::scan::FileScanTask> = scan
                .plan_files()
                .await
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?
                .try_collect()
                .await
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

            let mut batches = Vec::new();
            for task in tasks {
                let path = task.data_file_path();
                if self.is_object_store {
                    let bytes = self
                        .store
                        .read(path)
                        .await
                        .map_err(|e| LakehouseError::Io(format!("read data file {path}: {e}")))?;
                    batches.extend(read_parquet_bytes(bytes)?);
                } else {
                    let local = path.strip_prefix("file://").unwrap_or(path);
                    let file = fs::File::open(local)
                        .map_err(|e| LakehouseError::Io(format!("open data file {local}: {e}")))?;
                    let reader =
                        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                            file,
                        )
                        .map_err(|e| LakehouseError::Io(e.to_string()))?
                        .build()
                        .map_err(|e| LakehouseError::Io(e.to_string()))?;
                    for batch in reader {
                        batches.push(batch.map_err(|e| LakehouseError::Io(e.to_string()))?);
                    }
                }
            }
            Ok(batches)
        }

        /// The source offsets embedded in the current committed snapshot's
        /// summary (empty for an uncommitted table). DUR-2 recovery gates an
        /// idempotent re-commit on this: a prepared epoch whose offsets are
        /// already covered here was committed and must not be appended again
        /// (`fast_append` is not otherwise idempotent — a blind retry would
        /// double-write the rows).
        pub async fn committed_kafka_offsets(
            &self,
        ) -> Result<BTreeMap<String, i64>, LakehouseError> {
            let table = self
                .catalog
                .load_table(&self.ident)
                .await
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
            let Some(snapshot) = table.metadata().current_snapshot() else {
                return Ok(BTreeMap::new());
            };
            Ok(snapshot
                .summary()
                .additional_properties
                .get(KAFKA_OFFSETS_SUMMARY_KEY)
                .map(|json| crate::lakehouse::two_phase::parse_kafka_offsets_json(json))
                .unwrap_or_default())
        }

        /// Read a single staged Parquet file back into Arrow batches. DUR-2
        /// upsert recovery replays the row-level merge from the staged rows.
        pub fn read_staged_parquet(&self, path: &str) -> Result<Vec<RecordBatch>, LakehouseError> {
            if self.is_object_store {
                let bytes = krishiv_common::async_util::block_on(self.store.read(path))
                    .map_err(|e| LakehouseError::Io(e.to_string()))?;
                read_parquet_bytes(bytes)
            } else {
                let file = fs::File::open(path).map_err(|e| LakehouseError::Io(e.to_string()))?;
                let reader =
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                        .map_err(|e| LakehouseError::Io(e.to_string()))?
                        .build()
                        .map_err(|e| LakehouseError::Io(e.to_string()))?;
                let mut batches = Vec::new();
                for batch in reader {
                    batches.push(batch.map_err(|e| LakehouseError::Io(e.to_string()))?);
                }
                Ok(batches)
            }
        }

        /// Scheme-aware sidecar/cleanup helpers used by the streaming sink's
        /// DUR-2 path so its `.dur2.json` writes/reads and staged-file cleanup
        /// hit the same backend as the staged Parquet.
        ///
        /// Write `contents` to `path` (an `s3://…` URI on the object-store
        /// branch, a local FS path otherwise).
        pub fn write_file(&self, path: &str, contents: &str) -> Result<(), LakehouseError> {
            if self.is_object_store {
                krishiv_common::async_util::block_on(
                    self.store.write(path, Bytes::from(contents.to_string())),
                )
                .map_err(|e| LakehouseError::Io(e.to_string()))
            } else {
                fs::write(path, contents).map_err(|e| LakehouseError::Io(e.to_string()))
            }
        }

        /// Read `path` to a string, returning `None` if it does not exist
        /// (the DUR-2 idempotency signal: a missing sidecar = already finalized).
        pub fn read_file_opt(&self, path: &str) -> Result<Option<String>, LakehouseError> {
            if self.is_object_store {
                if !krishiv_common::async_util::block_on(self.store.exists(path))
                    .map_err(|e| LakehouseError::Io(e.to_string()))?
                {
                    return Ok(None);
                }
                let bytes = krishiv_common::async_util::block_on(self.store.read(path))
                    .map_err(|e| LakehouseError::Io(e.to_string()))?;
                Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
            } else {
                match fs::read_to_string(path) {
                    Ok(s) => Ok(Some(s)),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(LakehouseError::Io(e.to_string())),
                }
            }
        }

        /// Best-effort delete tolerant of an already-absent object (idempotent
        /// cleanup); a genuine error is logged and the object left for VACUUM.
        pub fn remove_file_best_effort(&self, path: &str) {
            let result = if self.is_object_store {
                krishiv_common::async_util::block_on(self.store.delete(path))
                    .map_err(|e| LakehouseError::Io(e.to_string()))
            } else {
                match fs::remove_file(path) {
                    Ok(()) => Ok(()),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(e) => Err(LakehouseError::Io(e.to_string())),
                }
            };
            if let Err(e) = result {
                tracing::warn!(
                    path,
                    error = %e,
                    "iceberg streaming sink: cleanup remove failed (left as orphan for VACUUM)"
                );
            }
        }

        /// Atomically replace all table data with `batches`.
        ///
        /// iceberg-rust 0.9.1 exposes no overwrite snapshot action, so the
        /// replacement is a new table *generation* at the same root. CONN-3
        /// (crash atomicity, found live by the G8 kill test): the previous
        /// generation — including the metadata files the version-hint points
        /// at — must stay untouched until the replacement is fully durable.
        /// Ordering:
        ///
        ///   1. build the new generation (creation metadata + committed
        ///      snapshot) under a THROWAWAY in-memory catalog over the same
        ///      root — new uuid-named files, old generation untouched;
        ///   2. atomically flip the version-hint to the new metadata — the
        ///      single durable commit point of the overwrite;
        ///   3. rebind the live catalog: drop the old entry (purges the old
        ///      generation's metadata files, now unreferenced) and register
        ///      the new location.
        ///
        /// A crash before 2 leaves the hint on the intact old generation; a
        /// crash after 2 leaves it on the complete new one. Old data files
        /// and pre-flip orphan metadata are cleaned by a future VACUUM.
        pub async fn overwrite_commit(
            &self,
            batches: Vec<RecordBatch>,
            kafka_offsets: BTreeMap<String, i64>,
            schema_version: &SchemaVersion,
        ) -> Result<i64, LakehouseError> {
            let namespace = self.ident.namespace().clone();
            let table_uri = if self.is_object_store {
                self.root_uri.clone()
            } else {
                path_to_uri(&self.root)?
            };
            let storage_factory: Arc<dyn StorageFactory> = if self.is_object_store {
                Arc::new(KrishivStorageFactory)
            } else {
                Arc::new(LocalFsStorageFactory)
            };

            // 1. New generation under a throwaway catalog (same root/FileIO).
            let scratch = MemoryCatalogBuilder::default()
                .with_storage_factory(storage_factory)
                .load(
                    "local-overwrite",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), table_uri.clone())]),
                )
                .await
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
            let _ = scratch.create_namespace(&namespace, HashMap::new()).await;
            let iceberg_schema = schema_version_to_iceberg(schema_version)?;
            let creation = TableCreation::builder()
                .name(self.ident.name().to_string())
                .schema(iceberg_schema)
                .location(table_uri)
                .build();
            let created = scratch
                .create_table(&namespace, creation)
                .await
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

            let (new_meta_loc, snapshot_id) = if batches.is_empty() {
                let loc = created
                    .metadata_location()
                    .map(String::from)
                    .ok_or_else(|| {
                        LakehouseError::Iceberg(
                            "replacement table creation returned no metadata location".into(),
                        )
                    })?;
                (loc, 0)
            } else {
                let (_, data_file) = self.stage_parquet(&batches)?;
                let committed =
                    fast_append_via(&scratch, &self.ident, vec![data_file], &kafka_offsets).await?;
                let loc = committed
                    .metadata_location()
                    .map(String::from)
                    .ok_or_else(|| {
                        LakehouseError::Iceberg(
                            "replacement commit returned no metadata location".into(),
                        )
                    })?;
                let snap = committed
                    .metadata()
                    .current_snapshot()
                    .map(|s| s.snapshot_id())
                    .ok_or_else(|| LakehouseError::Concurrency {
                        message: "snapshot id missing after overwrite commit".to_string(),
                    })?;
                (loc, snap)
            };

            // 2. Durable commit point: flip the hint to the new generation.
            self.update_version_hint(&new_meta_loc)?;

            // 3. Rebind the live catalog. drop_table purges the old
            // generation's metadata files — safe now that the hint moved on.
            // The scratch catalog is simply dropped from memory (never
            // drop_table'd), so the new generation's files survive it.
            let _ = self.catalog.drop_table(&self.ident).await;
            self.pending.lock().await.clear();
            self.catalog
                .register_table(&self.ident, new_meta_loc)
                .await
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

            Ok(snapshot_id)
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
            if batches.is_empty() {
                let staged_id = self.snap_counter.fetch_add(1, Ordering::Relaxed);
                self.pending
                    .lock()
                    .await
                    .insert(staged_id, StagedEntry { data_files: vec![] });
                return Ok(StagedSnapshot {
                    snapshot_id: staged_id,
                    batches: vec![],
                });
            }

            let (_, data_file) = self.stage_parquet(&batches)?;
            // stage_parquet consumed one counter value for the file name; take
            // a fresh one as the pending-map key so ids stay unique.
            let staged_id = self.snap_counter.fetch_add(1, Ordering::Relaxed);

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

            self.append_data_files(entry.data_files, kafka_offsets)
                .await
        }

        /// Discard a staged snapshot.  The Parquet file remains on disk as an
        /// orphan and will be removed by a future `VACUUM` operation.
        async fn abort(&self, staged: StagedSnapshot) -> Result<(), LakehouseError> {
            self.pending.lock().await.remove(&staged.snapshot_id);
            Ok(())
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    /// Run one `fast_append` transaction for `ident` on `catalog`, embedding
    /// `kafka_offsets` in the snapshot summary. Shared by the in-place append
    /// path and the overwrite path (which commits into a throwaway catalog
    /// before flipping the version-hint). Does NOT touch the version-hint.
    async fn fast_append_via(
        catalog: &MemoryCatalog,
        ident: &TableIdent,
        data_files: Vec<iceberg::spec::DataFile>,
        kafka_offsets: &BTreeMap<String, i64>,
    ) -> Result<iceberg::table::Table, LakehouseError> {
        let mut snap_props: HashMap<String, String> = kafka_offsets
            .iter()
            .map(|(k, v)| (format!("krishiv.kafka.offset.{k}"), v.to_string()))
            .collect();
        snap_props.insert(
            KAFKA_OFFSETS_SUMMARY_KEY.to_string(),
            kafka_offsets_json(kafka_offsets),
        );

        let table = catalog
            .load_table(ident)
            .await
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(data_files)
            .set_snapshot_properties(snap_props);
        let tx = action
            .apply(tx)
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

        tx.commit(catalog)
            .await
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))
    }

    /// CONN-2: Atomically write the version-hint file (temp + fsync + rename +
    /// dir-sync) so a crash or power loss cannot leave a torn hint that makes
    /// the table unopenable.
    pub(crate) fn write_version_hint(
        root: &Path,
        metadata_location: &str,
    ) -> Result<(), LakehouseError> {
        let dir = root.join("metadata");
        let target = dir.join(VERSION_HINT);
        let temp = dir.join(format!(
            "{VERSION_HINT}.tmp.{}.{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        fs::write(&temp, metadata_location).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let f = fs::File::open(&temp).map_err(|e| LakehouseError::Io(e.to_string()))?;
        f.sync_all()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        drop(f);
        fs::rename(&temp, &target).map_err(|e| LakehouseError::Io(e.to_string()))?;
        // Sync the parent directory so the rename is durable.
        if let Ok(dir_handle) = fs::File::open(&dir) {
            let _ = dir_handle.sync_all();
        }
        Ok(())
    }

    /// Convert an absolute local path to a `file://` URI.
    fn path_to_uri(path: &Path) -> Result<String, LakehouseError> {
        Url::from_file_path(path)
            .map(|u| u.to_string())
            .map_err(|()| {
                LakehouseError::Io(format!("cannot convert path to URI: {}", path.display()))
            })
    }

    /// The version-hint object/file URI beneath a table root URI.
    fn version_hint_uri(root_uri: &str) -> String {
        format!("{root_uri}/metadata/{VERSION_HINT}")
    }

    /// Serialize `batches` to an in-memory Parquet buffer, returning
    /// `(bytes, record_count, file_size)`. The object-store staging path uses
    /// this so a single atomic `put` replaces the local temp+rename write.
    fn write_parquet_to_buf(batches: &[RecordBatch]) -> Result<(Vec<u8>, u64, u64), LakehouseError> {
        let schema = batches
            .first()
            .ok_or_else(|| LakehouseError::Io("empty batches".to_string()))?
            .schema();
        let mut buf: Vec<u8> = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buf, schema, None).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let mut record_count: u64 = 0;
        for batch in batches {
            record_count += batch.num_rows() as u64;
            writer
                .write(batch)
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
        }
        writer.close().map_err(|e| LakehouseError::Io(e.to_string()))?;
        let file_size = buf.len() as u64;
        Ok((buf, record_count, file_size))
    }

    /// Decode an in-memory Parquet buffer into Arrow batches (object-store reads).
    fn read_parquet_bytes(bytes: Bytes) -> Result<Vec<RecordBatch>, LakehouseError> {
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| LakehouseError::Io(e.to_string()))?
            .build()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch.map_err(|e| LakehouseError::Io(e.to_string()))?);
        }
        Ok(batches)
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

        /// CONN-3 regression (found live by the G8 kill test): a crash in the
        /// middle of `overwrite_commit` must never leave the version-hint
        /// dangling. The two durable states an overwrite can leave are (a)
        /// pre-flip — hint on the old generation with the replacement's
        /// orphan metadata coexisting in the same dir — and (b) post-flip.
        /// Both must reopen and read.
        #[tokio::test]
        async fn iceberg_native_overwrite_crash_states_stay_readable() {
            use std::collections::HashMap;

            use iceberg::CatalogBuilder as _;
            use iceberg::io::LocalFsStorageFactory;
            use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};

            let dir = tempfile::tempdir().unwrap();
            let tpc = IcebergNativeTwoPhaseCommit::open(dir.path(), "test", &schema_version())
                .await
                .unwrap();
            let staged = tpc.prepare(vec![batch(vec![1, 2, 3])]).await.unwrap();
            tpc.commit(staged, BTreeMap::new()).await.unwrap();
            drop(tpc);

            // (a) Simulate the pre-flip crash window: a replacement
            // generation's creation metadata lands in the same dir (what
            // overwrite_commit's scratch catalog writes first), hint
            // untouched. Reopen must still read the old generation.
            {
                let root = dir.path().canonicalize().unwrap();
                let uri = url::Url::from_directory_path(&root).unwrap().to_string();
                let scratch = MemoryCatalogBuilder::default()
                    .with_storage_factory(Arc::new(LocalFsStorageFactory))
                    .load(
                        "local-overwrite",
                        HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), uri.clone())]),
                    )
                    .await
                    .unwrap();
                let ns = iceberg::NamespaceIdent::new("default".to_string());
                let _ = scratch.create_namespace(&ns, HashMap::new()).await;
                let creation = iceberg::TableCreation::builder()
                    .name("test".to_string())
                    .schema(super::schema_version_to_iceberg(&schema_version()).unwrap())
                    .location(uri)
                    .build();
                scratch.create_table(&ns, creation).await.unwrap();
                // The scratch catalog dies here without drop_table — exactly
                // like a process crash before the hint flip.
            }
            let reopened = IcebergNativeTwoPhaseCommit::open(dir.path(), "test", &schema_version())
                .await
                .expect("pre-flip crash state must reopen from the old generation");
            let rows: usize = reopened
                .read_all()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum();
            assert_eq!(rows, 3, "old generation must remain fully readable");

            // (b) A completed overwrite: hint target must exist and read back.
            reopened
                .overwrite_commit(vec![batch(vec![7, 8])], BTreeMap::new(), &schema_version())
                .await
                .unwrap();
            let hint =
                std::fs::read_to_string(dir.path().join("metadata").join(super::VERSION_HINT))
                    .unwrap();
            let hinted = hint.trim().strip_prefix("file://").unwrap_or(hint.trim());
            assert!(
                std::path::Path::new(hinted).exists(),
                "version-hint must point at an existing metadata file, got {hinted}"
            );
            drop(reopened);
            let after = IcebergNativeTwoPhaseCommit::open(dir.path(), "test", &schema_version())
                .await
                .unwrap();
            let rows: usize = after
                .read_all()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum();
            assert_eq!(rows, 2, "post-flip generation must be the new data");
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
