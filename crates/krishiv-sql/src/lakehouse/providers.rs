//! Register delta/hudi URI tables with DataFusion (R18 S1.2, S2.3).

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use krishiv_connectors::lakehouse::{AsOfSpec, HudiQueryType, HudiSnapshotReader};

use crate::SqlError;
use crate::SqlResult;

/// Register `delta.<path>` as a logical table name `delta_<sanitized>`.
pub async fn register_delta_uri(
    ctx: &SessionContext,
    table_name: &str,
    path: &str,
    version: Option<i64>,
) -> SqlResult<()> {
    let _ = ctx.deregister_table(table_name);
    let handle = krishiv_connectors::lakehouse::DeltaTableHandle::open(path, version)
        .await
        .map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
    let schema = handle.schema().await.map_err(|e| SqlError::DataFusion {
        message: e.to_string(),
    })?;
    let provider = Arc::new(DeltaScanProvider { handle, schema });
    ctx.register_table(table_name, provider)
        .map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
    Ok(())
}

/// Register `hudi.<path>` table.
pub async fn register_hudi_uri(
    ctx: &SessionContext,
    table_name: &str,
    path: &str,
    query_type: HudiQueryType,
    begin_instant: Option<&str>,
) -> SqlResult<()> {
    let _ = ctx.deregister_table(table_name);
    let reader = {
        let mut r = HudiSnapshotReader::open(path).with_query_type(query_type);
        if let Some(inst) = begin_instant {
            r = r.with_begin_instant(inst);
        }
        r
    };
    // `HudiSnapshotReader::schema` does blocking filesystem/Parquet-decode I/O
    // (unlike the object-store-backed Hudi reader, which is genuinely async).
    // Run it on the blocking pool so registering a Hudi table doesn't stall
    // this async DataFusion/Flight SQL task.
    let reader_for_schema = reader.clone();
    let schema = tokio::task::spawn_blocking(move || reader_for_schema.schema())
        .await
        .map_err(|e| SqlError::DataFusion {
            message: format!("hudi schema read task panicked: {e}"),
        })?
        .map_err(|e| SqlError::DataFusion {
            message: format!("hudi: failed to read table schema: {e}"),
        })?;
    let provider = Arc::new(HudiScanProvider { reader, schema });
    ctx.register_table(table_name, provider)
        .map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
    Ok(())
}

#[derive(Debug)]
struct DeltaScanProvider {
    handle: krishiv_connectors::lakehouse::DeltaTableHandle,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for DeltaScanProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&datafusion::logical_expr::Expr],
    ) -> DfResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        // Inexact: the Parquet reader prunes row groups by statistics, which
        // discards non-matching data but does not guarantee every surviving row
        // matches. DataFusion keeps its own filter above, so this is a pure
        // reduction in bytes read.
        Ok(vec![
            datafusion::logical_expr::TableProviderFilterPushDown::Inexact;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Scan the snapshot's Parquet files directly instead of reading the
        // whole table into a `MemTable` first.
        //
        // The old path called `scan_batches()` — which decodes **every file of
        // every column** — and only then handed the result to `MemTable::scan`
        // for projection, filtering and limiting. So `SELECT one_col FROM t
        // LIMIT 10` read the entire table, and a Delta table larger than the
        // executor's memory could not be queried at all regardless of how
        // selective the query was.
        //
        // A Delta snapshot *is* a list of Parquet files, so handing that list
        // to DataFusion's own Parquet scan gives projection pushdown,
        // statistics-based row-group pruning, the limit, and per-file
        // parallelism — none of which a `MemTable` can offer.
        let files = krishiv_connectors::lakehouse::list_table_data_files(
            self.handle.path(),
            self.handle.version().map(|v| v as u64),
        )
        .map_err(|e| DataFusionError::External(e.to_string().into()))?;

        let table = parquet_files_table(files, self.schema(), state.config().target_partitions())?;
        table.scan(state, projection, filters, limit).await
    }
}

/// A `TableProvider` over an explicit list of local Parquet files.
///
/// An empty list becomes an empty `MemTable` rather than a `ListingTable`:
/// `ListingTable` with no paths cannot infer anything and errors, and "this
/// version of the table has no data files" is a legitimate state (an empty
/// table, or every file removed by a delete) that must scan to zero rows
/// instead of failing the query.
///
/// # Why the listing options are not left at their defaults
///
/// `ListingOptions::new` is not "defaults" in the usual sense — it sets
/// `collect_stat: false` and `target_partitions: 1`, and DataFusion expects a
/// caller to follow it with `.with_session_config_options(..)` (which is what
/// `register_parquet` does). Left as constructed it undoes the two things this
/// function exists to provide:
///
/// * **`collect_stat: false`** — no row counts and no byte sizes, so
///   `SpillableJoinSelection` and broadcast selection cannot size a build side
///   and every size-based decision defaults to "keep the hash join". Measured
///   on the SF100 cluster when the same defect reached the parquet-registration
///   path: `unmeasurable == hash_joins` in all 414 passes, **zero** conversions,
///   and two queries dying on unspillable build sides.
/// * **`target_partitions: 1`** — the file list is handed to the scan as a
///   single group, so a Delta snapshot of a hundred files is read serially.
///   The doc comment above promises "per-file parallelism"; that promise was
///   this one line short of true.
///
/// `target_partitions` comes from the scanning session rather than a constant
/// so a Delta scan splits exactly as wide as any other scan in the same query.
fn parquet_files_table(
    files: Vec<std::path::PathBuf>,
    schema: SchemaRef,
    target_partitions: usize,
) -> DfResult<Arc<dyn TableProvider>> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };

    // `file.exists()` mirrors `local_delta::read_table`, which skips missing
    // files rather than failing: a concurrent vacuum can remove a file between
    // listing and reading it.
    let urls: Vec<ListingTableUrl> = files
        .iter()
        .filter(|file| file.exists())
        .map(|file| ListingTableUrl::parse(file.to_string_lossy().as_ref()))
        .collect::<DfResult<Vec<_>>>()?;
    if urls.is_empty() {
        return Ok(Arc::new(MemTable::try_new(schema, vec![vec![]])?));
    }

    let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet")
        .with_collect_stat(true)
        .with_target_partitions(target_partitions.max(1));
    let config = ListingTableConfig::new_with_multi_paths(urls)
        .with_listing_options(options)
        // The table's declared schema, not one inferred per file: the Delta log
        // is the authority on the snapshot's schema, and inferring would also
        // cost a metadata read per file on every scan.
        .with_schema(schema);
    Ok(Arc::new(ListingTable::try_new(config)?))
}

#[derive(Debug)]
struct HudiScanProvider {
    reader: HudiSnapshotReader,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for HudiScanProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&datafusion::logical_expr::Expr],
    ) -> DfResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        Ok(vec![
            datafusion::logical_expr::TableProviderFilterPushDown::Inexact;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Resolve the query's files and let DataFusion's Parquet scan read
        // them, rather than `scan_batches()` decoding every row of every file
        // into a `MemTable` before projection, filtering or the limit apply.
        //
        // `parquet_files()` was written for exactly this — its own doc says
        // readers use it "to scan one file at a time instead of materializing
        // the whole table" — and this was a caller that ignored it, the same
        // way `schema()` did until it was fixed to read a footer instead of the
        // table. Commit selection (snapshot vs incremental, `begin_instant`)
        // still happens in the reader, so the file list is the query's, not the
        // whole table's.
        //
        // Listing is filesystem I/O, so it keeps the `spawn_blocking` the
        // decode used to need.
        let reader = self.reader.clone();
        let files = tokio::task::spawn_blocking(move || reader.parquet_files())
            .await
            .map_err(|e| DataFusionError::External(format!("hudi scan task panicked: {e}").into()))?
            .map_err(|e| DataFusionError::External(e.to_string().into()))?;

        let table = parquet_files_table(
            files,
            Arc::clone(&self.schema),
            state.config().target_partitions(),
        )?;
        table.scan(state, projection, filters, limit).await
    }
}

/// Register in-memory batches as a DataFusion table.
pub async fn register_scan_batches(
    ctx: &SessionContext,
    name: &str,
    batches: Vec<RecordBatch>,
) -> SqlResult<()> {
    // Allow overwriting an existing table (used by MERGE write-back).
    let _ = ctx.deregister_table(name);
    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
    let table = MemTable::try_new(schema, vec![batches]).map_err(|e| SqlError::DataFusion {
        message: e.to_string(),
    })?;
    ctx.register_table(name, Arc::new(table))
        .map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
    Ok(())
}

/// Apply `AS OF` qualifiers by re-registering pinned delta tables.
///
/// # Why this refuses instead of skipping
///
/// [`super::as_of::preprocess_as_of_sql`] **removes** the `AS OF` clause from
/// the SQL before DataFusion ever sees it, and collects a ref for *every*
/// table it appears on — not only `delta.`-prefixed ones. So anything this
/// function declines to honour is not merely unsupported: the qualifier has
/// already been erased, the query runs against the current snapshot, and
/// nothing anywhere reports it. A user asking for last quarter's numbers
/// silently receives today's.
///
/// Two shapes used to fall into that hole:
///
///   * any table not named `delta.<path>` — an Iceberg or catalog table with
///     `VERSION AS OF 3` was collected and then dropped on the floor;
///   * `delta.<path>` with a **timestamp**, because the spec was matched with
///     `AsOfSpec::Version(v) => Some(v), _ => None`, and `None` is exactly how
///     [`register_delta_uri`] spells "open the latest version".
///
/// Both now error. Returning wrong data quietly is the one outcome a
/// time-travel query must never produce.
pub async fn apply_as_of_refs(
    ctx: &SessionContext,
    refs: &[super::as_of::AsOfTableRef],
) -> SqlResult<()> {
    for reference in refs.iter() {
        let Some(path) = reference
            .table
            .strip_prefix("delta.")
            .map(|p| p.trim_matches('`'))
        else {
            return Err(SqlError::Unsupported {
                feature: format!(
                    "time travel on '{}': AS OF is currently resolved only for Delta tables \
                     referenced as delta.<path>. The qualifier has already been stripped from \
                     the query, so continuing would silently read the current snapshot instead \
                     of the requested one.",
                    reference.table
                ),
            });
        };
        let version = match reference.spec {
            AsOfSpec::Version(v) => v,
            AsOfSpec::Timestamp(ts) => {
                return Err(SqlError::Unsupported {
                    feature: format!(
                        "time travel on '{}' AS OF {ts}: resolving a timestamp to a Delta \
                         version is not implemented (DeltaTableHandle::open takes a version \
                         number). Use VERSION AS OF <n>. Proceeding would read the current \
                         snapshot, not the one at that timestamp.",
                        reference.table
                    ),
                });
            }
        };
        // Register under the alias `preprocess_as_of_sql` rewrote the query
        // to use. This used to register under `table.replace('.', "_")` — a
        // name the rewritten SQL never mentioned, so the pinned provider was
        // unreachable and the query named a table DataFusion could not
        // resolve. Nothing caught it because no test ran an AS OF query.
        register_delta_uri(ctx, &reference.alias, path, Some(version)).await?;
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use super::super::as_of::AsOfTableRef;
    use chrono::TimeZone;

    fn ctx() -> SessionContext {
        SessionContext::new()
    }

    /// Write a two-file Delta table of `Int64` rows 1..=6 and return its path.
    ///
    /// Two appends, so the table has two data files — which is what makes
    /// "did the scan read everything?" an answerable question.
    async fn two_file_delta_table(dir: &std::path::Path) -> String {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let path = dir.join("delta_tbl").to_string_lossy().into_owned();
        for chunk in [[1i64, 2, 3], [4, 5, 6]] {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(chunk.to_vec())),
                    Arc::new(Int64Array::from(chunk.iter().map(|v| v * 10).collect::<Vec<_>>())),
                ],
            )
            .unwrap();
            krishiv_connectors::lakehouse::write_delta(
                path.clone(),
                vec![batch],
                krishiv_connectors::lakehouse::DeltaWriteMode::Append,
                false,
            )
            .await
            .unwrap();
        }
        path
    }

    /// A Delta scan must be a Parquet scan, not a full read into memory.
    ///
    /// `DeltaScanProvider::scan` used to call `scan_batches()` — decoding every
    /// file and every column — and only then hand the result to
    /// `MemTable::scan` for projection, filtering and limiting. So
    /// `SELECT one_col LIMIT 1` read the whole table, and a Delta table larger
    /// than memory could not be queried however selective the query was.
    ///
    /// Asserting on the physical plan is the point: the *answers* were correct
    /// the whole time, so only the plan shows the difference.
    #[tokio::test]
    async fn a_delta_scan_reads_parquet_rather_than_draining_the_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = two_file_delta_table(dir.path()).await;
        let ctx = ctx();
        register_delta_uri(&ctx, "d", &path, None).await.unwrap();

        let logical = ctx
            .sql("SELECT a FROM d LIMIT 1")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        let physical = ctx.state().create_physical_plan(&logical).await.unwrap();
        let plan = format!(
            "{}",
            datafusion::physical_plan::displayable(physical.as_ref()).indent(false)
        );

        assert!(
            plan.contains("DataSourceExec") && plan.to_lowercase().contains("parquet"),
            "the scan must reach the Parquet files:\n{plan}"
        );
        assert!(
            !plan.contains("MemorySourceConfig") && !plan.contains("MemoryExec"),
            "the table must not be drained into memory first:\n{plan}"
        );
    }

    /// The rewrite must not change any answer.
    #[tokio::test]
    async fn a_delta_scan_still_returns_every_row_and_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = two_file_delta_table(dir.path()).await;
        let ctx = ctx();
        register_delta_uri(&ctx, "d", &path, None).await.unwrap();

        let batches = ctx
            .sql("SELECT a, b FROM d ORDER BY a")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 6, "both files must be scanned");

        let total = ctx
            .sql("SELECT sum(a) AS s, sum(b) AS t FROM d")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let sum_a = datafusion::common::cast::as_int64_array(total[0].column(0)).unwrap();
        let sum_b = datafusion::common::cast::as_int64_array(total[0].column(1)).unwrap();
        assert_eq!(sum_a.value(0), 21, "1+2+3+4+5+6");
        assert_eq!(sum_b.value(0), 210, "each b is 10x its a");
    }

    /// A snapshot with no data files scans to zero rows instead of failing.
    ///
    /// `ListingTable` cannot be built from an empty path list, and "no data
    /// files" is a legitimate state — an empty table, or every file removed —
    /// so the provider falls back to an empty in-memory table there.
    #[tokio::test]
    async fn an_empty_delta_snapshot_scans_to_zero_rows() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let table = parquet_files_table(Vec::new(), schema, 4).unwrap();
        let ctx = ctx();
        ctx.register_table("empty", table).unwrap();
        let batches = ctx.sql("SELECT * FROM empty").await.unwrap().collect().await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    }

    /// No qualifiers, nothing to do — the common path must stay quiet.
    #[tokio::test]
    async fn no_refs_is_a_no_op() {
        assert!(apply_as_of_refs(&ctx(), &[]).await.is_ok());
    }

    /// The silent-wrong-answer case: a time-travel qualifier on a table this
    /// cannot pin. `preprocess_as_of_sql` has already deleted the clause from
    /// the SQL, so skipping it means running against the current snapshot with
    /// no diagnostic anywhere.
    #[tokio::test]
    async fn a_non_delta_table_cannot_be_silently_ignored() {
        let refs = vec![AsOfTableRef {
            table: "orders".into(),
            alias: "__krishiv_as_of_0".into(),
            spec: AsOfSpec::Version(3),
        }];
        let err = apply_as_of_refs(&ctx(), &refs)
            .await
            .expect_err("AS OF on a non-delta table must not be dropped on the floor");
        let msg = err.to_string();
        assert!(msg.contains("orders"), "must name the table: {msg}");
        assert!(
            msg.contains("current snapshot"),
            "must say what the alternative would have done: {msg}"
        );
    }

    /// A timestamp on a Delta table used to become `version = None`, which is
    /// how `register_delta_uri` spells "latest" — so the query silently read
    /// the present.
    #[tokio::test]
    async fn a_timestamp_spec_is_refused_rather_than_read_as_latest() {
        let ts = chrono::Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let refs = vec![AsOfTableRef {
            table: "delta./tmp/does-not-matter".into(),
            alias: "__krishiv_as_of_0".into(),
            spec: AsOfSpec::Timestamp(ts),
        }];
        let err = apply_as_of_refs(&ctx(), &refs)
            .await
            .expect_err("a timestamp must not silently resolve to the latest version");
        assert!(
            err.to_string().contains("VERSION AS OF"),
            "the error should point at the supported spelling: {err}"
        );
    }

    /// Every spec variant must be handled explicitly. If a third one is added,
    /// this fails to compile rather than falling into a `_ => None` arm — which
    /// is precisely how the timestamp case became a silent latest-version read.
    #[test]
    fn every_as_of_spec_variant_is_accounted_for() {
        let ts = chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        for spec in [AsOfSpec::Version(1), AsOfSpec::Timestamp(ts)] {
            match spec {
                AsOfSpec::Version(_) | AsOfSpec::Timestamp(_) => {}
            }
        }
    }
}
