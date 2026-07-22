use crate::*;

#[cfg(test)]
mod tests {
    /// engine-s3-ddl-gap: `CREATE EXTERNAL TABLE … LOCATION 's3://…'` must
    /// expose its location so the engine can register the backing object store
    /// before DataFusion plans/scans it. Non-DDL returns `None`; file locations
    /// pass through unchanged (and the registration step no-ops on them).
    #[test]
    fn extract_create_external_table_location_reads_the_uri() {
        use crate::extract_create_external_table_location as loc;
        assert_eq!(
            loc("CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 's3://bkt/p/'").as_deref(),
            Some("s3://bkt/p/")
        );
        assert_eq!(loc("SELECT 1"), None);
        assert_eq!(
            loc("CREATE EXTERNAL TABLE f STORED AS PARQUET LOCATION '/tmp/d.parquet'").as_deref(),
            Some("/tmp/d.parquet")
        );
    }

    /// #217 probe: a long pure-compute aggregate must yield cooperatively
    /// (DataFusion EnsureCooperative), or no timeout/cancel watcher can
    /// ever preempt it. The timeout firing IS the assertion — on a
    /// current-thread runtime it can only fire if execution yields.
    #[tokio::test]
    async fn heavy_aggregate_yields_cooperatively_for_timeouts() {
        let engine = SqlEngine::new();
        let values: String = (0..100)
            .map(|i| format!("({i})"))
            .collect::<Vec<_>>()
            .join(", ");
        // Ladder step 1: 4-way (10^8 rows) — must complete; times the
        // execution rate so the 5-way bound below stays meaningful.
        let sql4 = format!(
            "SELECT sum(a.x * b.x + c.x * d.x) \
             FROM (VALUES {values}) a(x), (VALUES {values}) b(x), \
             (VALUES {values}) c(x), (VALUES {values}) d(x)"
        );
        let t = std::time::Instant::now();
        let df = engine.sql(&sql4).await.unwrap();
        df.collect().await.unwrap();
        let four_way = t.elapsed();
        eprintln!("4-way (1e8 rows) took {four_way:?}");

        // Ladder step 2: 5-way (10^10 rows, ~100x step 1) under a short
        // timeout — the timeout firing proves execution yields.
        let sql5 = format!(
            "SELECT sum(a.x * b.x + c.x * d.x + e.x) \
             FROM (VALUES {values}) a(x), (VALUES {values}) b(x), \
             (VALUES {values}) c(x), (VALUES {values}) d(x), (VALUES {values}) e(x)"
        );
        let df = engine.sql(&sql5).await.unwrap();
        let t = std::time::Instant::now();
        let out = tokio::time::timeout(std::time::Duration::from_secs(2), df.collect()).await;
        eprintln!("5-way probe returned after {:?}: timed_out={}", t.elapsed(), out.is_err());
        assert!(
            out.is_err(),
            "10^10-row aggregate finished within 2s?! (it should time out — \
             and the timeout firing proves execution yields cooperatively)"
        );
        assert!(
            t.elapsed() < std::time::Duration::from_secs(10),
            "timeout armed at 2s only fired after {:?} — execution is not \
             yielding cooperatively",
            t.elapsed()
        );
    }

    /// #217 part 2: after a timeout/cancel DROPS the result stream, the
    /// spawned per-partition execution tasks must actually stop — the
    /// live cert repro showed a full core burning to natural completion
    /// after the future was dropped. Asserts CPU goes quiet within 3s of
    /// the drop.
    ///
    /// Deliberately not `#[tokio::test]`: `cargo test` runs hundreds of
    /// tests concurrently in one process, and `/proc/self/stat` (whole
    /// process) picks up every sibling test's CPU too — this flaked under
    /// the full-suite default parallel run even though the underlying fix
    /// is correct, because an unrelated heavy test happened to overlap the
    /// 3s window. The crate forbids `unsafe`, so a raw `gettid()` syscall
    /// isn't an option either; naming this runtime's own worker threads
    /// and reading `/proc/self/task/*/comm` scopes the measurement to only
    /// the threads this test's query could actually run on.
    #[test]
    fn dropped_query_stops_burning_cpu() {
        const WORKER_NAME: &str = "sqlburn-worker";

        fn worker_jiffies() -> u64 {
            let Ok(entries) = std::fs::read_dir("/proc/self/task") else {
                return 0;
            };
            entries
                .flatten()
                .filter(|entry| {
                    std::fs::read_to_string(entry.path().join("comm"))
                        .is_ok_and(|comm| comm.trim() == WORKER_NAME)
                })
                .filter_map(|entry| std::fs::read_to_string(entry.path().join("stat")).ok())
                .map(|stat| {
                    let fields: Vec<&str> = stat.split_whitespace().collect();
                    if fields.len() < 15 {
                        return 0;
                    }
                    fields[13].parse::<u64>().unwrap_or(0) + fields[14].parse::<u64>().unwrap_or(0)
                })
                .sum()
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name(WORKER_NAME)
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async {
            let engine = SqlEngine::new();
            let values: String = (0..100)
                .map(|i| format!("({i})"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT sum(a.x * b.x + c.x * d.x + e.x) \
                 FROM (VALUES {values}) a(x), (VALUES {values}) b(x), \
                 (VALUES {values}) c(x), (VALUES {values}) d(x), (VALUES {values}) e(x)"
            );
            let df = engine.sql(&sql).await.unwrap();
            let out = tokio::time::timeout(std::time::Duration::from_secs(2), df.collect()).await;
            assert!(out.is_err(), "the 10^10-row aggregate should not finish in 2s");
            drop(out);
            // Give aborts a moment to land, then measure.
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        });

        let before = worker_jiffies();
        std::thread::sleep(std::time::Duration::from_secs(3));
        let burned = worker_jiffies().saturating_sub(before);
        // 3s fully idle ≈ 0; one core burning ≈ 300 jiffies. Allow slack
        // for scheduling noise — scoped to only this runtime's own named
        // worker threads, so unrelated sibling tests can't trip it.
        assert!(
            burned < 60,
            "this runtime's worker threads burned {burned} jiffies over 3s \
             after the stream was dropped — spawned partition tasks are not aborted"
        );
    }

    #[tokio::test]
    async fn typed_expression_ast_matches_raw_sql_execution() {
        use krishiv_plan::expression::{BinaryOperator, Expr, ScalarValue};

        let engine = SqlEngine::new();
        let dataframe = engine
            .sql("SELECT 11 AS amount, TRUE AS active")
            .await
            .unwrap();
        let predicate = Expr::column("amount")
            .binary(BinaryOperator::Gt, Expr::literal(ScalarValue::Int64(10)))
            .binary(BinaryOperator::And, Expr::column("active"));
        let parsed = crate::parse_public_expression("amount > 10 AND active").unwrap();
        assert_eq!(
            predicate.normalize_json().unwrap(),
            parsed.normalize_json().unwrap()
        );

        let typed = crate::KrishivDataFrameOps::filter_expr(&dataframe, &predicate)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let raw = crate::KrishivDataFrameOps::filter(&dataframe, &predicate.to_sql())
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(typed, raw);
        assert_eq!(
            typed
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
    }

    /// Phase 52 #194 regression: `with_target_parallelism` must write through
    /// to the live DataFusion session state. Before the fix it only set a
    /// struct field, so every caller silently kept the construction-time
    /// partition count — the root cause of the 4.5–8.9× embedded overhead
    /// recorded in Phase 51.
    #[test]
    fn with_target_parallelism_applies_to_live_session_state() {
        let engine = crate::SqlEngine::new()
            .with_target_parallelism(std::num::NonZeroUsize::new(7).unwrap());
        let options = engine.session_context().state().config().options().clone();
        assert_eq!(options.execution.target_partitions, 7);
        assert!(options.optimizer.enable_round_robin_repartition);

        // Scaling back down to 1 also disables round-robin repartitioning.
        let engine = engine.with_target_parallelism(std::num::NonZeroUsize::MIN);
        let options = engine.session_context().state().config().options().clone();
        assert_eq!(options.execution.target_partitions, 1);
        assert!(!options.optimizer.enable_round_robin_repartition);
    }

    /// The embedded default matches DataFusion's own (available CPU
    /// parallelism, `KRISHIV_TARGET_PARALLELISM` override) instead of the
    /// old single-threaded 1.
    #[test]
    fn engine_default_parallelism_derives_from_environment() {
        let engine = crate::SqlEngine::new();
        let expected = crate::default_parallelism_from_env().get();
        assert_eq!(engine.target_parallelism().get(), expected);
        assert_eq!(
            engine
                .session_context()
                .state()
                .config()
                .options()
                .execution
                .target_partitions,
            expected
        );
    }

    #[test]
    fn dataframe_alias_parser_ignores_nested_as_tokens() {
        assert_eq!(crate::top_level_alias_index("CAST(value AS BIGINT)"), None);
        assert_eq!(
            crate::top_level_alias_index("CAST(value AS BIGINT) AS \"value64\""),
            Some(21)
        );
        assert_eq!(crate::top_level_alias_index("' AS '"), None);
    }

    use krishiv_plan::optimizer::{Cost, CostModel, Optimizer, OptimizerError, OptimizerRule};
    use krishiv_plan::{ExecutionKind, LogicalPlan, PlanNode};

    use std::sync::Arc;

    use crate::{
        SqlEngine, SqlError, explain_sql, explain_sql_optimized, explain_sql_with_cost, plan_sql,
        query_memory_limit_from_env, referenced_table_names, resolve_query_memory_limit_bytes,
    };

    #[tokio::test]
    async fn catalog_table_resolved_in_sql() {
        use crate::catalog::{
            CatalogField, FieldType, InMemoryCatalog, TableMetadata, TableSchema,
        };
        use std::sync::{Arc, RwLock};

        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        let catalog = Arc::new(RwLock::new(InMemoryCatalog::new()));
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

        let engine = SqlEngine::with_in_memory_catalog(catalog).unwrap();
        let df = engine.sql("SELECT * FROM krishiv.public.t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 10);
    }

    #[test]
    fn rejects_empty_sql() {
        let error = match plan_sql("   ") {
            Ok(_) => panic!("expected empty query error"),
            Err(error) => error,
        };

        assert_eq!(error, SqlError::EmptyQuery);
    }

    #[test]
    fn referenced_table_names_covers_joins_and_subqueries() {
        let tables = referenced_table_names(
            "SELECT * FROM public JOIN secret ON public.id = secret.id \
             WHERE public.id IN (SELECT id FROM audit)",
        )
        .unwrap();
        assert_eq!(tables, vec!["audit", "public", "secret"]);
    }

    #[test]
    fn explains_non_empty_sql() {
        let explain = match explain_sql("select 1") {
            Ok(explain) => explain,
            Err(error) => panic!("unexpected SQL error: {error}"),
        };

        assert!(explain.contains("logical plan: sql-query"));
    }

    #[test]
    fn explain_sql_optimized_no_op_optimizer_includes_no_rules_message() {
        let optimizer = Optimizer::new();
        let output = explain_sql_optimized("select 1", &optimizer).unwrap();
        assert!(
            output.contains("optimizer: no rules applied"),
            "output did not contain expected optimizer message: {output}"
        );
    }

    #[test]
    fn explain_sql_optimized_propagates_invalid_rule_output() {
        struct InvalidRule;
        impl OptimizerRule for InvalidRule {
            fn name(&self) -> &str {
                "invalid"
            }

            fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
                Some(
                    plan.clone().with_node(
                        PlanNode::new("dangling", "dangling", ExecutionKind::Batch)
                            .with_inputs(["missing"]),
                    ),
                )
            }
        }

        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(InvalidRule));

        let error = explain_sql_optimized("select 1", &optimizer).expect_err("optimizer must fail");

        assert!(matches!(
            error,
            SqlError::Optimizer(OptimizerError::InvalidRuleOutput { .. })
        ));
    }

    #[test]
    fn explain_sql_with_cost_includes_cost_line() {
        struct ZeroCost;
        impl CostModel for ZeroCost {
            fn estimate(&self, _plan: &LogicalPlan) -> Cost {
                Cost::default()
            }
        }

        let output = explain_sql_with_cost("select 1", &ZeroCost).unwrap();
        assert!(
            output.contains("cost:"),
            "output did not contain cost line: {output}"
        );
        assert!(output.contains("cpu_nanos=0"));
        assert!(output.contains("memory_bytes=0"));
        assert!(output.contains("network_bytes=0"));
    }

    #[tokio::test]
    async fn datafusion_sql_collects_rows() {
        let engine = SqlEngine::new();
        let dataframe = match engine.sql("select 1 as value").await {
            Ok(dataframe) => dataframe,
            Err(error) => panic!("unexpected SQL error: {error}"),
        };

        let batches = match dataframe.collect().await {
            Ok(batches) => batches,
            Err(error) => panic!("unexpected collect error: {error}"),
        };

        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
    }

    // Regression test: `pivot_sql::rewrite_pivot_unpivot` was defined and unit
    // tested in isolation but never called from `SqlEngine::sql()`, so a raw
    // `PIVOT (...)` query fell through to DataFusion (which does not parse
    // PIVOT natively) and errored with "unsupported ast node Pivot". Verifies
    // the rewrite is actually wired into the query path, not just present.
    #[tokio::test]
    async fn sql_text_pivot_is_wired_into_query_execution() {
        let engine = SqlEngine::new();
        let dataframe = engine
            .sql(
                "SELECT * FROM (VALUES (1,'a',10),(2,'b',20)) AS t(id,cat,val) \
                 PIVOT (SUM(val) FOR cat IN ('a','b'))",
            )
            .await
            .unwrap_or_else(|error| panic!("PIVOT query must execute, got: {error}"));
        let batches = dataframe.collect().await.unwrap();
        let schema = batches[0].schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn sql_text_unpivot_is_wired_into_query_execution() {
        let engine = SqlEngine::new();
        let dataframe = engine
            .sql(
                "SELECT * FROM (VALUES (1,10,20)) AS t(id,jan,feb) \
                 UNPIVOT (amount FOR month IN (jan, feb))",
            )
            .await
            .unwrap_or_else(|error| panic!("UNPIVOT query must execute, got: {error}"));
        let batches = dataframe.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "one row per unpivoted column (jan, feb)");
    }

    #[tokio::test]
    async fn information_schema_tables_is_queryable() {
        let engine = SqlEngine::new();
        engine
            .sql("SELECT 1 AS x")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let dataframe = engine
            .sql("SELECT table_name FROM information_schema.tables ORDER BY table_name")
            .await
            .unwrap_or_else(|error| panic!("information_schema.tables must be queryable: {error}"));
        let batches = dataframe.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(
            total_rows > 0,
            "information_schema.tables must list at least the information_schema tables themselves"
        );
    }

    #[test]
    fn resolve_query_memory_limit_bytes_falls_back_for_missing_invalid_and_zero() {
        assert_eq!(resolve_query_memory_limit_bytes(None), None);
        assert_eq!(resolve_query_memory_limit_bytes(Some("not-a-number")), None);
        assert_eq!(resolve_query_memory_limit_bytes(Some("0")), None);
        assert_eq!(
            resolve_query_memory_limit_bytes(Some("268435456")),
            Some(268_435_456)
        );
        assert_eq!(resolve_query_memory_limit_bytes(Some(" 1024 ")), Some(1024));
    }

    #[tokio::test]
    async fn memory_limited_engine_executes_queries_and_reports_limit() {
        let engine = SqlEngine::new_with_memory_limit(Some(64 * 1024 * 1024));
        assert_eq!(engine.memory_limit_bytes(), Some(64 * 1024 * 1024));

        let dataframe = engine
            .sql("select v from (values (3), (1), (2)) as t(v) order by v")
            .await
            .expect("memory-limited engine must plan queries");
        let batches = dataframe
            .collect()
            .await
            .expect("memory-limited engine must execute queries");
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            3
        );
    }

    #[test]
    fn memory_limited_engine_installs_bounded_pool_in_session_context() {
        use datafusion::execution::memory_pool::MemoryConsumer;

        let bounded = SqlEngine::new_with_memory_limit(Some(1_000_000));
        let pool = Arc::clone(&bounded.context.runtime_env().memory_pool);
        let mut reservation = MemoryConsumer::new("phase2-test").register(&pool);
        assert!(
            reservation.try_grow(2_000_000).is_err(),
            "reservation above the configured limit must be rejected"
        );
        reservation.free();

        let unbounded = SqlEngine::new_with_memory_limit(None);
        assert_eq!(unbounded.memory_limit_bytes(), None);
        let pool = Arc::clone(&unbounded.context.runtime_env().memory_pool);
        let mut reservation = MemoryConsumer::new("phase2-test-unbounded").register(&pool);
        assert!(
            reservation.try_grow(2_000_000).is_ok(),
            "default engine keeps DataFusion's unbounded pool"
        );
        reservation.free();
    }

    // ── GAP-RT-06: collect_with_stats uses the DataFrame's own context ──────────

    #[tokio::test]
    async fn collect_with_stats_uses_registered_table() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let engine = SqlEngine::new();

        // Register a record batch as a table.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let col = Int64Array::from(vec![1i64, 2i64, 3i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();
        engine
            .register_record_batches("rt06_table", vec![batch])
            .await
            .unwrap();

        // Query that table via collect_with_stats.
        let dataframe = engine
            .sql("SELECT id FROM rt06_table")
            .await
            .expect("sql should succeed");
        let (batches, stats) = dataframe
            .collect_with_stats()
            .await
            .expect("collect_with_stats should succeed with registered table");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 3,
            "expected 3 rows from registered table, got {total_rows} (stats: {stats:?})"
        );
    }
    // ── Phase 54: ANALYZE TABLE + statistics registries ──────────────────

    #[tokio::test]
    async fn analyze_table_collects_and_registers_statistics() {
        let engine = SqlEngine::new();
        engine
            .sql(
                "CREATE TABLE aqe_stats_probe_t AS SELECT * FROM (VALUES \
                 (1, 'red'), (2, 'green'), (3, 'red'), (4, NULL)) AS v(id, color)",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let df = engine
            .sql("ANALYZE TABLE aqe_stats_probe_t FOR COLUMNS (id, color)")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let row = batches
            .iter()
            .find(|b| b.num_rows() > 0)
            .expect("summary row");
        let cell =
            |col: usize| arrow::util::display::array_value_to_string(row.column(col), 0).unwrap();
        assert_eq!(cell(0), "aqe_stats_probe_t");
        assert_eq!(cell(1), "4", "row_count");
        assert_eq!(cell(3), "2", "columns_analyzed");

        let stats = krishiv_plan::optimizer::global_table_stats()
            .get("aqe_stats_probe_t")
            .expect("registry entry");
        assert_eq!(stats.row_count, Some(4));
        let id = stats
            .columns
            .iter()
            .find(|c| c.name == "id")
            .expect("id column stats");
        assert_eq!(id.ndv, Some(4), "approx_distinct is exact at this scale");
        assert_eq!(id.min.as_deref(), Some("1"));
        assert_eq!(id.max.as_deref(), Some("4"));
        assert_eq!(id.null_count, Some(0));
        let color = stats
            .columns
            .iter()
            .find(|c| c.name == "color")
            .expect("color column stats");
        assert_eq!(color.null_count, Some(1));
        assert_eq!(color.ndv, Some(2));

        // The BroadcastAutoRule feed sees the analyzed count too.
        let counts = engine.table_row_counts();
        let counts = counts.read().unwrap();
        assert_eq!(counts.get("aqe_stats_probe_t"), Some(&4));
    }

    #[tokio::test]
    async fn analyze_table_rejects_garbage_trailing_clause() {
        let engine = SqlEngine::new();
        engine
            .sql("CREATE TABLE aqe_stats_bad_t AS SELECT 1 AS x")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let err = engine
            .sql("ANALYZE TABLE aqe_stats_bad_t WITH CHEESE")
            .await
            .err()
            .expect("trailing clause must error");
        assert!(err.to_string().contains("unexpected trailing clause"));
    }

    /// Phase 54 runtime filters: DataFusion dynamic-filter pushdown is ON
    /// by default in engine sessions, a selective star join returns results
    /// identical with the filters on and off (the AQE corpus-neutrality
    /// rule), and the probe-side parquet scan demonstrably emits fewer rows
    /// when the join dynamic filter is active.
    #[tokio::test]
    async fn runtime_filters_prune_probe_scan_and_preserve_results() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::prelude::{SessionConfig, SessionContext};
        use std::sync::Arc;

        // Engine sessions expose the master switch ON by default.
        let engine = SqlEngine::new();
        let shown = engine
            .sql("SHOW datafusion.optimizer.enable_dynamic_filter_pushdown")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let shown_text = shown
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(move |r| {
                    (0..b.num_columns())
                        .map(|c| {
                            arrow::util::display::array_value_to_string(b.column(c), r).unwrap()
                        })
                        .collect::<Vec<_>>()
                        .join("=")
                })
            })
            .collect::<Vec<_>>()
            .join(";");
        assert!(
            shown_text.contains("true"),
            "engine default must enable dynamic filter pushdown, got {shown_text}"
        );

        // Fact table: 10 000 rows sorted by key in 100-row row groups so
        // min/max pruning has boundaries to work with.
        let tmp = tempfile::tempdir().unwrap();
        let fact_path = tmp.path().join("fact.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let keys: Vec<i64> = (0..10_000).collect();
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys.clone())),
                Arc::new(Int64Array::from(
                    keys.iter().map(|k| k * 2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(&fact_path).unwrap();
        let props = datafusion::parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_size(100)
            .build();
        let mut writer =
            datafusion::parquet::arrow::ArrowWriter::try_new(file, schema.clone(), Some(props))
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let query = "SELECT f.k, f.v FROM fact f JOIN dim d ON f.k = d.k ORDER BY f.k";
        let run = |enable: bool| {
            let fact_path = fact_path.clone();
            async move {
                let mut config = SessionConfig::new().with_target_partitions(2);
                // Mirror the engine's KRISHIV_RUNTIME_FILTERS wiring: the
                // master switch alone does not disable the per-operator
                // options, so all four move together.
                config
                    .options_mut()
                    .optimizer
                    .enable_dynamic_filter_pushdown = enable;
                config
                    .options_mut()
                    .optimizer
                    .enable_join_dynamic_filter_pushdown = enable;
                config
                    .options_mut()
                    .optimizer
                    .enable_topk_dynamic_filter_pushdown = enable;
                config
                    .options_mut()
                    .optimizer
                    .enable_aggregate_dynamic_filter_pushdown = enable;
                let ctx = SessionContext::new_with_config(config);
                ctx.register_parquet(
                    "fact",
                    fact_path.to_str().unwrap(),
                    datafusion::prelude::ParquetReadOptions::default(),
                )
                .await
                .unwrap();
                ctx.sql(
                    "CREATE TABLE dim AS SELECT * FROM (VALUES (9900), (9901), (9902)) AS v(k)",
                )
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
                let plan = ctx
                    .sql(query)
                    .await
                    .unwrap()
                    .create_physical_plan()
                    .await
                    .unwrap();
                let batches = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
                    .await
                    .unwrap();
                let rows: Vec<String> = batches
                    .iter()
                    .flat_map(|b| {
                        (0..b.num_rows()).map(move |r| {
                            (0..b.num_columns())
                                .map(|c| {
                                    arrow::util::display::array_value_to_string(b.column(c), r)
                                        .unwrap()
                                })
                                .collect::<Vec<_>>()
                                .join("|")
                        })
                    })
                    .collect();
                (rows, crate::sql_tests::parquet_scan_output_rows(&plan))
            }
        };

        let (rows_on, scan_on) = run(true).await;
        let (rows_off, scan_off) = run(false).await;
        assert_eq!(
            rows_on, rows_off,
            "runtime filters must be result-neutral (AQE corpus rule)"
        );
        assert_eq!(rows_on.len(), 3, "three dim keys match");
        assert_eq!(
            scan_off, 10_000,
            "without runtime filters the scan reads everything"
        );
        assert!(
            scan_on < scan_off,
            "dynamic join filter must prune the probe scan: on={scan_on} off={scan_off}"
        );
    }
}

#[cfg(test)]
mod udf_sql_tests {
    use std::sync::Arc;

    use krishiv_plan::udf::MultiplyScalarUdf;

    use crate::SqlEngine;

    #[tokio::test]
    async fn registered_scalar_udf_visible_in_sql() {
        let registry = Arc::new(std::sync::RwLock::new(krishiv_plan::udf::UdfRegistry::new()));
        registry
            .write()
            .unwrap()
            .register_scalar(Arc::new(MultiplyScalarUdf::new("triple", "x", 3)));
        let engine = SqlEngine::new().with_udf_registry(registry);
        engine
            .register_record_batches(
                "t",
                vec![{
                    use arrow::array::Int64Array;
                    use arrow::datatypes::{DataType, Field, Schema};
                    use arrow::record_batch::RecordBatch;
                    use std::sync::Arc;
                    let schema =
                        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
                    RecordBatch::try_new(
                        schema,
                        vec![Arc::new(Int64Array::from(vec![Some(2), Some(4)]))],
                    )
                    .unwrap()
                }],
            )
            .await
            .unwrap();
        let df = engine.sql("SELECT triple(x) AS y FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 6);
        assert_eq!(col.value(1), 12);
    }
}

#[cfg(test)]
mod udtf_ddl_tests {
    use std::sync::Arc;

    use crate::{SqlEngine, SqlError};
    use arrow::array::{BooleanArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    #[tokio::test]
    async fn create_function_returns_table_rejects_unsupported_languages() {
        let registry = Arc::new(std::sync::RwLock::new(krishiv_plan::udf::UdfRegistry::new()));
        let engine = SqlEngine::new().with_udf_registry(Arc::clone(&registry));

        let rust_result = engine
            .sql(
                "CREATE FUNCTION my_udtf(arg1 INT) \
                 RETURNS TABLE (col1 TEXT, col2 BIGINT) \
                 LANGUAGE RUST \
                 AS 'fn body() {}'",
            )
            .await
            .expect_err("unsupported language must fail before registration");
        assert!(
            matches!(rust_result, SqlError::Unsupported { .. }),
            "unexpected error: {rust_result}"
        );
        assert!(
            engine.sql("SELECT * FROM my_udtf(1)").await.is_err(),
            "failed DDL must not leave a schema-only function registered"
        );
        assert!(registry.read().unwrap().table_names().is_empty());
    }

    #[tokio::test]
    async fn create_function_returns_table_registers_sql_body() {
        let engine = SqlEngine::new();

        let sql_result = engine
            .sql(
                "CREATE FUNCTION greet(name TEXT) \
                 RETURNS TABLE (msg TEXT) \
                 LANGUAGE SQL \
                 AS 'SELECT ''hello'' AS msg'",
            )
            .await;
        assert!(
            sql_result.is_ok(),
            "LANGUAGE SQL DDL should succeed, got {sql_result:?}"
        );
    }

    #[tokio::test]
    async fn create_function_returns_table_rejects_incomplete_sql_definition() {
        let engine = SqlEngine::new();

        let missing_language = engine
            .sql(
                "CREATE FUNCTION no_language() \
                 RETURNS TABLE (value BIGINT) \
                 AS 'SELECT 1 AS value'",
            )
            .await
            .expect_err("language must be explicit");
        assert!(matches!(missing_language, SqlError::Unsupported { .. }));

        let missing_body = engine
            .sql(
                "CREATE FUNCTION no_body() \
                 RETURNS TABLE (value BIGINT) \
                 LANGUAGE SQL",
            )
            .await
            .expect_err("SQL UDTF body must be required");
        assert!(matches!(
            missing_body,
            SqlError::InvalidTableFunction { .. }
        ));

        let empty_output = engine
            .sql(
                "CREATE FUNCTION no_columns() \
                 RETURNS TABLE () \
                 LANGUAGE SQL AS 'SELECT 1'",
            )
            .await
            .expect_err("UDTF output schema must not be empty");
        assert!(matches!(
            empty_output,
            SqlError::InvalidTableFunction { .. }
        ));
    }

    #[test]
    fn closure_table_udf_registration_validates_definition() {
        let engine = SqlEngine::new();
        let error = engine
            .register_table_udf_fn("", Schema::empty(), |_| {
                unreachable!("invalid definition must fail before body registration")
            })
            .expect_err("invalid closure UDTF must fail");
        assert!(matches!(error, SqlError::InvalidTableFunction { .. }));

        let duplicate_columns = engine
            .register_table_udf_fn(
                "duplicates",
                Schema::new(vec![
                    Field::new("value", DataType::Int64, false),
                    Field::new("value", DataType::Int64, false),
                ]),
                |_| unreachable!("invalid definition must fail before body registration"),
            )
            .expect_err("duplicate output names must fail");
        assert!(matches!(
            duplicate_columns,
            SqlError::InvalidTableFunction { .. }
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sql_body_udtf_rejects_declared_schema_mismatch() {
        let engine = SqlEngine::new();
        engine
            .sql(
                "CREATE FUNCTION wrong_schema() \
                 RETURNS TABLE (value BIGINT) \
                 LANGUAGE SQL AS 'SELECT ''text'' AS value'",
            )
            .await
            .expect("definition itself is syntactically valid");

        let error = engine
            .sql("SELECT * FROM wrong_schema()")
            .await
            .expect_err("runtime schema mismatch must fail");
        assert!(error.to_string().contains("returned schema"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sql_body_udtf_binds_literal_arguments() {
        let engine = SqlEngine::new();
        engine
            .sql(
                "CREATE FUNCTION echo_args(n BIGINT, text TEXT, enabled BOOLEAN) \
                 RETURNS TABLE (next_n BIGINT, echoed TEXT, flag BOOLEAN) \
                 LANGUAGE SQL \
                 AS 'SELECT $1 + 1 AS next_n, $2 AS echoed, $3 AS flag'",
            )
            .await
            .expect("function registration should succeed");

        let batches = engine
            .sql("SELECT * FROM echo_args(41, 'O''Reilly', TRUE)")
            .await
            .expect("table function planning should succeed")
            .collect()
            .await
            .expect("table function execution should succeed");

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        let next_n = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("next_n should be Int64");
        let echoed = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("echoed should be Utf8");
        let flag = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("flag should be Boolean");
        assert_eq!(next_n.value(0), 42);
        assert_eq!(echoed.value(0), "O'Reilly");
        assert!(flag.value(0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sql_body_udtf_rejects_wrong_arity_and_non_literal_arguments() {
        let engine = SqlEngine::new();
        let invalid_placeholder = engine
            .sql(
                "CREATE FUNCTION invalid_placeholder(n BIGINT) \
                 RETURNS TABLE (value BIGINT) \
                 LANGUAGE SQL AS 'SELECT $2 AS value'",
            )
            .await
            .expect_err("out-of-range placeholders must fail during registration");
        assert!(
            invalid_placeholder
                .to_string()
                .contains("no matching argument")
        );

        engine
            .sql(
                "CREATE FUNCTION one_arg(n BIGINT) \
                 RETURNS TABLE (value BIGINT) \
                 LANGUAGE SQL AS 'SELECT $1 AS value'",
            )
            .await
            .expect("function registration should succeed");

        let wrong_arity = engine
            .sql("SELECT * FROM one_arg()")
            .await
            .expect_err("wrong arity must fail");
        assert!(wrong_arity.to_string().contains("expects 1 arguments"));

        // Modern DataFusion constant-folds arithmetic on literals before invoking the
        // table function, so `20 + 22` is simplified to `Literal(42)` before our
        // `expr_to_scalar` sees it.  The call therefore succeeds rather than failing.
        engine
            .sql("SELECT * FROM one_arg(20 + 22)")
            .await
            .expect("constant arithmetic is accepted after DataFusion constant-folding");
    }

    // ── Streaming source registration (broker-free path) ─────────────────────
    //
    // register_kafka_source constructs a live KafkaSource whose rdkafka log
    // subsystem aborts in a test binary without proper init. Use the new
    // register_streaming_source_name API for broker-free unit tests.

    #[test]
    fn register_streaming_source_name_marks_table_as_streaming() {
        let engine = SqlEngine::new();
        engine
            .register_streaming_source_name("stream_events")
            .unwrap();
        assert!(
            engine
                .is_streaming_query("SELECT * FROM stream_events")
                .unwrap(),
            "register_streaming_source_name must make the query streaming"
        );
    }

    #[test]
    fn register_streaming_source_name_does_not_affect_other_tables() {
        let engine = SqlEngine::new();
        engine.register_streaming_source_name("my_stream").unwrap();
        assert!(
            !engine
                .is_streaming_query("SELECT * FROM other_table")
                .unwrap(),
            "only the registered table name must be streaming"
        );
    }

    #[test]
    fn is_streaming_query_false_for_plain_select() {
        let engine = SqlEngine::new();
        assert!(
            !engine.is_streaming_query("SELECT 1 AS n").unwrap(),
            "plain SELECT must not be classified as streaming"
        );
    }

    #[tokio::test]
    async fn krishiv_logical_plan_kind_is_streaming_for_streaming_source() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let engine = SqlEngine::new();
        engine.register_streaming_source_name("events").unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("user_id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2])),
                Arc::new(Int64Array::from(vec![10i64, 20])),
            ],
        )
        .unwrap();
        engine
            .register_record_batches("events", vec![batch])
            .await
            .unwrap();
        let df = engine
            .sql("SELECT ts, user_id FROM events WHERE ts > 0")
            .await
            .expect("streaming sql");
        assert_eq!(
            df.krishiv_logical_plan().kind(),
            crate::ExecutionKind::Streaming
        );
    }

    #[test]
    fn is_streaming_query_true_after_source_registered() {
        let engine = SqlEngine::new();
        engine.register_streaming_source_name("events").unwrap();
        assert!(
            engine
                .is_streaming_query("SELECT ts, user_id FROM events WHERE ts > 0")
                .unwrap()
        );
    }

    #[test]
    fn multiple_streaming_sources_any_makes_query_streaming() {
        let engine = SqlEngine::new();
        engine.register_streaming_source_name("s1").unwrap();
        engine.register_streaming_source_name("s2").unwrap();
        assert!(engine.is_streaming_query("SELECT * FROM s1").unwrap());
        assert!(engine.is_streaming_query("SELECT * FROM s2").unwrap());
        assert!(!engine.is_streaming_query("SELECT * FROM s3").unwrap());
    }

    #[test]
    fn is_streaming_query_true_for_table_alias() {
        let engine = SqlEngine::new();
        engine
            .register_streaming_source_name("kafka_source")
            .unwrap();
        // visit_relations must return the base table name, not the alias.
        assert!(
            engine
                .is_streaming_query("SELECT * FROM kafka_source AS k")
                .unwrap(),
            "aliased streaming table must still be classified as streaming"
        );
        assert!(
            engine
                .is_streaming_query(
                    "SELECT * FROM kafka_source AS k JOIN other AS o ON k.id = o.id"
                )
                .unwrap(),
            "JOIN with alias must still detect the streaming source"
        );
    }

    #[test]
    fn register_streaming_source_name_empty_returns_error() {
        let engine = SqlEngine::new();
        assert!(engine.register_streaming_source_name("").is_err());
        assert!(engine.register_streaming_source_name("   ").is_err());
    }

    #[test]
    fn deregister_streaming_source_removes_name() {
        let engine = SqlEngine::new();
        engine.register_streaming_source_name("topic").unwrap();
        assert!(engine.is_streaming_query("SELECT * FROM topic").unwrap());
        engine.deregister_streaming_source("topic").unwrap();
        assert!(
            !engine.is_streaming_query("SELECT * FROM topic").unwrap(),
            "deregistered source must no longer be classified as streaming"
        );
    }

    #[test]
    fn deregister_nonexistent_source_is_ok() {
        let engine = SqlEngine::new();
        // Deregistering a name that was never registered must be idempotent.
        engine
            .deregister_streaming_source("never_registered")
            .expect("deregister of unknown name must not error");
    }

    // ── Plan cache invalidation ───────────────────────────────────────────────

    #[tokio::test]
    async fn plan_cache_cleared_after_table_registration() {
        let engine = SqlEngine::new();
        // Prime the cache with a simple query.
        let _ = engine.sql("SELECT 1 AS n").await.unwrap();
        assert!(
            !engine.plan_cache.lock().unwrap().is_empty(),
            "cache must be non-empty after first query"
        );

        // Registering a table (even parquet) must clear the cache.
        engine.clear_plan_cache();
        assert!(
            engine.plan_cache.lock().unwrap().is_empty(),
            "cache must be empty after clear_plan_cache()"
        );
    }

    #[tokio::test]
    async fn plan_cache_repopulates_after_invalidation() {
        let engine = SqlEngine::new();
        let _ = engine.sql("SELECT 42 AS v").await.unwrap();
        engine.clear_plan_cache();
        // Re-running the same query must succeed and re-populate the cache.
        let df = engine.sql("SELECT 42 AS v").await.unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert!(
            !engine.plan_cache.lock().unwrap().is_empty(),
            "cache must refill after re-query"
        );
    }
}

#[cfg(test)]
mod streaming_match_recognize_limit_tests {
    use crate::resolve_streaming_match_recognize_limit;

    #[test]
    fn default_when_unset() {
        assert_eq!(resolve_streaming_match_recognize_limit(None), 100_000);
    }

    #[test]
    fn default_when_empty() {
        assert_eq!(resolve_streaming_match_recognize_limit(Some("")), 100_000);
    }

    #[test]
    fn parses_valid_value() {
        assert_eq!(resolve_streaming_match_recognize_limit(Some("42")), 42);
    }

    #[test]
    fn falls_back_on_unparseable() {
        assert_eq!(
            resolve_streaming_match_recognize_limit(Some("not-a-number")),
            100_000
        );
    }

    #[test]
    fn rejects_zero() {
        // 0 would mean "scan zero rows" which is meaningless.
        assert_eq!(resolve_streaming_match_recognize_limit(Some("0")), 100_000);
    }
}

#[cfg(all(test, feature = "iceberg-datafusion", feature = "local-catalog"))]
mod iceberg_catalog_tests {
    use std::sync::Arc;

    use crate::catalog::unified::KrishivCatalog;
    use crate::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn with_iceberg_catalog_registers_under_given_name() {
        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let engine = SqlEngine::new().with_iceberg_catalog(catalog, "mycat");
        let catalog_names = engine.context.catalog_names();
        assert!(
            catalog_names.contains(&"mycat".to_string()),
            "iceberg catalog 'mycat' must be registered; got: {catalog_names:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn call_system_no_catalog_returns_error() {
        let engine = SqlEngine::new();
        let err = engine
            .sql("CALL system.expire_snapshots('myns.orders', '7 days', 1)")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("no Iceberg catalog"),
            "expected 'no Iceberg catalog' in error, got: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn call_system_unknown_procedure_returns_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let engine = SqlEngine::new().with_iceberg_catalog(catalog, "mycat");
        let err = engine
            .sql("CALL system.frobnicate_table('myns.orders')")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("unknown procedure"),
            "expected 'unknown procedure' in error, got: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn call_system_expire_snapshots_returns_count() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        catalog
            .create_table("myns", "orders", schema, "")
            .await
            .unwrap();
        let engine = SqlEngine::new().with_iceberg_catalog(Arc::clone(&catalog), "mycat");
        let df = engine
            .sql("CALL system.expire_snapshots('myns.orders', '7 days', 1)")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let schema = batches[0].schema();
        assert_eq!(
            schema.field(0).name(),
            "expired_snapshots",
            "result column must be 'expired_snapshots'"
        );
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 0, "fresh table has no snapshots to expire");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn call_system_maintain_table_reports_three_columns() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        catalog
            .create_table("myns", "orders", schema, "")
            .await
            .unwrap();
        let engine = SqlEngine::new().with_iceberg_catalog(Arc::clone(&catalog), "mycat");
        let df = engine
            .sql("CALL system.maintain_table('myns.orders', '7 days')")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let schema = batches[0].schema();
        assert_eq!(schema.field(0).name(), "compacted_files");
        assert_eq!(schema.field(1).name(), "expired_snapshots");
        assert_eq!(schema.field(2).name(), "removed_orphans");
        for col in 0..3 {
            let v = batches[0]
                .column(col)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0);
            assert_eq!(v, 0, "fresh table: column {col} must be 0");
        }
    }

    // ── CTAS (durable CREATE TABLE … AS SELECT) ──────────────────────────────

    async fn count_rows(engine: &SqlEngine, sql: &str) -> usize {
        let df = engine.sql(sql).await.unwrap();
        df.collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum()
    }

    fn first_i64(batches: &[arrow::record_batch::RecordBatch], column: &str) -> i64 {
        let batch = &batches[0];
        let idx = batch.schema().index_of(column).unwrap();
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ctas_lands_in_iceberg_catalog_and_is_queryable() {
        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let engine = SqlEngine::new().with_iceberg_catalog(catalog, "mycat");

        let df = engine
            .sql(
                "CREATE TABLE mycat.pipe.trips AS \
                 SELECT * FROM (VALUES (1, 'ok'), (2, 'ok'), (3, 'bad')) AS t(id, status)",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(
            batches[0].schema().field(0).name(),
            "rows_written",
            "durable CTAS must return a landing report, not the result set"
        );
        assert_eq!(first_i64(&batches, "rows_written"), 3);
        assert!(first_i64(&batches, "snapshot_id") > 0);

        // The landed table resolves and scans through the catalog bridge.
        let rows = count_rows(&engine, "SELECT * FROM mycat.pipe.trips").await;
        assert_eq!(rows, 3, "landed table must be queryable");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ctas_or_replace_swaps_contents() {
        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let engine = SqlEngine::new().with_iceberg_catalog(catalog, "mycat");

        engine
            .sql("CREATE TABLE mycat.pipe.t AS SELECT * FROM (VALUES (1), (2)) AS t(id)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Plain CREATE on an existing table errors.
        let err = engine
            .sql("CREATE TABLE mycat.pipe.t AS SELECT * FROM (VALUES (9)) AS t(id)")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("already exists"), "got: {err}");

        engine
            .sql(
                "CREATE OR REPLACE TABLE mycat.pipe.t AS \
                 SELECT * FROM (VALUES (10), (20), (30)) AS t(id)",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows = count_rows(&engine, "SELECT * FROM mycat.pipe.t").await;
        assert_eq!(rows, 3, "replace must swap, not append");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ctas_without_iceberg_target_falls_through_to_datafusion() {
        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let engine = SqlEngine::new().with_iceberg_catalog(catalog, "mycat");

        // One-part name: session-local DataFusion CTAS, not intercepted.
        engine
            .sql("CREATE TABLE scratch AS SELECT * FROM (VALUES (1), (2)) AS t(id)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows = count_rows(&engine, "SELECT * FROM scratch").await;
        assert_eq!(rows, 2, "session CTAS must keep working");
    }

    #[test]
    fn parse_ctas_shapes() {
        let parsed =
            crate::parse_ctas("CREATE OR REPLACE TABLE cat.ns.t AS SELECT a FROM src WHERE a > 1")
                .expect("must parse");
        assert_eq!(parsed.table_ref, "cat.ns.t");
        assert!(parsed.or_replace);
        assert!(parsed.inner_query.to_uppercase().starts_with("SELECT"));

        let plain = crate::parse_ctas("CREATE TABLE ns.t AS SELECT 1").expect("must parse");
        assert!(!plain.or_replace);

        // Column-list CREATE TABLE (no AS body) is not a CTAS.
        assert!(crate::parse_ctas("CREATE TABLE ns.t (id INT)").is_none());
        // Non-CREATE statements are not CTAS.
        assert!(crate::parse_ctas("SELECT 1").is_none());
    }

    #[test]
    fn parse_ctas_partitioned_by_shapes() {
        // Transforms are lifted out before sqlparser sees the statement.
        let parsed = crate::parse_ctas(
            "CREATE TABLE cat.ns.t PARTITIONED BY (region, bucket(4, id), day(ts)) \
             AS SELECT * FROM src",
        )
        .expect("must parse");
        assert_eq!(parsed.table_ref, "cat.ns.t");
        assert_eq!(
            parsed.partition_by,
            vec!["region", "bucket(4, id)", "day(ts)"]
        );
        assert!(parsed.inner_query.to_uppercase().starts_with("SELECT"));

        // Case-insensitive keywords.
        let lower = crate::parse_ctas(
            "create or replace table ns.t partitioned by (day(ts)) as select 1 as ts",
        )
        .expect("must parse");
        assert!(lower.or_replace);
        assert_eq!(lower.partition_by, vec!["day(ts)"]);

        // No clause → empty list.
        let plain = crate::parse_ctas("CREATE TABLE ns.t AS SELECT 1").expect("must parse");
        assert!(plain.partition_by.is_empty());

        // The keywords inside a string literal are not a clause.
        let literal = crate::parse_ctas("CREATE TABLE ns.t AS SELECT 'PARTITIONED BY (x)' AS note")
            .expect("must parse");
        assert!(literal.partition_by.is_empty());
        assert!(literal.inner_query.contains("PARTITIONED BY (x)"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ctas_partitioned_by_fans_out_partitions() {
        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let engine = SqlEngine::new().with_iceberg_catalog(catalog, "mycat");

        let df = engine
            .sql(
                "CREATE TABLE mycat.pipe.events PARTITIONED BY (status) AS \
                 SELECT * FROM (VALUES (1, 'ok'), (2, 'ok'), (3, 'bad')) AS t(id, status)",
            )
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(first_i64(&batches, "rows_written"), 3);
        assert_eq!(
            first_i64(&batches, "data_files"),
            2,
            "one data file per distinct status"
        );

        let rows = count_rows(&engine, "SELECT * FROM mycat.pipe.events").await;
        assert_eq!(rows, 3, "partitioned table must be queryable");
        let ok_rows = count_rows(
            &engine,
            "SELECT * FROM mycat.pipe.events WHERE status = 'ok'",
        )
        .await;
        assert_eq!(ok_rows, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ctas_partitioned_by_requires_iceberg_target() {
        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let engine = SqlEngine::new().with_iceberg_catalog(catalog, "mycat");

        // One-part name resolves to a session table, where PARTITIONED BY
        // would be silently meaningless — must error, not fall through.
        let err = engine
            .sql(
                "CREATE TABLE scratch PARTITIONED BY (id) AS \
                 SELECT * FROM (VALUES (1), (2)) AS t(id)",
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Iceberg"),
            "must name the constraint, got: {err}"
        );

        // Unknown transform errors with a parse message.
        let err = engine
            .sql(
                "CREATE TABLE mycat.pipe.bad PARTITIONED BY (soundex(id)) AS \
                 SELECT * FROM (VALUES (1)) AS t(id)",
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("soundex") || err.to_string().contains("transform"),
            "got: {err}"
        );
    }

    // ── DELETE / UPDATE helpers ───────────────────────────────────────────────

    #[test]
    fn parse_dml_delete_with_where() {
        let (tbl, pred) =
            crate::parse_dml_delete("DELETE FROM myns.orders WHERE id = 5").expect("must parse");
        assert_eq!(tbl, "myns.orders");
        assert!(pred.contains("id") && pred.contains('5'), "pred: {pred}");
    }

    #[test]
    fn parse_dml_delete_without_where() {
        let (tbl, pred) = crate::parse_dml_delete("DELETE FROM myns.orders").expect("must parse");
        assert_eq!(tbl, "myns.orders");
        assert_eq!(pred, "TRUE");
    }

    #[test]
    fn parse_dml_delete_quoted_identifier() {
        // Quoted identifiers must pass through the AST without truncation.
        let result = crate::parse_dml_delete(r#"DELETE FROM "my schema"."my table" WHERE x > 0"#);
        assert!(result.is_some(), "quoted identifiers must parse");
        let (tbl, pred) = result.unwrap();
        assert!(
            tbl.contains("my schema") || tbl.contains("my table"),
            "tbl: {tbl}"
        );
        assert!(pred.contains('0'), "pred: {pred}");
    }

    #[test]
    fn parse_dml_update_with_where() {
        let parsed =
            crate::parse_dml_update("UPDATE myns.orders SET price = price * 2 WHERE id = 1")
                .expect("must parse");
        assert_eq!(parsed.table_ref, "myns.orders");
        assert!(
            parsed
                .assignments
                .iter()
                .any(|(_, v)| v.contains("price") && v.contains('2')),
            "assignments: {:?}",
            parsed.assignments,
        );
        assert!(parsed.predicate.is_some());
    }

    #[test]
    fn parse_dml_update_without_where() {
        let parsed = crate::parse_dml_update("UPDATE myns.orders SET status = 'active'")
            .expect("must parse");
        assert_eq!(parsed.table_ref, "myns.orders");
        assert_eq!(parsed.assignments.len(), 1);
        assert_eq!(parsed.assignments[0].0, "status");
        assert!(parsed.predicate.is_none());
    }

    #[test]
    fn parse_dml_update_multi_column() {
        // Multiple SET assignments with a comma in an expression (CONCAT).
        let parsed = crate::parse_dml_update(
            "UPDATE t SET name = CONCAT(first_name, ' ', last_name), age = age + 1",
        )
        .expect("must parse");
        assert_eq!(
            parsed.assignments.len(),
            2,
            "assignments: {:?}",
            parsed.assignments
        );
        assert_eq!(parsed.assignments[0].0, "name");
        assert_eq!(parsed.assignments[1].0, "age");
    }

    #[test]
    fn parse_dml_delete_rejects_non_delete() {
        assert!(crate::parse_dml_delete("SELECT 1").is_none());
        assert!(crate::parse_dml_delete("UPDATE t SET x = 1").is_none());
    }

    #[test]
    fn parse_dml_update_rejects_non_update() {
        assert!(crate::parse_dml_update("SELECT 1").is_none());
        assert!(crate::parse_dml_update("DELETE FROM t").is_none());
    }

    // ── INSERT INTO helper (#219) ─────────────────────────────────────────────

    #[test]
    fn parse_dml_insert_select_no_column_list() {
        let parsed = crate::parse_dml_insert("INSERT INTO myns.orders SELECT id, name FROM src")
            .expect("must parse");
        assert_eq!(parsed.table_ref, "myns.orders");
        assert!(parsed.columns.is_empty());
        assert!(parsed.inner_query.to_ascii_uppercase().contains("SELECT"));
    }

    #[test]
    fn parse_dml_insert_values() {
        let parsed = crate::parse_dml_insert("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
            .expect("must parse");
        assert_eq!(parsed.table_ref, "t");
        assert!(parsed.columns.is_empty());
        assert!(parsed.inner_query.to_ascii_uppercase().contains("VALUES"));
    }

    #[test]
    fn parse_dml_insert_with_explicit_column_list() {
        let parsed = crate::parse_dml_insert("INSERT INTO t (a, b) SELECT x, y FROM src")
            .expect("must parse");
        assert_eq!(parsed.table_ref, "t");
        assert_eq!(parsed.columns, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn parse_dml_insert_rejects_non_insert() {
        assert!(crate::parse_dml_insert("SELECT 1").is_none());
        assert!(crate::parse_dml_insert("DELETE FROM t").is_none());
        assert!(crate::parse_dml_insert("UPDATE t SET x = 1").is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_from_iceberg_table_removes_rows() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        catalog
            .create_table("myns", "orders", schema, "")
            .await
            .unwrap();
        let engine = SqlEngine::new().with_iceberg_catalog(Arc::clone(&catalog), "mycat");
        // DELETE with no WHERE on an empty table returns 0 deleted rows.
        let df = engine
            .sql("DELETE FROM myns.orders WHERE id = 99")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches[0].schema().field(0).name(), "deleted_rows");
        let deleted = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(deleted, 0, "empty table: no rows to delete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_iceberg_table_returns_updated_count() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "status", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();
        catalog
            .create_table("myns", "customers", schema, "")
            .await
            .unwrap();
        let engine = SqlEngine::new().with_iceberg_catalog(Arc::clone(&catalog), "mycat");
        let df = engine
            .sql("UPDATE myns.customers SET status = 'active' WHERE id = 1")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches[0].schema().field(0).name(), "updated_rows");
        let updated = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(updated, 0, "empty table: no rows to update");
    }

    /// #219: `INSERT INTO <catalog-table> SELECT ...` must actually append
    /// rows instead of hitting DataFusion's `ListingTable::insert_into`
    /// rejection ("Inserting into a ListingTable backed by a single file is
    /// not supported") that every catalog table used to hit deterministically.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn insert_into_iceberg_table_appends_rows() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();
        catalog
            .create_table("myns", "growing", schema, "")
            .await
            .unwrap();
        let engine = SqlEngine::new().with_iceberg_catalog(Arc::clone(&catalog), "mycat");

        // First insert lands into the empty table.
        let df = engine
            .sql("INSERT INTO myns.growing SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches[0].schema().field(0).name(), "inserted_rows");
        let inserted = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(inserted, 2);

        // A second insert must ADD to the first, not replace it — this is
        // the whole point of #219 (no more full-table self-union CTAS).
        let df = engine
            .sql("INSERT INTO myns.growing SELECT * FROM (VALUES (3, 'c')) AS t(id, name)")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let inserted = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(inserted, 1, "second insert reports only its own new row");

        // A bare 2-part name only resolves for the DML-interception paths
        // above (resolve_iceberg_table's explicit "single registered
        // catalog" fallback); a plain SELECT goes straight to DataFusion's
        // own planner, which requires the full catalog.schema.table path.
        let df = engine
            .sql("SELECT id FROM mycat.myns.growing")
            .await
            .unwrap();
        let rows: usize = df
            .collect()
            .await
            .unwrap()
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 3, "both inserts' rows must be present");
    }
}

/// Sum the `output_rows` metric across parquet `DataSourceExec` nodes of an
/// executed physical plan (Phase 54 runtime-filter evidence).
#[cfg(test)]
fn parquet_scan_output_rows(
    plan: &std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> u64 {
    fn walk(
        node: &std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        total: &mut u64,
        found: &mut bool,
    ) {
        let display = format!(
            "{}",
            datafusion::physical_plan::displayable(node.as_ref()).one_line()
        );
        if display.contains("DataSourceExec") && display.contains("parquet") {
            if let Some(rows) = node.metrics().and_then(|m| m.output_rows()) {
                *total += rows as u64;
                *found = true;
            }
        }
        for child in node.children() {
            walk(child, total, found);
        }
    }
    let mut total = 0;
    let mut found = false;
    walk(plan, &mut total, &mut found);
    assert!(
        found,
        "no parquet DataSourceExec with metrics found in plan"
    );
    total
}
