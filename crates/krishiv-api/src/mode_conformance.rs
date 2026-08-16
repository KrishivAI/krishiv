/// Embedded and remote mode conformance tests.
///
/// The claim under test: the same query over the same data returns the same
/// rows in every execution mode, because mode selects *where* data-plane work
/// runs and nothing else.
///
/// Audit (2026-08-16): that claim used to sit in this doc comment with nothing
/// behind it. Every executing test in this file built an **embedded** session;
/// the one single-node test was `#[ignore]`d and there were no distributed
/// tests at all, so the file asserted cross-mode identity and verified
/// single-mode correctness. Three real embedded/remote divergences were found
/// by hand that this file existed to catch.
///
/// `live_conformance` below closes that: it stands up a real in-process Flight
/// SQL server, points a remote session at it, and runs a corpus through both
/// modes comparing rendered row sets. It needs no external daemon, so it runs
/// in CI with everything else.
#[cfg(test)]
mod mode_conformance_tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::session::Session;
    use crate::types::ExecutionMode;

    fn make_batch(vals: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let arr: Int64Array = vals.iter().copied().collect();
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn embedded_session() -> Session {
        Session::builder()
            .with_execution_mode(ExecutionMode::Embedded)
            .build()
            .expect("embedded session")
    }

    fn collect_i64_from_session(session: &Session, sql: &str) -> Vec<i64> {
        let df = session.sql(sql).expect("sql");
        let result = df.collect().expect("collect");
        result
            .into_batches()
            .into_iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    // ── Embedded mode conformance ───────────────────────────────────────────

    #[test]
    fn embedded_literal_query_returns_correct_value() {
        let session = embedded_session();
        let vals = collect_i64_from_session(&session, "SELECT 2 + 2 AS v");
        assert_eq!(vals[0], 4);
    }

    #[test]
    fn embedded_session_mode_is_embedded() {
        let session = embedded_session();
        assert_eq!(session.mode(), ExecutionMode::Embedded);
    }

    #[test]
    fn embedded_registered_table_is_queryable() {
        let session = embedded_session();
        session
            .register_record_batches("nums", vec![make_batch(&[10, 20, 30])])
            .unwrap();
        let vals = collect_i64_from_session(&session, "SELECT SUM(v) AS s FROM nums");
        assert_eq!(vals[0], 60);
    }

    #[test]
    fn embedded_filter_and_project() {
        let session = embedded_session();
        session
            .register_record_batches("vals", vec![make_batch(&[1, 2, 3, 4, 5])])
            .unwrap();
        let mut vals =
            collect_i64_from_session(&session, "SELECT v * 2 AS v2 FROM vals WHERE v > 2");
        vals.sort_unstable();
        assert_eq!(vals, vec![6, 8, 10]);
    }

    #[test]
    fn embedded_aggregate_sum_matches_manual() {
        let session = embedded_session();
        session
            .register_record_batches("data", vec![make_batch(&[100, 200, 300])])
            .unwrap();
        let df = session
            .sql("SELECT COUNT(*) AS cnt, SUM(v) AS s, MIN(v) AS lo, MAX(v) AS hi FROM data")
            .unwrap();
        let result = df.collect().unwrap();
        let batches = result.batches();
        let b = &batches[0];
        let cnt = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let s = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let lo = b
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let hi = b
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt, 3);
        assert_eq!(s, 600);
        assert_eq!(lo, 100);
        assert_eq!(hi, 300);
    }

    #[test]
    fn embedded_join_produces_cartesian_product_without_condition() {
        let session = embedded_session();
        session
            .register_record_batches("a", vec![make_batch(&[1, 2])])
            .unwrap();
        session
            .register_record_batches("b", vec![make_batch(&[10, 20])])
            .unwrap();
        let df = session
            .sql("SELECT a.v AS av, b.v AS bv FROM a CROSS JOIN b ORDER BY av, bv")
            .unwrap();
        let result = df.collect().unwrap();
        let batches = result.batches();
        let b = &batches[0];
        let avs: Vec<i64> = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        let bvs: Vec<i64> = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(avs, vec![1, 1, 2, 2]);
        assert_eq!(bvs, vec![10, 20, 10, 20]);
    }

    // ── Single-node mode conformance (requires live daemon; ignored by default) ──

    #[test]
    #[ignore = "requires local krishiv coordinator on :9090"]
    fn single_node_literal_query_returns_correct_value() {
        let session = Session::builder()
            .with_execution_mode(ExecutionMode::SingleNode)
            .with_coordinator_grpc("http://127.0.0.1:9090")
            .build()
            .expect("single-node session");
        let vals = collect_i64_from_session(&session, "SELECT 2 + 2 AS v");
        assert_eq!(vals[0], 4);
    }

    // ── Determinism: same query, same data → same result ───────────────────

    #[test]
    fn repeated_queries_produce_identical_results() {
        let session = embedded_session();
        session
            .register_record_batches("rep", vec![make_batch(&[5, 3, 1, 4, 2])])
            .unwrap();
        let sql = "SELECT v FROM rep ORDER BY v";
        let v1 = collect_i64_from_session(&session, sql);
        let v2 = collect_i64_from_session(&session, sql);
        assert_eq!(v1, v2);
        assert_eq!(v1, vec![1, 2, 3, 4, 5]);
    }
}

/// Live cross-mode conformance: embedded versus a real Flight SQL server.
///
/// The gate the file above was missing. Each corpus query runs through an
/// embedded session and through a remote session pointed at an in-process
/// server, and the rendered row sets must match exactly. A divergence names the
/// query and prints both results.
#[cfg(test)]
mod live_conformance {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::session::Session;
    use crate::types::ExecutionMode;

    /// Queries that must agree across modes.
    ///
    /// Chosen for the shapes where a placement difference would actually show:
    /// aggregation (partial/final split), ordering (a cross-task sort),
    /// grouping, joins, DISTINCT, and NULL handling. Every one reads the same
    /// fixture table so both modes resolve the same data.
    const CORPUS: &[(&str, &str)] = &[
        ("literal", "SELECT 2 + 2 AS v"),
        ("projection", "SELECT id, name FROM t ORDER BY id"),
        ("filter", "SELECT id FROM t WHERE amount > 15 ORDER BY id"),
        ("aggregate", "SELECT COUNT(*) AS c, SUM(amount) AS s FROM t"),
        (
            "group_by",
            "SELECT name, SUM(amount) AS s FROM t GROUP BY name ORDER BY name",
        ),
        (
            "order_limit",
            "SELECT id FROM t ORDER BY amount DESC LIMIT 2",
        ),
        ("distinct", "SELECT DISTINCT name FROM t ORDER BY name"),
        (
            "case_null",
            "SELECT id, CASE WHEN score IS NULL THEN -1.0 ELSE score END AS s FROM t ORDER BY id",
        ),
        (
            "self_join",
            "SELECT a.id, b.id AS b_id FROM t a JOIN t b ON a.name = b.name ORDER BY a.id, b_id",
        ),
        (
            "having",
            "SELECT name, COUNT(*) AS c FROM t GROUP BY name HAVING COUNT(*) > 1 ORDER BY name",
        ),
    ];

    fn fixture() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("score", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "a", "c", "b"])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 5, 25])),
                // A NULL in the middle so null-handling differences surface.
                Arc::new(Float64Array::from(vec![
                    Some(1.5),
                    None,
                    Some(2.5),
                    Some(0.0),
                    None,
                ])),
            ],
        )
        .expect("fixture batch")
    }

    /// Write the fixture as parquet so both modes can resolve `t`: the embedded
    /// session reads the file directly, and the remote session ships it inline
    /// as Arrow IPC through `registered_parquet`.
    fn write_fixture(dir: &std::path::Path) -> std::path::PathBuf {
        let path = dir.join("t.parquet");
        let batch = fixture();
        let file = std::fs::File::create(&path).expect("create fixture");
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
        path
    }

    /// Render a result as sorted lines so the comparison is order-independent
    /// where the query does not pin an order, and value-exact everywhere.
    fn render(result: &crate::QueryResult) -> String {
        let batches = result.batches();
        let text = arrow::util::pretty::pretty_format_batches(batches)
            .expect("format")
            .to_string();
        let mut lines: Vec<&str> = text.lines().collect();
        lines.sort_unstable();
        lines.join("\n")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_corpus_query_agrees_across_modes() {
        use krishiv_flight_sql::make_flight_sql_server;
        use tonic::transport::Server;

        let dir = tempfile::tempdir().expect("tempdir");
        let fixture_path = write_fixture(dir.path());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(make_flight_sql_server().expect("flight sql server"))
                .serve_with_incoming(incoming)
                .await
                .expect("serve");
        });

        let embedded = Session::builder()
            .with_execution_mode(ExecutionMode::Embedded)
            .build()
            .expect("embedded session");
        let remote = Session::builder()
            .with_coordinator(format!("http://{addr}"))
            .with_remote_execution(true)
            .build()
            .expect("remote session");
        assert!(
            remote.execution_runtime().uses_remote_execution(),
            "the remote session must actually route to the server, or this \
             harness silently compares embedded against embedded — the exact \
             failure it was written to end"
        );

        for session in [&embedded, &remote] {
            session
                .register_parquet("t", &fixture_path)
                .expect("register fixture");
        }

        let mut divergences = Vec::new();
        for (label, query) in CORPUS {
            let want = embedded
                .sql_async(query)
                .await
                .expect("embedded plan")
                .collect_async()
                .await
                .expect("embedded collect");
            let got = match remote.sql_async(query).await {
                Ok(df) => match df.collect_async().await {
                    Ok(result) => result,
                    Err(error) => {
                        divergences.push(format!("[{label}] remote collect failed: {error}"));
                        continue;
                    }
                },
                Err(error) => {
                    divergences.push(format!("[{label}] remote plan failed: {error}"));
                    continue;
                }
            };
            let (want, got) = (render(&want), render(&got));
            if want != got {
                divergences.push(format!(
                    "[{label}] {query}\n  embedded:\n{want}\n  remote:\n{got}"
                ));
            }
        }

        server.abort();
        assert!(
            divergences.is_empty(),
            "modes must return identical rows; {} of {} corpus queries diverged:\n\n{}",
            divergences.len(),
            CORPUS.len(),
            divergences.join("\n\n")
        );
    }

    /// A transformed DataFrame must agree across modes too — this is the shape
    /// that was embedded-only until the plan was rendered back to SQL.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dataframe_transforms_agree_across_modes() {
        use krishiv_flight_sql::make_flight_sql_server;
        use tonic::transport::Server;

        let dir = tempfile::tempdir().expect("tempdir");
        let fixture_path = write_fixture(dir.path());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(make_flight_sql_server().expect("flight sql server"))
                .serve_with_incoming(incoming)
                .await
                .expect("serve");
        });

        let embedded = Session::builder()
            .with_execution_mode(ExecutionMode::Embedded)
            .build()
            .expect("embedded session");
        let remote = Session::builder()
            .with_coordinator(format!("http://{addr}"))
            .with_remote_execution(true)
            .build()
            .expect("remote session");
        for session in [&embedded, &remote] {
            session
                .register_parquet("t", &fixture_path)
                .expect("register fixture");
        }

        let build = |session: &Session| {
            let df = session.sql("SELECT id, amount FROM t").expect("sql");
            df.filter("amount > 10").expect("filter")
        };

        let want = build(&embedded)
            .collect_async()
            .await
            .expect("embedded collect");
        let got = build(&remote).collect_async().await;
        server.abort();

        let got = got.expect(
            "a transformed DataFrame must execute remotely; before the unparser \
             pass this failed with \"remote execution requires a SQL query\"",
        );
        assert_eq!(
            render(&want),
            render(&got),
            "a filtered DataFrame must return the same rows in both modes"
        );
    }
}
