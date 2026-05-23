//! TPC-H query execution via Spark Connect (R15 S5.2).

mod tpch_fixtures;

use std::fs;
use std::path::PathBuf;

use futures::StreamExt;
use krishiv_proto::spark_connect::connect::spark_connect_service_server::SparkConnectService;
use krishiv_proto::spark_connect::connect::{relation, ExecutePlanRequest, Plan, Relation, Sql, UserContext};
use krishiv_spark_connect::SparkConnectServiceImpl;
use krishiv_sql::SqlEngine;
use tempfile::TempDir;
use tonic::Request;

fn user() -> UserContext {
    UserContext {
        user_id: "tpch".into(),
        user_name: "tpch".into(),
        ..Default::default()
    }
}

fn sql_plan(query: &str) -> Plan {
    Plan {
        op_type: Some(krishiv_proto::spark_connect::connect::plan::OpType::Root(
            Relation {
                rel_type: Some(relation::RelType::Sql(Sql {
                    query: query.into(),
                    ..Default::default()
                })),
                ..Default::default()
            },
        )),
    }
}

fn load_query(n: usize) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/tpch")
        .join(format!("q{n}.sql"));
    let raw = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    raw.lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n")
}

async fn register_tpch(engine: &SqlEngine, dir: &std::path::Path) {
    for table in [
        "lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region",
    ] {
        let path = dir.join(format!("{table}.parquet"));
        engine
            .register_parquet(table, path.to_str().unwrap())
            .await
            .unwrap_or_else(|e| panic!("register {table}: {e}"));
    }
}

#[tokio::test]
async fn tpch_all_22_queries_execute_via_spark_connect() {
    let tmp = TempDir::new().unwrap();
    if let Ok(data_dir) = std::env::var("KRISHIV_TPCH_DATA_DIR") {
        tpch_fixtures::write_tpch_mini_dataset(tmp.path()).ok();
        let engine = SqlEngine::new();
        register_tpch(&engine, std::path::Path::new(&data_dir)).await;
        run_queries(&engine).await;
        return;
    }
    tpch_fixtures::write_tpch_mini_dataset(tmp.path()).expect("fixtures");
    let engine = SqlEngine::new();
    register_tpch(&engine, tmp.path()).await;
    run_queries(&engine).await;
}

async fn run_queries(engine: &SqlEngine) {
    let svc = SparkConnectServiceImpl::new(engine.clone());
    for n in 1..=22 {
        let query = load_query(n);
        let req = ExecutePlanRequest {
            session_id: format!("tpch-q{n}"),
            user_context: Some(user()),
            plan: Some(sql_plan(&query)),
            ..Default::default()
        };
        let mut stream = svc
            .execute_plan(Request::new(req))
            .await
            .unwrap_or_else(|e| panic!("Q{n} execute_plan: {e}"))
            .into_inner();
        assert!(
            stream.next().await.is_some(),
            "Q{n} produced no response batches"
        );
    }
}
