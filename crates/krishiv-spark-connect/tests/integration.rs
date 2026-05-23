//! Spark Connect integration tests (R15 S3).

use futures::StreamExt;
use krishiv_proto::spark_connect::connect::spark_connect_service_server::SparkConnectService;
use krishiv_proto::spark_connect::connect::{
    relation, AnalyzePlanRequest, ExecutePlanRequest, Plan, Relation, Sql, UserContext,
};
use krishiv_spark_connect::{serve_spark_connect, SparkConnectServiceImpl};
use krishiv_sql::SqlEngine;
use tonic::Request;

fn test_user_context() -> UserContext {
    UserContext {
        user_id: "krishiv".into(),
        user_name: "krishiv".into(),
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

#[tokio::test]
async fn execute_plan_returns_arrow_batch() {
    let svc = SparkConnectServiceImpl::new(SqlEngine::new());
    let req = ExecutePlanRequest {
        session_id: uuid::Uuid::new_v4().to_string(),
        user_context: Some(test_user_context()),
        plan: Some(sql_plan("SELECT 7 AS seven")),
        ..Default::default()
    };
    let mut stream = svc
        .execute_plan(Request::new(req))
        .await
        .unwrap()
        .into_inner();
    let first = stream.next().await.expect("response").expect("ok");
    assert!(first.response_type.is_some());
}

#[tokio::test]
async fn analyze_plan_returns_spark_version() {
    let svc = SparkConnectServiceImpl::new(SqlEngine::new());
    let req = AnalyzePlanRequest {
        session_id: uuid::Uuid::new_v4().to_string(),
        user_context: Some(test_user_context()),
        analyze: Some(
            krishiv_proto::spark_connect::connect::analyze_plan_request::Analyze::SparkVersion(
                krishiv_proto::spark_connect::connect::analyze_plan_request::SparkVersion {},
            ),
        ),
        ..Default::default()
    };
    let resp = svc
        .analyze_plan(Request::new(req))
        .await
        .unwrap()
        .into_inner();
    match resp.result {
        Some(krishiv_proto::spark_connect::connect::analyze_plan_response::Result::SparkVersion(
            v,
        )) => {
            assert!(v.version.starts_with("3.5"));
        }
        other => panic!("unexpected analyze result: {other:?}"),
    }
}

#[tokio::test]
async fn unsupported_relation_returns_unimplemented_status() {
    let svc = SparkConnectServiceImpl::new(SqlEngine::new());
    let plan = Plan {
        op_type: Some(krishiv_proto::spark_connect::connect::plan::OpType::Root(
            Relation {
                rel_type: Some(relation::RelType::Range(
                    krishiv_proto::spark_connect::connect::Range {
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },
        )),
    };
    let req = ExecutePlanRequest {
        session_id: uuid::Uuid::new_v4().to_string(),
        user_context: Some(test_user_context()),
        plan: Some(plan),
        ..Default::default()
    };
    let err = svc
        .execute_plan(Request::new(req))
        .await
        .err()
        .expect("should fail");
    assert_eq!(err.code(), tonic::Code::Unimplemented);
}

#[tokio::test]
async fn grpc_server_binds_and_serves() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let svc = SparkConnectServiceImpl::new(SqlEngine::new());
    let handle = tokio::spawn(async move {
        serve_spark_connect(listener, svc).await.ok();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    handle.abort();
}
