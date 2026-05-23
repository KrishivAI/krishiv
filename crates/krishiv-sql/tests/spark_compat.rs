//! Spark SQL compatibility integration tests (R15 S1).

use datafusion::prelude::SessionContext;
use krishiv_sql::spark_compat::{self, spark_alias_count, spark_function_test_cases};

#[test]
fn spark_compat_registers_at_least_100_aliases() {
    assert!(
        spark_alias_count() >= 100,
        "expected >= 100 Spark aliases, got {}",
        spark_alias_count()
    );
}

#[tokio::test]
async fn spark_compat_ifnull_alias() {
    let ctx = SessionContext::new();
    spark_compat::register_spark_functions(&ctx).expect("register");
    let df = ctx
        .sql("SELECT ifnull(NULL, 42) AS v")
        .await
        .expect("query");
    let batches = df.collect().await.expect("collect");
    assert_eq!(batches[0].num_rows(), 1);
}

#[test]
fn spark_compat_harness_runs_all_cases() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let ctx = SessionContext::new();
        spark_compat::register_spark_functions(&ctx).expect("register");
        for case in spark_function_test_cases() {
            let df = ctx
                .sql(&case.sql)
                .await
                .unwrap_or_else(|e| panic!("case {} failed: {e}", case.name));
            let _ = df.collect().await.unwrap_or_else(|e| {
                panic!("collect for {} failed: {e}", case.name);
            });
        }
    });
}
