//! TEMPORARY research probe — delete after use.
#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::expect_used)]

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use datafusion::sql::unparser::plan_to_sql;

async fn ctx() -> SessionContext {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 1])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
            Arc::new(StringArray::from(vec!["a", "b", "a"])),
        ],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("orders", Arc::new(table)).unwrap();
    ctx
}

fn banner(t: &str) {
    println!("\n======== {t} ========");
}

#[tokio::test]
async fn probe() {
    let ctx = ctx().await;

    // ---- 1. SQL aggregate: unparse then re-plan, compare plans ----
    banner("1. SQL GROUP BY: plan -> sql -> plan");
    let df = ctx
        .sql("SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id")
        .await
        .unwrap();
    let p1 = df.logical_plan().clone();
    println!("p1:\n{p1}");
    println!("p1 schema fields: {:?}", p1.schema().field_names());
    let sql = plan_to_sql(&p1).unwrap().to_string();
    println!("unparsed: {sql}");
    let p2 = ctx.state().create_logical_plan(&sql).await.unwrap();
    println!("p2:\n{p2}");
    println!("p2 schema fields: {:?}", p2.schema().field_names());
    println!("PLAN EQ: {}", format!("{p1}") == format!("{p2}"));

    // ---- 2. DataFrame .aggregate() (no SQL text), unparse ----
    banner("2. DataFrame::aggregate -> sql -> plan");
    let df = ctx
        .table("orders")
        .await
        .unwrap()
        .aggregate(
            vec![col("customer_id")],
            vec![datafusion::functions_aggregate::expr_fn::sum(col("amount"))],
        )
        .unwrap();
    let p1 = df.logical_plan().clone();
    println!("p1:\n{p1}");
    println!("p1 schema fields: {:?}", p1.schema().field_names());
    match plan_to_sql(&p1) {
        Ok(s) => {
            let s = s.to_string();
            println!("unparsed: {s}");
            match ctx.state().create_logical_plan(&s).await {
                Ok(p2) => {
                    println!("p2:\n{p2}");
                    println!("p2 schema fields: {:?}", p2.schema().field_names());
                    println!("PLAN EQ: {}", format!("{p1}") == format!("{p2}"));
                }
                Err(e) => println!("REPLAN FAILED: {e}"),
            }
        }
        Err(e) => println!("UNPARSE FAILED: {e}"),
    }

    // ---- 3. sub-tree extraction: take the Aggregate INPUT of a Projection ----
    banner("3. sub-tree: strip the root Projection, unparse the Aggregate");
    let df = ctx
        .sql("SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id")
        .await
        .unwrap();
    let root = df.logical_plan().clone();
    println!("root:\n{root}");
    let sub = root.inputs()[0].clone();
    println!("sub:\n{sub}");
    println!("sub schema fields: {:?}", sub.schema().field_names());
    match plan_to_sql(&sub) {
        Ok(s) => {
            let s = s.to_string();
            println!("sub unparsed: {s}");
            match ctx.state().create_logical_plan(&s).await {
                Ok(p2) => {
                    println!("sub replanned:\n{p2}");
                    println!("sub replan fields: {:?}", p2.schema().field_names());
                    println!("SUB PLAN EQ: {}", format!("{sub}") == format!("{p2}"));
                }
                Err(e) => println!("SUB REPLAN FAILED: {e}"),
            }
        }
        Err(e) => println!("SUB UNPARSE FAILED: {e}"),
    }

    // ---- 4. a filter referencing the mangled aggregate column name ----
    banner("4. DataFrame filter on top of DataFrame::aggregate (mangled col name)");
    let agg = ctx
        .table("orders")
        .await
        .unwrap()
        .aggregate(
            vec![col("region")],
            vec![datafusion::functions_aggregate::expr_fn::sum(col("amount"))],
        )
        .unwrap();
    println!("agg fields: {:?}", agg.schema().field_names());
    let filtered = agg.filter(col(r#"sum(orders.amount)"#).gt(lit(15_i64)));
    match filtered {
        Ok(df) => {
            let p = df.logical_plan().clone();
            println!("p:\n{p}");
            match plan_to_sql(&p) {
                Ok(s) => {
                    let s = s.to_string();
                    println!("unparsed: {s}");
                    match ctx.state().create_logical_plan(&s).await {
                        Ok(p2) => println!("replanned:\n{p2}"),
                        Err(e) => println!("REPLAN FAILED: {e}"),
                    }
                }
                Err(e) => println!("UNPARSE FAILED: {e}"),
            }
        }
        Err(e) => println!("FILTER BUILD FAILED: {e}"),
    }

    // ---- 5. optimized plan unparse ----
    banner("5. OPTIMIZED plan -> sql -> plan");
    let df = ctx
        .sql("SELECT customer_id, SUM(amount) AS total FROM orders WHERE amount > 5 GROUP BY customer_id")
        .await
        .unwrap();
    let opt = df.clone().into_optimized_plan().unwrap();
    println!("optimized:\n{opt}");
    match plan_to_sql(&opt) {
        Ok(s) => {
            let s = s.to_string();
            println!("unparsed(optimized): {s}");
            match ctx.state().create_logical_plan(&s).await {
                Ok(p2) => println!("replanned:\n{p2}"),
                Err(e) => println!("REPLAN FAILED: {e}"),
            }
        }
        Err(e) => println!("UNPARSE FAILED (optimized): {e}"),
    }

    // ---- 6. unnest at root ----
    banner("6. Unnest at root");
    let ctx2 = SessionContext::new();
    let df = ctx2
        .sql("SELECT unnest([1,2,3]) AS v")
        .await
        .unwrap();
    let p = df.logical_plan().clone();
    println!("p:\n{p}");
    match plan_to_sql(&p) {
        Ok(s) => println!("unparsed: {s}"),
        Err(e) => println!("UNPARSE FAILED: {e}"),
    }

    // ---- 7. TableScan with pushed-down filter/projection (post-optimizer shape) ----
    banner("7. window function plan");
    let df = ctx
        .sql("SELECT customer_id, SUM(amount) OVER (PARTITION BY region) AS w FROM orders")
        .await
        .unwrap();
    let p = df.logical_plan().clone();
    println!("p:\n{p}");
    match plan_to_sql(&p) {
        Ok(s) => {
            let s = s.to_string();
            println!("unparsed: {s}");
            match ctx.state().create_logical_plan(&s).await {
                Ok(p2) => println!("replanned:\n{p2}"),
                Err(e) => println!("REPLAN FAILED: {e}"),
            }
        }
        Err(e) => println!("UNPARSE FAILED: {e}"),
    }

    // ---- 8. VALUES / no table (what krishiv's own test uses) ----
    banner("8. distinct + limit + order by chain via DataFrame API");
    let df = ctx
        .table("orders")
        .await
        .unwrap()
        .filter(col("amount").gt(lit(5_i64)))
        .unwrap()
        .select(vec![col("region"), col("amount")])
        .unwrap()
        .distinct()
        .unwrap()
        .sort(vec![col("region").sort(true, false)])
        .unwrap()
        .limit(0, Some(2))
        .unwrap();
    let p = df.logical_plan().clone();
    println!("p:\n{p}");
    match plan_to_sql(&p) {
        Ok(s) => {
            let s = s.to_string();
            println!("unparsed: {s}");
            match ctx.state().create_logical_plan(&s).await {
                Ok(p2) => println!("replanned:\n{p2}"),
                Err(e) => println!("REPLAN FAILED: {e}"),
            }
        }
        Err(e) => println!("UNPARSE FAILED: {e}"),
    }
}
