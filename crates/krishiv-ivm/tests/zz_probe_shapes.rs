use ahash::AHashMap;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use krishiv_ivm::plan::{ViewPlan, build_view_plan};
use std::sync::Arc;

fn schemas() -> AHashMap<String, SchemaRef> {
    let sales: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("ts", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
        Field::new("day", DataType::Utf8, false),
    ]));
    // an "upstream view": a 2-column relation
    let v_map: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("dbl", DataType::Int64, false),
    ]));
    let dim: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("mgr", DataType::Utf8, false),
    ]));
    let mut m = AHashMap::new();
    m.insert("sales".to_string(), sales);
    m.insert("v_map".to_string(), v_map);
    m.insert("dim".to_string(), dim);
    m
}

async fn dump(sql: &str) {
    let ctx = SessionContext::new();
    for (n, s) in schemas() {
        let empty = arrow::record_batch::RecordBatch::new_empty(s.clone());
        ctx.register_table(n.as_str(), Arc::new(MemTable::try_new(s, vec![vec![empty]]).unwrap()))
            .unwrap();
    }
    match ctx.sql(sql).await {
        Ok(df) => println!("SQL: {sql}\nPLAN:\n{}", df.logical_plan().display_indent()),
        Err(e) => println!("SQL: {sql}\nPLAN ERR: {e}"),
    }
}

async fn k(sql: &str, out: SchemaRef) -> ViewPlan {
    build_view_plan(sql, &out, &schemas(), &[]).await
}

fn sch(fields: Vec<(&str, DataType)>) -> SchemaRef {
    Arc::new(Schema::new(
        fields.into_iter().map(|(n, t)| Field::new(n, t, true)).collect::<Vec<_>>(),
    ))
}

fn label(p: &ViewPlan) -> String {
    match p {
        ViewPlan::Aggregate { source, op, filter } => format!(
            "Aggregate(source={source}, filter={}, emits={:?})",
            filter.is_some(),
            op.output_schema().fields().iter().map(|f| format!("{}:{:?}", f.name(), f.data_type())).collect::<Vec<_>>()
        ),
        ViewPlan::Join { left_source, right_source, op, left_filter, right_filter } => format!(
            "Join(l={left_source}, r={right_source}, lf={}, rf={}, emits={:?})",
            left_filter.is_some(), right_filter.is_some(),
            op.output_schema().fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>()
        ),
        ViewPlan::Distinct { source, .. } => format!("Distinct(source={source})"),
        ViewPlan::Map { source, op } => format!(
            "Map(source={source}, emits={:?})",
            op.output_schema().fields().iter().map(|f| format!("{}:{:?}", f.name(), f.data_type())).collect::<Vec<_>>()
        ),
        ViewPlan::TopN { source, .. } => format!("TopN(source={source})"),
        ViewPlan::DiffBased => "DiffBased".to_string(),
    }
}

#[tokio::test]
async fn probe() {
    for sql in [
        "SELECT region, SUM(amount) AS total FROM sales GROUP BY region, date_trunc('day', ts)",
        "SELECT DISTINCT region FROM sales",
        "SELECT region, amount, ts, day FROM sales ORDER BY region LIMIT 5",
        "SELECT region, dbl FROM v_map",
        "SELECT s.region, s.amount, s.ts, s.day, d.mgr FROM sales s JOIN dim d ON s.region = d.region",
        "SELECT region, COUNT(*) AS cnt FROM sales GROUP BY region",
    ] { dump(sql).await; }

    let out_rt = sch(vec![("region", DataType::Utf8), ("total", DataType::Int64)]);

    println!("\n--- results ---");
    // extra group key NOT in declared schema
    println!("EXTRA-GROUP-KEY(day):        {}", label(&k("SELECT region, SUM(amount) AS total FROM sales GROUP BY region, day", out_rt.clone()).await));
    // non-column group expr
    println!("NON-COL-GROUP(date_trunc):   {}", label(&k("SELECT region, SUM(amount) AS total FROM sales GROUP BY region, date_trunc('day', ts)", out_rt.clone()).await));
    // aggregate over an upstream view
    println!("AGG-OVER-VIEW:               {}", label(&k("SELECT region, SUM(dbl) AS total FROM v_map GROUP BY region", out_rt.clone()).await));
    // map over an upstream view
    println!("MAP-OVER-VIEW:               {}", label(&k("SELECT region, dbl*2 AS total FROM v_map", out_rt.clone()).await));
    // distinct over an upstream view (full relation)
    let out_vm = sch(vec![("region", DataType::Utf8), ("dbl", DataType::Int64)]);
    println!("DISTINCT-OVER-VIEW:          {}", label(&k("SELECT DISTINCT region, dbl FROM v_map", out_vm.clone()).await));
    println!("DISTINCT-PROJECTED:          {}", label(&k("SELECT DISTINCT region FROM sales", sch(vec![("region", DataType::Utf8)])).await));
    // topn over an upstream view
    println!("TOPN-OVER-VIEW:              {}", label(&k("SELECT region, dbl FROM v_map ORDER BY dbl DESC LIMIT 3", out_vm.clone()).await));
    // topn narrowing projection
    println!("TOPN-NARROWING:              {}", label(&k("SELECT region, amount FROM sales ORDER BY amount DESC LIMIT 3", sch(vec![("region", DataType::Utf8), ("amount", DataType::Int64)])).await));
    // join with a view on one side
    let out_j = sch(vec![("region", DataType::Utf8), ("dbl", DataType::Int64), ("mgr", DataType::Utf8)]);
    println!("JOIN-VIEW-X-SOURCE:          {}", label(&k("SELECT v.region, v.dbl, d.mgr FROM v_map v JOIN dim d ON v.region = d.region", out_j.clone()).await));
    // join then aggregate (fused) -> ?
    println!("AGG-OVER-JOIN:               {}", label(&k("SELECT s.region, SUM(s.amount) AS total FROM sales s JOIN dim d ON s.region = d.region GROUP BY s.region", out_rt.clone()).await));
    // join with a projection above it that narrows
    println!("JOIN-PROJECTED:              {}", label(&k("SELECT s.region, d.mgr FROM sales s JOIN dim d ON s.region = d.region", sch(vec![("region", DataType::Utf8), ("mgr", DataType::Utf8)])).await));
    // count(*)
    println!("COUNT-STAR:                  {}", label(&k("SELECT region, COUNT(*) AS cnt FROM sales GROUP BY region", sch(vec![("region", DataType::Utf8), ("cnt", DataType::Int64)])).await));
    // filter above aggregate == HAVING
    println!("MAP-OVER-AGG-VIEW-DECLARED:  {}", label(&k("SELECT region, total*1 AS total FROM v_map", out_rt.clone()).await));
    // 3-way join
    println!("THREE-WAY-JOIN:              {}", label(&k("SELECT s.region, s.amount, d.mgr FROM sales s JOIN dim d ON s.region=d.region JOIN v_map v ON v.region=s.region", sch(vec![("region", DataType::Utf8), ("amount", DataType::Int64), ("mgr", DataType::Utf8)])).await));
    // map with WHERE on a view
    println!("MAP-VIEW-WHERE:              {}", label(&k("SELECT region, dbl FROM v_map WHERE dbl > 2", out_vm.clone()).await));
    // aggregate with WHERE
    println!("AGG-WHERE:                   {}", label(&k("SELECT region, SUM(amount) AS total FROM sales WHERE amount > 3 GROUP BY region", out_rt.clone()).await));
    // aggregate declared type mismatch
    println!("AGG-DECLARED-UTF8:           {}", label(&k("SELECT region, SUM(amount) AS total FROM sales GROUP BY region", sch(vec![("region", DataType::Utf8), ("total", DataType::Utf8)])).await));
    // ORDER BY expression at root
    println!("ORDERBY-EXPR:                {}", label(&k("SELECT region, dbl FROM v_map ORDER BY dbl+1", out_vm.clone()).await));
    // OFFSET
    println!("TOPN-OFFSET:                 {}", label(&k("SELECT region, dbl FROM v_map ORDER BY dbl DESC LIMIT 3 OFFSET 2", out_vm.clone()).await));
    // subquery alias
    println!("SUBQ-ALIAS-MAP:              {}", label(&k("SELECT region, dbl FROM (SELECT * FROM v_map) t", out_vm.clone()).await));
    // union
    println!("UNION:                       {}", label(&k("SELECT region, dbl FROM v_map UNION ALL SELECT region, dbl FROM v_map", out_vm.clone()).await));
    // cross join
    println!("CROSS-JOIN:                  {}", label(&k("SELECT v.region, v.dbl, d.mgr FROM v_map v, dim d", out_j.clone()).await));
    // window fn
    println!("WINDOW:                      {}", label(&k("SELECT region, ROW_NUMBER() OVER (PARTITION BY region ORDER BY dbl) AS rn FROM v_map", sch(vec![("region", DataType::Utf8), ("rn", DataType::UInt64)])).await));
}
