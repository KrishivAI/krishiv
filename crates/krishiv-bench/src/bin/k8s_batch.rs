use krishiv_api::SessionBuilder;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Running Distributed Batch TPC-H Q1 (Rust) ---");
    let session = SessionBuilder::new()
        .with_coordinator("http://127.0.0.1:30051") // Use Flight server address
        .with_remote_execution(true)
        .build()?;

    // Register table via remote SQL
    session.execute_remote_async("CREATE EXTERNAL TABLE lineitem STORED AS PARQUET LOCATION '/home/code/krishiv/tpch_sf10/lineitem.parquet'").await?;

    let q1 = "
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date '1998-12-01' - interval '90' day
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus
    ";

    let start = Instant::now();
    let result = session.execute_remote_async(q1).await?.collect()?;
    let duration = start.elapsed();

    println!("{}", result.pretty()?);
    println!(
        "Distributed Batch Execution Time: {:.4} seconds",
        duration.as_secs_f64()
    );

    Ok(())
}
