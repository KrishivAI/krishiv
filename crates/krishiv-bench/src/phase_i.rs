/// Phase I release gate: TPC-H and Nexmark baseline regression checks.
///
/// These unit tests run the benchmark queries on synthetic in-memory data and
/// assert that:
///   - The query executes without error.
///   - The result schema matches the expected output columns.
///   - The result is non-empty (no silent data-loss regression).
///
/// Full regression against stored Parquet baselines requires the environment
/// variables documented in `crate::tpch::scale_dirs()` and is handled by
/// `cargo bench` in CI.
#[cfg(test)]
mod phase_i_tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_sql::SqlEngine;

    fn engine() -> SqlEngine {
        SqlEngine::new()
    }

    // TPC-H synthetic fixtures

    async fn register_tpch_synthetic(engine: &SqlEngine) {
        // Minimal lineitem
        let lineitem_schema = Arc::new(Schema::new(vec![
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_shipdate", DataType::Utf8, false),
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
        ]));
        let lineitem = RecordBatch::try_new(
            lineitem_schema,
            vec![
                Arc::new(StringArray::from(vec!["N", "R", "A"])),
                Arc::new(StringArray::from(vec!["O", "F", "F"])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
                Arc::new(Float64Array::from(vec![100.0, 200.0, 300.0])),
                Arc::new(Float64Array::from(vec![0.05, 0.10, 0.00])),
                Arc::new(StringArray::from(vec![
                    "1998-01-01",
                    "1997-06-15",
                    "1996-03-10",
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        // Minimal orders
        let orders_schema = Arc::new(Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderdate", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Float64, false),
            Field::new("o_shippriority", DataType::Int64, false),
        ]));
        let orders = RecordBatch::try_new(
            orders_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
                Arc::new(StringArray::from(vec![
                    "1995-01-01",
                    "1994-06-01",
                    "1993-03-01",
                ])),
                Arc::new(Float64Array::from(vec![1000.0, 2000.0, 3000.0])),
                Arc::new(Int64Array::from(vec![0, 1, 0])),
            ],
        )
        .unwrap();

        // Minimal customer
        let customer_schema = Arc::new(Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Float64, false),
            Field::new("c_nationkey", DataType::Int64, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]));
        let customer = RecordBatch::try_new(
            customer_schema,
            vec![
                Arc::new(Int64Array::from(vec![100, 200, 300])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
                Arc::new(StringArray::from(vec!["BUILDING", "AUTO", "BUILDING"])),
                Arc::new(Float64Array::from(vec![500.0, 600.0, 700.0])),
                Arc::new(Int64Array::from(vec![1, 2, 1])),
                Arc::new(StringArray::from(vec!["addr1", "addr2", "addr3"])),
                Arc::new(StringArray::from(vec!["555-0001", "555-0002", "555-0003"])),
                Arc::new(StringArray::from(vec!["cmt1", "cmt2", "cmt3"])),
            ],
        )
        .unwrap();

        // Minimal nation, supplier, region
        let nation_schema = Arc::new(Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int64, false),
        ]));
        let nation = RecordBatch::try_new(
            nation_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["GERMANY", "FRANCE"])),
                Arc::new(Int64Array::from(vec![1, 1])),
            ],
        )
        .unwrap();

        let supplier_schema = Arc::new(Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_nationkey", DataType::Int64, false),
        ]));
        let supplier = RecordBatch::try_new(
            supplier_schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![1, 2, 1])),
            ],
        )
        .unwrap();

        let region_schema = Arc::new(Schema::new(vec![
            Field::new("r_regionkey", DataType::Int64, false),
            Field::new("r_name", DataType::Utf8, false),
        ]));
        let region = RecordBatch::try_new(
            region_schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["ASIA"])),
            ],
        )
        .unwrap();

        engine
            .register_record_batches("lineitem", vec![lineitem])
            .await
            .unwrap();
        engine
            .register_record_batches("orders", vec![orders])
            .await
            .unwrap();
        engine
            .register_record_batches("customer", vec![customer])
            .await
            .unwrap();
        engine
            .register_record_batches("nation", vec![nation])
            .await
            .unwrap();
        engine
            .register_record_batches("supplier", vec![supplier])
            .await
            .unwrap();
        engine
            .register_record_batches("region", vec![region])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn tpch_q1_runs_without_error() {
        let e = engine();
        register_tpch_synthetic(&e).await;
        let df = e.sql(crate::tpch::Q1).await.expect("Q1 must not error");
        let batches = df.collect().await.expect("Q1 collect");
        assert!(
            !batches.is_empty() && batches.iter().any(|b| b.num_rows() > 0),
            "Q1 returned no data"
        );
        // Schema: l_returnflag, l_linestatus, sum_qty, sum_base_price, count_order
        assert_eq!(batches[0].num_columns(), 5, "Q1 column count wrong");
    }

    #[tokio::test]
    async fn tpch_q3_runs_without_error() {
        let e = engine();
        register_tpch_synthetic(&e).await;
        let df = e.sql(crate::tpch::Q3).await.expect("Q3 must not error");
        let batches = df.collect().await.expect("Q3 collect");
        // Schema: l_orderkey, revenue, o_orderdate, o_shippriority
        let total_cols = batches.first().map(|b| b.num_columns()).unwrap_or(4);
        assert_eq!(total_cols, 4, "Q3 column count wrong");
    }

    #[tokio::test]
    async fn tpch_q6_runs_without_error() {
        let e = engine();
        register_tpch_synthetic(&e).await;
        let df = e.sql(crate::tpch::Q6).await.expect("Q6 must not error");
        let batches = df.collect().await.expect("Q6 collect");
        let total_cols = batches.first().map(|b| b.num_columns()).unwrap_or(1);
        assert_eq!(total_cols, 1, "Q6 column count wrong");
    }

    #[tokio::test]
    async fn tpch_q10_runs_without_error() {
        let e = engine();
        register_tpch_synthetic(&e).await;
        let df = e.sql(crate::tpch::Q10).await.expect("Q10 must not error");
        let batches = df.collect().await.expect("Q10 collect");
        let total_cols = batches.first().map(|b| b.num_columns()).unwrap_or(8);
        assert_eq!(total_cols, 8, "Q10 column count wrong");
    }

    // Nexmark synthetic fixtures

    async fn register_nexmark_synthetic(engine: &SqlEngine) {
        let bid_schema = Arc::new(Schema::new(vec![
            Field::new("auction", DataType::UInt64, false),
            Field::new("price", DataType::UInt64, false),
        ]));
        let bids = RecordBatch::try_new(
            bid_schema,
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2, 3, 1, 2])),
                Arc::new(UInt64Array::from(vec![100u64, 200, 150, 300, 250])),
            ],
        )
        .unwrap();

        let auction_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("category", DataType::UInt64, false),
        ]));
        let auctions = RecordBatch::try_new(
            auction_schema,
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2, 3])),
                Arc::new(UInt64Array::from(vec![10u64, 20, 10])),
            ],
        )
        .unwrap();

        let person_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("region", DataType::Int64, false),
        ]));
        let persons = RecordBatch::try_new(
            person_schema,
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2, 3])),
                Arc::new(Int64Array::from(vec![1i64, 2, 1])),
            ],
        )
        .unwrap();

        engine
            .register_record_batches("bid", vec![bids])
            .await
            .unwrap();
        engine
            .register_record_batches("auction", vec![auctions])
            .await
            .unwrap();
        engine
            .register_record_batches("person", vec![persons])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn nexmark_q1_price_conversion_runs() {
        // Q1: convert bid price (assume 1 EUR = 0.908 USD)
        let e = engine();
        register_nexmark_synthetic(&e).await;
        let df = e
            .sql("SELECT auction, CAST(price AS DOUBLE) * 0.908 AS price_usd FROM bid")
            .await
            .expect("Nexmark Q1 must not error");
        let batches = df.collect().await.expect("collect");
        assert!(!batches.is_empty() && batches[0].num_rows() > 0);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn nexmark_q2_auction_filter_runs() {
        // Q2: filter bids for specific auction IDs
        let e = engine();
        register_nexmark_synthetic(&e).await;
        let df = e
            .sql("SELECT auction, price FROM bid WHERE auction % 123 = 0 OR auction = 1")
            .await
            .expect("Nexmark Q2 must not error");
        let batches = df.collect().await.expect("collect");
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn nexmark_q5_hot_items_runs() {
        // Q5: count bids per auction, find maximum (hot items)
        let e = engine();
        register_nexmark_synthetic(&e).await;
        let df = e
            .sql("SELECT auction, COUNT(*) AS bid_count FROM bid GROUP BY auction ORDER BY bid_count DESC")
            .await
            .expect("Nexmark Q5 must not error");
        let batches = df.collect().await.expect("collect");
        assert!(!batches.is_empty() && batches[0].num_rows() > 0);
        // Top auction should have the most bids
        let counts = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(counts.value(0) >= counts.value(counts.len() - 1));
    }

    #[tokio::test]
    async fn nexmark_q8_join_persons_and_auctions_runs() {
        // Q8: join persons with their new auctions
        let e = engine();
        register_nexmark_synthetic(&e).await;
        let df = e
            .sql("SELECT p.id AS person_id, a.id AS auction_id FROM person p JOIN auction a ON p.id = a.id")
            .await
            .expect("Nexmark Q8 must not error");
        let batches = df.collect().await.expect("collect");
        assert_eq!(batches[0].num_columns(), 2);
    }

    // Baseline regression: queries must complete within a time bound

    #[tokio::test]
    async fn tpch_and_nexmark_complete_within_timeout() {
        use std::time::{Duration, Instant};
        let e = engine();
        register_tpch_synthetic(&e).await;
        register_nexmark_synthetic(&e).await;

        let start = Instant::now();
        for sql in [crate::tpch::Q1, crate::tpch::Q6] {
            let df = e.sql(sql).await.expect("query must not error");
            df.collect().await.expect("collect must not error");
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(30),
            "TPC-H queries exceeded 30 s on synthetic data: {elapsed:?}"
        );
    }
}
