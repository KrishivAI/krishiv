#![forbid(unsafe_code)]
//! Shared support code for the Krishiv benchmark suite.
//!
//! The [`tpch`] module holds the TPC-H query texts, the Parquet table set each
//! query reads, and the scale-factor ladder resolved from environment
//! variables. Both the embedded (`tpch_sf10`) and distributed
//! (`tpch_distributed`) bench targets consume these definitions so the two
//! harnesses always measure identical workloads.

/// Structured benchmark results and comparison framework.
pub mod comparison;
pub mod phase_i;

pub mod tpcds;

pub mod tpch {
    //! TPC-H query texts, per-query table sets, and the scale-factor ladder.

    /// TPC-H Q1: pricing summary report.
    pub const Q1: &str = "SELECT \
        l_returnflag, l_linestatus, \
        SUM(l_quantity) AS sum_qty, \
        SUM(l_extendedprice) AS sum_base_price, \
        COUNT(*) AS count_order \
        FROM lineitem \
        WHERE l_shipdate <= '1998-09-02' \
        GROUP BY l_returnflag, l_linestatus \
        ORDER BY l_returnflag, l_linestatus";

    /// Tables read by [`Q1`].
    pub const Q1_TABLES: &[&str] = &["lineitem"];

    /// TPC-H Q3: shipping priority — join orders, lineitem, customer; filter
    /// by date and segment.
    pub const Q3: &str = "SELECT \
        l_orderkey, \
        SUM(l_extendedprice * (1 - l_discount)) AS revenue, \
        o_orderdate, \
        o_shippriority \
        FROM customer, orders, lineitem \
        WHERE c_mktsegment = 'BUILDING' \
          AND c_custkey = o_custkey \
          AND l_orderkey = o_orderkey \
          AND o_orderdate < '1995-03-15' \
          AND l_shipdate > '1995-03-15' \
        GROUP BY l_orderkey, o_orderdate, o_shippriority \
        ORDER BY revenue DESC, o_orderdate \
        LIMIT 10";

    /// Tables read by [`Q3`].
    pub const Q3_TABLES: &[&str] = &["lineitem", "orders", "customer"];

    /// TPC-H Q5: local supplier volume — multi-table join with region filter.
    pub const Q5: &str = "SELECT \
        n_name, \
        SUM(l_extendedprice * (1 - l_discount)) AS revenue \
        FROM customer, orders, lineitem, supplier, nation, region \
        WHERE c_custkey = o_custkey \
          AND l_orderkey = o_orderkey \
          AND l_suppkey = s_suppkey \
          AND c_nationkey = s_nationkey \
          AND s_nationkey = n_nationkey \
          AND n_regionkey = r_regionkey \
          AND r_name = 'ASIA' \
          AND o_orderdate >= '1994-01-01' \
          AND o_orderdate < '1995-01-01' \
        GROUP BY n_name \
        ORDER BY revenue DESC";

    /// Tables read by [`Q5`].
    pub const Q5_TABLES: &[&str] = &[
        "lineitem", "orders", "customer", "supplier", "nation", "region",
    ];

    /// TPC-H Q6: forecasting revenue change.
    pub const Q6: &str = "SELECT SUM(l_extendedprice * l_discount) AS revenue \
        FROM lineitem \
        WHERE l_shipdate >= '1994-01-01' \
          AND l_shipdate < '1995-01-01' \
          AND l_discount BETWEEN 0.05 AND 0.07 \
          AND l_quantity < 24";

    /// Tables read by [`Q6`].
    pub const Q6_TABLES: &[&str] = &["lineitem"];

    /// TPC-H Q10: returned-item reporting — join customer, orders, lineitem,
    /// nation; group by customer and report the top 20 by lost revenue.
    pub const Q10: &str = "SELECT \
        c_custkey, \
        c_name, \
        SUM(l_extendedprice * (1 - l_discount)) AS revenue, \
        c_acctbal, \
        n_name, \
        c_address, \
        c_phone, \
        c_comment \
        FROM customer, orders, lineitem, nation \
        WHERE c_custkey = o_custkey \
          AND l_orderkey = o_orderkey \
          AND o_orderdate >= '1993-10-01' \
          AND o_orderdate < '1994-01-01' \
          AND l_returnflag = 'R' \
          AND c_nationkey = n_nationkey \
        GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment \
        ORDER BY revenue DESC \
        LIMIT 20";

    /// Tables read by [`Q10`].
    pub const Q10_TABLES: &[&str] = &["lineitem", "orders", "customer", "nation"];

    /// TPC-H Q18: large-volume customer — customers whose orders total more
    /// than 300 units (HAVING SUM(l_quantity) > 300).
    pub const Q18: &str = "SELECT \
        c_name, \
        c_custkey, \
        o_orderkey, \
        o_orderdate, \
        o_totalprice, \
        SUM(l_quantity) AS total_quantity \
        FROM customer, orders, lineitem \
        WHERE o_orderkey IN ( \
            SELECT l_orderkey \
            FROM lineitem \
            GROUP BY l_orderkey \
            HAVING SUM(l_quantity) > 300) \
          AND c_custkey = o_custkey \
          AND o_orderkey = l_orderkey \
        GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice \
        ORDER BY o_totalprice DESC, o_orderdate \
        LIMIT 100";

    /// Tables read by [`Q18`].
    pub const Q18_TABLES: &[&str] = &["lineitem", "orders", "customer"];

    /// TPC-H Q9: product type profit measure — complex 6-table join with
    /// group-by on nation and year.
    pub const Q9: &str = "SELECT \
        n_name, \
        EXTRACT(YEAR FROM o_orderdate) AS o_year, \
        SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS sum_profit \
        FROM lineitem \
        JOIN orders ON l_orderkey = o_orderkey \
        JOIN part ON l_partkey = p_partkey \
        JOIN partsupp ON l_partkey = ps_partkey AND l_suppkey = ps_suppkey \
        JOIN supplier ON l_suppkey = s_suppkey \
        JOIN nation ON s_nationkey = n_nationkey \
        WHERE p_name LIKE '%green%' \
        GROUP BY n_name, EXTRACT(YEAR FROM o_orderdate) \
        ORDER BY n_name, o_year DESC";

    /// Tables read by [`Q9`].
    pub const Q9_TABLES: &[&str] = &[
        "lineitem", "orders", "part", "partsupp", "supplier", "nation",
    ];

    /// TPC-H Q12: shipping mode order priority — two-table join with
    /// EXISTS/NOT EXISTS subqueries.
    pub const Q12: &str = "SELECT \
        l_shipmode, \
        SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' \
            THEN 1 ELSE 0 END) AS high_line_count, \
        SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' \
            THEN 1 ELSE 0 END) AS low_line_count \
        FROM orders, lineitem \
        WHERE o_orderkey = l_orderkey \
          AND l_shipmode IN ('MAIL', 'SHIP') \
          AND l_commitdate < l_receiptdate \
          AND l_shipdate < l_commitdate \
          AND l_receiptdate >= '1994-01-01' \
          AND l_receiptdate < '1995-01-01' \
        GROUP BY l_shipmode \
        ORDER BY l_shipmode";

    /// Tables read by [`Q12`].
    pub const Q12_TABLES: &[&str] = &["lineitem", "orders"];

    /// TPC-H Q14: promotion effect — conditional ratio query with LIKE filter.
    pub const Q14: &str = "SELECT \
        100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' \
            THEN l_extendedprice * (1 - l_discount) \
            ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue \
        FROM lineitem, part \
        WHERE l_partkey = p_partkey \
          AND l_shipdate >= '1995-09-01' \
          AND l_shipdate < '1995-10-01'";

    /// Tables read by [`Q14`].
    pub const Q14_TABLES: &[&str] = &["lineitem", "part"];

    /// TPC-H Q19: discounted revenue — complex filter on part attributes and
    /// shipping conditions.
    pub const Q19: &str = "SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue \
        FROM lineitem, part \
        WHERE (p_brand = 'Brand#12' \
            AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') \
            AND l_quantity >= 1 AND l_quantity <= 11 \
            AND p_size BETWEEN 1 AND 5 \
            AND l_shipmode IN ('AIR', 'AIR REG') \
            AND l_shipinstruct = 'DELIVER IN PERSON') \
           OR (p_brand = 'Brand#23' \
            AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') \
            AND l_quantity >= 10 AND l_quantity <= 20 \
            AND p_size BETWEEN 1 AND 10 \
            AND l_shipmode IN ('AIR', 'AIR REG') \
            AND l_shipinstruct = 'DELIVER IN PERSON') \
           OR (p_brand = 'Brand#34' \
            AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') \
            AND l_quantity >= 20 AND l_quantity <= 30 \
            AND p_size BETWEEN 1 AND 15 \
            AND l_shipmode IN ('AIR', 'AIR REG') \
            AND l_shipinstruct = 'DELIVER IN PERSON')";

    /// Tables read by [`Q19`].
    pub const Q19_TABLES: &[&str] = &["lineitem", "part"];

    /// TPC-H Q22: global sales opportunity — subquery with IN and AVG on
    /// customer balance.
    pub const Q22: &str = "SELECT \
        cntrycode, \
        COUNT(*) AS numcust, \
        SUM(c_acctbal) AS totacctbal \
        FROM ( \
            SELECT SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal \
            FROM customer \
            WHERE SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17') \
              AND c_acctbal > ( \
                  SELECT AVG(c_acctbal) \
                  FROM customer \
                  WHERE c_acctbal > 0.00 \
                    AND SUBSTRING(c_phone FROM 1 FOR 2) IN ('13', '31', '23', '29', '30', '18', '17') \
              ) \
              AND NOT EXISTS ( \
                  SELECT * FROM orders WHERE o_custkey = c_custkey \
              ) \
        ) AS custsale \
        GROUP BY cntrycode \
        ORDER BY cntrycode";

    /// Tables read by [`Q22`].
    pub const Q22_TABLES: &[&str] = &["customer", "orders"];

    /// All queries as `(name, sql, tables)` for iteration in benchmarks.
    pub const ALL_QUERIES: &[(&str, &str, &[&str])] = &[
        ("q1", Q1, Q1_TABLES),
        ("q3", Q3, Q3_TABLES),
        ("q5", Q5, Q5_TABLES),
        ("q6", Q6, Q6_TABLES),
        ("q9", Q9, Q9_TABLES),
        ("q10", Q10, Q10_TABLES),
        ("q12", Q12, Q12_TABLES),
        ("q14", Q14, Q14_TABLES),
        ("q18", Q18, Q18_TABLES),
        ("q19", Q19, Q19_TABLES),
        ("q22", Q22, Q22_TABLES),
    ];

    /// Scale factors recognised by the ladder, with the env var naming the
    /// Parquet data directory for each.
    const SCALE_LADDER: &[(&str, &str)] = &[
        ("sf1", "KRISHIV_TPCH_DATA_DIR_SF1"),
        ("sf10", "KRISHIV_TPCH_DATA_DIR_SF10"),
        ("sf100", "KRISHIV_TPCH_DATA_DIR_SF100"),
    ];

    /// Legacy alias for the SF10 data directory.
    const LEGACY_SF10_VAR: &str = "KRISHIV_TPCH_DATA_DIR";

    /// Resolve the scale-factor ladder from the environment.
    ///
    /// Returns one `(scale_factor, data_dir)` pair per configured scale
    /// factor: `KRISHIV_TPCH_DATA_DIR_SF1`, `KRISHIV_TPCH_DATA_DIR_SF10`
    /// (with `KRISHIV_TPCH_DATA_DIR` honoured as a legacy alias for SF10),
    /// and `KRISHIV_TPCH_DATA_DIR_SF100`. Scale factors whose env var is
    /// unset are skipped with an eprintln notice so `cargo bench` still runs
    /// (and records nothing) on machines without the datasets.
    pub fn scale_dirs() -> Vec<(&'static str, String)> {
        let mut dirs = Vec::new();
        for (sf, var) in SCALE_LADDER {
            let mut dir = std::env::var(var).ok();
            if dir.is_none() && *sf == "sf10" {
                dir = std::env::var(LEGACY_SF10_VAR).ok();
            }
            match dir {
                Some(d) => dirs.push((*sf, d)),
                #[allow(clippy::print_stderr)]
                None => eprintln!("skipping TPC-H {sf}: {var} not set"),
            }
        }
        dirs
    }

    /// True when every Parquet file for `tables` exists under `dir`.
    pub fn tables_exist(dir: &str, tables: &[&str]) -> bool {
        tables
            .iter()
            .all(|t| std::path::Path::new(&format!("{dir}/{t}.parquet")).exists())
    }
}
