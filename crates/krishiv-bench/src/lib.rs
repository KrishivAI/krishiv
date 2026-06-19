#![forbid(unsafe_code)]
//! Shared support code for the Krishiv benchmark suite.
//!
//! The [`tpch`] module holds the TPC-H query texts, the Parquet table set each
//! query reads, and the scale-factor ladder resolved from environment
//! variables. Both the embedded (`tpch_sf10`) and distributed
//! (`tpch_distributed`) bench targets consume these definitions so the two
//! harnesses always measure identical workloads.

pub mod phase_i;

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
