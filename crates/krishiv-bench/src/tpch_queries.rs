//! The 22 TPC-H queries, as one shared corpus.
//!
//! Previously the benchmark hard-coded six queries (Q1/Q3/Q5/Q6/Q10/Q18) inline
//! in `benches/tpch_sf10.rs`, which meant results labelled "TPC-H" covered
//! 6/22 and the harness gave no way to tell. Worse, any distributed runner
//! would have had to re-type the SQL, so a single-node vs distributed
//! comparison could silently be comparing different queries.
//!
//! Both runners now read this corpus, so "same query, two topologies" is
//! structurally guaranteed rather than maintained by hand.
//!
//! # Dialect choices
//!
//! - Date arithmetic is **pre-computed into literals** rather than written as
//!   `DATE '...' + INTERVAL '3' MONTH`. The interval spelling varies across
//!   engines and a benchmark should measure execution, not parser tolerance;
//!   the substitution values are the TPC-H spec's validation parameters.
//! - `substr(x, start, len)` rather than `SUBSTRING(x FROM a FOR b)`.
//! - Q15's `CREATE VIEW revenue0` is inlined as a CTE — semantically identical
//!   and keeps every entry a single self-contained statement, which the
//!   distributed submit path needs.

/// The scale-factor placeholder in Q11's substitution parameter.
///
/// TPC-H specifies Q11's threshold as `0.0001 / SF`, not `0.0001`. The corpus
/// hard-coded the SF1 value, so at SF100 the threshold was 100x too high and
/// **q11 returned zero rows on every engine**.
///
/// That is worse than a wrong number. A q11 that returns nothing cannot tell a
/// correct engine from one that dropped every row, so the query was scored
/// "ok" while validating nothing — and it stayed that way through a full
/// cross-engine baseline, where both engines agreeing on "0 rows" read as
/// confirmation.
pub const SCALE_FRACTION_PLACEHOLDER: &str = "{{SCALE_FRACTION}}";

/// One query: stable id, the SQL template, and the tables it reads.
///
/// `tables` drives registration — a runner registers exactly these, so a query
/// never accidentally measures the cost of registering all eight.
pub struct TpchQuery {
    pub id: &'static str,
    pub name: &'static str,
    /// The SQL, possibly containing [`SCALE_FRACTION_PLACEHOLDER`].
    ///
    /// Deliberately *not* named `sql` and deliberately not runnable as-is:
    /// bind it with [`TpchQuery::sql_at_scale`]. A caller that reaches past
    /// that and executes the template gets a parse error naming the
    /// placeholder, which is the loud failure the previous silent
    /// wrong-at-SF100 behaviour lacked.
    pub sql_template: &'static str,
    pub tables: &'static [&'static str],
}

impl TpchQuery {
    /// The runnable SQL for `scale_factor`, with substitution parameters bound.
    ///
    /// Only Q11 is scale-dependent today; every other query returns its
    /// template unchanged, so this is cheap to call on all 22.
    pub fn sql_at_scale(&self, scale_factor: f64) -> String {
        if !self.sql_template.contains(SCALE_FRACTION_PLACEHOLDER) {
            return self.sql_template.to_owned();
        }
        // Fixed-point, never scientific notation: `1e-6` is valid in some SQL
        // dialects and a syntax error in others, and this corpus is executed
        // by more than one engine. 12 places holds the spec's fraction down to
        // SF 10^8.
        let fraction = format!("{:.12}", 0.0001_f64 / scale_factor);
        self.sql_template
            .replace(SCALE_FRACTION_PLACEHOLDER, &fraction)
    }
}

/// All 22 queries in spec order.
pub static TPCH_QUERIES: &[TpchQuery] = &[
    TpchQuery {
        id: "q1",
        name: "pricing_summary",
        tables: &["lineitem"],
        sql_template: "SELECT l_returnflag, l_linestatus, \
              sum(l_quantity) AS sum_qty, \
              sum(l_extendedprice) AS sum_base_price, \
              sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, \
              sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, \
              avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, \
              avg(l_discount) AS avg_disc, count(*) AS count_order \
              FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' \
              GROUP BY l_returnflag, l_linestatus \
              ORDER BY l_returnflag, l_linestatus",
    },
    TpchQuery {
        id: "q2",
        name: "minimum_cost_supplier",
        tables: &["part", "supplier", "partsupp", "nation", "region"],
        sql_template: "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment \
              FROM part, supplier, partsupp, nation, region \
              WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 \
              AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey \
              AND n_regionkey = r_regionkey AND r_name = 'EUROPE' \
              AND ps_supplycost = ( \
                SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region \
                WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey \
                AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey \
                AND r_name = 'EUROPE') \
              ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100",
    },
    TpchQuery {
        id: "q3",
        name: "shipping_priority",
        tables: &["customer", "orders", "lineitem"],
        sql_template: "SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, \
              o_orderdate, o_shippriority \
              FROM customer, orders, lineitem \
              WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey \
              AND l_orderkey = o_orderkey AND o_orderdate < DATE '1995-03-15' \
              AND l_shipdate > DATE '1995-03-15' \
              GROUP BY l_orderkey, o_orderdate, o_shippriority \
              ORDER BY revenue DESC, o_orderdate LIMIT 10",
    },
    TpchQuery {
        id: "q4",
        name: "order_priority_checking",
        tables: &["orders", "lineitem"],
        sql_template: "SELECT o_orderpriority, count(*) AS order_count FROM orders \
              WHERE o_orderdate >= DATE '1993-07-01' AND o_orderdate < DATE '1993-10-01' \
              AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey \
                          AND l_commitdate < l_receiptdate) \
              GROUP BY o_orderpriority ORDER BY o_orderpriority",
    },
    TpchQuery {
        id: "q5",
        name: "local_supplier_volume",
        tables: &["customer", "orders", "lineitem", "supplier", "nation", "region"],
        sql_template: "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue \
              FROM customer, orders, lineitem, supplier, nation, region \
              WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey \
              AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey \
              AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey \
              AND r_name = 'ASIA' AND o_orderdate >= DATE '1994-01-01' \
              AND o_orderdate < DATE '1995-01-01' \
              GROUP BY n_name ORDER BY revenue DESC",
    },
    TpchQuery {
        id: "q6",
        name: "forecasting_revenue_change",
        tables: &["lineitem"],
        sql_template: "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem \
              WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' \
              AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24",
    },
    TpchQuery {
        id: "q7",
        name: "volume_shipping",
        tables: &["supplier", "lineitem", "orders", "customer", "nation"],
        sql_template: "SELECT supp_nation, cust_nation, l_year, sum(volume) AS revenue FROM ( \
                SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, \
                EXTRACT(YEAR FROM l_shipdate) AS l_year, \
                l_extendedprice * (1 - l_discount) AS volume \
                FROM supplier, lineitem, orders, customer, nation n1, nation n2 \
                WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey \
                AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey \
                AND c_nationkey = n2.n_nationkey \
                AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') \
                  OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')) \
                AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31') AS shipping \
              GROUP BY supp_nation, cust_nation, l_year \
              ORDER BY supp_nation, cust_nation, l_year",
    },
    TpchQuery {
        id: "q8",
        name: "national_market_share",
        tables: &["part", "supplier", "lineitem", "orders", "customer", "nation", "region"],
        sql_template: "SELECT o_year, sum(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) AS mkt_share \
              FROM ( \
                SELECT EXTRACT(YEAR FROM o_orderdate) AS o_year, \
                l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation \
                FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region \
                WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey \
                AND l_orderkey = o_orderkey AND o_custkey = c_custkey \
                AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey \
                AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey \
                AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' \
                AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations \
              GROUP BY o_year ORDER BY o_year",
    },
    TpchQuery {
        id: "q9",
        name: "product_type_profit_measure",
        tables: &["part", "supplier", "lineitem", "partsupp", "orders", "nation"],
        sql_template: "SELECT nation, o_year, sum(amount) AS sum_profit FROM ( \
                SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year, \
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount \
                FROM part, supplier, lineitem, partsupp, orders, nation \
                WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey \
                AND ps_partkey = l_partkey AND p_partkey = l_partkey \
                AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey \
                AND p_name LIKE '%green%') AS profit \
              GROUP BY nation, o_year ORDER BY nation, o_year DESC",
    },
    TpchQuery {
        id: "q10",
        name: "returned_item_reporting",
        tables: &["customer", "orders", "lineitem", "nation"],
        sql_template: "SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) AS revenue, \
              c_acctbal, n_name, c_address, c_phone, c_comment \
              FROM customer, orders, lineitem, nation \
              WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey \
              AND o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01' \
              AND l_returnflag = 'R' AND c_nationkey = n_nationkey \
              GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment \
              ORDER BY revenue DESC LIMIT 20",
    },
    TpchQuery {
        id: "q11",
        name: "important_stock_identification",
        tables: &["partsupp", "supplier", "nation"],
        sql_template: "SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value \
              FROM partsupp, supplier, nation \
              WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY' \
              GROUP BY ps_partkey \
              HAVING sum(ps_supplycost * ps_availqty) > ( \
                SELECT sum(ps_supplycost * ps_availqty) * {{SCALE_FRACTION}} \
                FROM partsupp, supplier, nation \
                WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY') \
              ORDER BY value DESC",
    },
    TpchQuery {
        id: "q12",
        name: "shipping_modes_and_order_priority",
        tables: &["orders", "lineitem"],
        sql_template: "SELECT l_shipmode, \
              sum(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, \
              sum(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count \
              FROM orders, lineitem \
              WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP') \
              AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate \
              AND l_receiptdate >= DATE '1994-01-01' AND l_receiptdate < DATE '1995-01-01' \
              GROUP BY l_shipmode ORDER BY l_shipmode",
    },
    TpchQuery {
        id: "q13",
        name: "customer_distribution",
        tables: &["customer", "orders"],
        sql_template: "SELECT c_count, count(*) AS custdist FROM ( \
                SELECT c_custkey, count(o_orderkey) AS c_count \
                FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey \
                AND o_comment NOT LIKE '%special%requests%' \
                GROUP BY c_custkey) AS c_orders \
              GROUP BY c_count ORDER BY custdist DESC, c_count DESC",
    },
    TpchQuery {
        id: "q14",
        name: "promotion_effect",
        tables: &["lineitem", "part"],
        sql_template: "SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%' \
              THEN l_extendedprice * (1 - l_discount) ELSE 0 END) \
              / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue \
              FROM lineitem, part \
              WHERE l_partkey = p_partkey AND l_shipdate >= DATE '1995-09-01' \
              AND l_shipdate < DATE '1995-10-01'",
    },
    TpchQuery {
        id: "q15",
        name: "top_supplier",
        tables: &["supplier", "lineitem"],
        // The spec's CREATE VIEW revenue0 inlined as a CTE (same semantics,
        // one self-contained statement the distributed submit path can carry).
        sql_template: "WITH revenue0 AS ( \
                SELECT l_suppkey AS supplier_no, \
                sum(l_extendedprice * (1 - l_discount)) AS total_revenue \
                FROM lineitem \
                WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01' \
                GROUP BY l_suppkey) \
              SELECT s_suppkey, s_name, s_address, s_phone, total_revenue \
              FROM supplier, revenue0 \
              WHERE s_suppkey = supplier_no \
              AND total_revenue = (SELECT max(total_revenue) FROM revenue0) \
              ORDER BY s_suppkey",
    },
    TpchQuery {
        id: "q16",
        name: "parts_supplier_relationship",
        tables: &["partsupp", "part", "supplier"],
        sql_template: "SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey) AS supplier_cnt \
              FROM partsupp, part \
              WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45' \
              AND p_type NOT LIKE 'MEDIUM POLISHED%' \
              AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9) \
              AND ps_suppkey NOT IN (SELECT s_suppkey FROM supplier \
                                     WHERE s_comment LIKE '%Customer%Complaints%') \
              GROUP BY p_brand, p_type, p_size \
              ORDER BY supplier_cnt DESC, p_brand, p_type, p_size",
    },
    TpchQuery {
        id: "q17",
        name: "small_quantity_order_revenue",
        tables: &["lineitem", "part"],
        sql_template: "SELECT sum(l_extendedprice) / 7.0 AS avg_yearly FROM lineitem, part \
              WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX' \
              AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem \
                                WHERE l_partkey = p_partkey)",
    },
    TpchQuery {
        id: "q18",
        name: "large_volume_customer",
        tables: &["customer", "orders", "lineitem"],
        sql_template: "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) \
              FROM customer, orders, lineitem \
              WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem \
                                   GROUP BY l_orderkey HAVING sum(l_quantity) > 300) \
              AND c_custkey = o_custkey AND o_orderkey = l_orderkey \
              GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice \
              ORDER BY o_totalprice DESC, o_orderdate LIMIT 100",
    },
    TpchQuery {
        id: "q19",
        name: "discounted_revenue",
        tables: &["lineitem", "part"],
        sql_template: "SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue FROM lineitem, part \
              WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12' \
                AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') \
                AND l_quantity >= 1 AND l_quantity <= 11 AND p_size BETWEEN 1 AND 5 \
                AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') \
              OR (p_partkey = l_partkey AND p_brand = 'Brand#23' \
                AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') \
                AND l_quantity >= 10 AND l_quantity <= 20 AND p_size BETWEEN 1 AND 10 \
                AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') \
              OR (p_partkey = l_partkey AND p_brand = 'Brand#34' \
                AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') \
                AND l_quantity >= 20 AND l_quantity <= 30 AND p_size BETWEEN 1 AND 15 \
                AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')",
    },
    TpchQuery {
        id: "q20",
        name: "potential_part_promotion",
        tables: &["supplier", "nation", "partsupp", "part", "lineitem"],
        sql_template: "SELECT s_name, s_address FROM supplier, nation \
              WHERE s_suppkey IN ( \
                SELECT ps_suppkey FROM partsupp \
                WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%') \
                AND ps_availqty > (SELECT 0.5 * sum(l_quantity) FROM lineitem \
                                   WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey \
                                   AND l_shipdate >= DATE '1994-01-01' \
                                   AND l_shipdate < DATE '1995-01-01')) \
              AND s_nationkey = n_nationkey AND n_name = 'CANADA' ORDER BY s_name",
    },
    TpchQuery {
        id: "q21",
        name: "suppliers_who_kept_orders_waiting",
        tables: &["supplier", "lineitem", "orders", "nation"],
        sql_template: "SELECT s_name, count(*) AS numwait FROM supplier, lineitem l1, orders, nation \
              WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey \
              AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate \
              AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey \
                          AND l2.l_suppkey <> l1.l_suppkey) \
              AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey \
                              AND l3.l_suppkey <> l1.l_suppkey \
                              AND l3.l_receiptdate > l3.l_commitdate) \
              AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA' \
              GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100",
    },
    TpchQuery {
        id: "q22",
        name: "global_sales_opportunity",
        tables: &["customer", "orders"],
        sql_template: "SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal FROM ( \
                SELECT substr(c_phone, 1, 2) AS cntrycode, c_acctbal FROM customer \
                WHERE substr(c_phone, 1, 2) IN ('13','31','23','29','30','18','17') \
                AND c_acctbal > (SELECT avg(c_acctbal) FROM customer \
                                 WHERE c_acctbal > 0.00 \
                                 AND substr(c_phone, 1, 2) IN ('13','31','23','29','30','18','17')) \
                AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)) AS custsale \
              GROUP BY cntrycode ORDER BY cntrycode",
    },
];

/// Every distinct table referenced across the corpus.
pub fn all_tables() -> Vec<&'static str> {
    let mut tables: Vec<&'static str> =
        TPCH_QUERIES.iter().flat_map(|q| q.tables.iter().copied()).collect();
    tables.sort_unstable();
    tables.dedup();
    tables
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corpus_is_the_full_22_with_unique_ids() {
        assert_eq!(TPCH_QUERIES.len(), 22, "TPC-H has 22 queries");
        let mut ids: Vec<_> = TPCH_QUERIES.iter().map(|q| q.id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 22, "query ids must be unique");
        for n in 1..=22 {
            let id = format!("q{n}");
            assert!(
                TPCH_QUERIES.iter().any(|q| q.id == id),
                "missing {id} — a gap here silently narrows every published number"
            );
        }
    }

    /// True when `sql` names `table` as a whole identifier.
    ///
    /// Substring matching is wrong here: `partsupp` contains `part`, and
    /// `ps_partkey` contains it too. The first version of this check used
    /// `contains` and reported q11 as referencing `part` when it does not.
    fn names_table(sql: &str, table: &str) -> bool {
        let boundary = |c: char| !c.is_ascii_alphanumeric() && c != '_';
        sql.match_indices(table).any(|(index, _)| {
            let before_ok = index == 0
                || sql[..index].chars().next_back().is_some_and(boundary);
            let after = index + table.len();
            let after_ok = after >= sql.len()
                || sql[after..].chars().next().is_some_and(boundary);
            before_ok && after_ok
        })
    }

    #[test]
    fn every_query_declares_the_tables_it_names() {
        // A missing declaration means the runner does not register that table
        // and the query fails at plan time — cheap to catch here.
        for q in TPCH_QUERIES {
            for table in all_tables() {
                if names_table(q.sql_template, table) {
                    assert!(
                        q.tables.contains(&table),
                        "{} references {table} but does not declare it",
                        q.id
                    );
                }
            }
        }
    }

    #[test]
    fn names_table_respects_identifier_boundaries() {
        assert!(names_table("FROM partsupp, part WHERE", "part"));
        assert!(!names_table("FROM partsupp WHERE ps_partkey = 1", "part"));
        assert!(names_table("SELECT * FROM lineitem", "lineitem"));
    }

    fn q11() -> &'static TpchQuery {
        TPCH_QUERIES.iter().find(|q| q.id == "q11").expect("q11")
    }

    /// The regression test for the bug this placeholder exists to fix.
    ///
    /// Q11's threshold is `0.0001 / SF`. It was hard-coded at the SF1 value, so
    /// at SF100 it was 100x too high and the query returned zero rows — scoring
    /// "ok" while validating nothing. Asserting the *ratio* rather than the
    /// literal is the point: any future rewrite that stops scaling fails here.
    #[test]
    fn q11_threshold_scales_inversely_with_scale_factor() {
        assert!(
            q11().sql_template.contains(SCALE_FRACTION_PLACEHOLDER),
            "q11 must bind its threshold from the scale factor, not hard-code it"
        );
        assert!(q11().sql_at_scale(1.0).contains("0.000100000000"));
        assert!(q11().sql_at_scale(100.0).contains("0.000001000000"));
        assert!(q11().sql_at_scale(1000.0).contains("0.000000100000"));
    }

    /// Binding must leave no placeholder behind, at any scale, in any query.
    ///
    /// A leftover `{{...}}` reaches the engine as a syntax error — loud, which
    /// is the intended failure mode — but this catches it before a cluster run
    /// burns an hour discovering it.
    #[test]
    fn binding_leaves_no_placeholder_in_any_query() {
        for scale in [1.0, 10.0, 100.0, 1000.0, 10_000.0] {
            for q in TPCH_QUERIES {
                let sql = q.sql_at_scale(scale);
                assert!(
                    !sql.contains("{{") && !sql.contains("}}"),
                    "{} still holds a placeholder at SF{scale}: {sql}",
                    q.id
                );
            }
        }
    }

    /// Scientific notation is a portability trap: this corpus is executed by
    /// more than one engine and `1e-6` is not universally accepted.
    #[test]
    fn bound_fraction_is_never_scientific_notation() {
        for (scale, expected) in [
            (1.0, "0.000100000000"),
            (100.0, "0.000001000000"),
            (1_000_000.0, "0.000000000100"),
            (100_000_000.0, "0.000000000001"),
        ] {
            let sql = q11().sql_at_scale(scale);
            assert!(
                sql.contains(expected),
                "SF{scale} should bind {expected}, got: {sql}"
            );
            // `1e-6` parses in some SQL dialects and is a syntax error in
            // others, and this corpus is executed by more than one engine.
            assert!(
                !sql.contains("e-") && !sql.contains("E-"),
                "SF{scale} produced exponent notation: {sql}"
            );
        }
    }

    /// Only Q11 is scale-dependent; every other query binds to itself.
    #[test]
    fn non_scaled_queries_bind_to_their_template_unchanged() {
        for q in TPCH_QUERIES.iter().filter(|q| q.id != "q11") {
            assert_eq!(
                q.sql_at_scale(100.0),
                q.sql_template,
                "{} changed under binding but declares no placeholder",
                q.id
            );
        }
    }

    /// The placeholder sits where the spec's substitution parameter goes — in
    /// the subquery's multiplier, not in the outer aggregate.
    #[test]
    fn q11_binds_the_subquery_multiplier() {
        let sql = q11().sql_at_scale(100.0);
        let subquery = sql.split("HAVING").nth(1).expect("q11 has a HAVING");
        assert!(
            subquery.contains("0.000001000000"),
            "the fraction must land inside the HAVING subquery: {subquery}"
        );
    }
}
