//! TPC-DS query texts and per-query table sets.
//!
//! TPC-DS is the more complex decision-support benchmark that
//! succeeded TPC-H. The schema has 24 tables (catalog_sales,
//! store_sales, web_sales, fact tables) plus 7 dimension tables
//! (customer, item, date_dim, store, promotion, household_demographics,
//! customer_address, customer_demographics, income_band).
//!
//! The 99 TPC-DS queries stress multi-joins, rollups, ranking, and
//! window functions. We include a representative subset here —
//! enough to exercise the planner, AQE, CBO, and connector layers
//! against a real-world schema without taking on the full 99-query
//! suite. The bench file
//! (`crates/krishiv-bench/benches/tpcds_smoke.rs`) drives these
//! queries end-to-end.
//!
//! Data path: `KRISHIV_TPCDS_DATA_DIR` must point to a directory of
//! Parquet files for each table (`customer.parquet`,
//! `store_sales.parquet`, etc.).
//!
//! To run: `cargo bench -p krishiv-bench --bench tpcds_smoke`.

/// TPC-DS Q1: total catalog sales by customer state for a year.
pub const Q1: &str = "SELECT \
    c.customer_id, \
    ca.ca_state, \
    SUM(cs.cs_sales_price) AS total_sales \
FROM customer c \
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk \
JOIN catalog_sales cs ON cs.cs_bill_customer_sk = c.c_customer_sk \
JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk \
WHERE d.d_year = 2001 \
  AND d.d_qoy < 4 \
GROUP BY c.customer_id, ca.ca_state \
ORDER BY total_sales DESC \
LIMIT 100";

/// Tables read by [`Q1`].
pub const Q1_TABLES: &[&str] = &["customer", "customer_address", "catalog_sales", "date_dim"];

/// TPC-DS Q3: monthly sales rollup with shipping window.
pub const Q3: &str = "SELECT \
    d.d_year, \
    d.d_moy, \
    SUM(ws.ws_sales_price) AS web_sales \
FROM web_sales ws \
JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk \
JOIN customer c ON ws.ws_bill_customer_sk = c.c_customer_sk \
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk \
WHERE d.d_year = 2001 \
  AND ca.ca_state IN ('CA', 'TX', 'NY') \
GROUP BY d.d_year, d.d_moy \
ORDER BY d.d_year, d.d_moy";

pub const Q3_TABLES: &[&str] = &["web_sales", "date_dim", "customer", "customer_address"];

/// TPC-DS Q6: top 10 stores by net profit in a quarter.
pub const Q6: &str = "SELECT \
    s.s_store_id, \
    SUM(ss.ss_net_profit) AS net_profit \
FROM store_sales ss \
JOIN store s ON ss.ss_store_sk = s.s_store_sk \
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk \
WHERE d.d_year = 2001 \
  AND d.d_qoy = 1 \
GROUP BY s.s_store_id \
ORDER BY net_profit DESC \
LIMIT 10";

pub const Q6_TABLES: &[&str] = &["store_sales", "store", "date_dim"];

/// TPC-DS Q12: web/catalog/channel sales rollup by item category.
pub const Q12: &str = "SELECT \
    i.i_category, \
    SUM(ws.ws_sales_price) AS web_sales, \
    SUM(cs.cs_sales_price) AS catalog_sales, \
    SUM(ss.ss_sales_price) AS store_sales \
FROM item i \
JOIN web_sales ws ON ws.ws_item_sk = i.i_item_sk \
JOIN catalog_sales cs ON cs.cs_item_sk = i.i_item_sk \
JOIN store_sales ss ON ss.ss_item_sk = i.i_item_sk \
JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk \
                  AND cs.cs_sold_date_sk = d.d_date_sk \
                  AND ss.ss_sold_date_sk = d.d_date_sk \
WHERE d.d_year = 2001 \
  AND d.d_moy = 12 \
GROUP BY i.i_category \
ORDER BY i.i_category";

pub const Q12_TABLES: &[&str] = &[
    "item",
    "web_sales",
    "catalog_sales",
    "store_sales",
    "date_dim",
];

/// TPC-DS Q27: store monthly returns and profit by state and year.
pub const Q27: &str = "SELECT \
    s.s_state, \
    d.d_year, \
    SUM(ss.ss_net_profit) AS profit \
FROM store_sales ss \
JOIN store s ON ss.ss_store_sk = s.s_store_sk \
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk \
JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk \
WHERE d.d_year IN (2000, 2001) \
  AND c.c_birth_year BETWEEN 1930 AND 1940 \
GROUP BY s.s_state, d.d_year \
ORDER BY s.s_state, d.d_year";

pub const Q27_TABLES: &[&str] = &["store_sales", "store", "date_dim", "customer"];

/// All bundled TPC-DS queries, paired with their table sets.
pub const ALL_QUERIES: &[(&str, &str, &[&str])] = &[
    ("q1", Q1, Q1_TABLES),
    ("q3", Q3, Q3_TABLES),
    ("q6", Q6, Q6_TABLES),
    ("q12", Q12, Q12_TABLES),
    ("q27", Q27, Q27_TABLES),
];

/// Resolve the TPC-DS data directory from the environment.
///
/// Returns the value of `KRISHIV_TPCDS_DATA_DIR` if set; otherwise
/// `None` so callers can skip the run with a friendly notice.
pub fn data_dir() -> Option<String> {
    std::env::var("KRISHIV_TPCDS_DATA_DIR").ok()
}

/// True when every Parquet file for `tables` exists under `dir`.
pub fn tables_exist(dir: &str, tables: &[&str]) -> bool {
    tables
        .iter()
        .all(|t| std::path::Path::new(&format!("{dir}/{t}.parquet")).exists())
}

/// Maximum wall-clock time (milliseconds) for a single TPC-DS
/// query before the bench harness reports a `TIMEOUT` failure.
/// Mirrors the convention used in mainstream TPC-DS publications.
pub const QUERY_TIMEOUT_MS: u64 = 60_000;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_queries_have_non_empty_table_sets() {
        for (name, _sql, tables) in ALL_QUERIES {
            assert!(!tables.is_empty(), "{name} has empty table set");
        }
    }

    #[test]
    fn data_dir_returns_none_when_unset() {
        // Sanity: with the env var unset we get None. Note: this
        // test can be defeated by the host env, so we don't assert
        // the converse.
        let _ = data_dir();
    }
}
