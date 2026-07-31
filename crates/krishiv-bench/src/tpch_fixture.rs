//! A tiny, in-memory TPC-H database that the corpus queries actually match.
//!
//! # Why this exists
//!
//! The 22 queries in [`crate::tpch_queries`] were only ever run against real
//! SF10/SF100 data, which means every correctness question about them — does
//! staging change the answer? does converting a join change the answer? — could
//! only be asked on a cluster, hours at a time. Several bugs lived for weeks
//! behind that cost: q17 and q19 failed every SF100 sweep with a bare Arrow type
//! error that reproduces here in under a second.
//!
//! # The predicates are the hard part
//!
//! TPC-H's selectivity is the point of the benchmark, and a fixture of arbitrary
//! rows returns nothing for almost every query — which passes an
//! "staged == direct" assertion vacuously. So the values below are chosen to
//! satisfy the spec's validation parameters: `EUROPE`/`ASIA`/`AMERICA` regions,
//! `GERMANY`/`FRANCE`/`BRAZIL` nations, `Brand#12|23|34`, `MED BOX`/`SM CASE`
//! containers, `%BRASS` and `PROMO%` types, `BUILDING` market segment, ship
//! modes `MAIL`/`SHIP`/`AIR`/`TRUCK`/`RAIL`/`REG AIR`/`FOB`, return flags
//! `A`/`N`/`R`, and dates inside the 1994-1996 windows the queries ask for.
//!
//! [`row_producing_queries`] records which queries this yields rows for, so a
//! test can assert the fixture still exercises them instead of quietly decaying
//! into 22 empty comparisons.

/// `CREATE TABLE` statements for all eight TPC-H tables, in dependency order.
///
/// Typed columns with untyped literal rows: the declared type is what the
/// planner sees (decimal widths and dates drive both predicate pushdown and the
/// decimal arithmetic q17 and q19 turn on), while the rows stay readable.
#[must_use]
pub fn fixture_ddl() -> Vec<&'static str> {
    vec![
        "CREATE TABLE region(r_regionkey BIGINT, r_name VARCHAR, r_comment VARCHAR) AS VALUES \
         (0, 'AFRICA', 'a'), (1, 'AMERICA', 'b'), (2, 'ASIA', 'c'), \
         (3, 'EUROPE', 'd'), (4, 'MIDDLE EAST', 'e')",

        "CREATE TABLE nation(n_nationkey BIGINT, n_name VARCHAR, n_regionkey BIGINT, \
         n_comment VARCHAR) AS VALUES \
         (0, 'ALGERIA', 0, 'a'), (1, 'ARGENTINA', 1, 'b'), (2, 'BRAZIL', 1, 'c'), \
         (3, 'CANADA', 1, 'd'), (7, 'GERMANY', 3, 'e'), (6, 'FRANCE', 3, 'f'), \
         (8, 'INDIA', 2, 'g'), (9, 'INDONESIA', 2, 'h'), \
         (18, 'CHINA', 2, 'i'), (24, 'UNITED STATES', 1, 'j')",

        "CREATE TABLE supplier(s_suppkey BIGINT, s_name VARCHAR, s_address VARCHAR, \
         s_nationkey BIGINT, s_phone VARCHAR, s_acctbal DECIMAL(15,2), s_comment VARCHAR) AS VALUES \
         (1, 'Supplier#000000001', 'addr1', 7, '27-918-335-1736', 5755.94, 'ordinary'), \
         (2, 'Supplier#000000002', 'addr2', 6, '15-679-861-2259', 4032.68, 'furiously'), \
         (3, 'Supplier#000000003', 'addr3', 2, '11-383-516-1199', 4192.40, 'Customer Complaints'), \
         (4, 'Supplier#000000004', 'addr4', 1, '25-843-787-7479', 4641.08, 'Customer complaints'), \
         (5, 'Supplier#000000005', 'addr5', 8, '21-151-690-3663', -283.84, 'regular'), \
         (6, 'Supplier#000000006', 'addr6', 24, '34-627-449-5959', 1365.79, 'quickly'), \
         (7, 'Supplier#000000007', 'addr7', 18, '28-101-101-1010', 2000.00, 'even')",

        "CREATE TABLE part(p_partkey BIGINT, p_name VARCHAR, p_mfgr VARCHAR, p_brand VARCHAR, \
         p_type VARCHAR, p_size INT, p_container VARCHAR, p_retailprice DECIMAL(15,2), \
         p_comment VARCHAR) AS VALUES \
         (1, 'goldenrod lace', 'Manufacturer#1', 'Brand#13', 'PROMO BURNISHED COPPER', 14, \
          'JUMBO PKG', 901.00, 'a'), \
         (2, 'blush thistle', 'Manufacturer#1', 'Brand#12', 'LARGE BRASS', 15, \
          'SM CASE', 902.00, 'b'), \
         (3, 'dark green', 'Manufacturer#4', 'Brand#42', 'STANDARD POLISHED TIN', 23, \
          'MED BOX', 903.00, 'c'), \
         (4, 'chocolate metallic', 'Manufacturer#3', 'Brand#23', 'SMALL PLATED BRASS', 15, \
          'MED BOX', 904.00, 'd'), \
         (5, 'forest blush', 'Manufacturer#3', 'Brand#34', 'PROMO PLATED STEEL', 10, \
          'LG CASE', 905.00, 'e'), \
         (6, 'white ivory', 'Manufacturer#2', 'Brand#12', 'PROMO BRUSHED STEEL', 4, \
          'SM BOX', 906.00, 'f'), \
         (7, 'blue saddle', 'Manufacturer#2', 'Brand#23', 'MEDIUM BURNISHED TIN', 9, \
          'MED BAG', 907.00, 'g'), \
         (8, 'ivory olive', 'Manufacturer#4', 'Brand#34', 'ECONOMY BRUSHED NICKEL', 15, \
          'LG BOX', 908.00, 'h')",

        "CREATE TABLE partsupp(ps_partkey BIGINT, ps_suppkey BIGINT, ps_availqty INT, \
         ps_supplycost DECIMAL(15,2), ps_comment VARCHAR) AS VALUES \
         (1, 1, 3325, 771.64, 'a'), (2, 1, 8895, 378.49, 'b'), (2, 2, 4969, 915.27, 'c'), \
         (3, 2, 8539, 438.37, 'd'), (4, 3, 3956, 920.92, 'e'), (4, 4, 4069, 100.43, 'f'), \
         (5, 4, 8895, 336.42, 'g'), (6, 5, 4297, 137.85, 'h'), (7, 5, 3325, 771.64, 'i'), \
         (8, 6, 1287, 771.64, 'j'), (1, 2, 2222, 500.00, 'k'), (3, 1, 1111, 250.00, 'l')",

        "CREATE TABLE customer(c_custkey BIGINT, c_name VARCHAR, c_address VARCHAR, \
         c_nationkey BIGINT, c_phone VARCHAR, c_acctbal DECIMAL(15,2), c_mktsegment VARCHAR, \
         c_comment VARCHAR) AS VALUES \
         (1, 'Customer#000000001', 'addr1', 7, '13-719-748-1637', 711.56, 'BUILDING', 'a'), \
         (2, 'Customer#000000002', 'addr2', 6, '23-768-687-3665', 8000.00, 'AUTOMOBILE', 'b'), \
         (3, 'Customer#000000003', 'addr3', 1, '11-719-748-1637', 7498.12, 'AUTOMOBILE', 'c'), \
         (4, 'Customer#000000004', 'addr4', 2, '31-345-227-5993', 2866.83, 'MACHINERY', 'd'), \
         (5, 'Customer#000000005', 'addr5', 3, '13-750-942-6364', 794.47, 'HOUSEHOLD', 'e'), \
         (6, 'Customer#000000006', 'addr6', 24, '30-114-968-4951', 7638.57, 'BUILDING', 'f'), \
         (7, 'Customer#000000007', 'addr7', 18, '29-345-227-5993', 9561.95, 'BUILDING', 'g'), \
         (8, 'Customer#000000008', 'addr8', 8, '17-750-942-6364', 6819.74, 'BUILDING', 'h')",

        "CREATE TABLE orders(o_orderkey BIGINT, o_custkey BIGINT, o_orderstatus VARCHAR, \
         o_totalprice DECIMAL(15,2), o_orderdate DATE, o_orderpriority VARCHAR, o_clerk VARCHAR, \
         o_shippriority INT, o_comment VARCHAR) AS VALUES \
         (1, 1, 'O', 173665.47, DATE '1996-01-02', '5-LOW', 'Clerk#000000951', 0, 'a'), \
         (2, 3, 'O', 46929.18, DATE '1996-12-01', '1-URGENT', 'Clerk#000000880', 0, 'b'), \
         (3, 5, 'F', 193846.25, DATE '1993-08-14', '5-LOW', 'Clerk#000000955', 0, 'c'), \
         (4, 6, 'O', 32151.78, DATE '1995-10-11', '5-LOW', 'Clerk#000000124', 0, 'd'), \
         (5, 7, 'F', 144659.20, DATE '1994-07-30', '5-LOW', 'Clerk#000000925', 0, 'e'), \
         (6, 8, 'F', 58749.59, DATE '1992-02-21', '4-NOT SPECIFIED', 'Clerk#000000058', 0, 'f'), \
         (7, 1, 'O', 252004.18, DATE '1996-01-10', '2-HIGH', 'Clerk#000000470', 0, 'g'), \
         (32, 4, 'O', 208660.75, DATE '1995-07-16', '2-HIGH', 'Clerk#000000616', 0, 'h'), \
         (33, 6, 'F', 163243.98, DATE '1993-10-27', '3-MEDIUM', 'Clerk#000000409', 0, 'i'), \
         (34, 7, 'O', 58949.67, DATE '1998-07-21', '3-MEDIUM', 'Clerk#000000223', 0, 'j')",

        // Two lines per order for most orders, so the multi-line predicates
        // (q18's quantity sum, q21's multi-supplier EXISTS/NOT EXISTS) have
        // something to find rather than trivially failing.
        "CREATE TABLE lineitem(l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, \
         l_linenumber INT, l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), \
         l_discount DECIMAL(15,2), l_tax DECIMAL(15,2), l_returnflag VARCHAR, \
         l_linestatus VARCHAR, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, \
         l_shipinstruct VARCHAR, l_shipmode VARCHAR, l_comment VARCHAR) AS VALUES \
         (1, 2, 1, 1, 17.00, 21168.23, 0.04, 0.02, 'N', 'O', DATE '1996-03-13', \
          DATE '1996-02-12', DATE '1996-03-22', 'DELIVER IN PERSON', 'TRUCK', 'a'), \
         (1, 4, 3, 2, 36.00, 45983.16, 0.09, 0.06, 'N', 'O', DATE '1996-04-12', \
          DATE '1996-02-28', DATE '1996-04-20', 'TAKE BACK RETURN', 'MAIL', 'b'), \
         (2, 6, 5, 1, 8.00, 13309.60, 0.10, 0.02, 'N', 'O', DATE '1997-01-28', \
          DATE '1997-01-14', DATE '1997-02-02', 'TAKE BACK RETURN', 'RAIL', 'c'), \
         (3, 2, 2, 1, 45.00, 54058.05, 0.06, 0.00, 'R', 'F', DATE '1994-02-02', \
          DATE '1994-01-04', DATE '1994-02-23', 'NONE', 'AIR', 'd'), \
         (3, 5, 4, 2, 49.00, 46796.47, 0.10, 0.00, 'R', 'F', DATE '1993-11-09', \
          DATE '1993-12-20', DATE '1993-11-24', 'TAKE BACK RETURN', 'RAIL', 'e'), \
         (4, 7, 5, 1, 30.00, 30690.90, 0.03, 0.08, 'N', 'O', DATE '1995-10-23', \
          DATE '1995-11-25', DATE '1995-11-01', 'NONE', 'REG AIR', 'f'), \
         (5, 3, 2, 1, 15.00, 14761.65, 0.02, 0.04, 'R', 'F', DATE '1994-10-31', \
          DATE '1994-08-31', DATE '1994-11-20', 'NONE', 'AIR', 'g'), \
         (5, 8, 6, 2, 26.00, 26113.18, 0.07, 0.08, 'A', 'F', DATE '1994-09-30', \
          DATE '1994-09-01', DATE '1994-10-12', 'DELIVER IN PERSON', 'FOB', 'h'), \
         (6, 4, 3, 1, 37.00, 39890.88, 0.08, 0.03, 'A', 'F', DATE '1992-04-27', \
          DATE '1992-05-15', DATE '1992-05-02', 'TAKE BACK RETURN', 'TRUCK', 'i'), \
         (7, 2, 1, 1, 12.00, 14208.00, 0.07, 0.03, 'N', 'O', DATE '1996-01-30', \
          DATE '1996-02-07', DATE '1996-02-03', 'DELIVER IN PERSON', 'FOB', 'j'), \
         (7, 6, 5, 2, 9.00, 10063.44, 0.08, 0.08, 'N', 'O', DATE '1996-02-01', \
          DATE '1996-02-19', DATE '1996-02-05', 'TAKE BACK RETURN', 'MAIL', 'k'), \
         (32, 4, 4, 1, 28.00, 28638.20, 0.05, 0.08, 'N', 'O', DATE '1995-08-07', \
          DATE '1995-10-07', DATE '1995-08-23', 'DELIVER IN PERSON', 'AIR', 'l'), \
         (32, 1, 1, 2, 32.00, 30583.68, 0.02, 0.00, 'N', 'O', DATE '1995-08-14', \
          DATE '1995-10-07', DATE '1995-08-27', 'COLLECT COD', 'SHIP', 'm'), \
         (33, 5, 4, 1, 31.00, 31509.55, 0.09, 0.04, 'A', 'F', DATE '1993-10-29', \
          DATE '1993-12-19', DATE '1993-11-08', 'COLLECT COD', 'TRUCK', 'n'), \
         (33, 7, 5, 2, 32.00, 32189.44, 0.02, 0.05, 'R', 'F', DATE '1993-12-09', \
          DATE '1993-12-25', DATE '1993-12-23', 'TAKE BACK RETURN', 'AIR', 'o'), \
         (34, 8, 6, 1, 13.00, 13058.13, 0.00, 0.07, 'N', 'O', DATE '1998-10-23', \
          DATE '1998-09-14', DATE '1998-11-06', 'NONE', 'REG AIR', 'p'), \
         (5, 3, 7, 3, 21.00, 20500.00, 0.05, 0.03, 'N', 'O', DATE '1995-04-01', \
          DATE '1995-03-20', DATE '1995-04-10', 'NONE', 'SHIP', 'q'), \
         (5, 6, 2, 4, 11.00, 11500.00, 0.03, 0.02, 'N', 'O', DATE '1994-08-01', \
          DATE '1994-08-15', DATE '1994-09-01', 'NONE', 'MAIL', 'r'), \
         (1, 5, 2, 3, 24.00, 24500.00, 0.06, 0.04, 'N', 'O', DATE '1995-06-15', \
          DATE '1995-06-01', DATE '1995-06-25', 'NONE', 'AIR', 's')",
    ]
}

/// Query ids this fixture is known to produce at least one output row for.
///
/// Not every TPC-H query can be made non-empty by a handful of rows without
/// distorting the data, but a test that compared 22 empty results against 22
/// empty results would pass while proving nothing. Asserting this set keeps the
/// fixture honest: if a change makes one of these go empty, the coverage it was
/// providing is gone and the test says so.
#[must_use]
pub fn row_producing_queries() -> &'static [&'static str] {
    &[
        "q1", "q3", "q4", "q5", "q6", "q7", "q10", "q11", "q12", "q13", "q14", "q16", "q19", "q22",
    ]
}

/// Re-register every fixture table carrying the primary key TPC-H declares for
/// it, so key-dependent optimizations can be exercised offline.
///
/// # Why this is not part of [`fixture_ddl`]
///
/// `CREATE TABLE … AS VALUES` has nowhere to put a constraint, and the tables
/// have to exist before their contents can be read back. So this is a second
/// pass: read each table's batches, rebuild it as a `MemTable` carrying
/// `Constraint::PrimaryKey`, and swap it in. The data is a few dozen rows, so
/// the round trip costs nothing.
///
/// Without this, every functional-dependency optimization —
/// `optimize_projections`' group-by pruning and
/// `krishiv_sql::late_materialize`'s join-back — is **inert against the
/// fixture**, and a test suite that never declares a key cannot tell a rewrite
/// that is correct from one that never ran.
///
/// # Errors
///
/// Returns the first failure as a string: a missing table, a key column that is
/// not in the schema, or a registration that the context rejects.
pub async fn declare_fixture_primary_keys(
    ctx: &datafusion::prelude::SessionContext,
) -> Result<(), String> {
    use datafusion::common::{Constraint, Constraints};
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    for (table, key) in crate::tpch_queries::TPCH_PRIMARY_KEYS {
        let df = ctx
            .table(*table)
            .await
            .map_err(|e| format!("fixture table '{table}': {e}"))?;
        let schema: datafusion::arrow::datatypes::SchemaRef =
            Arc::new(df.schema().as_arrow().clone());
        let mut indices = Vec::with_capacity(key.len());
        for column in *key {
            indices.push(
                schema
                    .index_of(column)
                    .map_err(|_| format!("'{column}' is not a column of fixture '{table}'"))?,
            );
        }
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("reading fixture '{table}': {e}"))?;
        let provider = MemTable::try_new(Arc::clone(&schema), vec![batches])
            .map_err(|e| format!("rebuilding fixture '{table}': {e}"))?
            .with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                indices,
            )]));
        ctx.deregister_table(*table)
            .map_err(|e| format!("deregistering fixture '{table}': {e}"))?;
        ctx.register_table(*table, Arc::new(provider))
            .map_err(|e| format!("re-registering fixture '{table}': {e}"))?;
    }
    Ok(())
}
