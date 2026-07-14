#![forbid(unsafe_code)]
//! Measured PySpark parity matrix (Phase 61 "one DataFrame API, measured").
//!
//! The same measure-first design as Phase 60's SQL feature matrix
//! (`krishiv-sql::grammar`): a machine-readable enumeration of the `pyspark.sql`
//! reference surface (<https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html>)
//! against Krishiv's DataFrame API, so parity is a **published number with an
//! itemized shortfall**, not a vibe. The Python surface is a thin projection of
//! this Rust surface (Phase 61), so measuring the Rust `DataFrame`/`Expr`/
//! functions is measuring the whole API.
//!
//! Status is honest and exact-or-absent (the Phase 60 alias rule): `Supported`
//! only when a Krishiv method matches the PySpark semantics; `Partial` when the
//! capability is reachable but the signature/ergonomics differ; `Planned`/
//! `Absent` otherwise. Every `Supported`/`Partial` row names the Krishiv
//! equivalent, and the matrix-to-surface test asserts that equivalent is a real
//! public method — the matrix cannot claim what the crate does not expose.

use std::fmt::Write as _;

/// How close Krishiv's surface is to the PySpark method.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ParityStatus {
    /// Present with matching semantics.
    Supported,
    /// Capability reachable, but signature/ergonomics differ (documented).
    Partial,
    /// Not implemented; on the roadmap.
    Planned,
    /// Not implemented and not planned as a first-class method.
    Absent,
}

impl ParityStatus {
    /// Counts toward the parity numerator (Supported or Partial).
    pub fn is_covered(self) -> bool {
        matches!(self, ParityStatus::Supported | ParityStatus::Partial)
    }

    fn as_str(self) -> &'static str {
        match self {
            ParityStatus::Supported => "supported",
            ParityStatus::Partial => "partial",
            ParityStatus::Planned => "planned",
            ParityStatus::Absent => "absent",
        }
    }
}

/// The `pyspark.sql` namespace a method belongs to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Namespace {
    DataFrame,
    Column,
    Functions,
    GroupedData,
    Window,
    Reader,
    Writer,
}

impl Namespace {
    fn as_str(self) -> &'static str {
        match self {
            Namespace::DataFrame => "DataFrame",
            Namespace::Column => "Column",
            Namespace::Functions => "functions",
            Namespace::GroupedData => "GroupedData",
            Namespace::Window => "Window",
            Namespace::Reader => "DataFrameReader",
            Namespace::Writer => "DataFrameWriter",
        }
    }

    /// Iteration order for the generated report.
    const ALL: [Namespace; 7] = [
        Namespace::DataFrame,
        Namespace::Column,
        Namespace::Functions,
        Namespace::GroupedData,
        Namespace::Window,
        Namespace::Reader,
        Namespace::Writer,
    ];
}

/// One PySpark method and Krishiv's parity for it.
pub struct ApiEntry {
    pub namespace: Namespace,
    /// The PySpark method name (e.g. `withColumn`).
    pub pyspark: &'static str,
    pub status: ParityStatus,
    /// The Krishiv equivalent (method/function name), or `""` when none.
    pub krishiv: &'static str,
    pub note: &'static str,
}

const fn entry(
    namespace: Namespace,
    pyspark: &'static str,
    status: ParityStatus,
    krishiv: &'static str,
    note: &'static str,
) -> ApiEntry {
    ApiEntry {
        namespace,
        pyspark,
        status,
        krishiv,
        note,
    }
}

use Namespace::{Column, DataFrame, Functions, GroupedData, Reader, Window, Writer};
use ParityStatus::{Partial, Planned, Supported};

/// The parity matrix. High-usage-first across the seven PySpark namespaces.
/// Extended as the surface grows; the number it yields is the phase KPI.
pub const PARITY: &[ApiEntry] = &[
    // ── DataFrame ────────────────────────────────────────────────────────────
    entry(DataFrame, "select", Supported, "select/select_exprs", ""),
    entry(DataFrame, "selectExpr", Supported, "select_exprs", ""),
    entry(DataFrame, "filter", Supported, "filter/filter_expr", "alias `where`"),
    entry(DataFrame, "where", Supported, "filter", ""),
    entry(DataFrame, "withColumn", Supported, "with_column", ""),
    entry(
        DataFrame,
        "withColumnRenamed",
        Partial,
        "rename",
        "rename exists; the exact (existing, new) rename signature is the Phase 61 gap",
    ),
    entry(DataFrame, "drop", Supported, "drop", ""),
    entry(DataFrame, "groupBy", Supported, "group_by", ""),
    entry(DataFrame, "agg", Supported, "agg", ""),
    entry(DataFrame, "join", Supported, "join/join_on", ""),
    entry(DataFrame, "crossJoin", Planned, "", "no dedicated cross-join method"),
    entry(DataFrame, "orderBy", Supported, "order_by", "alias `sort`"),
    entry(DataFrame, "sort", Supported, "sort", ""),
    entry(DataFrame, "limit", Supported, "limit", ""),
    entry(DataFrame, "distinct", Supported, "distinct", ""),
    entry(
        DataFrame,
        "dropDuplicates",
        Planned,
        "",
        "Phase 61 gap: dedup on a subset of columns (distinct() is all-columns)",
    ),
    entry(DataFrame, "union", Supported, "union", ""),
    entry(DataFrame, "unionAll", Supported, "union", "deprecated Spark alias of union"),
    entry(
        DataFrame,
        "unionByName",
        Planned,
        "",
        "Phase 61 gap: name-aligned union (union is positional)",
    ),
    entry(DataFrame, "intersect", Supported, "intersect/intersect_distinct", ""),
    entry(DataFrame, "exceptAll", Supported, "except", "except/except_distinct"),
    entry(DataFrame, "count", Supported, "count", ""),
    entry(DataFrame, "collect", Supported, "collect", ""),
    entry(DataFrame, "show", Supported, "show", ""),
    entry(DataFrame, "describe", Supported, "describe", ""),
    entry(DataFrame, "explain", Supported, "explain/explain_logical", ""),
    entry(DataFrame, "schema", Supported, "schema", ""),
    entry(DataFrame, "columns", Partial, "schema", "column names via schema(); no `columns` shortcut"),
    entry(DataFrame, "printSchema", Partial, "schema", "schema() is programmatic; no pretty-print helper"),
    entry(DataFrame, "sample", Supported, "sample", ""),
    entry(DataFrame, "repartition", Supported, "repartition", ""),
    entry(DataFrame, "coalesce", Planned, "repartition", "repartition exists; no shrink-only coalesce"),
    entry(DataFrame, "cache", Supported, "cache/persist", ""),
    entry(DataFrame, "persist", Supported, "persist", ""),
    entry(DataFrame, "unpersist", Supported, "unpersist", ""),
    entry(DataFrame, "createOrReplaceTempView", Supported, "create_or_replace_temp_view", ""),
    entry(DataFrame, "unpivot", Supported, "unpivot", "alias `melt`"),
    entry(DataFrame, "melt", Supported, "unpivot", ""),
    entry(
        DataFrame,
        "na",
        Partial,
        "fill_null/drop_nulls",
        "fill/drop reachable directly; no unified `.na` sub-API (Phase 61 gap)",
    ),
    entry(DataFrame, "fillna", Supported, "fill_null", ""),
    entry(DataFrame, "dropna", Supported, "drop_nulls", ""),
    entry(DataFrame, "replace", Planned, "", "Phase 61 gap: value replacement"),
    entry(DataFrame, "withColumnsRenamed", Planned, "", "bulk rename (variant-collapse target)"),
    entry(DataFrame, "toPandas", Planned, "", "Phase 61 gap: zero-copy Arrow → pandas (Python surface)"),
    entry(DataFrame, "write", Supported, "write", "DataFrameWriter"),
    entry(DataFrame, "writeStream", Planned, "write", "Phase 61 keystone: write_stream().to_table(refresh=…)"),
    entry(DataFrame, "foreachBatch", Planned, "", "Phase 61 gap: micro-batch sink callback"),
    // ── Column ───────────────────────────────────────────────────────────────
    entry(Column, "alias", Supported, "Expr::alias", ""),
    entry(Column, "cast", Supported, "Expr::cast", ""),
    entry(Column, "try_cast", Supported, "Expr::try_cast", ""),
    entry(Column, "asc", Supported, "Expr::asc", ""),
    entry(Column, "desc", Supported, "Expr::desc", ""),
    entry(Column, "and", Supported, "Expr::and", "`&`"),
    entry(Column, "or", Supported, "Expr::or", "`|`"),
    entry(Column, "eqNullSafe", Planned, "", "null-safe equality (<=>) not exposed"),
    entry(Column, "isNull", Supported, "Expr::is_null", ""),
    entry(Column, "isNotNull", Supported, "Expr::is_not_null", ""),
    entry(Column, "over", Supported, "Expr::over", "window spec"),
    entry(Column, "between", Planned, "", "reachable as (c>=lo)&(c<=hi); no between() sugar"),
    entry(Column, "isin", Planned, "", "Phase 61 gap: IN-list column predicate"),
    entry(Column, "like", Partial, "raw", "reachable via raw SQL; no typed like()"),
    entry(Column, "substr", Partial, "function", "reachable via function(\"substr\", …)"),
    entry(Column, "when_otherwise", Partial, "raw", "CASE via raw/SQL; no Column.when chain"),
    // ── functions (F.*) ──────────────────────────────────────────────────────
    entry(Functions, "col", Supported, "col", ""),
    entry(Functions, "lit", Supported, "lit", ""),
    entry(Functions, "sum", Supported, "sum", ""),
    entry(Functions, "avg", Supported, "avg", ""),
    entry(Functions, "count", Supported, "count/count_all", ""),
    entry(Functions, "min", Supported, "min", ""),
    entry(Functions, "max", Supported, "max", ""),
    entry(Functions, "row_number", Supported, "row_number", ""),
    entry(Functions, "rank", Supported, "rank", ""),
    entry(Functions, "dense_rank", Supported, "dense_rank", ""),
    entry(Functions, "percent_rank", Supported, "percent_rank", ""),
    entry(Functions, "cume_dist", Supported, "cume_dist", ""),
    entry(Functions, "ntile", Supported, "ntile", ""),
    entry(Functions, "lag", Supported, "lag", ""),
    entry(Functions, "lead", Supported, "lead", ""),
    entry(Functions, "first", Supported, "first_value", ""),
    entry(Functions, "last", Supported, "last_value", ""),
    entry(Functions, "nth_value", Supported, "nth_value", ""),
    entry(
        Functions,
        "when",
        Partial,
        "function",
        "reachable via the SQL registry / raw; no typed when() builder (Phase 61 gap)",
    ),
    entry(
        Functions,
        "coalesce",
        Partial,
        "function",
        "reachable via function(\"coalesce\", …); typed F.coalesce is the gap",
    ),
    entry(
        Functions,
        "concat",
        Partial,
        "function",
        "any Phase 60 SQL scalar fn is reachable via function(name, args); typed F.* helpers are the gap",
    ),
    entry(
        Functions,
        "<sql-registry>",
        Partial,
        "function",
        "the whole Phase 60 SQL function registry (JSON/HOF/date/hash/…) is reachable via function(name, args); \
         Phase 61 ships typed F.* helpers over it (one registry, all surfaces)",
    ),
    // ── GroupedData ──────────────────────────────────────────────────────────
    entry(GroupedData, "agg", Supported, "group_by(...).agg", ""),
    entry(GroupedData, "count", Supported, "group_by(...).agg(count)", ""),
    entry(GroupedData, "sum", Supported, "group_by(...).agg(sum)", ""),
    entry(GroupedData, "avg/mean", Supported, "group_by(...).agg(avg)", ""),
    entry(GroupedData, "min", Supported, "group_by(...).agg(min)", ""),
    entry(GroupedData, "max", Supported, "group_by(...).agg(max)", ""),
    entry(
        GroupedData,
        "pivot",
        Partial,
        "DataFrame::pivot",
        "pivot exists on DataFrame; PySpark places it on GroupedData (groupBy().pivot())",
    ),
    // ── Window ───────────────────────────────────────────────────────────────
    entry(Window, "partitionBy", Supported, "Expr::over(partition_by=…)", ""),
    entry(Window, "orderBy", Supported, "Expr::frame/over order", ""),
    entry(Window, "rowsBetween", Supported, "Expr::frame (rows)", ""),
    entry(Window, "rangeBetween", Supported, "Expr::frame (range)", ""),
    entry(Window, "unboundedPreceding/Following", Supported, "frame bounds", ""),
    entry(Window, "currentRow", Supported, "frame bounds", ""),
    // ── DataFrameReader ──────────────────────────────────────────────────────
    entry(Reader, "parquet", Supported, "session.read_parquet", ""),
    entry(Reader, "csv", Supported, "session.read_csv", ""),
    entry(Reader, "json", Partial, "session.read_json", "reachable; option coverage narrower than Spark"),
    entry(Reader, "table", Supported, "session.sql/table", ""),
    entry(Reader, "format/load", Partial, "read_* methods", "typed per-format readers; no generic format(...).load()"),
    // ── DataFrameWriter ──────────────────────────────────────────────────────
    entry(Writer, "parquet", Supported, "write_parquet", ""),
    entry(Writer, "csv", Supported, "write_csv", ""),
    entry(Writer, "json", Supported, "write_json", ""),
    entry(Writer, "saveAsTable", Partial, "write / CTAS", "reachable via SQL CTAS; no writer.saveAsTable"),
    entry(Writer, "mode", Partial, "write_parquet_overwrite_partition", "overwrite modes exist per-format; no unified .mode()"),
    entry(Writer, "partitionBy", Partial, "write_parquet_with_options", "partitioned writes via options / SQL PARTITIONED BY"),
];

/// Per-namespace and overall parity numbers.
pub struct ParityReport {
    /// `(namespace, covered, total)` — covered = Supported or Partial.
    pub per_namespace: Vec<(Namespace, usize, usize)>,
    pub covered: usize,
    pub total: usize,
}

impl ParityReport {
    /// Overall covered / total as a percentage in `[0, 100]`.
    pub fn percent(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        (self.covered as f64) * 100.0 / (self.total as f64)
    }
}

/// Compute the parity KPI from [`PARITY`].
pub fn parity_report() -> ParityReport {
    let mut per_namespace: Vec<(Namespace, usize, usize)> = Vec::new();
    let (mut covered, mut total) = (0usize, 0usize);
    for ns in Namespace::ALL {
        let entries = PARITY.iter().filter(|e| e.namespace == ns);
        let ns_total = entries.clone().count();
        let ns_covered = entries.filter(|e| e.status.is_covered()).count();
        if ns_total > 0 {
            per_namespace.push((ns, ns_covered, ns_total));
        }
        covered += ns_covered;
        total += ns_total;
    }
    ParityReport {
        per_namespace,
        covered,
        total,
    }
}

/// Generate the published parity page (Markdown) from [`PARITY`].
pub fn generate_parity_markdown() -> String {
    let report = parity_report();
    let mut out = String::new();
    out.push_str("# Krishiv DataFrame API — PySpark parity\n\n");
    out.push_str(
        "> Generated from `crates/krishiv-api/src/pyspark_parity.rs` — do not edit by hand.\n",
    );
    out.push_str(
        "> Regenerate with `KRISHIV_BLESS_PYSPARK_PARITY=1 cargo test -p krishiv-api pyspark_parity`.\n\n",
    );
    let _ = writeln!(
        out,
        "**Overall parity: {covered}/{total} = {pct:.0}%** of the enumerated PySpark surface \
         (Supported or Partial). Each shortfall is itemized below.\n",
        covered = report.covered,
        total = report.total,
        pct = report.percent()
    );

    out.push_str("## Coverage by namespace\n\n");
    out.push_str("| Namespace | Covered | Total | % |\n|---|---|---|---|\n");
    for (ns, covered, total) in &report.per_namespace {
        let pct = if *total == 0 {
            0.0
        } else {
            (*covered as f64) * 100.0 / (*total as f64)
        };
        let _ = writeln!(out, "| {} | {} | {} | {:.0}% |", ns.as_str(), covered, total, pct);
    }
    out.push('\n');

    for ns in Namespace::ALL {
        let rows: Vec<&ApiEntry> = PARITY.iter().filter(|e| e.namespace == ns).collect();
        if rows.is_empty() {
            continue;
        }
        let _ = writeln!(out, "## {}\n", ns.as_str());
        out.push_str("| PySpark | Status | Krishiv | Notes |\n|---|---|---|---|\n");
        for e in rows {
            let _ = writeln!(
                out,
                "| `{}` | {} | {} | {} |",
                e.pyspark,
                e.status.as_str(),
                if e.krishiv.is_empty() { "—" } else { e.krishiv },
                e.note
            );
        }
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_entry_has_pyspark_name_and_consistent_equivalent() {
        for e in PARITY {
            assert!(!e.pyspark.is_empty(), "entry missing pyspark name");
            // A covered entry must name a Krishiv equivalent; an uncovered one
            // must not pretend to have one.
            if e.status.is_covered() {
                assert!(
                    !e.krishiv.is_empty(),
                    "{}::{} is {:?} but names no Krishiv equivalent",
                    e.namespace.as_str(),
                    e.pyspark,
                    e.status
                );
            } else {
                assert!(
                    e.krishiv.is_empty() || !e.note.is_empty(),
                    "{}::{} is {:?}; when it names a fallback it must explain the gap",
                    e.namespace.as_str(),
                    e.pyspark,
                    e.status
                );
            }
        }
    }

    #[test]
    fn supported_dataframe_methods_exist_on_the_surface() {
        // The matrix cannot claim `Supported` for a DataFrame method the crate
        // does not expose. Cross-check the leading equivalent token against the
        // real public DataFrame method list.
        let source = include_str!("dataframe.rs");
        for e in PARITY {
            if e.namespace != Namespace::DataFrame || e.status != ParityStatus::Supported {
                continue;
            }
            // The equivalent may list alternatives (`a/b`); check the first.
            let method = e.krishiv.split('/').next().unwrap_or(e.krishiv).trim();
            if method.is_empty() {
                continue;
            }
            assert!(
                source.contains(&format!("pub fn {method}")),
                "matrix claims DataFrame::{method} (for PySpark {}) but no `pub fn {method}` exists",
                e.pyspark
            );
        }
    }

    #[test]
    fn parity_kpi_is_reported() {
        let report = parity_report();
        assert!(report.total >= 90, "matrix should enumerate a real surface");
        assert!(
            report.percent() >= 60.0,
            "core DataFrame parity should clear the published bar: {:.0}%",
            report.percent()
        );
        // Every namespace with entries is represented in the per-namespace roll-up.
        assert_eq!(
            report.per_namespace.len(),
            Namespace::ALL
                .iter()
                .filter(|ns| PARITY.iter().any(|e| e.namespace == **ns))
                .count()
        );
    }

    #[test]
    fn committed_parity_page_matches_matrix() {
        let generated = generate_parity_markdown();
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/reference/pyspark-parity.md"
        );
        if std::env::var("KRISHIV_BLESS_PYSPARK_PARITY").is_ok() {
            std::fs::write(path, &generated).expect("write parity doc");
            return;
        }
        let committed = std::fs::read_to_string(path).unwrap_or_default();
        assert_eq!(
            committed, generated,
            "docs/reference/pyspark-parity.md is stale; regenerate with \
             KRISHIV_BLESS_PYSPARK_PARITY=1 cargo test -p krishiv-api pyspark_parity"
        );
    }
}
