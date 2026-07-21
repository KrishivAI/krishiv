#![forbid(unsafe_code)]
//! SQL grammar and **engine-dimensioned** feature matrix for Krishiv.
//!
//! Provides a machine-readable inventory of which SQL dialect features are
//! supported, and — crucially — *in which of the three execution engines*:
//! batch (DataFusion planner + Krishiv extensions), streaming (continuous
//! window compiler), and incremental (IVM / krishiv-delta). A single feature is
//! frequently "supported in batch, partial in streaming, n/a in incremental",
//! so each [`FeatureEntry`] carries a per-engine [`FeatureStatus`] rather than
//! one global status — otherwise "measured coverage" silently means *batch*
//! coverage (Phase 60).
//!
//! The public SQL reference page is **generated** from this matrix
//! ([`generate_reference_markdown`]), never hand-written, and a CI drift guard
//! (see `coverage.rs`) asserts the checked-in page matches and that every
//! non-`n/a` engine cell is backed by an executable coverage case.

/// Support status for a single SQL feature **in one engine**.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureStatus {
    /// Fully supported in the current release.
    Supported,
    /// Partially supported; the `note` field explains the gap.
    Partial,
    /// Planned for a future release.
    Planned,
    /// Not applicable to this engine.
    NotApplicable,
}

impl FeatureStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Partial => "partial",
            Self::Planned => "planned",
            Self::NotApplicable => "n/a",
        }
    }

    /// A non-`n/a`, non-`planned` cell — i.e. one that must be backed by an
    /// executable coverage case (the matrix-to-test CI rule).
    pub fn is_claimed(self) -> bool {
        matches!(self, Self::Supported | Self::Partial)
    }
}

impl std::fmt::Display for FeatureStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The three Krishiv execution engines a feature can be dimensioned across.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    Batch,
    Streaming,
    Incremental,
}

impl Engine {
    pub const ALL: [Engine; 3] = [Engine::Batch, Engine::Streaming, Engine::Incremental];

    pub fn as_str(self) -> &'static str {
        match self {
            Engine::Batch => "batch",
            Engine::Streaming => "streaming",
            Engine::Incremental => "incremental",
        }
    }
}

/// An execution surface a feature can be reached from. A `supported` engine
/// cell alone cannot say *where* the feature runs (audit §9b): several real,
/// tested streaming operators are reachable only from the embedded
/// `StreamingDataFrame`/process API, while the distributed `stream:loop`
/// runtime executes only `WindowExecutionSpec` shapes (windows, window-join,
/// CEP) and the SQL front door compiles a further subset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    /// The embedded Rust `StreamingDataFrame` / process API and its Python mirror.
    EmbeddedApi,
    /// The SQL front door (text SQL parse → plan → execute).
    Sql,
    /// The distributed runtime (`stream:loop` / partitioned batch fragments).
    Distributed,
}

impl Placement {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EmbeddedApi => "embedded API",
            Self::Sql => "SQL front door",
            Self::Distributed => "distributed runtime",
        }
    }
}

/// A single entry in the Krishiv SQL feature matrix, dimensioned per engine.
#[derive(Debug, Clone)]
pub struct FeatureEntry {
    /// Stable identifier (e.g. `"select.distinct"`).
    pub id: &'static str,
    /// Broad feature category (e.g. `"SELECT"`, `"JOIN"`, `"DML"`).
    pub category: &'static str,
    /// Human-readable description.
    pub description: &'static str,
    /// Support status in the batch engine.
    pub batch: FeatureStatus,
    /// Support status in the streaming (continuous) engine.
    pub streaming: FeatureStatus,
    /// Support status in the incremental (IVM) engine.
    pub incremental: FeatureStatus,
    /// Optional clarifying note (gap description, limitations, workarounds).
    pub note: Option<&'static str>,
    /// Where the feature actually executes. `None` means the feature is
    /// reachable from every surface its engine cells imply (the common case).
    /// `Some(..)` restricts the claim — e.g. embedded-API-only operators —
    /// and is rendered into the generated reference page.
    pub placement: Option<&'static [Placement]>,
}

impl FeatureEntry {
    const fn new(
        id: &'static str,
        category: &'static str,
        description: &'static str,
        batch: FeatureStatus,
        streaming: FeatureStatus,
        incremental: FeatureStatus,
    ) -> Self {
        Self {
            id,
            category,
            description,
            batch,
            streaming,
            incremental,
            note: None,
            placement: None,
        }
    }

    /// A batch-only feature: `n/a` in the streaming and incremental engines.
    const fn batch_only(
        id: &'static str,
        category: &'static str,
        description: &'static str,
        batch: FeatureStatus,
    ) -> Self {
        Self::new(id, category, description, batch, NA, NA)
    }

    const fn with_note(mut self, note: &'static str) -> Self {
        self.note = Some(note);
        self
    }

    /// Restrict where this feature executes (placement honesty, audit §9b).
    const fn placed(mut self, placement: &'static [Placement]) -> Self {
        self.placement = Some(placement);
        self
    }

    /// True when the entry's execution surface is restricted below what its
    /// engine cells imply (i.e. it carries an explicit placement set that
    /// excludes at least one surface).
    pub fn placement_restricted(&self) -> bool {
        matches!(self.placement, Some(p) if p.len() < 3)
    }

    /// Status for a given engine.
    pub fn status_for(&self, engine: Engine) -> FeatureStatus {
        match engine {
            Engine::Batch => self.batch,
            Engine::Streaming => self.streaming,
            Engine::Incremental => self.incremental,
        }
    }
}

impl std::fmt::Display for FeatureEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[batch:{} streaming:{} incremental:{}] {} — {}",
            self.batch, self.streaming, self.incremental, self.id, self.description
        )?;
        if let Some(note) = self.note {
            write!(f, " ({note})")?;
        }
        Ok(())
    }
}

// ── Feature matrix ────────────────────────────────────────────────────────────

/// Return the complete Krishiv SQL feature matrix.
pub fn feature_matrix() -> &'static [FeatureEntry] {
    FEATURES
}

/// Return only entries matching `category` (case-insensitive prefix match).
pub fn features_for_category(category: &str) -> Vec<&'static FeatureEntry> {
    let cat_upper = category.to_uppercase();
    FEATURES
        .iter()
        .filter(|e| e.category.to_uppercase().starts_with(&cat_upper))
        .collect()
}

/// Return entries whose **batch** status equals `status`. (Batch is the
/// primary column; use [`FeatureEntry::status_for`] for the other engines.)
pub fn features_by_status(status: FeatureStatus) -> Vec<&'static FeatureEntry> {
    FEATURES.iter().filter(|e| e.batch == status).collect()
}

/// Render the public "Krishiv SQL feature matrix" reference page from the
/// matrix. This is the single source of truth; the checked-in markdown is
/// regenerated from here and drift-guarded in CI.
pub fn generate_reference_markdown() -> String {
    let mut out = String::new();
    out.push_str("# Krishiv SQL feature matrix\n\n");
    out.push_str(
        "_Generated from `krishiv-sql/src/grammar.rs` — do not edit by hand._\n\n\
         Each feature is dimensioned across the three Krishiv execution engines: \
         **batch** (DataFusion + extensions), **streaming** (continuous windows), \
         and **incremental** (IVM). `n/a` means the feature does not apply to that \
         engine.\n\n",
    );

    // Stable category order = order of first appearance in the matrix.
    let mut categories: Vec<&'static str> = Vec::new();
    for e in FEATURES {
        if !categories.contains(&e.category) {
            categories.push(e.category);
        }
    }

    for cat in categories {
        out.push_str(&format!("## {cat}\n\n"));
        out.push_str("| Feature | Description | Batch | Streaming | Incremental | Notes |\n");
        out.push_str("|---|---|---|---|---|---|\n");
        for e in FEATURES.iter().filter(|e| e.category == cat) {
            let mut notes = String::new();
            if let Some(placement) = e.placement {
                let surfaces: Vec<&str> = placement.iter().map(|p| p.as_str()).collect();
                notes.push_str(&format!("**placement: {} only.** ", surfaces.join(" + ")));
            }
            notes.push_str(e.note.unwrap_or(""));
            out.push_str(&format!(
                "| `{}` | {} | {} | {} | {} | {} |\n",
                e.id,
                e.description,
                e.batch,
                e.streaming,
                e.incremental,
                notes.trim_end()
            ));
        }
        out.push('\n');
    }

    // Placement honesty (audit §9b): the embedded-only operator tier has no SQL
    // spellings, so it has no rows above — but a user who builds on these
    // operators embedded cannot run that job distributed, and this page must
    // say so rather than stay silent. (Operators that DO have a SQL matrix row,
    // like streaming.dedup, carry the placement marker on their row instead.)
    out.push_str("## Embedded-API-only streaming operators\n\n");
    out.push_str(
        "These operators are real and tested but reachable **only** from the embedded \
         Rust `StreamingDataFrame`/process API and its Python mirror — they are not \
         compiled from SQL, and the distributed `stream:loop` runtime executes only \
         `WindowExecutionSpec` shapes (windows, window-join, CEP). A job built on them \
         cannot run distributed today.\n\n",
    );
    for (name, desc) in EMBEDDED_ONLY_OPERATORS {
        out.push_str(&format!("- **{name}** — {desc}\n"));
    }
    out.push('\n');
    out
}

/// The embedded-API-only streaming operator tier (audit §9b). These have no
/// SQL spelling, so they carry no [`FeatureEntry`] row; they are published on
/// the generated reference page so the placement restriction is stated
/// somewhere a user will read it.
pub const EMBEDDED_ONLY_OPERATORS: &[(&str, &str)] = &[
    (
        "temporal join",
        "event-time temporal (versioned lookup) join between two streams",
    ),
    (
        "side outputs",
        "route late/rejected/tagged rows to a secondary stream",
    ),
    (
        "broadcast state",
        "low-volume control stream broadcast to all tasks of a keyed stream",
    ),
    (
        "connected streams",
        "two-input operators sharing state across both inputs",
    ),
    (
        "ProcessFunction + timers",
        "per-key user logic with registered event/processing-time timers",
    ),
];

/// Render the "Krishiv SQL vs Spark SQL" dialect honesty / migration page from
/// the matrix + the documented per-feature semantic differences. Generated,
/// never hand-written: what maps 1:1, what differs semantically, what is absent.
pub fn generate_honesty_markdown() -> String {
    let mut out = String::new();
    out.push_str("# Krishiv SQL vs Spark SQL\n\n");
    out.push_str(
        "_Generated from `krishiv-sql/src/grammar.rs` — do not edit by hand._\n\n\
         Krishiv targets Spark-SQL reference parity as a **measured** number. This page \
         is the honest ledger: what maps 1:1, what differs semantically, and what is \
         absent, derived from the feature matrix.\n\n",
    );

    // Documented semantic differences (the correctness-trap items an alias layer
    // must surface loudly rather than silently mis-behave on).
    out.push_str("## Documented semantic differences\n\n");
    out.push_str(
        "- **`date_format(ts, fmt)` pattern letters.** Krishiv uses **Spark/Java** \
         `DateTimeFormatter` letters (`yyyy-MM-dd`), not chrono/strftime (`%Y-%m-%d`). \
         Supported letters translate exactly; unsupported letters (era `G`, timezone \
         `z`/`X`) raise a clear error instead of emitting wrong output.\n\
         - **`exists(array, x -> …)`.** The `exists(` spelling is shadowed by the \
         EXISTS-subquery keyword in the parser; use `any_match(array, x -> …)` (the \
         byte-identical implementation) for the Spark higher-order `exists`.\n\
         - **Lambda / array-literal syntax.** The SQL front door parses with a \
         lambda-capable dialect so `transform(arr, x -> …)` and `[1, 2, 3]` work; \
         the array constructor is `make_array(...)` / `[...]` (Spark's `array(...)` \
         maps to these).\n\
         - **ANSI mode, integral division, NULL ordering** follow DataFusion \
         semantics, which match Spark ANSI mode for the covered surface; divergences \
         are tracked as matrix notes.\n\n",
    );

    // 1:1 — supported in batch with no divergence note.
    out.push_str("## Maps 1:1 (supported, no semantic caveat)\n\n");
    for e in FEATURES
        .iter()
        .filter(|e| e.batch == FeatureStatus::Supported && e.note.is_none())
    {
        out.push_str(&format!("- `{}` — {}\n", e.id, e.description));
    }
    out.push('\n');

    // Partial / caveated.
    out.push_str("## Supported with caveats (partial or noted)\n\n");
    for e in FEATURES.iter().filter(|e| {
        (e.batch == FeatureStatus::Partial) || (e.batch.is_claimed() && e.note.is_some())
    }) {
        out.push_str(&format!(
            "- `{}` — {} _({})_\n",
            e.id,
            e.description,
            e.note.unwrap_or("partial")
        ));
    }
    out.push('\n');

    // Absent / planned.
    out.push_str("## Absent (planned — itemized shortfall)\n\n");
    for e in FEATURES.iter().filter(|e| {
        e.batch == FeatureStatus::Planned
            && e.streaming != FeatureStatus::Supported
            && e.incremental != FeatureStatus::Supported
    }) {
        out.push_str(&format!(
            "- `{}` — {} _({})_\n",
            e.id,
            e.description,
            e.note.unwrap_or("planned")
        ));
    }
    out.push('\n');
    out
}

const S: FeatureStatus = FeatureStatus::Supported;
const P: FeatureStatus = FeatureStatus::Partial;
const PL: FeatureStatus = FeatureStatus::Planned;
const NA: FeatureStatus = FeatureStatus::NotApplicable;

static FEATURES: &[FeatureEntry] = &[
    // ── SELECT ────────────────────────────────────────────────────────────────
    // The shared relational core: fully supported in batch; in streaming it is
    // usable only inside the windowed continuous plan (Partial); IVM maintains
    // it via krishiv-delta with a DiffBased recompute fallback (Partial).
    FeatureEntry::new("select.projection", "SELECT", "Column projection and aliases", S, P, P),
    FeatureEntry::new("select.star", "SELECT", "SELECT * expansion", S, P, P),
    FeatureEntry::new("select.distinct", "SELECT", "SELECT DISTINCT deduplication", S, NA, P),
    FeatureEntry::new("select.where", "SELECT", "WHERE predicate filtering", S, P, P),
    FeatureEntry::new(
        "select.order_by",
        "SELECT",
        "ORDER BY with ASC/DESC and NULLS FIRST/LAST",
        S,
        NA,
        NA,
    )
    .with_note("streaming/IVM: unbounded ordering is not a maintainable operator"),
    FeatureEntry::new("select.limit_offset", "SELECT", "LIMIT / OFFSET pagination", S, NA, NA),
    FeatureEntry::new("select.having", "SELECT", "HAVING post-aggregation filter", S, P, P),
    FeatureEntry::new(
        "select.case",
        "SELECT",
        "CASE WHEN … THEN … ELSE … END expressions",
        S,
        P,
        P,
    ),
    FeatureEntry::new("select.cast", "SELECT", "CAST(expr AS type) and TRY_CAST", S, P, P),
    FeatureEntry::new(
        "select.subquery_scalar",
        "SELECT",
        "Scalar subqueries in projection/predicate",
        S,
        NA,
        P,
    ),
    FeatureEntry::new(
        "select.subquery_exists",
        "SELECT",
        "EXISTS / NOT EXISTS correlated subqueries",
        S,
        NA,
        P,
    ),
    FeatureEntry::new("select.subquery_in", "SELECT", "IN / NOT IN subqueries", S, NA, P),
    FeatureEntry::new("select.values", "SELECT", "VALUES clause for inline data", S, NA, P),
    // ── GROUP BY ─────────────────────────────────────────────────────────────
    FeatureEntry::new("groupby.basic", "GROUP BY", "Basic GROUP BY column list", S, P, P)
        .with_note("streaming: only inside a window TVF (windowed aggregation)"),
    FeatureEntry::new("groupby.rollup", "GROUP BY", "ROLLUP grouping sets", S, NA, P),
    FeatureEntry::new("groupby.cube", "GROUP BY", "CUBE grouping sets", S, NA, P),
    FeatureEntry::new("groupby.grouping_sets", "GROUP BY", "Explicit GROUPING SETS", S, NA, P),
    FeatureEntry::new(
        "groupby.grouping_function",
        "GROUP BY",
        "GROUPING() function for NULL disambiguation",
        S,
        NA,
        P,
    ),
    // ── JOIN ─────────────────────────────────────────────────────────────────
    FeatureEntry::new("join.inner", "JOIN", "INNER JOIN (equi and non-equi)", S, NA, P),
    FeatureEntry::new("join.left_outer", "JOIN", "LEFT OUTER JOIN", S, NA, P),
    FeatureEntry::new("join.right_outer", "JOIN", "RIGHT OUTER JOIN", S, NA, P),
    FeatureEntry::new("join.full_outer", "JOIN", "FULL OUTER JOIN", S, NA, P),
    FeatureEntry::new("join.cross", "JOIN", "CROSS JOIN", S, NA, P),
    FeatureEntry::new("join.natural", "JOIN", "NATURAL JOIN (column-name matching)", S, NA, P),
    FeatureEntry::new("join.using", "JOIN", "JOIN … USING (column_list)", S, NA, P),
    FeatureEntry::new("join.lateral", "JOIN", "LATERAL JOIN / CROSS JOIN LATERAL", S, NA, NA),
    FeatureEntry::new(
        "join.interval",
        "JOIN",
        "Streaming interval join on event-time bounds",
        PL,
        P,
        NA,
    )
    .with_note(
        "DataFrame-only today (audit §9b): the interval-join operator has no SQL planning path, \
         so batch SQL cannot express it (Planned); the streaming operator exists (Partial). \
         Corrected from the prior over-claim of batch Supported.",
    ),
    FeatureEntry::new(
        "join.temporal_as_of",
        "JOIN",
        "Temporal AS OF point-in-time join",
        PL,
        NA,
        NA,
    )
    .with_note(
        "no SQL temporal-join planning path: `lakehouse/as_of.rs` is table time-travel \
         (temporal.as_of), not a temporal join. Marked Planned rather than the prior Supported.",
    ),
    FeatureEntry::new(
        "join.broadcast_hint",
        "JOIN",
        "/*+ BROADCAST(t) */ optimizer hint",
        P,
        NA,
        NA,
    )
    .with_note("hint parsed and recorded; broadcast decision is cost-based (see hints.* entries)"),
    // ── HINTS (Phase 60 statement completion) ───────────────────────────────
    FeatureEntry::new(
        "hints.join_strategy",
        "HINTS",
        "/*+ MERGE|SHUFFLE_HASH|BROADCAST(t) */ join-strategy hints",
        P,
        NA,
        NA,
    )
    .with_note("parsed always; honored where the executor supports the strategy (Phase 52/54), recorded either way"),
    FeatureEntry::new(
        "hints.repartition",
        "HINTS",
        "/*+ REPARTITION(n)|COALESCE(n) */ partitioning hints",
        P,
        NA,
        NA,
    )
    .with_note("parsed and recorded; applied where the distributed planner supports it"),
    // ── WINDOW FUNCTIONS ─────────────────────────────────────────────────────
    FeatureEntry::new("window.over", "WINDOW", "OVER () window function clauses", S, NA, NA),
    FeatureEntry::new("window.partition_by", "WINDOW", "PARTITION BY inside OVER", S, NA, NA),
    FeatureEntry::new("window.order_by", "WINDOW", "ORDER BY inside OVER", S, NA, NA),
    FeatureEntry::new("window.rows_range", "WINDOW", "ROWS / RANGE frame specification", S, NA, NA),
    FeatureEntry::new(
        "window.rank_dense_rank",
        "WINDOW",
        "RANK(), DENSE_RANK(), ROW_NUMBER()",
        S,
        NA,
        NA,
    ),
    FeatureEntry::new("window.lead_lag", "WINDOW", "LEAD() and LAG()", S, NA, NA),
    FeatureEntry::new(
        "window.first_last_value",
        "WINDOW",
        "FIRST_VALUE() and LAST_VALUE()",
        S,
        NA,
        NA,
    ),
    FeatureEntry::new("window.nth_value", "WINDOW", "NTH_VALUE()", S, NA, NA),
    FeatureEntry::new("window.ntile", "WINDOW", "NTILE(n)", S, NA, NA),
    FeatureEntry::new(
        "window.cume_dist_percent",
        "WINDOW",
        "CUME_DIST() and PERCENT_RANK()",
        S,
        NA,
        NA,
    ),
    FeatureEntry::new(
        "window.tumble",
        "WINDOW",
        "TUMBLE(col, interval) streaming window",
        S,
        S,
        P,
    )
    .with_note("batch rewrites the TVF to scalar UDFs (streaming_tvf.rs); streaming compiles it natively"),
    FeatureEntry::new("window.hop", "WINDOW", "HOP(col, slide, size) sliding window", S, S, P),
    FeatureEntry::new("window.session", "WINDOW", "Session window on inactivity gap", S, S, NA),
    // ── CTE ──────────────────────────────────────────────────────────────────
    FeatureEntry::new("cte.non_recursive", "CTE", "WITH … AS (…) non-recursive CTEs", S, P, P),
    FeatureEntry::new(
        "cte.recursive",
        "CTE",
        "WITH RECURSIVE … (UNION ALL base + recursive)",
        S,
        NA,
        NA,
    ),
    FeatureEntry::new("cte.multiple", "CTE", "Multiple CTEs in one WITH clause", S, P, P),
    // ── SET OPERATIONS ────────────────────────────────────────────────────────
    FeatureEntry::new("set.union_all", "SET", "UNION ALL", S, P, P),
    FeatureEntry::new("set.union_distinct", "SET", "UNION (DISTINCT)", S, NA, P),
    FeatureEntry::new("set.intersect", "SET", "INTERSECT", S, NA, P),
    FeatureEntry::new("set.except", "SET", "EXCEPT", S, NA, P),
    // ── LATERAL / UNNEST ─────────────────────────────────────────────────────
    FeatureEntry::batch_only("lateral.unnest", "LATERAL", "UNNEST(array_col) in FROM clause", S),
    FeatureEntry::batch_only(
        "lateral.generate_series",
        "LATERAL",
        "generate_series() table function",
        S,
    ),
    FeatureEntry::batch_only(
        "lateral.cross_join_unnest",
        "LATERAL",
        "CROSS JOIN UNNEST(…) AS t(col)",
        S,
    ),
    // ── PIVOT / UNPIVOT ───────────────────────────────────────────────────────
    FeatureEntry::batch_only("pivot.pivot", "PIVOT", "PIVOT(agg FOR col IN (v1, v2, …))", S),
    FeatureEntry::batch_only("pivot.unpivot", "PIVOT", "UNPIVOT(value FOR col IN (c1, c2, …))", S),
    // ── FUNCTIONS: JSON (Phase 60) ───────────────────────────────────────────
    FeatureEntry::batch_only(
        "functions.json.get_json_object",
        "FUNCTIONS",
        "get_json_object(json, path) Spark JSONPath extraction",
        S,
    ),
    FeatureEntry::batch_only(
        "functions.json.json_array_length",
        "FUNCTIONS",
        "json_array_length(json) top-level array element count",
        S,
    ),
    FeatureEntry::batch_only(
        "functions.json.from_to_json",
        "FUNCTIONS",
        "from_json / to_json struct⇄JSON conversion",
        PL,
    )
    .with_note(
        "requires a typed arrow⇄JSON converter + a Spark-DDL schema parser with Spark's \
         version-specific null-field/timestamp rules; itemized shortfall, not shipped approximate",
    ),
    FeatureEntry::batch_only(
        "functions.json.json_tuple",
        "FUNCTIONS",
        "json_tuple(json, k1, k2, …) multi-key extraction (generator)",
        PL,
    )
    .with_note("needs table-generating/LATERAL VIEW machinery; use get_json_object per key today"),
    FeatureEntry::batch_only(
        "functions.json.schema_of_json",
        "FUNCTIONS",
        "schema_of_json(json) infer a DDL schema string",
        PL,
    ),
    // ── FUNCTIONS: higher-order array lambdas (Phase 60) ─────────────────────
    FeatureEntry::batch_only(
        "functions.hof.transform",
        "FUNCTIONS",
        "transform(array, x -> …) — Spark alias for array_transform",
        S,
    ),
    FeatureEntry::batch_only(
        "functions.hof.filter",
        "FUNCTIONS",
        "filter(array, x -> …) — Spark alias for array_filter",
        S,
    ),
    FeatureEntry::batch_only(
        "functions.hof.exists",
        "FUNCTIONS",
        "exists / any_match(array, x -> …) predicate-any",
        P,
    )
    .with_note(
        "any_match is reachable; the `exists(...)` spelling is shadowed by the EXISTS-subquery \
         keyword in the parser (documented dialect difference)",
    ),
    FeatureEntry::batch_only(
        "functions.hof.forall",
        "FUNCTIONS",
        "forall(array, x -> …) predicate-all (new, exact all-match)",
        S,
    ),
    FeatureEntry::batch_only(
        "functions.hof.aggregate_zip_map",
        "FUNCTIONS",
        "aggregate/reduce, zip_with, map_filter, transform_keys/values",
        PL,
    )
    .with_note("require DataFusion's multi-step lambda / map-lambda protocol; itemized shortfall"),
    // ── FUNCTIONS: Spark scalar alias layer (Phase 60) ───────────────────────
    FeatureEntry::batch_only(
        "functions.spark.nvl",
        "FUNCTIONS",
        "nvl / nvl2 null-coalescing (DataFusion-native, exact)",
        S,
    ),
    FeatureEntry::batch_only(
        "functions.spark.substring_index",
        "FUNCTIONS",
        "substring_index(str, delim, count) (DataFusion-native, exact)",
        S,
    ),
    FeatureEntry::batch_only(
        "functions.spark.date_format",
        "FUNCTIONS",
        "date_format(ts, fmt) with **Spark** pattern letters (yyyy-MM-dd)",
        S,
    )
    .with_note(
        "supported Spark pattern letters translate exactly to chrono; unsupported letters \
         (era/timezone) error clearly rather than emitting wrong output. Differs from \
         DataFusion's chrono-pattern date_format — see honesty page.",
    ),
    FeatureEntry::batch_only(
        "functions.spark.crc32",
        "FUNCTIONS",
        "crc32(expr) IEEE CRC-32 as BIGINT (exact)",
        S,
    ),
    FeatureEntry::batch_only(
        "functions.spark.hash_generators",
        "FUNCTIONS",
        "xxhash64, stack, posexplode, inline",
        PL,
    )
    .with_note(
        "xxhash64 needs byte-exact replication of Spark's seed-42 typed hashing; \
         stack/posexplode/inline need generator machinery — itemized shortfall",
    ),
    // ── DML ──────────────────────────────────────────────────────────────────
    FeatureEntry::batch_only("dml.copy_to", "DML", "COPY (query) TO 'path' (FORMAT …)", S)
        .with_note("inherited from DataFusion's native parser/planner; no Krishiv-side code involved"),
    FeatureEntry::new("dml.insert_into", "DML", "INSERT INTO table SELECT …", S, NA, NA),
    FeatureEntry::batch_only(
        "dml.insert_overwrite",
        "DML",
        "INSERT OVERWRITE (full partition replace)",
        S,
    ),
    FeatureEntry::batch_only("dml.delete", "DML", "DELETE FROM table WHERE …", P)
        .with_note("supported on Iceberg tables; in-memory and Parquet tables require rewrite"),
    FeatureEntry::batch_only("dml.update", "DML", "UPDATE table SET col = … WHERE …", P)
        .with_note("supported on Iceberg tables via MERGE rewrite"),
    FeatureEntry::batch_only(
        "dml.merge",
        "DML",
        "MERGE INTO target USING source ON … WHEN MATCHED …",
        S,
    ),
    FeatureEntry::batch_only(
        "dml.iceberg_merge",
        "DML",
        "Atomic Iceberg MERGE with row-level deletes",
        S,
    ),
    FeatureEntry::batch_only("dml.truncate", "DML", "TRUNCATE TABLE (Iceberg + memory)", PL)
        .with_note("itemized shortfall: TRUNCATE is not yet wired for memory/Iceberg session tables"),
    // ── DDL ──────────────────────────────────────────────────────────────────
    FeatureEntry::batch_only(
        "ddl.create_external_table",
        "DDL",
        "CREATE EXTERNAL TABLE … STORED AS …",
        S,
    ),
    FeatureEntry::batch_only("ddl.create_view", "DDL", "CREATE VIEW name AS SELECT …", S),
    FeatureEntry::batch_only(
        "ddl.create_function",
        "DDL",
        "CREATE FUNCTION … LANGUAGE SQL|PYTHON",
        S,
    ),
    FeatureEntry::batch_only("ddl.drop_table", "DDL", "DROP TABLE [IF EXISTS]", S),
    FeatureEntry::batch_only("ddl.drop_view", "DDL", "DROP VIEW [IF EXISTS]", S),
    FeatureEntry::batch_only("ddl.create_table_as", "DDL", "CREATE TABLE … AS SELECT (CTAS)", S)
        .with_note("durable Iceberg landing (G17) when the target resolves to a registered Iceberg catalog; session table otherwise"),
    FeatureEntry::batch_only(
        "ddl.partitioned_by",
        "DDL",
        "CREATE TABLE … PARTITIONED BY (col | bucket/truncate/year/month/day/hour(col)) AS SELECT",
        S,
    )
    .with_note("Iceberg catalog tables only; transforms follow the Iceberg partition spec"),
    FeatureEntry::batch_only("ddl.alter_table", "DDL", "ALTER TABLE ADD/DROP COLUMN, RENAME", P)
        .with_note("Iceberg schema evolution via ALTER TABLE is supported"),
    FeatureEntry::batch_only("ddl.create_schema", "DDL", "CREATE SCHEMA name", S)
        .with_note("inherited from DataFusion's native catalog; no Krishiv-side code involved"),
    FeatureEntry::new(
        "ddl.create_materialized_view",
        "DDL",
        "CREATE [OR REPLACE] MATERIALIZED VIEW … AS SELECT → IVM view (REFRESH/DROP)",
        NA,
        NA,
        S,
    )
    .with_note(
        "Phase 60 SQL-DDL-for-IVM: ANSI/Spark synonym routed onto the same IVM engine as \
         CREATE MATERIALIZED INCREMENTAL VIEW; REFRESH/DROP MATERIALIZED VIEW lifecycle; \
         engine primitive under the platform's governed pipelines",
    ),
    FeatureEntry::new(
        "ddl.create_streaming_table",
        "DDL",
        "CREATE [OR REPLACE] STREAMING TABLE … AS SELECT → continuous job",
        NA,
        PL,
        NA,
    )
    .with_note(
        "Phase 60: SQL front door + planner validation land (the body lowers through the shared \
         streaming compiler); continuous-job execution is coordinator-gated — a cluster-attached \
         session submits the validated plan via the continuous-stream registration API",
    ),
    FeatureEntry::batch_only(
        "ddl.live_table",
        "DDL",
        "CREATE / REFRESH / DROP LIVE TABLE via session.sql()",
        S,
    ),
    // ── CONNECTOR DDL (Phase 60) ─────────────────────────────────────────────
    FeatureEntry::batch_only(
        "ddl.connector_source_sink",
        "DDL",
        "CREATE SOURCE/SINK … WITH (connector=…) resolved through the connector registry",
        P,
    )
    .with_note(
        "registry-backed dispatch replacing the parquet-only hardcoded factory (audit §8b); \
         supported kinds come from connector descriptors, unsupported kinds fail loudly",
    ),
    // ── SESSION / CONFIG STATEMENTS (Phase 60) ───────────────────────────────
    FeatureEntry::batch_only("stmt.set_reset", "SESSION", "SET / RESET / SET TIMEZONE session config", S)
        .with_note("DataFusion-native session config"),
    FeatureEntry::batch_only("stmt.use", "SESSION", "USE [CATALOG|SCHEMA] current-namespace", S)
        .with_note("Phase 60: mutates the session default catalog/schema"),
    FeatureEntry::batch_only(
        "stmt.cache",
        "SESSION",
        "CACHE / UNCACHE / CLEAR CACHE TABLE (session materialization)",
        PL,
    )
    .with_note("itemized shortfall: needs a session-scoped materialization + provider swap/restore"),
    // ── SHOW / DESCRIBE (Phase 60) ───────────────────────────────────────────
    FeatureEntry::batch_only(
        "show.tables_databases_functions",
        "SHOW",
        "SHOW TABLES | DATABASES | SCHEMAS | FUNCTIONS | COLUMNS",
        P,
    )
    .with_note(
        "TABLES/FUNCTIONS/COLUMNS are DataFusion-native; DATABASES/SCHEMAS added in Phase 60 \
         (information_schema.schemata). SHOW PARTITIONS (Iceberg) and SHOW VIEWS remain the gap.",
    ),
    FeatureEntry::batch_only(
        "describe.function_database_query",
        "DESCRIBE",
        "DESCRIBE FUNCTION | DATABASE | QUERY",
        PL,
    )
    .with_note("DESCRIBE <table> is native; FUNCTION/DATABASE/QUERY are the itemized shortfall"),
    // ── TEMPORAL ─────────────────────────────────────────────────────────────
    FeatureEntry::batch_only("temporal.as_of", "TEMPORAL", "AS OF TIMESTAMP point-in-time queries", S),
    FeatureEntry::new(
        "temporal.match_recognize",
        "TEMPORAL",
        "MATCH_RECOGNIZE pattern matching over ordered rows",
        P,
        P,
        NA,
    )
    .with_note(
        "streaming CEP subset: PARTITION BY / ORDER BY / PATTERN (…) / WITHIN <duration>; \
         DEFINE (pattern-variable predicates) and MEASURES (computed output) clauses are the \
         remaining gap vs Oracle/Flink's full grammar",
    ),
    FeatureEntry::batch_only(
        "temporal.system_time",
        "TEMPORAL",
        "FOR SYSTEM_TIME AS OF (Iceberg time-travel)",
        P,
    )
    .with_note("alias for AS OF on Iceberg tables"),
    // ── PREPARED STATEMENTS ───────────────────────────────────────────────────
    FeatureEntry::batch_only(
        "prepared.create",
        "PREPARED",
        "CREATE PREPARED STATEMENT via Flight SQL action",
        S,
    ),
    FeatureEntry::batch_only("prepared.execute", "PREPARED", "Execute prepared statement by handle", S),
    FeatureEntry::batch_only(
        "prepared.close",
        "PREPARED",
        "CLOSE PREPARED STATEMENT to release server memory",
        S,
    ),
    FeatureEntry::batch_only(
        "prepared.parameters",
        "PREPARED",
        "Positional parameter binding ($1, $2, …)",
        S,
    )
    .with_note("local PreparedStatement::bind and Flight SQL DoPut parameter batches"),
    FeatureEntry::batch_only(
        "prepared.sql_text",
        "PREPARED",
        "PREPARE name AS …; EXECUTE name(…); DEALLOCATE name",
        S,
    )
    .with_note("inherited from DataFusion's native parser/planner (session-scoped named plans)"),
    // ── OPERATION CONTROL ────────────────────────────────────────────────────
    FeatureEntry::new("operation.id", "OPERATION", "Operation IDs for query tracking", S, S, S),
    FeatureEntry::new("operation.cancel", "OPERATION", "Cancel a running operation by ID", S, S, S),
    FeatureEntry::new("operation.timeout", "OPERATION", "Per-query execution timeout", S, NA, NA),
    FeatureEntry::new(
        "operation.progress",
        "OPERATION",
        "Query progress reporting via QueryHandle",
        S,
        S,
        S,
    ),
    // ── ERROR HANDLING ────────────────────────────────────────────────────────
    FeatureEntry::batch_only("error.sqlstate", "ERROR", "SQLSTATE codes on error responses", S),
    FeatureEntry::batch_only("error.error_position", "ERROR", "Source line/column in error messages", P)
        .with_note("DataFusion provides message but not structured position"),
    // ── FLIGHT SQL ────────────────────────────────────────────────────────────
    FeatureEntry::batch_only(
        "flight.get_flight_info",
        "FLIGHT SQL",
        "GetFlightInfo for statement execution",
        S,
    ),
    FeatureEntry::batch_only("flight.do_get", "FLIGHT SQL", "DoGet streaming result delivery", S),
    FeatureEntry::batch_only(
        "flight.prepared_statements",
        "FLIGHT SQL",
        "Prepared statement create/execute/close",
        S,
    ),
    FeatureEntry::batch_only(
        "flight.do_action",
        "FLIGHT SQL",
        "DoAction for custom Krishiv operations",
        S,
    ),
    FeatureEntry::batch_only(
        "flight.get_sql_info",
        "FLIGHT SQL",
        "GetSqlInfo capability introspection",
        S,
    ),
    FeatureEntry::batch_only("flight.auth", "FLIGHT SQL", "Bearer token authentication", S),
    FeatureEntry::batch_only("flight.policy", "FLIGHT SQL", "Table-level access policy enforcement", S),
    FeatureEntry::batch_only(
        "flight.transactions",
        "FLIGHT SQL",
        "BEGIN/COMMIT/ROLLBACK transactions",
        P,
    )
    .with_note("Flight SQL BeginTransaction/EndTransaction actions; SQL BEGIN/COMMIT not routed"),
    FeatureEntry::batch_only(
        "flight.schemas",
        "FLIGHT SQL",
        "GetDbSchemas / GetTables catalog introspection",
        P,
    )
    .with_note("tables listed via Krishiv catalog; schema introspection via get_sql_info"),
    // ── STREAMING SQL ─────────────────────────────────────────────────────────
    FeatureEntry::new(
        "streaming.continuous_select",
        "STREAMING",
        "Continuous SELECT over unbounded input",
        NA,
        S,
        NA,
    ),
    FeatureEntry::new(
        "streaming.window_agg",
        "STREAMING",
        "Windowed aggregations over streaming input",
        NA,
        S,
        P,
    ),
    FeatureEntry::new(
        "streaming.watermark",
        "STREAMING",
        "Event-time watermarks for late-data handling",
        NA,
        S,
        NA,
    ),
    FeatureEntry::new(
        "streaming.interval_join",
        "STREAMING",
        "Streaming-to-streaming interval join",
        NA,
        S,
        NA,
    )
    .placed(&[Placement::EmbeddedApi, Placement::Distributed])
    .with_note(
        "no SQL planning path; embedded PerKeyIntervalJoin, and distributed only as the \
         watermark window-join WindowExecutionSpec shape",
    ),
    FeatureEntry::new("streaming.cep", "STREAMING", "MATCH_RECOGNIZE CEP over streaming input", NA, S, NA),
    FeatureEntry::new(
        "streaming.dedup",
        "STREAMING",
        "Streaming deduplication (dropDuplicates)",
        NA,
        S,
        NA,
    )
    .placed(&[Placement::EmbeddedApi])
    .with_note(
        "embedded API only — not compiled from SQL and not a distributed \
         stream:loop shape (audit §9b)",
    ),
    FeatureEntry::new(
        "streaming.sink_modes",
        "STREAMING",
        "Append / Update / Complete output modes",
        NA,
        S,
        P,
    ),
    // ── INTROSPECTION ─────────────────────────────────────────────────────────
    FeatureEntry::batch_only(
        "introspection.describe",
        "INTROSPECTION",
        "DESCRIBE / DESC / SHOW COLUMNS table schema",
        S,
    ),
    FeatureEntry::batch_only(
        "introspection.explain",
        "INTROSPECTION",
        "EXPLAIN [LOGICAL|PHYSICAL|ANALYZE] query plans",
        S,
    ),
    FeatureEntry::batch_only(
        "introspection.information_schema",
        "INTROSPECTION",
        "information_schema.{tables,columns,views,df_settings,routines,parameters,schemata}",
        S,
    ),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_matrix_is_non_empty() {
        assert!(!feature_matrix().is_empty());
    }

    #[test]
    fn all_ids_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for e in feature_matrix() {
            assert!(seen.insert(e.id), "duplicate feature id: {}", e.id);
        }
    }

    #[test]
    fn every_entry_has_at_least_one_non_na_engine() {
        // A row that is n/a in all three engines is meaningless.
        for e in feature_matrix() {
            assert!(
                e.batch != NA || e.streaming != NA || e.incremental != NA,
                "feature {} is n/a in every engine",
                e.id
            );
        }
    }

    #[test]
    fn features_for_category_returns_subset() {
        let join_features = features_for_category("JOIN");
        assert!(!join_features.is_empty());
        for f in &join_features {
            assert!(f.category.to_uppercase().starts_with("JOIN"), "{}", f.id);
        }
    }

    #[test]
    fn features_by_status_supported_is_non_empty() {
        assert!(!features_by_status(FeatureStatus::Supported).is_empty());
    }

    #[test]
    fn feature_entry_display_includes_id_and_engines() {
        let entry = feature_matrix()
            .iter()
            .find(|e| e.id == "window.tumble")
            .unwrap();
        let s = entry.to_string();
        assert!(s.contains("window.tumble"));
        assert!(s.contains("batch:supported"));
        assert!(s.contains("streaming:supported"));
    }

    #[test]
    fn generated_reference_has_all_three_engine_columns() {
        let md = generate_reference_markdown();
        assert!(md.contains("| Batch | Streaming | Incremental |"));
        // Spot-check a per-engine divergence is rendered.
        assert!(md.contains("`window.tumble`"));
        assert!(md.contains("`functions.hof.forall`"));
    }

    #[test]
    fn drift_check_ctas_is_supported_not_partial() {
        // Regression: CTAS was marked Partial after G17 shipped durable Iceberg CTAS.
        let ctas = feature_matrix()
            .iter()
            .find(|e| e.id == "ddl.create_table_as")
            .unwrap();
        assert_eq!(ctas.batch, FeatureStatus::Supported);
    }

    #[test]
    fn drift_check_interval_join_has_no_batch_sql_path() {
        // Regression: join.interval was over-claimed as batch Supported; it is
        // DataFrame-only with no SQL planning path (audit §9b).
        let ij = feature_matrix()
            .iter()
            .find(|e| e.id == "join.interval")
            .unwrap();
        assert_eq!(ij.batch, FeatureStatus::Planned);
        assert_eq!(ij.streaming, FeatureStatus::Partial);
    }

    #[test]
    fn embedded_only_operators_carry_the_placement_marker() {
        // Regression (audit §9b): "supported" must say WHERE. dedup is
        // embedded-API-only; interval join has no SQL planning path and is
        // distributed only as the watermark window-join shape.
        let dedup = feature_matrix()
            .iter()
            .find(|e| e.id == "streaming.dedup")
            .unwrap();
        assert_eq!(dedup.placement, Some(&[Placement::EmbeddedApi][..]));
        assert!(dedup.placement_restricted());

        let ij = feature_matrix()
            .iter()
            .find(|e| e.id == "streaming.interval_join")
            .unwrap();
        let p = ij
            .placement
            .expect("streaming.interval_join must carry a placement set");
        assert!(p.contains(&Placement::EmbeddedApi) && p.contains(&Placement::Distributed));
        assert!(
            !p.contains(&Placement::Sql),
            "no SQL planning path exists for interval join"
        );
    }

    #[test]
    fn placement_restricted_entries_always_explain_themselves() {
        // A restricted placement without a note is a claim without a reason.
        for e in feature_matrix() {
            if e.placement_restricted() {
                assert!(
                    e.note.is_some(),
                    "feature {} restricts placement but has no note",
                    e.id
                );
            }
        }
    }

    #[test]
    fn generated_reference_renders_placement_and_embedded_only_ledger() {
        let md = generate_reference_markdown();
        assert!(md.contains("**placement: embedded API only.**"));
        assert!(md.contains("**placement: embedded API + distributed runtime only.**"));
        assert!(md.contains("## Embedded-API-only streaming operators"));
        for (name, _) in EMBEDDED_ONLY_OPERATORS {
            assert!(md.contains(name), "embedded-only ledger is missing {name}");
        }
    }
}
