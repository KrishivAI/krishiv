//! Late materialisation of a bounded top-N aggregate.
//!
//! A `GROUP BY` that lists a key **and the columns that key determines** carries
//! those columns through every join and every shuffle beneath it, only to
//! display a handful of them at the end. This rule groups on the key alone,
//! takes the top N, and re-fetches the wide columns for the survivors.
//!
//! # The query that motivated this, and the number that justifies it
//!
//! TPC-H q10 groups by seven columns —
//! `c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment` — and
//! returns twenty rows. Six of the seven are functionally determined by
//! `c_custkey`; `c_comment` alone averages ~73 B/row.
//!
//! Measured at SF100 on the three-node cluster by hand-writing the *narrowed*
//! query (same joins, same filters, same `LIMIT`, but `GROUP BY c_custkey`):
//!
//! ```text
//!                       real q10 (wide)      narrowed
//!   s1  customer scan   52 task-s / 3.40 GB  16.5 / 240 MB   14x fewer bytes
//!   s2  orders⋈customer 8,968 task-s         38.9            230x
//!   s5  final agg+TopK  1,510                6.3             240x
//!   wall clock          1784.57 s            120.88 s        14.8x
//! ```
//!
//! `s2` fell **230x while its input bytes fell only ~10x**, so the cost is
//! superlinear in the wide columns — per-row string handling in the hash join,
//! not wire volume. That is why nothing about transport fixed it (see the
//! `q10-dist-s2-is-the-whole-query` record: neither a cross-stage runtime filter
//! nor a deeper shuffle prefetch moved it).
//!
//! # Two rewrites that do not work, so they are not attempted again
//!
//! **Declaring the key alone is not enough.** `ParquetTableSpec::with_primary_key`
//! (shipped separately) gives DataFusion the functional dependency, and
//! `optimize_projections` will happily shrink a `GROUP BY` with it — but only
//! `(columns the parent requires) ∪ (minimal FD subset)`. q10 *selects* all
//! seven grouped columns, so the parent requires them and no key declaration can
//! prune them.
//!
//! **Narrowing the group key alone is not enough either**, and this one was
//! measured rather than reasoned. Rewriting q10 as `GROUP BY c_custkey` plus
//! `first_value(...)` per determined column — exactly what a local
//! composite-key rule would emit — ran at SF100 in **1955.65 s against the
//! 1784.57 s baseline, ~10% slower**, and *not one stage improved*: `s1` shipped
//! the identical 3.40 GB. `first_value` still takes the wide columns as
//! aggregate **inputs**, so they cross every join and shuffle exactly as before.
//! Narrowing the group key only saves hashing, and hashing was never the cost.
//!
//! The cost is the wide columns **flowing through the joins**. Only a join-back
//! removes them, which is why this rule is non-local: the non-locality is the
//! optimization, not an inconvenience around it.
//!
//! # The rewrite
//!
//! ```text
//!   Sort: revenue DESC, fetch=20
//!     Projection: c_custkey, c_name, revenue, c_acctbal, n_name, ...
//!       Aggregate: groupBy=[c_custkey, c_name, c_acctbal, c_phone,
//!                           n_name, c_address, c_comment]
//!                  aggr=[sum(...)]
//!         <customer ⋈ orders ⋈ lineitem ⋈ nation>
//! ```
//!
//! becomes
//!
//! ```text
//!   Sort: revenue DESC, fetch=20                          (unchanged)
//!     Projection: ...                                     (unchanged)
//!       Projection: <exactly the aggregate's old schema>
//!         Inner Join: customer.c_nationkey = nation.n_nationkey
//!           Inner Join: __krishiv_lm.c_custkey = customer.c_custkey
//!             SubqueryAlias: __krishiv_lm
//!               Sort: sum(...) DESC, fetch=20
//!                 Aggregate: groupBy=[c_custkey], aggr=[sum(...)]
//!                   <customer ⋈ orders ⋈ lineitem ⋈ nation>
//!             TableScan: customer
//!           TableScan: nation
//! ```
//!
//! Only the `Aggregate` node is replaced; everything above it keeps the exact
//! same schema, so the enclosing `Projection` and `Sort` are untouched. The
//! inner `Sort` carries the same `fetch`, which is what bounds the join-back to
//! twenty rows — and is why the rule refuses without one.
//!
//! Nothing prunes the wide columns from the narrow branch directly: once the
//! aggregate stops referencing them, DataFusion's own `optimize_projections`
//! does it on the next pass, and the `customer` scan under the aggregate drops
//! to `[c_custkey, c_nationkey]`.
//!
//! # Reaching a column through more than one table
//!
//! `n_name` lives in `nation`, not in `customer`, so no direct join-back on
//! `c_custkey` can fetch it. It is still determined: `c_custkey` → (customer's
//! key) → `c_nationkey`, `c_nationkey = n_nationkey` is an equality of the
//! original join, and `n_nationkey` is nation's key → `n_name`. DataFusion's FD
//! machinery does not compose dependencies across join equalities, so this rule
//! computes its own closure:
//!
//! > a table's columns become available when **every** column of its declared
//! > primary key is either already available or equated — by an inner-join `ON`
//! > pair in the aggregate's input — to a column that is.
//!
//! The same closure decides which group columns may be dropped and, run
//! forwards, emits the join-back chain, so the two can never disagree about what
//! is recoverable.
//!
//! # Why it is safe
//!
//! - **The key really determines the columns.** `Constraint::PrimaryKey` is
//!   unverified here, exactly as Spark/Databricks `RELY` is. That single
//!   declaration is doing two jobs — uniqueness (so the join-back returns one
//!   row per key) and non-nullness (so the key equi-joins at all) — which is
//!   precisely what a primary key means. `Constraint::Unique` is **refused**:
//!   DataFusion marks it `nullable`, and a null key would silently fetch the
//!   wrong row or none.
//! - **Inner joins only, everywhere.** Every node between the aggregate and its
//!   base scans must preserve column *values*: an outer join null-pads its
//!   non-preserved side, so a column re-fetched from the base table would come
//!   back non-null where the original plan had a null. Any node this rule does
//!   not understand — a `SubqueryAlias`, a nested aggregate, a union — makes it
//!   decline rather than guess.
//! - **The join-back is `Inner`, deliberately.** A `Left` join is the instinct,
//!   and it is wrong here for a mechanical reason: `PartitionMode::CollectLeft`
//!   with a join type that emits unmatched build rows cannot be split across
//!   distributed tasks, so `redistribute_unsplittable_broadcast_joins` would
//!   convert it to a hash-partitioned join and shuffle the very columns this
//!   rule exists to keep off the wire. `Inner` is also exactly right
//!   semantically: the surviving key came from a row that already joined.
//! - **The ordering is computable before the columns are.** Every `ORDER BY`
//!   expression must resolve to a retained key column or an aggregate output;
//!   a sort on a deferred column would need the column it is deferring. Group
//!   columns the sort names are added back to the key rather than refused.
//! - **Bounded output only.** Without a `fetch` the join-back re-joins every
//!   group and the rewrite is a pure loss. See [`MAX_LATE_MATERIALIZE_FETCH`].
//! - **Names cannot be crossed.** Columns are matched by fully-qualified name,
//!   and a projection that *reuses* an input's qualified name for a different
//!   expression makes the rule decline; duplicate qualified names across the
//!   collected scans (a self-join) do too.
//!
//! Set `KRISHIV_LATE_MATERIALIZATION=off` to disable.

use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DFSchema, Dependency, NullEquality, Result, TableReference};
use datafusion::logical_expr::{
    Aggregate, Expr, LogicalPlan, LogicalPlanBuilder, Projection, SubqueryAlias, TableScan,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use std::collections::HashSet;
use std::sync::Arc;

/// Environment switch for late materialisation.
pub const LATE_MATERIALIZATION_ENV: &str = "KRISHIV_LATE_MATERIALIZATION";

/// Qualifier given to the bounded top-N branch.
///
/// The join-back puts the narrowed aggregate and the re-fetched table side by
/// side, and both carry the key column under its original name. Aliasing one of
/// them is what keeps `customer.c_custkey` unambiguous; the prefix is deliberately
/// unusable as a SQL identifier a user would write.
const TOPN_ALIAS: &str = "__krishiv_lm";

/// Largest `fetch` this rule will rewrite under.
///
/// The join-back costs one extra scan of each re-fetched table and pays for it
/// by removing the deferred columns from every join and shuffle below. That
/// trade is overwhelming at q10's twenty rows and evaporates as the bound grows:
/// with no bound at all it is a pure loss, because the "top N" is then every
/// group and the join-back re-joins all of them.
///
/// 10,000 is the same ceiling [`crate::distributed_plan`] uses to decide a
/// gathered sort is small enough to cut a stage for, and for the same reason —
/// it is the point past which "a handful of rows" stops being true.
const MAX_LATE_MATERIALIZE_FETCH: usize = 10_000;

/// Ceiling on how many tables the join-back chain may re-join.
///
/// Each link is justified by a declared key, so a long chain is not *wrong* —
/// but it is a lot of extra joins bought on unverified declarations, and past a
/// handful the shape is more likely to be something this rule has misread than a
/// genuine star schema.
const MAX_DIM_CHAIN: usize = 4;

/// Whether late materialisation is enabled (default: yes).
pub fn late_materialization_enabled() -> bool {
    enabled_from(&std::env::var(LATE_MATERIALIZATION_ENV).unwrap_or_default())
}

/// The switch's parsing, separated from reading the environment.
///
/// Kept pure so it can be tested directly: mutating process environment from a
/// test is unsound under a multi-threaded test runner, and the workspace denies
/// the `unsafe` that edition 2024 now requires for `set_var`.
fn enabled_from(value: &str) -> bool {
    !matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "0" | "off" | "false" | "no"
    )
}

/// Replace a bounded top-N aggregate's determined grouping columns with a
/// join-back, so they never enter the joins beneath it.
#[derive(Debug, Default)]
pub struct LateMaterializeTopKAggregate {
    /// Bypass [`late_materialization_enabled`] and always apply.
    ///
    /// The env switch cannot be exercised from a test: mutating process
    /// environment is unsound under a multi-threaded runner and `set_var` is
    /// unsafe since edition 2024, which this workspace denies. Without this the
    /// rule's own tests would silently test nothing the day the default flips.
    forced: bool,
}

impl LateMaterializeTopKAggregate {
    /// The rule with its env gate bypassed, for tests and explicit opt-in.
    #[must_use]
    pub fn forced() -> Self {
        Self { forced: true }
    }
}

impl OptimizerRule for LateMaterializeTopKAggregate {
    fn name(&self) -> &str {
        "late_materialize_topk_aggregate"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // Top-down: the bound lives on the `Sort` at the top and the aggregate
        // is underneath it, so the match starts from the outside in.
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !self.forced && !late_materialization_enabled() {
            return Ok(Transformed::no(plan));
        }
        let LogicalPlan::Sort(sort) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let Some(fetch) = sort.fetch.filter(|n| *n <= MAX_LATE_MATERIALIZE_FETCH) else {
            return Ok(Transformed::no(plan));
        };

        // Walk down to the aggregate, lowering the sort expressions through
        // every projection on the way so they can be re-expressed against the
        // aggregate's own output.
        let mut sort_exprs: Vec<Expr> = sort.expr.iter().map(|s| s.expr.clone()).collect();
        let mut node: &LogicalPlan = &sort.input;
        let mut projections: Vec<&Projection> = Vec::new();
        loop {
            match node {
                LogicalPlan::Projection(proj) => {
                    let Some(lowered) = lower_exprs_through(proj, &sort_exprs) else {
                        return Ok(Transformed::no(plan));
                    };
                    sort_exprs = lowered;
                    projections.push(proj);
                    node = &proj.input;
                }
                LogicalPlan::Aggregate(_) => break,
                _ => return Ok(Transformed::no(plan)),
            }
        }
        let LogicalPlan::Aggregate(agg) = node else {
            return Ok(Transformed::no(plan));
        };

        let Some(rewritten) = rewrite_aggregate(agg, &sort_exprs, sort, fetch)? else {
            return Ok(Transformed::no(plan));
        };

        // Rebuild the projection chain over the new node, outermost last.
        let mut rebuilt = rewritten;
        for proj in projections.into_iter().rev() {
            rebuilt = LogicalPlan::Projection(Projection::try_new(
                proj.expr.clone(),
                Arc::new(rebuilt),
            )?);
        }
        Ok(Transformed::yes(LogicalPlan::Sort(
            datafusion::logical_expr::Sort {
                expr: sort.expr.clone(),
                input: Arc::new(rebuilt),
                fetch: sort.fetch,
            },
        )))
    }
}

/// Rewrite one aggregate, or return `None` to leave the plan alone.
///
/// `sort_exprs` are the enclosing sort's expressions already lowered to the
/// aggregate's output schema; `sort` supplies the ordering directions for the
/// inner bounded sort.
fn rewrite_aggregate(
    agg: &Aggregate,
    sort_exprs: &[Expr],
    sort: &datafusion::logical_expr::Sort,
    fetch: usize,
) -> Result<Option<LogicalPlan>> {
    // Plain grouping columns only. GROUPING SETS / ROLLUP / CUBE produce
    // several group lists at once and computed group expressions have no
    // column to fetch back, so neither has a key to narrow to.
    let mut group_cols = Vec::with_capacity(agg.group_expr.len());
    for expr in &agg.group_expr {
        let Expr::Column(col) = expr else {
            return Ok(None);
        };
        group_cols.push(col.clone());
    }
    if group_cols.len() < 2 {
        // With one grouping column there is nothing determined to defer.
        return Ok(None);
    }

    let Some(facts) = InputFacts::collect(&agg.input) else {
        return Ok(None);
    };
    if facts.tables.is_empty() {
        return Ok(None);
    }

    // The retained key: the smallest subset of the grouping columns whose
    // closure still reaches all the others, plus any column the ordering names
    // (which must be computable before the deferred columns exist).
    let mut key = facts.minimal_key(&group_cols);
    for expr in sort_exprs {
        for col in expr.column_refs() {
            if group_cols.contains(col) && !key.contains(col) {
                key.push(col.clone());
            }
        }
    }
    // Restore the group list's own order, so the narrowed aggregate's schema is
    // a sub-sequence of the original's rather than an arbitrary permutation.
    key.sort_by_key(|col| group_cols.iter().position(|g| g == col).unwrap_or(usize::MAX));
    let deferred: Vec<Column> = group_cols
        .iter()
        .filter(|col| !key.contains(col))
        .cloned()
        .collect();
    if deferred.is_empty() || key.is_empty() {
        return Ok(None);
    }

    // Every sort expression must be answerable from the narrowed aggregate:
    // retained key columns and aggregate outputs, nothing else.
    let agg_schema = agg.schema.as_ref();
    for expr in sort_exprs {
        for col in expr.column_refs() {
            let is_aggregate_output = agg_schema
                .index_of_column(col)
                .is_ok_and(|idx| idx >= group_cols.len());
            if !key.contains(col) && !is_aggregate_output {
                return Ok(None);
            }
        }
    }

    let Some(chain) = facts.dim_chain(&key, &deferred) else {
        return Ok(None);
    };

    // ── the narrow branch: same input, key-only grouping, same bound ────────
    let key_exprs: Vec<Expr> = key.iter().cloned().map(Expr::Column).collect();
    let narrow = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::clone(&agg.input),
        key_exprs,
        agg.aggr_expr.clone(),
    )?);
    // `SubqueryAlias` requalifies every field and keeps its *name*, so two
    // fields that differ only by qualifier — `a.id` and `b.id` in the key, say —
    // would collide into one ambiguous `__krishiv_lm.id` and every reference
    // into the branch would be a coin flip.
    {
        let mut names = HashSet::new();
        if !narrow
            .schema()
            .fields()
            .iter()
            .all(|field| names.insert(field.name().clone()))
        {
            return Ok(None);
        }
    }
    let narrow_sort = LogicalPlan::Sort(datafusion::logical_expr::Sort {
        expr: sort
            .expr
            .iter()
            .zip(sort_exprs)
            .map(|(original, lowered)| datafusion::logical_expr::SortExpr {
                expr: lowered.clone(),
                asc: original.asc,
                nulls_first: original.nulls_first,
            })
            .collect(),
        input: Arc::new(narrow),
        fetch: Some(fetch),
    });
    let topn = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
        Arc::new(narrow_sort),
        TableReference::bare(TOPN_ALIAS),
    )?);

    // ── the join-back ───────────────────────────────────────────────────────
    let mut joined = topn;
    for link in &chain {
        // A key that does not resolve on the side it is meant to index is not
        // an error DataFusion reports: `join_detailed` drops the pair and the
        // physical planner produces a **cross join**, which multiplies the
        // result by the whole dimension table. That is exactly what the first
        // version of this rule did — it probed with `customer.c_custkey` when
        // the narrowed branch had already been requalified to
        // `__krishiv_lm.c_custkey` — and every row came back duplicated.
        // Refusing here turns a silent wrong answer into no rewrite at all.
        let resolves = link
            .probe_keys
            .iter()
            .all(|col| index_of(joined.schema(), col).is_some())
            && link
                .key_columns
                .iter()
                .all(|col| index_of(link.scan.schema(), col).is_some());
        if !resolves {
            return Ok(None);
        }
        joined = LogicalPlanBuilder::from(joined)
            .join_detailed(
                link.scan.clone(),
                // Inner, not Left. See the module docs: `CollectLeft` cannot be
                // split across distributed tasks for a join type that emits
                // unmatched build rows, so a Left join here is converted to a
                // hash-partitioned one and shuffles the columns this rule
                // exists to keep off the wire.
                datafusion::common::JoinType::Inner,
                (link.probe_keys.clone(), link.key_columns.clone()),
                None,
                NullEquality::NullEqualsNothing,
            )?
            .build()?;
    }

    // ── restore the aggregate's exact output schema ──────────────────────────
    let mut exprs = Vec::with_capacity(agg_schema.fields().len());
    for (idx, (qualifier, field)) in agg_schema.iter().enumerate() {
        let target = Column::new(qualifier.cloned(), field.name());
        // An aggregate's schema is its grouping columns followed by its
        // aggregate outputs, so a field past `group_cols.len()` is an aggregate
        // and can only come from the narrowed branch.
        let source = match group_cols.get(idx).filter(|col| deferred.contains(col)) {
            // A deferred column comes back from the re-joined table under its
            // original qualified name, so it needs no alias at all.
            Some(col) => col.clone(),
            None => Column::new(Some(TableReference::bare(TOPN_ALIAS)), field.name()),
        };
        exprs.push(if source == target {
            Expr::Column(source)
        } else {
            Expr::Column(source).alias_qualified(qualifier.cloned(), field.name())
        });
    }
    Ok(Some(LogicalPlan::Projection(Projection::try_new(
        exprs,
        Arc::new(joined),
    )?)))
}

/// One link of the join-back: a base table, and the equality that reaches it.
#[derive(Debug)]
struct DimLink {
    /// The `TableScan` node, cloned from the aggregate's input.
    scan: LogicalPlan,
    /// The already-available columns matched against, in key order.
    probe_keys: Vec<Column>,
    /// This table's declared key columns, in the same order.
    key_columns: Vec<Column>,
}

/// A base relation found beneath the aggregate, with the key it declares.
#[derive(Debug, Clone)]
struct DimTable {
    scan: LogicalPlan,
    /// Every column the scan projects, fully qualified.
    columns: Vec<Column>,
    /// The declared **primary** key. Never a merely `Unique` constraint — see
    /// the module docs on why nullability rules that out.
    primary_key: Vec<Column>,
}

/// What the aggregate's input tells us about where columns come from.
#[derive(Debug, Default)]
struct InputFacts {
    tables: Vec<DimTable>,
    /// Inner-join `ON` pairs, which are the only way a column of one table can
    /// stand in for a column of another.
    equalities: Vec<(Column, Column)>,
}

impl InputFacts {
    /// Read the base scans and inner-join equalities out of an aggregate input.
    ///
    /// `None` means the shape is not one this rule reasons about — an outer
    /// join, a nested aggregate, a `SubqueryAlias`, a projection that reuses an
    /// input column's qualified name for a different expression, or two scans
    /// that would make a qualified name ambiguous. Declining is always safe;
    /// guessing is not.
    fn collect(plan: &LogicalPlan) -> Option<Self> {
        let mut facts = Self::default();
        facts.walk(plan)?;
        // A qualified name has to identify exactly one column or every match
        // below is a coin flip. Two scans of the same table (a self-join) are
        // the way this happens in practice.
        let mut seen = HashSet::new();
        for table in &facts.tables {
            for col in &table.columns {
                if !seen.insert(col.flat_name()) {
                    return None;
                }
            }
        }
        Some(facts)
    }

    fn walk(&mut self, plan: &LogicalPlan) -> Option<()> {
        match plan {
            LogicalPlan::TableScan(scan) => {
                // A scan without a declared key is still collected: it cannot
                // supply a deferred column, but its column names still have to
                // take part in the ambiguity check below.
                self.tables.push(dim_table(scan));
                Some(())
            }
            LogicalPlan::Filter(filter) => self.walk(&filter.input),
            LogicalPlan::Projection(proj) => {
                projection_preserves_names(proj).then_some(())?;
                self.walk(&proj.input)
            }
            LogicalPlan::Join(join) => {
                // Inner only: an outer join null-pads its non-preserved side, so
                // a column re-fetched from the base table would be non-null
                // where the original plan had a null.
                (join.join_type == datafusion::common::JoinType::Inner).then_some(())?;
                for (left, right) in &join.on {
                    if let (Expr::Column(l), Expr::Column(r)) = (left, right) {
                        self.equalities.push((l.clone(), r.clone()));
                    }
                }
                self.walk(&join.left)?;
                self.walk(&join.right)
            }
            // Everything else — SubqueryAlias, Aggregate, Union, Window,
            // Distinct, Limit — either renames columns or changes what a row
            // means, and this rule has no rule for it.
            _ => None,
        }
    }

    /// Grow `available` by one table whose declared key is reachable from it.
    ///
    /// Returns the link that reaches it, or `None` when nothing new is
    /// reachable. `skip` are tables already in the chain.
    ///
    /// This single step is both halves of the rule: run to a fixpoint it decides
    /// which grouping columns may be dropped, and run forwards it emits the
    /// join-back. Sharing it is what stops the two from ever disagreeing about
    /// what is recoverable.
    fn reach_one(&self, available: &HashSet<String>, skip: &[usize]) -> Option<(usize, DimLink)> {
        for (index, table) in self.tables.iter().enumerate() {
            if skip.contains(&index) || table.primary_key.is_empty() {
                continue;
            }
            // Every column of the key must be available, directly or through an
            // equality with an available column.
            let mut probe_keys = Vec::with_capacity(table.primary_key.len());
            let resolved = table.primary_key.iter().all(|key_col| {
                if available.contains(&key_col.flat_name()) {
                    probe_keys.push(key_col.clone());
                    return true;
                }
                for (left, right) in &self.equalities {
                    for (near, far) in [(left, right), (right, left)] {
                        if near == key_col && available.contains(&far.flat_name()) {
                            probe_keys.push(far.clone());
                            return true;
                        }
                    }
                }
                false
            });
            if !resolved {
                continue;
            }
            // Already fully covered: reaching it again would add nothing.
            if table
                .columns
                .iter()
                .all(|col| available.contains(&col.flat_name()))
            {
                continue;
            }
            return Some((
                index,
                DimLink {
                    scan: table.scan.clone(),
                    probe_keys,
                    key_columns: table.primary_key.clone(),
                },
            ));
        }
        None
    }

    /// Every column reachable from `seed`.
    fn closure(&self, seed: &[Column]) -> HashSet<String> {
        let mut available: HashSet<String> = seed.iter().map(Column::flat_name).collect();
        let mut used: Vec<usize> = Vec::new();
        while let Some((index, _)) = self.reach_one(&available, &used) {
            for col in &self.tables[index].columns {
                available.insert(col.flat_name());
            }
            used.push(index);
        }
        available
    }

    /// The smallest subset of `group_cols` whose closure still reaches them all.
    ///
    /// Greedy removal in the group list's own order, which is both deterministic
    /// and — since a key is conventionally written first — the order that keeps
    /// the key and drops its dependents. A different order could pick a
    /// different minimal set, but never an incorrect one: each removal is
    /// checked against the set that survives it, so no column is ever justified
    /// by one that was already dropped.
    fn minimal_key(&self, group_cols: &[Column]) -> Vec<Column> {
        let mut retained: Vec<Column> = group_cols.to_vec();
        let mut index = 0;
        while index < retained.len() {
            let candidate = retained[index].clone();
            let mut without = retained.clone();
            without.remove(index);
            if self.closure(&without).contains(&candidate.flat_name()) {
                retained = without;
            } else {
                index += 1;
            }
        }
        retained
    }

    /// The join-back chain that fetches `deferred` starting from `key`.
    ///
    /// Only the links that actually contribute survive: a table reached purely
    /// as a stepping stone stays, one that supplies nothing and leads nowhere is
    /// dropped. That pruning is not cosmetic — an unnecessary link could be a
    /// fact table, and re-joining `lineitem` to fetch twenty rows would cost
    /// more than the rewrite saves.
    fn dim_chain(&self, key: &[Column], deferred: &[Column]) -> Option<Vec<DimLink>> {
        let mut available: HashSet<String> = key.iter().map(Column::flat_name).collect();
        // What the join-back has actually built so far, as opposed to what the
        // closure merely knows is determined. The retained key columns live on
        // the narrowed branch under [`TOPN_ALIAS`], not under their original
        // table's qualifier, so a probe into them has to be requalified —
        // without this the pair silently fails to resolve and DataFusion turns
        // the join into a cross join.
        let mut materialised: HashSet<String> = HashSet::new();
        let mut links: Vec<(usize, DimLink)> = Vec::new();
        let mut used: Vec<usize> = Vec::new();
        while !deferred
            .iter()
            .all(|col| available.contains(&col.flat_name()))
        {
            if links.len() >= MAX_DIM_CHAIN {
                return None;
            }
            let (index, mut link) = self.reach_one(&available, &used)?;
            for probe in &mut link.probe_keys {
                if !materialised.contains(&probe.flat_name()) {
                    *probe = Column::new(
                        Some(TableReference::bare(TOPN_ALIAS)),
                        probe.name.clone(),
                    );
                }
            }
            for col in &self.tables[index].columns {
                available.insert(col.flat_name());
                materialised.insert(col.flat_name());
            }
            used.push(index);
            links.push((index, link));
        }

        // Walk back from the links that supply a deferred column, keeping every
        // link whose columns another kept link probes into.
        let mut keep = vec![false; links.len()];
        let mut wanted: HashSet<String> = deferred.iter().map(Column::flat_name).collect();
        for position in (0..links.len()).rev() {
            let (index, link) = &links[position];
            let supplies = self.tables[*index]
                .columns
                .iter()
                .any(|col| wanted.contains(&col.flat_name()));
            if supplies {
                keep[position] = true;
                for probe in &link.probe_keys {
                    wanted.insert(probe.flat_name());
                }
            }
        }
        let pruned: Vec<DimLink> = links
            .into_iter()
            .zip(keep)
            .filter_map(|((_, link), keep)| keep.then_some(link))
            .collect();
        (!pruned.is_empty()).then_some(pruned)
    }
}

/// Read a scan's declared **primary** key out of the functional dependencies
/// DataFusion attached to its projected schema.
///
/// `TableScan::try_new` turns `Constraints` into `FunctionalDependencies` and
/// projects them, dropping any whose key columns the projection removed — so
/// reading them here needs no knowledge of the source's full schema.
///
/// `Dependency::Single` is uniqueness; `!nullable` is what separates
/// `Constraint::PrimaryKey` from `Constraint::Unique`. Both are required: the
/// join-back leans on uniqueness for row counts and on non-nullness for the
/// equi-join to match at all.
fn dim_table(scan: &TableScan) -> DimTable {
    let schema = scan.projected_schema.as_ref();
    let columns: Vec<Column> = schema
        .iter()
        .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()))
        .collect();
    let primary_key = schema
        .functional_dependencies()
        .iter()
        .find(|dep| dep.mode == Dependency::Single && !dep.nullable)
        .map(|dep| {
            dep.source_indices
                .iter()
                .filter_map(|idx| columns.get(*idx).cloned())
                .collect::<Vec<Column>>()
        })
        .filter(|key| !key.is_empty())
        .unwrap_or_default();
    DimTable {
        scan: LogicalPlan::TableScan(scan.clone()),
        columns,
        primary_key,
    }
}

/// Does this projection leave every input column's qualified name meaning what
/// it meant below?
///
/// A projection may compute, drop and reorder freely. What it may **not** do,
/// for this rule's purposes, is reuse a name the input already has for a
/// different expression: every column here is matched by qualified name, and
/// `customer.c_custkey + 1 AS customer.c_custkey` would silently make the
/// join-back fetch by the wrong value.
fn projection_preserves_names(proj: &Projection) -> bool {
    let below: HashSet<String> = proj.input.schema().field_names().into_iter().collect();
    proj.schema
        .iter()
        .enumerate()
        .all(|(idx, (qualifier, field))| {
            let out = Column::new(qualifier.cloned(), field.name());
            if !below.contains(&out.flat_name()) {
                return true;
            }
            matches!(proj.expr.get(idx), Some(Expr::Column(col)) if *col == out)
        })
}

/// Rewrite expressions stated over a projection's output into its input.
///
/// `None` when some column resolves to a computed expression, which cannot be
/// pushed below the projection that computes it.
fn lower_exprs_through(proj: &Projection, exprs: &[Expr]) -> Option<Vec<Expr>> {
    use datafusion::common::tree_node::TreeNode;

    let mut lowered = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let mut unfollowable = false;
        let rewritten = expr
            .clone()
            .transform(|node| {
                if let Expr::Column(col) = &node {
                    return match index_of(&proj.schema, col).and_then(|idx| proj.expr.get(idx)) {
                        Some(inner) => match unalias(inner) {
                            Some(inner) => Ok(Transformed::yes(inner)),
                            None => {
                                unfollowable = true;
                                Ok(Transformed::no(node))
                            }
                        },
                        // Not this projection's column at all — a literal's
                        // sibling, or already an input column. Leave it.
                        None => Ok(Transformed::no(node)),
                    };
                }
                Ok(Transformed::no(node))
            })
            .ok()?;
        if unfollowable {
            return None;
        }
        lowered.push(rewritten.data);
    }
    Some(lowered)
}

/// Strip aliases down to the column underneath, if that is all there is.
fn unalias(expr: &Expr) -> Option<Expr> {
    match expr {
        Expr::Column(_) => Some(expr.clone()),
        Expr::Alias(alias) => unalias(&alias.expr),
        _ => None,
    }
}

/// Position of `col` in `schema`, or `None` if it is not there.
fn index_of(schema: &DFSchema, col: &Column) -> Option<usize> {
    schema.index_of_column(col).ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array as _, Decimal128Array, Int64Array, StringArray};
    use datafusion::common::get_required_group_by_exprs_indices;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::{Constraint, Constraints};
    use datafusion::datasource::MemTable;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    /// `customer`: a primary key plus the wide columns it determines.
    fn customer_table(with_key: bool) -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int64, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("c_comment", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["alice", "bob", "cara", "dan"])),
                Arc::new(StringArray::from(vec!["a1", "a2", "a3", "a4"])),
                Arc::new(Int64Array::from(vec![7i64, 7, 8, 8])),
                Arc::new(StringArray::from(vec!["p1", "p2", "p3", "p4"])),
                Arc::new(
                    Decimal128Array::from(vec![100i128, 200, 300, 400])
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ),
                Arc::new(StringArray::from(vec!["k1", "k2", "k3", "k4"])),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        Arc::new(if with_key {
            table.with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                vec![0],
            )]))
        } else {
            table
        })
    }

    /// `nation`: reachable only *through* `customer.c_nationkey`, which is what
    /// exercises the transitive half of the closure.
    fn nation_table(with_key: bool) -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![7i64, 8])),
                Arc::new(StringArray::from(vec!["GERMANY", "FRANCE"])),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        Arc::new(if with_key {
            table.with_constraints(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                vec![0],
            )]))
        } else {
            table
        })
    }

    /// `orders`: the fact side. Several rows per customer, so a join-back that
    /// duplicated rows would show up immediately in the aggregate values.
    fn orders_table() -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![10i64, 11, 12, 13, 14, 15])),
                Arc::new(Int64Array::from(vec![1i64, 1, 2, 3, 3, 3])),
                Arc::new(
                    Decimal128Array::from(vec![500i128, 700, 900, 100, 200, 300])
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        Arc::new(table.with_constraints(Constraints::new_unverified(vec![
            Constraint::PrimaryKey(vec![0]),
        ])))
    }

    fn context(with_rule: bool, with_keys: bool) -> SessionContext {
        let mut builder = SessionStateBuilder::new().with_default_features();
        if with_rule {
            builder =
                builder.with_optimizer_rule(Arc::new(LateMaterializeTopKAggregate::forced()));
        }
        let ctx = SessionContext::new_with_state(builder.build());
        ctx.register_table("customer", customer_table(with_keys))
            .unwrap();
        ctx.register_table("nation", nation_table(with_keys))
            .unwrap();
        ctx.register_table("orders", orders_table()).unwrap();
        ctx
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let mut cells = Vec::new();
                for col in 0..batch.num_columns() {
                    let casted =
                        datafusion::arrow::compute::cast(batch.column(col), &DataType::Utf8)
                            .unwrap();
                    let array = datafusion::common::cast::as_string_array(&casted).unwrap();
                    cells.push(if array.is_null(row) {
                        String::from("NULL")
                    } else {
                        array.value(row).to_string()
                    });
                }
                out.push(cells.join("|"));
            }
        }
        out
    }

    async fn plan_of(ctx: &SessionContext, sql: &str) -> String {
        format!(
            "{}",
            ctx.sql(sql)
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap()
                .display_indent()
        )
    }

    /// TPC-H q10's shape: seven grouping columns, six determined by the key,
    /// one of them (`n_name`) reachable only through a second table.
    const Q10_SHAPE: &str = "SELECT c_custkey, c_name, sum(o_totalprice) AS revenue, \
        c_acctbal, n_name, c_address, c_phone, c_comment \
        FROM customer, orders, nation \
        WHERE c_custkey = o_custkey AND c_nationkey = n_nationkey \
        GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment \
        ORDER BY revenue DESC LIMIT 20";

    #[tokio::test]
    async fn the_group_by_narrows_to_the_key_alone() {
        let plan = plan_of(&context(true, true), Q10_SHAPE).await;
        assert!(
            plan.contains("groupBy=[[customer.c_custkey]]"),
            "expected a single-column group by:\n{plan}"
        );
        assert!(
            plan.contains(TOPN_ALIAS),
            "expected the bounded top-N branch:\n{plan}"
        );
    }

    /// The property the whole rewrite has to buy: the wide columns must stop
    /// being read on the branch that feeds the joins. A narrowed `GROUP BY`
    /// that still scanned them would have moved the cost, not removed it —
    /// which is exactly how the `first_value` rewrite failed.
    #[tokio::test]
    async fn the_wide_columns_leave_the_aggregate_branch() {
        let plan = plan_of(&context(true, true), Q10_SHAPE).await;
        let narrow_branch = subtree_under(&plan, &format!("SubqueryAlias: {TOPN_ALIAS}"));
        for wide in ["c_comment", "c_address", "c_phone", "c_name"] {
            assert!(
                !narrow_branch.contains(wide),
                "{wide} still crosses the joins under the aggregate:\n\
                 --- narrowed branch ---\n{narrow_branch}\n--- whole plan ---\n{plan}"
            );
        }
        // The point of the rewrite, stated positively: the customer scan that
        // feeds the joins reads two narrow keys instead of seven wide columns.
        assert!(
            narrow_branch.contains("TableScan: customer projection=[c_custkey, c_nationkey]"),
            "the narrowed branch should scan only the keys:\n{narrow_branch}"
        );
        // …and the wide columns are still read exactly once, on the join-back.
        assert!(
            plan.contains("TableScan: customer projection=[c_custkey, c_name, c_address"),
            "the join-back must still fetch the deferred columns:\n{plan}"
        );
    }

    /// The lines of `plan` strictly indented under the first line containing
    /// `header` — i.e. that node's subtree, and nothing beside it.
    ///
    /// Substring-matching the whole rendered plan does not work here: the
    /// restoring projection at the very top names the alias too, so
    /// "everything after the alias appears" is most of the query.
    fn subtree_under(plan: &str, header: &str) -> String {
        let indent = |line: &str| line.len() - line.trim_start().len();
        let mut lines = plan.lines().skip_while(|line| !line.contains(header));
        let root = lines.next().expect("subtree root not found");
        std::iter::once(root)
            .chain(lines.take_while(|line| indent(line) > indent(root)))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// A faster wrong answer is the one outcome worse than a slow right one.
    #[tokio::test]
    async fn results_are_identical_with_and_without_the_rule() {
        for sql in [
            Q10_SHAPE,
            // ties on the ordering column, where "which N" could differ
            "SELECT c_custkey, c_name, count(*) AS n, c_comment FROM customer, orders \
             WHERE c_custkey = o_custkey GROUP BY c_custkey, c_name, c_comment \
             ORDER BY n DESC, c_custkey LIMIT 3",
            // an aggregate over a column that is itself deferred
            "SELECT c_custkey, c_name, max(c_comment) AS m, sum(o_totalprice) AS t \
             FROM customer, orders WHERE c_custkey = o_custkey \
             GROUP BY c_custkey, c_name ORDER BY t DESC LIMIT 2",
            // the transitive reach, with the second table's column in the sort
            "SELECT c_custkey, n_name, sum(o_totalprice) AS t FROM customer, orders, nation \
             WHERE c_custkey = o_custkey AND c_nationkey = n_nationkey \
             GROUP BY c_custkey, n_name ORDER BY t DESC, c_custkey LIMIT 4",
            // a smaller limit than there are groups, and a larger one
            "SELECT c_custkey, c_name, c_address, sum(o_totalprice) AS t \
             FROM customer, orders WHERE c_custkey = o_custkey \
             GROUP BY c_custkey, c_name, c_address ORDER BY t DESC LIMIT 1",
            "SELECT c_custkey, c_name, c_address, sum(o_totalprice) AS t \
             FROM customer, orders WHERE c_custkey = o_custkey \
             GROUP BY c_custkey, c_name, c_address ORDER BY t DESC LIMIT 100",
        ] {
            let with = rows(&context(true, true), sql).await;
            let without = rows(&context(false, true), sql).await;
            assert_eq!(with, without, "results diverged for:\n{sql}");
            assert!(!with.is_empty(), "test query returned nothing: {sql}");
        }
    }

    /// Row *order* is part of the answer for an `ORDER BY … LIMIT`, and the
    /// join-back reorders rows freely — the outer sort is what puts them back.
    #[tokio::test]
    async fn the_ordering_survives_the_join_back() {
        let out = rows(&context(true, true), Q10_SHAPE).await;
        let revenues: Vec<f64> = out
            .iter()
            .map(|row| row.split('|').nth(2).unwrap().parse().unwrap())
            .collect();
        assert!(
            revenues.windows(2).all(|w| w[0] >= w[1]),
            "rows came back out of order: {revenues:?}"
        );
        assert_eq!(
            out,
            rows(&context(false, true), Q10_SHAPE).await,
            "the ordered result must match the unrewritten plan row for row"
        );
    }

    /// Without a declared key nothing is determined, so there is nothing to
    /// defer and the rule must leave the plan exactly as it found it.
    #[tokio::test]
    async fn an_undeclared_key_is_not_assumed() {
        let plan = plan_of(&context(true, false), Q10_SHAPE).await;
        assert!(
            !plan.contains(TOPN_ALIAS),
            "no declared key means no rewrite:\n{plan}"
        );
        assert_eq!(
            rows(&context(true, false), Q10_SHAPE).await,
            rows(&context(false, false), Q10_SHAPE).await
        );
    }

    /// Unbounded output makes the join-back a pure loss: the "top N" is every
    /// group, so it re-joins all of them for nothing.
    #[tokio::test]
    async fn an_unbounded_aggregate_is_left_alone() {
        let sql = "SELECT c_custkey, c_name, sum(o_totalprice) AS t FROM customer, orders \
            WHERE c_custkey = o_custkey GROUP BY c_custkey, c_name ORDER BY t DESC";
        let plan = plan_of(&context(true, true), sql).await;
        assert!(
            !plan.contains(TOPN_ALIAS),
            "no fetch means no bound to exploit:\n{plan}"
        );
    }

    /// An outer join below null-pads its non-preserved side, so a column
    /// re-fetched from the base table would come back non-null where the
    /// original plan had a null.
    #[tokio::test]
    async fn an_outer_join_below_refuses_the_rewrite() {
        let sql = "SELECT c_custkey, c_name, c_comment, sum(o_totalprice) AS t \
            FROM customer LEFT JOIN orders ON c_custkey = o_custkey \
            GROUP BY c_custkey, c_name, c_comment ORDER BY t DESC LIMIT 5";
        let plan = plan_of(&context(true, true), sql).await;
        assert!(
            !plan.contains(TOPN_ALIAS),
            "must not rewrite under an outer join:\n{plan}"
        );
        assert_eq!(
            rows(&context(true, true), sql).await,
            rows(&context(false, true), sql).await
        );
    }

    /// Ordering by a column the rewrite wants to defer would need the column
    /// before it exists. Keeping it in the key instead is strictly better than
    /// refusing, so the rule must still fire — and still be right.
    #[tokio::test]
    async fn a_sort_on_a_determined_column_keeps_it_in_the_key() {
        let sql = "SELECT c_custkey, c_name, c_comment, sum(o_totalprice) AS t \
            FROM customer, orders WHERE c_custkey = o_custkey \
            GROUP BY c_custkey, c_name, c_comment ORDER BY c_name DESC LIMIT 3";
        let plan = plan_of(&context(true, true), sql).await;
        assert!(
            plan.contains("groupBy=[[customer.c_custkey, customer.c_name]]"),
            "the sorted column must stay in the key:\n{plan}"
        );
        assert_eq!(
            rows(&context(true, true), sql).await,
            rows(&context(false, true), sql).await
        );
    }

    /// The optimizer runs rules to a fixed point. The rewrite contains a
    /// bounded sort over an aggregate — its own trigger shape — so without the
    /// "nothing left to defer" guard it would rewrite itself forever.
    #[tokio::test]
    async fn the_rewrite_is_applied_exactly_once() {
        let plan = plan_of(&context(true, true), Q10_SHAPE).await;
        assert_eq!(
            plan.matches(TOPN_ALIAS).count(),
            // one SubqueryAlias node, plus the key column reference the
            // restoring projection makes into it
            plan.matches(&format!("{TOPN_ALIAS}.")).count() + 1,
            "expected a single aliased branch:\n{plan}"
        );
        assert_eq!(
            plan.matches("SubqueryAlias").count(),
            1,
            "expected exactly one rewrite:\n{plan}"
        );
    }

    /// The join-back must never multiply rows. `orders` has three rows for
    /// customer 3, so a chain that re-joined the fact table instead of the
    /// dimension would triple the group — visible as a changed count.
    #[tokio::test]
    async fn the_join_back_does_not_duplicate_rows() {
        let sql = "SELECT c_custkey, c_name, c_comment, count(*) AS n \
            FROM customer, orders WHERE c_custkey = o_custkey \
            GROUP BY c_custkey, c_name, c_comment ORDER BY n DESC, c_custkey LIMIT 10";
        let with = rows(&context(true, true), sql).await;
        assert_eq!(with, rows(&context(false, true), sql).await);
        assert_eq!(with.len(), 3, "expected one row per customer: {with:?}");
    }

    /// A self-join makes a qualified name ambiguous, and every match in this
    /// rule is by qualified name.
    #[tokio::test]
    async fn a_self_join_is_refused() {
        let sql = "SELECT a.c_custkey, a.c_name, a.c_comment, count(*) AS n \
            FROM customer a, customer b \
            WHERE a.c_nationkey = b.c_nationkey \
            GROUP BY a.c_custkey, a.c_name, a.c_comment ORDER BY n DESC LIMIT 5";
        assert_eq!(
            rows(&context(true, true), sql).await,
            rows(&context(false, true), sql).await,
            "a self-join must not change the answer"
        );
    }

    /// Physical plan text, which is where a lost equijoin key becomes visible:
    /// the logical plan looks fine either way.
    async fn physical_plan_of(ctx: &SessionContext, sql: &str) -> String {
        let logical = ctx.sql(sql).await.unwrap().into_optimized_plan().unwrap();
        let physical = ctx.state().create_physical_plan(&logical).await.unwrap();
        format!(
            "{}",
            datafusion::physical_plan::displayable(physical.as_ref()).indent(false)
        )
    }

    /// **The join-back must never become a nested loop or a cross join.**
    ///
    /// This is the failure the first version of the rule actually had: it
    /// probed with `customer.c_custkey` while the narrowed branch had been
    /// requalified to `__krishiv_lm.c_custkey`, `join_detailed` dropped the
    /// unresolvable pair, and the planner produced a **cross join** against the
    /// whole 15M-row customer table. Every row came back duplicated.
    ///
    /// The same class of bug cost this codebase 18.4x on q2 through the
    /// semi-join rules, and neither a results test nor a logical-plan test
    /// catches it on its own — only the physical plan does.
    #[tokio::test]
    async fn the_join_back_is_a_real_equi_join() {
        for sql in [
            Q10_SHAPE,
            "SELECT c_custkey, c_name, c_comment, sum(o_totalprice) AS t \
             FROM customer, orders WHERE c_custkey = o_custkey \
             GROUP BY c_custkey, c_name, c_comment ORDER BY t DESC LIMIT 5",
        ] {
            let plan = physical_plan_of(&context(true, true), sql).await;
            assert!(
                !plan.contains("NestedLoopJoin") && !plan.contains("CrossJoin"),
                "the join-back lost its keys for:\n{sql}\n\n{plan}"
            );
        }
    }

    /// The inner bound is the entire economics of the rewrite: it is what makes
    /// the join-back cost twenty probes instead of fifteen million. If any pass
    /// — logical or physical `EnforceSorting` — drops that `fetch`, the rule
    /// silently becomes a pessimization that still returns the right answer.
    #[tokio::test]
    async fn the_inner_bound_survives_physical_planning() {
        let plan = physical_plan_of(&context(true, true), Q10_SHAPE).await;
        let bounded = plan
            .lines()
            .filter(|line| line.contains("Sort") && line.contains("fetch=20"))
            .count();
        assert!(
            bounded >= 2,
            "expected a bounded sort on the narrowed branch as well as on top, \
             found {bounded}:\n{plan}"
        );
    }

    /// The switch has to actually switch it off — a flag that is declared but
    /// never read is worse than no flag, because the registry gate makes it
    /// look supported.
    #[test]
    fn the_env_switch_is_honoured() {
        for off in ["off", "OFF", "0", "false", "no", " off "] {
            assert!(!enabled_from(off), "{off:?} should disable the rule");
        }
        for on in ["", "on", "1", "true", "anything-else"] {
            assert!(enabled_from(on), "{on:?} should leave the rule enabled");
        }
    }

    /// DataFusion's own FD helper is what `optimize_projections` uses, and it
    /// stops at the first table: it keeps `n_name` in the key because nothing
    /// composes `c_custkey → c_nationkey = n_nationkey → n_name`. This records
    /// the gap the rule's own closure exists to fill, so a future DataFusion
    /// that closes it does not leave two mechanisms fighting.
    #[tokio::test]
    async fn datafusion_alone_does_not_reach_through_the_second_table() {
        let ctx = context(false, true);
        let plan = ctx.sql(Q10_SHAPE).await.unwrap().into_optimized_plan().unwrap();
        let agg = find_aggregate(&plan)
            .unwrap_or_else(|| panic!("no aggregate in:\n{}", plan.display_indent()));
        let names: Vec<String> = agg
            .group_expr
            .iter()
            .map(|e| e.schema_name().to_string())
            .collect();
        let minimal = get_required_group_by_exprs_indices(agg.input.schema(), &names)
            .expect("customer's declared key must be visible");
        let kept: Vec<&String> = minimal.iter().map(|i| &names[*i]).collect();
        assert!(
            kept.iter().any(|n| n.ends_with("n_name")),
            "DataFusion is expected to keep n_name; it kept {kept:?}"
        );
    }

    /// The first `Aggregate` anywhere in a plan.
    fn find_aggregate(plan: &LogicalPlan) -> Option<&Aggregate> {
        if let LogicalPlan::Aggregate(agg) = plan {
            return Some(agg);
        }
        plan.inputs().into_iter().find_map(find_aggregate)
    }
}
