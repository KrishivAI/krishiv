#![forbid(unsafe_code)]
//! Spark's row generators: `explode`, `posexplode`, `inline`, `stack`,
//! `json_tuple` (and the `_outer` forms), in the SELECT list, in
//! `LATERAL VIEW`, and as `[CROSS] JOIN UNNEST(...)`.
//!
//! DataFusion 54 plans none of these. It rejects `LATERAL VIEW`, has no
//! function called `explode`, and although it *plans* a lateral `UNNEST` it
//! cannot execute one ("Physical plan does not support … OuterReferenceColumn").
//! What it does execute is `unnest(...)` in a SELECT list. Three pieces turn
//! the first into the second:
//!
//! 1. [`rewrite_generator_statement`] rewrites the parsed statement so every
//!    generator becomes a lateral *relation*: `LATERAL VIEW explode(x) t AS c`
//!    and a SELECT-list `explode(x) AS c` both become
//!    `CROSS JOIN LATERAL explode(x) AS t(c)`.
//! 2. [`GeneratorRelationPlanner`] plans such a relation. By then the argument
//!    types are known, so it can write the generator out as a one-row lateral
//!    subquery of plain `unnest` calls with Spark's column names — the fields
//!    of an `inline` struct, `key`/`value` for a map, `pos`/`col`, `col0…`.
//! 3. [`LateralGeneratorRule`] removes the lateral join: a generator subquery
//!    joined to `L` is the same rows as that subquery's projections evaluated
//!    *over* `L`, which needs no correlation at all.
//!
//! The `_outer` forms keep a row for an empty or NULL input by unnesting
//! `[NULL]` instead.

use std::ops::ControlFlow;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, Result, plan_datafusion_err, plan_err};
use datafusion::logical_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion::logical_expr::{Expr, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, Join, JoinConstraint,
    JoinOperator, ObjectName, Query, Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr,
    TableAlias, TableAliasColumnDef, TableFactor, TableWithJoins, VisitMut, VisitorMut,
    WildcardAdditionalOptions,
};
use datafusion::sql::sqlparser::dialect::{Dialect, DuckDbDialect};
use datafusion::sql::sqlparser::parser::Parser;

use crate::{SqlError, SqlResult};

/// The dialect the engine plans with (DuckDB's), plus Spark's multi-column
/// alias `expr AS (a, b)`, which generators need and DuckDB does not parse.
///
/// Every method `DuckDbDialect` overrides is forwarded, and the dialect
/// identifies itself as DuckDB's, so a statement parses here exactly as it
/// would on the normal path apart from that one addition.
#[derive(Debug)]
struct GeneratorDialect;

macro_rules! forward_to_duckdb {
    ($($method:ident),* $(,)?) => {
        $(fn $method(&self) -> bool {
            DuckDbDialect {}.$method()
        })*
    };
}

impl Dialect for GeneratorDialect {
    fn dialect(&self) -> std::any::TypeId {
        std::any::TypeId::of::<DuckDbDialect>()
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        DuckDbDialect {}.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        DuckDbDialect {}.is_identifier_part(ch)
    }

    fn supports_select_item_multi_column_alias(&self) -> bool {
        true
    }

    forward_to_duckdb!(
        supports_trailing_commas,
        supports_filter_during_aggregation,
        supports_group_by_expr,
        supports_bitwise_shift_operators,
        supports_named_fn_args_with_eq_operator,
        supports_named_fn_args_with_assignment_operator,
        supports_dictionary_syntax,
        support_map_literal_syntax,
        supports_lambda_functions,
        allow_extract_single_quotes,
        supports_explain_with_utility_options,
        supports_load_extension,
        supports_array_typedef_with_brackets,
        supports_from_first_select,
        supports_order_by_all,
        supports_select_wildcard_exclude,
        supports_notnull_operator,
        supports_install,
        supports_detach,
        supports_select_wildcard_replace,
        supports_comma_separated_trim,
    );
}

/// Prefix of the table alias given to a generator that had none.
const GENERATED_ALIAS: &str = "__krishiv_gen";

/// Register the relation planner and the lateral-join rule.
pub fn register_spark_generators(ctx: &SessionContext) -> Result<()> {
    ctx.register_relation_planner(Arc::new(GeneratorRelationPlanner))?;
    ctx.add_analyzer_rule(Arc::new(LateralGeneratorRule));
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Generator {
    Explode,
    PosExplode,
    Inline,
    Stack,
    JsonTuple,
    Unnest,
}

/// A generator and whether it is the `_outer` form.
fn generator_named(name: &str) -> Option<(Generator, bool)> {
    let lower = name.to_ascii_lowercase();
    let (base, outer) = match lower.strip_suffix("_outer") {
        Some(base) => (base, true),
        None => (lower.as_str(), false),
    };
    let generator = match base {
        "explode" => Generator::Explode,
        "posexplode" => Generator::PosExplode,
        "inline" => Generator::Inline,
        "stack" if !outer => Generator::Stack,
        "json_tuple" if !outer => Generator::JsonTuple,
        "unnest" if !outer => Generator::Unnest,
        _ => return None,
    };
    Some((generator, outer))
}

/// The unqualified function name, when the name is a single identifier.
fn simple_name(name: &ObjectName) -> Option<String> {
    match name.0.as_slice() {
        [part] => part.as_ident().map(|ident| ident.value.clone()),
        _ => None,
    }
}

// ── 1. Statement rewrite ─────────────────────────────────────────────────────

/// Rewrite the generators in `sql` into lateral relations.
///
/// Returns `None` when the statement uses none (the text is then planned as
/// usual), or when it does not parse here — DataFusion reports that better.
pub(crate) fn rewrite_generator_statement(sql: &str) -> SqlResult<Option<DFStatement>> {
    let lower = sql.to_ascii_lowercase();
    let mentions_generator = ["explode", "inline", "stack", "json_tuple", "unnest"]
        .iter()
        .any(|name| lower.contains(name))
        || lower.contains("lateral view")
        // The same pass rewrites ASOF JOIN; see `crate::asof_join`.
        || lower.contains("asof");
    if !mentions_generator {
        return Ok(None);
    }
    let Ok(mut statements) = DFParser::parse_sql_with_dialect(sql, &GeneratorDialect) else {
        return Ok(None);
    };
    if statements.len() != 1 {
        return Ok(None);
    }
    let Some(DFStatement::Statement(mut statement)) = statements.pop_front() else {
        return Ok(None);
    };
    let mut rewriter = GeneratorRewriter::default();
    if let ControlFlow::Break(error) = statement.visit(&mut rewriter) {
        return Err(error);
    }
    Ok(rewriter
        .changed
        .then_some(DFStatement::Statement(statement)))
}

#[derive(Default)]
struct GeneratorRewriter {
    changed: bool,
    next_alias: usize,
}

impl VisitorMut for GeneratorRewriter {
    type Break = SqlError;

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<SqlError> {
        match self.rewrite_set_expr(&mut query.body) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }
    }
}

fn unsupported(message: impl Into<String>) -> SqlError {
    SqlError::DataFusion {
        message: message.into(),
    }
}

impl GeneratorRewriter {
    fn rewrite_set_expr(&mut self, body: &mut SetExpr) -> SqlResult<()> {
        match body {
            SetExpr::Select(select) => self.rewrite_select(select),
            SetExpr::SetOperation { left, right, .. } => {
                self.rewrite_set_expr(left)?;
                self.rewrite_set_expr(right)
            }
            // A nested query is visited in its own right.
            _ => Ok(()),
        }
    }

    fn fresh_alias(&mut self) -> Ident {
        let alias = Ident::new(format!("{GENERATED_ALIAS}{}", self.next_alias));
        self.next_alias += 1;
        alias
    }

    fn rewrite_select(&mut self, select: &mut Select) -> SqlResult<()> {
        // A generator called as a table function: only one that follows
        // another relation can see that relation's columns.
        let mut first = true;
        for item in &mut select.from {
            if !first {
                self.make_lateral(&mut item.relation);
            }
            first = false;
            for join in &mut item.joins {
                self.make_lateral(&mut join.relation);
            }
        }

        for item in &mut select.from {
            for join in &mut item.joins {
                self.rewrite_asof(join)?;
            }
        }

        let mut generators: Vec<TableFactor> = Vec::new();

        for view in std::mem::take(&mut select.lateral_views) {
            let SqlExpr::Function(function) = view.lateral_view else {
                return Err(unsupported(
                    "LATERAL VIEW expects a generator function such as explode(...)",
                ));
            };
            let name = simple_name(&function.name).unwrap_or_default();
            let Some((_, already_outer)) = generator_named(&name) else {
                return Err(unsupported(format!(
                    "LATERAL VIEW {name}(...): not a generator function"
                )));
            };
            let name = if view.outer && !already_outer {
                format!("{name}_outer")
            } else {
                name
            };
            let alias = match view.lateral_view_name.0.as_slice() {
                [part] => part.as_ident().cloned(),
                _ => None,
            }
            .unwrap_or_else(|| self.fresh_alias());
            generators.push(generator_relation(
                &name,
                function_args(function.args)?,
                alias,
                view.lateral_col_alias,
            ));
        }

        let mut seen_in_select = false;
        for item in &mut select.projection {
            let (function, aliases) = match item {
                SelectItem::UnnamedExpr(SqlExpr::Function(function)) => (function, Vec::new()),
                SelectItem::ExprWithAlias {
                    expr: SqlExpr::Function(function),
                    alias,
                } => (function, vec![alias.clone()]),
                SelectItem::ExprWithAliases {
                    expr: SqlExpr::Function(function),
                    aliases,
                } => (function, aliases.clone()),
                _ => continue,
            };
            let Some(name) = simple_name(&function.name) else {
                continue;
            };
            // `unnest(...)` in a SELECT list is DataFusion's own.
            match generator_named(&name) {
                Some((Generator::Unnest, _)) | None => continue,
                Some(_) => {}
            }
            if seen_in_select {
                return Err(unsupported(
                    "only one generator function is allowed per SELECT list",
                ));
            }
            seen_in_select = true;
            let alias = self.fresh_alias();
            let args = function_args(std::mem::replace(
                &mut function.args,
                FunctionArguments::None,
            ))?;
            generators.push(generator_relation(&name, args, alias.clone(), aliases));
            *item = SelectItem::QualifiedWildcard(
                SelectItemQualifiedWildcardKind::ObjectName(ObjectName::from(vec![alias])),
                WildcardAdditionalOptions::default(),
            );
        }

        if generators.is_empty() {
            return Ok(());
        }
        self.changed = true;

        // The generators go on the end of one join chain, so each can see
        // every relation before it: fold `FROM a, b` into `a CROSS JOIN b`.
        let mut from = std::mem::take(&mut select.from).into_iter();
        let mut generators = generators.into_iter();
        let mut chain = match from.next() {
            Some(first) => first,
            None => TableWithJoins {
                // No FROM: the first generator stands alone, uncorrelated.
                relation: match generators.next() {
                    Some(TableFactor::Function {
                        name, args, alias, ..
                    }) => TableFactor::Function {
                        lateral: false,
                        name,
                        args,
                        with_ordinality: false,
                        alias,
                    },
                    Some(other) => other,
                    None => return Ok(()),
                },
                joins: Vec::new(),
            },
        };
        for later in from {
            chain.joins.push(cross_join(later.relation));
            chain.joins.extend(later.joins);
        }
        chain.joins.extend(generators.map(cross_join));
        select.from = vec![chain];
        Ok(())
    }

    /// `ASOF JOIN r MATCH_CONDITION (c) ON k` becomes `LEFT JOIN r ON k AND
    /// marker(c)`, which DataFusion can plan and `AsofJoinRule` then completes.
    fn rewrite_asof(&mut self, join: &mut Join) -> SqlResult<()> {
        let JoinOperator::AsOf {
            match_condition,
            constraint,
        } = &join.join_operator
        else {
            return Ok(());
        };
        let marker = format!("{}({match_condition})", crate::asof_join::ASOF_MARKER);
        let on = match constraint {
            JoinConstraint::On(keys) => format!("({keys}) AND {marker}"),
            JoinConstraint::None => marker,
            JoinConstraint::Using(_) | JoinConstraint::Natural => {
                return Err(unsupported(
                    "ASOF JOIN takes its keys in an ON clause, not USING or NATURAL",
                ));
            }
        };
        let on = Parser::new(&GeneratorDialect)
            .try_with_sql(&on)
            .and_then(|mut parser| parser.parse_expr())
            .map_err(|e| unsupported(format!("ASOF JOIN: {e}")))?;
        join.join_operator = JoinOperator::LeftOuter(JoinConstraint::On(on));
        self.changed = true;
        Ok(())
    }

    /// `… JOIN UNNEST(x) AS u(v)` parses as a plain table function, which
    /// cannot refer to the tables beside it. Make it a lateral one.
    fn make_lateral(&mut self, relation: &mut TableFactor) {
        let TableFactor::Table {
            name,
            alias,
            args: Some(_),
            ..
        } = relation
        else {
            return;
        };
        if simple_name(name)
            .and_then(|n| generator_named(&n))
            .is_none()
        {
            return;
        }
        let name = name.clone();
        let alias = alias.clone();
        let TableFactor::Table {
            args: Some(args), ..
        } = std::mem::replace(
            relation,
            TableFactor::Function {
                lateral: true,
                name: name.clone(),
                args: Vec::new(),
                with_ordinality: false,
                alias: alias.clone(),
            },
        )
        else {
            return;
        };
        *relation = TableFactor::Function {
            lateral: true,
            name,
            args: args.args,
            with_ordinality: false,
            alias,
        };
        self.changed = true;
    }
}

fn function_args(arguments: FunctionArguments) -> SqlResult<Vec<FunctionArg>> {
    match arguments {
        FunctionArguments::List(list) => Ok(list.args),
        FunctionArguments::None => Ok(Vec::new()),
        FunctionArguments::Subquery(_) => Err(unsupported(
            "a generator function does not take a subquery argument",
        )),
    }
}

fn generator_relation(
    name: &str,
    args: Vec<FunctionArg>,
    alias: Ident,
    columns: Vec<Ident>,
) -> TableFactor {
    TableFactor::Function {
        lateral: true,
        name: ObjectName::from(vec![Ident::new(name)]),
        args,
        with_ordinality: false,
        alias: Some(TableAlias {
            explicit: true,
            name: alias,
            columns: columns
                .into_iter()
                .map(|name| TableAliasColumnDef {
                    name,
                    data_type: None,
                })
                .collect(),
            at: None,
        }),
    }
}

fn cross_join(relation: TableFactor) -> Join {
    Join {
        relation,
        global: false,
        join_operator: JoinOperator::CrossJoin(JoinConstraint::None),
    }
}

// ── 2. Relation planner ──────────────────────────────────────────────────────

/// Plans a generator relation as a one-row subquery of `unnest` calls.
#[derive(Debug)]
pub struct GeneratorRelationPlanner;

impl RelationPlanner for GeneratorRelationPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        let (lateral, name, args, alias) = match &relation {
            TableFactor::Function {
                lateral,
                name,
                args,
                alias,
                ..
            } => (*lateral, name, args.clone(), alias.clone()),
            TableFactor::Table {
                name,
                alias,
                args: Some(args),
                ..
            } => (false, name, args.args.clone(), alias.clone()),
            _ => return Ok(RelationPlanning::Original(Box::new(relation))),
        };
        let Some(function) = simple_name(name) else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };
        let Some((generator, outer)) = generator_named(&function) else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };

        let args = args
            .into_iter()
            .map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
                other => plan_err!("{function}: unsupported argument `{other}`"),
            })
            .collect::<Result<Vec<_>>>()?;
        let columns = generator_columns(&function, generator, outer, &args, context)?;

        let names: Vec<String> = match &alias {
            Some(alias) if !alias.columns.is_empty() => {
                if alias.columns.len() != columns.len() {
                    return plan_err!(
                        "{function} produces {} column(s) but {} alias(es) were given",
                        columns.len(),
                        alias.columns.len()
                    );
                }
                alias.columns.iter().map(|c| c.name.value.clone()).collect()
            }
            _ => columns.iter().map(|(name, _)| name.clone()).collect(),
        };
        let items = columns
            .iter()
            .zip(&names)
            .map(|((_, expr), name)| format!("{expr} AS {}", quoted_ident(name)))
            .collect::<Vec<_>>()
            .join(", ");
        let subquery = Parser::new(&DuckDbDialect {})
            .try_with_sql(&format!("SELECT {items}"))
            .and_then(|mut parser| parser.parse_query())
            .map_err(|e| plan_datafusion_err!("{function}: {e}"))?;

        let plan = context.plan(TableFactor::Derived {
            lateral,
            subquery,
            alias: alias.map(|alias| TableAlias {
                explicit: true,
                name: alias.name,
                columns: Vec::new(),
                at: None,
            }),
            sample: None,
        })?;
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan, None,
        ))))
    }
}

fn quoted_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// `[NULL]` in place of an empty or NULL array, so one row survives.
fn keep_empty(array: &str) -> String {
    format!("CASE WHEN ({array}) IS NULL OR cardinality({array}) = 0 THEN [NULL] ELSE {array} END")
}

/// The `(default column name, SQL expression)` pairs a generator produces.
fn generator_columns(
    function: &str,
    generator: Generator,
    outer: bool,
    args: &[SqlExpr],
    context: &mut dyn RelationPlannerContext,
) -> Result<Vec<(String, String)>> {
    let mut type_of = |expr: &SqlExpr| -> Result<DataType> {
        let empty = DFSchema::empty();
        context.sql_to_expr(expr.clone(), &empty)?.get_type(&empty)
    };
    let one_arg = || match args {
        [arg] => Ok(arg),
        _ => plan_err!("{function} takes exactly one argument"),
    };
    let array = |text: String| if outer { keep_empty(&text) } else { text };

    match generator {
        Generator::Unnest => {
            if args.is_empty() {
                return plan_err!("{function} needs at least one array");
            }
            Ok(args
                .iter()
                .enumerate()
                .map(|(index, arg)| {
                    let name = if index == 0 {
                        String::from("unnest")
                    } else {
                        format!("unnest_{index}")
                    };
                    (name, format!("unnest({arg})"))
                })
                .collect())
        }
        Generator::Explode => {
            let arg = one_arg()?;
            if let DataType::Map(..) = type_of(arg)? {
                return Ok(vec![
                    (
                        String::from("key"),
                        format!("unnest({})", array(format!("map_keys({arg})"))),
                    ),
                    (
                        String::from("value"),
                        format!("unnest({})", array(format!("map_values({arg})"))),
                    ),
                ]);
            }
            Ok(vec![(
                String::from("col"),
                format!("unnest({})", array(arg.to_string())),
            )])
        }
        Generator::PosExplode => {
            let arg = one_arg()?;
            let positions = format!("range(0, cardinality({arg}))");
            let positions = if outer {
                format!(
                    "CASE WHEN ({arg}) IS NULL OR cardinality({arg}) = 0 THEN [NULL] ELSE {positions} END"
                )
            } else {
                positions
            };
            Ok(vec![
                (
                    String::from("pos"),
                    format!("CAST(unnest({positions}) AS INT)"),
                ),
                (
                    String::from("col"),
                    format!("unnest({})", array(arg.to_string())),
                ),
            ])
        }
        Generator::Inline => {
            let arg = one_arg()?;
            let data_type = type_of(arg)?;
            let element = match &data_type {
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::FixedSizeList(field, _) => field.data_type().clone(),
                other => other.clone(),
            };
            let DataType::Struct(fields) = element else {
                return plan_err!("{function} expects an array of structs, got {data_type}");
            };
            let rows = array(arg.to_string());
            Ok(fields
                .iter()
                .map(|field| {
                    (
                        field.name().clone(),
                        format!(
                            "get_field(unnest({rows}), {})",
                            string_literal(field.name())
                        ),
                    )
                })
                .collect())
        }
        Generator::Stack => {
            let Some((count, values)) = args.split_first() else {
                return plan_err!("{function} needs a row count and at least one value");
            };
            let rows = count
                .to_string()
                .parse::<usize>()
                .ok()
                .filter(|rows| *rows > 0)
                .ok_or_else(|| {
                    plan_datafusion_err!(
                        "{function}: the row count must be a positive integer literal, got {count}"
                    )
                })?;
            if values.is_empty() {
                return plan_err!("{function} needs at least one value");
            }
            let width = values.len().div_ceil(rows);
            Ok((0..width)
                .map(|column| {
                    let cells = (0..rows)
                        .map(|row| {
                            values
                                .get(row * width + column)
                                .map_or_else(|| String::from("NULL"), ToString::to_string)
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    // `make_array`, not `[…]`: DataFusion resolves the outer
                    // row's columns in a function call but not in the literal.
                    (
                        format!("col{column}"),
                        format!("unnest(make_array({cells}))"),
                    )
                })
                .collect())
        }
        Generator::JsonTuple => {
            let Some((json, keys)) = args.split_first() else {
                return plan_err!("{function} needs a JSON string and at least one key");
            };
            if keys.is_empty() {
                return plan_err!("{function} needs at least one key");
            }
            Ok(keys
                .iter()
                .enumerate()
                .map(|(index, key)| {
                    (
                        format!("c{index}"),
                        format!("get_json_object({json}, concat('$.', {key}))"),
                    )
                })
                .collect())
        }
    }
}

// ── 3. Lateral join removal ──────────────────────────────────────────────────

/// Replaces `L CROSS JOIN LATERAL (one-row generator subquery)` with that
/// subquery's projections evaluated over `L`.
#[derive(Debug)]
pub struct LateralGeneratorRule;

impl AnalyzerRule for LateralGeneratorRule {
    fn name(&self) -> &str {
        "lateral_generator"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|node| match unnest_over_left(&node)? {
            Some(rewritten) => Ok(Transformed::yes(rewritten)),
            None => Ok(Transformed::no(node)),
        })
        .map(|transformed| transformed.data)
    }
}

/// The rewrite for one join, or `None` when the join is not that shape.
fn unnest_over_left(node: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Join(join) = node else {
        return Ok(None);
    };
    if join.join_type != JoinType::Inner || !join.on.is_empty() || join.filter.is_some() {
        return Ok(None);
    }
    let LogicalPlan::SubqueryAlias(aliased) = join.right.as_ref() else {
        return Ok(None);
    };
    let LogicalPlan::Subquery(subquery) = aliased.input.as_ref() else {
        return Ok(None);
    };

    // Top-down: projections and unnests over a single produced row.
    let mut chain = Vec::new();
    let mut current = subquery.subquery.as_ref();
    loop {
        match current {
            LogicalPlan::Projection(projection) => {
                chain.push(current);
                current = projection.input.as_ref();
            }
            LogicalPlan::Unnest(unnest) => {
                chain.push(current);
                current = unnest.input.as_ref();
            }
            LogicalPlan::EmptyRelation(empty) if empty.produce_one_row => break,
            _ => return Ok(None),
        }
    }
    let Some(LogicalPlan::Projection(_)) = chain.first() else {
        return Ok(None);
    };

    let left = join.left.as_ref();
    let left_schema = left.schema();
    // Every correlated reference must be to the left side of this join.
    let mut correlated = false;
    let mut resolvable = true;
    for node in &chain {
        for expr in node.expressions() {
            expr.apply(|e| {
                if let Expr::OuterReferenceColumn(_, column) = e {
                    correlated = true;
                    resolvable &= left_schema.has_column(column);
                }
                Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
            })?;
        }
    }
    if !correlated || !resolvable {
        return Ok(None);
    }

    let left_columns: Vec<Expr> = left_schema
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    let bind = |expr: &Expr| -> Result<Expr> {
        expr.clone()
            .transform_up(|e| match e {
                Expr::OuterReferenceColumn(_, column) => Ok(Transformed::yes(Expr::Column(column))),
                other => Ok(Transformed::no(other)),
            })
            .map(|transformed| transformed.data)
    };

    let mut plan = left.clone();
    let last = chain.len() - 1;
    for (depth, node) in chain.iter().rev().enumerate() {
        plan = match node {
            LogicalPlan::Unnest(unnest) => LogicalPlanBuilder::from(plan)
                .unnest_columns_with_options(unnest.exec_columns.clone(), unnest.options.clone())?
                .build()?,
            LogicalPlan::Projection(projection) => {
                let mut exprs = left_columns.clone();
                if depth == last {
                    // The join's right-hand columns, under the relation's
                    // alias so they cannot collide with the left's.
                    for (expr, field) in projection.expr.iter().zip(projection.schema.fields()) {
                        let bound = bind(expr)?.unalias_nested().data;
                        exprs
                            .push(bound.alias_qualified(Some(aliased.alias.clone()), field.name()));
                    }
                } else {
                    for expr in &projection.expr {
                        exprs.push(bind(expr)?);
                    }
                }
                LogicalPlanBuilder::from(plan).project(exprs)?.build()?
            }
            _ => return Ok(None),
        };
    }

    // Same columns as the join it replaces, or leave the join alone.
    let expected: Vec<Column> = node.schema().columns();
    if plan.schema().columns() != expected {
        return Ok(None);
    }
    Ok(Some(plan))
}

#[cfg(test)]
mod tests {
    use arrow::util::pretty::pretty_format_batches;

    async fn engine() -> crate::SqlEngine {
        let engine = crate::SqlEngine::new();
        for ddl in [
            "CREATE TABLE t AS SELECT * FROM (VALUES \
               (1, [10, 20]), (2, arrow_cast([], 'List(Int64)')), (3, NULL)) AS v(id, arr)",
            "CREATE TABLE s AS SELECT 1 AS id, \
               [named_struct('a', 1, 'b', 'x'), named_struct('a', 2, 'b', 'y')] AS st, \
               map(['k1', 'k2'], [100, 200]) AS m, \
               '{\"a\": 1, \"b\": \"two\"}' AS j",
        ] {
            engine
                .sql(ddl)
                .await
                .expect("plan ddl")
                .collect()
                .await
                .expect("run ddl");
        }
        engine
    }

    /// Header and rows, one `|`-joined string per line, borders removed.
    async fn rows(sql: &str) -> Vec<String> {
        let batches = engine()
            .await
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("plan `{sql}`: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("run `{sql}`: {e}"));
        pretty_format_batches(&batches)
            .expect("format")
            .to_string()
            .lines()
            .filter(|line| line.starts_with('|'))
            .map(|line| {
                line.trim_matches('|')
                    .split('|')
                    .map(str::trim)
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect()
    }

    async fn error(sql: &str) -> String {
        let engine = engine().await;
        match engine.sql(sql).await {
            Err(e) => e.to_string(),
            Ok(df) => df.collect().await.expect_err("must fail").to_string(),
        }
    }

    #[tokio::test]
    async fn explode_in_the_select_list() {
        assert_eq!(
            rows("SELECT id, explode(arr) AS v FROM t ORDER BY id, v").await,
            ["id|v", "1|10", "1|20"]
        );
        // Unaliased, the column is Spark's `col`.
        assert_eq!(
            rows("SELECT explode(arr) FROM t ORDER BY 1").await,
            ["col", "10", "20"]
        );
        // The generator may sit anywhere in the list and other columns keep
        // their place around it.
        assert_eq!(
            rows("SELECT explode(arr) AS v, id FROM t WHERE id = 1 ORDER BY v").await,
            ["v|id", "10|1", "20|1"]
        );
    }

    #[tokio::test]
    async fn explode_outer_keeps_rows_with_nothing_to_explode() {
        assert_eq!(
            rows("SELECT id, explode_outer(arr) AS v FROM t ORDER BY id, v").await,
            ["id|v", "1|10", "1|20", "2|", "3|"]
        );
    }

    #[tokio::test]
    async fn posexplode_numbers_from_zero() {
        assert_eq!(
            rows("SELECT id, posexplode(arr) FROM t ORDER BY id, pos").await,
            ["id|pos|col", "1|0|10", "1|1|20"]
        );
        assert_eq!(
            rows("SELECT id, posexplode(arr) AS (p, c) FROM t ORDER BY id, p").await,
            ["id|p|c", "1|0|10", "1|1|20"]
        );
        assert_eq!(
            rows("SELECT id, posexplode_outer(arr) FROM t ORDER BY id, pos").await,
            ["id|pos|col", "1|0|10", "1|1|20", "2||", "3||"]
        );
    }

    #[tokio::test]
    async fn explode_of_a_map_yields_key_and_value() {
        assert_eq!(
            rows("SELECT id, explode(m) FROM s ORDER BY key").await,
            ["id|key|value", "1|k1|100", "1|k2|200"]
        );
    }

    #[tokio::test]
    async fn inline_names_columns_after_the_struct_fields() {
        assert_eq!(
            rows("SELECT id, inline(st) FROM s ORDER BY a").await,
            ["id|a|b", "1|1|x", "1|2|y"]
        );
        assert_eq!(
            rows("SELECT inline(st) AS (n, label) FROM s ORDER BY n").await,
            ["n|label", "1|x", "2|y"]
        );
    }

    #[tokio::test]
    async fn stack_lays_values_out_in_rows() {
        assert_eq!(
            rows("SELECT stack(2, 1, 'a', 2, 'b') ORDER BY col0").await,
            ["col0|col1", "1|a", "2|b"]
        );
        // An uneven count pads the last row with NULL.
        assert_eq!(
            rows("SELECT stack(2, 1, 2, 3) ORDER BY col0").await,
            ["col0|col1", "1|2", "3|"]
        );
        // Values may come from the row being expanded.
        assert_eq!(
            rows("SELECT id, stack(2, 'x', id, 'y', id * 10) AS (k, v) FROM t WHERE id = 1 ORDER BY k")
                .await,
            ["id|k|v", "1|x|1", "1|y|10"]
        );
        assert!(
            error("SELECT stack(id, 1, 2) FROM t")
                .await
                .contains("positive integer literal")
        );
    }

    #[tokio::test]
    async fn json_tuple_extracts_each_key() {
        assert_eq!(
            rows("SELECT id, json_tuple(j, 'a', 'b') FROM s").await,
            ["id|c0|c1", "1|1|two"]
        );
        assert_eq!(
            rows("SELECT json_tuple(j, 'a', 'missing') AS (a, gone) FROM s").await,
            ["a|gone", "1|"]
        );
    }

    #[tokio::test]
    async fn lateral_view_joins_each_row_to_its_elements() {
        assert_eq!(
            rows("SELECT id, v FROM t LATERAL VIEW explode(arr) x AS v ORDER BY id, v").await,
            ["id|v", "1|10", "1|20"]
        );
        // The view alias qualifies its columns, and WHERE may filter on them.
        assert_eq!(
            rows("SELECT t.id, x.v FROM t LATERAL VIEW explode(arr) x AS v WHERE x.v > 10").await,
            ["id|v", "1|20"]
        );
        assert_eq!(
            rows("SELECT id, v FROM t LATERAL VIEW OUTER explode(arr) x AS v ORDER BY id, v").await,
            ["id|v", "1|10", "1|20", "2|", "3|"]
        );
        assert_eq!(
            rows("SELECT id, p, c FROM t LATERAL VIEW posexplode(arr) x AS p, c ORDER BY p").await,
            ["id|p|c", "1|0|10", "1|1|20"]
        );
        assert_eq!(
            rows("SELECT id, a, b FROM s LATERAL VIEW inline(st) x AS a, b ORDER BY a").await,
            ["id|a|b", "1|1|x", "1|2|y"]
        );
    }

    #[tokio::test]
    async fn lateral_views_chain_and_aggregate() {
        // The second view reads the first's output.
        assert_eq!(
            rows(
                "SELECT id, v, w FROM t \
                 LATERAL VIEW explode(arr) x AS v \
                 LATERAL VIEW explode(make_array(v, v + 1)) y AS w \
                 ORDER BY v, w"
            )
            .await,
            ["id|v|w", "1|10|10", "1|10|11", "1|20|20", "1|20|21"]
        );
        assert_eq!(
            rows("SELECT id, sum(v) AS total FROM t LATERAL VIEW explode(arr) x AS v GROUP BY id")
                .await,
            ["id|total", "1|30"]
        );
    }

    #[tokio::test]
    async fn cross_join_unnest_reads_the_left_table() {
        assert_eq!(
            rows("SELECT t.id, u.v FROM t CROSS JOIN UNNEST(t.arr) AS u(v) ORDER BY 1, 2").await,
            ["id|v", "1|10", "1|20"]
        );
        assert_eq!(
            rows("SELECT t.id, u.v FROM t, UNNEST(t.arr) AS u(v) ORDER BY 1, 2").await,
            ["id|v", "1|10", "1|20"]
        );
        // A generated column may share a name with a left-hand one.
        assert_eq!(
            rows("SELECT t.id, u.id FROM t CROSS JOIN UNNEST(t.arr) AS u(id) ORDER BY 2").await,
            ["id|id", "1|10", "1|20"]
        );
        // Standing alone it is an ordinary table.
        assert_eq!(
            rows("SELECT * FROM UNNEST([1, 2, 3]) AS u(n) ORDER BY n").await,
            ["n", "1", "2", "3"]
        );
    }

    #[tokio::test]
    async fn generators_work_inside_subqueries_and_ctas() {
        assert_eq!(
            rows("SELECT max(v) AS top FROM (SELECT explode(arr) AS v FROM t) AS inner_query")
                .await,
            ["top", "20"]
        );
        assert_eq!(
            rows("WITH e AS (SELECT id, explode(arr) AS v FROM t) SELECT count(*) AS n FROM e")
                .await,
            ["n", "2"]
        );
    }

    #[tokio::test]
    async fn misuse_is_reported() {
        assert!(
            error("SELECT explode(arr), explode(arr) FROM t")
                .await
                .contains("only one generator")
        );
        assert!(
            error("SELECT posexplode(arr) AS (only_one) FROM t")
                .await
                .contains("alias")
        );
        assert!(
            error("SELECT inline(arr) FROM t")
                .await
                .contains("array of structs")
        );
    }

    /// A query with no generator must not be re-planned through this path.
    #[test]
    fn statements_without_generators_are_left_alone() {
        for sql in [
            "SELECT 1",
            "SELECT unnest(arr) FROM t",
            "SELECT 'explode me' AS note FROM t",
            "SELECT exploded FROM t",
        ] {
            assert!(
                super::rewrite_generator_statement(sql)
                    .expect("rewrite")
                    .is_none(),
                "{sql}"
            );
        }
    }
}
