//! Translate Spark Connect `Relation` messages to Krishiv SQL.

use krishiv_proto::spark_connect::connect::{expression, join, read, relation, set_operation, Expression, LocalRelation, Read, Relation};

/// Translation failure for unsupported plan nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparkTranslateError {
    pub relation_kind: String,
    pub message: String,
}

impl std::fmt::Display for SparkTranslateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unsupported Spark relation '{}': {}", self.relation_kind, self.message)
    }
}

impl std::error::Error for SparkTranslateError {}

/// Convert a supported Spark Connect relation tree to SQL text.
pub fn relation_to_sql(rel: &Relation) -> Result<String, SparkTranslateError> {
    match rel.rel_type.as_ref() {
        Some(relation::RelType::Sql(sql)) => Ok(sql.query.clone()),
        Some(relation::RelType::LocalRelation(local)) => local_relation_sql(local),
        Some(relation::RelType::Read(read)) => read_relation_sql(read),
        Some(relation::RelType::Filter(f)) => {
            let input = relation_to_sql(f.input.as_ref().ok_or(missing("filter.input"))?)?;
            let cond = expr_to_sql(f.condition.as_ref().ok_or(missing("filter.condition"))?)?;
            Ok(format!("SELECT * FROM ({input}) WHERE {cond}"))
        }
        Some(relation::RelType::Project(p)) => {
            let input = relation_to_sql(p.input.as_ref().ok_or(missing("project.input"))?)?;
            let cols = p
                .expressions
                .iter()
                .map(expr_to_sql)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("SELECT {} FROM ({input})", cols.join(", ")))
        }
        Some(relation::RelType::Aggregate(a)) => {
            let input = relation_to_sql(a.input.as_ref().ok_or(missing("aggregate.input"))?)?;
            let group: Vec<_> = a
                .grouping_expressions
                .iter()
                .map(expr_to_sql)
                .collect::<Result<_, _>>()?;
            let aggs: Vec<_> = a
                .aggregate_expressions
                .iter()
                .map(expr_to_sql)
                .collect::<Result<_, _>>()?;
            let select = [group.clone(), aggs].concat();
            let mut sql = format!("SELECT {} FROM ({input})", select.join(", "));
            if !group.is_empty() {
                sql.push_str(&format!(" GROUP BY {}", group.join(", ")));
            }
            Ok(sql)
        }
        Some(relation::RelType::Sort(s)) => {
            let input = relation_to_sql(s.input.as_ref().ok_or(missing("sort.input"))?)?;
            let order: Vec<_> = s
                .order
                .iter()
                .map(sort_order_to_sql)
                .collect::<Result<_, _>>()?;
            Ok(format!("SELECT * FROM ({input}) ORDER BY {}", order.join(", ")))
        }
        Some(relation::RelType::Limit(l)) => {
            let input = relation_to_sql(l.input.as_ref().ok_or(missing("limit.input"))?)?;
            Ok(format!("SELECT * FROM ({input}) LIMIT {}", l.limit))
        }
        Some(relation::RelType::Join(j)) => {
            let left = relation_to_sql(j.left.as_ref().ok_or(missing("join.left"))?)?;
            let right = relation_to_sql(j.right.as_ref().ok_or(missing("join.right"))?)?;
            let join_type = join_type_sql(j.join_type());
            let on = if !j.using_columns.is_empty() {
                let cols = j.using_columns.join(", ");
                format!("USING ({cols})")
            } else {
                let cond = expr_to_sql(j.join_condition.as_ref().ok_or(missing("join.condition"))?)?;
                format!("ON {cond}")
            };
            Ok(format!(
                "SELECT * FROM ({left}) {join_type} JOIN ({right}) {on}"
            ))
        }
        Some(relation::RelType::SetOp(set)) => {
            let left = relation_to_sql(set.left_input.as_ref().ok_or(missing("set.left"))?)?;
            let right = relation_to_sql(set.right_input.as_ref().ok_or(missing("set.right"))?)?;
            let op = match set.set_op_type() {
                set_operation::SetOpType::Union => "UNION",
                set_operation::SetOpType::Intersect => "INTERSECT",
                set_operation::SetOpType::Except => "EXCEPT",
                _ => {
                    return Err(unsupported("set_op", "set operation type not specified"));
                }
            };
            let all = if set.is_all.unwrap_or(false) { " ALL" } else { "" };
            Ok(format!("{left} {op}{all} {right}"))
        }
        Some(relation::RelType::SubqueryAlias(a)) => {
            let input = relation_to_sql(a.input.as_ref().ok_or(missing("alias.input"))?)?;
            Ok(format!("({input}) AS {}", quote_ident(&a.alias)))
        }
        Some(relation::RelType::Deduplicate(d)) => {
            let input = relation_to_sql(d.input.as_ref().ok_or(missing("deduplicate.input"))?)?;
            if d.all_columns_as_keys.unwrap_or(false) {
                Ok(format!("SELECT DISTINCT * FROM ({input})"))
            } else if !d.column_names.is_empty() {
                let cols = d.column_names.join(", ");
                Ok(format!("SELECT DISTINCT {cols} FROM ({input})"))
            } else {
                Ok(format!("SELECT DISTINCT * FROM ({input})"))
            }
        }
        Some(other) => Err(unsupported(
            "relation",
            &format!("{:?}", std::mem::discriminant(other)),
        )),
        None => Err(unsupported("relation", "missing rel_type")),
    }
}

fn read_relation_sql(
    read: &Read,
) -> Result<String, SparkTranslateError> {
    if let Some(read::ReadType::NamedTable(t)) = read.read_type.as_ref() {
        return Ok(format!("SELECT * FROM {}", quote_ident(&t.unparsed_identifier)));
    }
    if let Some(read::ReadType::DataSource(ds)) = read.read_type.as_ref()
        && let Some(path) = ds.paths.first()
    {
            let format = ds.format.as_deref().unwrap_or("parquet");
            return Ok(format!(
                "SELECT * FROM {} '{}'",
                format.to_uppercase(),
                path.replace('\'', "''")
            ));
    }
    Err(unsupported("read", "named table or parquet path required"))
}

fn local_relation_sql(
    local: &LocalRelation,
) -> Result<String, SparkTranslateError> {
    if local.data.as_ref().is_none_or(|d| d.is_empty()) {
        return Err(unsupported("local_relation", "empty inline data"));
    }
    // Inline Arrow IPC — use VALUES fallback when schema is simple; tests use SQL relation.
    Err(unsupported(
        "local_relation",
        "use SQL relation or register table via Read",
    ))
}

fn join_type_sql(jt: join::JoinType) -> &'static str {
    match jt {
        join::JoinType::LeftOuter => "LEFT",
        join::JoinType::RightOuter => "RIGHT",
        join::JoinType::FullOuter => "FULL OUTER",
        join::JoinType::LeftSemi => "LEFT SEMI",
        join::JoinType::LeftAnti => "LEFT ANTI",
        join::JoinType::Cross => "CROSS",
        _ => "INNER",
    }
}

fn sort_order_to_sql(so: &expression::SortOrder) -> Result<String, SparkTranslateError> {
    let child = expr_to_sql(so.child.as_ref().ok_or(missing("sort.child"))?)?;
    use expression::sort_order::SortDirection;
    let dir = if so.direction == SortDirection::Descending as i32 {
        "DESC"
    } else {
        "ASC"
    };
    Ok(format!("{child} {dir}"))
}

fn expr_to_sql(expr: &Expression) -> Result<String, SparkTranslateError> {
    use krishiv_proto::spark_connect::connect::expression;
    match expr.expr_type.as_ref() {
        Some(expression::ExprType::Literal(lit)) => literal_to_sql(lit),
        Some(expression::ExprType::UnresolvedAttribute(attr)) => {
            Ok(quote_ident(&attr.unparsed_identifier))
        }
        Some(expression::ExprType::Alias(a)) => {
            let inner = expr_to_sql(a.expr.as_ref().ok_or(missing("alias.expr"))?)?;
            Ok(format!("{inner} AS {}", a.name.iter().map(|n| quote_ident(n)).collect::<Vec<_>>().join(".")))
        }
        Some(expression::ExprType::Cast(c)) => {
            let inner = expr_to_sql(c.expr.as_ref().ok_or(missing("cast.expr"))?)?;
            Ok(format!("CAST({inner} AS STRING)"))
        }
        Some(expression::ExprType::UnresolvedFunction(f)) => {
            let args: Vec<_> = f
                .arguments
                .iter()
                .map(expr_to_sql)
                .collect::<Result<_, _>>()?;
            Ok(format!("{}({})", f.function_name, args.join(", ")))
        }
        Some(other) => Err(unsupported(
            "expression",
            &format!("{:?}", std::mem::discriminant(other)),
        )),
        None => Err(unsupported("expression", "missing expr_type")),
    }
}

fn literal_to_sql(
    lit: &expression::Literal,
) -> Result<String, SparkTranslateError> {
    use krishiv_proto::spark_connect::connect::expression::literal;
    match lit.literal_type.as_ref() {
        Some(literal::LiteralType::String(s)) => Ok(format!("'{}'", s.replace('\'', "''"))),
        Some(literal::LiteralType::Integer(i)) => Ok(i.to_string()),
        Some(literal::LiteralType::Long(l)) => Ok(l.to_string()),
        Some(literal::LiteralType::Double(d)) => Ok(d.to_string()),
        Some(literal::LiteralType::Boolean(b)) => Ok(if *b { "TRUE" } else { "FALSE" }.into()),
        Some(literal::LiteralType::Null(_)) => Ok("NULL".into()),
        _ => Err(unsupported("literal", "unsupported literal type")),
    }
}

fn quote_ident(name: &str) -> String {
    if name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.is_empty()
    {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

fn missing(field: &str) -> SparkTranslateError {
    SparkTranslateError {
        relation_kind: "relation".into(),
        message: format!("missing field {field}"),
    }
}

fn unsupported(kind: &str, msg: &str) -> SparkTranslateError {
    SparkTranslateError {
        relation_kind: kind.into(),
        message: msg.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_proto::spark_connect::connect::{relation, Relation};

    #[test]
    fn sql_relation_round_trip() {
        let rel = Relation {
            rel_type: Some(relation::RelType::Sql(krishiv_proto::spark_connect::connect::Sql {
                query: "SELECT 1 AS n".into(),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(relation_to_sql(&rel).unwrap(), "SELECT 1 AS n");
    }
}
