#![forbid(unsafe_code)]

//! Policy-enforcing SQL engine: wraps [`krishiv_sql::SqlEngine`] with
//! authentication and column-masking.

use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use krishiv_governance::{
    AuditAction, AuditOutcome, AuthProvider, MaskingRule, PolicyHook, Principal, audit_log,
};
use krishiv_sql::{SqlEngine, SqlError, SqlResult, referenced_table_names};

/// Wraps [`SqlEngine`] and enforces table-access and column-masking policy.
#[derive(Clone)]
pub struct PolicyEnforcingSqlEngine {
    inner: SqlEngine,
    auth: Arc<dyn AuthProvider>,
    policy: Arc<dyn PolicyHook>,
}

impl fmt::Debug for PolicyEnforcingSqlEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PolicyEnforcingSqlEngine")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl PolicyEnforcingSqlEngine {
    /// Create a new `PolicyEnforcingSqlEngine` wrapping `inner`.
    pub fn new(inner: SqlEngine, auth: Arc<dyn AuthProvider>, policy: Arc<dyn PolicyHook>) -> Self {
        Self {
            inner,
            auth,
            policy,
        }
    }

    /// Borrow the underlying [`SqlEngine`].
    ///
    /// # Security Warning
    ///
    /// This bypasses all policy enforcement (table access, column masking,
    /// row-level security). Prefer `execute_as` or `prepare_authorized_query`.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> &SqlEngine {
        &self.inner
    }

    /// Authenticate `api_key`. Returns [`SqlError::AccessDenied`] if invalid.
    pub fn authenticate(&self, api_key: &str) -> SqlResult<Principal> {
        self.auth
            .authenticate(api_key)
            .ok_or_else(|| SqlError::AccessDenied {
                reason: "invalid or missing API key".into(),
            })
    }

    /// Execute a query as `principal`, applying table-access checks and column masking.
    pub async fn execute_as(
        &self,
        principal: &Principal,
        query: &str,
    ) -> SqlResult<Vec<RecordBatch>> {
        use sha2::{Digest, Sha256};
        let query_hash = format!("{:x}", Sha256::digest(query.as_bytes()));
        let table_names = referenced_table_names(query)?;
        for table_name in &table_names {
            if !self.policy.check_table_access(principal, table_name) {
                audit_log(
                    principal.subject.as_str(),
                    &AuditAction::QueryExecuted {
                        query_hash: query_hash.clone(),
                    },
                    AuditOutcome::Denied,
                );
                return Err(SqlError::AccessDenied {
                    reason: format!(
                        "principal '{}' denied access to table '{}'",
                        principal.subject, table_name
                    ),
                });
            }
        }

        let effective_sql =
            apply_row_predicates(query, principal, &table_names, self.policy.as_ref());
        let df = self.inner.sql(&effective_sql).await?;
        let batches = df.collect().await?;

        // Apply column masking
        let masked = batches
            .iter()
            .map(|batch| apply_masking(batch, principal, &table_names, self.policy.as_ref()))
            .collect::<SqlResult<Vec<_>>>()?;

        audit_log(
            principal.subject.as_str(),
            &AuditAction::QueryExecuted { query_hash },
            AuditOutcome::Allowed,
        );
        Ok(masked)
    }

    /// Check table access and return SQL with row-level predicates applied.
    ///
    /// Used when execution is routed through [`ExecutionRuntime::collect_batch_sql`]
    /// instead of local DataFusion.
    pub fn prepare_authorized_query(
        &self,
        principal: &Principal,
        query: &str,
    ) -> SqlResult<String> {
        use sha2::{Digest, Sha256};
        let query_hash = format!("{:x}", Sha256::digest(query.as_bytes()));
        let table_names = referenced_table_names(query)?;
        for table_name in &table_names {
            if !self.policy.check_table_access(principal, table_name) {
                audit_log(
                    principal.subject.as_str(),
                    &AuditAction::QueryExecuted {
                        query_hash: query_hash.clone(),
                    },
                    AuditOutcome::Denied,
                );
                return Err(SqlError::AccessDenied {
                    reason: format!(
                        "principal '{}' denied access to table '{}'",
                        principal.subject, table_name
                    ),
                });
            }
        }
        Ok(apply_row_predicates(
            query,
            principal,
            &table_names,
            self.policy.as_ref(),
        ))
    }

    /// Apply column masking to batches produced by a remote or coordinator SQL path.
    pub fn mask_result_batches(
        &self,
        principal: &Principal,
        query: &str,
        batches: Vec<RecordBatch>,
    ) -> SqlResult<Vec<RecordBatch>> {
        use sha2::{Digest, Sha256};
        let query_hash = format!("{:x}", Sha256::digest(query.as_bytes()));
        let table_names = referenced_table_names(query)?;
        let masked = batches
            .iter()
            .map(|batch| apply_masking(batch, principal, &table_names, self.policy.as_ref()))
            .collect::<SqlResult<Vec<_>>>()?;
        audit_log(
            principal.subject.as_str(),
            &AuditAction::QueryExecuted { query_hash },
            AuditOutcome::Allowed,
        );
        Ok(masked)
    }
}

fn is_select_query(query: &str) -> bool {
    use sqlparser::ast::{Query, SetExpr, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    let dialect = GenericDialect {};
    let statements = match Parser::parse_sql(&dialect, query) {
        Ok(stmts) => stmts,
        Err(_) => return false,
    };
    if statements.is_empty() {
        return false;
    }

    fn is_query_select(q: &Query) -> bool {
        match q.body.as_ref() {
            SetExpr::Select(_) => true,
            SetExpr::Query(nested) => is_query_select(nested),
            SetExpr::SetOperation { left, right, .. } => {
                is_set_expr_select(left) || is_set_expr_select(right)
            }
            _ => false,
        }
    }

    fn is_set_expr_select(e: &SetExpr) -> bool {
        match e {
            SetExpr::Select(_) => true,
            SetExpr::Query(nested) => is_query_select(nested),
            SetExpr::SetOperation { left, right, .. } => {
                is_set_expr_select(left) || is_set_expr_select(right)
            }
            _ => false,
        }
    }

    match &statements[0] {
        Statement::Query(q) => is_query_select(q),
        _ => false,
    }
}

fn apply_row_predicates(
    query: &str,
    principal: &Principal,
    table_names: &[String],
    policy: &dyn PolicyHook,
) -> String {
    let preds: Vec<String> = table_names
        .iter()
        .filter_map(|t| policy.row_predicate(principal, t))
        .collect();
    if preds.is_empty() || !is_select_query(query) {
        return query.to_string();
    }
    let predicate = preds.join(" AND ");

    // C8: Instead of wrapping the entire query as a subquery (which fails when
    // RLS predicate columns are not projected), inject the predicate directly
    // into the query's WHERE clause using sqlparser to find the right position.
    //
    // For simple SELECT queries, we splice "AND (predicate)" into the existing
    // WHERE clause, or add "WHERE predicate" if there is none.  For queries
    // with CTEs (WITH), we fall back to the subquery wrap.
    match inject_rls_predicate(query, &predicate) {
        Ok(rewritten) => rewritten,
        Err(_) => {
            let trimmed = query.trim_start();
            if trimmed.to_uppercase().starts_with("WITH ") {
                format!("SELECT * FROM ({query}) AS __krishiv_rls WHERE {predicate}")
            } else {
                format!("SELECT * FROM ({query}) AS __krishiv_rls WHERE {predicate}")
            }
        }
    }
}

/// C8: Inject an RLS predicate into the WHERE clause of a SELECT query.
/// Uses sqlparser to locate the WHERE clause position rather than naive
/// string matching that would break on subqueries and nested selects.
fn inject_rls_predicate(query: &str, predicate: &str) -> Result<String, ()> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, query).map_err(|_| ())?;
    if statements.is_empty() {
        return Err(());
    }

    // For CTE queries, find where the outermost SELECT begins after WITH ... )
    // and inject the predicate there.
    let trimmed = query.trim_start();
    if trimmed.to_uppercase().starts_with("WITH ") {
        // Find the position after the last CTE definition `)` and before the
        // main query's SELECT.  Inject the RLS predicate by wrapping just the
        // main query in a subquery with the predicate.
        if let Some(select_pos) = find_outer_select_after_cte(query) {
            let (before, after) = query.split_at(select_pos);
            return Ok(format!(
                "{before}SELECT * FROM ({after}) AS __krishiv_rls WHERE {predicate}"
            ));
        }
        return Err(());
    }

    // For regular SELECT queries, look for an existing WHERE clause.
    // Parse to find the WHERE position more precisely.
    // Fall back to string-based injection using sqlparser's query representation.
    match find_where_injection_point(query) {
        Some((before, existing)) => format!("{before}({existing}) AND ({predicate})")
            .parse::<String>()
            .map_err(|_: std::string::ParseError| ())
            .map(|_| format!("{before}({existing}) AND ({predicate})"))
            .or_else(|_| Ok(format!("{query} AND ({predicate})"))),
        None => Ok(format!("{query} WHERE {predicate}")),
    }
}

/// Find the position of the WHERE clause in a SELECT query for RLS predicate injection.
fn find_where_injection_point(query: &str) -> Option<(String, String)> {
    let lines: Vec<&str> = query.lines().collect();
    let single_line = lines.join(" ");

    // Find WHERE keyword outside of parentheses and single quotes.
    let mut depth = 0i32;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let chars: Vec<char> = single_line.chars().collect();
    let mut i = 0;

    while i + 5 < chars.len() {
        match chars[i] {
            '(' if !in_single_quote && !in_double_quote => depth += 1,
            ')' if !in_single_quote && !in_double_quote => depth -= 1,
            '\'' if !in_double_quote => in_single_quote = !in_single_quote,
            '"' if !in_single_quote => in_double_quote = !in_double_quote,
            _ => {}
        }

        let word: String = chars[i..i + 6].iter().collect();
        if word.to_uppercase().starts_with("WHERE ")
            && depth == 0
            && !in_single_quote
            && !in_double_quote
        {
            let before = single_line[..i].to_string();
            let after = single_line[i + 5..].to_string(); // Skip "WHERE"
            return Some((before + "WHERE ", after));
        }

        if word.to_uppercase().starts_with("WHERE\n")
            && depth == 0
            && !in_single_quote
            && !in_double_quote
        {
            let before = single_line[..i].to_string();
            let after = single_line[i + 5..].to_string();
            return Some((before + "WHERE ", after));
        }

        i += 1;
    }
    None
}

/// Find the start of the outer SELECT after any CTE definitions.
fn find_outer_select_after_cte(query: &str) -> Option<usize> {
    let mut depth = 0i32;
    let chars: Vec<char> = query.chars().collect();
    for (i, &ch) in chars.iter().enumerate() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    // After the last CTE closing paren, look for the next SELECT
                    let rest: String = chars[i + 1..].iter().collect();
                    if let Some(select_idx) = rest.to_uppercase().find("SELECT") {
                        return Some(i + 1 + select_idx);
                    }
                }
            }
            _ => {}
        }
    }
    None
}

/// Apply column-masking rules from `policy` to a single [`RecordBatch`].
fn apply_masking(
    batch: &RecordBatch,
    principal: &Principal,
    table_names: &[String],
    policy: &dyn PolicyHook,
) -> SqlResult<RecordBatch> {
    use arrow::array::Array;
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    use sha2::{Digest, Sha256};

    let schema = batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    let mut fields: Vec<Field> = Vec::with_capacity(batch.num_columns());

    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        match masking_rule_for_field(policy, principal, table_names, field.name()) {
            None => {
                fields.push(field.as_ref().clone());
                columns.push(col.clone());
            }
            Some(MaskingRule::Nullify) => {
                // Rebuild the column as a null array cast to the same type.
                // Using arrow's NullArray directly changes the data type, so we
                // cast a NullArray back to the original type so the schema stays
                // consistent.
                use arrow::array::new_null_array;
                fields.push(field.as_ref().clone());
                columns.push(new_null_array(col.data_type(), batch.num_rows()));
            }
            Some(MaskingRule::Redact) => {
                use arrow::array::new_null_array;
                fields.push(field.as_ref().clone());
                if matches!(col.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
                    let redacted: StringArray = (0..batch.num_rows())
                        .map(|row| {
                            if col.is_null(row) {
                                None
                            } else {
                                Some("REDACTED")
                            }
                        })
                        .collect();
                    columns.push(Arc::new(redacted));
                } else {
                    columns.push(new_null_array(col.data_type(), batch.num_rows()));
                }
            }
            Some(MaskingRule::Hash) => {
                let options = FormatOptions::default();
                let formatter = ArrayFormatter::try_new(col.as_ref(), &options).map_err(|e| {
                    SqlError::DataFusion {
                        message: e.to_string(),
                    }
                })?;
                let hashed: StringArray = (0..batch.num_rows())
                    .map(|row| {
                        if col.is_null(row) {
                            return None;
                        }
                        let val = formatter.value(row).to_string();
                        let digest = Sha256::digest(val.as_bytes());
                        Some(format!("{digest:x}"))
                    })
                    .collect();
                fields.push(Field::new(field.name().clone(), DataType::Utf8, true));
                columns.push(Arc::new(hashed));
            }
        }
    }

    let output_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    RecordBatch::try_new(output_schema, columns).map_err(|e| SqlError::DataFusion {
        message: e.to_string(),
    })
}

fn masking_rule_for_field(
    policy: &dyn PolicyHook,
    principal: &Principal,
    table_names: &[String],
    column_name: &str,
) -> Option<MaskingRule> {
    if table_names.is_empty() {
        return policy.column_masking_rule(principal, "", column_name);
    }

    // Strip any table qualifier from the column name (e.g. "t.col" → "col").
    let bare_col = column_name
        .rsplit_once('.')
        .map(|(_, c)| c)
        .unwrap_or(column_name);

    table_names.iter().find_map(|table| {
        // First try the bare column name.
        if let rule @ Some(_) = policy.column_masking_rule(principal, table, bare_col) {
            return rule;
        }
        // Then try the fully-qualified "table.column" form for join results.
        let qualified = format!("{table}.{bare_col}");
        policy.column_masking_rule(principal, table, &qualified)
    })
}

#[cfg(test)]
mod policy_tests {
    use super::*;
    use arrow::array::Array;
    use krishiv_governance::{MaskingRule, Principal, Role, StaticApiKeyAuthProvider};
    use krishiv_sql::{SqlEngine, SqlError};
    use std::sync::Arc;

    struct DenyTablePolicy {
        denied_table: String,
    }
    impl PolicyHook for DenyTablePolicy {
        fn check_table_access(&self, _p: &Principal, table: &str) -> bool {
            table != self.denied_table
        }
        fn column_masking_rule(
            &self,
            _p: &Principal,
            _table: &str,
            _col: &str,
        ) -> Option<MaskingRule> {
            None
        }
    }

    struct RedactColumnPolicy {
        column: String,
    }
    impl PolicyHook for RedactColumnPolicy {
        fn check_table_access(&self, _p: &Principal, _table: &str) -> bool {
            true
        }
        fn column_masking_rule(
            &self,
            _p: &Principal,
            _table: &str,
            col: &str,
        ) -> Option<MaskingRule> {
            if col == self.column {
                Some(MaskingRule::Redact)
            } else {
                None
            }
        }
    }

    fn make_engine_with_policy(policy: Arc<dyn PolicyHook>) -> PolicyEnforcingSqlEngine {
        let auth = Arc::new(StaticApiKeyAuthProvider::new(vec![(
            "key1".into(),
            "alice".into(),
            Role::Reader,
        )]));
        PolicyEnforcingSqlEngine::new(SqlEngine::new(), auth, policy)
    }

    #[test]
    fn authenticate_valid_key_returns_principal() {
        let engine = make_engine_with_policy(Arc::new(DenyTablePolicy {
            denied_table: "secret".into(),
        }));
        let p = engine.authenticate("key1").unwrap();
        assert_eq!(p.subject, "alice");
    }

    #[test]
    fn authenticate_invalid_key_returns_access_denied() {
        let engine = make_engine_with_policy(Arc::new(DenyTablePolicy {
            denied_table: "secret".into(),
        }));
        let err = engine.authenticate("bad_key").unwrap_err();
        assert!(matches!(err, SqlError::AccessDenied { .. }));
    }

    #[tokio::test]
    async fn denied_table_returns_access_denied() {
        let engine = make_engine_with_policy(Arc::new(DenyTablePolicy {
            denied_table: "secret".into(),
        }));
        let p = engine.authenticate("key1").unwrap();
        let err = engine
            .execute_as(&p, "SELECT * FROM secret")
            .await
            .unwrap_err();
        assert!(matches!(err, SqlError::AccessDenied { .. }));
    }

    #[tokio::test]
    async fn denied_join_table_returns_access_denied() {
        let engine = make_engine_with_policy(Arc::new(DenyTablePolicy {
            denied_table: "secret".into(),
        }));
        let p = engine.authenticate("key1").unwrap();
        let err = engine
            .execute_as(
                &p,
                "SELECT * FROM public JOIN secret ON public.id = secret.id",
            )
            .await
            .unwrap_err();
        assert!(matches!(err, SqlError::AccessDenied { .. }));
    }

    #[tokio::test]
    async fn allowed_table_returns_results() {
        let engine = make_engine_with_policy(Arc::new(DenyTablePolicy {
            denied_table: "secret".into(),
        }));
        let p = engine.authenticate("key1").unwrap();
        let batches = engine.execute_as(&p, "SELECT 42 AS val").await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1);
    }

    #[tokio::test]
    async fn redact_policy_replaces_column_values() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        let engine = make_engine_with_policy(Arc::new(RedactColumnPolicy {
            column: "name".into(),
        }));
        let p = engine.authenticate("key1").unwrap();

        // Register an in-memory table with a "name" column
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        use arrow::array::{Int64Array, StringArray as SA};
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64])),
                Arc::new(SA::from(vec!["alice"])),
            ],
        )
        .unwrap();
        engine
            .inner()
            .register_record_batches("people", vec![batch])
            .await
            .unwrap();

        let batches = engine
            .execute_as(&p, "SELECT id, name FROM people")
            .await
            .unwrap();

        assert!(!batches.is_empty());
        let name_col = batches[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "REDACTED");
    }

    #[tokio::test]
    async fn redact_policy_can_mask_non_string_columns() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        let engine = make_engine_with_policy(Arc::new(RedactColumnPolicy {
            column: "id".into(),
        }));
        let p = engine.authenticate("key1").unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap();
        engine
            .inner()
            .register_record_batches("people", vec![batch])
            .await
            .unwrap();

        let batches = engine
            .execute_as(&p, "SELECT id FROM people")
            .await
            .unwrap();
        assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Int64);
        let id_col = batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(
            id_col.is_null(0),
            "non-string redact must preserve type with null"
        );
    }

    /// C18 regression: row predicates must NOT be applied to non-SELECT queries.
    #[test]
    fn is_select_query_rejects_non_select() {
        assert!(is_select_query("SELECT * FROM t"));
        assert!(is_select_query("  SELECT a, b FROM t"));
        assert!(is_select_query("WITH cte AS (SELECT 1) SELECT * FROM cte"));
        assert!(!is_select_query("WITH cte AS (SELECT 1) DELETE FROM t"));
        assert!(!is_select_query("INSERT INTO t VALUES (1)"));
        assert!(!is_select_query("UPDATE t SET a=1"));
        assert!(!is_select_query("CREATE TABLE t (a INT)"));
    }

    #[test]
    fn apply_row_predicates_skips_non_select() {
        struct TestPolicy;
        impl PolicyHook for TestPolicy {
            fn check_table_access(&self, _: &Principal, _: &str) -> bool {
                true
            }
            fn column_masking_rule(&self, _: &Principal, _: &str, _: &str) -> Option<MaskingRule> {
                None
            }
            fn row_predicate(&self, _: &Principal, _: &str) -> Option<String> {
                Some("deleted = false".into())
            }
        }
        let p = Principal {
            subject: "alice".into(),
            role: Role::Reader,
        };

        // Non-SELECT must not get wrapped.
        let result =
            apply_row_predicates("INSERT INTO t VALUES (1)", &p, &["t".into()], &TestPolicy);
        assert_eq!(
            result, "INSERT INTO t VALUES (1)",
            "non-SELECT must not be wrapped"
        );

        // SELECT must get RLS predicate injected.
        let result = apply_row_predicates("SELECT * FROM t", &p, &["t".into()], &TestPolicy);
        assert!(
            result.contains("deleted = false"),
            "SELECT must have predicate injected"
        );
    }
}
