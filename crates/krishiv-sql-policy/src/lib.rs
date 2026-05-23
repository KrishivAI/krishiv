#![forbid(unsafe_code)]

//! Policy-enforcing SQL engine: wraps [`krishiv_sql::SqlEngine`] with
//! authentication and column-masking.

use std::sync::Arc;
use std::fmt;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use krishiv_governance::{
    audit_log, AuditAction, AuditOutcome, AuthProvider, MaskingRule, PolicyHook, Principal,
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
    pub fn new(
        inner: SqlEngine,
        auth: Arc<dyn AuthProvider>,
        policy: Arc<dyn PolicyHook>,
    ) -> Self {
        Self {
            inner,
            auth,
            policy,
        }
    }

    /// Borrow the underlying [`SqlEngine`].
    pub fn inner(&self) -> &SqlEngine {
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
                        query_hash: &query_hash,
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

        let effective_sql = apply_row_predicates(query, principal, &table_names, self.policy.as_ref());
        let df = self.inner.sql(&effective_sql).await?;
        let batches = df.collect().await?;

        // Apply column masking
        let masked = batches
            .iter()
            .map(|batch| apply_masking(batch, principal, &table_names, self.policy.as_ref()))
            .collect::<SqlResult<Vec<_>>>()?;

        audit_log(
            principal.subject.as_str(),
            &AuditAction::QueryExecuted {
                query_hash: &query_hash,
            },
            AuditOutcome::Allowed,
        );
        Ok(masked)
    }
}

fn apply_row_predicates(
    query: &str,
    principal: &Principal,
    table_names: &[String],
    policy: &dyn PolicyHook,
) -> String {
    let mut preds: Vec<String> = table_names
        .iter()
        .filter_map(|t| policy.row_predicate(principal, t))
        .collect();
    if preds.is_empty() {
        return query.to_string();
    }
    if preds.len() == 1 {
        return format!("SELECT * FROM ({query}) AS __krishiv_rls WHERE {}", preds[0]);
    }
    format!(
        "SELECT * FROM ({query}) AS __krishiv_rls WHERE {}",
        preds.join(" AND ")
    )
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
                let redacted: StringArray = (0..batch.num_rows())
                    .map(|row| {
                        if col.is_null(row) {
                            None
                        } else {
                            Some("REDACTED")
                        }
                    })
                    .collect();
                fields.push(Field::new(field.name().clone(), DataType::Utf8, true));
                columns.push(Arc::new(redacted));
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

    let output_schema =
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
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

    table_names
        .iter()
        .find_map(|table| policy.column_masking_rule(principal, table, column_name))
}

#[cfg(test)]
mod policy_tests {
    use super::*;
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
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        let engine = make_engine_with_policy(Arc::new(RedactColumnPolicy {
            column: "id".into(),
        }));
        let p = engine.authenticate("key1").unwrap();
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        use arrow::array::Int64Array;
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        engine
            .inner()
            .register_record_batches("people", vec![batch])
            .await
            .unwrap();

        let batches = engine
            .execute_as(&p, "SELECT id FROM people")
            .await
            .unwrap();
        assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Utf8);
        let id_col = batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "REDACTED");
    }
}
