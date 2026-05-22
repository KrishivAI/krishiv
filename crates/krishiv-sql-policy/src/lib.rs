#![forbid(unsafe_code)]

//! Policy-enforcing SQL engine: wraps [`krishiv_sql::SqlEngine`] with
//! authentication and column-masking.

use std::sync::Arc;
use std::fmt;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use krishiv_governance::{AuthProvider, MaskingRule, PolicyHook, Principal};
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
        let table_names = referenced_table_names(query)?;
        for table_name in &table_names {
            if !self.policy.check_table_access(principal, table_name) {
                return Err(SqlError::AccessDenied {
                    reason: format!(
                        "principal '{}' denied access to table '{}'",
                        principal.subject, table_name
                    ),
                });
            }
        }

        // Execute query via inner engine
        let df = self.inner.sql(query).await?;
        let batches = df.collect().await?;

        // Apply column masking
        let masked = batches
            .iter()
            .map(|batch| apply_masking(batch, principal, &table_names, self.policy.as_ref()))
            .collect::<SqlResult<Vec<_>>>()?;

        Ok(masked)
    }
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
