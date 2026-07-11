//! Typed catalog, namespace, table, view, and function identifiers.

// Deliberate sync-over-async boundary module (Phase 51 async contract):
// block_on here bridges a synchronous public surface to the async core.
#![allow(clippy::disallowed_methods)]

use arrow::datatypes::SchemaRef;

use crate::{DataFrame, KrishivError, Result, Session};

/// A validated SQL identifier segment.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Identifier(String);

impl Identifier {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() || value.contains('\0') {
            return Err(KrishivError::InvalidConfig {
                message: "identifier must be non-empty and contain no NUL bytes".into(),
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn quoted(&self) -> String {
        format!("\"{}\"", self.0.replace('"', "\"\""))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Namespace {
    pub catalog: Option<Identifier>,
    pub name: Identifier,
}

impl Namespace {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        Ok(Self {
            catalog: None,
            name: Identifier::new(name)?,
        })
    }

    pub fn in_catalog(catalog: impl Into<String>, name: impl Into<String>) -> Result<Self> {
        Ok(Self {
            catalog: Some(Identifier::new(catalog)?),
            name: Identifier::new(name)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableIdentifier {
    pub namespace: Option<Namespace>,
    pub name: Identifier,
}

impl TableIdentifier {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        Ok(Self {
            namespace: None,
            name: Identifier::new(name)?,
        })
    }

    pub fn in_namespace(namespace: Namespace, name: impl Into<String>) -> Result<Self> {
        Ok(Self {
            namespace: Some(namespace),
            name: Identifier::new(name)?,
        })
    }

    pub fn quoted(&self) -> String {
        let mut parts = Vec::new();
        if let Some(namespace) = &self.namespace {
            if let Some(catalog) = &namespace.catalog {
                parts.push(catalog.quoted());
            }
            parts.push(namespace.name.quoted());
        }
        parts.push(self.name.quoted());
        parts.join(".")
    }
}

pub type ViewIdentifier = TableIdentifier;
pub type FunctionIdentifier = TableIdentifier;

#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub identifier: TableIdentifier,
    pub schema: SchemaRef,
    pub boundedness: crate::Boundedness,
}

impl Session {
    /// List tables as validated typed identifiers.
    pub fn list_table_identifiers(&self) -> Result<Vec<TableIdentifier>> {
        self.list_tables()?
            .into_iter()
            .map(TableIdentifier::new)
            .collect()
    }

    /// Resolve a typed table identifier lazily.
    pub fn table(&self, identifier: &TableIdentifier) -> Result<DataFrame> {
        krishiv_common::async_util::block_on(self.table_async(identifier))
    }

    /// Canonical async table resolution entry point.
    pub async fn table_async(&self, identifier: &TableIdentifier) -> Result<DataFrame> {
        self.sql_async(format!("SELECT * FROM {}", identifier.quoted()))
            .await
    }

    /// Resolve table schema and boundedness metadata without collecting rows.
    pub fn table_metadata(&self, identifier: &TableIdentifier) -> Result<TableMetadata> {
        let dataframe = self.table(identifier)?;
        Ok(TableMetadata {
            identifier: identifier.clone(),
            schema: dataframe.schema()?,
            boundedness: dataframe.boundedness(),
        })
    }

    /// Create a typed view from a SQL query in this session's catalog.
    pub fn create_temp_view(&self, identifier: &ViewIdentifier, query: &str) -> Result<()> {
        let dataframe = self.sql(format!("CREATE VIEW {} AS {query}", identifier.quoted()))?;
        let _ = dataframe.collect()?;
        Ok(())
    }

    /// Drop a typed table or view.
    pub fn drop_relation(&self, identifier: &TableIdentifier) -> Result<()> {
        let quoted = identifier.quoted();
        let drop_table_sql = format!("DROP TABLE {quoted}");
        match self.execute_ddl(&drop_table_sql) {
            Ok(()) => Ok(()),
            Err(table_error) => {
                let drop_view_sql = format!("DROP VIEW {quoted}");
                self.execute_ddl(&drop_view_sql)
                    .map_err(|view_error| KrishivError::Runtime {
                        message: format!(
                            "failed to drop table or view '{}': \
                             DROP TABLE failed with {table_error}; \
                             DROP VIEW failed with {view_error}",
                            identifier.quoted()
                        ),
                    })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionMetadata {
    pub identifier: FunctionIdentifier,
    pub kind: &'static str,
}

impl Session {
    /// Return the current typed catalog name.
    pub fn current_catalog(&self) -> Result<Identifier> {
        Identifier::new(
            self.get_config("krishiv.sql.catalog")
                .unwrap_or_else(|| "default".into()),
        )
    }

    /// Return the current typed namespace.
    pub fn current_namespace(&self) -> Result<Namespace> {
        Namespace::in_catalog(
            self.current_catalog()?.as_str(),
            self.get_config("krishiv.sql.namespace")
                .unwrap_or_else(|| "public".into()),
        )
    }

    /// Register a scalar function through a typed function identifier.
    pub fn register_function(
        &self,
        identifier: &FunctionIdentifier,
        udf: std::sync::Arc<dyn krishiv_plan::udf::ScalarUdf>,
    ) -> Result<()> {
        if identifier.namespace.is_some() {
            return Err(KrishivError::unsupported(
                "qualified function registration is not supported by the local UDF registry",
            ));
        }
        if udf.name() != identifier.name.as_str() {
            return Err(KrishivError::InvalidConfig {
                message: format!(
                    "function identifier '{}' does not match UDF name '{}'",
                    identifier.name.as_str(),
                    udf.name()
                ),
            });
        }
        self.register_scalar_udf(udf)
    }

    pub fn function_metadata(
        &self,
        identifier: &FunctionIdentifier,
    ) -> Result<Option<FunctionMetadata>> {
        let registry = self.udf_registry();
        let registry = registry.read().map_err(|_| KrishivError::Runtime {
            message: "UDF registry lock poisoned".into(),
        })?;
        Ok(registry
            .get_scalar(identifier.name.as_str())
            .map(|_| FunctionMetadata {
                identifier: identifier.clone(),
                kind: "scalar",
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_identifiers_quote_each_segment() {
        let namespace = Namespace::in_catalog("cat", "sales data").unwrap();
        let table = TableIdentifier::in_namespace(namespace, "orders\"").unwrap();
        assert_eq!(table.quoted(), "\"cat\".\"sales data\".\"orders\"\"\"");
        assert!(Identifier::new(" ").is_err());
        assert!(Identifier::new("bad\0name").is_err());
    }
}
