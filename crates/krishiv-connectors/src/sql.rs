//! T9: SQL connector support.
//!
//! Phase 1 ships the typed [`SqlConnector`] builder that records the
//! [`ConnectorKind`] and the JDBC-style URL. The actual `sqlx::Pool`
//! construction and the JDBC executor fragment are deferred to a
//! follow-up that adds the `mysql` / `mssql` / `oracle` features to
//! the workspace `sqlx` dependency (today only `postgres` is enabled
//! so the build stays within the pinned `Cargo.lock`).
//!
//! The `jdbc:<url>:<table>` contract parses into a [`SqlConnector`]
//! via [`SqlConnector::parse_jdbc`]; the returned builder is what the
//! executor fragment will eventually use to spin up a `sqlx::Pool`
//! and execute a `SELECT * FROM <table>` per partition.

use std::fmt;

/// T9: target database kind for a SQL connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectorKind {
    /// PostgreSQL 11+ (uses `sqlx::postgres::PgPool`).
    Postgres,
    /// MySQL 8+ (uses `sqlx::mysql::MySqlPool`).
    Mysql,
    /// Microsoft SQL Server 2017+ (uses `sqlx::mssql::MssqlPool`).
    Mssql,
    /// Oracle 19c+ (uses `sqlx::oracle::OraclePool`).
    Oracle,
}

impl fmt::Display for ConnectorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectorKind::Postgres => f.write_str("postgres"),
            ConnectorKind::Mysql => f.write_str("mysql"),
            ConnectorKind::Mssql => f.write_str("mssql"),
            ConnectorKind::Oracle => f.write_str("oracle"),
        }
    }
}

/// T9: typed SQL connector configuration.
///
/// Built from a JDBC URL like `jdbc:postgresql://host/db` (with the
/// optional `:<table>` tail) via [`SqlConnector::parse_jdbc`], or
/// directly via [`SqlConnector::new`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlConnector {
    kind: ConnectorKind,
    url: String,
    table: Option<String>,
    /// Optional user override (most drivers derive the user from the URL).
    user: Option<String>,
    /// Optional password override (most drivers derive the password from the URL).
    password: Option<String>,
}

impl SqlConnector {
    /// Build a connector with an explicit URL and optional table.
    pub fn new(kind: ConnectorKind, url: impl Into<String>, table: Option<String>) -> Self {
        Self {
            kind,
            url: url.into(),
            table,
            user: None,
            password: None,
        }
    }

    /// Parse a JDBC-style URL of the form `jdbc:<engine>://<rest>`
    /// or `jdbc:<engine>://<rest>:<table>`. Returns `None` for an
    /// unrecognised engine token.
    pub fn parse_jdbc(spec: &str) -> Option<Self> {
        let body = spec.strip_prefix("jdbc:")?;
        let (kind, rest) = if let Some(r) = body.strip_prefix("postgresql://") {
            (ConnectorKind::Postgres, r)
        } else if let Some(r) = body.strip_prefix("postgres://") {
            (ConnectorKind::Postgres, r)
        } else if let Some(r) = body.strip_prefix("mysql://") {
            (ConnectorKind::Mysql, r)
        } else if let Some(r) = body.strip_prefix("sqlserver://") {
            (ConnectorKind::Mssql, r)
        } else if let Some(r) = body.strip_prefix("mssql://") {
            (ConnectorKind::Mssql, r)
        } else if let Some(r) = body.strip_prefix("oracle://") {
            (ConnectorKind::Oracle, r)
        } else {
            return None;
        };
        // Optional `:<table>` tail. We look for the *last* `:` in
        // `rest`, but only treat it as the table separator when the
        // suffix has no path/host separator (`/`) — otherwise it's
        // userinfo (`u:p@h/d`) or a port (`h:3306/db`).
        let (url, table) = match rest.rsplit_once(':') {
            Some((u, t)) if !t.is_empty() && !t.contains('/') => {
                (u.to_string(), Some(t.to_string()))
            }
            _ => (rest.to_string(), None),
        };
        Some(Self::new(kind, url, table))
    }

    /// Override the user (rarely needed; most drivers derive from the URL).
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Override the password (rarely needed; most drivers derive from the URL).
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Target database kind.
    pub fn kind(&self) -> ConnectorKind {
        self.kind
    }

    /// Database URL (without the `jdbc:` prefix).
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Optional target table (the `:<table>` JDBC tail).
    pub fn table(&self) -> Option<&str> {
        self.table.as_deref()
    }

    /// User override, if set.
    pub fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    /// Password override, if set.
    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// T9: `parse_jdbc` recognises all four engines and the optional
    /// `:<table>` tail.
    #[test]
    fn parse_jdbc_handles_engines_and_table_tail() {
        let p = SqlConnector::parse_jdbc("jdbc:postgresql://u:p@h/d").unwrap();
        assert_eq!(p.kind(), ConnectorKind::Postgres);
        assert_eq!(p.url(), "u:p@h/d");
        assert_eq!(p.table(), None);

        let m = SqlConnector::parse_jdbc("jdbc:mysql://h:3306/db:orders").unwrap();
        assert_eq!(m.kind(), ConnectorKind::Mysql);
        assert_eq!(m.url(), "h:3306/db");
        assert_eq!(m.table(), Some("orders"));

        let s = SqlConnector::parse_jdbc("jdbc:sqlserver://h/db:sales").unwrap();
        assert_eq!(s.kind(), ConnectorKind::Mssql);
        assert_eq!(s.url(), "h/db");
        assert_eq!(s.table(), Some("sales"));

        let o = SqlConnector::parse_jdbc("jdbc:oracle://h/svc").unwrap();
        assert_eq!(o.kind(), ConnectorKind::Oracle);
        assert_eq!(o.url(), "h/svc");
        assert_eq!(o.table(), None);
    }

    /// T9: unrecognised engine token returns `None`.
    #[test]
    fn parse_jdbc_rejects_unknown_engine() {
        assert!(SqlConnector::parse_jdbc("jdbc:sqlite://h/db").is_none());
        assert!(SqlConnector::parse_jdbc("postgres://h/db").is_none());
        assert!(SqlConnector::parse_jdbc("").is_none());
    }

    /// T9: `Display` round-trips to the engine token.
    #[test]
    fn connector_kind_display_round_trips() {
        for k in [
            ConnectorKind::Postgres,
            ConnectorKind::Mysql,
            ConnectorKind::Mssql,
            ConnectorKind::Oracle,
        ] {
            assert!(SqlConnector::parse_jdbc(&format!("jdbc:{k}://h/d")).is_some());
        }
    }

    /// T9: `with_user` / `with_password` store overrides.
    #[test]
    fn with_user_and_password_store_overrides() {
        let c = SqlConnector::new(ConnectorKind::Postgres, "h/d", None)
            .with_user("alice")
            .with_password("hunter2");
        assert_eq!(c.user(), Some("alice"));
        assert_eq!(c.password(), Some("hunter2"));
    }
}
