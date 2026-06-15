use krishiv_api::{BlockingSession, DataFrame, QueryResult, Session, SessionBuilder};

use crate::error::{GatewayError, GatewayQueryResult, GatewayResult};

/// Blocking SQL gateway session backed by the public Krishiv APIs.
///
/// JDBC/ODBC drivers should target this facade rather than embedding protocol
/// behavior inside [`krishiv_api`].
pub struct GatewaySession {
    blocking: BlockingSession,
}

impl GatewaySession {
    pub fn embedded() -> GatewayResult<Self> {
        BlockingSession::embedded()
            .map(|blocking| Self { blocking })
            .map_err(GatewayError::from_krishiv_error)
    }

    pub fn connect(coordinator_url: impl Into<String>) -> GatewayResult<Self> {
        BlockingSession::connect(coordinator_url)
            .map(|blocking| Self { blocking })
            .map_err(GatewayError::from_krishiv_error)
    }

    pub fn execute_sql(&self, query: &str) -> GatewayResult<GatewayQueryResult> {
        self.blocking
            .sql(query)
            .map(|result| GatewayQueryResult { result })
            .map_err(GatewayError::from_krishiv_error)
    }

    pub fn collect(&self, dataframe: DataFrame) -> GatewayResult<GatewayQueryResult> {
        self.blocking
            .collect(dataframe)
            .map(|result| GatewayQueryResult { result })
            .map_err(GatewayError::from_krishiv_error)
    }

    pub fn session(&self) -> &Session {
        self.blocking.session()
    }
}

impl GatewaySession {
    pub fn builder() -> SessionBuilder {
        SessionBuilder::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_embedded_select_round_trip() {
        let gateway = GatewaySession::embedded().expect("embedded gateway");
        let result = gateway
            .execute_sql("SELECT 42 AS answer")
            .expect("gateway query");
        assert_eq!(result.result.row_count(), 1);
    }

    #[test]
    fn gateway_maps_syntax_errors_to_sqlstate() {
        let gateway = GatewaySession::embedded().expect("embedded gateway");
        let error = gateway
            .execute_sql("SELECT FROM")
            .expect_err("invalid SQL should fail");
        assert_eq!(error.sqlstate().len(), 5);
        assert!(!error.message().is_empty());
    }
}
