//! Routes CDC events from a source to per-table Delta stores with schema normalization and fan-out.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::lakehouse::{DeltaOp, DeltaStore, MemoryDeltaStore};

use crate::ConnectorError;
use crate::cdc::{CdcEvent, CdcEventSource, CdcOp};

/// Routes CDC events from one source to per-table delta stores.
pub struct CdcRouter {
    routes: HashMap<String, Arc<Mutex<TableRoute>>>,
    /// Test seam: invoked between the schema capture and the append re-check
    /// so the capture/append race with `update_schema` is deterministically
    /// reproducible.
    #[cfg(test)]
    schema_race_hook: Option<Box<dyn Fn() + Send + Sync>>,
}

struct TableRoute {
    store: Arc<dyn DeltaStore>,
    target_schema: Arc<arrow::datatypes::Schema>,
}

impl CdcRouter {
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
            #[cfg(test)]
            schema_race_hook: None,
        }
    }

    pub fn register_table(
        &mut self,
        table_name: impl Into<String>,
        target_schema: Arc<arrow::datatypes::Schema>,
        store: Option<Arc<dyn DeltaStore>>,
    ) {
        let name = table_name.into();
        self.routes.insert(
            name.clone(),
            Arc::new(Mutex::new(TableRoute {
                store: store.unwrap_or_else(|| Arc::new(MemoryDeltaStore::new())),
                target_schema,
            })),
        );
    }

    pub fn route_event(&self, event: &CdcEvent) -> Result<(), ConnectorError> {
        let route = self.routes.get(&event.table).ok_or_else(|| {
            ConnectorError::Cdc(format!("no live table route for {}", event.table))
        })?;
        let batch = event
            .after
            .as_ref()
            .or(event.before.as_ref())
            .ok_or_else(|| ConnectorError::Cdc("cdc event missing payload".into()))?;
        let op = match event.op {
            CdcOp::Insert | CdcOp::SnapshotRead => DeltaOp::Insert,
            CdcOp::Update => DeltaOp::Update,
            CdcOp::Delete => DeltaOp::Delete,
        };
        // Clone the target schema outside the mutex lock so schema normalization
        // does not extend the critical section unnecessarily. A concurrent
        // `update_schema` may land between the capture and the append, so the
        // schema is re-checked under the append lock and normalization retried
        // against the fresh schema when it changed.
        const MAX_SCHEMA_RETRIES: usize = 8;
        for attempt in 0..MAX_SCHEMA_RETRIES {
            let target_schema = {
                route
                    .lock()
                    .map_err(|_| ConnectorError::Cdc("cdc router lock poisoned".into()))?
                    .target_schema
                    .clone()
            };
            #[cfg(test)]
            if attempt == 0
                && let Some(hook) = &self.schema_race_hook
            {
                hook();
            }
            #[cfg(not(test))]
            let _ = attempt;
            let normalized =
                crate::schema_normalize::SchemaNormalizeOperator::new(target_schema.clone())
                    .normalize(batch)
                    .map_err(|e| ConnectorError::Cdc(e.to_string()))?;
            let guard = route
                .lock()
                .map_err(|_| ConnectorError::Cdc("cdc router lock poisoned".into()))?;
            if !Arc::ptr_eq(&guard.target_schema, &target_schema) {
                continue;
            }
            return guard
                .store
                .append(normalized, op)
                .map_err(|e| ConnectorError::Cdc(e.to_string()));
        }
        // Contention fallback: normalize while holding the lock so the schema
        // cannot change underneath the append.
        let guard = route
            .lock()
            .map_err(|_| ConnectorError::Cdc("cdc router lock poisoned".into()))?;
        let normalized =
            crate::schema_normalize::SchemaNormalizeOperator::new(guard.target_schema.clone())
                .normalize(batch)
                .map_err(|e| ConnectorError::Cdc(e.to_string()))?;
        guard
            .store
            .append(normalized, op)
            .map_err(|e| ConnectorError::Cdc(e.to_string()))
    }

    pub fn poll_and_route<S: CdcEventSource>(
        &self,
        source: &mut S,
        max: usize,
    ) -> Result<usize, ConnectorError> {
        let raw = source.poll_events(max)?;
        let mut routed = 0usize;
        let mut dropped = 0usize;
        for (i, json) in raw.iter().enumerate() {
            match crate::cdc::parse_debezium_envelope(json, 0, i as i64) {
                Ok(event) => {
                    self.route_event(&event)?;
                    routed += 1;
                }
                Err(e) => {
                    dropped += 1;
                    tracing::warn!(
                        index = i,
                        error = %e,
                        "dropping unparseable CDC event; check source format"
                    );
                }
            }
        }
        if dropped > 0 {
            tracing::warn!(dropped, routed, "CDC poll partially succeeded");
        }
        Ok(routed)
    }

    pub fn update_schema(
        &self,
        table: &str,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<(), ConnectorError> {
        let route = self
            .routes
            .get(table)
            .ok_or_else(|| ConnectorError::Cdc(format!("no live table route for {table}")))?;
        let mut guard = route
            .lock()
            .map_err(|_| ConnectorError::Cdc("cdc router lock poisoned".into()))?;
        guard.target_schema = schema;
        Ok(())
    }

    pub fn table_count(&self) -> usize {
        self.routes.len()
    }
}

impl Default for CdcRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::cdc::{CdcEvent, CdcOp, InMemoryCdcEventSource};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]))
    }

    fn event(table: &str, id: &str, op: CdcOp) -> CdcEvent {
        let s = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let batch =
            RecordBatch::try_new(s, vec![Arc::new(StringArray::from(vec![Some(id)]))]).unwrap();
        CdcEvent {
            op,
            before: None,
            after: Some(batch),
            source_lsn: None,
            source_ts_ms: None,
            partition_id: 0,
            offset: 0,
            table: table.to_string(),
        }
    }

    #[test]
    fn cdc_router_fanout_three_tables() {
        let mut router = CdcRouter::new();
        for table in ["orders", "products", "customers"] {
            router.register_table(table, schema(), None);
        }
        router
            .route_event(&event("orders", "1", CdcOp::Insert))
            .unwrap();
        router
            .route_event(&event("products", "2", CdcOp::Insert))
            .unwrap();
        router
            .route_event(&event("customers", "3", CdcOp::Insert))
            .unwrap();
        assert_eq!(router.table_count(), 3);
    }

    #[test]
    fn cdc_router_poll_and_route() {
        let mut router = CdcRouter::new();
        router.register_table("orders", schema(), None);
        let json = r#"{"op":"c","after":{"id":"9"},"source":{"table":"orders"}}"#;
        let mut source = InMemoryCdcEventSource::new([json]);
        let n = router.poll_and_route(&mut source, 10).unwrap();
        assert_eq!(n, 1);
    }

    /// Regression (Wave 2 — Error Propagation): routing an event for a table
    /// with no registered route must surface a `ConnectorError::Cdc` (not
    /// panic or silently drop the event), and `poll_and_route` must propagate
    /// that error to the caller rather than swallowing it.
    #[test]
    fn route_event_for_unknown_table_returns_cdc_error() {
        let router = CdcRouter::new();
        let err = router
            .route_event(&event("missing_table", "1", CdcOp::Insert))
            .unwrap_err();
        match err {
            ConnectorError::Cdc(msg) => {
                assert!(
                    msg.contains("missing_table"),
                    "error must name the unrouted table, got: {msg}"
                );
            }
            other => panic!("expected ConnectorError::Cdc, got: {other:?}"),
        }
    }

    /// Regression (Wave 2 — Error Propagation): an event with neither a
    /// `before` nor `after` payload must surface a `ConnectorError::Cdc`
    /// rather than panicking on the missing batch.
    #[test]
    fn route_event_with_no_payload_returns_cdc_error() {
        let mut router = CdcRouter::new();
        router.register_table("orders", schema(), None);
        let mut event = event("orders", "1", CdcOp::Insert);
        event.after = None;
        let err = router.route_event(&event).unwrap_err();
        assert!(
            matches!(&err, ConnectorError::Cdc(msg) if msg.contains("missing payload")),
            "expected a 'missing payload' ConnectorError::Cdc, got: {err:?}"
        );
    }

    /// Regression: a concurrent `update_schema` landing between the schema
    /// capture and the append must not produce a stale-schema append — the
    /// re-check under the append lock retries normalization against the fresh
    /// schema. The race window is made deterministic via `schema_race_hook`.
    #[test]
    fn route_event_renormalizes_when_schema_updated_between_capture_and_append() {
        use std::sync::Mutex;

        use crate::lakehouse::{DeltaEntry, LakehouseError};

        #[derive(Default)]
        struct RecordingStore {
            appended_schemas: Mutex<Vec<Arc<Schema>>>,
        }

        impl crate::lakehouse::DeltaStore for RecordingStore {
            fn append(&self, batch: RecordBatch, _op: DeltaOp) -> Result<(), LakehouseError> {
                self.appended_schemas.lock().unwrap().push(batch.schema());
                Ok(())
            }

            fn scan(&self) -> Result<Vec<DeltaEntry>, LakehouseError> {
                Ok(Vec::new())
            }

            fn truncate(&self) -> Result<(), LakehouseError> {
                Ok(())
            }

            fn len(&self) -> Result<usize, LakehouseError> {
                Ok(self.appended_schemas.lock().unwrap().len())
            }
        }

        let updated_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
        ]));
        let store = Arc::new(RecordingStore::default());
        let mut router = CdcRouter::new();
        router.register_table("orders", schema(), Some(store.clone()));

        // Flip the route's target schema in the window between the schema
        // capture and the append re-check.
        let route = Arc::clone(router.routes.get("orders").unwrap());
        let hook_schema = Arc::clone(&updated_schema);
        router.schema_race_hook = Some(Box::new(move || {
            route.lock().unwrap().target_schema = Arc::clone(&hook_schema);
        }));

        router
            .route_event(&event("orders", "1", CdcOp::Insert))
            .unwrap();

        let appended = store.appended_schemas.lock().unwrap();
        assert_eq!(appended.len(), 1);
        assert!(
            appended[0].index_of("extra").is_ok(),
            "batch was appended with the stale pre-update schema: {:?}",
            appended[0]
        );
    }

    /// Regression (Wave 2 — Error Propagation): `poll_and_route` must
    /// propagate a `ConnectorError` from `route_event` to the caller (e.g.
    /// when the parsed event targets an unregistered table) instead of
    /// swallowing it the way malformed-event parse failures are swallowed.
    #[test]
    fn poll_and_route_propagates_route_event_error() {
        let router = CdcRouter::new();
        let json = r#"{"op":"c","after":{"id":"9"},"source":{"table":"unrouted"}}"#;
        let mut source = InMemoryCdcEventSource::new([json]);
        let mut router = router;
        router.register_table("placeholder", schema(), None);
        let err = router.poll_and_route(&mut source, 10).unwrap_err();
        assert!(
            matches!(&err, ConnectorError::Cdc(msg) if msg.contains("unrouted")),
            "expected route_event's error to propagate through poll_and_route, got: {err:?}"
        );
    }
}
