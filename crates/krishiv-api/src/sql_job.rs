//! Compile a self-contained SQL pipeline script into a [`CompiledJob`].
//!
//! This is the SQL front-end's path to the unified engine spine: a script of
//! `CREATE SOURCE` / `CREATE SINK` statements (the existing pipeline DDL) is
//! lowered to one [`CompiledJob`], and [`Session::submit_sql`](crate::Session::submit_sql)
//! dispatches it through the same [`run_job`](crate::run_job) every front-end
//! shares. Engine selection is **not** the SQL surface's: it falls out of the
//! declared connector kinds and whether the transform is windowed, via the one
//! shared [`EngineKind::infer`](crate::EngineKind) site inside `CompiledJob::new`.
//!
//! # Supported shape
//!
//! ```sql
//! CREATE SOURCE orders FROM parquet(path='/data/orders.parquet');
//! CREATE SOURCE summary AS SELECT k, SUM(v) AS total FROM orders GROUP BY k;
//! CREATE SINK out FROM summary INTO parquet(path='/data/out.parquet');
//! ```
//!
//! - Connector sources (`FROM <connector>(...)`) become the job's input sources.
//! - A query source (`AS <SELECT>`) named by the sink is the transform; the
//!   sink may also read a connector source directly (pass-through) or an inline
//!   `(SELECT …)`.
//! - Exactly one `CREATE SINK … INTO <connector>(...)` defines the output.
//!
//! `parquet` is the wired connector (matching [`connector_runtime`](crate::connector_runtime));
//! other kinds return a typed [`KrishivError`]. Multi-level view graphs and
//! `START PIPELINE`/`REFRESH` triggers are out of scope here — `submit_sql` is
//! itself the run trigger.

use krishiv_common::sql_util::{quote_identifier, split_sql_statements};
use std::collections::HashMap;

use krishiv_sql::pipeline_ddl::{
    ConnectorSpec, PipelineStatement, SourceSpec as SqlSourceSpec, parse_pipeline_statement,
};
use krishiv_sql::streaming_window_plan::is_windowed_streaming_sql;

use crate::{CompiledJob, KrishivError, Result, SinkSpec, SourceSpec};

/// Compile a SQL pipeline script into a [`CompiledJob`].
///
/// Returns a typed [`KrishivError`] when the script is not a recognised
/// single-sink connector pipeline (see the [module docs](self) for the shape).
pub fn compile_sql_job(sql: &str) -> Result<CompiledJob> {
    let mut connector_sources: Vec<(String, ConnectorSpec)> = Vec::new();
    let mut query_sources: HashMap<String, String> = HashMap::new();
    let mut sink: Option<(String, String, ConnectorSpec)> = None;

    for raw in split_sql_statements(sql) {
        // Normalise internal whitespace so multi-line statements parse: the
        // prefix-match DDL parser keys on literal `" AS "` / `" FROM "`, which a
        // newline after the keyword would otherwise defeat.
        let stmt_sql = raw.split_whitespace().collect::<Vec<_>>().join(" ");
        if stmt_sql.is_empty() {
            continue;
        }
        let stmt = parse_pipeline_statement(&stmt_sql)
            .map_err(KrishivError::from)?
            .ok_or_else(|| {
                KrishivError::unsupported(format!(
                    "statement is not pipeline DDL the SQL job compiler understands: \
                     '{stmt_sql}'; use CREATE SOURCE / CREATE SINK"
                ))
            })?;

        match stmt {
            PipelineStatement::CreateSource { name, source } => match source {
                SqlSourceSpec::Connector(connector) => connector_sources.push((name, connector)),
                SqlSourceSpec::Query(query) => {
                    query_sources.insert(name, query);
                }
            },
            PipelineStatement::CreateSink {
                name,
                view,
                connector,
            } => {
                let connector = connector.ok_or_else(|| {
                    KrishivError::unsupported(format!(
                        "sink '{name}' has no INTO <connector>(...); the SQL job compiler writes \
                         to a connector sink, e.g. INTO parquet(path='...')"
                    ))
                })?;
                if sink.is_some() {
                    return Err(KrishivError::unsupported(
                        "the SQL job compiler supports exactly one CREATE SINK per job",
                    ));
                }
                sink = Some((name, view, connector));
            }
            // `submit_sql` is itself the run trigger; an explicit one is ignored.
            PipelineStatement::StartPipeline { .. } | PipelineStatement::RefreshPipeline { .. } => {
            }
            PipelineStatement::DropSource { .. } | PipelineStatement::DropSink { .. } => {
                return Err(KrishivError::unsupported(
                    "DROP statements are not part of a SQL job script",
                ));
            }
        }
    }

    let (sink_name, view, sink_connector) = sink.ok_or_else(|| {
        KrishivError::unsupported(
            "the SQL job needs one CREATE SINK ... INTO <connector>(...) to define its output",
        )
    })?;

    if connector_sources.is_empty() {
        return Err(KrishivError::unsupported(
            "the SQL job needs at least one connector source: \
             CREATE SOURCE <name> FROM <connector>(...)",
        ));
    }

    let query = resolve_view_query(&view, &query_sources, &connector_sources)?;
    let sources = connector_sources
        .iter()
        .map(|(name, connector)| source_spec(name, connector))
        .collect::<Result<Vec<_>>>()?;
    let sink_spec = sink_spec(&sink_name, &sink_connector)?;
    let event_time_window = is_windowed_streaming_sql(&query);
    // Parse the transform query once, here at compile time, so a malformed query
    // fails fast with a typed error instead of surfacing deep inside whichever
    // engine runs it. Windowed queries use table-valued window syntax that the
    // streaming compiler validates on its own parse (`is_windowed_streaming_sql`
    // already parsed it to detect the window), so they are checked there rather
    // than re-parsed with the plain dialect here.
    if !event_time_window {
        validate_query_parses(&query)?;
    }

    Ok(CompiledJob::new(
        sink_name,
        query,
        sources,
        vec![sink_spec],
        event_time_window,
    ))
}

/// Parse `query` once as SQL, returning a typed error if it does not parse.
///
/// Syntactic only — no planning or schema resolution — so it needs no source
/// tables and is cheap. This is the first concrete step of "compile once,
/// dispatch anywhere": today each engine re-parses the query string at run; at
/// minimum we validate it once, up front, here.
fn validate_query_parses(query: &str) -> Result<()> {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    Parser::parse_sql(&GenericDialect {}, query)
        .map(|_| ())
        .map_err(|e| KrishivError::unsupported(format!("job query does not parse as SQL: {e}")))
}

/// Split a pipeline script into statements on `;`, ignoring semicolons inside
/// Resolve the transform query a sink reads: a named query source, an inline
/// `(SELECT …)`, or a direct connector-source pass-through.
fn resolve_view_query(
    view: &str,
    query_sources: &HashMap<String, String>,
    connector_sources: &[(String, ConnectorSpec)],
) -> Result<String> {
    let view = view.trim();
    if let Some(inner) = view.strip_prefix('(').and_then(|s| s.strip_suffix(')')) {
        let inner = inner.trim();
        if inner.is_empty() {
            return Err(KrishivError::unsupported(
                "sink reads an empty inline query",
            ));
        }
        return Ok(inner.to_string());
    }
    if let Some(query) = query_sources.get(view) {
        return Ok(query.clone());
    }
    if connector_sources.iter().any(|(name, _)| name == view) {
        return Ok(format!("SELECT * FROM {}", quote_identifier(view)));
    }
    Err(KrishivError::unsupported(format!(
        "sink reads '{view}', which is neither a CREATE SOURCE ... AS <query> nor a declared \
         connector source; define the transform as a named query source"
    )))
}

fn source_spec(name: &str, connector: &ConnectorSpec) -> Result<SourceSpec> {
    match connector.kind.as_str() {
        "parquet" => Ok(SourceSpec::bounded(
            name,
            "parquet",
            connector.require("path").map_err(KrishivError::from)?,
        )),
        other => Err(KrishivError::unsupported(format!(
            "connector source '{other}' is not yet supported by the SQL job compiler; \
             supported: parquet"
        ))),
    }
}

fn sink_spec(view: &str, connector: &ConnectorSpec) -> Result<SinkSpec> {
    match connector.kind.as_str() {
        "parquet" => Ok(SinkSpec::new(
            view,
            "parquet",
            connector.require("path").map_err(KrishivError::from)?,
        )),
        other => Err(KrishivError::unsupported(format!(
            "connector sink '{other}' is not yet supported by the SQL job compiler; \
             supported: parquet"
        ))),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use krishiv_engine_core::EngineKind;

    use super::*;

    #[test]
    fn compiles_batch_job_from_parquet_script() {
        let sql = "
            CREATE SOURCE orders FROM parquet(path='/data/orders.parquet');
            CREATE SOURCE summary AS SELECT k, SUM(v) AS total FROM orders GROUP BY k;
            CREATE SINK out FROM summary INTO parquet(path='/data/out.parquet');
        ";
        let job = compile_sql_job(sql).unwrap();
        assert_eq!(job.engine, EngineKind::Batch);
        assert_eq!(job.name, "out");
        assert_eq!(
            job.query,
            "SELECT k, SUM(v) AS total FROM orders GROUP BY k"
        );
        assert_eq!(job.sources.len(), 1);
        assert_eq!(job.sources[0].name, "orders");
        assert_eq!(job.sources[0].connector, "parquet");
        assert_eq!(job.sources[0].uri, "/data/orders.parquet");
        assert_eq!(job.sinks.len(), 1);
        assert_eq!(job.sinks[0].uri, "/data/out.parquet");
    }

    #[test]
    fn compiles_streaming_job_when_transform_is_windowed() {
        let sql = "
            CREATE SOURCE events FROM parquet(path='/data/events.parquet');
            CREATE SOURCE windowed AS
                SELECT user_id, SUM(amount) AS total
                FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 60000)
                GROUP BY user_id, window_start, window_end;
            CREATE SINK out FROM windowed INTO parquet(path='/data/out.parquet');
        ";
        let job = compile_sql_job(sql).unwrap();
        assert_eq!(job.engine, EngineKind::Streaming);
    }

    #[test]
    fn inline_select_in_sink_is_supported() {
        let sql = "
            CREATE SOURCE orders FROM parquet(path='/in.parquet');
            CREATE SINK out FROM (SELECT SUM(v) AS total FROM orders) INTO parquet(path='/out.parquet');
        ";
        let job = compile_sql_job(sql).unwrap();
        assert_eq!(job.query, "SELECT SUM(v) AS total FROM orders");
        assert_eq!(job.engine, EngineKind::Batch);
    }

    #[test]
    fn connector_source_passthrough_sink() {
        let sql = "
            CREATE SOURCE orders FROM parquet(path='/in.parquet');
            CREATE SINK out FROM orders INTO parquet(path='/out.parquet');
        ";
        let job = compile_sql_job(sql).unwrap();
        assert_eq!(job.query, "SELECT * FROM \"orders\"");
    }

    #[test]
    fn rejects_sink_without_connector() {
        let sql = "
            CREATE SOURCE orders FROM parquet(path='/in.parquet');
            CREATE SINK out FROM orders;
        ";
        let err = compile_sql_job(sql).unwrap_err();
        assert!(matches!(err, KrishivError::Unsupported { .. }));
    }

    #[test]
    fn rejects_missing_sink() {
        let sql = "CREATE SOURCE orders FROM parquet(path='/in.parquet');";
        let err = compile_sql_job(sql).unwrap_err();
        assert!(matches!(err, KrishivError::Unsupported { .. }));
    }

    #[test]
    fn rejects_unknown_connector() {
        let sql = "
            CREATE SOURCE orders FROM kafka(topic='orders');
            CREATE SINK out FROM orders INTO parquet(path='/out.parquet');
        ";
        let err = compile_sql_job(sql).unwrap_err();
        assert!(matches!(err, KrishivError::Unsupported { .. }));
    }

    #[test]
    fn rejects_transform_query_that_does_not_parse() {
        // The bounded AS-query is syntactically broken. The pipeline parser
        // captures it as opaque text, so without the compile-time parse check it
        // would only fail deep inside an engine at run; here it fails fast.
        let sql = "
            CREATE SOURCE orders FROM parquet(path='/in.parquet');
            CREATE SOURCE broken AS SELECT FROM WHERE GROUP;
            CREATE SINK out FROM broken INTO parquet(path='/out.parquet');
        ";
        let err = compile_sql_job(sql).unwrap_err();
        assert!(matches!(err, KrishivError::Unsupported { .. }));
    }

    #[test]
    fn accepts_well_formed_transform_query() {
        // Sanity: a valid bounded AS-query still compiles (the parse check is a
        // gate against malformed SQL, not a tightening of accepted SQL).
        let sql = "
            CREATE SOURCE orders FROM parquet(path='/in.parquet');
            CREATE SOURCE rolled AS SELECT k, SUM(v) AS total FROM orders GROUP BY k;
            CREATE SINK out FROM rolled INTO parquet(path='/out.parquet');
        ";
        let job = compile_sql_job(sql).unwrap();
        assert_eq!(
            job.query,
            "SELECT k, SUM(v) AS total FROM orders GROUP BY k"
        );
    }

    #[test]
    fn semicolon_inside_quoted_path_does_not_split_statement() {
        // A semicolon inside a quoted connector path must not split the script.
        let sql = "CREATE SOURCE orders FROM parquet(path='/data/a;b.parquet'); \
                   CREATE SINK out FROM orders INTO parquet(path='/tmp/o;ut.parquet');";
        let job = compile_sql_job(sql).unwrap();
        assert_eq!(job.sources.len(), 1);
        assert_eq!(job.sources[0].uri, "/data/a;b.parquet");
        assert_eq!(job.sinks[0].uri, "/tmp/o;ut.parquet");
    }

    #[test]
    fn split_statements_ignores_semicolons_in_quotes() {
        let parts = split_sql_statements("A 'x;y'; B; ");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "A 'x;y'");
    }

    #[tokio::test]
    async fn submit_sql_runs_batch_over_parquet_end_to_end() {
        use std::sync::Arc;

        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use krishiv_connectors::parquet::{ParquetSink, ParquetSource};
        use krishiv_connectors::{Sink, Source};

        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("orders.parquet");
        let output = dir.path().join("summary.parquet");

        // Write the input parquet the SQL job will read.
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
        let mut writer = ParquetSink::create(&input).unwrap();
        writer.write_batch(batch).await.unwrap();
        writer.flush().await.unwrap();

        let sql = format!(
            "CREATE SOURCE orders FROM parquet(path='{}'); \
             CREATE SOURCE summary AS SELECT SUM(v) AS total FROM orders; \
             CREATE SINK out FROM summary INTO parquet(path='{}');",
            input.to_str().unwrap(),
            output.to_str().unwrap()
        );

        let session = crate::SessionBuilder::new().build().unwrap();
        let handle = session.submit_sql(&sql).await.unwrap();
        assert_eq!(
            handle.status(),
            krishiv_engine_core::JobStatus::Completed,
            "batch SQL job runs to completion"
        );

        let mut reader = ParquetSource::open(&output).unwrap();
        let out = reader
            .read_batch()
            .await
            .unwrap()
            .expect("one output batch");
        let total = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 6);
    }
}
