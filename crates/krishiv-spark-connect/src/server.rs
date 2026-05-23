//! Tonic Spark Connect gRPC service.

use std::pin::Pin;
use std::sync::Arc;

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use futures::Stream;
use krishiv_proto::spark_connect::connect::{
    spark_connect_service_server::{SparkConnectService, SparkConnectServiceServer},
    AnalyzePlanRequest, AnalyzePlanResponse, ConfigRequest, ConfigResponse, ExecutePlanRequest,
    ExecutePlanResponse, Plan, UserContext,
};
use krishiv_proto::spark_connect::SparkConnectCompatMatrix;
use krishiv_sql::SqlEngine;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::translate::relation_to_sql;

/// Spark Connect listener configuration.
#[derive(Debug, Clone)]
pub struct SparkConnectConfig {
    pub bind_addr: String,
    pub spark_version: String,
}

impl Default for SparkConnectConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:7070".into(),
            spark_version: "3.5.3".into(),
        }
    }
}

/// Spark Connect service backed by [`SqlEngine`].
#[derive(Clone)]
pub struct SparkConnectServiceImpl {
    engine: Arc<SqlEngine>,
    matrix: SparkConnectCompatMatrix,
    spark_version: String,
}

impl SparkConnectServiceImpl {
    pub fn new(engine: SqlEngine) -> Self {
        Self {
            engine: Arc::new(engine),
            matrix: SparkConnectCompatMatrix::krishiv_default(),
            spark_version: SparkConnectConfig::default().spark_version,
        }
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.spark_version = version.into();
        self
    }

    pub fn compat_matrix(&self) -> &SparkConnectCompatMatrix {
        &self.matrix
    }

    async fn run_sql(&self, sql: &str) -> Result<Vec<RecordBatch>, Status> {
        let engine = self.engine.clone();
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async move {
                engine
                    .sql(&sql)
                    .await
                    .map_err(|e| Status::invalid_argument(e.to_string()))?
                    .collect()
                    .await
                    .map_err(|e| Status::internal(e.to_string()))
            })
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
    }

    fn plan_root_relation(plan: &Plan) -> Result<&krishiv_proto::spark_connect::connect::Relation, Status> {
        match plan.op_type.as_ref() {
            Some(krishiv_proto::spark_connect::connect::plan::OpType::Root(rel)) => Ok(rel),
            _ => Err(Status::unimplemented(
                "only Relation plans are supported; see spark-sql-compat-matrix.md",
            )),
        }
    }
}

#[tonic::async_trait]
impl SparkConnectService for SparkConnectServiceImpl {
    type ExecutePlanStream =
        Pin<Box<dyn Stream<Item = Result<ExecutePlanResponse, Status>> + Send>>;
    type ReattachExecuteStream = Self::ExecutePlanStream;

    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        let req = request.into_inner();
        let session_id = req.session_id;
        let plan = req.plan.ok_or_else(|| Status::invalid_argument("missing plan"))?;
        let rel = Self::plan_root_relation(&plan)?;
        let sql = relation_to_sql(rel).map_err(|e| {
            Status::unimplemented(format!("{e}"))
        })?;

        let batches = self.run_sql(&sql).await?;
        let empty = batches.is_empty();
        let operation_id = req
            .operation_id
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let (tx, rx) = tokio::sync::mpsc::channel(8);
        for batch in batches {
            let mut buf = Vec::new();
            {
                let schema = batch.schema();
                let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref())
                    .map_err(|e| Status::internal(e.to_string()))?;
                writer
                    .write(&batch)
                    .map_err(|e| Status::internal(e.to_string()))?;
                writer
                    .finish()
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
            let resp = ExecutePlanResponse {
                session_id: session_id.clone(),
                operation_id: operation_id.clone(),
                response_id: uuid::Uuid::new_v4().to_string(),
                response_type: Some(
                    krishiv_proto::spark_connect::connect::execute_plan_response::ResponseType::ArrowBatch(
                        krishiv_proto::spark_connect::connect::execute_plan_response::ArrowBatch {
                            row_count: batch.num_rows() as i64,
                            data: buf,
                        },
                    ),
                ),
                ..Default::default()
            };
            tx.send(Ok(resp)).await.ok();
        }
        // Empty result set still returns one batch per Spark Connect contract.
        if empty {
            let empty = RecordBatch::new_empty(std::sync::Arc::new(
                arrow::datatypes::Schema::empty(),
            ));
            let mut buf = Vec::new();
            let mut writer = StreamWriter::try_new(&mut buf, empty.schema().as_ref())
                .map_err(|e| Status::internal(e.to_string()))?;
            writer
                .finish()
                .map_err(|e| Status::internal(e.to_string()))?;
            tx.send(Ok(ExecutePlanResponse {
                session_id: session_id.clone(),
                operation_id: operation_id.clone(),
                response_id: uuid::Uuid::new_v4().to_string(),
                response_type: Some(
                    krishiv_proto::spark_connect::connect::execute_plan_response::ResponseType::ArrowBatch(
                        krishiv_proto::spark_connect::connect::execute_plan_response::ArrowBatch {
                            row_count: 0,
                            data: buf,
                        },
                    ),
                ),
                ..Default::default()
            }))
            .await
            .ok();
        }

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn analyze_plan(
        &self,
        request: Request<AnalyzePlanRequest>,
    ) -> Result<Response<AnalyzePlanResponse>, Status> {
        let req = request.into_inner();
        let session_id = req.session_id;
        if let Some(analyze) = req.analyze {
            use krishiv_proto::spark_connect::connect::analyze_plan_request::Analyze;
            match analyze {
                Analyze::SparkVersion(_) => {
                    return Ok(Response::new(AnalyzePlanResponse {
                        session_id,
                        result: Some(
                            krishiv_proto::spark_connect::connect::analyze_plan_response::Result::SparkVersion(
                                krishiv_proto::spark_connect::connect::analyze_plan_response::SparkVersion {
                                    version: self.spark_version.clone(),
                                },
                            ),
                        ),
                    }));
                }
                Analyze::Schema(s) => {
                    let plan = s.plan.ok_or_else(|| Status::invalid_argument("missing plan"))?;
                    let rel = Self::plan_root_relation(&plan)?;
                    let sql = relation_to_sql(rel).map_err(|e| Status::unimplemented(e.to_string()))?;
                    let batches = self.run_sql(&sql).await?;
                    let schema = batches
                        .first()
                        .map(|b| b.schema())
                        .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));
                    let _ddl = format!("{schema:?}");
                    return Ok(Response::new(AnalyzePlanResponse {
                        session_id,
                        result: Some(
                            krishiv_proto::spark_connect::connect::analyze_plan_response::Result::Schema(
                                krishiv_proto::spark_connect::connect::analyze_plan_response::Schema {
                                    schema: None,
                                },
                            ),
                        ),
                    }));
                }
                _ => {}
            }
        }
        Err(Status::unimplemented("analyze mode not supported"))
    }


    async fn add_artifacts(
        &self,
        _request: Request<tonic::Streaming<krishiv_proto::spark_connect::connect::AddArtifactsRequest>>,
    ) -> Result<Response<krishiv_proto::spark_connect::connect::AddArtifactsResponse>, Status> {
        Err(Status::unimplemented("AddArtifacts not supported"))
    }

    async fn artifact_status(
        &self,
        _request: Request<krishiv_proto::spark_connect::connect::ArtifactStatusesRequest>,
    ) -> Result<Response<krishiv_proto::spark_connect::connect::ArtifactStatusesResponse>, Status> {
        Err(Status::unimplemented("ArtifactStatus not supported"))
    }

    async fn interrupt(
        &self,
        _request: Request<krishiv_proto::spark_connect::connect::InterruptRequest>,
    ) -> Result<Response<krishiv_proto::spark_connect::connect::InterruptResponse>, Status> {
        Err(Status::unimplemented("Interrupt not supported"))
    }

    async fn reattach_execute(
        &self,
        _request: Request<krishiv_proto::spark_connect::connect::ReattachExecuteRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        Err(Status::unimplemented("ReattachExecute not supported"))
    }

    async fn release_execute(
        &self,
        _request: Request<krishiv_proto::spark_connect::connect::ReleaseExecuteRequest>,
    ) -> Result<Response<krishiv_proto::spark_connect::connect::ReleaseExecuteResponse>, Status> {
        Err(Status::unimplemented("ReleaseExecute not supported"))
    }

    async fn config(
        &self,
        request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(ConfigResponse {
            session_id: req.session_id,
            warnings: vec!["Krishiv Spark Connect: unknown config keys are ignored".into()],
            ..Default::default()
        }))
    }
}

/// Bind and serve Spark Connect on `config.bind_addr`.
pub async fn serve_spark_connect(
    listener: tokio::net::TcpListener,
    service: SparkConnectServiceImpl,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(SparkConnectServiceServer::new(service))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await
}

/// Build default user context for tests.
#[allow(dead_code)]
pub fn test_user_context() -> UserContext {
    UserContext {
        user_id: "krishiv".into(),
        user_name: "krishiv".into(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_proto::spark_connect::connect::{relation, Plan, Sql};

    #[tokio::test]
    async fn execute_plan_sql_relation() {
        let svc = SparkConnectServiceImpl::new(SqlEngine::new());
        let plan = Plan {
            op_type: Some(krishiv_proto::spark_connect::connect::plan::OpType::Root(
                krishiv_proto::spark_connect::connect::Relation {
                    rel_type: Some(relation::RelType::Sql(Sql {
                        query: "SELECT 42 AS answer".into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )),
        };
        let req = ExecutePlanRequest {
            session_id: uuid::Uuid::new_v4().to_string(),
            user_context: Some(test_user_context()),
            plan: Some(plan),
            ..Default::default()
        };
        let mut stream = svc
            .execute_plan(Request::new(req))
            .await
            .unwrap()
            .into_inner();
        use futures::StreamExt;
        let first = stream.next().await.expect("one batch");
        assert!(first.is_ok());
    }
}
