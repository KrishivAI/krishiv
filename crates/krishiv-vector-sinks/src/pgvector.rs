#[cfg(feature = "pgvector")]
mod imp {
    use std::collections::HashMap;

    use async_trait::async_trait;
    use sqlx::PgPool;
    use sqlx::postgres::PgPoolOptions;

    use crate::batch::EmbeddingBatch;
    use crate::id::point_id_from_doc_epoch;
    use crate::traits::{
        PayloadFilter, PayloadValue, ScoredChunk, VectorSink, VectorSinkError, VectorSinkResult,
        validate_identifier,
    };

    fn vector_to_pg(v: &[f32]) -> String {
        let inner = v
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(",");
        format!("[{inner}]")
    }

    /// PostgreSQL pgvector sink.
    #[derive(Clone)]
    pub struct PgvectorSink {
        pool: PgPool,
        table_name: String,
        vector_dim: usize,
    }

    impl PgvectorSink {
        /// Connect using a PostgreSQL connection string.
        pub async fn connect(
            database_url: &str,
            table_name: impl Into<String>,
            vector_dim: usize,
        ) -> VectorSinkResult<Self> {
            let table_name = table_name.into();
            validate_identifier(&table_name)?;
            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect(database_url)
                .await
                .map_err(|e| VectorSinkError::Connection(e.to_string()))?;
            let sink = Self {
                pool,
                table_name,
                vector_dim,
            };
            sink.ensure_table().await?;
            Ok(sink)
        }

        async fn ensure_table(&self) -> VectorSinkResult<()> {
            let sql = format!(
                r#"
                CREATE EXTENSION IF NOT EXISTS vector;
                CREATE TABLE IF NOT EXISTS {} (
                    id TEXT PRIMARY KEY,
                    vector vector({}),
                    payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    epoch BIGINT NOT NULL
                );
                "#,
                self.table_name, self.vector_dim
            );
            sqlx::query(&sql)
                .execute(&self.pool)
                .await
                .map_err(|e| VectorSinkError::SchemaConflict(e.to_string()))?;
            Ok(())
        }
    }

    #[async_trait]
    impl VectorSink for PgvectorSink {
        fn sink_name(&self) -> &str {
            "pgvector"
        }

        async fn upsert_batch(&self, batch: &EmbeddingBatch) -> VectorSinkResult<()> {
            for ((doc_id, vector), payload) in batch
                .doc_ids
                .iter()
                .zip(batch.vectors.iter())
                .zip(batch.payloads.iter())
            {
                let id = point_id_from_doc_epoch(doc_id, batch.epoch);
                let payload_json = serde_json::to_value(payload_to_json(payload))
                    .map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
                let sql = format!(
                    r#"
                    INSERT INTO {} (id, vector, payload, epoch)
                    VALUES ($1, $2::vector, $3::jsonb, $4)
                    ON CONFLICT (id) DO UPDATE
                    SET vector = EXCLUDED.vector,
                        payload = EXCLUDED.payload,
                        epoch = EXCLUDED.epoch
                    "#,
                    self.table_name
                );
                sqlx::query(&sql)
                    .bind(&id)
                    .bind(vector_to_pg(vector))
                    .bind(payload_json)
                    .bind(batch.epoch as i64)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
            }
            Ok(())
        }

        async fn delete_by_ids(&self, ids: &[String]) -> VectorSinkResult<()> {
            for id in ids {
                let sql = format!("DELETE FROM {} WHERE id = $1", self.table_name);
                sqlx::query(&sql)
                    .bind(id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
            }
            Ok(())
        }

        async fn query_nearest(
            &self,
            vector: &[f32],
            top_k: usize,
            _filter: Option<&PayloadFilter>,
        ) -> VectorSinkResult<Vec<ScoredChunk>> {
            let sql = format!(
                r#"
                SELECT payload, 1 - (vector <=> $1::vector) AS score
                FROM {}
                ORDER BY vector <=> $1::vector
                LIMIT $2
                "#,
                self.table_name
            );
            let rows = sqlx::query_as::<_, (serde_json::Value, f64)>(&sql)
                .bind(vector_to_pg(vector))
                .bind(top_k as i64)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| VectorSinkError::Query(e.to_string()))?;
            Ok(rows
                .into_iter()
                .map(|(payload, score)| json_to_chunk(&payload, score as f32))
                .collect())
        }
    }

    fn payload_to_json(map: &HashMap<String, PayloadValue>) -> HashMap<String, serde_json::Value> {
        map.iter().map(|(k, v)| (k.clone(), v.to_json())).collect()
    }

    fn json_to_chunk(payload: &serde_json::Value, score: f32) -> ScoredChunk {
        let obj = payload.as_object().cloned().unwrap_or_default();
        let doc_id = obj
            .get("doc_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let text = obj
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        ScoredChunk {
            doc_id,
            chunk_index: 0,
            text,
            score,
            payload: HashMap::new(),
        }
    }
}

#[cfg(feature = "pgvector")]
pub use imp::PgvectorSink;
