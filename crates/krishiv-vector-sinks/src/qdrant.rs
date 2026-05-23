#[cfg(feature = "qdrant")]
mod imp {
    use std::collections::HashMap;

    use async_trait::async_trait;
    use qdrant_client::qdrant::{
        PointId, vectors_config::Config, CreateCollection, Distance, PointStruct,
        UpsertPoints, VectorParams, VectorsConfig,
    };
    use qdrant_client::Qdrant;

    use crate::batch::EmbeddingBatch;
    use crate::id::point_id_from_doc_epoch;
    use crate::traits::{
        PayloadFilter, PayloadValue, ScoredChunk, VectorSink, VectorSinkError, VectorSinkResult,
    };

    fn payload_to_qdrant(map: &HashMap<String, PayloadValue>) -> HashMap<String, qdrant_client::qdrant::Value> {
        map.iter()
            .map(|(k, v)| {
                let value = match v {
                    PayloadValue::String(s) => qdrant_client::qdrant::Value {
                        kind: Some(qdrant_client::qdrant::value::Kind::StringValue(s.clone())),
                    },
                    PayloadValue::Int(i) => qdrant_client::qdrant::Value {
                        kind: Some(qdrant_client::qdrant::value::Kind::IntegerValue(*i)),
                    },
                    PayloadValue::Float(f) => qdrant_client::qdrant::Value {
                        kind: Some(qdrant_client::qdrant::value::Kind::DoubleValue(*f)),
                    },
                    PayloadValue::Bool(b) => qdrant_client::qdrant::Value {
                        kind: Some(qdrant_client::qdrant::value::Kind::BoolValue(*b)),
                    },
                };
                (k.clone(), value)
            })
            .collect()
    }

    /// Qdrant vector sink.
    #[derive(Clone)]
    pub struct QdrantSink {
        client: Qdrant,
        collection_name: String,
        vector_size: u64,
        create_collection_if_missing: bool,
    }

    impl QdrantSink {
        /// Connect to Qdrant at `url`.
        pub async fn connect(
            url: &str,
            collection_name: impl Into<String>,
            vector_size: u64,
            create_collection_if_missing: bool,
        ) -> VectorSinkResult<Self> {
            let client = Qdrant::from_url(url)
                .build()
                .map_err(|e| VectorSinkError::Connection(e.to_string()))?;
            let sink = Self {
                client,
                collection_name: collection_name.into(),
                vector_size,
                create_collection_if_missing,
            };
            if create_collection_if_missing {
                sink.ensure_collection().await?;
            }
            Ok(sink)
        }

        async fn ensure_collection(&self) -> VectorSinkResult<()> {
            let collections = self
                .client
                .list_collections()
                .await
                .map_err(|e| VectorSinkError::Connection(e.to_string()))?;
            if collections
                .collections
                .iter()
                .any(|c| c.name == self.collection_name)
            {
                return Ok(());
            }
            self.client
                .create_collection(CreateCollection {
                    collection_name: self.collection_name.clone(),
                    vectors_config: Some(VectorsConfig {
                        config: Some(Config::Params(VectorParams {
                            size: self.vector_size,
                            distance: Distance::Cosine.into(),
                            ..Default::default()
                        })),
                    }),
                    ..Default::default()
                })
                .await
                .map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
            Ok(())
        }

        fn point_id_u64(id_hex: &str) -> u64 {
            u64::from_str_radix(id_hex, 16).unwrap_or_else(|_| {
                let mut h = 0u64;
                for b in id_hex.as_bytes() {
                    h = h.wrapping_mul(31).wrapping_add(*b as u64);
                }
                h
            })
        }
    }

    #[async_trait]
    impl VectorSink for QdrantSink {
        fn sink_name(&self) -> &str {
            "qdrant"
        }

        async fn upsert_batch(&self, batch: &EmbeddingBatch) -> VectorSinkResult<()> {
            if self.create_collection_if_missing {
                self.ensure_collection().await?;
            }
            let points: Vec<PointStruct> = batch
                .doc_ids
                .iter()
                .zip(batch.vectors.iter())
                .zip(batch.payloads.iter())
                .map(|((doc_id, vector), payload)| {
                    let id_hex = point_id_from_doc_epoch(doc_id, batch.epoch);
                    let mut payload_map = payload_to_qdrant(payload);
                    payload_map.insert(
                        "doc_id".into(),
                        qdrant_client::qdrant::Value {
                            kind: Some(qdrant_client::qdrant::value::Kind::StringValue(
                                doc_id.clone(),
                            )),
                        },
                    );
                    PointStruct {
                        id: Some(PointId::from(Self::point_id_u64(&id_hex))),
                        vectors: Some(vector.clone().into()),
                        payload: payload_map,
                    }
                })
                .collect();
            self.client
                .upsert_points(UpsertPoints {
                    collection_name: self.collection_name.clone(),
                    points,
                    ..Default::default()
                })
                .await
                .map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
            Ok(())
        }

        async fn delete_by_ids(&self, ids: &[String]) -> VectorSinkResult<()> {
            let point_ids: Vec<PointId> = ids
                .iter()
                .map(|id| PointId::from(Self::point_id_u64(id)))
                .collect();
            self.client
                .delete_points(qdrant_client::qdrant::DeletePoints {
                    collection_name: self.collection_name.clone(),
                    points: Some(qdrant_client::qdrant::PointsSelector {
                        points_selector_one_of: Some(
                            qdrant_client::qdrant::points_selector::PointsSelectorOneOf::Points(
                                qdrant_client::qdrant::PointsIdsList { ids: point_ids },
                            ),
                        ),
                    }),
                    ..Default::default()
                })
                .await
                .map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
            Ok(())
        }

        async fn query_nearest(
            &self,
            vector: &[f32],
            top_k: usize,
            _filter: Option<&PayloadFilter>,
        ) -> VectorSinkResult<Vec<ScoredChunk>> {
            let response = self
                .client
                .search_points(qdrant_client::qdrant::SearchPoints {
                    collection_name: self.collection_name.clone(),
                    vector: vector.to_vec(),
                    limit: top_k as u64,
                    with_payload: Some(true.into()),
                    ..Default::default()
                })
                .await
                .map_err(|e| VectorSinkError::Query(e.to_string()))?;
            let chunks = response
                .result
                .into_iter()
                .map(|p| {
                    let payload = p.payload;
                    let doc_id = payload
                        .get("doc_id")
                        .and_then(|v| match &v.kind {
                            Some(qdrant_client::qdrant::value::Kind::StringValue(s)) => {
                                Some(s.clone())
                            }
                            _ => None,
                        })
                        .unwrap_or_default();
                    let text = payload
                        .get("text")
                        .and_then(|v| match &v.kind {
                            Some(qdrant_client::qdrant::value::Kind::StringValue(s)) => {
                                Some(s.clone())
                            }
                            _ => None,
                        })
                        .unwrap_or_default();
                    let chunk_index = payload
                        .get("chunk_index")
                        .and_then(|v| match &v.kind {
                            Some(qdrant_client::qdrant::value::Kind::IntegerValue(i)) => {
                                Some(*i as usize)
                            }
                            _ => None,
                        })
                        .unwrap_or(0);
                    ScoredChunk {
                        doc_id,
                        chunk_index,
                        text,
                        score: p.score,
                        payload: HashMap::new(),
                    }
                })
                .collect();
            Ok(chunks)
        }
    }
}

#[cfg(feature = "qdrant")]
pub use imp::QdrantSink;
