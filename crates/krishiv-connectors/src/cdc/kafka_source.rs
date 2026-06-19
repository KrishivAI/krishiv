#[cfg(feature = "kafka")]
use crate::ConnectorError;

#[cfg(feature = "kafka")]
use super::{CdcEventSource, RawCdcRecord};

/// Configuration for the rdkafka-backed CDC event source.
///
/// Construct via the builder pattern and pass to [`RdkafkaCdcEventSource::new`].
#[cfg(feature = "kafka")]
#[derive(Clone)]
pub struct KafkaCdcConfig {
    /// Comma-separated list of `host:port` bootstrap broker addresses.
    pub bootstrap_servers: String,
    /// Consumer group id used for offset management.
    pub group_id: String,
    /// Topic that carries Debezium CDC envelopes.
    pub topic: String,
    /// Security protocol (e.g. `"PLAINTEXT"`, `"SASL_SSL"`).
    pub security_protocol: String,
    /// SASL mechanism (e.g. `"PLAIN"`, `"SCRAM-SHA-256"`).  `None` for
    /// unauthenticated connections.
    pub sasl_mechanism: Option<String>,
    /// SASL username.  `None` for unauthenticated connections.
    pub sasl_username: Option<String>,
    /// SASL password.  `None` for unauthenticated connections.
    pub sasl_password: Option<String>,
}

#[cfg(feature = "kafka")]
impl std::fmt::Debug for KafkaCdcConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaCdcConfig")
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("group_id", &self.group_id)
            .field("topic", &self.topic)
            .field("security_protocol", &self.security_protocol)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field(
                "sasl_username",
                &self.sasl_username.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "sasl_password",
                &self.sasl_password.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[cfg(feature = "kafka")]
impl KafkaCdcConfig {
    /// Create a minimal unauthenticated config for local/test brokers.
    pub fn new(
        bootstrap_servers: impl Into<String>,
        group_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            group_id: group_id.into(),
            topic: topic.into(),
            security_protocol: "PLAINTEXT".to_string(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
        }
    }

    /// Set the security protocol.
    #[must_use]
    pub fn with_security_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.security_protocol = protocol.into();
        self
    }

    /// Configure SASL authentication.
    #[must_use]
    pub fn with_sasl(
        mut self,
        mechanism: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.sasl_mechanism = Some(mechanism.into());
        self.sasl_username = Some(username.into());
        self.sasl_password = Some(password.into());
        self
    }

    /// Validate the configuration.  Returns an error if required fields are
    /// missing or inconsistent.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.bootstrap_servers.is_empty() {
            return Err(ConnectorError::Cdc(
                "bootstrap_servers must not be empty".into(),
            ));
        }
        if self.group_id.is_empty() {
            return Err(ConnectorError::Cdc("group_id must not be empty".into()));
        }
        if self.topic.is_empty() {
            return Err(ConnectorError::Cdc("topic must not be empty".into()));
        }
        // If any SASL field is set, all three must be set.
        let sasl_fields = [
            self.sasl_mechanism.is_some(),
            self.sasl_username.is_some(),
            self.sasl_password.is_some(),
        ];
        let sasl_count = sasl_fields.iter().filter(|&&v| v).count();
        if sasl_count != 0 && sasl_count != 3 {
            return Err(ConnectorError::Cdc(
                "sasl_mechanism, sasl_username, and sasl_password must all be set together".into(),
            ));
        }
        Ok(())
    }
}

/// A [`CdcEventSource`] backed by a real Kafka broker via `rdkafka`.
///
/// Uses a `StreamConsumer` with group-level offset management.  After each
/// successful downstream sink commit the consumer commits offsets
/// synchronously.
///
/// Construct with [`RdkafkaCdcEventSource::new`] and pass to
/// [`CdcToLakehousePipeline::run_with_source`].
#[cfg(feature = "kafka")]
pub struct RdkafkaCdcEventSource {
    consumer: std::sync::Arc<rdkafka::consumer::StreamConsumer>,
    /// Maximum number of milliseconds to wait for a single message poll.
    poll_timeout_ms: u64,
}

#[cfg(feature = "kafka")]
impl RdkafkaCdcEventSource {
    /// Create a new source from a [`KafkaCdcConfig`].
    ///
    /// Validates the config, builds a `StreamConsumer`, and subscribes to the
    /// configured topic.  Returns `ConnectorError` if configuration or consumer
    /// creation fails.
    pub fn new(config: &KafkaCdcConfig) -> Result<Self, ConnectorError> {
        use rdkafka::ClientConfig;
        use rdkafka::consumer::Consumer;

        config.validate()?;

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("group.id", &config.group_id)
            .set("security.protocol", &config.security_protocol)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest");

        if let (Some(mechanism), Some(username), Some(password)) = (
            &config.sasl_mechanism,
            &config.sasl_username,
            &config.sasl_password,
        ) {
            client_config
                .set("sasl.mechanisms", mechanism)
                .set("sasl.username", username)
                .set("sasl.password", password);
        }

        let consumer: rdkafka::consumer::StreamConsumer = client_config
            .create()
            .map_err(|e| ConnectorError::Cdc(format!("rdkafka consumer creation failed: {e}")))?;

        consumer
            .subscribe(&[config.topic.as_str()])
            .map_err(|e| ConnectorError::Cdc(format!("rdkafka subscribe failed: {e}")))?;

        Ok(Self {
            consumer: std::sync::Arc::new(consumer),
            poll_timeout_ms: 100,
        })
    }

    /// Override the per-message poll timeout (default: 100 ms).
    #[must_use]
    pub fn with_poll_timeout_ms(mut self, ms: u64) -> Self {
        self.poll_timeout_ms = ms;
        self
    }

    /// Commit consumer group offsets for the currently assigned partitions.
    ///
    /// Called internally after a successful downstream sink commit.
    fn commit_offsets_inner(&self) -> Result<(), ConnectorError> {
        use rdkafka::consumer::Consumer;
        self.consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Sync)
            .map_err(|e| ConnectorError::Cdc(format!("rdkafka offset commit failed: {e}")))
    }
}

#[cfg(feature = "kafka")]
impl CdcEventSource for RdkafkaCdcEventSource {
    /// Poll up to `max` Debezium JSON strings from Kafka.
    ///
    /// Each call blocks for at most `poll_timeout_ms` per message.  Returns an
    /// empty `Vec` when no messages are available within the timeout window
    /// (the pipeline interprets this as a momentary idle, not shutdown).
    fn poll_events(&mut self, max: usize) -> Result<Vec<String>, ConnectorError> {
        Ok(self
            .poll_records(max)?
            .into_iter()
            .map(|record| record.payload)
            .collect())
    }

    /// Poll up to `max` Debezium records with real Kafka partition/offset metadata.
    fn poll_records(&mut self, max: usize) -> Result<Vec<RawCdcRecord>, ConnectorError> {
        use rdkafka::Message;

        let mut events = Vec::with_capacity(max.min(64));

        for _ in 0..max {
            // `poll_records` is a sync trait method invoked from async pipeline
            // loops, so receiving from the async Kafka consumer requires
            // re-entering a runtime. `krishiv_common::async_util::block_on`
            // picks the correct strategy for the active runtime flavor —
            // the naive `block_in_place(|| Handle::current().block_on(..))`
            // pattern panics on current-thread runtimes.
            let msg = krishiv_common::async_util::block_on(tokio::time::timeout(
                std::time::Duration::from_millis(self.poll_timeout_ms),
                self.consumer.recv(),
            ));

            match msg {
                // Timed out – no more messages available right now.
                Err(_timeout) => break,
                Ok(Err(e)) => {
                    return Err(ConnectorError::Cdc(format!("rdkafka receive error: {e}")));
                }
                Ok(Ok(msg)) => {
                    let payload = match msg.payload_view::<str>() {
                        Some(Ok(s)) => s.to_string(),
                        Some(Err(e)) => {
                            return Err(ConnectorError::Cdc(format!(
                                "rdkafka payload is not valid UTF-8 at partition {} offset {}: {e}",
                                msg.partition(),
                                msg.offset()
                            )));
                        }
                        None => {
                            // C6: explicitly store the tombstone offset so that the
                            // next commit_consumer_state call includes it.  Without
                            // this, a partition whose last message before a commit
                            // is a tombstone would not have its offset advanced in
                            // the Iceberg snapshot metadata.
                            tracing::warn!(
                                partition = msg.partition(),
                                offset = msg.offset(),
                                "skipping tombstone message (null payload); offset committed"
                            );
                            use rdkafka::consumer::Consumer;
                            let _ = self.consumer.store_offset(
                                msg.topic(),
                                msg.partition(),
                                msg.offset(),
                            );
                            continue;
                        }
                    };
                    let partition_id = u32::try_from(msg.partition()).map_err(|_| {
                        ConnectorError::Cdc(format!(
                            "rdkafka returned invalid partition {}",
                            msg.partition()
                        ))
                    })?;
                    events.push(RawCdcRecord::new(payload, partition_id, msg.offset()));
                }
            }
        }

        Ok(events)
    }

    fn is_live(&self) -> bool {
        true
    }

    fn commit_offsets(&mut self) -> Result<(), ConnectorError> {
        self.commit_offsets_inner()
    }
}
