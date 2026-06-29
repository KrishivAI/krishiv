# Krishiv Enterprise Examples

Real-connector examples demonstrating Krishiv's batch, streaming, and
delta-batch (IVM) capabilities against live infrastructure.

## Infrastructure

All services run locally via Docker Compose.

| Service          | Port  | Purpose                         |
|-----------------|-------|---------------------------------|
| Kafka (KRaft)    | 9092  | message broker                  |
| Schema Registry  | 8081  | Avro schema store               |
| PostgreSQL       | 5432  | CDC source (WAL logical)        |
| Debezium         | 8083  | CDC connector for Postgres      |
| LocalStack       | 4566  | Kinesis + S3 (AWS-compat)       |
| MinIO            | 9000  | S3-compatible object store      |
| Elasticsearch    | 9200  | full-text search sink           |
| Cassandra        | 9042  | wide-column sink                |
| Pulsar           | 6650  | alternative message broker      |

### Quick start

```bash
# Start all infrastructure
make infra-up

# Create topics, Kinesis stream, S3 bucket, Cassandra schema
make seed-all

# (optional) register Debezium CDC connector for PostgreSQL
make debezium-up
```

## Example Matrix

| # | Name                         | Delivery    | Source       | Sink        |
|---|------------------------------|-------------|--------------|-------------|
| 01 | Kafka → Parquet (at-least-once) | at-least-once | Kafka | Parquet |
| 02 | Kafka → Parquet (exactly-once)  | exactly-once  | Kafka | Parquet (2PC) |
| 03 | CDC Debezium → Delta Lake       | at-least-once | Kafka/Debezium | Delta |
| 04 | Kafka → Tumbling Window         | best-effort   | Kafka | stdout |
| 05 | Kinesis → Parquet               | at-least-once | Kinesis | Parquet |
| 06 | Parquet → Elasticsearch         | at-least-once | Parquet | Elasticsearch |
| 07 | Parquet → Cassandra             | at-least-once | Parquet | Cassandra |
| 08 | Multi-source join               | at-least-once | Kafka + Parquet | Parquet |
| 09 | CEP fraud detection             | best-effort   | Kafka | Kafka (alerts) |
| 10 | S3 ETL pipeline                 | at-least-once | S3/MinIO | S3/MinIO |

## Running Rust Examples

```bash
# Individual
make rust-01      # Kafka → Parquet (at-least-once)
make rust-09      # CEP fraud detection

# All
make rust-all
```

Or directly via cargo:

```bash
cd examples/enterprise
KAFKA_BOOTSTRAP=localhost:9092 cargo run -p krishiv-enterprise-examples \
  --release --bin ent_01_kafka_parquet_at_least_once
```

## Running Python Examples

```bash
# Install dependencies once
make py-install

# Individual
make py-01        # Kafka → Parquet
make py-09        # CEP fraud detection

# All
make py-all
```

## Example Details

### 01 · Kafka → Parquet (at-least-once)

Reads from `orders` topic with manual offset commit. Write succeeds before
offset advances, so a crash leaves the consumer behind the last commit and
re-reads on restart. Uses `PostWriteOffsetCommitProtocol` (Rust) /
`enable.auto.commit=false` + explicit `commit()` (Python).

### 02 · Kafka → Parquet (exactly-once 2PC)

Stages each batch as `<epoch>-N.parquet.tmp`, then atomically renames on
epoch barrier. Kafka offset is committed only after all renames succeed.
Mirrors `LocalParquetTwoPhaseCommitSink` + `EpochTransactionLog` in the
Krishiv executor. The `.tmp` files survive a crash and can be re-committed
on restart.

### 03 · CDC Debezium → Delta Lake

Parses Debezium 2.x JSON envelopes (`op: c/u/d`) from
`pgserver.public.orders`, normalises schema across Insert/Update/Delete,
and writes to a Delta table. Schema evolution (new `notes` column) is
handled via nullable column merging.

### 04 · Kafka → Tumbling Window

Assigns events to 10-second tumbling windows on the `ts` event-time
field. Computes `order_count` and `total_amount` per (window, customer).
Watermark lag of 2 s tolerates minor out-of-order events.

### 05 · Kinesis → Parquet (checkpointed)

Reads from a single Kinesis shard (LocalStack). Saves the last sequence
number to `/tmp/...checkpoint.txt`; re-runs restore via
`AfterSequenceNumber` for at-least-once semantics.

### 06 · Parquet → Elasticsearch

Loads a product catalog, enriches it with `inventory_value` and
`price_tier` columns, then bulk-indexes into `krishiv-products`. Uses
`product_id` as the Elasticsearch `_id` for idempotent upserts.

### 07 · Parquet → Cassandra

Filters orders to `shipped` + `delivered` status, then writes via
`UNLOGGED BATCH` CQL for throughput. Partition key is `order_id` so
re-runs overwrite existing rows (idempotent).

### 08 · Multi-source join

Demonstrates stream-table join: Kafka order events enriched with a static
Parquet product catalog. Mirrors `krishiv.stream_table_join()` in the
Python API and the interval-join executor fragment.

### 09 · CEP fraud detection

Stateful sequence matcher per `user_id`:
`login → purchase < 60 s → large_txn > $5000 < 30 s`.
Matches publish JSON alerts to the `fraud-alerts` Kafka topic. Implements
`PartitionedCepMatcher` semantics from the Krishiv `stream:cep:` fragment.

### 10 · S3 ETL pipeline

Full batch ETL: synthetic IoT sensor data → S3 upload → download →
pandas transform (filter invalid humidity, add `temp_f`, `temp_bucket`,
`is_anomaly`) → curated Parquet upload. Swap `AWS_ENDPOINT_URL` for real
AWS S3 with no code changes.

## Connector delivery guarantees

| Delivery | How achieved                                 | Example(s) |
|----------|----------------------------------------------|------------|
| BestEffort    | Fire-and-forget, no commit ordering     | 04, 09     |
| AtLeastOnce  | Write then commit; replay on crash      | 01, 03, 05–08, 10 |
| ExactlyOnce  | 2PC stage-rename + post-rename commit   | 02         |
