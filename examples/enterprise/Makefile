# Enterprise connector examples — infrastructure + runner targets.
# Prerequisites: docker, docker-compose, cargo, python3, awscli (for LocalStack).

COMPOSE = docker compose -f docker-compose.yml
AWS     = aws --endpoint-url=http://localhost:4566 --region us-east-1 \
              --no-verify-ssl \
              --aws-access-key-id krishiv \
              --aws-secret-access-key krishiv

KAFKA_BOOTSTRAP = localhost:9092
KINESIS_STREAM  = krishiv-events
S3_BUCKET       = krishiv-data

# ── Infrastructure ─────────────────────────────────────────────────────────

.PHONY: infra-up infra-down infra-status infra-wait seed-kafka seed-aws seed-all

infra-up:
	$(COMPOSE) up -d kafka schema-registry postgres localstack minio elasticsearch cassandra pulsar
	@echo "Waiting for services…"
	@$(MAKE) infra-wait

infra-down:
	$(COMPOSE) down -v

infra-status:
	$(COMPOSE) ps

infra-ui:
	$(COMPOSE) --profile debug up -d kafka-ui
	@echo "Kafka UI → http://localhost:8090"

infra-wait:
	@echo "[wait] kafka…"
	@until docker exec krishiv-kafka kafka-broker-api-versions \
		--bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 2; done
	@echo "[wait] elasticsearch…"
	@until curl -sf http://localhost:9200/_cluster/health?wait_for_status=yellow >/dev/null; do sleep 3; done
	@echo "[wait] cassandra…"
	@until docker exec krishiv-cassandra cqlsh -e "describe keyspaces" >/dev/null 2>&1; do sleep 5; done
	@echo "[wait] localstack…"
	@until curl -sf http://localhost:4566/_localstack/health | grep -q '"kinesis": "available"'; do sleep 2; done
	@echo "[wait] minio…"
	@until curl -sf http://localhost:9000/minio/health/live >/dev/null; do sleep 2; done
	@echo "All services ready."

# ── Seed data ──────────────────────────────────────────────────────────────

seed-kafka:
	@echo "Creating Kafka topics…"
	docker exec krishiv-kafka kafka-topics \
		--bootstrap-server localhost:9092 --create --if-not-exists \
		--topic orders --partitions 3 --replication-factor 1
	docker exec krishiv-kafka kafka-topics \
		--bootstrap-server localhost:9092 --create --if-not-exists \
		--topic transactions --partitions 3 --replication-factor 1
	docker exec krishiv-kafka kafka-topics \
		--bootstrap-server localhost:9092 --create --if-not-exists \
		--topic fraud-alerts --partitions 1 --replication-factor 1
	docker exec krishiv-kafka kafka-topics \
		--bootstrap-server localhost:9092 --create --if-not-exists \
		--topic clickstream --partitions 3 --replication-factor 1
	@echo "Producing seed orders…"
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		echo '{"order_id":'"$$i"',"customer":"user_'"$$i"'","product":"item_'"$$i"'","amount":'"$$((RANDOM % 500 + 10))"',"ts":'"$$(($(shell date +%s) * 1000))"'}' | \
		docker exec -i krishiv-kafka kafka-console-producer \
			--bootstrap-server localhost:9092 --topic orders; \
	done
	@echo "Producing seed transactions (for CEP)…"
	@echo '{"user_id":"u1","event":"login","amount":0,"ts":1000}' | \
		docker exec -i krishiv-kafka kafka-console-producer \
			--bootstrap-server localhost:9092 --topic transactions
	@echo '{"user_id":"u1","event":"purchase","amount":50,"ts":2000}' | \
		docker exec -i krishiv-kafka kafka-console-producer \
			--bootstrap-server localhost:9092 --topic transactions
	@echo '{"user_id":"u1","event":"large_txn","amount":9500,"ts":3000}' | \
		docker exec -i krishiv-kafka kafka-console-producer \
			--bootstrap-server localhost:9092 --topic transactions

seed-aws:
	@echo "Creating Kinesis stream…"
	$(AWS) kinesis create-stream --stream-name $(KINESIS_STREAM) --shard-count 1 || true
	@sleep 2
	@echo "Putting Kinesis records…"
	@for i in 1 2 3 4 5; do \
		$(AWS) kinesis put-record \
			--stream-name $(KINESIS_STREAM) \
			--partition-key pk-$$i \
			--data "$$(echo '{"sensor_id":"s'"$$i"'","temp":'"$$((20 + RANDOM % 30))"',"ts":'"$$(($(shell date +%s) * 1000))"'}' | base64)"; \
	done
	@echo "Creating S3 bucket and uploading seed parquet…"
	$(AWS) s3 mb s3://$(S3_BUCKET) 2>/dev/null || true
	$(AWS) s3 cp /tmp/seed.parquet s3://$(S3_BUCKET)/input/seed.parquet 2>/dev/null || \
		echo "(seed parquet not found — run ent_10 which generates it)"

seed-cassandra:
	@echo "Creating Cassandra keyspace + table…"
	docker exec krishiv-cassandra cqlsh -e \
		"CREATE KEYSPACE IF NOT EXISTS krishiv WITH replication = {'class':'SimpleStrategy','replication_factor':1};"
	docker exec krishiv-cassandra cqlsh -e \
		"CREATE TABLE IF NOT EXISTS krishiv.orders (order_id bigint PRIMARY KEY, customer text, product text, amount decimal, status text);"

seed-all: seed-kafka seed-aws seed-cassandra
	@echo "All seed data loaded."

# ── Debezium connector registration ────────────────────────────────────────

debezium-up:
	$(COMPOSE) up -d debezium
	@echo "Waiting for Debezium…"
	@until curl -sf http://localhost:8083/connectors >/dev/null; do sleep 5; done
	curl -sf -X POST http://localhost:8083/connectors \
		-H 'Content-Type: application/json' \
		-d @debezium-connector.json
	@echo "Debezium connector registered."

# ── Rust examples ──────────────────────────────────────────────────────────

RUST_BIN = cargo run -p krishiv-enterprise-examples --release --bin

.PHONY: rust-all $(addprefix rust-,01 02 03 04 05 06 07 08 09 10)

rust-01:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) $(RUST_BIN) ent_01_kafka_parquet_at_least_once
rust-02:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) $(RUST_BIN) ent_02_kafka_parquet_exactly_once
rust-03:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) $(RUST_BIN) ent_03_cdc_debezium_to_delta
rust-04:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) $(RUST_BIN) ent_04_kafka_tumbling_window
rust-05:
	AWS_ENDPOINT_URL=http://localhost:4566 $(RUST_BIN) ent_05_kinesis_to_parquet
rust-06:
	ELASTICSEARCH_URL=http://localhost:9200 $(RUST_BIN) ent_06_parquet_to_elasticsearch
rust-07:
	CASSANDRA_HOST=localhost $(RUST_BIN) ent_07_parquet_to_cassandra
rust-08:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) $(RUST_BIN) ent_08_multi_source_join
rust-09:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) $(RUST_BIN) ent_09_cep_fraud_detection
rust-10:
	AWS_ENDPOINT_URL=http://localhost:4566 \
	AWS_ACCESS_KEY_ID=krishiv AWS_SECRET_ACCESS_KEY=krishiv \
	$(RUST_BIN) ent_10_s3_etl_pipeline

rust-all:
	@for t in 01 02 03 04 05 06 07 08 09 10; do $(MAKE) rust-$$t || exit 1; done

# ── Python examples ────────────────────────────────────────────────────────

PY = python3

.PHONY: py-install py-all $(addprefix py-,01 02 03 04 05 06 07 08 09 10)

py-install:
	pip install -r python/requirements.txt

py-01:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) $(PY) python/ent_01_kafka_parquet_at_least_once.py
py-02:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) $(PY) python/ent_02_kafka_parquet_exactly_once.py
py-03:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) $(PY) python/ent_03_cdc_debezium_to_delta.py
py-04:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) $(PY) python/ent_04_kafka_tumbling_window.py
py-05:
	AWS_ENDPOINT_URL=http://localhost:4566 $(PY) python/ent_05_kinesis_to_parquet.py
py-06:
	ELASTICSEARCH_URL=http://localhost:9200 $(PY) python/ent_06_parquet_to_elasticsearch.py
py-07:
	CASSANDRA_HOST=localhost $(PY) python/ent_07_parquet_to_cassandra.py
py-08:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) $(PY) python/ent_08_multi_source_join.py
py-09:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) $(PY) python/ent_09_cep_fraud_detection.py
py-10:
	AWS_ENDPOINT_URL=http://localhost:4566 \
	AWS_ACCESS_KEY_ID=krishiv AWS_SECRET_ACCESS_KEY=krishiv \
	$(PY) python/ent_10_s3_etl_pipeline.py

py-all:
	@for t in 01 02 03 04 05 06 07 08 09 10; do $(MAKE) py-$$t || exit 1; done

# ── Convenience ────────────────────────────────────────────────────────────

.PHONY: all clean logs

all: infra-up seed-all rust-all py-all

logs:
	$(COMPOSE) logs -f --tail=50

clean:
	$(COMPOSE) down -v
	rm -rf /tmp/krishiv-enterprise-*
