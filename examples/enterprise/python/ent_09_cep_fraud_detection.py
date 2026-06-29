"""Enterprise 09 · CEP fraud detection: Kafka transactions → fraud alerts

Reads user transaction events from the "transactions" Kafka topic and applies
a stateful sequence-matching pattern per user_id:

  PATTERN: login → purchase (< 60 s) → large_txn amount > 5000 (< 30 s)

Matches are published to the "fraud-alerts" Kafka topic as JSON.
Implements PartitionedCepMatcher logic in pure Python (no external library
needed — state is a dict of per-user partial-match stacks).

Prerequisites:
    make infra-up seed-kafka

Run:
    KAFKA_BOOTSTRAP=localhost:9092 python python/ent_09_cep_fraud_detection.py
"""

import json
import os
import time
from dataclasses import dataclass, field
from typing import Optional

from confluent_kafka import Consumer, Producer, KafkaException

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
IN_TOPIC  = "transactions"
OUT_TOPIC = "fraud-alerts"
GROUP_ID  = "krishiv-ent-py-09-cep"
TIMEOUT_S = 20

SYNTHETIC_EVENTS = [
    # u1: fraud sequence
    {"user_id": "u1", "event": "login",     "amount": 0.0,    "ts": 1_000},
    {"user_id": "u1", "event": "purchase",  "amount": 50.0,   "ts": 2_000},
    {"user_id": "u1", "event": "large_txn", "amount": 9500.0, "ts": 3_000},
    # u2: incomplete (no large_txn)
    {"user_id": "u2", "event": "login",     "amount": 0.0,    "ts": 1_000},
    {"user_id": "u2", "event": "purchase",  "amount": 120.0,  "ts": 2_000},
    # u3: out-of-order (large_txn without login first)
    {"user_id": "u3", "event": "large_txn", "amount": 8000.0, "ts": 1_000},
]

# Pattern definition: each step is (event_type, amount_condition, max_gap_ms_from_prev)
PATTERN_STEPS = [
    ("login",     lambda a: True,       None),
    ("purchase",  lambda a: True,       60_000),
    ("large_txn", lambda a: a > 5000.0, 30_000),
]


@dataclass
class PartialMatch:
    step: int = 0
    events: list[dict] = field(default_factory=list)
    last_ts: int = 0


def main() -> None:
    print("=== Enterprise 09 (Python): CEP Fraud Detection ===")
    print(f"  source : kafka://{BOOTSTRAP}  topic={IN_TOPIC}")
    print(f"  alerts : {OUT_TOPIC}")

    events = _collect_kafka(IN_TOPIC, TIMEOUT_S)
    if not events:
        print("  no live Kafka — using synthetic transaction events")
        events = SYNTHETIC_EVENTS

    state: dict[str, PartialMatch] = {}
    alerts: list[dict] = []
    stats = {"events": 0, "matched": 0, "expired": 0}

    for e in sorted(events, key=lambda x: x.get("ts", 0)):
        stats["events"] += 1
        user_id = e.get("user_id", "?")
        event_type = e.get("event", "")
        amount = float(e.get("amount", 0))
        ts = int(e.get("ts", 0))

        pm = state.get(user_id, PartialMatch())

        step_type, step_cond, max_gap = PATTERN_STEPS[pm.step]

        if event_type == step_type and step_cond(amount):
            # Check time gap constraint.
            if pm.step > 0 and max_gap is not None:
                if ts - pm.last_ts > max_gap:
                    print(f"  [{user_id}] gap expired at step {pm.step} — reset")
                    stats["expired"] += 1
                    pm = PartialMatch()  # reset

            pm.events.append(e)
            pm.last_ts = ts
            pm.step += 1

            if pm.step == len(PATTERN_STEPS):
                # Pattern complete!
                alert = {
                    "alert_type": "fraud_sequence",
                    "user_id":    user_id,
                    "severity":   "HIGH",
                    "events":     pm.events,
                    "total_amount": sum(x.get("amount", 0) for x in pm.events),
                }
                alerts.append(alert)
                stats["matched"] += 1
                print(f"\n  FRAUD ALERT for {user_id}: {json.dumps(alert, indent=2)}")
                del state[user_id]
                continue

        state[user_id] = pm

    # Publish alerts to Kafka.
    if alerts:
        producer = Producer({"bootstrap.servers": BOOTSTRAP})
        for alert in alerts:
            producer.produce(
                OUT_TOPIC,
                key=alert["user_id"].encode(),
                value=json.dumps(alert).encode(),
            )
        producer.flush(timeout=5)
        print(f"\n✓ {len(alerts)} fraud alert(s) published to kafka://{OUT_TOPIC}")
    else:
        print("\n  no fraud patterns matched")

    print("\n--- CEP stats ---")
    print(f"  events processed : {stats['events']}")
    print(f"  patterns matched : {stats['matched']}")
    print(f"  patterns expired : {stats['expired']}")
    print(f"  open partitions  : {len(state)}")


def _collect_kafka(topic: str, timeout_s: float) -> list[dict]:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "enable.auto.commit": True,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])
    events: list[dict] = []
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                break
            try:
                events.append(json.loads(msg.value().decode("utf-8")))
            except Exception:
                continue
    finally:
        consumer.close()
    return events


if __name__ == "__main__":
    main()
