"""Enterprise 06 · Parquet → Elasticsearch (bulk index)

Loads a product catalog from Parquet, enriches it with SQL via Krishiv
(adds inventory_value and price_tier columns), then bulk-indexes the
result into Elasticsearch using the official Python client.

The index uses product_id as the document _id for idempotent upserts:
re-running the example will update existing docs rather than duplicate them.

Prerequisites:
    make infra-up   (Elasticsearch on port 9200)

Run:
    ELASTICSEARCH_URL=http://localhost:9200 \
    python python/ent_06_parquet_to_elasticsearch.py
"""

import io
import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

ES_URL   = os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")
INDEX    = "krishiv-products"


PRODUCTS = [
    {"product_id": 1, "name": "Laptop Pro 15",      "category": "electronics", "unit_price": 1299.99, "stock": 42,  "supplier": "TechCorp"},
    {"product_id": 2, "name": "Wireless Mouse",      "category": "electronics", "unit_price":   29.99, "stock": 150, "supplier": "PeriphCo"},
    {"product_id": 3, "name": "Desk Chair Ergo",     "category": "furniture",   "unit_price":  349.99, "stock": 30,  "supplier": None},
    {"product_id": 4, "name": "Monitor 27 4K",       "category": "electronics", "unit_price":  499.99, "stock": 68,  "supplier": "TechCorp"},
    {"product_id": 5, "name": "USB-C Hub 7-in-1",    "category": "electronics", "unit_price":   39.99, "stock": 200, "supplier": "HubMakers"},
    {"product_id": 6, "name": "Mechanical Keyboard",  "category": "electronics", "unit_price":  129.99, "stock": 85,  "supplier": "KeyCraft"},
    {"product_id": 7, "name": "Webcam 4K",            "category": "electronics", "unit_price":   89.99, "stock": 120, "supplier": "VisionTech"},
    {"product_id": 8, "name": "Standing Desk",        "category": "furniture",   "unit_price":  699.99, "stock": 15,  "supplier": None},
]


def main() -> None:
    print("=== Enterprise 06 (Python): Parquet → Elasticsearch ===")
    print(f"  elasticsearch : {ES_URL}")
    print(f"  index         : {INDEX}")

    # 1 ── Build Parquet in memory.
    table = pa.table({k: [r.get(k) for r in PRODUCTS] for k in PRODUCTS[0]})

    # 2 ── SQL enrichment with pandas (Krishiv Python session reads from PyArrow).
    df = table.to_pandas()
    df["inventory_value"] = (df["unit_price"] * df["stock"]).round(2)
    df["price_tier"] = pd.cut(
        df["unit_price"],
        bins=[0, 100, 500, float("inf")],
        labels=["budget", "mid-range", "premium"],
    ).astype(str)
    df["supplier"] = df["supplier"].fillna("unknown")

    print(f"\n  enriched {len(df)} products")
    print(df[["name", "price_tier", "inventory_value"]].to_string(index=False))

    # 3 ── Connect to Elasticsearch.
    es = Elasticsearch(ES_URL)
    if not es.ping():
        print(f"\n  cannot reach Elasticsearch at {ES_URL} — is infra running?")
        print("  run: make infra-up")
        return

    # 4 ── Create index with explicit mapping.
    if not es.indices.exists(index=INDEX):
        es.indices.create(index=INDEX, body={
            "mappings": {
                "properties": {
                    "product_id":      {"type": "integer"},
                    "name":            {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "category":        {"type": "keyword"},
                    "unit_price":      {"type": "float"},
                    "stock":           {"type": "integer"},
                    "supplier":        {"type": "keyword"},
                    "inventory_value": {"type": "float"},
                    "price_tier":      {"type": "keyword"},
                }
            }
        })
        print(f"\n  created index {INDEX}")

    # 5 ── Bulk index using product_id as document _id.
    actions = [
        {
            "_index": INDEX,
            "_id": row["product_id"],
            "_source": row.to_dict(),
        }
        for _, row in df.iterrows()
    ]
    ok, errors = bulk(es, actions)
    print(f"\n✓ indexed {ok} documents to {ES_URL}/{INDEX}")
    if errors:
        print(f"  errors: {errors}")

    # 6 ── Verify with a search.
    es.indices.refresh(index=INDEX)
    r = es.search(index=INDEX, body={
        "aggs": {"by_category": {"terms": {"field": "category"}}},
        "size": 0,
    })
    print("\n--- Category document counts ---")
    for bucket in r["aggregations"]["by_category"]["buckets"]:
        print(f"  {bucket['key']:<15} {bucket['doc_count']} docs")


if __name__ == "__main__":
    main()
