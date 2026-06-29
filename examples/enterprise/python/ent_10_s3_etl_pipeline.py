"""Enterprise 10 · S3/MinIO ETL pipeline: read Parquet → transform → write output

Full batch ETL using S3-compatible storage (LocalStack or MinIO):
  1. Generate synthetic IoT sensor data and upload to s3://krishiv-data/input/
  2. Download, filter, and enrich with pandas SQL-style operations
  3. Write curated output to s3://krishiv-data/output/curated.parquet

In production, remove AWS_ENDPOINT_URL to target real AWS S3.

Prerequisites:
    make infra-up seed-aws   (creates the bucket)

Run:
    AWS_ENDPOINT_URL=http://localhost:4566 \
    AWS_ACCESS_KEY_ID=krishiv AWS_SECRET_ACCESS_KEY=krishiv \
    python python/ent_10_s3_etl_pipeline.py
"""

import io
import os

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

S3_BUCKET  = "krishiv-data"
INPUT_KEY  = "input/sensors.parquet"
OUTPUT_KEY = "output/curated.parquet"


def get_s3_client():
    kwargs = {
        "region_name":          "us-east-1",
        "aws_access_key_id":    os.environ.get("AWS_ACCESS_KEY_ID",     "krishiv"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "krishiv"),
    }
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("s3", **kwargs)


def main() -> None:
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "https://s3.amazonaws.com")
    print("=== Enterprise 10 (Python): S3 ETL Pipeline ===")
    print(f"  S3 endpoint : {endpoint}")
    print(f"  input       : s3://{S3_BUCKET}/{INPUT_KEY}")
    print(f"  output      : s3://{S3_BUCKET}/{OUTPUT_KEY}")

    s3 = get_s3_client()

    # 1 ── Ensure bucket exists (LocalStack / MinIO).
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=S3_BUCKET)
        print(f"  created bucket s3://{S3_BUCKET}")

    # 2 ── Upload synthetic input data if not already present.
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=INPUT_KEY)
        print("  input already in S3 — downloading…")
        raw = _s3_download(s3, INPUT_KEY)
        df = pq.read_table(io.BytesIO(raw)).to_pandas()
    except Exception:
        print("  generating synthetic sensor data…")
        df = _make_sensor_df()
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df), buf)
        buf.seek(0)
        s3.put_object(Bucket=S3_BUCKET, Key=INPUT_KEY, Body=buf.read())
        print(f"  uploaded {len(df)} sensor records to s3://{S3_BUCKET}/{INPUT_KEY}")

    # 3 ── SQL-style transformation with pandas.
    print(f"\n  raw sensor stats ({len(df)} records):")
    print(df.groupby("sensor_id")["temp_c"].agg(["count", "mean", "max"]).round(2))

    # Filter invalid humidity, add Fahrenheit, bucket by temperature.
    curated = df[df["humidity_pct"].between(10, 95)].copy()
    curated["temp_f"] = (curated["temp_c"] * 9 / 5 + 32).round(2)
    curated["temp_bucket"] = pd.cut(
        curated["temp_c"],
        bins=[-float("inf"), 0, 15, 25, 35, float("inf")],
        labels=["freezing", "cold", "comfortable", "warm", "hot"],
    ).astype(str)
    curated["is_anomaly"] = (curated["temp_c"] > 38) | (curated["temp_c"] < -5)

    print(f"\n  {len(curated)} records after filter (removed {len(df) - len(curated)} invalid)")

    # 4 ── Upload curated output to S3.
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(curated), buf)
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=buf.read())
    print(f"\n✓ curated {len(curated)} rows → s3://{S3_BUCKET}/{OUTPUT_KEY}")

    # 5 ── Summary.
    summary = curated.groupby("temp_bucket").agg(
        count=("sensor_id", "count"),
        avg_temp=("temp_c", "mean"),
        anomalies=("is_anomaly", "sum"),
    ).round(2)
    print("\n--- Curated summary by temp bucket ---")
    print(summary.to_string())


def _s3_download(s3, key: str) -> bytes:
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return obj["Body"].read()


def _make_sensor_df() -> pd.DataFrame:
    base = 1_716_200_000_000
    return pd.DataFrame([
        {"sensor_id": "s001", "ts_ms": base + 0,       "temp_c": 22.1, "humidity_pct": 45.0, "location": "warehouse-a"},
        {"sensor_id": "s001", "ts_ms": base + 60_000,  "temp_c": 22.5, "humidity_pct": 44.5, "location": "warehouse-a"},
        {"sensor_id": "s001", "ts_ms": base + 120_000, "temp_c": 23.0, "humidity_pct": 46.0, "location": "warehouse-a"},
        {"sensor_id": "s001", "ts_ms": base + 180_000, "temp_c": 22.8, "humidity_pct": 45.5, "location": "warehouse-a"},
        {"sensor_id": "s002", "ts_ms": base + 10_000,  "temp_c": -3.5, "humidity_pct": 80.0, "location": "cold-storage"},
        {"sensor_id": "s002", "ts_ms": base + 70_000,  "temp_c": -2.0, "humidity_pct": 78.0, "location": "cold-storage"},
        {"sensor_id": "s002", "ts_ms": base + 130_000, "temp_c": -4.1, "humidity_pct": 82.0, "location": "cold-storage"},
        {"sensor_id": "s002", "ts_ms": base + 190_000, "temp_c": 39.5, "humidity_pct": 101.0,"location": "cold-storage"},  # invalid humidity
        {"sensor_id": "s003", "ts_ms": base + 20_000,  "temp_c": 18.0, "humidity_pct": 55.0, "location": "office"},
        {"sensor_id": "s003", "ts_ms": base + 80_000,  "temp_c": 18.2, "humidity_pct": 54.0, "location": "office"},
        {"sensor_id": "s003", "ts_ms": base + 140_000, "temp_c": 18.5, "humidity_pct": 56.0, "location": "office"},
    ])


if __name__ == "__main__":
    main()
