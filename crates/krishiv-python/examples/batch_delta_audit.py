#!/usr/bin/env python3
"""Delta Lake time-travel query and audit batch example in Python."""

import os
import json
import time
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq
import krishiv as ks

def write_delta(root_path, table, mode="append"):
    """Helper to write local Delta Lake commits (Parquet + JSON metadata)."""
    log_dir = os.path.join(root_path, "_delta_log")
    os.makedirs(log_dir, exist_ok=True)
    
    # Get next version number
    max_ver = -1
    for f in os.listdir(log_dir):
        if f.endswith(".json"):
            try:
                v = int(f.split(".")[0])
                if v > max_ver:
                    max_ver = v
            except ValueError:
                pass
    version = max_ver + 1
    
    # Overwrite processing
    removed_files = []
    if mode == "overwrite":
        for f in os.listdir(root_path):
            if f.endswith(".parquet"):
                fp = os.path.join(root_path, f)
                removed_files.append(f)
                try:
                    os.remove(fp)
                except OSError:
                    pass
                    
    # Write data parquet file
    file_name = f"part-{version:05d}.parquet"
    file_path = os.path.join(root_path, file_name)
    pq.write_table(table, file_path)
    file_size = os.path.getsize(file_path)
    
    # Write delta commit JSON
    commit_info = {"commitInfo": {"operation": "WRITE", "timestamp": int(time.time() * 1000)}}
    add_info = {"add": {"path": file_name, "size": file_size, "dataChange": True}}
    
    log_path = os.path.join(log_dir, f"{version:020d}.json")
    with open(log_path, "w") as f:
        f.write(json.dumps(commit_info) + "\n")
        f.write(json.dumps(add_info) + "\n")
        for removed in removed_files:
            f.write(json.dumps({"remove": {"path": removed, "dataChange": True}}) + "\n")

def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        delta_path = os.path.join(temp_dir, "my_delta_table")
        os.makedirs(delta_path, exist_ok=True)

        # 1. Create a Delta Lake table and write Version 0
        schema = pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
        ])
        table_v0 = pa.Table.from_pydict({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
        }, schema=schema)
        
        write_delta(delta_path, table_v0, mode="overwrite")

        # 2. Append to the table to create Version 1
        table_v1 = pa.Table.from_pydict({
            "id": [3],
            "name": ["Charlie"],
        }, schema=schema)
        
        write_delta(delta_path, table_v1, mode="append")

        # 3. Build the embedded session
        session = ks.Session.from_env()

        # 4. Query the latest version (Version 1)
        print("--- Current Version (Latest) ---")
        current_df = ks.read_delta(session, delta_path, version=None)
        print(current_df.collect().pretty())

        # 5. Query the historical version 0 (Time Travel!)
        print("--- Historical Version 0 ---")
        historical_df = ks.read_delta(session, delta_path, version=0)
        print(historical_df.collect().pretty())

if __name__ == "__main__":
    main()
