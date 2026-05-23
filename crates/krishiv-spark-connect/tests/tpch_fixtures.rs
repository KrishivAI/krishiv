//! Minimal TPC-H parquet fixtures for Spark Connect TPC-H tests.

use std::path::Path;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use std::fs::File;

pub fn write_tpch_mini_dataset(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    write_lineitem(dir)?;
    write_orders(dir)?;
    write_customer(dir)?;
    write_part(dir)?;
    write_partsupp(dir)?;
    write_supplier(dir)?;
    write_nation(dir)?;
    write_region(dir)?;
    Ok(())
}

fn write_parquet(path: &Path, schema: Schema, batches: Vec<arrow::record_batch::RecordBatch>) -> std::io::Result<()> {
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), None).map_err(|e| std::io::Error::other(e.to_string()))?;
    for b in batches {
        writer.write(&b).map_err(|e| std::io::Error::other(e.to_string()))?;
    }
    writer.close().map(|_| ()).map_err(|e| std::io::Error::other(e.to_string()))
}

fn write_lineitem(dir: &Path) -> std::io::Result<()> {
    let schema = Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, false),
    ]);
    let d = 10592i32; // ~1998-12-01
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Float64Array::from(vec![10.0])),
            Arc::new(Float64Array::from(vec![1000.0])),
            Arc::new(Float64Array::from(vec![0.05])),
            Arc::new(Float64Array::from(vec![0.08])),
            Arc::new(StringArray::from(vec!["N"])),
            Arc::new(StringArray::from(vec!["O"])),
            Arc::new(Date32Array::from(vec![d])),
            Arc::new(Date32Array::from(vec![d])),
            Arc::new(Date32Array::from(vec![d])),
            Arc::new(StringArray::from(vec!["DELIVER"])),
            Arc::new(StringArray::from(vec!["TRUCK"])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .map_err(|e| std::io::Error::other(e.to_string()))?;
    write_parquet(&dir.join("lineitem.parquet"), schema, vec![batch])
}

fn write_orders(dir: &Path) -> std::io::Result<()> {
    let schema = Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_clerk", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8, false),
    ]);
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["O"])),
            Arc::new(Float64Array::from(vec![1000.0])),
            Arc::new(Date32Array::from(vec![10592])),
            Arc::new(StringArray::from(vec!["1-URGENT"])),
            Arc::new(StringArray::from(vec!["clerk"])),
            Arc::new(Int32Array::from(vec![0])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .map_err(|e| std::io::Error::other(e.to_string()))?;
    write_parquet(&dir.join("orders.parquet"), schema, vec![batch])
}

fn write_customer(dir: &Path) -> std::io::Result<()> {
    let schema = Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int64, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_acctbal", DataType::Float64, false),
        Field::new("c_mktsegment", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, false),
    ]);
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Customer#1"])),
            Arc::new(StringArray::from(vec!["Addr"])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["1-000-000-0001"])),
            Arc::new(Float64Array::from(vec![1000.0])),
            Arc::new(StringArray::from(vec!["BUILDING"])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .map_err(|e| std::io::Error::other(e.to_string()))?;
    write_parquet(&dir.join("customer.parquet"), schema, vec![batch])
}

fn write_part(dir: &Path) -> std::io::Result<()> {
    let schema = Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8, false),
        Field::new("p_retailprice", DataType::Float64, false),
        Field::new("p_comment", DataType::Utf8, false),
    ]);
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["part"])),
            Arc::new(StringArray::from(vec!["mfgr"])),
            Arc::new(StringArray::from(vec!["brand"])),
            Arc::new(StringArray::from(vec!["TYPE"])),
            Arc::new(Int32Array::from(vec![15])),
            Arc::new(StringArray::from(vec!["SM BOX"])),
            Arc::new(Float64Array::from(vec![100.0])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .map_err(|e| std::io::Error::other(e.to_string()))?;
    write_parquet(&dir.join("part.parquet"), schema, vec![batch])
}

fn write_partsupp(dir: &Path) -> std::io::Result<()> {
    let schema = Schema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("ps_suppkey", DataType::Int64, false),
        Field::new("ps_availqty", DataType::Int32, false),
        Field::new("ps_supplycost", DataType::Float64, false),
        Field::new("ps_comment", DataType::Utf8, false),
    ]);
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![100])),
            Arc::new(Float64Array::from(vec![10.0])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .map_err(|e| std::io::Error::other(e.to_string()))?;
    write_parquet(&dir.join("partsupp.parquet"), schema, vec![batch])
}

fn write_supplier(dir: &Path) -> std::io::Result<()> {
    let schema = Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, false),
        Field::new("s_nationkey", DataType::Int64, false),
        Field::new("s_phone", DataType::Utf8, false),
        Field::new("s_acctbal", DataType::Float64, false),
        Field::new("s_comment", DataType::Utf8, false),
    ]);
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Supplier#1"])),
            Arc::new(StringArray::from(vec!["Addr"])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["1-000-000-0001"])),
            Arc::new(Float64Array::from(vec![1000.0])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .map_err(|e| std::io::Error::other(e.to_string()))?;
    write_parquet(&dir.join("supplier.parquet"), schema, vec![batch])
}

fn write_nation(dir: &Path) -> std::io::Result<()> {
    let schema = Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, false),
        Field::new("n_regionkey", DataType::Int64, false),
        Field::new("n_comment", DataType::Utf8, false),
    ]);
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["UNITED STATES"])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .map_err(|e| std::io::Error::other(e.to_string()))?;
    write_parquet(&dir.join("nation.parquet"), schema, vec![batch])
}

fn write_region(dir: &Path) -> std::io::Result<()> {
    let schema = Schema::new(vec![
        Field::new("r_regionkey", DataType::Int64, false),
        Field::new("r_name", DataType::Utf8, false),
        Field::new("r_comment", DataType::Utf8, false),
    ]);
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["AMERICA"])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .map_err(|e| std::io::Error::other(e.to_string()))?;
    write_parquet(&dir.join("region.parquet"), schema, vec![batch])
}
