//! Every TPC-H query, staged, in every configuration the cluster can be in.
//!
//! # Why this file exists
//!
//! `krishiv-sql` has staged tests for q17, q19 and q22 — three of twenty-two —
//! and those three exist because each was already failing on the cluster. Every
//! other query's distributed correctness was only ever checked by running SF100,
//! which costs hours and answers "did it error", not "did it answer correctly".
//!
//! Worse, the configuration matrix had a hole. Conversion was tested *with*
//! broadcast; no-broadcast was tested with joins *unconverted*. SF100 is the one
//! cell that is both at once — no build side fits under the 32 MiB broadcast
//! ceiling, and all of them exceed the spill threshold — and in that cell q17
//! and q19 both failed, for weeks, with a bare Arrow type error.
//!
//! So: all 22 queries, all four algorithm/broadcast combinations, against the
//! fixture in [`krishiv_bench::tpch_fixture`]. Each case runs the plan through
//! the real stage cutter, encodes and decodes every fragment, and routes real
//! batches through a shuffle store — then compares against single-node
//! execution of the same query.
//!
//! It runs in seconds. The point is that a bug like q17's should never again be
//! something a cluster has to discover.

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use krishiv_bench::tpch_fixture::{fixture_ddl, row_producing_queries};
use krishiv_bench::tpch_queries::TPCH_QUERIES;
use krishiv_sql::distributed_plan::{
    ShufflePartitionReader, build_distributed_stages, execute_dfplan_body,
    fragment_decode_session_context, planning_session_context_with_options,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// In-memory stand-in for the shuffle service, keyed exactly as the cluster
/// keys it: (upstream stage, map task, partition).
#[derive(Debug, Default)]
struct StageStore {
    partitions: Mutex<HashMap<(usize, usize, usize), Vec<RecordBatch>>>,
}

/// The trait is `krishiv-sql`'s and `Arc` is `std`'s, so the impl needs a local
/// type to hang off.
#[derive(Debug)]
struct StoreReader(Arc<StageStore>);

impl ShufflePartitionReader for StoreReader {
    fn read_partition(
        &self,
        upstream_stage_index: usize,
        map_task_index: usize,
        partition: usize,
    ) -> futures::future::BoxFuture<'static, Result<Vec<RecordBatch>, String>> {
        let found = self
            .0
            .partitions
            .lock()
            .map(|p| {
                p.get(&(upstream_stage_index, map_task_index, partition))
                    .cloned()
                    .unwrap_or_default()
            })
            .map_err(|_| String::from("stage store poisoned"));
        Box::pin(async move { found })
    }
}

/// Route a batch to `num_partitions` buckets by a key column. Any consistent
/// hash is correct — the cluster's choice of hash is not what is under test.
fn route(batch: &RecordBatch, key_column: &str, num_partitions: usize) -> Vec<RecordBatch> {
    use std::hash::{Hash as _, Hasher as _};
    let Ok(key_idx) = batch.schema().index_of(key_column) else {
        // The declared key is not in this batch's schema; send everything to
        // one bucket rather than silently dropping rows.
        return (0..num_partitions)
            .map(|p| {
                if p == 0 {
                    batch.clone()
                } else {
                    RecordBatch::new_empty(batch.schema())
                }
            })
            .collect();
    };
    let column = batch.column(key_idx);
    let mut selections: Vec<Vec<u32>> = vec![Vec::new(); num_partitions];
    for row in 0..batch.num_rows() {
        let value = datafusion::arrow::util::display::array_value_to_string(column, row)
            .expect("value renders");
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        value.hash(&mut hasher);
        let bucket = usize::try_from(hasher.finish() % num_partitions as u64).unwrap_or(0);
        selections[bucket].push(u32::try_from(row).expect("row fits u32"));
    }
    selections
        .into_iter()
        .map(|rows| {
            let indices = datafusion::arrow::array::UInt32Array::from(rows);
            datafusion::arrow::compute::take_record_batch(batch, &indices).expect("take")
        })
        .collect()
}

/// Run every stage in dependency order, as the cluster does.
async fn run_staged(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let df = ctx.sql(sql).await.map_err(|e| e.to_string())?;
    let plan = df.create_physical_plan().await.map_err(|e| e.to_string())?;
    let staged = build_distributed_stages(plan)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| String::from("declined to stage"))?;

    let store = Arc::new(StageStore::default());
    let exec_ctx = fragment_decode_session_context();
    let mut result = Vec::new();
    for (stage_index, stage) in staged.stages.iter().enumerate() {
        for (task_index, body) in stage.task_bodies.iter().enumerate() {
            let reader: Arc<dyn ShufflePartitionReader> =
                Arc::new(StoreReader(Arc::clone(&store)));
            let (declared, mut stream) = execute_dfplan_body(body, &exec_ctx, Some(reader))
                .map_err(|e| format!("stage {stage_index} task {task_index} start: {e}"))?;
            while let Some(batch) = futures::StreamExt::next(&mut stream).await {
                let batch =
                    batch.map_err(|e| format!("stage {stage_index} task {task_index}: {e}"))?;
                // The invariant a same-process store would otherwise hide: a
                // stage whose declared schema disagrees with the batches it
                // produces only dies on the wire, where the reduce side
                // concatenates real IPC data against the declared schema.
                if batch.schema() != declared {
                    return Err(format!(
                        "stage {stage_index} task {task_index} declares {declared:?} but produced \
                         {:?}",
                        batch.schema()
                    ));
                }
                if batch.num_rows() == 0 {
                    continue;
                }
                match &stage.shuffle {
                    None => result.push(batch),
                    Some(shuffle) => {
                        let buckets = match shuffle.key_columns.first() {
                            Some(key) => route(&batch, key, shuffle.num_output_partitions),
                            // A keyless shuffle is a gather: everything to 0.
                            None => vec![batch],
                        };
                        for (bucket, part) in buckets.into_iter().enumerate() {
                            if part.num_rows() > 0 {
                                store
                                    .partitions
                                    .lock()
                                    .expect("store lock")
                                    .entry((stage_index, task_index, bucket))
                                    .or_default()
                                    .push(part);
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(result)
}

/// Every cell of every row, sorted. Stage output order is not single-node
/// order, so only a set comparison is meaningful — and only *cells* catch a
/// plan that returns the right shape with the wrong values.
fn cells(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(move |r| {
                (0..b.num_columns())
                    .map(|c| {
                        datafusion::arrow::util::display::array_value_to_string(b.column(c), r)
                            .expect("cell")
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
        })
        .collect();
    rows.sort();
    rows
}

async fn context(join_threshold: Option<u64>, broadcast_bytes: Option<usize>) -> SessionContext {
    let ctx = planning_session_context_with_options(4, join_threshold, broadcast_bytes);
    for ddl in fixture_ddl() {
        ctx.sql(ddl)
            .await
            .unwrap_or_else(|e| panic!("fixture DDL failed: {e}\n{ddl}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("fixture DDL execution failed: {e}\n{ddl}"));
    }
    ctx
}

/// The four cells. `join_threshold: Some(0)` converts every join it can;
/// `broadcast_bytes: Some(0)` forbids collecting a build side, so both sides
/// hash-shuffle. The last row is the SF100 configuration.
const MATRIX: &[(&str, Option<u64>, Option<usize>)] = &[
    ("hash + broadcast", None, None),
    ("hash + no-broadcast", None, Some(0)),
    ("converted + broadcast", Some(0), None),
    ("converted + no-broadcast", Some(0), Some(0)),
];

#[tokio::test(flavor = "multi_thread")]
async fn every_tpch_query_stages_to_the_same_answer_in_every_configuration() {
    let mut failures: Vec<String> = Vec::new();
    let mut checked = 0usize;

    for (label, threshold, broadcast) in MATRIX {
        let ctx = context(*threshold, *broadcast).await;
        for query in TPCH_QUERIES {
            let expected = match ctx.sql(query.sql).await {
                Ok(df) => match df.collect().await {
                    Ok(batches) => cells(&batches),
                    Err(e) => {
                        failures.push(format!("{label}/{}: direct execution failed: {e}", query.id));
                        continue;
                    }
                },
                Err(e) => {
                    failures.push(format!("{label}/{}: direct planning failed: {e}", query.id));
                    continue;
                }
            };
            match run_staged(&ctx, query.sql).await {
                Ok(actual) => {
                    checked += 1;
                    let actual = cells(&actual);
                    if actual != expected {
                        failures.push(format!(
                            "{label}/{}: staged answer differs\n  staged:   {actual:?}\n  \
                             single-node: {expected:?}",
                            query.id
                        ));
                    }
                }
                // A query with no exchange to cut is legitimately un-staged —
                // it is a single task by construction, not a failure.
                Err(e) if e.contains("no exchange to cut") => {}
                Err(e) => failures.push(format!("{label}/{}: {e}", query.id)),
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} staged cases disagreed with single-node execution:\n{}",
        failures.len(),
        checked,
        failures.join("\n")
    );
    assert!(
        checked >= 40,
        "only {checked} cases actually staged; the matrix has stopped covering anything"
    );
}

/// The fixture must keep matching the queries' predicates.
///
/// Without this, tuning the fixture could quietly reduce the test above to
/// twenty-two comparisons of empty against empty — which passes, and proves
/// nothing at all.
#[tokio::test(flavor = "multi_thread")]
async fn the_fixture_still_produces_rows_for_the_queries_it_claims() {
    let ctx = context(None, None).await;
    let mut empty: Vec<&str> = Vec::new();
    for id in row_producing_queries() {
        let query = TPCH_QUERIES
            .iter()
            .find(|q| q.id == *id)
            .unwrap_or_else(|| panic!("{id} is not in the corpus"));
        let batches = ctx
            .sql(query.sql)
            .await
            .unwrap_or_else(|e| panic!("{id}: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{id}: {e}"));
        if batches.iter().all(|b| b.num_rows() == 0) {
            empty.push(id);
        }
    }
    assert!(
        empty.is_empty(),
        "the fixture no longer produces rows for {empty:?}; the staged comparison for those \
         queries is now vacuous"
    );
}

/// Every query in the corpus must at least *plan* against the fixture schema.
///
/// A typo in a column name would otherwise surface as "direct execution
/// failed" inside the big matrix test, mixed in with real disagreements.
#[tokio::test(flavor = "multi_thread")]
async fn every_query_plans_against_the_fixture_schema() {
    let ctx = context(None, None).await;
    let mut broken = Vec::new();
    for query in TPCH_QUERIES {
        if let Err(e) = ctx.sql(query.sql).await {
            broken.push(format!("{}: {e}", query.id));
        }
    }
    assert!(broken.is_empty(), "queries do not plan:\n{}", broken.join("\n"));
    assert_eq!(TPCH_QUERIES.len(), 22);
}
