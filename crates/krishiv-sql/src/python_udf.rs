//! Distributed Python UDF execution.
//!
//! The engine's executors are pure Rust with no embedded interpreter, so a
//! Python-callable UDF cannot run in-process there. This module runs it in a
//! persistent `python3` worker subprocess instead (the model PySpark uses):
//! the client cloudpickles the callable and ships the bytes with the query; the
//! executor spawns one worker per engine and applies the UDF to each Arrow
//! batch over a length-framed stdin/stdout protocol. The worker caches each UDF
//! by id after first use, so the pickle travels once.
//!
//! Requires `python3` on `PATH` with `pyarrow` and `cloudpickle` (plus whatever
//! the UDF itself imports) available in the runtime environment.

use std::collections::HashSet;
use std::io::{Read, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use krishiv_plan::udf::{ScalarUdf, UdfError};

/// The worker program, embedded in the binary and launched via `python3 -c`.
const WORKER_SRC: &str = include_str!("udf_worker.py");

/// Worker request mode: apply the callable per row (scalar UDF).
const WORKER_MODE_SCALAR: u8 = 0;
/// Worker request mode: apply the callable to the whole accumulated group
/// (aggregate-UDF finalize), returning one scalar.
const WORKER_MODE_AGGREGATE: u8 = 1;

/// Process-global worker pool. One `python3` worker per process (executor or
/// embedded engine) is spawned lazily on first Python-UDF use and shared by all
/// engines/tasks; UDFs are distinguished by name, and access is serialized. This
/// avoids one process-spawn per UDF and keeps hot imports (numpy, a model) loaded.
pub fn global_pool() -> Result<Arc<PythonWorkerPool>, UdfError> {
    use std::sync::OnceLock;
    static POOL: OnceLock<Mutex<Option<Arc<PythonWorkerPool>>>> = OnceLock::new();
    let cell = POOL.get_or_init(|| Mutex::new(None));
    let mut guard = cell.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(pool) = guard.as_ref() {
        return Ok(Arc::clone(pool));
    }
    let pool = PythonWorkerPool::spawn()?;
    *guard = Some(Arc::clone(&pool));
    Ok(pool)
}

fn exec_err(msg: impl Into<String>) -> UdfError {
    UdfError::Execution {
        message: msg.into(),
    }
}

/// A persistent `python3` worker that applies cloudpickled UDFs over Arrow IPC.
/// One pool is shared by every Python UDF in an engine; access is serialized
/// through the mutex (one in-flight batch at a time per worker).
pub struct PythonWorkerPool {
    io: Mutex<WorkerIo>,
}

struct WorkerIo {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    /// UDF ids whose pickle has already been sent (and cached worker-side).
    sent: HashSet<String>,
}

impl std::fmt::Debug for PythonWorkerPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonWorkerPool").finish_non_exhaustive()
    }
}

impl PythonWorkerPool {
    /// Spawn the worker process. Fails if `python3` is unavailable.
    pub fn spawn() -> Result<Arc<Self>, UdfError> {
        let mut child = Command::new("python3")
            .arg("-c")
            .arg(WORKER_SRC)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| exec_err(format!("failed to spawn python3 UDF worker: {e}")))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| exec_err("worker stdin unavailable"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| exec_err("worker stdout unavailable"))?;
        Ok(Arc::new(Self {
            io: Mutex::new(WorkerIo {
                child,
                stdin,
                stdout,
                sent: HashSet::new(),
            }),
        }))
    }

    /// Apply `pickle` (the cloudpickled callable) over `batch` in scalar mode,
    /// returning one output value per input row.
    fn eval(&self, id: &str, pickle: &[u8], batch: &RecordBatch) -> Result<ArrayRef, UdfError> {
        self.eval_mode(WORKER_MODE_SCALAR, id, pickle, batch)
    }

    /// Apply `pickle` over the whole `batch` in aggregate-finalize mode: the
    /// callable receives the accumulated group's column(s) and returns a single
    /// scalar, delivered back as a one-row single-column array.
    fn eval_aggregate(
        &self,
        id: &str,
        pickle: &[u8],
        batch: &RecordBatch,
    ) -> Result<ArrayRef, UdfError> {
        self.eval_mode(WORKER_MODE_AGGREGATE, id, pickle, batch)
    }

    /// Shared request/response cycle for both worker modes. The pickle is sent
    /// to the worker only the first time an `id` is seen; later calls reuse the
    /// cached callable.
    fn eval_mode(
        &self,
        mode: u8,
        id: &str,
        pickle: &[u8],
        batch: &RecordBatch,
    ) -> Result<ArrayRef, UdfError> {
        let ipc = write_ipc(batch)?;
        let mut io = self.io.lock().unwrap_or_else(|e| e.into_inner());

        let need_pickle = !io.sent.contains(id);
        let pickle_frame: &[u8] = if need_pickle { pickle } else { &[] };
        io.stdin
            .write_all(&[mode])
            .map_err(|e| exec_err(format!("worker mode write failed: {e}")))?;
        write_frame(&mut io.stdin, id.as_bytes())?;
        write_frame(&mut io.stdin, pickle_frame)?;
        write_frame(&mut io.stdin, &ipc)?;
        io.stdin
            .flush()
            .map_err(|e| exec_err(format!("worker write failed: {e}")))?;
        if need_pickle {
            io.sent.insert(id.to_string());
        }

        let mut hdr = [0u8; 5];
        io.stdout
            .read_exact(&mut hdr)
            .map_err(|e| exec_err(format!("worker read failed (process died?): {e}")))?;
        let status = hdr[0];
        let n = u32::from_le_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut payload = vec![0u8; n];
        io.stdout
            .read_exact(&mut payload)
            .map_err(|e| exec_err(format!("worker payload read failed: {e}")))?;

        if status != 0 {
            // Worker-side failure: drop the cached id so a re-register re-sends.
            io.sent.remove(id);
            return Err(exec_err(format!(
                "python UDF '{id}': {}",
                String::from_utf8_lossy(&payload)
            )));
        }
        read_ipc_first_column(&payload)
    }
}

impl Drop for WorkerIo {
    fn drop(&mut self) {
        // Closing stdin makes the worker's read loop hit EOF and exit cleanly.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn write_frame(w: &mut impl Write, bytes: &[u8]) -> Result<(), UdfError> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| exec_err("UDF frame exceeds 4 GiB"))?
        .to_le_bytes();
    w.write_all(&len)
        .and_then(|()| w.write_all(bytes))
        .map_err(|e| exec_err(format!("worker frame write failed: {e}")))
}

fn write_ipc(batch: &RecordBatch) -> Result<Vec<u8>, UdfError> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| exec_err(format!("arrow IPC writer: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| exec_err(format!("arrow IPC write: {e}")))?;
        writer
            .finish()
            .map_err(|e| exec_err(format!("arrow IPC finish: {e}")))?;
    }
    Ok(buf)
}

fn read_ipc_first_column(bytes: &[u8]) -> Result<ArrayRef, UdfError> {
    let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| exec_err(format!("arrow IPC reader: {e}")))?;
    let batch = reader
        .next()
        .ok_or_else(|| exec_err("worker returned no batch"))?
        .map_err(|e| exec_err(format!("arrow IPC decode: {e}")))?;
    if batch.num_columns() == 0 {
        return Err(exec_err("worker returned a batch with no columns"));
    }
    Ok(Arc::clone(batch.column(0)))
}

/// A scalar UDF whose implementation is a cloudpickled Python callable executed
/// in a [`PythonWorkerPool`]. Ships to and runs on the distributed executors.
pub struct PythonWorkerUdf {
    name: String,
    pickle: Vec<u8>,
    input_schema: Schema,
    output_field: Field,
    pool: Arc<PythonWorkerPool>,
}

impl std::fmt::Debug for PythonWorkerUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonWorkerUdf")
            .field("name", &self.name)
            .field("pickle_len", &self.pickle.len())
            .finish_non_exhaustive()
    }
}

impl PythonWorkerUdf {
    pub fn new(
        name: impl Into<String>,
        pickle: Vec<u8>,
        input_schema: Schema,
        output_field: Field,
        pool: Arc<PythonWorkerPool>,
    ) -> Self {
        Self {
            name: name.into(),
            pickle,
            input_schema,
            output_field,
            pool,
        }
    }
}

impl ScalarUdf for PythonWorkerUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> &Schema {
        &self.input_schema
    }

    fn output_field(&self) -> &Field {
        &self.output_field
    }

    fn call(&self, batch: &RecordBatch) -> Result<ArrayRef, UdfError> {
        self.pool.eval(&self.name, &self.pickle, batch)
    }
}

// ── Aggregate (GROUPED_AGG) Python UDF ──────────────────────────────────────

use krishiv_plan::udf::{AggState, AggregateUdf, ScalarValue};

/// An aggregate UDF whose implementation is a cloudpickled Python callable.
///
/// Semantics follow PySpark's `GROUPED_AGG` pandas UDF: the accumulated rows of
/// a group are buffered (as Arrow IPC frames appended into [`AggState`]) and the
/// callable is applied to the whole group exactly once at finalize. This makes
/// the aggregate trivially mergeable across partitions and executors — `merge`
/// is byte concatenation of two partial buffers — so it works in distributed
/// two-phase aggregation (partial per map task, final after the shuffle) through
/// the existing [`crate::udf`] `KrishivAggregateAccumulator` bridge.
///
/// The callable receives each input column of the group as a numpy array (one
/// positional argument per input column) and returns a Python scalar; a callable
/// marked `_krishiv_arrow_udf=True` instead receives the whole Arrow batch.
pub struct PythonWorkerAggregateUdf {
    name: String,
    pickle: Vec<u8>,
    input_schema: Schema,
    output_field: Field,
    pool: Arc<PythonWorkerPool>,
}

impl std::fmt::Debug for PythonWorkerAggregateUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonWorkerAggregateUdf")
            .field("name", &self.name)
            .field("pickle_len", &self.pickle.len())
            .finish_non_exhaustive()
    }
}

impl PythonWorkerAggregateUdf {
    pub fn new(
        name: impl Into<String>,
        pickle: Vec<u8>,
        input_schema: Schema,
        output_field: Field,
        pool: Arc<PythonWorkerPool>,
    ) -> Self {
        Self {
            name: name.into(),
            pickle,
            input_schema,
            output_field,
            pool,
        }
    }
}

/// Append one Arrow-IPC-encoded batch as a length-prefixed frame onto a state
/// buffer. The buffer is a flat concatenation of `[u32 le len][ipc]` frames, so
/// `accumulate` is O(batch) and `merge` is O(1) byte concatenation.
fn push_state_frame(data: &mut Vec<u8>, ipc: &[u8]) -> Result<(), UdfError> {
    let len = u32::try_from(ipc.len())
        .map_err(|_| exec_err("aggregate state frame exceeds 4 GiB"))?;
    data.extend_from_slice(&len.to_le_bytes());
    data.extend_from_slice(ipc);
    Ok(())
}

/// Decode all length-prefixed IPC frames in a state buffer back into batches.
fn decode_state_frames(data: &[u8]) -> Result<Vec<RecordBatch>, UdfError> {
    let mut batches = Vec::new();
    let mut rest = data;
    while !rest.is_empty() {
        let (len_bytes, after_len) = rest
            .split_at_checked(4)
            .ok_or_else(|| exec_err("aggregate state truncated (length header)"))?;
        let len_arr: [u8; 4] = len_bytes
            .try_into()
            .map_err(|_| exec_err("aggregate state length header not 4 bytes"))?;
        let len = u32::from_le_bytes(len_arr) as usize;
        let (frame, remainder) = after_len
            .split_at_checked(len)
            .ok_or_else(|| exec_err("aggregate state truncated (frame body)"))?;
        rest = remainder;
        let reader = StreamReader::try_new(std::io::Cursor::new(frame), None)
            .map_err(|e| exec_err(format!("aggregate state IPC reader: {e}")))?;
        for batch in reader {
            batches.push(batch.map_err(|e| exec_err(format!("aggregate state IPC decode: {e}")))?);
        }
    }
    Ok(batches)
}

impl AggregateUdf for PythonWorkerAggregateUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> &Schema {
        &self.input_schema
    }

    fn output_field(&self) -> &Field {
        &self.output_field
    }

    fn accumulate(&self, state: &mut AggState, batch: &RecordBatch) -> Result<(), UdfError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let ipc = write_ipc(batch)?;
        push_state_frame(&mut state.data, &ipc)
    }

    fn merge(&self, mut a: AggState, b: AggState) -> Result<AggState, UdfError> {
        // Both buffers are already sequences of length-prefixed frames, so a
        // merge is exactly their concatenation.
        a.data.extend_from_slice(&b.data);
        Ok(a)
    }

    fn finalize(&self, state: AggState) -> Result<ScalarValue, UdfError> {
        let schema = Arc::new(self.input_schema.clone());
        let batches = decode_state_frames(&state.data)?;
        let combined = if batches.is_empty() {
            RecordBatch::new_empty(Arc::clone(&schema))
        } else {
            arrow::compute::concat_batches(&schema, &batches)
                .map_err(|e| exec_err(format!("aggregate concat: {e}")))?
        };
        let array = self
            .pool
            .eval_aggregate(&self.name, &self.pickle, &combined)?;
        scalar_from_array(&array, self.output_field.data_type())
    }
}

/// Extract element 0 of a one-row worker result array as a [`ScalarValue`] of
/// the declared output type, casting first so a Python `int` result satisfies a
/// declared `float64` output (and similar widenings).
fn scalar_from_array(
    array: &ArrayRef,
    want: &arrow::datatypes::DataType,
) -> Result<ScalarValue, UdfError> {
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::DataType;

    if array.is_empty() || array.is_null(0) {
        return Ok(ScalarValue::Null);
    }
    let casted = if array.data_type() == want {
        Arc::clone(array)
    } else {
        arrow::compute::cast(array, want)
            .map_err(|e| exec_err(format!("aggregate result cast to {want:?}: {e}")))?
    };
    let downcast_err = |t: &str| exec_err(format!("aggregate result not a {t} array"));
    match want {
        DataType::Float64 => {
            let a = casted
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| downcast_err("Float64"))?;
            Ok(ScalarValue::Float64(a.value(0)))
        }
        DataType::Int64 => {
            let a = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| downcast_err("Int64"))?;
            Ok(ScalarValue::Int64(a.value(0)))
        }
        DataType::Boolean => {
            let a = casted
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| downcast_err("Boolean"))?;
            Ok(ScalarValue::Boolean(a.value(0)))
        }
        DataType::Utf8 => {
            let a = casted
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| downcast_err("Utf8"))?;
            Ok(ScalarValue::Utf8(a.value(0).to_string()))
        }
        other => Err(exec_err(format!(
            "unsupported aggregate output type {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::DataType;

    /// Ask python3 to cloudpickle a lambda and return the bytes, so the Rust
    /// test exercises the real serialization path.
    fn cloudpickle(expr: &str) -> Option<Vec<u8>> {
        let out = Command::new("python3")
            .arg("-c")
            .arg(format!(
                "import sys,cloudpickle; sys.stdout.buffer.write(cloudpickle.dumps({expr}))"
            ))
            .output()
            .ok()?;
        if out.status.success() && !out.stdout.is_empty() {
            Some(out.stdout)
        } else {
            None
        }
    }

    #[test]
    fn python_worker_runs_scalar_and_caches() {
        let Some(pickle) = cloudpickle("lambda x: x + 1000") else {
            eprintln!("skipping: python3/cloudpickle unavailable");
            return;
        };
        let pool = PythonWorkerPool::spawn().expect("spawn worker");
        let udf = PythonWorkerUdf::new(
            "inc",
            pickle,
            Schema::new(vec![Field::new("a0", DataType::Int64, true)]),
            Field::new("out", DataType::Int64, true),
            pool,
        );
        let batch = RecordBatch::try_new(
            Arc::new(udf.input_schema().clone()),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        // First call sends the pickle; second reuses the cached callable.
        for _ in 0..2 {
            let out = udf.call(&batch).expect("udf call");
            let vals = out.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(vals.values(), &[1001, 1002, 1003]);
        }
    }

    #[test]
    fn python_worker_vectorized_numpy() {
        // A vectorized (arrow-native) UDF using numpy inside — the "heavy Python"
        // case that cannot be a SQL expression.
        let expr = "(lambda: (lambda f: (setattr(f, '_krishiv_arrow_udf', True), f)[1])(\
                     __import__('cloudpickle') and (lambda b: __import__('pyarrow').array(\
                     __import__('numpy').sqrt(b.column(0).to_numpy(zero_copy_only=False))))))()";
        let Some(pickle) = cloudpickle(expr) else {
            eprintln!("skipping: python3/numpy/cloudpickle unavailable");
            return;
        };
        let pool = PythonWorkerPool::spawn().expect("spawn worker");
        let udf = PythonWorkerUdf::new(
            "vsqrt",
            pickle,
            Schema::new(vec![Field::new("a0", DataType::Float64, true)]),
            Field::new("out", DataType::Float64, true),
            pool,
        );
        let batch = RecordBatch::try_new(
            Arc::new(udf.input_schema().clone()),
            vec![Arc::new(Float64Array::from(vec![4.0, 9.0, 16.0]))],
        )
        .unwrap();
        let out = udf.call(&batch).expect("udf call");
        let vals = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(vals.values(), &[2.0, 3.0, 4.0]);
    }

    #[test]
    fn python_aggregate_merges_partial_states() {
        // Geometric mean = exp(mean(log(x))): a genuinely custom aggregate that
        // is not a SQL SUM/AVG. Split the input across two partial states and
        // merge them, exercising the distributed two-phase path.
        let expr = "lambda a: float(__import__('numpy').exp(__import__('numpy').log(a).mean()))";
        let Some(pickle) = cloudpickle(expr) else {
            eprintln!("skipping: python3/numpy/cloudpickle unavailable");
            return;
        };
        let pool = PythonWorkerPool::spawn().expect("spawn worker");
        let udf = PythonWorkerAggregateUdf::new(
            "geomean",
            pickle,
            Schema::new(vec![Field::new("a0", DataType::Float64, true)]),
            Field::new("out", DataType::Float64, true),
            pool,
        );
        let schema = Arc::new(udf.input_schema().clone());
        let mk = |vals: Vec<f64>| {
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Float64Array::from(vals))])
                .unwrap()
        };

        // Partition A accumulates {1,2,4}; partition B accumulates {8,16}.
        let mut state_a = AggState::default();
        udf.accumulate(&mut state_a, &mk(vec![1.0, 2.0, 4.0]))
            .unwrap();
        let mut state_b = AggState::default();
        udf.accumulate(&mut state_b, &mk(vec![8.0, 16.0])).unwrap();

        let merged = udf.merge(state_a, state_b).unwrap();
        let result = udf.finalize(merged).expect("finalize");
        match result {
            ScalarValue::Float64(v) => assert!(
                (v - 4.0).abs() < 1e-9,
                "geomean of 1,2,4,8,16 should be 4.0, got {v}"
            ),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn python_aggregate_int_result_casts_to_declared_type() {
        // A Python callable returning an int64 count, declared as int64 output.
        let Some(pickle) = cloudpickle("lambda a: int(len(a))") else {
            eprintln!("skipping: python3/cloudpickle unavailable");
            return;
        };
        let pool = PythonWorkerPool::spawn().expect("spawn worker");
        let udf = PythonWorkerAggregateUdf::new(
            "cnt",
            pickle,
            Schema::new(vec![Field::new("a0", DataType::Int64, true)]),
            Field::new("out", DataType::Int64, true),
            pool,
        );
        let schema = Arc::new(udf.input_schema().clone());
        let mut state = AggState::default();
        udf.accumulate(
            &mut state,
            &RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![5, 6, 7, 8]))])
                .unwrap(),
        )
        .unwrap();
        match udf.finalize(state).expect("finalize") {
            ScalarValue::Int64(v) => assert_eq!(v, 4),
            other => panic!("expected Int64, got {other:?}"),
        }
    }
}
