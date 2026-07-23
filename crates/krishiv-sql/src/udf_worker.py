"""Persistent Python UDF worker: the execution runtime the Rust executors will
spawn. Protocol over stdin/stdout, length-framed:

  request  = [u8 mode][u32 udf_id_len][udf_id][u32 pickle_len][pickle_or_empty][u32 ipc_len][arrow_ipc_batch]
  response = [u8 status][u32 len][payload]   status 0=ok(arrow ipc, 1 col), 1=error(utf8 msg)

`mode` selects the apply strategy:
  0 = scalar   — apply the callable to the input batch, return one value per row.
  1 = aggregate — apply the callable to the whole input column(s) (a GROUPED_AGG
      finalize over the accumulated group), return a single-row, single-column
      batch holding the scalar result.

The worker caches each UDF by id after first registration (pickle sent once; later
calls send an empty pickle). A callable marked `_krishiv_arrow_udf=True` receives
the whole batch (vectorized); otherwise, for scalar mode it is applied per row
over the batch's columns, and for aggregate mode it receives each input column as
a numpy array (a single positional arg per input column)."""
import struct
import sys
import io
import cloudpickle
import pyarrow as pa

_CACHE = {}


def _read_exact(f, n):
    buf = b""
    while len(buf) < n:
        chunk = f.read(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def _read_frame(f):
    hdr = _read_exact(f, 4)
    if hdr is None:
        return None
    (n,) = struct.unpack("<I", hdr)
    return _read_exact(f, n) if n else b""


def _apply_scalar(fn, batch):
    if getattr(fn, "_krishiv_arrow_udf", False):
        out = fn(batch)
        return out if isinstance(out, (pa.Array, pa.ChunkedArray)) else pa.array(out)
    cols = [batch.column(i).to_pylist() for i in range(batch.num_columns)]
    return pa.array([fn(*row) for row in zip(*cols)])


def _apply_aggregate(fn, batch):
    # GROUPED_AGG finalize: hand the whole accumulated group to the callable and
    # wrap its scalar result as a one-element array so the reply is a 1-row batch.
    if getattr(fn, "_krishiv_arrow_udf", False):
        result = fn(batch)
    else:
        cols = [
            batch.column(i).to_numpy(zero_copy_only=False)
            for i in range(batch.num_columns)
        ]
        result = fn(*cols)
    return pa.array([result])


def main():
    inp, out = sys.stdin.buffer, sys.stdout.buffer
    while True:
        mode_byte = _read_exact(inp, 1)
        if mode_byte is None:
            return
        mode = mode_byte[0]
        udf_id = _read_frame(inp)
        if udf_id is None:
            return
        udf_id = udf_id.decode()
        pickle = _read_frame(inp)
        ipc = _read_frame(inp)
        try:
            if pickle:
                _CACHE[udf_id] = cloudpickle.loads(pickle)
            fn = _CACHE[udf_id]
            reader = pa.ipc.open_stream(pa.BufferReader(ipc))
            batch = reader.read_next_batch()
            result = _apply_aggregate(fn, batch) if mode == 1 else _apply_scalar(fn, batch)
            rb = pa.RecordBatch.from_arrays([result], names=["out"])
            sink = io.BytesIO()
            with pa.ipc.new_stream(sink, rb.schema) as w:
                w.write_batch(rb)
            payload = sink.getvalue()
            out.write(struct.pack("<BI", 0, len(payload)))
            out.write(payload)
        except Exception as e:  # noqa: BLE001
            msg = f"{type(e).__name__}: {e}".encode()
            out.write(struct.pack("<BI", 1, len(msg)))
            out.write(msg)
        out.flush()


if __name__ == "__main__":
    main()
