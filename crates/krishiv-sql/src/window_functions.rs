//! SQL window helper functions: TUMBLE_START, TUMBLE_END, HOP_START, HOP_END.
//!
//! These scalar UDFs let users write windowed aggregations in standard SQL:
//!
//! ```sql
//! SELECT
//!     tumble_start(ts, 60000) AS window_start,
//!     tumble_end(ts, 60000)   AS window_end,
//!     user_id,
//!     COUNT(*) AS cnt
//! FROM events
//! GROUP BY tumble_start(ts, 60000), user_id
//! ```
//!
//! All functions operate on `Int64` millisecond timestamps and return `Int64`.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::SessionContext;

/// Register all window helper UDFs with the DataFusion session context.
pub fn register_window_functions(ctx: &SessionContext) -> Result<(), DataFusionError> {
    ctx.register_udf(make_tumble_start());
    ctx.register_udf(make_tumble_end());
    ctx.register_udf(make_hop_start());
    ctx.register_udf(make_hop_first_start());
    ctx.register_udf(make_hop_end());
    ctx.register_udf(make_session_start());
    ctx.register_udf(make_session_end());
    Ok(())
}

/// TUMBLE_START(ts_ms, window_size_ms) → floor of the window containing `ts`.
///
/// Returns `ts - (ts % size)` (truncated to window boundary).
fn make_tumble_start() -> datafusion::logical_expr::ScalarUDF {
    create_udf(
        "tumble_start",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let a0 = args
                .first()
                .ok_or_else(|| DataFusionError::Internal("tumble_start: missing arg 0".into()))?;
            let a1 = args
                .get(1)
                .ok_or_else(|| DataFusionError::Internal("tumble_start: missing arg 1".into()))?;
            apply2(a0, a1, |t, s| if s != 0 { t - t.rem_euclid(s) } else { t })
        }),
    )
}

/// TUMBLE_END(ts_ms, window_size_ms) → exclusive end of the window containing `ts`.
///
/// Returns `TUMBLE_START(ts, size) + size`.
fn make_tumble_end() -> datafusion::logical_expr::ScalarUDF {
    create_udf(
        "tumble_end",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let a0 = args
                .first()
                .ok_or_else(|| DataFusionError::Internal("tumble_end: missing arg 0".into()))?;
            let a1 = args
                .get(1)
                .ok_or_else(|| DataFusionError::Internal("tumble_end: missing arg 1".into()))?;
            apply2(
                a0,
                a1,
                |t, s| {
                    if s != 0 { t - t.rem_euclid(s) + s } else { t }
                },
            )
        }),
    )
}

/// HOP_START(ts_ms, slide_ms, window_size_ms) → start of the hop window slot containing `ts`.
///
/// Returns `floor(ts / slide) * slide` using Euclidean floor division so
/// negative timestamps map to the correct preceding window boundary.
fn make_hop_start() -> datafusion::logical_expr::ScalarUDF {
    create_udf(
        "hop_start",
        vec![DataType::Int64, DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let a0 = args
                .first()
                .ok_or_else(|| DataFusionError::Internal("hop_start: missing arg 0".into()))?;
            let a1 = args
                .get(1)
                .ok_or_else(|| DataFusionError::Internal("hop_start: missing arg 1".into()))?;
            let a2 = args
                .get(2)
                .ok_or_else(|| DataFusionError::Internal("hop_start: missing arg 2".into()))?;
            apply3(
                a0,
                a1,
                a2,
                |t, sl, _sz| {
                    if sl != 0 { t - t.rem_euclid(sl) } else { t }
                },
            )
        }),
    )
}

/// HOP_FIRST_START(ts_ms, slide_ms, window_size_ms) → the *earliest* hop window
/// start whose window still contains `ts`.
///
/// A hopping window of `size` sliding by `slide` places each event in
/// `ceil(size / slide)` overlapping windows, not one. Window `[s, s + size)`
/// contains `ts` exactly when `ts - size < s <= ts`, so the earliest qualifying
/// start is the smallest multiple of `slide` strictly greater than `ts - size`:
///
/// ```text
/// first = (floor_div(ts - size, slide) + 1) * slide
/// last  = HOP_START(ts, slide, size)
/// ```
///
/// Together with [`make_hop_start`] this bounds the series of window starts an
/// event belongs to, which is what `streaming_tvf` expands with
/// `generate_series` + `unnest`. Without it a HOP TVF could only ever emit one
/// row per event, silently undercounting every grouped aggregate by
/// `size / slide`.
///
/// Euclidean division is used so negative timestamps floor toward `-inf` rather
/// than truncating toward zero.
fn make_hop_first_start() -> datafusion::logical_expr::ScalarUDF {
    create_udf(
        "hop_first_start",
        vec![DataType::Int64, DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let a0 = args.first().ok_or_else(|| {
                DataFusionError::Internal("hop_first_start: missing arg 0".into())
            })?;
            let a1 = args.get(1).ok_or_else(|| {
                DataFusionError::Internal("hop_first_start: missing arg 1".into())
            })?;
            let a2 = args.get(2).ok_or_else(|| {
                DataFusionError::Internal("hop_first_start: missing arg 2".into())
            })?;
            apply3(a0, a1, a2, |t, sl, sz| {
                if sl != 0 {
                    (t.saturating_sub(sz).div_euclid(sl) + 1).saturating_mul(sl)
                } else {
                    t
                }
            })
        }),
    )
}

/// HOP_END(ts_ms, slide_ms, window_size_ms) → end of the hop window slot containing `ts`.
///
/// Returns `HOP_START(ts, slide, size) + size`.
fn make_hop_end() -> datafusion::logical_expr::ScalarUDF {
    create_udf(
        "hop_end",
        vec![DataType::Int64, DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let a0 = args
                .first()
                .ok_or_else(|| DataFusionError::Internal("hop_end: missing arg 0".into()))?;
            let a1 = args
                .get(1)
                .ok_or_else(|| DataFusionError::Internal("hop_end: missing arg 1".into()))?;
            let a2 = args
                .get(2)
                .ok_or_else(|| DataFusionError::Internal("hop_end: missing arg 2".into()))?;
            apply3(a0, a1, a2, |t, sl, sz| {
                if sl != 0 {
                    t - t.rem_euclid(sl) + sz
                } else {
                    t
                }
            })
        }),
    )
}

/// SESSION_START(ts_ms, gap_ms) → marker for the session window start.
///
/// In the TVF rewrite, session boundaries require a reduce-side computation
/// (group consecutive events within `gap_ms` of each other).  This UDF returns
/// a pessimistic estimate: the event timestamp itself, so that `GROUP BY
/// session_start(ts, gap)` groups identically-timestamped events together.
/// Real session window merging is performed by the streaming executor when
/// the query is dispatched to `GroupStateExecutor` via the session window
/// operator.
fn make_session_start() -> datafusion::logical_expr::ScalarUDF {
    create_udf(
        "session_start",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let a0 = args
                .first()
                .ok_or_else(|| DataFusionError::Internal("session_start: missing arg 0".into()))?;
            let a1 = args
                .get(1)
                .ok_or_else(|| DataFusionError::Internal("session_start: missing arg 1".into()))?;
            // Return the event timestamp as the provisional session start.
            apply2(a0, a1, |t, _gap| t)
        }),
    )
}

/// SESSION_END(ts_ms, gap_ms) → provisional session end estimate.
///
/// Returns `ts + gap_ms` as the upper bound of the session window containing
/// the event.  Final session merging is performed by the streaming executor.
fn make_session_end() -> datafusion::logical_expr::ScalarUDF {
    create_udf(
        "session_end",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let a0 = args
                .first()
                .ok_or_else(|| DataFusionError::Internal("session_end: missing arg 0".into()))?;
            let a1 = args
                .get(1)
                .ok_or_else(|| DataFusionError::Internal("session_end: missing arg 1".into()))?;
            apply2(a0, a1, |t, gap| t + gap)
        }),
    )
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn cast_to_int64_array(args: &[ColumnarValue], idx: usize) -> Result<Int64Array, DataFusionError> {
    use datafusion::scalar::ScalarValue;
    match args.get(idx).ok_or_else(|| {
        DataFusionError::Internal(format!("window function: missing argument {idx}"))
    })? {
        ColumnarValue::Array(arr) => {
            let typed = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "window function argument {idx} expected Int64, got {:?}",
                    arr.data_type()
                ))
            })?;
            Ok(typed.clone())
        }
        ColumnarValue::Scalar(ScalarValue::Int64(v)) => {
            // Broadcast scalar to a single-element array for uniform iteration.
            Ok(Int64Array::from(vec![*v]))
        }
        ColumnarValue::Scalar(other) => Err(DataFusionError::Internal(format!(
            "window function argument {idx} expected Int64 scalar, got {other:?}"
        ))),
    }
}

/// Apply a 2-arg function over two ColumnarValues, handling all-scalar case efficiently.
fn apply2(
    lhs: &ColumnarValue,
    rhs: &ColumnarValue,
    f: impl Fn(i64, i64) -> i64,
) -> Result<ColumnarValue, DataFusionError> {
    use datafusion::scalar::ScalarValue;
    // Fast path: both scalars → return scalar.
    if let (
        ColumnarValue::Scalar(ScalarValue::Int64(a)),
        ColumnarValue::Scalar(ScalarValue::Int64(b)),
    ) = (lhs, rhs)
    {
        let result = match (a, b) {
            (Some(a), Some(b)) => Some(f(*a, *b)),
            _ => None,
        };
        return Ok(ColumnarValue::Scalar(ScalarValue::Int64(result)));
    }
    // Array path.
    let a_arr = cast_to_int64_array(std::slice::from_ref(lhs), 0)?;
    let b_arr = cast_to_int64_array(std::slice::from_ref(rhs), 0)?;
    if a_arr.is_empty() || b_arr.is_empty() {
        return Ok(ColumnarValue::Array(
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())) as ArrayRef,
        ));
    }
    if a_arr.len() != 1 && b_arr.len() != 1 && a_arr.len() != b_arr.len() {
        return Err(DataFusionError::Internal(format!(
            "window function: incompatible array lengths {} and {}",
            a_arr.len(),
            b_arr.len()
        )));
    }
    let len = a_arr.len().max(b_arr.len());
    let a_val = |i: usize| {
        if a_arr.len() == 1 {
            a_arr.value(0)
        } else {
            a_arr.value(i)
        }
    };
    let b_val = |i: usize| {
        if b_arr.len() == 1 {
            b_arr.value(0)
        } else {
            b_arr.value(i)
        }
    };
    let result: Int64Array = (0..len)
        .map(|i| {
            if a_arr.is_null(i.min(a_arr.len() - 1)) || b_arr.is_null(i.min(b_arr.len() - 1)) {
                None
            } else {
                Some(f(a_val(i), b_val(i)))
            }
        })
        .collect();
    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
}

/// Apply a 3-arg function over three ColumnarValues, handling all-scalar case efficiently.
fn apply3(
    a: &ColumnarValue,
    b: &ColumnarValue,
    c: &ColumnarValue,
    f: impl Fn(i64, i64, i64) -> i64,
) -> Result<ColumnarValue, DataFusionError> {
    use datafusion::scalar::ScalarValue;
    if let (
        ColumnarValue::Scalar(ScalarValue::Int64(va)),
        ColumnarValue::Scalar(ScalarValue::Int64(vb)),
        ColumnarValue::Scalar(ScalarValue::Int64(vc)),
    ) = (a, b, c)
    {
        let result = match (va, vb, vc) {
            (Some(a), Some(b), Some(c)) => Some(f(*a, *b, *c)),
            _ => None,
        };
        return Ok(ColumnarValue::Scalar(ScalarValue::Int64(result)));
    }
    let a_arr = cast_to_int64_array(std::slice::from_ref(a), 0)?;
    let b_arr = cast_to_int64_array(std::slice::from_ref(b), 0)?;
    let c_arr = cast_to_int64_array(std::slice::from_ref(c), 0)?;
    if a_arr.is_empty() || b_arr.is_empty() || c_arr.is_empty() {
        return Ok(ColumnarValue::Array(
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())) as ArrayRef,
        ));
    }
    let max_len = a_arr.len().max(b_arr.len()).max(c_arr.len());
    for (name, len) in [("a", a_arr.len()), ("b", b_arr.len()), ("c", c_arr.len())] {
        if len != 1 && len != max_len {
            return Err(DataFusionError::Internal(format!(
                "window function: argument '{name}' length {len} incompatible with max length {max_len}"
            )));
        }
    }
    let a_val = |i: usize| {
        if a_arr.len() == 1 {
            a_arr.value(0)
        } else {
            a_arr.value(i)
        }
    };
    let b_val = |i: usize| {
        if b_arr.len() == 1 {
            b_arr.value(0)
        } else {
            b_arr.value(i)
        }
    };
    let c_val = |i: usize| {
        if c_arr.len() == 1 {
            c_arr.value(0)
        } else {
            c_arr.value(i)
        }
    };
    let result: Int64Array = (0..max_len)
        .map(|i| {
            let ai = i.min(a_arr.len() - 1);
            let bi = i.min(b_arr.len() - 1);
            let ci = i.min(c_arr.len() - 1);
            if a_arr.is_null(ai) || b_arr.is_null(bi) || c_arr.is_null(ci) {
                None
            } else {
                Some(f(a_val(i), b_val(i), c_val(i)))
            }
        })
        .collect();
    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
}

#[cfg(test)]
mod tests {
    use arrow::array::cast::AsArray;
    use arrow::datatypes::Int64Type;

    use super::*;

    fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        register_window_functions(&ctx).unwrap();
        ctx
    }

    async fn query_i64(ctx: &SessionContext, sql: &str) -> i64 {
        let result = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        // DataFusion may fold constant expressions to a dict-encoded or primitive array.
        let col = result.first().expect("empty result").column(0);
        // Try Int64Array directly.
        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            return arr.value(0);
        }
        // Try via as_primitive helper.
        col.as_primitive::<Int64Type>().value(0)
    }

    #[tokio::test]
    async fn tumble_start_aligns_to_window() {
        let ctx = make_ctx();
        let val = query_i64(&ctx, "SELECT tumble_start(65000, 60000) AS ws").await;
        assert_eq!(val, 60000, "65s → window starting at 60s");
    }

    #[tokio::test]
    async fn tumble_end_is_start_plus_size() {
        let ctx = make_ctx();
        let val = query_i64(&ctx, "SELECT tumble_end(65000, 60000) AS we").await;
        assert_eq!(val, 120000, "window end = 60000 + 60000");
    }

    #[tokio::test]
    async fn hop_start_aligns_to_slide() {
        let ctx = make_ctx();
        let val = query_i64(&ctx, "SELECT hop_start(65000, 30000, 60000) AS hs").await;
        assert_eq!(val, 60000, "65s / 30s slide → hop start at 60s");
    }

    #[tokio::test]
    async fn hop_end_is_start_plus_size() {
        let ctx = make_ctx();
        let val = query_i64(&ctx, "SELECT hop_end(65000, 30000, 60000) AS he").await;
        assert_eq!(val, 120000, "hop end = 60000 + 60000");
    }

    /// `hop_first_start` must bound the series of windows containing `ts`.
    #[tokio::test]
    async fn hop_first_start_is_the_earliest_window_still_containing_ts() {
        let ctx = make_ctx();
        // size 60s sliding by 30s: t=65s sits in [30s,90s) and [60s,120s).
        let first = query_i64(&ctx, "SELECT hop_first_start(65000, 30000, 60000)").await;
        let last = query_i64(&ctx, "SELECT hop_start(65000, 30000, 60000)").await;
        assert_eq!(first, 30000, "earliest window start still covering 65s");
        assert_eq!(last, 60000, "latest window start at or before 65s");

        // t=95s: [60s,120s) and [90s,150s) — but NOT [30s,90s).
        assert_eq!(
            query_i64(&ctx, "SELECT hop_first_start(95000, 30000, 60000)").await,
            60000
        );

        // slide == size degenerates to tumbling: exactly one window.
        assert_eq!(
            query_i64(&ctx, "SELECT hop_first_start(65000, 60000, 60000)").await,
            60000
        );
    }

    /// A HOP TVF must emit `size / slide` rows per event, not one.
    ///
    /// The rewrite was a scalar projection, which can only ever produce one row
    /// per input row — so every grouped aggregate over a HOP undercounted by
    /// exactly this factor while the docs promised overlapping windows.
    #[tokio::test]
    async fn hop_tvf_fans_one_event_into_every_window_containing_it() {
        let engine = crate::SqlEngine::new();
        engine
            .sql("CREATE TABLE ev (ts BIGINT, k VARCHAR) AS VALUES (65000, 'a')")
            .await
            .expect("create")
            .collect()
            .await
            .expect("collect");

        let batches = engine
            .sql(
                "SELECT window_start, window_end FROM \
                 HOP(TABLE ev, DESCRIPTOR(ts), 30000, 60000) ORDER BY window_start",
            )
            .await
            .expect("plan HOP")
            .collect()
            .await
            .expect("execute HOP");

        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows, 2,
            "60s window sliding by 30s puts one event in 2 windows, got {rows}"
        );

        let starts = batches
            .first()
            .expect("a batch")
            .column(0)
            .as_primitive::<Int64Type>();
        assert_eq!(starts.value(0), 30000);
        assert_eq!(starts.value(1), 60000);
    }

    #[tokio::test]
    async fn window_functions_work_on_table_column() {
        let ctx = make_ctx();
        register_window_functions(&ctx).unwrap();
        ctx.sql(
            "CREATE TABLE events (ts BIGINT, user_id VARCHAR) AS VALUES (65000, 'alice'), (130000, 'bob')"
        ).await.unwrap().collect().await.unwrap();
        let result = ctx
            .sql("SELECT tumble_start(ts, 60000), user_id FROM events ORDER BY ts")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let starts = result
            .first()
            .expect("empty result")
            .column(0)
            .as_primitive::<Int64Type>();
        assert_eq!(starts.value(0), 60000);
        assert_eq!(starts.value(1), 120000);
    }
}
