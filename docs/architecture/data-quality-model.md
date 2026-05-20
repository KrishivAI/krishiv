# Data Quality Rule Model

## Design

Data quality enforcement is applied **at the sink boundary**: rules are evaluated per record batch before the batch is written to the sink. This placement ensures that quality failures are caught at write time, regardless of the upstream source or transformation chain.

A `DataQualityRule` is a predicate + action. Rules are attached to a `SinkConfig` via `DataQualityConfig`. Rules do not affect the query planner or scan path; they are a post-execution, pre-write gate.

---

## Rule Types

```rust
#[non_exhaustive]
pub enum DataQualityRule {
    /// Column value must not be NULL.
    NotNull { column: String },

    /// Numeric column value must be within [min, max] inclusive.
    Range { column: String, min: f64, max: f64 },

    /// String column value must match the given regular expression.
    Regex { column: String, pattern: String },

    /// Arbitrary boolean expression evaluated as a DataFusion SQL predicate.
    /// Expression receives the record batch schema; must return Boolean.
    Custom { expression: String },
}
```

`DataQualityRule` is `#[non_exhaustive]`: future rule types can be added without breaking stable consumers.

---

## Actions on Violation

```rust
#[non_exhaustive]
pub enum QualityAction {
    /// Abort the entire batch. The write does not proceed. Error returned to the job.
    Fail,

    /// Route the offending row to the RejectedRowOutput. Remaining rows proceed.
    Reject,

    /// Emit a metric counter, pass the row through unchanged. Non-blocking.
    Warn,
}
```

Each rule carries its own action, or falls back to the `DataQualityConfig::default_action`.

---

## `DataQualityConfig`

Attached to a `SinkConfig`:

```rust
pub struct DataQualityConfig {
    pub rules: Vec<(DataQualityRule, Option<QualityAction>)>,
    /// Action applied when a rule has no per-rule override.
    pub default_action: QualityAction,
    /// Configuration for the rejected-row output, if any.
    pub rejected_row_output: Option<RejectedRowOutputConfig>,
}
```

---

## Rejected-Row Output

Rows violating a `Reject`-action rule are routed to a `RejectedRowOutput`. This is a side-channel attached to the sink. Three output modes are supported:

| Mode | Description |
|---|---|
| `Log` | Write rejection details to structured log. Default if `rejected_row_output` is `None`. |
| `ParquetFile { path }` | Append rejected rows + error metadata to a secondary Parquet file. |
| `DeadLetter { sink_config }` | Write rejected rows to a `DeadLetterSink` backed by an arbitrary `Sink`. |

### Dead-Letter Sink

`DeadLetterSink` wraps a `Sink` and writes each rejected row as an enriched record with additional error metadata columns:

| Column | Type | Description |
|---|---|---|
| `_dq_rule_name` | Utf8 | Name of the violated rule (derived from `DataQualityRule` variant + column) |
| `_dq_column_name` | Utf8 | Column involved in the violation (empty for `Custom` rules) |
| `_dq_row_index` | Int64 | Zero-based row index within the batch |
| `_dq_timestamp` | TimestampMillisecond | Wall-clock time of the violation |
| `_dq_original_*` | original types | All original columns from the rejected row |

The dead-letter schema is fixed (error metadata columns) plus a passthrough of the original row columns. The dead-letter sink target can be any certified `Sink` implementation (e.g., a secondary `LocalParquetTwoPhaseCommitSink`).

---

## Metrics

Both metric counters are emitted via `krishiv-metrics` and exported via the OTLP exporter:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `krishiv.data_quality.rows_rejected_total` | Counter | `rule`, `table` | Total rows rejected (action = Reject or Fail) |
| `krishiv.data_quality.rows_passed_total` | Counter | `table` | Total rows that passed all quality checks |
| `krishiv.data_quality.batches_failed_total` | Counter | `table` | Total batches aborted due to Fail-action violation |

Labels:
- `rule`: the rule identifier string (e.g., `not_null:order_id`, `range:amount`, `custom:expr_0`)
- `table`: the sink's target table or file path

---

## Execution Model

Quality evaluation is performed inside the `QualityGateSink` wrapper, which wraps the downstream `Sink`:

```
upstream operator → QualityGateSink::write_batch(batch)
                        │
                        ├─ evaluate rules against batch (DataFusion expr eval)
                        │
                        ├─ split: passing rows → downstream Sink::write_batch
                        │
                        └─ failing rows → RejectedRowOutput::emit(row, metadata)
```

`QualityGateSink` implements the `Sink` trait so it is compositional: it can wrap any sink without modification to the downstream sink.

`Custom { expression }` rules are compiled to DataFusion physical expressions at pipeline startup. Compilation failure is a startup error, not a runtime error.

---

## Configuration Example

```toml
[sink.data_quality]
default_action = "reject"

[[sink.data_quality.rules]]
type = "not_null"
column = "order_id"
action = "fail"

[[sink.data_quality.rules]]
type = "range"
column = "amount"
min = 0.0
max = 1_000_000.0
action = "reject"

[[sink.data_quality.rules]]
type = "regex"
column = "email"
pattern = "^[^@]+@[^@]+\\.[^@]+$"
action = "warn"

[sink.data_quality.rejected_row_output]
mode = "dead_letter"
sink_type = "local_parquet"
path = "/data/rejected/orders/"
```
