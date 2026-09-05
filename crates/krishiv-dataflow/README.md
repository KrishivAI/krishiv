# krishiv-dataflow

Arrow-native streaming operators and the loop-policy kernel: tumbling,
sliding, session and count windows (in-memory and state-backed),
`ContinuousWindowExecutor`, watermarks (single and multi-source, idle
policy), interval and delta joins, dedup, CEP (`MATCH_RECOGNIZE`),
process functions with keyed state and timers, connected streams and
broadcast state, side outputs, the checkpoint-aware `OperatorQueue` and
`BarrierAligner`, `stream_driver` (the closed `StreamingLoop`/`DriverPolicy`
that every loop must answer) and `streaming_corpus` (cross-loop conformance).

Documentation: `docs/architecture/08-streaming.md`,
`docs/architecture/07-state-checkpoints-savepoints.md`.

License: Apache-2.0.
