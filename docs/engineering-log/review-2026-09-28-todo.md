# Codebase review 2026-09-28 — todo list

Source: full-tree review (11 units, read-only). Per-finding scenario/evidence/fix is in the
unit reports; none of these were in `crate-audit-register.md` at review time.
Status: `[ ]` open · `[~]` in progress · `[x]` fixed (with test) · `[-]` decided not to fix.

## Gates
- [x] G1 `cargo deny` fails: rustls 0.23.43 RUSTSEC-2026-0285 → `cargo update -p rustls` (>=0.23.45)
  - fixed 2026-09-28: rustls 0.23.45; `cargo deny check advisories` ok

## High
- [x] H1 `krishiv-mcp/src/lib.rs:1992` read-only gate is first-word only; multi-statement `EXPLAIN SELECT 1; DROP …` runs the DROP (register §20 note is wrong)
  - fixed 2026-09-28: sqlparser single-statement read-only classifier; test `execute_sql_rejects_write_hidden_behind_read_only_prefix`, `read_only_classifier`
- [x] H2 `krishiv-mcp/src/lib.rs:1038` `explain_sql` has no read-only gate at all (runs DDL; `analyze` runs DML)
  - fixed 2026-09-28: gate applied; test `explain_sql_rejects_write_sql_by_default`
- [x] H3 `krishiv-flight-sql/src/service.rs:1727` DoAction `BatchSql`/`BatchSqlSink`/`ExecutePlan` skip `check_table_access`
  - fixed 2026-09-28: `check_table_access` on Explain/ExecutePlan/BatchSql/BatchSqlSink; `action_policy_tests`
- [x] H4 `krishiv-sql/src/semi_join_reduction.rs:588` semi-join pushdown drops `null_aware`/`null_equality` → wrong NOT IN / EXCEPT ALL / INTERSECT ALL
  - fixed 2026-09-28: rules decline `null_aware` / `NullEqualsNull` joins (also covers M12); `not_in_with_null_in_subquery_keeps_null_aware_semantics`, `set_operations_keep_null_equals_null`
- [x] H5 `krishiv-sql/src/lakehouse/merge.rs:91` MERGE INTO keeps only first ON equality; ignores SET / INSERT lists
  - fixed 2026-09-28: refuses compound ON / SET lists / INSERT column lists (typed Unsupported); `merge_refuses_what_it_would_not_execute`
- [x] H6 `krishiv-connectors/src/lakehouse/delta_lake.rs:100` + `local_delta.rs:610` `merge_delta` has no read-version conflict check → concurrent append silently removed
  - fixed 2026-09-28: merge reads a pinned version and commits exactly on top of it (`write_table_based_on`); `overwrite_based_on_a_stale_read_is_a_conflict` (mutation-checked)
- [x] H7 `krishiv-connectors/src/lakehouse/local_delta.rs:140` + `delta_lake.rs:193` Delta reader ignores checkpoints / protocol / deletion vectors / partition values → wrong rows on foreign tables
  - fixed 2026-09-28: reader fails closed on checkpoints / non-contiguous log / DV / partitions / column mapping / reader>1, both local and object-store readers; `foreign_delta_features_are_refused_not_misread`
- [x] H8 `krishiv-connectors/src/kafka_transactional_sink.rs:236` "exactly-once" sink cannot recover a prepared txn after crash; 30 s txn timeout
  - fixed 2026-09-29: durable profiles refuse the sink unless `KRISHIV_KAFKA_SINK_ALLOW_UNRECOVERABLE_TXN=1` (`new_for_profile`, both executor call sites); crash recovery of a prepared txn remains unimplemented and is now an explicit opt-in; `durable_profiles_need_an_explicit_opt_in`
- [x] H9 `krishiv-connectors/src/elasticsearch_sink.rs:299` (+ `cassandra_sink.rs:261`, `hbase_connector.rs:280`) non-primitive types incl. `Utf8View` written as null
  - fixed 2026-09-28: shared `cast_columns_for_sink`; views/large → base, ES/HBase render the rest as strings, Cassandra errors on unmappable types
- [x] H10 `krishiv-scheduler/src/etcd_metadata.rs:292` metadata puts not fenced on leadership → deposed leader overwrites new leader's job records
  - fixed 2026-09-28: `EtcdLeaderFence`: every metadata put/delete is a Txn comparing the leader key's lease; wired when `--leader-backend etcd`; live test `a_deposed_leader_cannot_overwrite_the_new_leaders_records` (mutation-checked against a local etcd)
- [x] H11 `krishiv-ivm/src/plan.rs:548` Chain checkpoints TopN/KeyedTopN/Sessionize as empty but restore returns `Ok(true)`
  - fixed 2026-09-28: chain restore is all-or-nothing; a hop without checkpointed state makes restore return false so the flow re-seeds; `a_chain_ending_in_top_n_matches_the_uninterrupted_flow_after_restore`
- [x] H12 `krishiv-ivm/src/flow.rs:2093` operator-apply error still commits tick inputs → view permanently loses delta
  - fixed 2026-09-28: failed views drop their plan and recompute via SQL next tick (republishing the missed delta downstream); `a_view_that_fails_one_tick_recovers_the_rows_of_that_tick`
- [x] H13 `krishiv-state/src/dfs_backend.rs:576` DFS snapshot writes v2, all decoders accept v1 only → unrestorable
  - fixed 2026-09-28: DFS writes v1, loads v1+v2; three tests
- [x] H14 `krishiv-engine-core/src/error.rs:47` every Checkpoint error transient + substring match → whole-job retry after sinks flushed duplicates output (`krishiv-engines/src/lib.rs:77`)
  - fixed 2026-09-28: `run_job` retries only if the attempt never opened a sink; `connect` no longer matches `connector`; `run_job_does_not_retry_after_sink_output`
- [x] H15 `krishiv-operator/src/dynamic.rs:97` `--all-namespaces` patches via cluster-scoped handle → 404, no job admitted
  - fixed 2026-09-28: per-object patches use the object's namespace; `per_object_patches_target_the_objects_namespace`

## Medium — security
- [x] M1 No SQL entry point restricts file DDL / `COPY TO` / path directives (`flight-sql host.rs:160`, `flight_protocol.rs:456`) → shared `SQLOptions` + single-statement guard
  - fixed 2026-09-28: `krishiv_sql::sql_accesses_server_files`; Flight refuses file SQL/path directives/RegisterParquet in durable profiles unless `KRISHIV_FLIGHT_ALLOW_FILE_SQL=1`; `file_sql_is_refused_when_not_allowed`
- [x] M2 `krishiv-flight-sql` prepared-statement create runs `sql_query_schema` before policy (`service.rs:664`, `host.rs:663`)
  - fixed 2026-09-28: prepared-statement create checks policy before planning; `prepared_statement_create_checks_policy_before_planning`
- [x] M3 `krishiv-ui/src/handlers.rs:191` `/api/v1/sql` no row/byte cap or timeout, allows DDL
  - fixed 2026-09-28: console refuses server-file SQL, streams with a 10k-row cap (`truncated` flag) and a 30 s timeout; `sql_console_tests`
- [x] M4 `krishiv-mcp/src/lib.rs:1830` HTTP transport: no auth, no Origin/Host check (DNS rebinding)
  - fixed 2026-09-28: Origin/Host validation, `KRISHIV_MCP_BEARER_TOKEN` (constant-time), non-loopback bind requires a token
- [x] M5 `krishiv-common/src/production.rs:34` malformed `KRISHIV_DURABILITY_PROFILE` → DevLocal (auth off); also `krishiv/src/cli.rs:1313`, `kafka_table.rs:19`
  - fixed 2026-09-28: server entry points (krishiv, executor, flight server) refuse an unparsable profile at startup; resolver unchanged
- [x] M6 `krishiv/src/remote_client.rs:69` CLI remote client plaintext only; bearer token in cleartext
  - fixed 2026-09-28: https:// configures TLS from `KRISHIV_CA_CERT`; bearer token never sent over plaintext to a non-loopback host
- [x] M7 `krishiv-scheduler/src/continuous_stream_http.rs:2029` `parallelism` uncapped → coordinator OOM
  - fixed 2026-09-28: `MAX_CONTINUOUS_PARALLELISM = 1024`, refused before any spec is built; `registration_parallelism_is_capped`
- [x] M8 `krishiv-common/src/validate.rs:31` `validate_safe_id`/`is_safe_identifier` accept `"."`; `is_safe_path` accepts absolute → shuffle GC `remove_dir_all(<root>/.)`
  - fixed 2026-09-28: dot-only ids rejected by `validate_safe_id` and `is_safe_identifier`

## Medium — correctness
- [x] M9 `krishiv-scheduler/src/coordinator/mod.rs:843,970` stall/speculation CancelTask RPCs unbounded, awaited in heartbeat loop
  - fixed 2026-09-28: both fan-outs route through `dispatch_cancel_targets` (CANCEL_RPC_TIMEOUT per RPC); no dedicated test — covered by `dispatch_cancel_targets_bounds_a_hung_peer`
- [x] M10 `krishiv-scheduler/src/coordinator/job_lifecycle.rs:368` `cancel_job` has no terminal-state guard (Succeeded/Failed/Committing → Cancelled)
  - fixed 2026-09-28: terminal jobs: cancel is a no-op; Committing: refused with InvalidJob; `cancel_leaves_finished_and_committing_jobs_alone`
- [x] M11 `krishiv-scheduler/src/coordinator_sharded.rs:386` outer ack-timeout overwrites inner `Committing{N}` with `Failed{N}`
  - fixed 2026-09-28: outer→inner merge ignores a Failed for the epoch the inner copy is committing; `a_stale_timeout_failure_does_not_overwrite_a_commit_in_progress`
- [x] M12 `krishiv-sql/src/semi_join_reduction.rs:816` SemiJoinReductionThroughAggregate ignores `null_equality`
  - fixed 2026-09-28: with H4
- [x] M13 `krishiv-sql/src/rollup_rewrite.rs:398` count re-aggregated as `sum` → NULL instead of 0 on empty input
  - fixed 2026-09-28: count re-aggregated as `coalesce(sum(p), 0)`; `an_empty_rollup_counts_zero_not_null`
- [x] M14 `krishiv-sql/src/spark_sql_ext.rs:343` DESCRIBE EXTENDED substring detection rewrites literals
  - fixed 2026-09-28: literal-aware `sql_words` scanner; DESCRIBE EXTENDED only when the statement is DESC[RIBE] [TABLE] EXTENDED
- [x] M15 `krishiv-sql/src/spark_sql_ext.rs:270,378` TABLESAMPLE / SHOW TBLPROPERTIES Unicode-offset slice panic
  - fixed 2026-09-28: TABLESAMPLE / SHOW TBLPROPERTIES use byte-exact word offsets (no Unicode panic, no literal matches)
- [x] M16 `krishiv-sql/src/pivot_sql.rs:120,184,300` PIVOT drops trailing WHERE/ORDER BY; slice panic; literal match
  - fixed 2026-09-28: PIVOT/UNPIVOT found as words, FOR/IN parsed by word after the aggregate, trailing clauses refused
- [x] M17 `krishiv-connectors/src/registry/drivers/pulsar.rs:34` registry Pulsar source never acks; ignores `start_position`
  - fixed 2026-09-28: registry driver sets `ack_on_next_read` (acks the previous batch when the next is read) and parses `start_position`; `registry_pulsar_config_acks_and_honours_start_position`. A checkpoint-driven ack hook on DynSource remains the full fix.
- [x] M18 `krishiv-connectors/src/kinesis.rs:243` idle shard returns empty batch (spin); iterator taken before fallible call
  - fixed 2026-09-28: iterator/restore target consumed only after a successful AWS call; idle shard returns `Ok(None)`; `a_failed_read_keeps_the_shard_position`
- [x] M19 `krishiv-connectors/src/two_phase.rs:303` local Parquet 2PC: no fsync of tmp or dir
  - fixed 2026-09-28: prepare fsyncs the staging file and dir, commit fsyncs the dir after rename (no crash test possible)
- [x] M20 `krishiv-shuffle/src/disk_store.rs:524` dropped writer future commits truncated partition with valid sidecar
  - fixed 2026-09-28: writer commits only when the producer drained the stream; a dropped future aborts the write; `a_dropped_write_commits_nothing`
- [x] M21 `krishiv-executor/src/runner/result_spool.rs:196` partial spool leaked on error/cancel
  - fixed 2026-09-28: `PartialSpool` guard (shared with detached writes) deletes an incomplete spool; `a_failed_drain_leaves_no_spool_file`
- [x] M22 `krishiv-executor/src/fragment/shuffle_write_buffer.rs:642` failed/cancelled spill file leaked
  - fixed 2026-09-28: `SpillRun` owns the file before the write starts, shared with the blocking task (no dedicated test)
- [x] M23 `krishiv-dataflow/src/window/session.rs:415` single open session per key mishandles admitted out-of-order events
  - fixed 2026-09-29: multiple open sessions per key, merged when an event bridges them (`AggState::merge`), closed only by the watermark; `session_window_places_admitted_out_of_order_events_correctly`
- [x] M24 `krishiv-ivm/src/window_rewrite.rs:365` streaming TopN rewrite case-sensitive column compare → rewrites valid SQL
  - fixed 2026-09-29: identifiers normalised like the planner (unquoted → lowercase); `streaming_topn_compares_names_the_way_the_planner_does`
- [x] M25 `krishiv-executor/src/fragment/run_loop_classes.rs:377` rjoin/rpipe/rbatch bypass StreamingLoop gate; RunLoop EOS flush
  - fixed 2026-09-29: new gate variants `RunLoopJoin`/`RunLoopPipeline`/`RunLoopStateless`; rjoin/rpipe input, watermark, idle tick and EOS flush go through `StreamDriver`; RunLoop EOS is `FlushOnDirective` (only `stream-eos` flushes, cancel does not); quiet pipeline windows now tick closed; `a_pipeline_driver_ticks_quiet_windows_closed`, `a_pipeline_flushes_on_the_eos_directive_only`, `a_join_loop_never_ticks_a_pipeline`
- [x] M26 `krishiv-state/src/dfs_backend.rs:660` DFS restore doesn't delete post-checkpoint records
  - fixed 2026-09-29: restore writes the snapshot, then deletes DFS records not in it; `load_snapshot_removes_keys_written_after_the_checkpoint`
- [x] M27 `krishiv-state/src/dfs_backend.rs:178` DFS write not atomic; torn record decodes OK
  - fixed 2026-09-29: DFS writes go temp file → (fsync) → rename; fsync failures reported (no crash test possible)
- [x] M28 `krishiv-runtime/src/flight_client.rs:655` `do_action` retries non-idempotent push/drain after server applied
  - fixed 2026-09-29: `KrishivFlightAction::is_idempotent`; non-idempotent actions retry only the connection; `push_and_drain_are_not_retried`
- [x] M29 `krishiv-state/src/checkpoint/io.rs:364` sync manifest validation builds a Tokio runtime per entry on S3
  - fixed 2026-09-29: sync manifest validation reads on the caller thread and hashes chunks on the pool (no per-entry runtime)
- [x] M30 `krishiv-delta/src/snapshot_index.rs:269` Raw arm propagates SchemaMismatch → view stops advancing (error discarded at `krishiv-ivm/src/flow.rs:1324`)
  - fixed 2026-09-29: Raw arm falls back to the whole-snapshot path on SchemaMismatch; the discarded error at flow.rs is now logged; `a_raw_state_accepts_a_drifted_delta`
- [x] M31 `krishiv-connectors/src/lakehouse/local_delta.rs:157` time travel past latest / negative / pre-creation returns latest
  - fixed 2026-09-28: versions past the log, negative versions and pre-creation timestamps are NotFound; `time_travel_outside_the_log_is_an_error`
- [x] M32 `krishiv-connectors/src/lakehouse/iceberg_fs.rs:170` metadata-vN.json created then filled (not atomic)
  - fixed 2026-09-28: metadata published via fsynced temp file + `hard_link` (never visible partially written)
- [x] M33 `krishiv-connectors/src/lakehouse/hudi.rs:385` lost-update check is check-then-act
  - fixed 2026-09-28: put-if-absent successor claim per base instant (stale claims reclaimable after 10 min); `only_one_writer_claims_the_commit_after_a_base`
- [x] M34 `krishiv-connectors/src/lakehouse/delta_lake.rs:317` merge_delta positional columns; duplicate source keys
  - fixed 2026-09-28: source aligned to the target schema by name (cast when compatible), duplicate source keys refused; `merge_delta_matches_columns_by_name_and_refuses_duplicate_keys`

## Medium — broken as shipped
- [x] M35 `krishiv-scheduler/src/coordinator_daemon.rs:2180` JCP daemon calls unserved `/federation/*`; `deploy/k8s/operator/jcp-pod-template.yaml` bad flags → wire or delete
  - fixed 2026-09-29 (deleted): JCP daemon, `krishiv-job-coordinator` bin/multi-call alias, `KRISHIV_JCP_POLL_INTERVAL_SECS`, `jcp-pod-template.yaml`, docs
- [x] M36 `krishiv/src/cli.rs:855` `savepoint --label` dropped
  - fixed 2026-09-29: remote savepoint sends `--label`; local mode no longer echoes a label it did not apply
- [x] M37 `krishiv/src/cli.rs:973` `restore -c` hard-codes `./krishiv-checkpoints`; `from_savepoint` always false
  - fixed 2026-09-29: remote restore requires `--storage-path`; `--savepoint` added (remote only); `remote_restore_requires_an_explicit_storage_path`, `savepoint_restore_needs_a_coordinator`
- [x] M38 `krishiv/src/cluster_cmd.rs:170` `cluster start` boots clusterd rejecting all RPCs; failed executor spawns hidden
  - fixed 2026-09-29: clusterd gets rocksdb metadata under the data dir and `--insecure` (the removed `json` backend made it fail too); spawn failures reported; `clusterd_args_parse_and_run_insecure_on_loopback`
- [x] M39 `krishiv-operator/src/reconciler.rs:131` standby acts on deletions (strips finalizer, deletes pods)
  - fixed 2026-09-29: a standby returns InactiveCoordinator from the deletion path instead of stripping the finalizer; `a_standby_leaves_deletion_to_the_leader`
- [x] M40 `krishiv-operator/src/controller.rs:344` executor pod creation one-shot; launch-failure detection unreachable
  - fixed 2026-09-29: `ensure_executor_pods` on Submitted/Observed/WaitingForExecutors for live jobs: missing pods re-created, launch failures re-checked (no dedicated test: needs a kube mock)
- [x] M41 `krishiv-python/src/session.rs:1361` `Session.close()` never closes
  - fixed 2026-09-29: `Session::close_shared(&self)`; Python `close()` uses it; `closing_through_a_shared_handle_releases_shared_state`
- [x] M42 `krishiv-metrics/src/counters.rs:670` `remove_job` has no production caller → unbounded cardinality
  - fixed 2026-09-29: coordinator removes per-job metric series when it evicts a finished job; `evicting_a_job_removes_its_metric_series` (mutation-checked). Executor-side `executor_slots_used` still open.
- [x] M43 `python/krishiv-airflow/krishiv_airflow/operators.py:68` sensor can never complete
  - fixed 2026-09-29: sensor polls `GET /api/v1/jobs/{id}` and compares the exact `state`; operator refuses `coordinator_url` (CLI has no remote submit)
- [x] M44 `python/krishiv-dbt-adapter/krishiv_dbt_adapter/impl.py:39` silently no-op without flightsql
  - fixed 2026-09-29: missing flightsql raises; `dry_run=True` for record-only use
- [x] M45 `krishiv-runtime/src/flight_client.rs:1452-1512` four `#[ignore]` regression tests with stale reason (register note wrong)
  - fixed 2026-09-29: four `do_action_*` tests un-ignored (they bind 127.0.0.1:0 like their siblings); justfile line removed

## Low
- [x] L1 `krishiv-scheduler/src/store.rs:1501` terminal latch checked before store lock (resurrection race)
  - fixed: `admit_job_write` runs under the store lock (queued SaveJob + `save_job_checked`)
- [x] L2 `krishiv-shuffle/src/push_shuffle.rs:131` ESS push: task_id ignored; merge_read concatenates IPC streams (or delete path)
  - fixed (deleted): ESS push path — `push_shuffle.rs`, routes, `ess_client.rs`, executor push block, e2e test
- [x] L3 `krishiv-sql/src/lakehouse/providers.rs:132` Delta scan log replay on async task; dead `exists()` filter
  - fixed: Delta file listing in `spawn_blocking`; dead `exists()` filter removed
- [x] L4 `krishiv-sql/src/unnest_sql.rs` no callers — wire or delete
  - fixed (deleted): `unnest_sql.rs`; `lateral.cross_join_unnest` marked Planned (DataFusion lacks CROSS JOIN UNNEST); SQL docs regenerated
- [x] L5 `krishiv-ivm/src/window_rewrite.rs:39,95` TUMBLE/HOP truncating `%` vs floor for negative ts
  - fixed: floored modulo; `negative_timestamps_fall_in_the_window_that_contains_them`
- [x] L6 `krishiv-runtime/src/execution_runtime.rs:639` health loop aborted by its own clone's Drop
  - fixed: Drop aborts the health loop only for the last owner; `dropping_a_clone_keeps_the_shared_health_loop_running`
- [x] L7 `krishiv-runtime/src/continuous_stream.rs:383` drain size check after commit → double apply
  - fixed: consumed input popped before the drain-size check; error names the job and undelivered windows
- [x] L8 `krishiv-state/src/dfs_backend.rs:128` unescaped `__` namespace filenames
  - fixed: percent-encoded name parts; `namespaces_containing_the_separator_stay_distinct`
- [x] L9 `krishiv-runtime/src/execution_runtime.rs:1060` garbled error message whitespace
  - fixed: line continuations restored; 36 garbled space runs across 17 files collapsed
- [x] L10 `krishiv-delta/src/operators/session_window.rs:147` half-applied on NULL ts
  - fixed: NULL check before mutation; `a_null_event_time_rejects_the_whole_delta`
- [x] L11 `krishiv-delta` `join.rs:1147`, `distinct.rs:159`, `trace.rs:525` unbounded `with_capacity` from checkpoint count
  - fixed: capacities bounded by remaining bytes; `an_absurd_entry_count_is_rejected_without_allocating_it`
- [x] L12 `krishiv-plan/src/lowering.rs:57` filters joined with AND unparenthesised
  - fixed: each filter parenthesised when >1; `scan_filters_keep_their_own_grouping`
- [x] L13 `krishiv-plan/src/cep/matcher.rs:32` CEP partial recovery doc false; `CepOperator` unused
  - fixed: CEP docs corrected (matcher + dataflow)
- [x] L14 `krishiv-engines/src/lib.rs:759` StreamingEngine::run skips `validate()`
  - fixed: `run` calls `validate`; engine-kind check; `a_streaming_job_without_sinks_is_rejected`
- [x] L15 `krishiv/src/query_cli.rs:160` `sql --analyze` ignored; `--api-key` "policy-enforced" but AllowAll
  - fixed: `sql --analyze` rejected (exit 2); `--api-key` help corrected; `sql_rejects_the_explain_only_analyze_flag`
- [x] L16 `krishiv-chaos/tests/chaos_suite.rs:537-705` six tests cannot fail
  - fixed: vacuous tests removed with a note
- [x] L17 `krishiv-python/src/sinks.rs` GIL held across network I/O
  - fixed: `block_on_detached` releases the GIL around sink network I/O
- [x] L18 `deploy/k8s/operator/rbac.yaml` ClusterRole broader than needed
  - fixed: ClusterRole narrowed (krishivjobs get/list/watch/patch, pods get/create/delete, leases get/create/patch); manifest test updated
- [x] L19 `krishiv-connectors/src/two_phase_parquet_s3.rs` unused, misnamed, replacing rename
  - fixed (deleted)
- [x] L20 Delta: `LocalDeltaTwoPhaseCommitSink` v0 lacks protocol/metadata; `vacuum_table(0)` races unlogged writes
  - fixed: v0 writes protocol+metadata; `VACUUM_MIN_AGE` floor; two tests

## Register corrections
- [x] R1 §20 MCP first-token note — false since multi-statement support (a273347)
  - done: register §99
- [x] R2 ignored-test note for `flight_client` ResultTooLarge tests — reason is stale
  - done: register §99
- [x] R3 §4 "fencing token prevents split-brain writes" — checkpoints only
  - done: register §99
- [x] R4 §8 DFS v2 "never redistributed" — false
  - done: register §99
