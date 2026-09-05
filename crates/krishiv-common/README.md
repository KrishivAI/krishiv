# krishiv-common

Foundation shared by every crate, with minimal dependencies:

- `env_registry` — every `KRISHIV_*` variable with type, default, owner; the
  source of the generated `docs/reference/env-flags.md`.
- `durability` / `production` — `DurabilityProfile` (`dev-local`,
  `single-node-durable`, `distributed-durable`) and the production fail-closed
  guards.
- `executor_capacity` — one capacity decision per executor process (slots,
  memory pool, parallelism from the cgroup); `declare_single_query_process()`.
- `memory_budget`, `page_cache` — cgroup accounting and shuffle page-cache
  eviction.
- `partition` — the shared keyed hash (SHA-256 domain) used by shuffle, key
  groups, and IVM shards.
- `streaming_dials`, `backpressure::CreditGate`, `async_util` (`block_on`,
  `run_blocking`), `validate::validate_safe_id`, retry helpers, typed errors.

Documentation: `docs/architecture/15-configuration.md`,
`docs/architecture/05-executor-and-data-plane.md`,
`docs/architecture/14-deployment-and-durability.md`.

License: Apache-2.0.
