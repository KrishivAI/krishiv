# krishiv-ui

The embedded web console and its aggregation endpoints on the coordinator's
HTTP listener — not a separate server. `/console` serves the React 19 +
TanStack SPA built from this crate's `console/` (Vite output in
`console/dist`, embedded with `rust-embed` in release builds and read from
disk in debug). The SPA calls the coordinator's canonical `/api/v1/*` routes
(jobs, executors, batch-sql, continuous, IVM, queryable state) plus the
aggregations served here (`diagnose`, `queues`, `history`, `sql`) with a
stored bearer token; static assets are public.

Dev loop: `cd crates/krishiv-ui/console && npm run dev` (proxies to a local
coordinator on 7072; override with `KRISHIV_COORDINATOR_URL`).
`just console-build` rebuilds `console/dist`.

Documentation: `docs/architecture/11-public-interfaces.md`,
`docs/architecture/13-observability.md`.

License: Apache-2.0.
