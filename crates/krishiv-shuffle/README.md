# krishiv-shuffle

Inter-stage data exchange: the `ShuffleStore` contract with lease-token
fencing and streaming writes; in-memory, local-disk, object-store, tiered
(local + remote, both-commit) and push stores; `HashPartitioner` on the
shared keyed hash; sort-shuffle writer and index; LZ4/Zstd compression;
Bloom runtime filters; orphan reclamation with count-and-clock grace;
Arrow Flight shuffle service (`krishiv-shuffle-svc`, `krishiv shuffle-svc`)
with bearer auth.

Documentation: `docs/architecture/06-shuffle.md`.

License: Apache-2.0.
