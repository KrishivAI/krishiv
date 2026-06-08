# Contributing to Krishiv

## System dependencies

Some workspace crates require native libraries at link time. Install these before running a full workspace build or test pass:

| Package | Used by |
|---------|---------|
| `build-essential` / a C++ toolchain | Native deps (ONNX, fastembed) pulled by `krishiv-executor` |
| `python3-dev` | `krishiv-python` (PyO3) |
| `libssl-dev`, `pkg-config` | TLS and native builds |
| `protobuf-compiler`, `cmake` | gRPC / protobuf codegen and native extensions |

On Debian/Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y build-essential python3-dev libssl-dev pkg-config protobuf-compiler cmake
```

## Build and validation

```bash
cargo build --workspace
cargo test --workspace --lib
cargo fmt --all
cargo clippy --workspace --exclude krishiv-python -- -D warnings
```

`krishiv-python` is excluded from the default clippy gate when Python headers are unavailable. CI installs the packages above before running the full workspace.

## Native link jobs (B3)

If `cargo test -p krishiv-executor` fails at link time with missing C++ or ONNX symbols, install the system packages listed above rather than changing crate code. For local iteration without native deps, use per-crate tests:

```bash
cargo test -p krishiv-scheduler --lib
cargo test -p krishiv-sql --lib
cargo test -p krishiv-dataflow --lib
```
