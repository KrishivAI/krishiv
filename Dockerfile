FROM rust:1.85-bookworm AS builder

WORKDIR /workspace
COPY . .
RUN cargo build --release -p krishiv-cli -p krishiv-ui -p krishiv-operator -p krishiv-executor

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/target/release/krishiv /usr/local/bin/krishiv
COPY --from=builder /workspace/target/release/krishiv-ui /usr/local/bin/krishiv-ui
COPY --from=builder /workspace/target/release/krishiv-operator /usr/local/bin/krishiv-operator
COPY --from=builder /workspace/target/release/krishiv-executor /usr/local/bin/krishiv-executor

CMD ["krishiv"]
