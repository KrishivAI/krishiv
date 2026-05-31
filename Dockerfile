FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY target/debug/krishiv /usr/local/bin/krishiv
RUN chmod +x /usr/local/bin/krishiv
ENTRYPOINT ["/usr/local/bin/krishiv"]
