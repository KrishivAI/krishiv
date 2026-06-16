FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY dist/docker/krishiv /usr/local/bin/krishiv
COPY dist/docker/krishiv-operator /usr/local/bin/krishiv-operator
RUN chmod +x /usr/local/bin/krishiv /usr/local/bin/krishiv-operator
ENTRYPOINT ["/usr/local/bin/krishiv"]
