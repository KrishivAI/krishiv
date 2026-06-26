# krishiv-shuffle

Shuffle service for distributed data exchange between executors.

## Overview

`krishiv-shuffle` handles partitioned data transfer between scheduler and
executor nodes:

- Partitioned push/pull via Arrow Flight
- Zstd/LZ4 compression
- Local disk and S3-backed shuffle storage
- Binary: `krishiv-shuffle-svc`

## Usage

```bash
krishiv-shuffle-svc --listen 0.0.0.0:50052
```

## License

Apache-2.0
