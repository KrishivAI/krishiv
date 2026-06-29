# krishiv-proto

Protobuf definitions and generated gRPC types for Krishiv's control-plane and
data-plane APIs.

## Overview

`krishiv-proto` compiles `.proto` files into Rust types via `prost` and
`tonic`. It defines the wire format for:

- Job scheduling and lifecycle RPCs
- Data-plane shuffle and fetch protocols
- Metadata operations

## Features

| Feature | Description |
|---------|-------------|
| `serde` | Enable `Serialize`/`Deserialize` on proto types |

## License

Apache-2.0
