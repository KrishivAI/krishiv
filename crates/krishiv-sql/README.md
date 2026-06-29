# krishiv-sql

SQL parsing, planning, and execution engine built on DataFusion.

## Overview

`krishiv-sql` extends DataFusion with Krishiv-specific capabilities:

- SQL parsing via `sqlparser`
- DDL handling (CREATE TABLE, CREATE SINK, INSERT INTO)
- Iceberg catalog integration (REST, Glue, Unity)
- Delta Lake and Hudi support
- Information schema for metadata queries

## Features

| Feature | Description |
|---------|-------------|
| `iceberg` | Apache Iceberg catalog support (default) |
| `delta` | Delta Lake table support |
| `hudi` | Apache Hudi table support |
| `local-catalog` | Local filesystem catalog |
| `postgres-catalog` | PostgreSQL-backed Iceberg catalog |
| `rest-catalog` | REST Iceberg catalog |

## License

Apache-2.0
