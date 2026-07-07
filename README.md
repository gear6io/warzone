# warzone

> **Status: not ready for production.** Under active early development. APIs, config format, and storage layout may change without notice.

A PostgreSQL-compatible database that stores data natively as Apache Iceberg tables on Parquet. Goal: give developers a full database experience (SQL, transactions, wire protocol) while every byte lands in an open, engine-agnostic format — no separate catalog, compaction, or scheduling infrastructure to stand up.

## Why

Iceberg is becoming the standard open table format for lakehouses, but using it today means assembling and running your own catalog, query engine, object storage, compaction jobs, metadata cleanup, and orchestration. This project packages that stack behind a single database process, the same way PostgreSQL packages a storage engine and transaction manager behind a single process.

## Planned capabilities

- SQL query execution
- Native Iceberg table management
- Automatic metadata generation
- Background file compaction
- Snapshot lifecycle management
- Schema evolution
- Transaction management
- PostgreSQL wire protocol compatibility
- Local dev via a single container; production on S3 / GCS / Azure Blob Storage

## Design principles

- **Open by default** — data stored as standard Iceberg tables and Parquet files, readable by any Iceberg-compatible engine.
- **Zero operational overhead** — no Spark cluster, scheduler, or standalone catalog service needed for common setups.
- **Developer-first** — install and run should feel as simple as PostgreSQL.
- **Scale naturally** — same programming model from a laptop up to cloud object storage.

## Project layout

Rust workspace (`Cargo.toml`), built on `axum` + `tokio`.

- [`src/`](src/) — binary entrypoint, config loading, HTTP server wiring
- [`crates/silo`](crates/silo/) — Iceberg/Parquet backend: catalog, storage, ingest, destination handling
- [`crates/errors`](crates/errors/) — shared error types
- [`docs/`](docs/) — project documentation

## Contributing

See [`docs/contributing`](docs/contributing/).

## License

See [`LICENSE`](LICENSE).
