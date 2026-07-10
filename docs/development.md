# Development

How to run warzone locally. Two tiers:

- **Tier 1 — zero infra.** In-memory catalog + local-filesystem storage. No
  Docker. Fastest path to a running server; good for the write path and
  parquet reads. `make run`.
- **Tier 2 — realistic stack.** Apache Polaris (Iceberg REST catalog) +
  SeaweedFS (S3-compatible store) in Docker. Exercises the REST-catalog and
  S3 code paths and supports catalog-qualified SQL. `make dev-up` then
  `make run-stack`.

## Prerequisites

- Rust stable (`cargo`, `rustfmt`, `clippy`).
- Docker + the Compose plugin (Tier 2 only).
- `psql` and `curl` for the smoke tests (optional).

On the first query, DuckDB downloads its `iceberg` and `httpfs` extensions
over the network ([src/querier/attach.rs](../src/querier/attach.rs)) — the
first query after a fresh start is slower and needs internet access.

## Make targets

| Target | What it does |
| --- | --- |
| `make build` | `cargo build --workspace` |
| `make test` | `cargo test --workspace` (all tests are infra-free) |
| `make fmt` | `cargo fmt --all` |
| `make lint` | `cargo clippy --workspace --all-targets -- -D warnings` |
| `make run` | Tier 1: `cargo run -- --config dev/local.yaml` |
| `make dev-up` | Tier 2: start Polaris + SeaweedFS, create bucket + catalog |
| `make dev-down` | Tier 2: stop the stack and delete its volumes |
| `make run-stack` | Tier 2: `cargo run -- --config dev/stack.yaml` |
| `make clean-data` | Remove Tier 1 data under `dev/data/` |

## Ports

| Service | Address | Notes |
| --- | --- | --- |
| HTTP API | `127.0.0.1:3886` | Configurable via `service` in the config |
| Postgres wire | `127.0.0.1:5432` | **Hardcoded** ([src/server/pgwire/server.rs](../src/server/pgwire/server.rs)) — conflicts with a local Postgres; stop it or remap that one |
| SeaweedFS S3 | `127.0.0.1:8333` | Tier 2 only |
| Polaris REST/mgmt | `127.0.0.1:8181` | Tier 2 only |
| Polaris health | `127.0.0.1:8182` | Tier 2 only |

The Postgres wire port is a compile-time constant, so a Postgres already
listening on 5432 will make the server fail to bind. Stop it, or change the
constant.

## Tier 1: zero-infra quickstart

```sh
make run
```

Serves HTTP on `:3886` and the Postgres wire protocol on `:5432`. Insert,
read, and query:

```sh
curl -s -X POST localhost:3886/api/v1/insert \
  -H 'content-type: application/json' \
  -d '{"namespace":"demo","table":"events","data":{"id":1,"name":"hello"}}'

curl -s -X POST localhost:3886/api/v1/query \
  -H 'content-type: application/json' \
  -d '{"sql":"SELECT * FROM read_parquet('\''dev/data/warehouse/demo/events/data/*.parquet'\'')"}'

psql -h 127.0.0.1 -p 5432 -c \
  "SELECT * FROM read_parquet('dev/data/warehouse/demo/events/data/*.parquet')"
```

Files land under `dev/data/warehouse/<namespace>/<table>/` as Iceberg
metadata + Parquet data. Wipe it with `make clean-data`.

**Caveat — no catalog by name.** Tier 1 uses `CatalogConfig::Memory`, which
holds table metadata in-process only. The querier does not attach memory
catalogs to DuckDB, so `SELECT ... FROM demo.events` won't resolve — read the
parquet directly with `read_parquet(...)`. Parquet + Iceberg metadata persist
on disk across restarts, but a restarted server won't re-attach them. It's the
only zero-infra option; `CatalogConfig` has no persistent local variant. For
catalog-qualified SQL, use Tier 2.

## Tier 2: Polaris + SeaweedFS

```sh
make dev-up      # pulls images, starts both services, runs dev/init.sh
make run-stack   # warzone (on the host) against dev/stack.yaml
```

`make dev-up` waits until SeaweedFS and Polaris are healthy, then
[dev/init.sh](../dev/init.sh) creates the `warehouse` bucket and a Polaris
catalog named `warzone` backed by `s3://warehouse`. It's idempotent — safe to
re-run.

warzone runs on the host (not containerized), talking to Polaris on `:8181`
and SeaweedFS on `:8333`. Because tables are registered in the catalog,
catalog-qualified SQL works:

```sh
curl -s -X POST localhost:3886/api/v1/insert \
  -H 'content-type: application/json' \
  -d '{"namespace":"demo","table":"events","data":{"id":1,"name":"hello"}}'

curl -s -X POST localhost:3886/api/v1/query \
  -H 'content-type: application/json' \
  -d '{"sql":"SELECT * FROM warzone.demo.events"}'

psql -h 127.0.0.1 -p 5432 -c "SELECT * FROM warzone.demo.events"
```

Verify objects landed in SeaweedFS (`--recursive` lists the Parquet data +
Iceberg metadata). Force path-style addressing — SeaweedFS resolves buckets by
path here, not as `<bucket>.host` subdomains:

```sh
AWS_ACCESS_KEY_ID=warzone AWS_SECRET_ACCESS_KEY=warzone AWS_REGION=us-east-1 \
  aws configure set default.s3.addressing_style path
AWS_ACCESS_KEY_ID=warzone AWS_SECRET_ACCESS_KEY=warzone AWS_REGION=us-east-1 \
  aws --endpoint-url http://localhost:8333 s3 ls s3://warehouse --recursive
```

Tear down (removes volumes, so the in-memory Polaris metastore and bucket
reset on the next `dev-up`):

```sh
make dev-down
```

## Configuration reference

Config is YAML, passed with `--config`. Top level has `service` (a list of
servers) and `silo` (the write/read backends). See
[src/config/config.rs](../src/config/config.rs) and
[crates/silo/src/config.rs](../crates/silo/src/config.rs).

### `service`

A list of servers, each a YAML-tagged enum variant:

```yaml
service:
  - !Http
    port: 3886   # defaults to 3886 if <= 0
  - !PgWire      # no fields; binds 127.0.0.1:5432
```

### `silo.destinations[]`

Each destination has a `name`, a `catalog`, and a `storage`. Writes fan out to
every destination; the querier attaches each as its own DuckDB catalog (memory
catalogs excepted — see the Tier 1 caveat).

**`catalog`** (`type:` selects the variant):

- `memory` — in-memory, test/dev only. Fields: `warehouse` (a `file://` path,
  or a path relative to the working dir).
- `rest` — Iceberg REST catalog. Fields: `uri`, `warehouse`, plus optional
  auth props flattened at the same level: `token`, or OAuth2
  client-credentials via `credential` (`client_id:client_secret`),
  `oauth2-server-uri`, and `scope`.

**`storage`** (`type:` selects the variant):

- `file_system` — local disk. Field: `root_path`.
- `s3` — any S3-compatible store. Fields: `bucket`, `endpoint`, `region`,
  `path_style`, `access_key_id`, `secret_access_key`.
- `minio` — sugar for `s3` with `path_style: true` and `endpoint` set. Fields:
  `bucket`, `endpoint`, `access_key_id`, `secret_access_key`.

`dev/local.yaml` and `dev/stack.yaml` are worked examples of each tier.

## Tests

All current tests are infra-free — no Docker, no network (the all-memory
config path deliberately skips loading DuckDB's network-fetched extensions).

```sh
make test
```
