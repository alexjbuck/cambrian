# cambrian

SQL-driven migration runner for **Apache Iceberg** tables. Modeled on
[graphile-migrate](https://github.com/graphile/migrate) and the broader
Postgres-migration ecosystem (Flyway, Sqitch). Cambrian uses
[PyIceberg](https://py.iceberg.apache.org/) in-process — no JVM, Spark, or
Trino required.

## Status

Pre-1.0. Active development.

## Modes

- **Idempotent (default)**: write `CREATE TABLE IF NOT EXISTS` / `ADD COLUMN
  IF NOT EXISTS` so migrations are safe to re-apply. No rollback machinery,
  same code path locally and in CI.
- **Reset**: explicit opt-in for the rare migration that cannot be expressed
  idempotently. Captures a checkpoint, rolls back affected tables, re-runs.

Idempotent is the path. Reset is the relief valve.

## Install

```bash
uv tool install cambrian
# or
uvx cambrian --help
```

## Quickstart

(coming with M3 — `cambrian init` + `cambrian status` against a Lakekeeper REST catalog.)

## License

Apache-2.0 — see [`LICENSE`](./LICENSE).
