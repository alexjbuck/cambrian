# cambrian

SQL-driven migration runner for **Apache Iceberg** tables. Modeled on
[graphile-migrate](https://github.com/graphile/migrate) and the broader
Postgres-migration ecosystem (Flyway, Sqitch). Cambrian uses
[PyIceberg](https://py.iceberg.apache.org/) in-process — no JVM, no Spark,
no Trino.

## Status

Pre-1.0. Active development. The on-disk formats (`current.sql`,
`committed/<NNNN>_<slug>.sql`, sidecar tables) and CLI surface are stable.

## Why two modes?

> **Idempotent is the path. Reset is the relief valve.**

- **Idempotent** (default): every migration is written so it can be
  re-applied any number of times with the same end state — `CREATE TABLE
  IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, etc. No rollback machinery;
  the same code path runs locally and in CI.
- **Reset** (`--reset` or `[dev].mode = "reset"`): explicit opt-in for the
  rare migration that genuinely cannot be expressed idempotently. Captures
  a checkpoint, rolls the affected tables back, re-runs.

Reset is never the recommended fix — only the last resort. The defaults,
the error messages, and the docs all reinforce that.

## Install

```bash
uv tool install cambrian
# or one-shot:
uvx cambrian --help
```

## Quickstart (dev)

```bash
# 1. Scaffold a project pointing at your REST / SQL / Glue catalog.
cambrian init --path cambrian.toml

# 2. Edit migrations/current.sql with your DDL.
$EDITOR migrations/current.sql

# 3. Hot-reload loop. Saves trigger an idempotent re-apply.
cambrian watch

# 4. When you're happy with the diff, freeze it.
cambrian commit -m "add users + events tables"

# 5. Ship the new committed file via git. Production picks it up.
git add migrations/committed/*.sql cambrian.toml && git commit
```

The `committed/` directory is the contract between dev and prod. Production
applies them in order and treats each file as immutable; editing one after
it's been applied is refused.

## Production recipe (CI)

`cambrian.toml` should resolve secrets from the environment via `${VAR}`
substitution:

```toml
[catalog]
type      = "rest"
uri       = "${CAMBRIAN_CATALOG_URI}"
warehouse = "prod"
token     = "${CAMBRIAN_CATALOG_TOKEN}"

[migrations]
dir               = "./migrations"
sidecar_namespace = "_cambrian"
```

Need a literal `${...}` in a value? Escape it with `$${...}` — make-style.

GitHub Actions deploy step:

```yaml
- name: cambrian apply (prod)
  env:
    CAMBRIAN_CATALOG_URI:   ${{ secrets.CAMBRIAN_CATALOG_URI }}
    CAMBRIAN_CATALOG_TOKEN: ${{ secrets.CAMBRIAN_CATALOG_TOKEN }}
  run: |
    uvx cambrian@<version> apply --json --path cambrian.toml \
      | tee /tmp/apply.json
    test "$(jq -r .status /tmp/apply.json)" = "applied" \
      || test "$(jq -r .status /tmp/apply.json)" = "unchanged"
```

Idempotent semantics mean a repeated apply on an already-current catalog
is a no-op — re-running on a flaky job is safe.

## Command reference

| Command                       | Mode      | JSON shape                          |
|-------------------------------|-----------|-------------------------------------|
| `cambrian init`               | mutating  | n/a                                 |
| `cambrian status`             | read-only | [status][doc-json]                  |
| `cambrian apply`              | mutating  | [apply][doc-json]                   |
| `cambrian apply --reset`      | mutating  | [reset][doc-json]                   |
| `cambrian redo`               | mutating  | alias for `apply --reset`           |
| `cambrian rollback`           | mutating  | [reset][doc-json]                   |
| `cambrian commit -m <msg>`    | mutating  | [commit][doc-json]                  |
| `cambrian uncommit`           | mutating  | [uncommit][doc-json]                |
| `cambrian reset-to <id>`      | mutating  | [reset-to][doc-json]                |
| `cambrian watch`              | mutating  | NDJSON, one apply event per line    |
| `cambrian sync` / `download`  | mutating  | [sync][doc-json]                    |
| `cambrian config show`        | read-only | dump of config (credentials masked) |
| `cambrian config check`       | read-only | n/a                                 |

[doc-json]: ./docs/cli-json.md

- `init`: idempotent bootstrap of the `_cambrian` sidecar
  namespace + tables in the configured catalog.
- `status`: print the sidecar version, applied committed migrations, and
  the most recent `current.sql` apply event.
- `apply`: expand `current.sql` (resolving `--! include` directives),
  hash, compare against the most-recent apply event; replay any committed
  migrations not yet recorded; dispatch each statement via the configured
  sqlglot Spark-dialect parser. Re-runs are no-ops when the hash matches.
- `apply --reset` / `redo`: for migrations that can't be expressed
  idempotently. Captures a checkpoint, rolls the affected tables back to
  their last checkpoint, re-applies. External-write detection refuses
  unless `--force` is passed.
- `rollback`: roll the last apply's affected tables back to their
  checkpoint without re-applying — useful for discarding a dev iteration
  before editing `current.sql` from scratch.
- `commit -m`: freeze the current applied state as
  `committed/<NNNN>_<slug>.sql`; pin per-table checkpoints under
  `cambrian.committed.<n>.<slug>`; truncate `current.sql`.
- `uncommit`: pop the latest committed file back to `current.sql` and
  roll the affected tables to the pinned checkpoint. Refuses on a gap or
  a non-empty `current.sql` (`--force` overrides).
- `reset-to <migration_id>`: incident-response only. Roll affected
  tables back to a specific committed migration's checkpoint without
  touching downstream commit events.
- `watch`: filesystem watcher that re-applies on every save. Honours
  `[dev].mode` and `[dev].debounce_ms`. Ctrl-C exits cleanly.
- `sync` / `download`: rehydrate `committed/` from the catalog. The
  catalog is the source of truth; missing files are created, hash-matching
  files are skipped, and divergent files are refused (`--force` overrides,
  `--diff` prints a unified diff, `--dry-run` plans without writing).

## Exit codes

| Code | Meaning                                          |
|------|--------------------------------------------------|
| 0    | success                                          |
| 1    | generic error (parse, dispatch, IO, …)           |
| 2    | sidecar not initialised — run `cambrian init`    |
| 3    | sidecar version is newer than this binary        |
| 4    | external write detected (rollback / sync)        |

CI scripts can switch on these codes; failure cases also print a one-line
hint on stderr.

## Configuration

`cambrian.toml`:

```toml
[catalog]
# Passed verbatim to pyiceberg.catalog.load_catalog. Required: type, uri.
type      = "rest"
uri       = "http://localhost:8181/catalog"
warehouse = "my-warehouse"
# String values may reference env vars: ${NAME}. Literal: $${NAME}.

[migrations]
dir               = "./migrations"
sidecar_namespace = "_cambrian"

[dev]
mode        = "idempotent"   # "idempotent" (default) | "reset"
watch       = true
debounce_ms = 500
```

The sidecar tables (`events`, `table_states`, `version`) are append-only
and self-migrated forward by cambrian itself. Their names are fixed; only
the namespace is user-configurable.

## Release infrastructure

PyPI release is configured separately (no `release.yml` in this repo
yet). The package builds with `uv build` and publishes via trusted
publishing once the workflow is added.

## License

Apache-2.0 — see [`LICENSE`](./LICENSE).
