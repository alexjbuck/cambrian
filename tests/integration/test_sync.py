"""Integration tests for ``cambrian sync``.

Exercises the catalog-truth-source contract end-to-end against Lakekeeper:

* cross-instance roundtrip: ``commit`` from one ``migrations/`` directory,
  ``sync`` rehydrates a fresh empty ``migrations/`` directory pointed at the
  same catalog.
* conflict refusal + ``--force`` overwrite.
* uncommitted migrations are not re-written.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceNotEmptyError, NoSuchNamespaceError

from cambrian.config import CambrianConfig, CatalogConfig, MigrationsConfig
from cambrian.migrate import apply_idempotent
from cambrian.migrate.commit import (
    cambrian_commit,
    cambrian_uncommit,
)
from cambrian.migrate.sync import cambrian_sync
from cambrian.sidecar.events import committed_migrations

LAKEKEEPER_URL = "http://localhost:8181"
WAREHOUSE = "cambrian"


def _build_config(*, migrations_dir: Path, sidecar_namespace: str) -> CambrianConfig:
    return CambrianConfig(
        catalog=CatalogConfig(
            type="rest",
            uri=f"{LAKEKEEPER_URL}/catalog",
            **{
                "warehouse": WAREHOUSE,
                "s3.endpoint": "http://localhost:9000",
                "s3.access-key-id": "cambrian-access-key",
                "s3.secret-access-key": "cambrian-secret-key",
                "s3.region": "local",
                "s3.path-style-access": "true",
            },
        ),
        migrations=MigrationsConfig(
            dir=str(migrations_dir),
            sidecar_namespace=sidecar_namespace,
        ),
    )


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


@pytest.fixture
def sidecar_ns(rest_catalog: RestCatalog) -> Iterator[str]:
    namespace = f"_cambrian_test_{uuid.uuid4().hex[:8]}"
    yield namespace
    try:
        for ident in rest_catalog.list_tables(namespace):
            rest_catalog.drop_table(ident)
        rest_catalog.drop_namespace(namespace)
    except (NoSuchNamespaceError, NamespaceNotEmptyError):
        pass


def test_sync_cross_instance_roundtrip(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """Apply + commit from migrations_a/, then sync into a fresh migrations_b/.

    The two directories share a single catalog (the sidecar namespace) — that's
    the relationship between two clones of a repo pointing at the same prod
    catalog. After sync, the second directory mirrors the first's committed/
    set, file-for-file.
    """
    del rest_catalog
    sql1 = (
        f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n"
        f"INSERT INTO {ns}.t VALUES (0);\n"
    )

    migrations_a = tmp_path / "migrations_a"
    _write(migrations_a / "current.sql", sql1)
    cfg_a = _build_config(migrations_dir=migrations_a, sidecar_namespace=sidecar_ns)
    apply_idempotent(cfg_a)
    cambrian_commit(cfg_a, message="seed")

    # Second migration.
    sql2 = f"ALTER TABLE {ns}.t ADD COLUMN name STRING;\n"
    _write(migrations_a / "current.sql", sql2)
    apply_idempotent(cfg_a)
    cambrian_commit(cfg_a, message="add name")

    # Now run sync from a fresh empty directory.
    migrations_b = tmp_path / "migrations_b"
    migrations_b.mkdir()
    cfg_b = _build_config(migrations_dir=migrations_b, sidecar_namespace=sidecar_ns)

    result = cambrian_sync(cfg_b)

    assert result.written == 2
    assert result.refused == 0
    assert not result.has_refusals

    committed_b = migrations_b / "committed"
    file_1 = committed_b / "0001_seed.sql"
    file_2 = committed_b / "0002_add-name.sql"
    assert file_1.exists()
    assert file_2.exists()

    # Content matches the originals exactly.
    file_1_a = migrations_a / "committed" / "0001_seed.sql"
    file_2_a = migrations_a / "committed" / "0002_add-name.sql"
    assert file_1.read_text(encoding="utf-8") == file_1_a.read_text(encoding="utf-8")
    assert file_2.read_text(encoding="utf-8") == file_2_a.read_text(encoding="utf-8")

    # Re-running sync against the synced directory is a clean no-op (all skipped).
    second = cambrian_sync(cfg_b)
    assert second.written == 0
    assert second.skipped == 2
    assert not second.has_refusals


def test_sync_refuses_tampered_local_file(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """Edit a local committed file post-sync; the next sync refuses, --force overrides."""
    del rest_catalog
    sql = (
        f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n"
        f"INSERT INTO {ns}.t VALUES (0);\n"
    )
    migrations = tmp_path / "migrations"
    _write(migrations / "current.sql", sql)
    cfg = _build_config(migrations_dir=migrations, sidecar_namespace=sidecar_ns)
    apply_idempotent(cfg)
    commit_result = cambrian_commit(cfg, message="seed")

    # Tamper with the local file.
    tampered = sql + f"-- evil edit\nALTER TABLE {ns}.t ADD COLUMN evil STRING;\n"
    commit_result.committed_path.write_text(tampered, encoding="utf-8")

    # Sync refuses.
    result = cambrian_sync(cfg)
    assert result.refused == 1
    assert result.has_refusals
    # File is still tampered.
    assert commit_result.committed_path.read_text(encoding="utf-8") == tampered

    # With --diff (dry-run) we get a diff payload but the file remains tampered.
    diffed = cambrian_sync(cfg, diff=True)
    assert diffed.refused == 1
    assert diffed.files[0].diff is not None
    assert "evil" in diffed.files[0].diff
    assert commit_result.committed_path.read_text(encoding="utf-8") == tampered

    # --force overwrites.
    forced = cambrian_sync(cfg, force=True)
    assert forced.overwritten == 1
    assert forced.refused == 0
    assert commit_result.committed_path.read_text(encoding="utf-8") == sql


def test_sync_excludes_uncommitted(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """A commit followed by an uncommit must not be re-written by sync.

    The uncommit explicitly walked the migration back; sync rehydrating it
    would silently undo the user's intent on every fresh checkout.
    """
    del rest_catalog
    sql = (
        f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n"
        f"INSERT INTO {ns}.t VALUES (0);\n"
    )
    migrations = tmp_path / "migrations"
    _write(migrations / "current.sql", sql)
    cfg = _build_config(migrations_dir=migrations, sidecar_namespace=sidecar_ns)
    apply_idempotent(cfg)
    cambrian_commit(cfg, message="seed")
    cambrian_uncommit(cfg)

    # Confirm via the events log that no live commits remain.
    from cambrian.catalog import load_catalog

    catalog = load_catalog(cfg)
    live = committed_migrations(catalog, sidecar_ns)
    assert live == []

    # Sync from a fresh directory: writes nothing.
    fresh = tmp_path / "fresh"
    fresh.mkdir()
    cfg_fresh = _build_config(migrations_dir=fresh, sidecar_namespace=sidecar_ns)
    result = cambrian_sync(cfg_fresh)

    assert result.files == []
    assert result.written == 0
    assert not (fresh / "committed").exists() or list((fresh / "committed").iterdir()) == []
