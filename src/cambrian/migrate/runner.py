"""Apply orchestration for ``current.sql`` — idempotent mode (M5).

The idempotent runner expands ``current.sql`` (with includes), hashes the
expanded text, short-circuits if the hash matches the most recent ``apply``
event, and otherwise parses and dispatches each statement. One ``apply``
event is emitted regardless of full/partial success, carrying the new hash,
the full expanded SQL, and one ``table_states`` row per affected table.

**Reset mode (rollback before re-apply) is M6 and explicitly absent here.**
A failed statement under ``--allow-partial=False`` surfaces the error after
the partial-success event is written; the runner does NOT roll back. That
absence is the contract.
"""

from __future__ import annotations

import getpass
import socket
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import sqlglot
from pyiceberg.exceptions import NoSuchTableError

from cambrian.catalog import load_catalog
from cambrian.errors import (
    CambrianError,
    MigrationNotFoundError,
)
from cambrian.iceberg.affected import (
    TableIdent,
    affected_tables_with_overrides,
)
from cambrian.iceberg.checkpoint import capture
from cambrian.sidecar.events import TableStateRow, latest_event, write_event
from cambrian.sidecar.selfmigrate import ensure_current
from cambrian.sql.dialect import CambrianSpark
from cambrian.sql.dispatch import dispatch
from cambrian.sql.include import expand

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

    from cambrian.config import CambrianConfig

__all__ = ["ApplyResult", "StatementResult", "apply_idempotent"]


@dataclass
class StatementResult:
    """One statement's outcome inside an apply."""

    sql: str
    notes: str = ""
    affected_tables: list[TableIdent] = field(default_factory=list)
    error: str | None = None


@dataclass
class ApplyResult:
    """The aggregate outcome of an apply.

    ``status`` is one of ``"unchanged"`` (hash matched), ``"applied"``
    (every statement succeeded), or ``"partial"`` (at least one statement
    failed but the runner continued under ``allow_partial=True``).
    """

    status: str
    migration_hash: str
    sources: list[Path] = field(default_factory=list)
    statements: list[StatementResult] = field(default_factory=list)
    event_id: str | None = None
    error: str | None = None


def apply_idempotent(
    config: CambrianConfig,
    *,
    allow_partial: bool = False,
    actor: str | None = None,
) -> ApplyResult:
    """Apply ``current.sql`` once in idempotent mode.

    Steps:

    1. Expand includes + hash.
    2. Load catalog; ``ensure_current`` on the sidecar.
    3. Check the last ``apply`` event for ``migration_id="current"``. If its
       ``migration_hash`` matches the new one: no-op.
    4. Parse all statements via :class:`CambrianSpark`.
    5. For each statement: capture pre-state, dispatch, capture post-state.
    6. Emit one ``apply`` event with the full expanded SQL and N
       ``table_states`` rows.

    Raises:
        MigrationNotFoundError: ``current.sql`` doesn't exist.
        CambrianError: Various — see :mod:`cambrian.errors`.
    """
    current_sql_path = _resolve_current_sql(config)
    expanded = expand(current_sql_path)
    catalog = load_catalog(config)
    state = ensure_current(catalog, config.migrations.sidecar_namespace, allow_read_only=False)
    namespace = state.sidecar_namespace

    last = latest_event(catalog, namespace, event_type="apply", migration_id="current")
    if last is not None and last.migration_hash == expanded.hash:
        return ApplyResult(
            status="unchanged",
            migration_hash=expanded.hash,
            sources=expanded.sources,
        )

    statements_raw = sqlglot.parse(expanded.text, dialect=CambrianSpark)
    statements = [s for s in statements_raw if s is not None]

    per_stmt_tables = affected_tables_with_overrides(expanded.text, statements)
    # Pre-snapshot the table state for every affected table so the post-apply
    # event log has accurate before/after rows. Tables that don't exist yet
    # (e.g. about-to-be-CREATEd) get a row with None pre-fields.
    state_by_ident: dict[str, _TableStateAccumulator] = {}
    for ident in _unique_tables(per_stmt_tables):
        state_by_ident[str(ident)] = _capture_pre(catalog, ident)

    statement_results: list[StatementResult] = []
    overall_error: str | None = None
    fatal_error: CambrianError | None = None

    for stmt, tables in zip(statements, per_stmt_tables, strict=True):
        try:
            result = dispatch(catalog, stmt)
            statement_results.append(
                StatementResult(
                    sql=stmt.sql(),
                    notes=result.notes,
                    affected_tables=tables or result.affected_tables,
                )
            )
        except CambrianError as err:
            statement_results.append(
                StatementResult(
                    sql=stmt.sql(),
                    affected_tables=tables,
                    error=str(err),
                )
            )
            overall_error = str(err)
            if not allow_partial:
                # Per the M5 contract: emit the partial-success event but
                # *also* surface the error to the caller. Reset (rollback on
                # failure) is M6's contract — we never recover here.
                fatal_error = err
                break

    # Post-snapshot every affected table that exists now.
    for acc in state_by_ident.values():
        acc.capture_post(catalog)

    status = "applied"
    if overall_error and not allow_partial:
        status = "partial"
    elif overall_error and allow_partial:
        status = "partial"

    notes = _summarise(statement_results, status)
    event_id = write_event(
        catalog,
        namespace,
        event_type="apply",
        migration_id="current",
        migration_hash=expanded.hash,
        migration_sql=expanded.text,
        actor=actor or _default_actor(),
        notes=notes,
        table_states=[acc.to_row() for acc in state_by_ident.values()],
    )

    # If we hit a fatal error under ``allow_partial=False`` the event is
    # written first (so the audit trail captures the partial work), then we
    # surface the exception. Callers (CLI, tests) see the original error
    # type and message — no swallowing.
    if fatal_error is not None:
        raise fatal_error

    return ApplyResult(
        status=status,
        migration_hash=expanded.hash,
        sources=expanded.sources,
        statements=statement_results,
        event_id=event_id,
        error=overall_error,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_current_sql(config: CambrianConfig) -> Path:
    base = Path(config.migrations.dir).resolve()
    candidate = base / "current.sql"
    if not candidate.exists():
        raise MigrationNotFoundError(
            f"current.sql not found at {candidate} "
            f"(config: migrations.dir = {config.migrations.dir})"
        )
    return candidate


def _default_actor() -> str:
    """Build a default actor string for the event log.

    Format is ``<user>@<host>``; falls back to ``cambrian`` if either part
    can't be determined. The events table stores this verbatim — anything
    that lets a future audit trace "who applied this" works.
    """
    try:
        user = getpass.getuser()
    except OSError:
        user = "cambrian"
    try:
        host = socket.gethostname()
    except OSError:
        host = "unknown"
    return f"{user}@{host}"


def _summarise(stmts: list[StatementResult], status: str) -> str:
    n = len(stmts)
    errs = sum(1 for s in stmts if s.error)
    return f"status={status} statements={n} errors={errs}"


def _unique_tables(per_stmt: Iterable[Iterable[TableIdent]]) -> list[TableIdent]:
    """Flatten and dedupe a per-statement list-of-lists into a single ordered list."""
    seen: dict[str, TableIdent] = {}
    for tables in per_stmt:
        for t in tables:
            seen.setdefault(str(t), t)
    return list(seen.values())


@dataclass
class _TableStateAccumulator:
    """Helper that buffers pre- and post- snapshots of one table for the event log."""

    ident: TableIdent
    pre_snapshot_id: int | None = None
    pre_schema_id: int | None = None
    pre_spec_id: int | None = None
    pre_sort_order_id: int | None = None
    pre_metadata_loc: str | None = None
    post_snapshot_id: int | None = None
    post_schema_id: int | None = None
    post_spec_id: int | None = None
    post_sort_order_id: int | None = None

    def capture_post(self, catalog: Catalog) -> None:
        table = _load_or_none(catalog, self.ident)
        if table is None:
            return
        snap = table.current_snapshot()
        self.post_snapshot_id = snap.snapshot_id if snap is not None else None
        self.post_schema_id = table.schema().schema_id
        self.post_spec_id = table.spec().spec_id
        self.post_sort_order_id = table.sort_order().order_id

    def to_row(self) -> TableStateRow:
        return TableStateRow(
            table_ident=str(self.ident),
            pre_snapshot_id=self.pre_snapshot_id,
            pre_schema_id=self.pre_schema_id,
            pre_spec_id=self.pre_spec_id,
            pre_sort_order_id=self.pre_sort_order_id,
            pre_metadata_loc=self.pre_metadata_loc,
            post_snapshot_id=self.post_snapshot_id,
            post_schema_id=self.post_schema_id,
            post_spec_id=self.post_spec_id,
            post_sort_order_id=self.post_sort_order_id,
        )


def _capture_pre(catalog: Catalog, ident: TableIdent) -> _TableStateAccumulator:
    acc = _TableStateAccumulator(ident=ident)
    table = _load_or_none(catalog, ident)
    if table is None:
        return acc
    cp = capture(table)
    acc.pre_snapshot_id = cp.snapshot_id
    acc.pre_schema_id = cp.schema_id
    acc.pre_spec_id = cp.spec_id
    acc.pre_sort_order_id = cp.sort_order_id
    acc.pre_metadata_loc = cp.metadata_loc
    return acc


def _load_or_none(catalog: Catalog, ident: TableIdent) -> Table | None:
    """Return ``catalog.load_table(ident)`` or ``None`` if the table doesn't exist."""
    tup: tuple[str, ...]
    if ident.namespace:
        # ``ns.t`` → ``(ns, t)``; ``cat.ns.t`` → ``(cat, ns, t)``.
        tup = (*ident.namespace.split("."), ident.name)
    else:
        tup = (ident.name,)
    try:
        return catalog.load_table(tup)
    except NoSuchTableError:
        return None
