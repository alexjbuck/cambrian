"""Top-level Typer application.

M0 shipped ``--version`` and ``--help``. M1 adds the ``config`` sub-app for
loading, validating, and inspecting ``cambrian.toml``. M3 adds the lifecycle
commands ``init`` and ``status``. Later milestones register additional
sub-apps on ``app``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any

import typer

from cambrian import __version__
from cambrian.catalog import load_catalog
from cambrian.config import load_config, redacted_dump
from cambrian.errors import (
    CambrianError,
    NotInitializedError,
    SidecarVersionAheadError,
)
from cambrian.sidecar.events import committed_migrations, latest_event
from cambrian.sidecar.schema import CAMBRIAN_SIDECAR_VERSION
from cambrian.sidecar.selfmigrate import ensure_current

# Distinct exit codes so callers (CI scripts, wrappers) can distinguish
# "not initialized" from generic errors.
EXIT_NOT_INITIALIZED = 2
EXIT_VERSION_AHEAD = 3

app = typer.Typer(
    name="cambrian",
    help="SQL-driven migration runner for Apache Iceberg tables.",
    add_completion=False,
    no_args_is_help=True,
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"cambrian {__version__}")
        raise typer.Exit()


@app.callback()
def _root(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            help="Show the cambrian version and exit.",
            callback=_version_callback,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """cambrian — SQL-driven migration runner for Apache Iceberg tables."""


# ---------------------------------------------------------------------------
# `cambrian config` sub-app
# ---------------------------------------------------------------------------

config_app = typer.Typer(
    name="config",
    help="Inspect and validate the cambrian config file.",
    no_args_is_help=True,
)
app.add_typer(config_app, name="config")


def _path_option() -> typer.models.OptionInfo:
    """Build a fresh ``--path`` Option.

    Typer's Option instances aren't safe to share across commands (their
    decls list gets mutated by click during registration), so we mint a new
    one per command via this helper.
    """
    return typer.Option(
        "--path",
        "-p",
        help="Path to the cambrian config TOML.",
    )


def _render_human(data: dict[str, Any], indent: int = 0) -> str:
    """Render a dict as a TOML-ish indented summary for terminal output.

    Not strictly TOML — we keep it simple and readable. JSON output is
    available for machine consumers via ``--json``.
    """
    lines: list[str] = []
    prefix = "  " * indent
    # Render scalar fields first, then nested dicts.
    scalars = {k: v for k, v in data.items() if not isinstance(v, dict)}
    nested = {k: v for k, v in data.items() if isinstance(v, dict)}
    for key, value in scalars.items():
        lines.append(f"{prefix}{key} = {value!r}")
    for key, value in nested.items():
        lines.append(f"{prefix}[{key}]")
        lines.append(_render_human(value, indent=indent + 1))
    return "\n".join(line for line in lines if line)


@config_app.command("show")
def config_show(
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Emit machine-readable JSON instead of the human view."),
    ] = False,
) -> None:
    """Print the (redacted) effective config.

    Credential-shaped values (token, secret, password, credential, *_key
    patterns) are replaced with ``"***"``. See ``cambrian.config.redacted_dump``
    for the heuristic.
    """
    try:
        cfg = load_config(path)
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    dumped = redacted_dump(cfg)
    if as_json:
        typer.echo(json.dumps(dumped, indent=2, sort_keys=True, default=str))
    else:
        typer.echo(_render_human(dumped))


@config_app.command("check")
def config_check(
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
) -> None:
    """Validate the config file. Exits 0 on success, non-zero with a message on failure."""
    try:
        load_config(path)
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Config valid: {path}")


# ---------------------------------------------------------------------------
# `cambrian init` / `cambrian status`
# ---------------------------------------------------------------------------


def _load(path: Path) -> tuple[Any, Any]:
    """Helper: load config and catalog, mapping config errors to exit-1."""
    try:
        cfg = load_config(path)
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    catalog = load_catalog(cfg)
    return cfg, catalog


@app.command("init")
def init_command(
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
) -> None:
    """Bootstrap the ``_cambrian`` sidecar in the configured catalog.

    Idempotent: re-running on an already-initialised catalog is a no-op.
    Fails if the sidecar is at a *newer* version than this binary knows.
    """
    cfg, catalog = _load(path)
    namespace = cfg.migrations.sidecar_namespace

    from cambrian.sidecar.selfmigrate import _version_table_exists

    already = _version_table_exists(catalog, namespace)

    try:
        state = ensure_current(catalog, namespace, allow_read_only=False)
    except SidecarVersionAheadError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_VERSION_AHEAD) from exc
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    if already:
        typer.echo(
            f"Already initialized: sidecar at {state.sidecar_namespace} (version={state.version})"
        )
    else:
        typer.echo(f"Initialized sidecar at {state.sidecar_namespace} (version={state.version})")


def _status_payload(
    state: Any,
    committed: list[Any],
    current: Any,
) -> dict[str, Any]:
    return {
        "initialized": True,
        "sidecar_namespace": state.sidecar_namespace,
        "sidecar_version": state.version,
        "is_version_ahead": state.is_version_ahead,
        "committed_count": len(committed),
        "committed_migrations": [
            {
                "migration_id": c.migration_id,
                "event_id": c.event_id,
                "event_ts": c.event_ts.isoformat(),
            }
            for c in committed
        ],
        "current_applied": (
            None
            if current is None
            else {
                "event_id": current.event_id,
                "event_ts": current.event_ts.isoformat(),
                "migration_hash": current.migration_hash,
            }
        ),
    }


@app.command("status")
def status_command(
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Emit machine-readable JSON instead of the human view."),
    ] = False,
) -> None:
    """Report sidecar version, committed migrations, and the current in-flight apply.

    Read-only: tolerates a sidecar that is one or more versions ahead of
    this binary (prints a warning). Exits ``2`` if the sidecar has never
    been initialised in this catalog.
    """
    cfg, catalog = _load(path)
    namespace = cfg.migrations.sidecar_namespace

    try:
        state = ensure_current(catalog, namespace, allow_read_only=True)
    except NotInitializedError as exc:
        if as_json:
            typer.echo(
                json.dumps(
                    {
                        "initialized": False,
                        "sidecar_namespace": namespace,
                        "hint": "run `cambrian init`",
                    },
                    indent=2,
                )
            )
        else:
            typer.echo(
                f"error: sidecar not initialized in namespace '{namespace}'; run `cambrian init`",
                err=True,
            )
        raise typer.Exit(code=EXIT_NOT_INITIALIZED) from exc

    committed = committed_migrations(catalog, namespace)
    current = latest_event(catalog, namespace, event_type="apply", migration_id="current")

    if as_json:
        typer.echo(json.dumps(_status_payload(state, committed, current), indent=2))
        return

    if state.is_version_ahead:
        typer.echo(
            f"warning: sidecar is at version {state.version}, this cambrian only "
            f"understands up to version {CAMBRIAN_SIDECAR_VERSION}; running in read-only mode.",
            err=True,
        )

    typer.echo(f"sidecar namespace: {state.sidecar_namespace}")
    typer.echo(f"sidecar version: {state.version}")
    typer.echo(f"committed migrations: {len(committed)}")
    for c in committed:
        typer.echo(f"  - {c.migration_id} (event {c.event_id} @ {c.event_ts.isoformat()})")
    if current is None:
        typer.echo("current applied: <none>")
    else:
        typer.echo(
            f"current applied: event {current.event_id} @ {current.event_ts.isoformat()} "
            f"(hash {current.migration_hash[:12]}…)"
        )
