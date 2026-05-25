"""Top-level Typer application.

M0 shipped ``--version`` and ``--help``. M1 adds the ``config`` sub-app for
loading, validating, and inspecting ``cambrian.toml``. Later milestones
register additional sub-apps on ``app``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any

import typer

from cambrian import __version__
from cambrian.config import load_config, redacted_dump
from cambrian.errors import CambrianError

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
