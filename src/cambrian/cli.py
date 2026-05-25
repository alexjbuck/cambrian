"""Top-level Typer application.

M0 ships `--version` and `--help` only. Subsequent milestones register their
own commands on `app`.
"""

from typing import Annotated

import typer

from cambrian import __version__

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
