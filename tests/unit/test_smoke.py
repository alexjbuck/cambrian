"""Sanity tests that the package imports and exposes its version + CLI."""

from typer.testing import CliRunner

import cambrian
from cambrian.cli import app


def test_version_string_is_set() -> None:
    assert isinstance(cambrian.__version__, str)
    assert cambrian.__version__


def test_cli_version_flag() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert cambrian.__version__ in result.stdout


def test_cli_help_renders() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "cambrian" in result.stdout.lower()
