from __future__ import annotations

import sys
from typing import Optional

import typer

from . import __version__


app = typer.Typer(no_args_is_help=True, add_completion=False, help="SkewSentry CLI")


@app.command()
def version() -> None:
    """Print SkewSentry version."""
    typer.echo(__version__)


@app.callback()
def main(
    ctx: typer.Context,
    _version: Optional[bool] = typer.Option(
        None,
        "--version",
        help="Show version and exit",
        callback=lambda v: (typer.echo(__version__), sys.exit(0)) if v else None,
        is_eager=True,
    ),
) -> None:
    # Root callback for future global options
    return None


if __name__ == "__main__":
    app()

