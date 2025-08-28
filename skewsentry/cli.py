from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import List, Optional

# Ensure current working directory is in Python path for module imports
if os.getcwd() not in sys.path:
    sys.path.insert(0, os.getcwd())

import pandas as pd
import typer

from . import __version__
from .errors import ConfigurationError
from .runner import run_check
from .spec import Feature, FeatureSpec
from .adapters.python import PythonFunctionAdapter
from .adapters.http import HTTPAdapter


app = typer.Typer(no_args_is_help=True, add_completion=False, help="SkewSentry CLI")


@app.command()
def version() -> None:
    """Print SkewSentry version."""
    typer.echo(__version__)


def _infer_dtype(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "int"
    if pd.api.types.is_float_dtype(series):
        return "float"
    if pd.api.types.is_bool_dtype(series):
        return "bool"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    if pd.api.types.is_categorical_dtype(series):
        return "category"
    return "string"


@app.command(help="Scaffold a spec by inferring basic dtypes from a data sample")
def init(
    spec: str = typer.Argument(..., help="Path to write the spec YAML"),
    data: str = typer.Option(..., "--data", help="Path to CSV or Parquet to infer from"),
    keys: List[str] = typer.Option(..., "--keys", help="Key columns for alignment", show_default=False),
) -> None:
    try:
        path = Path(data)
        if path.suffix.lower() == ".csv":
            df = pd.read_csv(path)
        elif path.suffix.lower() == ".parquet":
            df = pd.read_parquet(path)
        else:
            raise ConfigurationError("--data must be a .csv or .parquet file")

        features: List[Feature] = []
        for col in df.columns:
            if col in keys:
                continue
            dtype = _infer_dtype(df[col])
            nullable = bool(df[col].isna().any())
            features.append(Feature(name=col, dtype=dtype, nullable=nullable))

        spec_obj = FeatureSpec(version=1, keys=keys, features=features)
        spec_obj.to_yaml(spec)
        typer.echo(f"Wrote spec to {spec}")
    except Exception as exc:  # noqa: BLE001
        typer.echo(f"Error: {exc}")
        raise typer.Exit(code=2)


@app.command(help="Run parity check between offline and online feature pipelines")
def check(
    spec: str = typer.Option(..., "--spec", help="Path to spec YAML"),
    offline: str = typer.Option(..., "--offline", help="Offline adapter (module:function)"),
    online: str = typer.Option(..., "--online", help="Online adapter (module:function)"),
    data: str = typer.Option(..., "--data", help="Input data (.csv or .parquet)"),
    sample: Optional[int] = typer.Option(None, "--sample", help="Sample size"),
    seed: Optional[int] = typer.Option(None, "--seed", help="Sampling seed"),
    html: Optional[str] = typer.Option(None, "--html", help="Write HTML report to path"),
    json_out: Optional[str] = typer.Option(None, "--json", help="Write JSON report to path"),
    timeout: Optional[float] = typer.Option(None, "--timeout", help="Timeout (reserved for HTTP adapter)"),
) -> None:
    try:
        spec_obj = FeatureSpec.from_yaml(spec)
        
        # Create offline adapter (always Python function)
        offline_adapter = PythonFunctionAdapter(offline)
        
        # Create online adapter (detect HTTP URLs vs Python functions)
        if online.startswith(('http://', 'https://')):
            online_adapter = HTTPAdapter(url=online, timeout=timeout or 10.0)
        else:
            online_adapter = PythonFunctionAdapter(online)

        report = run_check(
            spec=spec_obj,
            data=data,
            offline=offline_adapter,
            online=online_adapter,
            sample=sample,
            seed=seed,
            html_out=html,
            json_out=json_out,
        )

        if html:
            typer.echo(f"Wrote HTML: {html}")
        if json_out:
            typer.echo(f"Wrote JSON: {json_out}")

        code = 0 if report.ok else 1
        raise typer.Exit(code=code)
    except typer.Exit:
        # Re-raise intended exits without wrapping as error
        raise
    except Exception as exc:  # noqa: BLE001
        typer.echo(f"Error: {exc}")
        raise typer.Exit(code=2)


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
    return None


if __name__ == "__main__":
    app()

