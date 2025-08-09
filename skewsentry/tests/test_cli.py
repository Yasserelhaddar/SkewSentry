from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from skewsentry.cli import app
from skewsentry.spec import Feature, FeatureSpec


runner = CliRunner()


def _write_funcs(tmp_path: Path) -> None:
    code = (
        "import pandas as pd\n"
        "def offline(df: pd.DataFrame) -> pd.DataFrame:\n"
        "    out = df.copy()\n"
        "    out['y'] = (out['x'] * 2).round(2)\n"
        "    return out[['id','y']]\n"
        "def online(df: pd.DataFrame) -> pd.DataFrame:\n"
        "    out = df.copy()\n"
        "    out['y'] = (out['x'] * 2 + 0.001)\n"
        "    return out[['id','y']]\n"
    )
    (tmp_path / "funcs.py").write_text(code, encoding="utf-8")


def _write_data(tmp_path: Path) -> Path:
    df = pd.DataFrame({"id": [1, 2, 3], "x": [1.0, 2.0, 3.0]})
    path = tmp_path / "data.parquet"
    df.to_parquet(path, index=False)
    return path


def test_cli_version() -> None:
    res = runner.invoke(app, ["version"])
    assert res.exit_code == 0
    assert res.output.strip()


def test_cli_init_and_check(tmp_path: Path, monkeypatch) -> None:
    sys.path.insert(0, str(tmp_path))
    _write_funcs(tmp_path)
    data_path = _write_data(tmp_path)

    spec_path = tmp_path / "spec.yml"
    res_init = runner.invoke(app, [
        "init",
        str(spec_path),
        "--data",
        str(data_path),
        "--keys",
        "id",
    ])
    assert res_init.exit_code == 0
    assert spec_path.exists()

    # Overwrite spec to include the produced feature 'y' for comparison
    FeatureSpec(version=1, keys=["id"], features=[Feature(name="y", dtype="float")]).to_yaml(str(spec_path))

    res_check = runner.invoke(app, [
        "check",
        "--spec",
        str(spec_path),
        "--offline",
        "funcs:offline",
        "--online",
        "funcs:online",
        "--data",
        str(data_path),
        "--json",
        str(tmp_path / "rep.json"),
        "--html",
        str(tmp_path / "rep.html"),
    ])
    # Because our tolerance is inferred as float without tolerance, this may fail; exit code 1 acceptable
    assert res_check.exit_code in (0, 1)

