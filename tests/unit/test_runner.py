from __future__ import annotations

from pathlib import Path

import pandas as pd

from skewsentry.adapters.python import PythonFunctionAdapter
from skewsentry.runner import run_check
from skewsentry.spec import Feature, FeatureSpec, Tolerance


def _write_funcs(tmp_path: Path) -> str:
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
    mod = tmp_path / "funcs.py"
    mod.write_text(code, encoding="utf-8")
    return str(mod)


def test_runner_end_to_end(tmp_path: Path, monkeypatch) -> None:
    mod_path = _write_funcs(tmp_path)
    import sys

    sys.path.insert(0, str(tmp_path))
    offline = PythonFunctionAdapter("funcs:offline")
    online = PythonFunctionAdapter("funcs:online")

    spec = FeatureSpec(
        version=1,
        keys=["id"],
        features=[Feature(name="y", dtype="float", tolerance=Tolerance(abs=0.01))],
        null_policy="same",
    )
    df = pd.DataFrame({"id": [1, 2, 3], "x": [1.0, 2.0, 3.0]})

    report = run_check(spec=spec, data=df, offline=offline, online=online)
    assert report.ok is True
    txt = report.to_text()
    assert "OK: True" in txt
    assert any(f.feature_name == "y" for f in report.per_feature)

