from __future__ import annotations

from pathlib import Path

import pandas as pd

from skewsentry.adapters.python import PythonFunctionAdapter
from skewsentry.runner import run_check
from skewsentry.spec import FeatureSpec


def test_runner_end_to_end_example(tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "user_id": [1, 1, 2, 2, 3, 3, 3],
            "ts": pd.to_datetime([
                "2024-01-01",
                "2024-01-02",
                "2024-01-01",
                "2024-01-03",
                "2024-01-01",
                "2024-01-02",
                "2024-01-03",
            ]),
            "price": [10, 10, 5, 5, 1, 1, 1],
            "qty": [1, 2, 2, 2, 1, 1, 1],
            "country": ["UK", "UK", "US", "US", "DE", "DE", "DE"],
        }
    )
    pq = tmp_path / "data.parquet"
    df.to_parquet(pq, index=False)

    (tmp_path / "python_offline_features.py").write_text(Path("examples/python/offline_features.py").read_text(), encoding="utf-8")
    (tmp_path / "python_online_features.py").write_text(Path("examples/python/online_features.py").read_text(), encoding="utf-8")
    spec_path = tmp_path / "features.yml"
    spec_path.write_text(Path("examples/python/features.yml").read_text(), encoding="utf-8")

    import sys

    sys.path.insert(0, str(tmp_path))

    spec = FeatureSpec.from_yaml(str(spec_path))
    off = PythonFunctionAdapter("python_offline_features:build_features")
    on = PythonFunctionAdapter("python_online_features:get_features")
    report = run_check(spec=spec, data=str(pq), offline=off, online=on)
    assert isinstance(report.ok, bool)

