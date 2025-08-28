from __future__ import annotations

from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from skewsentry.cli import app


runner = CliRunner()


def test_cli_check_end_to_end(tmp_path: Path):
    # Prepare tiny example data
    df = pd.DataFrame(
        {
            "user_id": [1, 1, 2, 2, 3, 3, 3],
            "ts": pd.to_datetime(
                [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-01",
                    "2024-01-03",
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                ]
            ),
            "price": [10, 10, 5, 5, 1, 1, 1],
            "qty": [1, 2, 2, 2, 1, 1, 1],
            "country": ["UK", "UK", "US", "US", "DE", "DE", "DE"],
        }
    )
    pq = tmp_path / "data.parquet"
    df.to_parquet(pq, index=False)

    # Write example feature funcs in tmp with unique names to avoid import cache conflicts
    (tmp_path / "cli_offline_features.py").write_text((Path("examples/python/offline_features.py").read_text()), encoding="utf-8")
    (tmp_path / "cli_online_features.py").write_text((Path("examples/python/online_features.py").read_text()), encoding="utf-8")

    # Spec file copied
    spec_path = tmp_path / "features.yml"
    spec_path.write_text((Path("examples/python/features.yml").read_text()), encoding="utf-8")

    import sys

    sys.path.insert(0, str(tmp_path))

    res = runner.invoke(
        app,
        [
            "check",
            "--spec",
            str(spec_path),
            "--offline",
            "cli_offline_features:build_features",
            "--online",
            "cli_online_features:get_features",
            "--data",
            str(pq),
            "--json",
            str(tmp_path / "rep.json"),
            "--html",
            str(tmp_path / "rep.html"),
        ],
    )
    assert res.exit_code in (0, 1)
    assert (tmp_path / "rep.json").exists()
    assert (tmp_path / "rep.html").exists()

