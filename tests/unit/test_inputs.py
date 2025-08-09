from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from skewsentry.inputs import load_input, sample_dataframe


def _make_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "value": [0.1, 0.2, 0.3, 0.4, 0.5],
            "name": ["a", "b", "c", "d", "e"],
        }
    )


def test_load_from_dataframe_passthrough() -> None:
    df = _make_df()
    out = load_input(df)
    assert out.equals(df) is True
    assert out is not df  # copy


def test_load_csv_round_trip(tmp_path: Path) -> None:
    df = _make_df()
    csv_path = tmp_path / "data.csv"
    df.to_csv(csv_path, index=False)
    loaded = load_input(str(csv_path))
    # CSV re-reads should preserve values
    assert loaded.equals(df)


def test_load_parquet_round_trip(tmp_path: Path) -> None:
    df = _make_df()
    pq_path = tmp_path / "data.parquet"
    df.to_parquet(pq_path, index=False)
    loaded = load_input(pq_path)
    # Parquet preserves dtypes and values
    assert list(loaded.dtypes) == list(df.dtypes)
    assert loaded.equals(df)


def test_sampling_is_deterministic() -> None:
    df = pd.DataFrame({"x": list(range(100))})
    s1 = sample_dataframe(df, sample=10, seed=42)
    s2 = sample_dataframe(df, sample=10, seed=42)
    assert s1.index.tolist() == s2.index.tolist()
    s3 = sample_dataframe(df, sample=10, seed=43)
    assert s1.index.tolist() != s3.index.tolist()
    assert len(s1) == 10 and len(s2) == 10 and len(s3) == 10


def test_sampling_bounds() -> None:
    df = _make_df()
    # requesting >= len(df) returns original
    s = sample_dataframe(df, sample=len(df), seed=1)
    assert s.equals(df)
    with pytest.raises(ValueError):
        sample_dataframe(df, sample=0, seed=1)

