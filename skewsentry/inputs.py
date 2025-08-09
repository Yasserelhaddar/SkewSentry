from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

import pandas as pd


PathLike = Union[str, Path]


def load_input(
    data: Union[pd.DataFrame, PathLike],
    sample: Optional[int] = None,
    seed: Optional[int] = None,
) -> pd.DataFrame:
    """Load input data from a pandas DataFrame or a file path.

    Supports CSV and Parquet paths. If ``sample`` is provided, returns a
    deterministic sample without replacement using ``seed``.
    """
    df = _load(data)
    if sample is not None:
        df = sample_dataframe(df, sample=sample, seed=seed)
    return df


def _load(data: Union[pd.DataFrame, PathLike]) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data.copy()
    if isinstance(data, (str, Path)):
        path = Path(data)
        suffix = path.suffix.lower()
        if suffix == ".csv":
            return pd.read_csv(path)
        if suffix == ".parquet":
            return pd.read_parquet(path)
        raise ValueError(f"Unsupported file type: {suffix}. Use .csv or .parquet")
    raise TypeError("data must be a pandas DataFrame or a path to .csv/.parquet")


def sample_dataframe(df: pd.DataFrame, sample: int, seed: Optional[int] = None) -> pd.DataFrame:
    """Return a deterministic sample of rows without replacement.

    If ``sample`` is greater than or equal to the number of rows, returns the original DataFrame.
    """
    if sample <= 0:
        raise ValueError("sample must be a positive integer")
    if sample >= len(df):
        return df
    return df.sample(n=sample, replace=False, random_state=seed)


def load_sql(_query: str) -> pd.DataFrame:
    """Stub for future SQL loading (v0.2 roadmap)."""
    raise NotImplementedError("SQL loading is not implemented in v0.1.0")

