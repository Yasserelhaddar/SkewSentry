from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence, Tuple

import pandas as pd


@dataclass
class AlignmentDiagnostics:
    missing_in_online_count: int
    missing_in_offline_count: int
    missing_in_online_examples: pd.DataFrame
    missing_in_offline_examples: pd.DataFrame


def _ensure_no_duplicate_keys(df: pd.DataFrame, keys: Sequence[str], which: str) -> None:
    duplicated = df.duplicated(subset=list(keys), keep=False)
    if duplicated.any():
        dup_rows = df.loc[duplicated, list(keys)].head(5)
        raise ValueError(
            f"Duplicate keys detected in {which} data for keys {list(keys)}. Examples:\n{dup_rows}"
        )


def align_by_keys(
    offline_df: pd.DataFrame, online_df: pd.DataFrame, keys: Sequence[str]
) -> Tuple[pd.DataFrame, pd.DataFrame, AlignmentDiagnostics]:
    """Align offline and online DataFrames on given keys using an inner join.

    Returns two DataFrames filtered to the intersection of keys and sorted by keys,
    along with diagnostics about rows missing on either side.
    """
    if not keys:
        raise ValueError("keys must be a non-empty sequence")
    for k in keys:
        if k not in offline_df.columns or k not in online_df.columns:
            raise ValueError(f"Key column '{k}' missing in one of the inputs")

    _ensure_no_duplicate_keys(offline_df, keys, which="offline")
    _ensure_no_duplicate_keys(online_df, keys, which="online")

    offline_keys = offline_df[list(keys)].drop_duplicates()
    online_keys = online_df[list(keys)].drop_duplicates()

    # Identify missing on either side
    merged = offline_keys.merge(online_keys, on=list(keys), how="outer", indicator=True)
    missing_in_online = merged.loc[merged["_merge"] == "left_only", list(keys)]
    missing_in_offline = merged.loc[merged["_merge"] == "right_only", list(keys)]

    # Compute aligned keys (intersection) and sort for deterministic order
    aligned_keys = (
        offline_keys.merge(online_keys, on=list(keys), how="inner").sort_values(list(keys)).reset_index(drop=True)
    )

    # Join back to original frames to filter rows and preserve the sorted key order
    off_aligned = aligned_keys.merge(offline_df, on=list(keys), how="left")
    on_aligned = aligned_keys.merge(online_df, on=list(keys), how="left")

    diagnostics = AlignmentDiagnostics(
        missing_in_online_count=int(missing_in_online.shape[0]),
        missing_in_offline_count=int(missing_in_offline.shape[0]),
        missing_in_online_examples=missing_in_online.head(5),
        missing_in_offline_examples=missing_in_offline.head(5),
    )

    return off_aligned, on_aligned, diagnostics

