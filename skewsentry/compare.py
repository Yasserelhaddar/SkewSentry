from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from .spec import Feature, FeatureSpec, Tolerance


NUMERIC_DTYPES = {"int", "float"}
STRING_DTYPES = {"string"}
CATEGORY_DTYPES = {"category"}
DATETIME_DTYPES = {"datetime"}


@dataclass
class PerFeatureComparison:
    feature_name: str
    mismatch_mask: pd.Series
    mismatch_rate: float
    num_rows_compared: int
    mean_absolute_difference: Optional[float] = None
    unknown_categories: Optional[Dict[str, List[str]]] = None


def _null_equal_mask(a: pd.Series, b: pd.Series) -> pd.Series:
    return a.isna() & b.isna()


def _null_mismatch_mask(a: pd.Series, b: pd.Series) -> pd.Series:
    return a.isna() ^ b.isna()


def _numeric_mismatch(
    a: pd.Series,
    b: pd.Series,
    tolerance: Optional[Tolerance],
    null_policy: str,
) -> Tuple[pd.Series, Optional[float]]:
    a_vals = pd.to_numeric(a, errors="coerce")
    b_vals = pd.to_numeric(b, errors="coerce")

    null_mismatch = _null_mismatch_mask(a_vals, b_vals)
    both_null = _null_equal_mask(a_vals, b_vals)

    abs_diff = (a_vals - b_vals).abs()

    if tolerance is None:
        tol_abs = 0.0
        tol_rel = None
    else:
        tol_abs = float(tolerance.absolute or 0.0)
        tol_rel = float(tolerance.relative) if tolerance.relative is not None else None

    abs_ok = abs_diff <= tol_abs

    if tol_rel is not None:
        eps = np.finfo(float).eps
        denom = np.maximum(np.maximum(a_vals.abs(), b_vals.abs()), eps)
        rel_ok = abs_diff <= tol_rel * denom
        within_tolerance = abs_ok | rel_ok
    else:
        within_tolerance = abs_ok

    mismatch = ~within_tolerance

    if null_policy == "same":
        mismatch = mismatch | null_mismatch
        mismatch = mismatch & ~both_null
    elif null_policy == "allow_both_null":
        mismatch = mismatch | null_mismatch
        mismatch = mismatch & ~both_null
    else:
        raise ValueError(f"Unsupported null policy: {null_policy}")

    mean_abs = float(abs_diff[~both_null & ~null_mismatch].mean()) if len(abs_diff) else None
    return mismatch.fillna(True), mean_abs


def _equality_mismatch(
    a: pd.Series,
    b: pd.Series,
    null_policy: str,
) -> pd.Series:
    null_mismatch = _null_mismatch_mask(a, b)
    both_null = _null_equal_mask(a, b)
    eq = a == b
    eq = eq.fillna(False)
    mismatch = ~eq
    if null_policy in {"same", "allow_both_null"}:
        mismatch = mismatch | null_mismatch
        mismatch = mismatch & ~both_null
    else:
        raise ValueError(f"Unsupported null policy: {null_policy}")
    return mismatch


def _category_mismatch(
    a: pd.Series,
    b: pd.Series,
    categories: Optional[Sequence[str]],
    null_policy: str,
) -> Tuple[pd.Series, Dict[str, List[str]]]:
    mismatch = _equality_mismatch(a, b, null_policy=null_policy)
    unknown: Dict[str, List[str]] = {"offline_unknown": [], "online_unknown": []}
    if categories is not None:
        cat_set = set(categories)
        offline_unknown = sorted(set(a.dropna().astype(str)) - cat_set)
        online_unknown = sorted(set(b.dropna().astype(str)) - cat_set)
        if offline_unknown:
            unknown["offline_unknown"] = offline_unknown
        if online_unknown:
            unknown["online_unknown"] = online_unknown
    return mismatch, unknown


def compare_dataframe(
    offline_df: pd.DataFrame, online_df: pd.DataFrame, spec: FeatureSpec
) -> List[PerFeatureComparison]:
    results: List[PerFeatureComparison] = []
    keys = list(spec.keys)

    merged = offline_df.merge(online_df, on=keys, suffixes=("_off", "_on"))
    num_rows = len(merged)

    for feat in spec.features:
        a = merged[f"{feat.name}_off"] if f"{feat.name}_off" in merged else offline_df[feat.name]
        b = merged[f"{feat.name}_on"] if f"{feat.name}_on" in merged else online_df[feat.name]

        if feat.dtype in NUMERIC_DTYPES:
            mismatch, mean_abs = _numeric_mismatch(a, b, feat.tolerance, spec.null_policy)
            result = PerFeatureComparison(
                feature_name=feat.name,
                mismatch_mask=mismatch,
                mismatch_rate=float(mismatch.mean()) if num_rows else 0.0,
                num_rows_compared=num_rows,
                mean_absolute_difference=mean_abs,
            )
        elif feat.dtype in CATEGORY_DTYPES:
            mismatch, unknown = _category_mismatch(a, b, feat.categories, spec.null_policy)
            result = PerFeatureComparison(
                feature_name=feat.name,
                mismatch_mask=mismatch,
                mismatch_rate=float(mismatch.mean()) if num_rows else 0.0,
                num_rows_compared=num_rows,
                unknown_categories=unknown,
            )
        elif feat.dtype in STRING_DTYPES or feat.dtype in DATETIME_DTYPES:
            mismatch = _equality_mismatch(a, b, spec.null_policy)
            result = PerFeatureComparison(
                feature_name=feat.name,
                mismatch_mask=mismatch,
                mismatch_rate=float(mismatch.mean()) if num_rows else 0.0,
                num_rows_compared=num_rows,
            )
        else:
            raise ValueError(f"Unsupported dtype for comparison: {feat.dtype}")

        results.append(result)

    return results

