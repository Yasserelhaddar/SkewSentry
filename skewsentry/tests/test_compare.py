from __future__ import annotations

import numpy as np
import pandas as pd

from skewsentry.compare import compare_dataframe
from skewsentry.spec import Feature, FeatureSpec, Tolerance


def _spec_numeric() -> FeatureSpec:
    return FeatureSpec(
        version=1,
        keys=["id"],
        features=[
            Feature(name="x", dtype="float", nullable=True, tolerance=Tolerance(abs=0.01)),
        ],
        null_policy="same",
    )


def test_numeric_abs_tolerance() -> None:
    off = pd.DataFrame({"id": [1, 2, 3], "x": [1.00, 2.00, 3.00]})
    on = pd.DataFrame({"id": [1, 2, 3], "x": [1.005, 2.02, 2.99]})
    res = compare_dataframe(off, on, _spec_numeric())
    r = res[0]
    # id=2 is outside abs 0.01
    assert r.mismatch_mask.tolist() == [False, True, False]
    assert r.num_rows_compared == 3


def test_numeric_rel_tolerance() -> None:
    spec = FeatureSpec(
        version=1,
        keys=["id"],
        features=[Feature(name="x", dtype="float", tolerance=Tolerance(rel=0.05))],
        null_policy="same",
    )
    off = pd.DataFrame({"id": [1, 2], "x": [100.0, 0.1]})
    on = pd.DataFrame({"id": [1, 2], "x": [104.0, 0.099]})
    # 104 within 5% of 100, 0.099 within 5% of 0.1
    res = compare_dataframe(off, on, spec)
    assert res[0].mismatch_rate == 0.0


def test_null_policy_same_flags_mismatch() -> None:
    off = pd.DataFrame({"id": [1, 2], "x": [np.nan, 2.0]})
    on = pd.DataFrame({"id": [1, 2], "x": [1.0, np.nan]})
    res = compare_dataframe(off, on, _spec_numeric())
    # both rows are mismatches due to null policy 'same'
    assert res[0].mismatch_mask.tolist() == [True, True]


def test_category_and_unknowns() -> None:
    spec = FeatureSpec(
        version=1,
        keys=["id"],
        features=[Feature(name="country", dtype="category", categories=["US", "UK"])],
        null_policy="same",
    )
    off = pd.DataFrame({"id": [1, 2, 3], "country": ["US", "CA", "UK"]})
    on = pd.DataFrame({"id": [1, 2, 3], "country": ["US", "UK", "DE"]})
    res = compare_dataframe(off, on, spec)
    r = res[0]
    assert r.mismatch_mask.tolist() == [False, True, True]
    assert set(r.unknown_categories["offline_unknown"]) == {"CA"}
    assert set(r.unknown_categories["online_unknown"]) == {"DE"}


def test_string_and_datetime_equality() -> None:
    spec = FeatureSpec(
        version=1,
        keys=["id"],
        features=[
            Feature(name="name", dtype="string"),
            Feature(name="ts", dtype="datetime"),
        ],
        null_policy="same",
    )
    off = pd.DataFrame({"id": [1, 2], "name": ["a", "b"], "ts": pd.to_datetime(["2024-01-01", "2024-01-02"])})
    on = pd.DataFrame({"id": [1, 2], "name": ["a", "x"], "ts": pd.to_datetime(["2024-01-01", "2024-01-02"])})
    res = compare_dataframe(off, on, spec)
    name_res = next(r for r in res if r.feature_name == "name")
    ts_res = next(r for r in res if r.feature_name == "ts")
    assert name_res.mismatch_mask.tolist() == [False, True]
    assert ts_res.mismatch_rate == 0.0

