from __future__ import annotations

from pathlib import Path

import pytest

from skewsentry.spec import FeatureSpec


def _tmpfile(tmp_path: Path, name: str, content: str) -> str:
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return str(p)


def test_round_trip_yaml(tmp_path: Path) -> None:
    yaml_content = """
version: 1
keys: ["user_id", "ts"]
features:
  - name: spend_7d
    dtype: float
    nullable: true
    tolerance: { abs: 0.01, rel: 0.001 }
    window: { lookback_days: 7, timestamp_col: "ts", closed: "right" }
  - name: country
    dtype: category
    categories: [UK, US, DE]
    nullable: false
  - name: age
    dtype: int
    nullable: false
    range: [0, 120]
null_policy: same
"""
    path = _tmpfile(tmp_path, "features.yml", yaml_content)

    spec = FeatureSpec.from_yaml(path)
    assert spec.version == 1
    assert spec.keys == ["user_id", "ts"]
    assert len(spec.features) == 3
    assert spec.null_policy == "same"

    out_yaml = spec.to_yaml()
    path2 = _tmpfile(tmp_path, "features_roundtrip.yml", out_yaml)
    spec2 = FeatureSpec.from_yaml(path2)
    assert spec2 == spec


def test_invalid_duplicate_feature_names(tmp_path: Path) -> None:
    yaml_content = """
version: 1
keys: ["user_id"]
features:
  - name: f
    dtype: float
  - name: f
    dtype: float
"""
    path = _tmpfile(tmp_path, "dup.yml", yaml_content)
    with pytest.raises(ValueError):
        FeatureSpec.from_yaml(path)


def test_negative_tolerance_rejected(tmp_path: Path) -> None:
    yaml_content = """
version: 1
keys: ["user_id"]
features:
  - name: f
    dtype: float
    tolerance: { abs: -0.1 }
"""
    path = _tmpfile(tmp_path, "neg_tol.yml", yaml_content)
    with pytest.raises(ValueError):
        FeatureSpec.from_yaml(path)


def test_invalid_range_order(tmp_path: Path) -> None:
    yaml_content = """
version: 1
keys: ["user_id"]
features:
  - name: age
    dtype: int
    range: [10, 0]
"""
    path = _tmpfile(tmp_path, "bad_range.yml", yaml_content)
    with pytest.raises(ValueError):
        FeatureSpec.from_yaml(path)


def test_categories_no_duplicates(tmp_path: Path) -> None:
    yaml_content = """
version: 1
keys: ["user_id"]
features:
  - name: c
    dtype: category
    categories: [A, A]
"""
    path = _tmpfile(tmp_path, "dup_cat.yml", yaml_content)
    with pytest.raises(ValueError):
        FeatureSpec.from_yaml(path)

