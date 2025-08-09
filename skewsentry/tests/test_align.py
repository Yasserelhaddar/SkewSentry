from __future__ import annotations

import pandas as pd
import pytest

from skewsentry.align import AlignmentDiagnostics, align_by_keys


def test_align_happy_path() -> None:
    off = pd.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30]})
    on = pd.DataFrame({"id": [2, 3, 4], "x": [200, 300, 400]})
    off_al, on_al, diag = align_by_keys(off, on, keys=["id"])
    assert off_al["id"].tolist() == [2, 3]
    assert on_al["id"].tolist() == [2, 3]
    assert diag.missing_in_online_count == 1  # id=1
    assert diag.missing_in_offline_count == 1  # id=4


def test_align_duplicate_keys_raises() -> None:
    off = pd.DataFrame({"id": [1, 1], "x": [10, 20]})
    on = pd.DataFrame({"id": [1], "x": [100]})
    with pytest.raises(ValueError):
        align_by_keys(off, on, keys=["id"])


def test_align_missing_key_column() -> None:
    off = pd.DataFrame({"id": [1], "x": [10]})
    on = pd.DataFrame({"idx": [1], "x": [100]})
    with pytest.raises(ValueError):
        align_by_keys(off, on, keys=["id"])

