from __future__ import annotations

import pandas as pd
import pytest

from skewsentry.adapters.feast_adapter import FeastAdapter


class DummyResp:
    def __init__(self, data):
        self._df = pd.DataFrame(data)

    def to_df(self):
        return self._df


class DummyClient:
    def __init__(self, data):
        self.data = data

    def get_online_features(self, features, entity_rows, project=None):
        return DummyResp(self.data)


def test_feast_adapter_normalizes_response():
    client = DummyClient(data={"id": [1, 2], "f1": [10, 20]})
    adapter = FeastAdapter(feature_refs=["f1"], entity_keys=["id"], client=client, project="proj")
    df = pd.DataFrame({"id": [1, 2]})
    out = adapter.get_features(df)
    assert list(out.columns) == ["id", "f1"]
    assert out["f1"].tolist() == [10, 20]

