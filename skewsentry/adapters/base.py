from __future__ import annotations

from typing import Protocol

import pandas as pd


class FeatureAdapter(Protocol):
    def get_features(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover - protocol signature
        ...

