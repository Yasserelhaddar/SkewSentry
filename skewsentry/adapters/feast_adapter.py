from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence

import pandas as pd

from ..errors import AdapterError


@dataclass
class FeastAdapter:
    """Minimal Feast online store adapter (lazy import).

    This adapter expects a `client`-like object that provides a
    `get_online_features(features, entity_rows)` method and returns either:
      - an object with `.to_df()` -> pandas DataFrame
      - a list of dict rows
      - a dict of lists (column -> values)
    """

    feature_refs: Sequence[str]
    entity_keys: Sequence[str]
    client: object
    project: Optional[str] = None

    def _normalize_response(self, resp) -> pd.DataFrame:
        # Try OnlineResponse.to_df()
        if hasattr(resp, "to_df") and callable(getattr(resp, "to_df")):
            df = resp.to_df()
            if not isinstance(df, pd.DataFrame):
                raise AdapterError("Feast response .to_df() did not return a DataFrame")
            return df
        # Try list of dicts
        if isinstance(resp, list):
            return pd.DataFrame(resp)
        # Try dict of lists
        if isinstance(resp, dict):
            return pd.DataFrame(resp)
        raise AdapterError("Unsupported Feast response type")

    def get_features(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df[self.entity_keys].copy()

        try:
            entity_rows = df[self.entity_keys].to_dict(orient="records")
        except KeyError as exc:
            raise AdapterError(f"Missing entity key column: {exc}") from exc

        try:
            resp = self.client.get_online_features(
                features=list(self.feature_refs),
                entity_rows=entity_rows,
                project=self.project,
            )
        except TypeError:
            # Some Feast versions use positional args without `project`
            resp = self.client.get_online_features(list(self.feature_refs), entity_rows)
        except Exception as exc:  # noqa: BLE001
            raise AdapterError(f"Feast client error: {exc}") from exc

        out_df = self._normalize_response(resp)
        # Ensure entity keys are present (if Feast omits them, merge back)
        missing_keys = [k for k in self.entity_keys if k not in out_df.columns]
        if missing_keys:
            out_df = pd.concat([df[self.entity_keys].reset_index(drop=True), out_df.reset_index(drop=True)], axis=1)
        return out_df

