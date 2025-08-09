from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd
import requests

from ..errors import AdapterError


@dataclass
class HTTPAdapter:
    url: str
    batch_size: int = 256
    headers: Dict[str, str] = field(default_factory=dict)
    timeout: Optional[float] = 10.0
    retries: int = 1

    def _post_batch(self, records: List[dict]) -> List[dict]:
        attempt = 0
        last_exc: Optional[Exception] = None
        while attempt <= self.retries:
            try:
                resp = requests.post(
                    self.url,
                    data=json.dumps(records),
                    headers={"Content-Type": "application/json", **(self.headers or {})},
                    timeout=self.timeout,
                )
                if resp.status_code != 200:
                    raise AdapterError(f"HTTP {resp.status_code}: {resp.text[:200]}")
                try:
                    data = resp.json()
                except Exception as exc:  # noqa: BLE001
                    raise AdapterError(f"Invalid JSON response: {exc}") from exc
                if not isinstance(data, list):
                    raise AdapterError("Expected JSON array from server")
                return data
            except (requests.RequestException, AdapterError) as exc:
                last_exc = exc
                attempt += 1
                if attempt > self.retries:
                    break
                time.sleep(min(0.05 * attempt, 0.5))
        raise AdapterError(f"Request failed after {self.retries + 1} attempts: {last_exc}")

    def get_features(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()
        out_rows: List[dict] = []
        total = len(df)
        for start in range(0, total, self.batch_size):
            batch_df = df.iloc[start : start + self.batch_size]
            records = batch_df.to_dict(orient="records")
            resp_records = self._post_batch(records)
            out_rows.extend(resp_records)
        try:
            out_df = pd.DataFrame(out_rows)
        except Exception as exc:  # noqa: BLE001
            raise AdapterError(f"Failed to construct DataFrame from response: {exc}") from exc
        return out_df

