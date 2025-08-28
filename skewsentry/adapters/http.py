"""HTTP REST API adapter for SkewSentry feature pipelines.

This module provides the HTTPAdapter class for integrating HTTP/REST API endpoints
as feature sources. Handles JSON serialization, batching, retries, and automatic
timestamp conversion for seamless DataFrame integration.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd
import requests

from ..errors import AdapterError
from ..utils import get_logger

logger = get_logger(__name__)


@dataclass
class HTTPAdapter:
    """Feature adapter for HTTP REST API endpoints with batch processing.
    
    Makes POST requests to HTTP endpoints with JSON payloads containing input
    data records. Handles batching, retries, and automatic timestamp serialization
    for pandas Timestamp objects.
    
    Attributes:
        url: HTTP endpoint URL for feature requests
        batch_size: Maximum records per request batch (default: 256)
        headers: Additional HTTP headers to send with requests
        timeout: Request timeout in seconds (default: 10.0)
        retries: Number of retry attempts on failure (default: 1)
        
    Example:
        >>> adapter = HTTPAdapter("http://localhost:8080/features", timeout=30.0)
        >>> result = adapter.get_features(input_df)
    """
    url: str
    batch_size: int = 256
    headers: Dict[str, str] = field(default_factory=dict)
    timeout: Optional[float] = 10.0
    retries: int = 1

    def _post_batch(self, records: List[dict]) -> List[dict]:
        """Send a batch of records to the HTTP endpoint with retry logic.
        
        Args:
            records: List of dictionaries representing input data records
            
        Returns:
            List of feature record dictionaries from the server response
            
        Raises:
            AdapterError: If all retry attempts fail or server returns error
        """
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
                logger.debug("Request attempt %d failed, retrying: %s", attempt, exc)
                time.sleep(min(0.05 * attempt, 0.5))
        raise AdapterError(f"Request failed after {self.retries + 1} attempts: {last_exc}")

    def get_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Get features by sending batched HTTP POST requests to the endpoint.
        
        Automatically handles timestamp serialization for JSON and batch processing
        to avoid memory/timeout issues with large datasets.
        
        Args:
            df: Input DataFrame with entity keys and any required columns
            
        Returns:
            DataFrame with computed features from HTTP endpoint responses
            
        Raises:
            AdapterError: If HTTP requests fail or response format is invalid
        """
        if df.empty:
            return df.copy()
        out_rows: List[dict] = []
        total = len(df)
        logger.info("Processing %d records in batches of %d", total, self.batch_size)
        for start in range(0, total, self.batch_size):
            batch_df = df.iloc[start : start + self.batch_size]
            logger.debug("Processing batch %d-%d of %d", start, min(start + self.batch_size, total), total)
            # Convert timestamps to strings for JSON serialization
            records = []
            for _, row in batch_df.iterrows():
                record = {}
                for col, value in row.items():
                    if pd.isna(value):
                        record[col] = None
                    elif isinstance(value, pd.Timestamp):
                        record[col] = value.isoformat()
                    else:
                        record[col] = value
                records.append(record)
            
            resp_records = self._post_batch(records)
            out_rows.extend(resp_records)
        try:
            out_df = pd.DataFrame(out_rows)
            # Convert timestamp strings back to datetime if present
            for col in out_df.columns:
                if col == 'timestamp' or col.endswith('_timestamp') or col.endswith('_time'):
                    if out_df[col].dtype == 'object':
                        try:
                            out_df[col] = pd.to_datetime(out_df[col])
                        except (ValueError, TypeError):
                            pass  # Keep as string if conversion fails
        except Exception as exc:  # noqa: BLE001
            raise AdapterError(f"Failed to construct DataFrame from response: {exc}") from exc
        return out_df

