"""Base protocol definition for SkewSentry feature adapters.

This module defines the FeatureAdapter protocol that all adapter implementations
must follow. The protocol ensures consistent interface across different feature
sources (Python functions, HTTP APIs, etc.).
"""

from __future__ import annotations

from typing import Protocol

import pandas as pd


class FeatureAdapter(Protocol):
    """Protocol for feature adapter implementations.
    
    All feature adapters must implement this protocol to be compatible with
    SkewSentry's comparison pipeline. The protocol defines a single method
    that transforms input DataFrames into feature DataFrames.
    
    Example:
        >>> class MyAdapter:
        ...     def get_features(self, df: pd.DataFrame) -> pd.DataFrame:
        ...         # Compute and return features
        ...         return df.assign(feature1=df['col1'] * 2)
    """
    
    def get_features(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover - protocol signature
        """Transform input DataFrame into feature DataFrame.
        
        Args:
            df: Input DataFrame containing entity keys and source data
            
        Returns:
            DataFrame with computed features, preserving entity keys
        """
        ...

