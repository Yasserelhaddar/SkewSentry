"""Python function adapter for SkewSentry feature pipelines.

This module provides the PythonFunctionAdapter class for integrating Python functions
as feature sources. Functions are imported by module:function string paths and called
with DataFrame inputs to produce feature outputs.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Callable

import pandas as pd

from ..errors import AdapterError


def _import_callable(path: str) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Import a Python function from module:function path string.
    
    Args:
        path: Module and function path in format "module.name:function_name"
        
    Returns:
        Imported callable function
        
    Raises:
        AdapterError: If module/function cannot be imported or is not callable
        
    Example:
        >>> func = _import_callable("mymodule:transform_data")
    """
    if ":" not in path:
        raise AdapterError("Expected module:function path, e.g., 'pkg.mod:build_features'")
    module_name, func_name = path.split(":", 1)
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:  # noqa: BLE001
        raise AdapterError(f"Could not import module '{module_name}': {exc}") from exc
    try:
        func = getattr(module, func_name)
    except AttributeError as exc:
        raise AdapterError(f"Function '{func_name}' not found in module '{module_name}'") from exc
    if not callable(func):
        raise AdapterError(f"'{func_name}' is not callable in module '{module_name}'")
    return func


@dataclass
class PythonFunctionAdapter:
    """Feature adapter for Python functions imported by module:function path.
    
    Imports and calls Python functions that transform DataFrames. Functions must
    accept a DataFrame and return a DataFrame with the same index/keys but with
    computed feature columns.
    
    Attributes:
        target: Module and function path in format "module.name:function_name"
        
    Example:
        >>> adapter = PythonFunctionAdapter("features.offline:build_features")
        >>> result = adapter.get_features(input_df)
    """
    target: str

    def __post_init__(self) -> None:
        """Import and validate the target function on initialization."""
        self._callable: Callable[[pd.DataFrame], pd.DataFrame] = _import_callable(self.target)

    def get_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Get features by calling the imported Python function.
        
        Args:
            df: Input DataFrame with entity keys and any required columns
            
        Returns:
            DataFrame with computed features, preserving original index/keys
            
        Raises:
            AdapterError: If function returns non-DataFrame result
        """
        result = self._callable(df.copy())
        if not isinstance(result, pd.DataFrame):
            raise AdapterError("Adapter function must return a pandas DataFrame")
        return result

