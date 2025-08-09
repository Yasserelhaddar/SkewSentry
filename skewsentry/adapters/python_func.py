from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Callable

import pandas as pd

from ..errors import AdapterError


def _import_callable(path: str) -> Callable[[pd.DataFrame], pd.DataFrame]:
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
    target: str

    def __post_init__(self) -> None:
        self._callable: Callable[[pd.DataFrame], pd.DataFrame] = _import_callable(self.target)

    def get_features(self, df: pd.DataFrame) -> pd.DataFrame:
        result = self._callable(df.copy())
        if not isinstance(result, pd.DataFrame):
            raise AdapterError("Adapter function must return a pandas DataFrame")
        return result

