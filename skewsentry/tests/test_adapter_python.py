from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

from skewsentry.adapters.python_func import PythonFunctionAdapter
from skewsentry.errors import AdapterError


def _write_module(tmp_path: Path) -> str:
    code = (
        "import pandas as pd\n"
        "def offline(df: pd.DataFrame) -> pd.DataFrame:\n"
        "    out = df.copy()\n"
        "    out['sum'] = out['a'] + out['b']\n"
        "    return out[['id','sum']]\n"
        "def not_df(df):\n"
        "    return 123\n"
    )
    mod_path = tmp_path / "mod_offline.py"
    mod_path.write_text(code, encoding="utf-8")
    sys.path.insert(0, str(tmp_path))
    return "mod_offline"


def test_python_adapter_happy_path(tmp_path: Path) -> None:
    module_name = _write_module(tmp_path)
    adapter = PythonFunctionAdapter(f"{module_name}:offline")
    df = pd.DataFrame({"id": [1, 2], "a": [10, 20], "b": [1, 2]})
    out = adapter.get_features(df)
    assert list(out.columns) == ["id", "sum"]
    assert out["sum"].tolist() == [11, 22]


def test_python_adapter_raises_on_bad_return(tmp_path: Path) -> None:
    module_name = _write_module(tmp_path)
    adapter = PythonFunctionAdapter(f"{module_name}:not_df")
    with pytest.raises(AdapterError):
        adapter.get_features(pd.DataFrame({"id": [1]}))


def test_python_adapter_import_errors() -> None:
    with pytest.raises(AdapterError):
        PythonFunctionAdapter("nope:func")
    with pytest.raises(AdapterError):
        PythonFunctionAdapter("skewsentry:does_not_exist")

