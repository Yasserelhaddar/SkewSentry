from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator


SupportedDType = Literal["int", "float", "bool", "string", "category", "datetime"]
ClosedType = Literal["left", "right", "both", "neither"]
NullPolicy = Literal["same", "allow_both_null"]


class Tolerance(BaseModel):
    absolute: Optional[float] = Field(default=None, alias="abs")
    relative: Optional[float] = Field(default=None, alias="rel")

    @field_validator("absolute")
    @classmethod
    def validate_abs(cls, value: Optional[float]) -> Optional[float]:
        if value is None:
            return value
        if value < 0:
            raise ValueError("tolerance.abs must be non-negative")
        return float(value)

    @field_validator("relative")
    @classmethod
    def validate_rel(cls, value: Optional[float]) -> Optional[float]:
        if value is None:
            return value
        if value < 0:
            raise ValueError("tolerance.rel must be non-negative")
        return float(value)

    def model_dump_yaml(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        if self.absolute is not None:
            data["abs"] = self.absolute
        if self.relative is not None:
            data["rel"] = self.relative
        return data


class Window(BaseModel):
    lookback_days: int
    timestamp_col: str
    closed: ClosedType = "right"

    @field_validator("lookback_days")
    @classmethod
    def validate_lookback(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("window.lookback_days must be > 0")
        return value


class Feature(BaseModel):
    name: str
    dtype: SupportedDType
    nullable: bool = False
    tolerance: Optional[Tolerance] = None
    window: Optional[Window] = None
    categories: Optional[List[str]] = None
    value_range: Optional[Tuple[float, float]] = Field(default=None, alias="range")

    @field_validator("categories")
    @classmethod
    def validate_categories(cls, value: Optional[Sequence[str]]) -> Optional[List[str]]:
        if value is None:
            return None
        unique = list(dict.fromkeys(value))
        if len(unique) != len(value):
            raise ValueError("categories must not contain duplicates")
        return unique

    @model_validator(mode="after")
    def validate_internal_consistency(self) -> "Feature":
        if self.dtype == "category" and not self.categories:
            # categories optional in v0.1, but provide hint if missing
            pass
        if self.value_range is not None:
            lo, hi = self.value_range
            if lo > hi:
                raise ValueError("range lower bound must be <= upper bound")
            if self.dtype not in ("int", "float"):
                raise ValueError("range is only valid for numeric dtypes")
        return self


class FeatureSpec(BaseModel):
    version: int = 1
    keys: List[str]
    features: List[Feature]
    null_policy: NullPolicy = "same"

    @field_validator("keys")
    @classmethod
    def validate_keys(cls, value: List[str]) -> List[str]:
        if not value:
            raise ValueError("keys must contain at least one key column")
        if len(set(value)) != len(value):
            raise ValueError("keys must be unique")
        return value

    @field_validator("features")
    @classmethod
    def validate_feature_names_unique(cls, value: List[Feature]) -> List[Feature]:
        names = [f.name for f in value]
        if len(set(names)) != len(names):
            raise ValueError("feature names must be unique")
        return value

    # --- YAML I/O ---
    @classmethod
    def from_yaml(cls, path: str) -> "FeatureSpec":
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        # Normalize tolerance key names for pydantic aliases
        def _normalize(d: Any) -> Any:
            if isinstance(d, dict):
                return {k: _normalize(v) for k, v in d.items()}
            if isinstance(d, list):
                return [_normalize(x) for x in d]
            return d

        normalized = _normalize(data)
        try:
            return cls.model_validate(normalized)
        except ValidationError as exc:
            raise ValueError(f"Invalid FeatureSpec YAML: {exc}") from exc

    def to_yaml(self, path: Optional[str] = None) -> str:
        model_dict = self.model_dump(by_alias=True, exclude_none=True)
        # Re-map tolerance fields to {abs, rel}
        for feat in model_dict.get("features", []):
            tol = feat.get("tolerance")
            if tol is not None:
                # Already aliased by pydantic, but ensure only abs/rel keys
                feat["tolerance"] = {k: v for k, v in tol.items() if k in ("abs", "rel")}
        yaml_str = yaml.safe_dump(model_dict, sort_keys=False)
        if path is not None:
            with open(path, "w", encoding="utf-8") as f:
                f.write(yaml_str)
        return yaml_str

