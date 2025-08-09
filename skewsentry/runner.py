from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

import json

import pandas as pd

from .align import AlignmentDiagnostics, align_by_keys
from .compare import PerFeatureComparison, compare_dataframe
from .inputs import load_input
from .spec import FeatureSpec


@dataclass
class ComparisonReport:
    spec: FeatureSpec
    keys: List[str]
    per_feature: List[PerFeatureComparison]
    alignment: AlignmentDiagnostics

    @property
    def ok(self) -> bool:
        if self.alignment.missing_in_offline_count > 0 or self.alignment.missing_in_online_count > 0:
            return False
        for r in self.per_feature:
            if r.mismatch_rate > 0:
                return False
            if r.unknown_categories and (r.unknown_categories.get("offline_unknown") or r.unknown_categories.get("online_unknown")):
                return False
        return True

    @property
    def summary(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "keys": self.keys,
            "missing_in_online": self.alignment.missing_in_online_count,
            "missing_in_offline": self.alignment.missing_in_offline_count,
            "features": [
                {
                    "name": r.feature_name,
                    "mismatch_rate": r.mismatch_rate,
                    "num_rows": r.num_rows_compared,
                    "mean_abs_diff": r.mean_absolute_difference,
                    "unknown_categories": r.unknown_categories,
                }
                for r in self.per_feature
            ],
            "failing_features": [r.feature_name for r in self.per_feature if r.mismatch_rate > 0.0],
        }

    def to_text(self, max_rows: int = 10) -> str:
        lines: List[str] = []
        lines.append(f"OK: {self.ok}")
        lines.append(
            f"Missing rows — offline: {self.alignment.missing_in_offline_count}, online: {self.alignment.missing_in_online_count}"
        )
        lines.append("Per-feature mismatch rates:")
        for r in self.per_feature:
            lines.append(
                f"  - {r.feature_name}: mismatch_rate={r.mismatch_rate:.4f} rows={r.num_rows_compared} mean_abs_diff={r.mean_absolute_difference}"
            )
            if r.unknown_categories:
                lines.append(f"    unknown: {r.unknown_categories}")
        return "\n".join(lines)

    def to_json(self, path: Optional[str] = None) -> str:
        data = self.summary
        s = json.dumps(data, ensure_ascii=False, indent=2)
        if path:
            with open(path, "w", encoding="utf-8") as f:
                f.write(s)
        return s

    def to_html(self, path: Optional[str] = None) -> str:
        # Simple HTML skeleton for v0.1 runner skeleton (report.py will replace)
        rows = "".join(
            f"<tr><td>{r.feature_name}</td><td>{r.mismatch_rate:.4f}</td><td>{r.num_rows_compared}</td><td>{r.mean_absolute_difference}</td></tr>"
            for r in self.per_feature
        )
        html = f"""
<!doctype html>
<html>
  <head>
    <meta charset=\"utf-8\" />
    <title>SkewSentry Report</title>
  </head>
  <body>
    <h1>SkewSentry Report</h1>
    <p>OK: {self.ok}</p>
    <p>Missing — offline: {self.alignment.missing_in_offline_count}, online: {self.alignment.missing_in_online_count}</p>
    <table border=\"1\" cellpadding=\"6\" cellspacing=\"0\">
      <thead><tr><th>Feature</th><th>Mismatch rate</th><th>Rows</th><th>Mean abs diff</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </body>
</html>
"""
        if path:
            with open(path, "w", encoding="utf-8") as f:
                f.write(html)
        return html


def run_check(
    spec: FeatureSpec,
    data: pd.DataFrame | str,
    offline,
    online,
    sample: Optional[int] = None,
    seed: Optional[int] = None,
    html_out: Optional[str] = None,
    json_out: Optional[str] = None,
) -> ComparisonReport:
    # 1) Load data
    base_df = load_input(data, sample=sample, seed=seed)

    # 2) Call adapters
    df_off = offline.get_features(base_df.copy())
    df_on = online.get_features(base_df.copy())

    # 3) Align rows by keys
    off_aligned, on_aligned, diag = align_by_keys(df_off, df_on, keys=spec.keys)

    # 4) Compare per spec
    per_feature = compare_dataframe(off_aligned, on_aligned, spec)

    report = ComparisonReport(spec=spec, keys=list(spec.keys), per_feature=per_feature, alignment=diag)

    # 5) Output artifacts
    if json_out:
        report.to_json(json_out)
    if html_out:
        report.to_html(html_out)

    return report

