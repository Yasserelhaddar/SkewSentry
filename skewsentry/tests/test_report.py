from __future__ import annotations

import pandas as pd

from skewsentry.report import render_html, render_text
from skewsentry.runner import ComparisonReport
from skewsentry.spec import Feature, FeatureSpec
from skewsentry.compare import PerFeatureComparison
from skewsentry.align import AlignmentDiagnostics


def _dummy_report() -> ComparisonReport:
    spec = FeatureSpec(version=1, keys=["id"], features=[Feature(name="x", dtype="float")])
    per = [
        PerFeatureComparison(
            feature_name="x",
            mismatch_mask=pd.Series([False, True, False]),
            mismatch_rate=1 / 3,
            num_rows_compared=3,
            mean_absolute_difference=0.1,
        )
    ]
    diag = AlignmentDiagnostics(
        missing_in_online_count=0,
        missing_in_offline_count=0,
        missing_in_online_examples=pd.DataFrame(),
        missing_in_offline_examples=pd.DataFrame(),
    )
    return ComparisonReport(spec=spec, keys=["id"], per_feature=per, alignment=diag)


def test_render_text_contains_key_info() -> None:
    rep = _dummy_report()
    txt = render_text(rep)
    assert "Per-feature" in txt
    assert "mismatch_rate" in txt


def test_render_html_contains_sections(tmp_path) -> None:
    rep = _dummy_report()
    html_path = tmp_path / "report.html"
    html = render_html(rep, json_href="report.json", path=str(html_path))
    assert html_path.exists()
    s = html
    assert "SkewSentry Report" in s
    assert "Per-feature" in s
    assert "Mismatch rate" in s or "Mismatch rate" in s

