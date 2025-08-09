from __future__ import annotations

from pathlib import Path
from typing import Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .runner import ComparisonReport


def _get_env() -> Environment:
    templates_dir = Path(__file__).parent / "templates"
    env = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        autoescape=select_autoescape(["html", "xml"]),
        enable_async=False,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    return env


def render_text(report: ComparisonReport, max_rows: int = 10) -> str:
    return report.to_text(max_rows=max_rows)


def render_html(report: ComparisonReport, json_href: Optional[str] = None, path: Optional[str] = None) -> str:
    env = _get_env()
    tpl = env.get_template("report.html.j2")
    html = tpl.render(report=report, json_href=json_href)
    if path is not None:
        Path(path).write_text(html, encoding="utf-8")
    return html

