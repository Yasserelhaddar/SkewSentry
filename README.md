## SkewSentry

Catch training ↔ serving feature skew before you ship. Compare offline vs online pipelines over the same rows, enforce tolerances, and produce text/HTML reports suitable for CI.

### Quickstart

```bash
# optional: use uv (recommended)
# pipx install uv

# or use Python venv quickly for local runs
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"  # installs dev tools

# CLI help (stub)
skewsentry --help
```

### What you get (v0.1)
- Alignment by keys
- Numeric abs/rel tolerances
- Categorical equality and unknown detection
- Clear text + HTML reports
- Adapters: Python function, HTTP, Feast (optional)

See `plan.md` for the detailed build plan.

