# SkewSentry

<div align="center">

**Catch training ↔ serving feature skew before you ship**

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Test Coverage](https://img.shields.io/badge/coverage-86%25-brightgreen.svg)](https://pytest.org/)

Compare offline vs online feature pipelines with configurable tolerances and generate actionable reports for CI integration.

</div>

## Table of Contents
- [Why SkewSentry?](#why-skewsentry)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Usage](#usage)
- [Feature Specification](#feature-specification)
- [Adapters](#adapters)
- [Reporting](#reporting)
- [CI Integration](#ci-integration)
- [Examples](#examples)
- [Development](#development)
- [Roadmap](#roadmap)

## Why SkewSentry?

**The Problem**: 70% of ML model failures in production stem from training/serving skew - subtle differences between how features are computed during training vs serving. These bugs are notoriously difficult to detect and can silently degrade model performance for months.

**Common Sources of Skew**:
- Different windowing logic (`min_periods=1` vs `closed="left"`)
- Rounding differences (`round(2)` vs `math.floor`)
- Timezone handling inconsistencies
- Library version differences
- Data type conversions

**The Solution**: SkewSentry validates feature parity by running identical input data through both pipelines and comparing outputs with configurable tolerances. Catch these issues in CI before they reach production.

## Quick Start

```bash
# Install
pip install -e ".[dev]"

# Initialize spec from your data
skewsentry init features.yml --data validation.parquet --keys user_id timestamp

# Run parity check
skewsentry check \
  --spec features.yml \
  --offline training.pipeline:extract_features \
  --online serving.api:get_features \
  --data validation.parquet \
  --html report.html

# ✅ Exit 0: Features match within tolerance
# ❌ Exit 1: Parity violations detected (fails CI)
# 🚨 Exit 2: Configuration error
```

## Installation

**Requirements**: Python 3.9+, pandas 2.0+

### Using uv (Recommended)
```bash
pipx install uv
uv venv .venv && source .venv/bin/activate
uv pip install -e ".[dev]"
```

### Using pip
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### Dependencies
- **Core**: pandas, pydantic, typer, jinja2, pyyaml, requests
- **Optional**: feast (feature store integration), scipy (statistical tests)
- **Dev**: pytest, pytest-cov, rich, tabulate

## Usage

### Command Line Interface

#### Initialize Feature Spec
```bash
skewsentry init features.yml \
  --data sample_data.parquet \
  --keys user_id timestamp
```

#### Run Parity Check
```bash
skewsentry check \
  --spec features.yml \
  --offline module.offline:build_features \
  --online module.online:get_features \
  --data validation.parquet \
  --sample 10000 \
  --seed 42 \
  --html artifacts/report.html \
  --json artifacts/results.json
```

### Python API

```python
from skewsentry import FeatureSpec
from skewsentry.adapters.python_func import PythonFunctionAdapter
from skewsentry.adapters.http_adapter import HTTPAdapter
from skewsentry.runner import run_check

# Define feature comparison rules
spec = FeatureSpec.from_yaml("features.yml")

# Set up adapters for your pipelines
offline_adapter = PythonFunctionAdapter("training.pipeline:extract_features")
online_adapter = HTTPAdapter("https://api.myservice.com/features")

# Run comparison
report = run_check(
    spec=spec,
    data="validation_data.parquet",  # or DataFrame
    offline=offline_adapter,
    online=online_adapter,
    sample=5000,
    seed=42,
    html_out="report.html",
    json_out="results.json"
)

# Check results
if report.ok:
    print("✅ All features match within tolerance")
else:
    print("❌ Feature parity violations detected:")
    print(report.to_text(max_rows=10))
    
    # Fail CI/CD pipeline
    raise SystemExit(1)
```  

## Feature Specification

SkewSentry uses YAML configuration to define feature comparison rules:

```yaml
version: 1
keys: ["user_id", "timestamp"]  # Row alignment keys
null_policy: "same"              # "same" | "allow_both_null"

features:
  # Numeric features with tolerance
  - name: spend_7d
    dtype: float
    nullable: true
    tolerance:
      abs: 0.01      # Absolute tolerance (optional)
      rel: 0.001     # Relative tolerance (optional)
    window:
      lookback_days: 7
      timestamp_col: "timestamp"
      closed: "right"
      
  # Categorical features with validation
  - name: country
    dtype: category
    categories: ["US", "UK", "DE", "FR"]  # Expected values
    nullable: false
    
  # Integer features with range validation
  - name: age
    dtype: int
    nullable: false
    range: [0, 120]  # [min, max] bounds
    
  # String features (exact match)
  - name: user_segment
    dtype: string
    nullable: true
    
  # DateTime features (exact match)
  - name: last_login
    dtype: datetime
    nullable: true
```

### Supported Data Types
| Type | Comparison | Tolerance | Notes |
|------|------------|-----------|-------|
| `int` | Numeric | ✅ abs/rel | Coerced to float for comparison |
| `float` | Numeric | ✅ abs/rel | NaN handling per null_policy |
| `bool` | Exact | ❌ | True/False only |
| `string` | Exact | ❌ | Case sensitive |
| `category` | Exact + Unknown detection | ❌ | Validates against expected categories |
| `datetime` | Exact | ❌ | Timezone aware |

### Tolerance Configuration

**Absolute Tolerance**: `|offline_value - online_value| ≤ abs_tolerance`

**Relative Tolerance**: `|offline_value - online_value| ≤ rel_tolerance × max(|offline_value|, |online_value|, ε)`

Either or both can be specified. If both are provided, the comparison passes if *either* tolerance is satisfied.

## Adapters

SkewSentry supports multiple adapter types to connect with different feature pipeline architectures:

### Python Function Adapter

For in-process Python functions:

```python
from skewsentry.adapters.python_func import PythonFunctionAdapter

# Your feature function signature
def extract_features(df: pd.DataFrame) -> pd.DataFrame:
    """Extract features from input DataFrame.
    
    Args:
        df: Input DataFrame with raw data
        
    Returns:
        DataFrame with feature columns + key columns
    """
    return df[["user_id", "timestamp", "spend_7d", "country"]]

# Reference by module:function string
adapter = PythonFunctionAdapter("mypackage.features:extract_features")
```

### HTTP Adapter

For REST API endpoints:

```python
from skewsentry.adapters.http_adapter import HTTPAdapter

adapter = HTTPAdapter(
    url="https://features.myservice.com/batch",
    method="POST",
    headers={"Authorization": "Bearer token"},
    batch_size=1000,  # Records per request
    timeout=30.0,
    max_retries=3
)
```

**Expected API Contract**:
- **Request**: JSON array of input records
- **Response**: JSON array of feature records (same order)
- **Status**: 200 for success, 4xx/5xx for errors

### Feast Adapter (Optional)

For Feast feature stores:

```python
from skewsentry.adapters.feast_adapter import FeastAdapter

adapter = FeastAdapter(
    registry_path="feature_repo/feature_store.yaml",
    project="ml_pipeline",
    features=["user_stats:spend_7d", "user_profile:country"]
)
```

Requires `feast` package: `pip install feast`

## Reporting

SkewSentry generates multiple report formats for different use cases:

### Text Report
```python
# Console-friendly summary
print(report.to_text(max_rows=10))
```
```
OK: False
Missing rows — offline: 0, online: 3
Per-feature mismatch rates:
  - spend_7d: mismatch_rate=0.1200 rows=1000 mean_abs_diff=0.0845
  - country: mismatch_rate=0.0000 rows=1000 mean_abs_diff=None
```

### JSON Report
```python
# Machine-readable results
report.to_json("results.json")
```
```json
{
  "ok": false,
  "keys": ["user_id", "timestamp"],
  "missing_in_online": 3,
  "missing_in_offline": 0,
  "features": [
    {
      "name": "spend_7d",
      "mismatch_rate": 0.12,
      "num_rows": 1000,
      "mean_abs_diff": 0.0845,
      "unknown_categories": null
    }
  ],
  "failing_features": ["spend_7d"]
}
```

### HTML Report
```python
# Rich visual report for stakeholders
report.to_html("report.html")
```

Interactive HTML report includes:
- Executive summary with pass/fail status
- Per-feature mismatch statistics
- Sample mismatched rows with differences highlighted
- Missing row analysis
- Feature distribution comparisons

## CI Integration

### GitHub Actions

```yaml
name: Feature Parity Check
on: [push, pull_request]

jobs:
  parity-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install -e ".[dev]"
          
      - name: Run feature parity check
        run: |
          skewsentry check \
            --spec features.yml \
            --offline training.pipeline:extract_features \
            --online serving.api:get_features \
            --data tests/fixtures/validation.parquet \
            --html artifacts/parity-report.html \
            --json artifacts/parity-results.json
            
      - name: Upload report artifacts
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: parity-reports
          path: artifacts/
```

### Exit Codes
- **0**: All features match within specified tolerances ✅
- **1**: Feature parity violations detected ❌
- **2**: Configuration error or runtime failure 🚨

### Integration Patterns

**Pre-deployment Gate**:
```bash
# Block deployment if parity check fails
skewsentry check --spec features.yml --offline offline:fn --online online:fn --data validation.parquet
if [ $? -eq 1 ]; then
  echo "❌ Feature parity violations detected. Blocking deployment."
  exit 1
fi
```

**Model Registry Integration**:
```python
# Validate features before model registration
report = run_check(spec, data, offline_adapter, online_adapter)
if report.ok:
    model_registry.register_model(model, features=spec.features)
else:
    raise ValueError(f"Feature parity check failed: {report.failing_features}")
```

## Examples

### Real-World Bug Caught by SkewSentry

This is the exact type of production bug SkewSentry prevents:

```python
# Training pipeline (offline) - Spark/Python
def extract_features(df):
    # Rolling 7-day sum with pandas semantics
    spend_7d = df.groupby("user_id")["amount"] \
                 .rolling(7, min_periods=1) \
                 .sum() \
                 .round(2)
    return df.assign(spend_7d=spend_7d)

# Serving pipeline (online) - Java/Kafka Streams  
# Translated to Python equivalent for illustration
def get_features(df):
    # Rolling 7-day sum with different window semantics
    spend_7d = df.groupby("user_id")["amount"] \
                 .rolling(7, closed="left") \
                 .sum() \
                 .apply(lambda x: math.floor(x * 100) / 100)
    return df.assign(spend_7d=spend_7d)
```

**The Differences**:
1. **Window boundaries**: `min_periods=1` vs `closed="left"`
2. **Rounding logic**: `round(2)` vs `floor() * 100 / 100`

**The Impact**: 12% of feature values differed by 0.01-0.15, causing model accuracy to drop from 94% to 89% in production.

**The Solution**: SkewSentry with `tolerance: {abs: 0.01}` caught this in CI:
```bash
❌ Feature parity violations detected:
  - spend_7d: mismatch_rate=0.1200 rows=5000 mean_abs_diff=0.0845
```

### Complete Example

See [`examples/simple/`](examples/simple/) for a runnable demonstration showing how SkewSentry catches windowing and rounding differences between offline and online pipelines.

## Development

### Setup
```bash
git clone https://github.com/your-org/skewsentry.git
cd skewsentry
uv venv .venv && source .venv/bin/activate
uv pip install -e ".[dev]"
```

### Testing
```bash
# Run all tests
uv run pytest

# With coverage (enforces 85%+)
uv run pytest --cov=skewsentry --cov-fail-under=85

# Run specific test categories
uv run pytest -k test_spec              # Specification tests
uv run pytest -k test_adapter           # Adapter tests  
uv run pytest -m "e2e"                  # End-to-end integration tests
```

### Project Architecture

```
skewsentry/
├── __init__.py                    # Package exports
├── spec.py                        # FeatureSpec Pydantic models
├── inputs.py                      # Data loading and sampling
├── adapters/                      # Pipeline adapters
│   ├── __init__.py
│   ├── base.py                    # FeatureAdapter protocol
│   ├── python_func.py             # Python function adapter
│   ├── http_adapter.py            # HTTP/REST API adapter
│   └── feast_adapter.py           # Feast feature store adapter
├── align.py                       # Row alignment by keys
├── compare.py                     # Feature comparison logic
├── runner.py                      # Pipeline orchestration
├── report.py                      # Report generation
├── cli.py                         # Command-line interface
├── errors.py                      # Exception classes
└── utils.py                       # Logging utilities
```

### Contributing

1. **Issues**: Report bugs or request features via GitHub Issues
2. **Pull Requests**: Fork, create feature branch, add tests, submit PR
3. **Testing**: All changes must include tests and maintain 85%+ coverage
4. **Documentation**: Update README and docstrings for new features

## Roadmap

### v0.2.0 - Enhanced Analysis
- [ ] Statistical significance testing (KS-test, chi-square)
- [ ] Feature drift detection over time
- [ ] SQL adapter for database sources
- [ ] Streaming data support

### v0.3.0 - Scale & Performance  
- [ ] Spark/Dask backends for large datasets
- [ ] Distributed comparison for high-volume pipelines
- [ ] Advanced sampling strategies
- [ ] Performance benchmarking suite

### v1.0.0 - Production Features
- [ ] Web dashboard for monitoring
- [ ] Alert integrations (Slack, PagerDuty)
- [ ] Model performance correlation analysis
- [ ] Enterprise security features

---

**License**: MIT | **Python**: 3.9+ | **Maintained by**: SkewSentry Team

*Prevent ML model failures before they reach production. Start validating your feature pipelines today.*

