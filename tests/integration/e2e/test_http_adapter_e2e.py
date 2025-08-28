from __future__ import annotations

import subprocess
import time
from pathlib import Path

import pandas as pd
import pytest
import requests

from skewsentry.adapters.python import PythonFunctionAdapter
from skewsentry.adapters.http import HTTPAdapter
from skewsentry.runner import run_check
from skewsentry.spec import FeatureSpec


@pytest.fixture
def node_service_port():
    """Find an available port for the Node.js service."""
    import socket
    sock = socket.socket()
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@pytest.fixture
def ecommerce_data(tmp_path: Path):
    """Create sample e-commerce transaction data."""
    df = pd.DataFrame({
        "user_id": [1, 1, 1, 2, 2, 3],
        "timestamp": pd.to_datetime([
            "2024-01-01T10:00:00",
            "2024-01-02T11:00:00", 
            "2024-01-03T12:00:00",
            "2024-01-01T13:00:00",
            "2024-01-04T14:00:00",
            "2024-01-02T15:00:00",
        ]),
        "transaction_id": ["t1", "t2", "t3", "t4", "t5", "t6"],
        "price": [100.0, 150.0, 200.0, 75.0, 125.0, 50.0],
        "quantity": [1, 2, 1, 1, -1, 3],  # Include a return (negative quantity)
        "category": ["electronics", "clothing", "electronics", "books", "electronics", "home"],
        "is_return": [False, False, False, False, True, False],
        "is_weekend": [False, False, True, False, False, False],
        "country": ["US", "US", "US", "UK", "UK", None],  # Include null
        "user_type": ["regular", "regular", "regular", "casual", "casual", "power"],
        "payment_method": ["credit_card", "credit_card", "paypal", "debit_card", None, "apple_pay"],  # Include null
    })
    
    data_path = tmp_path / "ecommerce_data.parquet"
    df.to_parquet(data_path, index=False)
    return data_path


@pytest.fixture 
def ecommerce_files(tmp_path: Path):
    """Copy ecommerce example files to temp directory."""
    ecommerce_dir = Path("examples/http")
    
    # Copy Python offline features with unique name to avoid import cache conflicts
    offline_src = ecommerce_dir / "offline_features.py"
    offline_dst = tmp_path / "http_offline_features.py"
    offline_dst.write_text(offline_src.read_text(), encoding="utf-8")
    
    # Copy JavaScript online service
    js_src = ecommerce_dir / "online_features.js" 
    js_dst = tmp_path / "online_features.js"
    js_dst.write_text(js_src.read_text(), encoding="utf-8")
    
    # Copy package.json
    pkg_src = ecommerce_dir / "package.json"
    pkg_dst = tmp_path / "package.json"
    pkg_dst.write_text(pkg_src.read_text(), encoding="utf-8")
    
    # Copy feature spec (without boolean features)
    spec_src = ecommerce_dir / "features.yml"
    spec_dst = tmp_path / "features.yml"
    spec_dst.write_text(spec_src.read_text(), encoding="utf-8")
    
    return tmp_path


@pytest.fixture
def node_service(ecommerce_files: Path, node_service_port: int):
    """Start Node.js service for testing."""
    service_dir = ecommerce_files
    
    # Install npm dependencies
    subprocess.run(["npm", "install"], cwd=service_dir, check=True, capture_output=True)
    
    # Start Node.js service
    env = {"PORT": str(node_service_port)}
    process = subprocess.Popen(
        ["node", "online_features.js"],
        cwd=service_dir,
        env={**subprocess.os.environ, **env},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Wait for service to start
    service_url = f"http://localhost:{node_service_port}"
    max_wait = 10
    for _ in range(max_wait):
        try:
            response = requests.get(f"{service_url}/health", timeout=1)
            if response.status_code == 200:
                break
        except requests.RequestException:
            pass
        time.sleep(0.5)
    else:
        process.terminate()
        process.wait()
        raise RuntimeError(f"Node.js service failed to start on port {node_service_port}")
    
    yield service_url
    
    # Clean up
    process.terminate()
    process.wait()


def test_ecommerce_http_adapter_end_to_end(ecommerce_files: Path, ecommerce_data: Path, node_service: str):
    """Test complete e-commerce example with Python offline + JavaScript HTTP online."""
    import sys
    sys.path.insert(0, str(ecommerce_files))
    
    # Load feature spec
    spec_path = ecommerce_files / "features.yml"
    spec = FeatureSpec.from_yaml(str(spec_path))
    
    # Set up adapters
    offline_adapter = PythonFunctionAdapter("http_offline_features:extract_features")
    online_adapter = HTTPAdapter(url=f"{node_service}/features", timeout=10.0)
    
    # Run the comparison
    report = run_check(
        spec=spec,
        data=str(ecommerce_data),
        offline=offline_adapter,
        online=online_adapter,
        sample=None,  # Use all data (only 6 transactions)
        seed=42
    )
    
    # Verify report structure
    assert isinstance(report.ok, bool)
    assert len(report.per_feature) > 0
    assert report.alignment.missing_in_offline_count >= 0
    assert report.alignment.missing_in_online_count >= 0
    
    # Should have some feature differences due to intentional implementation differences
    # (This test verifies the system works, not that features match perfectly)
    feature_names = [f.feature_name for f in report.per_feature]
    expected_features = [
        "spend_7d", "spend_30d", "txn_count_7d", 
        "electronics_affinity", "avg_days_between_txns", "return_rate",
        "weekend_frequency", "days_since_last_txn", 
        "country", "user_type", "primary_payment_method"
    ]
    
    # Check that all expected features are tested
    for expected in expected_features:
        assert expected in feature_names, f"Feature '{expected}' not found in report"
    
    # Verify that HTTP adapter successfully processed requests
    assert report.alignment.missing_in_offline_count == 0, "Should have no missing offline rows"
    assert report.alignment.missing_in_online_count == 0, "Should have no missing online rows"


def test_node_service_health_check(node_service: str):
    """Test that the Node.js service health endpoint works."""
    response = requests.get(f"{node_service}/health")
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "ecommerce-online-features"


def test_node_service_features_endpoint(node_service: str):
    """Test that the Node.js features endpoint processes data correctly."""
    sample_data = [
        {
            "user_id": 1,
            "timestamp": "2024-01-01T10:00:00",
            "transaction_id": "test1",
            "price": 100.0,
            "quantity": 1,
            "category": "electronics",
            "is_return": False,
            "is_weekend": False,
            "country": "US",
            "user_type": "regular",
            "payment_method": "credit_card"
        }
    ]
    
    response = requests.post(
        f"{node_service}/features",
        json=sample_data,
        headers={"Content-Type": "application/json"}
    )
    
    assert response.status_code == 200
    features = response.json()
    
    assert len(features) == 1
    feature = features[0]
    
    # Check that required feature fields are present
    assert "user_id" in feature
    assert "timestamp" in feature
    assert "spend_7d" in feature
    assert "electronics_affinity" in feature
    assert "country" in feature
    
    # Verify basic data types
    assert isinstance(feature["user_id"], int)
    assert isinstance(feature["spend_7d"], (int, float))
    assert feature["country"] in ["US", "OTHER"]