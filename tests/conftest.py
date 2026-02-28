import os
import sys
import pytest
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

@pytest.fixture(scope="session")
def test_config():
    """Global test configuration."""
    return {
        "test_private_key": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        "test_node_address": "connect.akave.ai:5500",
        "test_bucket_name": "pytest-test-bucket",
        "test_timeout": 30,
        "max_retries": 3
    }

@pytest.fixture(scope="session")
def project_root_path():
    return project_root

@pytest.fixture
def sample_data():
    return {
        "small_data": b"Hello, Akave SDK!",
        "medium_data": b"x" * 1024,  # 1KB
        "large_data": b"y" * 1024 * 1024,  # 1MB
        "binary_data": bytes(range(256)),
        "text_data": "The quick brown fox jumps over the lazy dog".encode(),
        "unicode_data": "Hello 世界! 🌍".encode("utf-8")
    }

@pytest.fixture
def temp_file(tmp_path, sample_data):
    file_path = tmp_path / "test_file.bin"
    file_path.write_bytes(sample_data["medium_data"])
    return file_path

@pytest.fixture(autouse=True)
def setup_test_environment():
    os.environ["PYTEST_RUNNING"] = "1"
    yield
    if "PYTEST_RUNNING" in os.environ:
        del os.environ["PYTEST_RUNNING"]

pytest_plugins = []
