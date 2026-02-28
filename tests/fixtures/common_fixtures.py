import pytest
from unittest.mock import Mock, MagicMock
import io
import secrets
from pathlib import Path

@pytest.fixture
def mock_sdk_config():
    from sdk.config import SDKConfig
    return SDKConfig(
        address="mock.akave.ai:5500",
        private_key="a5c223e956644f1ba11f0dcc6f3df4992184ff3c919223744d0cf1db33dab4d6",
        max_concurrency=5,
        block_part_size=128*1024,
        use_connection_pool=True,
        chunk_buffer=10
    )

@pytest.fixture
def mock_grpc_channel():
    mock_channel = Mock()
    mock_channel.close = Mock()
    return mock_channel

@pytest.fixture
def mock_ipc_client():
    mock_client = Mock()
    mock_client.auth = Mock()
    mock_client.auth.address = "0x926c6731DE79ab807e3e7A099f8091e7dD7372D7"
    mock_client.auth.key = "a5c223e956644f1ba11f0dcc6f3df4992184ff3c919223744d0cf1db33dab4d6"
    
    mock_client.storage = Mock()
    mock_client.storage.contract_address = "0x0154953F6E583f09D9B38F7C4e7C6265906eC207"
    mock_client.storage.get_chain_id = Mock(return_value=21207)
    
    return mock_client

@pytest.fixture
def sample_cid():
    return "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"

@pytest.fixture
def sample_file_data():
    return {
        "small": b"Hello Akave!",
        "medium": b"x" * 1024,  # 1KB
        "large": b"y" * (1024 * 1024),  # 1MB
        "binary": bytes(range(256))
    }

@pytest.fixture
def temp_test_file(tmp_path, sample_file_data):
    file_path = tmp_path / "test_upload.bin"
    file_path.write_bytes(sample_file_data["medium"])
    return file_path

@pytest.fixture
def mock_file_reader(sample_file_data):
    return io.BytesIO(sample_file_data["medium"])

@pytest.fixture
def mock_bucket_info():
    return {
        "id": "984a6110b87ca4df9b8b7efa9fcf665fa2a674ad17f3b5b28a1b94848b683e4",
        "name": "test-bucket",
        "created_at": 1234567890,
        "owner": "0x926c6731DE79ab807e3e7A099f8091e7dD7372D7"
    }

@pytest.fixture
def mock_file_meta():
    return {
        "root_cid": "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi",
        "name": "test-file.bin",
        "bucket_name": "test-bucket",
        "size": 1024,
        "encoded_size": 1200,
        "created_at": 1234567890
    }

@pytest.fixture
def generate_test_bucket_name():
    def _generate():
        return f"pytest-bucket-{secrets.token_hex(6)}"
    return _generate

@pytest.fixture
def generate_test_file_name():
    def _generate():
        return f"pytest-file-{secrets.token_hex(4)}.bin"
    return _generate
