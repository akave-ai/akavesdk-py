import hashlib
import secrets
import time
from typing import Any, Dict, List
from unittest.mock import Mock, MagicMock

def calculate_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def generate_test_data(size: int) -> bytes:
    return secrets.token_bytes(size)

def create_mock_response(data: Dict[str, Any]) -> Mock:
    mock_response = Mock()
    for key, value in data.items():
        setattr(mock_response, key, value)
    return mock_response

def wait_for_condition(condition_func, timeout: int = 10, interval: float = 0.1) -> bool:
    start_time = time.time()
    while time.time() - start_time < timeout:
        if condition_func():
            return True
        time.sleep(interval)
    return False

def assert_eventually(condition_func, timeout: int = 5, message: str = "Condition not met"):
    if not wait_for_condition(condition_func, timeout):
        raise AssertionError(f"{message} within {timeout} seconds")

def create_mock_cid(cid_string: str = None) -> Mock:
    if cid_string is None:
        cid_string = "bafybeigweriqysuigpnsu3jmndgonrihee4dmx27rctlsd5mfn5arrnxyi"
    
    mock_cid = Mock()
    mock_cid.__str__ = Mock(return_value=cid_string)
    mock_cid.string = Mock(return_value=cid_string)
    mock_cid.toString = Mock(return_value=cid_string)
    mock_cid.bytes = Mock(return_value=b"mock_cid_bytes")
    return mock_cid

def create_mock_dag_node(cid: str = None, data: bytes = None) -> Mock:
    mock_node = Mock()
    mock_node.cid = create_mock_cid(cid) if cid else create_mock_cid()
    mock_node.data = data or b"mock_data"
    mock_node.raw_data_size = len(mock_node.data)
    mock_node.encoded_size = len(mock_node.data) + 100  # Add some overhead
    return mock_node

def create_mock_file_block(cid: str = None, data: bytes = None, node_address: str = None) -> Mock:
    mock_block = Mock()
    mock_block.cid = cid or "bafybeia3uzxlpmpopl6tsafgwfgygzibbgqhpnvvizg2acnjdtq"
    mock_block.data = data or b"mock_block_data"
    mock_block.node_address = node_address or "mock.node.address:8080"
    mock_block.node_id = "12D3KooWQP1LYXKebCq2bPBYFUGnEb8jRe83LFYNJFAXo5Z3dSoz"
    mock_block.permit = "mock_permit"
    return mock_block

def assert_mock_called_with_partial(mock_obj: Mock, expected_args: Dict[str, Any]):
    assert mock_obj.called, "Mock was not called"
    
    last_call = mock_obj.call_args
    if last_call is None:
        raise AssertionError("Mock was not called")
    
    args, kwargs = last_call
    
    for key, expected_value in expected_args.items():
        if key in kwargs:
            assert kwargs[key] == expected_value, f"Expected {key}={expected_value}, got {kwargs[key]}"
        else:
            raise AssertionError(f"Expected argument '{key}' not found in call")

class MockContextManager:
    
    def __init__(self, return_value=None):
        self.return_value = return_value
        self.entered = False
        self.exited = False
    
    def __enter__(self):
        self.entered = True
        return self.return_value
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.exited = True
        return False
