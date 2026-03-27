import pytest
from unittest.mock import Mock, MagicMock, patch, call
import grpc
import io
import time
from datetime import datetime

from sdk.sdk import (
    SDK, 
    AkaveContractFetcher,
    BucketCreateResult, 
    Bucket, 
    MonkitStats,
    WithRetry,
    SDKOption,
    WithMetadataEncryption,
    WithEncryptionKey,
    WithPrivateKey,
    WithStreamingMaxBlocksInChunk,
    WithErasureCoding,
    WithChunkBuffer,
    WithoutRetry,
    get_monkit_stats,
    extract_block_data,
    encryption_key_derivation,
    is_retryable_tx_error,
    skip_to_position,
    parse_timestamp,
    ENCRYPTION_OVERHEAD,
    MIN_FILE_SIZE
)
from sdk.config import SDKConfig, SDKError, BLOCK_SIZE
from tests.fixtures.common_fixtures import mock_sdk_config


class TestAkaveContractFetcher:
    
    def test_init(self):
        """Test AkaveContractFetcher initialization."""
        fetcher = AkaveContractFetcher("test.node.ai:5500")
        assert fetcher.node_address == "test.node.ai:5500"
        assert fetcher.channel is None
        assert fetcher.stub is None
    
    @patch('grpc.insecure_channel')
    @patch('sdk.sdk.ipcnodeapi_pb2_grpc.IPCNodeAPIStub')
    def test_connect_success(self, mock_stub_class, mock_channel):
        mock_channel_instance = Mock()
        mock_stub_instance = Mock()
        mock_channel.return_value = mock_channel_instance
        mock_stub_class.return_value = mock_stub_instance
        
        fetcher = AkaveContractFetcher("test.node.ai:5500")
        result = fetcher.connect()
        
        assert result is True
        assert fetcher.channel == mock_channel_instance
        assert fetcher.stub == mock_stub_instance
        mock_channel.assert_called_once_with("test.node.ai:5500")
    
    @patch('grpc.insecure_channel')
    def test_connect_grpc_error(self, mock_channel):
        # Create a proper mock RpcError exception
        mock_error = Exception("Connection failed")
        mock_channel.side_effect = mock_error
        
        fetcher = AkaveContractFetcher("test.node.ai:5500")
        result = fetcher.connect()
        
        assert result is False
        assert fetcher.channel is None
        assert fetcher.stub is None
    
    def test_fetch_contract_addresses_no_stub(self):
        fetcher = AkaveContractFetcher("test.node.ai:5500")
        result = fetcher.fetch_contract_addresses()
        assert result is None
    
    @patch('sdk.sdk.ipcnodeapi_pb2.ConnectionParamsRequest')
    def test_fetch_contract_addresses_success(self, mock_request):
        fetcher = AkaveContractFetcher("test.node.ai:5500")
        
        # Mock stub and response
        mock_stub = Mock()
        mock_response = Mock()
        mock_response.dial_uri = "https://dial.uri"
        mock_response.storage_address = "0x123..."
        mock_response.access_address = "0x456..."
        
        mock_stub.ConnectionParams.return_value = mock_response
        fetcher.stub = mock_stub
        
        result = fetcher.fetch_contract_addresses()
        
        expected = {
            'dial_uri': "https://dial.uri",
            'contract_address': "0x123...",
            'access_address': "0x456..."
        }
        assert result == expected
    
    def test_close(self):
        fetcher = AkaveContractFetcher("test.node.ai:5500")
        mock_channel = Mock()
        fetcher.channel = mock_channel
        
        fetcher.close()
        mock_channel.close.assert_called_once()
    
    def test_close_no_channel(self):
        fetcher = AkaveContractFetcher("test.node.ai:5500")
        fetcher.close()
    
    @patch('sdk.sdk.ipcnodeapi_pb2.ConnectionParamsRequest')
    def test_fetch_contract_addresses_exception(self, mock_request):
        fetcher = AkaveContractFetcher("test.node.ai:5500")
        
        mock_stub = Mock()
        mock_stub.ConnectionParams.side_effect = Exception("Network error")
        fetcher.stub = mock_stub
        
        result = fetcher.fetch_contract_addresses()
        assert result is None
    
    @patch('sdk.sdk.ipcnodeapi_pb2.ConnectionParamsRequest')
    def test_fetch_contract_addresses_no_access_address(self, mock_request):
        fetcher = AkaveContractFetcher("test.node.ai:5500")
        
        mock_stub = Mock()
        mock_response = Mock(spec=['dial_uri', 'storage_address'])
        mock_response.dial_uri = "https://dial.uri"
        mock_response.storage_address = "0x123..."
        
        mock_stub.ConnectionParams.return_value = mock_response
        fetcher.stub = mock_stub
        
        result = fetcher.fetch_contract_addresses()
        
        assert result['dial_uri'] == "https://dial.uri"
        assert result['contract_address'] == "0x123..."
        assert 'access_address' not in result


class TestWithRetry:
    
    def test_init_defaults(self):
        retry = WithRetry()
        assert retry.max_attempts == 5
        assert retry.base_delay == 0.1
    
    def test_init_custom(self):
        retry = WithRetry(max_attempts=3, base_delay=0.5)
        assert retry.max_attempts == 3
        assert retry.base_delay == 0.5
    
    def test_do_success_first_try(self):
        retry = WithRetry()
        
        def success_func():
            return False, None  # No retry needed, no error
        
        result = retry.do(None, success_func)
        assert result is None
    
    def test_do_non_retryable_error(self):
        retry = WithRetry()
        error = Exception("Non-retryable error")
        
        def non_retryable_func():
            return False, error  # No retry needed, but error
        
        result = retry.do(None, non_retryable_func)
        assert result == error
    
    @patch('time.sleep')
    def test_do_retryable_success(self, mock_sleep):
        retry = WithRetry(max_attempts=3, base_delay=0.1)
        call_count = 0
        
        def retryable_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return True, Exception("Retryable error")  # Retry needed
            return False, None  # Success on 3rd try
        
        result = retry.do(None, retryable_func)
        assert result is None
        assert call_count == 3
        assert mock_sleep.call_count == 2  # 2 retries
    
    @patch('time.sleep')
    def test_do_max_retries_exceeded(self, mock_sleep):
        retry = WithRetry(max_attempts=2, base_delay=0.1)
        original_error = Exception("Persistent error")
        
        def failing_func():
            return True, original_error  # Always retryable error
        
        result = retry.do(None, failing_func)
        assert "max retries exceeded" in str(result)
        assert mock_sleep.call_count == 2  # 2 retries
    
    def test_do_zero_max_attempts(self):
        retry = WithRetry(max_attempts=0)
        error = Exception("Error with no retries")
        
        def retryable_func():
            return True, error
        
        result = retry.do(None, retryable_func)
        assert result == error


class TestDataClasses:
    
    def test_bucket_create_result(self):
        now = datetime.now()
        result = BucketCreateResult(name="test-bucket", created_at=now)
        assert result.name == "test-bucket"
        assert result.created_at == now
    
    def test_bucket(self):
        now = datetime.now()
        bucket = Bucket(name="my-bucket", created_at=now)
        assert bucket.name == "my-bucket"
        assert bucket.created_at == now
    
    def test_monkit_stats_defaults(self):
        stats = MonkitStats(
            name="test.stat",
            successes=10,
            errors={"timeout": 2},
            highwater=5
        )
        assert stats.name == "test.stat"
        assert stats.successes == 10
        assert stats.errors == {"timeout": 2}
        assert stats.highwater == 5
        assert stats.success_times is None
        assert stats.failure_times is None
    
    def test_monkit_stats_with_times(self):
        stats = MonkitStats(
            name="test.stat",
            successes=5,
            errors={},
            highwater=3,
            success_times=[0.1, 0.2, 0.3],
            failure_times=[1.0, 2.0]
        )
        assert stats.success_times == [0.1, 0.2, 0.3]
        assert stats.failure_times == [1.0, 2.0]


class TestModuleConstants:
    
    def test_encryption_overhead(self):
        assert ENCRYPTION_OVERHEAD == 28
    
    def test_min_file_size(self):
        assert MIN_FILE_SIZE == 127


class TestSDKOptions:
    
    def test_with_metadata_encryption(self):
        option = WithMetadataEncryption()
        mock_sdk = Mock()
        
        option.apply(mock_sdk)
        assert mock_sdk.use_metadata_encryption is True
    
    def test_with_encryption_key(self):
        key = b"test_encryption_key_32_bytes123"
        option = WithEncryptionKey(key)
        mock_sdk = Mock()
        
        option.apply(mock_sdk)
        assert mock_sdk.encryption_key == key
    
    def test_with_private_key(self):
        key = "0x123456789..."
        option = WithPrivateKey(key)
        mock_sdk = Mock()
        
        option.apply(mock_sdk)
        assert mock_sdk.private_key == key
    
    def test_with_streaming_max_blocks_in_chunk(self):
        max_blocks = 10
        option = WithStreamingMaxBlocksInChunk(max_blocks)
        mock_sdk = Mock()
        
        option.apply(mock_sdk)
        assert mock_sdk.streaming_max_blocks_in_chunk == max_blocks
    
    def test_with_erasure_coding(self):
        parity_blocks = 3
        option = WithErasureCoding(parity_blocks)
        mock_sdk = Mock()
        
        option.apply(mock_sdk)
        assert mock_sdk.parity_blocks_count == parity_blocks
    
    def test_with_chunk_buffer(self):
        buffer_size = 20
        option = WithChunkBuffer(buffer_size)
        mock_sdk = Mock()
        
        option.apply(mock_sdk)
        assert mock_sdk.chunk_buffer == buffer_size
    
    def test_without_retry(self):
        option = WithoutRetry()
        mock_sdk = Mock()
        
        option.apply(mock_sdk)
        assert isinstance(mock_sdk.with_retry, WithRetry)
        assert mock_sdk.with_retry.max_attempts == 0
    
    def test_base_sdk_option(self):
        option = SDKOption()
        mock_sdk = Mock()
        option.apply(mock_sdk)
    
    def test_with_encryption_key_stores_key(self):
        key = b"a" * 32
        option = WithEncryptionKey(key)
        assert option.key == key
    
    def test_with_private_key_stores_key(self):
        key = "0xprivate"
        option = WithPrivateKey(key)
        assert option.key == key
    
    def test_with_streaming_max_blocks_stores_value(self):
        option = WithStreamingMaxBlocksInChunk(16)
        assert option.max_blocks_in_chunk == 16
    
    def test_with_erasure_coding_stores_value(self):
        option = WithErasureCoding(4)
        assert option.parity_blocks == 4
    
    def test_with_chunk_buffer_stores_value(self):
        option = WithChunkBuffer(50)
        assert option.buffer_size == 50


@patch('grpc.insecure_channel')
@patch('sdk.sdk.nodeapi_pb2_grpc.NodeAPIStub')
@patch('sdk.sdk.ipcnodeapi_pb2_grpc.IPCNodeAPIStub')
class TestSDK:
    
    def test_init_valid_config(self, mock_ipc_stub, mock_node_stub, mock_channel):
        """Test SDK initialization with valid config."""
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=1024
        )
        
        mock_channel_instance = Mock()
        mock_channel.return_value = mock_channel_instance
        
        sdk = SDK(config)
        
        assert sdk.config == config
        assert sdk.conn == mock_channel_instance
        assert sdk.ipc_conn == mock_channel_instance  # Same address
        mock_channel.assert_called_once_with("test.node.ai:5500")
    
    def test_init_invalid_block_part_size(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=0  # Invalid
        )
        
        with pytest.raises(SDKError, match="Invalid blockPartSize"):
            SDK(config)
    
    def test_init_invalid_encryption_key_length(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024,  # Valid block part size
            encryption_key=b"short_key"  # Not 32 bytes
        )
        
        mock_channel.return_value = Mock()
        
        with pytest.raises(SDKError, match="Encryption key length should be 32 bytes long"):
            SDK(config)
    
    def test_init_with_different_ipc_address(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            ipc_address="ipc.node.ai:5501",
            private_key="test_key",
            block_part_size=512 * 1024  # Valid block part size
        )
        
        mock_channel_instance1 = Mock()
        mock_channel_instance2 = Mock()
        mock_channel.side_effect = [mock_channel_instance1, mock_channel_instance2]
        
        sdk = SDK(config)
        
        assert sdk.conn == mock_channel_instance1
        assert sdk.ipc_conn == mock_channel_instance2
        assert mock_channel.call_count == 2
    
    def test_close(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024  # Valid block part size
        )
        mock_conn = Mock()
        mock_ipc_conn = Mock()
        mock_channel.side_effect = [mock_conn, mock_ipc_conn]
        
        config.ipc_address = "different.ipc.ai:5501"  # Different IPC address
        sdk = SDK(config)
        
        sdk.close()
        
        mock_conn.close.assert_called_once()
        mock_ipc_conn.close.assert_called_once()
    
    def test_validate_bucket_name_valid(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024  # Valid block part size
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        # Should not raise an exception
        sdk._validate_bucket_name("valid-bucket-name", "TestMethod")
    
    def test_validate_bucket_name_invalid(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024  # Valid block part size
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        with pytest.raises(SDKError, match="Invalid bucket name"):
            sdk._validate_bucket_name("", "TestMethod")
        
        with pytest.raises(SDKError, match="Invalid bucket name"):
            sdk._validate_bucket_name("ab", "TestMethod")
    
    @patch('sdk.sdk.nodeapi_pb2.BucketCreateRequest')
    def test_create_bucket_success(self, mock_request, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024  # Valid block part size
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        # Mock response
        mock_response = Mock()
        mock_response.name = "test-bucket"
        mock_response.created_at = Mock()
        mock_response.created_at.AsTime.return_value = datetime.now()
        
        sdk.client.BucketCreate.return_value = mock_response
        
        result = sdk.create_bucket("test-bucket")
        
        assert isinstance(result, BucketCreateResult)
        assert result.name == "test-bucket"
        sdk.client.BucketCreate.assert_called_once()
    
    def test_create_bucket_invalid_name(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024  # Valid block part size
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        with pytest.raises(SDKError, match="Invalid bucket name"):
            sdk.create_bucket("ab")  # Too short
    
    @patch('sdk.sdk.nodeapi_pb2.BucketViewRequest')
    def test_view_bucket_success(self, mock_request, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024  # Valid block part size
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        # Mock response
        mock_response = Mock()
        mock_response.name = "test-bucket"
        mock_response.created_at = Mock()
        mock_response.created_at.AsTime.return_value = datetime.now()
        
        sdk.client.BucketView.return_value = mock_response
        
        result = sdk.view_bucket("test-bucket")
        
        assert isinstance(result, Bucket)
        assert result.name == "test-bucket"
        sdk.client.BucketView.assert_called_once()
    
    @patch('sdk.sdk.nodeapi_pb2.BucketDeleteRequest')
    def test_delete_bucket_success(self, mock_request, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024  # Valid block part size
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        sdk.client.BucketDelete.return_value = Mock()
        
        result = sdk.delete_bucket("test-bucket")
        
        assert result is True
        sdk.client.BucketDelete.assert_called_once()
    
    @patch('sdk.sdk.StreamingAPI')
    def test_streaming_api(self, mock_streaming_api, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024  # Valid block part size
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        mock_streaming_instance = Mock()
        mock_streaming_api.return_value = mock_streaming_instance
        
        result = sdk.streaming_api()
        
        assert result == mock_streaming_instance
        mock_streaming_api.assert_called_once()
    
    @patch('sdk.sdk.IPC')
    @patch('sdk.sdk.Client.dial')
    def test_ipc_success(self, mock_dial, mock_ipc_class, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024  # Valid block part size
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        sdk._contract_info = {
            'dial_uri': 'https://test.dial.uri',
            'contract_address': '0x123...',
            'access_address': '0x456...'
        }
        
        mock_ipc_instance = Mock()
        mock_dial.return_value = mock_ipc_instance
        
        mock_ipc_result = Mock()
        mock_ipc_class.return_value = mock_ipc_result
        
        result = sdk.ipc()
        
        assert result == mock_ipc_result
        mock_dial.assert_called_once()
        mock_ipc_class.assert_called_once()
    
    def test_ipc_no_private_key(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            block_part_size=512 * 1024  # Valid block part size
        )  # No private key
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        with pytest.raises(SDKError, match="Private key is required"):
            sdk.ipc()
    
    def test_init_valid_encryption_key(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024,
            encryption_key=b"a" * 32
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        assert sdk.config.encryption_key == b"a" * 32
    
    def test_init_invalid_parity_blocks(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024,
            streaming_max_blocks_in_chunk=10,
            parity_blocks_count=6
        )
        mock_channel.return_value = Mock()
        
        with pytest.raises(SDKError, match="Parity blocks count"):
            SDK(config)
    
    @patch('sdk.sdk.ErasureCode')
    def test_init_with_erasure_code(self, mock_erasure_code, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024,
            streaming_max_blocks_in_chunk=10,
            parity_blocks_count=2
        )
        mock_channel.return_value = Mock()
        mock_erasure_instance = Mock()
        mock_erasure_code.return_value = mock_erasure_instance
        
        sdk = SDK(config)
        
        assert sdk.streaming_erasure_code == mock_erasure_instance
        mock_erasure_code.assert_called_once_with(8, 2)
    
    def test_init_block_part_size_at_max(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=BLOCK_SIZE
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        assert sdk.config.block_part_size == BLOCK_SIZE
    
    def test_init_block_part_size_exceeds_max(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=BLOCK_SIZE + 1
        )
        
        with pytest.raises(SDKError, match="Invalid blockPartSize"):
            SDK(config)
    
    def test_close_same_conn(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024
        )
        mock_conn = Mock()
        mock_channel.return_value = mock_conn
        
        sdk = SDK(config)
        sdk.close()
        
        mock_conn.close.assert_called_once()
    
    def test_view_bucket_invalid_name(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        with pytest.raises(SDKError, match="Invalid bucket name"):
            sdk.view_bucket("ab")
    
    def test_delete_bucket_invalid_name(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        with pytest.raises(SDKError, match="Invalid bucket name"):
            sdk.delete_bucket("")
    
    def test_do_grpc_call_error(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        sdk._grpc_base._handle_grpc_error = Mock(side_effect=SDKError("gRPC error"))
        
        mock_method = Mock(side_effect=grpc.RpcError())
        
        with pytest.raises(SDKError, match="gRPC error"):
            sdk._do_grpc_call("TestMethod", mock_method, Mock())
    
    @patch('sdk.sdk.Client.dial')
    def test_ipc_dial_failure(self, mock_dial, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        sdk._contract_info = {
            'dial_uri': 'https://test.uri',
            'contract_address': '0x123...'
        }
        
        mock_dial.side_effect = Exception("Connection refused")
        
        with pytest.raises(SDKError, match="Failed to dial IPC client"):
            sdk.ipc()
    
    def test_ipc_no_contract_info(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        sdk._fetch_contract_info = Mock(return_value=None)
        
        with pytest.raises(SDKError, match="Could not fetch contract information"):
            sdk.ipc()
    
    def test_validate_bucket_name_none(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        
        with pytest.raises(SDKError, match="Invalid bucket name"):
            sdk._validate_bucket_name(None, "TestMethod")
    
    def test_init_no_encryption_key(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024,
            encryption_key=None
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        assert sdk.config.encryption_key == []


class TestUtilityFunctions:
    
    def test_get_monkit_stats(self):
        stats = get_monkit_stats()
        assert isinstance(stats, list)
        assert len(stats) == 0
    
    @patch('multiformats.CID.decode')
    def test_extract_block_data_raw(self, mock_cid_decode):
        mock_cid = Mock()
        mock_cid.codec.name = "raw"
        mock_cid_decode.return_value = mock_cid
        
        data = b"test data"
        result = extract_block_data("test_cid", data)
        assert result == data
    
    @patch('multiformats.CID.decode')
    @patch('sdk.dag.extract_block_data')
    def test_extract_block_data_dag_pb(self, mock_dag_extract, mock_cid_decode):
        mock_cid = Mock()
        mock_cid.codec.name = "dag-pb"
        mock_cid_decode.return_value = mock_cid
        
        data = b"test data"
        expected_result = b"extracted data"
        mock_dag_extract.return_value = expected_result
        
        result = extract_block_data("test_cid", data)
        assert result == expected_result
        mock_dag_extract.assert_called_once_with("test_cid", data)
    
    @patch('multiformats.CID.decode')
    def test_extract_block_data_unknown_codec(self, mock_cid_decode):
        mock_cid = Mock()
        mock_cid.codec.name = "unknown"
        mock_cid_decode.return_value = mock_cid
        
        with pytest.raises(ValueError, match="unknown cid type"):
            extract_block_data("test_cid", b"test data")
    
    @patch('sdk.sdk.derive_key')
    def test_encryption_key_derivation_success(self, mock_derive_key):
        parent_key = b"parent_key_32_bytes_long_test123"
        info_data = ["bucket", "file"]
        expected_key = b"derived_key"
        
        mock_derive_key.return_value = expected_key
        
        result = encryption_key_derivation(parent_key, *info_data)
        
        assert result == expected_key
        mock_derive_key.assert_called_once_with(parent_key, "bucket/file".encode())
    
    def test_encryption_key_derivation_empty_key(self):
        result = encryption_key_derivation(b"", "bucket", "file")
        assert result == b""
    
    def test_is_retryable_tx_error(self):
        assert is_retryable_tx_error(Exception("nonce too low"))
        assert is_retryable_tx_error(Exception("REPLACEMENT TRANSACTION UNDERPRICED"))
        assert is_retryable_tx_error(Exception("EOF error"))
        
        assert not is_retryable_tx_error(Exception("other error"))
        assert not is_retryable_tx_error(None)
    
    def test_skip_to_position_seekable(self):
        data = b"0123456789"
        reader = io.BytesIO(data)
        
        skip_to_position(reader, 5)
        
        # Should be at position 5
        remaining = reader.read()
        assert remaining == b"56789"
    
    def test_skip_to_position_non_seekable(self):
        class NonSeekableReader:
            def __init__(self, data):
                self.data = data
                self.pos = 0
            
            def read(self, size=-1):
                if size == -1:
                    result = self.data[self.pos:]
                    self.pos = len(self.data)
                else:
                    result = self.data[self.pos:self.pos + size]
                    self.pos += len(result)
                return result
        
        reader = NonSeekableReader(b"0123456789")
        
        skip_to_position(reader, 5)
        
        remaining = reader.read()
        assert remaining == b"56789"
    
    def test_skip_to_position_zero(self):
        reader = io.BytesIO(b"test data")
        original_pos = reader.tell()
        
        skip_to_position(reader, 0)
        
        assert reader.tell() == original_pos
    
    def test_parse_timestamp_none(self):
        result = parse_timestamp(None)
        assert result is None
    
    def test_parse_timestamp_with_astime(self):
        mock_ts = Mock()
        expected_datetime = datetime.now()
        mock_ts.AsTime.return_value = expected_datetime
        
        result = parse_timestamp(mock_ts)
        assert result == expected_datetime
    
    def test_parse_timestamp_without_astime(self):
        mock_ts = Mock()
        del mock_ts.AsTime  # Remove AsTime method
        
        result = parse_timestamp(mock_ts)
        assert result == mock_ts  # Should return the object itself
    
    def test_extract_block_data_cid_decode_error(self):
        with patch('multiformats.CID.decode') as mock_decode:
            mock_decode.side_effect = Exception("Invalid CID format")
            
            with pytest.raises(ValueError, match="failed to decode CID"):
                extract_block_data("invalid_cid", b"data")
    
    @patch('multiformats.CID.decode')
    @patch('sdk.dag.extract_block_data')
    def test_extract_block_data_dag_pb_error(self, mock_dag_extract, mock_cid_decode):
        mock_cid = Mock()
        mock_cid.codec.name = "dag-pb"
        mock_cid_decode.return_value = mock_cid
        mock_dag_extract.side_effect = Exception("DAG decode failed")
        
        with pytest.raises(ValueError, match="failed to decode DAG-PB node"):
            extract_block_data("test_cid", b"data")
    
    @patch('multiformats.CID.decode')
    def test_extract_block_data_codec_without_name(self, mock_cid_decode):
        mock_cid = Mock()
        mock_cid.codec = Mock(spec=[])
        mock_cid_decode.return_value = mock_cid
        
        with pytest.raises(ValueError, match="unknown cid type"):
            extract_block_data("test_cid", b"data")
    
    @patch('sdk.sdk.derive_key')
    def test_encryption_key_derivation_error(self, mock_derive_key):
        mock_derive_key.side_effect = Exception("Key derivation failed")
        
        with pytest.raises(SDKError, match="failed to derive key"):
            encryption_key_derivation(b"parent_key_32_bytes_long_test123", "bucket")
    
    def test_encryption_key_derivation_single_info(self):
        with patch('sdk.sdk.derive_key') as mock_derive_key:
            mock_derive_key.return_value = b"derived"
            result = encryption_key_derivation(b"parent_key_32_bytes_long_test123", "single")
            mock_derive_key.assert_called_once_with(b"parent_key_32_bytes_long_test123", b"single")
            assert result == b"derived"
    
    def test_encryption_key_derivation_multiple_info(self):
        with patch('sdk.sdk.derive_key') as mock_derive_key:
            mock_derive_key.return_value = b"derived"
            result = encryption_key_derivation(b"key", "a", "b", "c")
            mock_derive_key.assert_called_once_with(b"key", b"a/b/c")
    
    def test_skip_to_position_no_seek_no_read(self):
        class NoOpReader:
            pass
        
        reader = NoOpReader()
        
        with pytest.raises(SDKError, match="reader does not support seek or read operations"):
            skip_to_position(reader, 5)
    
    def test_skip_to_position_seek_fails(self):
        class FailingSeekReader:
            def seek(self, pos, whence):
                raise OSError("Seek not supported")
            
            def read(self, size=-1):
                return b"x" * size if size > 0 else b""
        
        reader = FailingSeekReader()
        skip_to_position(reader, 5)
    
    def test_skip_to_position_eof_before_position(self):
        class ShortReader:
            def __init__(self):
                self.data = b"abc"
                self.pos = 0
            
            def read(self, size=-1):
                if self.pos >= len(self.data):
                    return b""
                result = self.data[self.pos:self.pos + size]
                self.pos += len(result)
                return result
        
        reader = ShortReader()
        skip_to_position(reader, 10)
    
    def test_is_retryable_tx_error_case_insensitive(self):
        assert is_retryable_tx_error(Exception("NONCE TOO LOW"))
        assert is_retryable_tx_error(Exception("Nonce Too Low"))
        assert is_retryable_tx_error(Exception("replacement transaction underpriced"))
    
    def test_is_retryable_tx_error_partial_match(self):
        assert is_retryable_tx_error(Exception("transaction failed: nonce too low for account"))
        assert is_retryable_tx_error(Exception("error: replacement transaction underpriced at block 123"))
        assert is_retryable_tx_error(Exception("connection eof"))
    
    def test_parse_timestamp_datetime_object(self):
        now = datetime.now()
        result = parse_timestamp(now)
        assert result == now


@pytest.mark.integration
class TestSDKIntegration:
    
    def test_sdk_lifecycle(self, mock_sdk_config):
        with patch('grpc.insecure_channel'), \
             patch('sdk.sdk.nodeapi_pb2_grpc.NodeAPIStub'), \
             patch('sdk.sdk.ipcnodeapi_pb2_grpc.IPCNodeAPIStub'):
            
            sdk = SDK(mock_sdk_config)   
            assert sdk.config == mock_sdk_config 
            sdk.close()
            sdk.conn.close.assert_called_once()
    
    @patch('sdk.sdk.AkaveContractFetcher')
    def test_fetch_contract_info_success(self, mock_fetcher_class, mock_sdk_config):
        with patch('grpc.insecure_channel'), \
             patch('sdk.sdk.nodeapi_pb2_grpc.NodeAPIStub'), \
             patch('sdk.sdk.ipcnodeapi_pb2_grpc.IPCNodeAPIStub'):
            
            sdk = SDK(mock_sdk_config)
            
            mock_fetcher = Mock()
            mock_fetcher.connect.return_value = True
            mock_fetcher.fetch_contract_addresses.return_value = {
                'dial_uri': 'https://test.uri',
                'contract_address': '0x123...'
            }
            mock_fetcher_class.return_value = mock_fetcher
            
            result = sdk._fetch_contract_info()
            
            assert result is not None
            assert result['dial_uri'] == 'https://test.uri'
            assert result['contract_address'] == '0x123...'
            
            result2 = sdk._fetch_contract_info()
            assert result2 == result
    
    @patch('sdk.sdk.AkaveContractFetcher')
    def test_fetch_contract_info_failure(self, mock_fetcher_class, mock_sdk_config):
        with patch('grpc.insecure_channel'), \
             patch('sdk.sdk.nodeapi_pb2_grpc.NodeAPIStub'), \
             patch('sdk.sdk.ipcnodeapi_pb2_grpc.IPCNodeAPIStub'):
            
            sdk = SDK(mock_sdk_config)
            
            mock_fetcher = Mock()
            mock_fetcher.connect.return_value = False
            mock_fetcher_class.return_value = mock_fetcher
            
            result = sdk._fetch_contract_info()
            
            assert result is None
    
    @patch('sdk.sdk.AkaveContractFetcher')
    def test_fetch_contract_info_incomplete_data(self, mock_fetcher_class, mock_sdk_config):
        with patch('grpc.insecure_channel'), \
             patch('sdk.sdk.nodeapi_pb2_grpc.NodeAPIStub'), \
             patch('sdk.sdk.ipcnodeapi_pb2_grpc.IPCNodeAPIStub'):
            
            sdk = SDK(mock_sdk_config)
            
            mock_fetcher = Mock()
            mock_fetcher.connect.return_value = True
            mock_fetcher.fetch_contract_addresses.return_value = {
                'dial_uri': None,
                'contract_address': '0x123...'
            }
            mock_fetcher_class.return_value = mock_fetcher
            
            result = sdk._fetch_contract_info()
            assert result is None
    
    @patch('sdk.sdk.AkaveContractFetcher')
    def test_fetch_contract_info_caching(self, mock_fetcher_class, mock_sdk_config):
        with patch('grpc.insecure_channel'), \
             patch('sdk.sdk.nodeapi_pb2_grpc.NodeAPIStub'), \
             patch('sdk.sdk.ipcnodeapi_pb2_grpc.IPCNodeAPIStub'):
            
            sdk = SDK(mock_sdk_config)
            
            sdk._contract_info = {
                'dial_uri': 'cached_uri',
                'contract_address': 'cached_address'
            }
            
            result = sdk._fetch_contract_info()
            
            assert result['dial_uri'] == 'cached_uri'
            mock_fetcher_class.assert_not_called()


@pytest.mark.integration
class TestSDKErrorHandling:
    
    @patch('grpc.insecure_channel')
    @patch('sdk.sdk.nodeapi_pb2_grpc.NodeAPIStub')
    @patch('sdk.sdk.ipcnodeapi_pb2_grpc.IPCNodeAPIStub')
    def test_grpc_unavailable_error(self, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        sdk.client.BucketCreate.side_effect = grpc.RpcError()
        sdk._grpc_base._handle_grpc_error = Mock(side_effect=SDKError("Service unavailable"))
        
        with pytest.raises(SDKError, match="Service unavailable"):
            sdk.create_bucket("test-bucket")
    
    @patch('grpc.insecure_channel')
    @patch('sdk.sdk.nodeapi_pb2_grpc.NodeAPIStub')
    @patch('sdk.sdk.ipcnodeapi_pb2_grpc.IPCNodeAPIStub')
    @patch('sdk.sdk.Client.dial')
    @patch('time.sleep')
    def test_ipc_retry_exhaustion(self, mock_sleep, mock_dial, mock_ipc_stub, mock_node_stub, mock_channel):
        config = SDKConfig(
            address="test.node.ai:5500",
            private_key="test_key",
            block_part_size=512 * 1024
        )
        mock_channel.return_value = Mock()
        
        sdk = SDK(config)
        sdk._contract_info = {
            'dial_uri': 'https://test.uri',
            'contract_address': '0x123...'
        }
        
        mock_dial.side_effect = Exception("Connection timeout")
        
        with pytest.raises(SDKError, match="Failed to dial IPC client after 3 attempts"):
            sdk.ipc()
        
        assert mock_dial.call_count == 3

def test_placeholder():
    """Placeholder test to keep file valid."""
    pass
