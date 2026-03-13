from unittest.mock import Mock

import pytest

from sdk.config import SDKConfig, SDKError
from sdk.model import IPCBucketCreateResult
from sdk.sdk_ipc import IPC


class TestCreateBucket:
    """Test create bucket functionality."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_conn = Mock()
        self.mock_ipc = Mock()
        self.mock_ipc.auth = Mock()
        self.mock_ipc.auth.address = "0x123"
        self.mock_ipc.auth.key = "key"
        self.mock_ipc.storage = Mock()
        self.mock_ipc.eth = Mock()
        self.mock_ipc.eth.eth = Mock()

        self.config = SDKConfig(
            address="test:5500", max_concurrency=10, block_part_size=1048576, use_connection_pool=True
        )
        self.ipc = IPC(self.mock_client, self.mock_conn, self.mock_ipc, self.config)

    def test_create_bucket_success(self):
        """Test successful bucket creation."""
        mock_receipt = Mock()
        mock_receipt.status = 1
        mock_receipt.blockNumber = 100
        mock_receipt.transactionHash = Mock()
        mock_receipt.transactionHash.hex.return_value = "0xabc"

        mock_block = Mock()
        mock_block.timestamp = 1234567890

        self.mock_ipc.storage.create_bucket.return_value = "0xtx"
        self.mock_ipc.eth.eth.wait_for_transaction_receipt.return_value = mock_receipt
        self.mock_ipc.eth.eth.get_block.return_value = mock_block

        result = self.ipc.create_bucket(None, "test-bucket")

        assert isinstance(result, IPCBucketCreateResult)
        assert result.name == "test-bucket"
        assert result.created_at == 1234567890


class TestFileDelete:
    """Test file delete functionality - Issue #55 fix."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_conn = Mock()
        self.mock_ipc = Mock()
        self.mock_ipc.auth = Mock()
        self.mock_ipc.auth.address = "0x123"
        self.mock_ipc.auth.key = "key"
        self.mock_ipc.storage = Mock()
        self.mock_ipc.eth = Mock()
        self.mock_ipc.eth.eth = Mock()

        self.config = SDKConfig(
            address="test:5500",
            max_concurrency=1,
            block_part_size=1024,
            use_connection_pool=False,
            streaming_max_blocks_in_chunk=10,
        )
        self.ipc = IPC(self.mock_client, self.mock_conn, self.mock_ipc, self.config)

    def test_file_delete_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket or file name"):
            self.ipc.file_delete(None, "", "file.txt")

    def test_file_delete_empty_filename(self):
        with pytest.raises(SDKError, match="empty bucket or file name"):
            self.ipc.file_delete(None, "bucket", "")

    def test_file_delete_success(self):
        mock_receipt = Mock()
        mock_receipt.status = 1

        # Mocks must return subscriptable elements since IPC expects bucket[0] and file_info[0]
        self.mock_ipc.storage.get_bucket_by_name.return_value = (b"mock_bucket_id", "mock_bucket_name")
        self.mock_ipc.storage.get_file_by_name.return_value = (b"mock_file_id", "mock_file_name")
        # get_full_file_info returns (File struct, index, exists)
        self.mock_ipc.storage.get_full_file_info.return_value = ((b"mock_file_id", "mock_file_name"), 2, True)

        self.mock_ipc.storage.delete_file.return_value = "0xtx"
        self.mock_ipc.eth.eth.wait_for_transaction_receipt.return_value = mock_receipt

        self.ipc.file_delete(None, "test-bucket", "test-file.txt")
        self.mock_ipc.storage.delete_file.assert_called_once()
