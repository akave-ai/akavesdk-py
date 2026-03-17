from unittest.mock import Mock

import pytest

from sdk.config import SDKConfig
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
