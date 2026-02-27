
from unittest.mock import Mock

import pytest

from sdk.config import SDKConfig, SDKError
from sdk.model import IPCBucketCreateResult
from sdk.sdk_ipc import IPC


class TestCreateBucket:
    """Test create bucket functionality."""


import pytest
from unittest.mock import Mock, MagicMock, patch, call
import io
from datetime import datetime

from sdk.sdk_ipc import IPC, encryption_key, maybe_encrypt_metadata, to_ipc_proto_chunk, TxWaitSignal
from sdk.config import SDKConfig, SDKError
from sdk.model import (
    IPCBucketCreateResult,
    IPCBucket,
    IPCFileMeta,
    IPCFileUpload,
    IPCFileDownload,
    FileBlockUpload,
    Chunk,
)


class TestTxWaitSignal:

    def test_init(self):
        chunk = Mock()
        tx = "0x123456"
        signal = TxWaitSignal(chunk, tx)

        assert signal.FileUploadChunk == chunk
        assert signal.Transaction == tx


class TestEncryptionKey:

    def test_encryption_key_empty_parent(self):
        result = encryption_key(b"", "bucket", "file")
        assert result == b""

    @patch("sdk.sdk_ipc.derive_key")
    def test_encryption_key_with_data(self, mock_derive):
        parent = b"parent_key_32bytes_test123456789"
        mock_derive.return_value = b"derived"

        result = encryption_key(parent, "bucket", "file")

        assert result == b"derived"
        mock_derive.assert_called_once_with(parent, b"bucket/file")

    @patch("sdk.sdk_ipc.derive_key")
    def test_encryption_key_multiple_info(self, mock_derive):
        parent = b"key"
        mock_derive.return_value = b"result"

        result = encryption_key(parent, "a", "b", "c")

        mock_derive.assert_called_once_with(parent, b"a/b/c")


class TestMaybeEncryptMetadata:

    def test_maybe_encrypt_metadata_no_key(self):
        result = maybe_encrypt_metadata("plain_value", "path", b"")
        assert result == "plain_value"

    @patch("sdk.sdk_ipc.derive_key")
    @patch("sdk.sdk_ipc.encrypt")
    def test_maybe_encrypt_metadata_with_key(self, mock_encrypt, mock_derive):
        key = b"encryption_key_32bytes_test12345"
        mock_derive.return_value = b"file_key"
        mock_encrypt.return_value = b"\x01\x02\x03"

        result = maybe_encrypt_metadata("value", "path/to/file", key)

        assert result == "010203"
        mock_derive.assert_called_once_with(key, b"path/to/file")
        mock_encrypt.assert_called_once_with(b"file_key", b"value", b"metadata")

    @patch("sdk.sdk_ipc.derive_key")
    @patch("sdk.sdk_ipc.encrypt")
    def test_maybe_encrypt_metadata_error(self, mock_encrypt, mock_derive):
        key = b"encryption_key_32bytes_test12345"
        mock_derive.side_effect = Exception("Derive failed")

        with pytest.raises(SDKError, match="failed to encrypt metadata"):
            maybe_encrypt_metadata("value", "path", key)


class TestToIPCProtoChunk:

    @patch("sdk.sdk_ipc.CID")
    @patch("sdk.sdk_ipc.ipcnodeapi_pb2")
    def test_to_ipc_proto_chunk_basic(self, mock_pb2, mock_cid):
        mock_cid.decode.return_value = b"bytes"
        mock_block_class = Mock()
        mock_pb2.IPCChunk.Block = mock_block_class
        mock_pb2.IPCChunk = Mock()

        blocks = [
            FileBlockUpload(cid="cid1", data=b"data1"),
            FileBlockUpload(cid="cid2", data=b"data2"),
        ]

        cids, sizes, proto_chunk, err = to_ipc_proto_chunk("chunk_cid", 0, 100, blocks)

        assert err is None
        assert isinstance(cids, list)
        assert isinstance(sizes, list)
        assert len(sizes) == 2

    def test_to_ipc_proto_chunk_empty_blocks(self):
        cids, sizes, proto_chunk, err = to_ipc_proto_chunk("cid", 0, 100, [])

        assert err is None
        assert cids == []
        assert sizes == []


class TestIPCInit:

    def test_ipc_init(self):
        mock_client = Mock()
        mock_conn = Mock()
        mock_ipc_instance = Mock()
        config = SDKConfig(
            address="test:5500",
            max_concurrency=5,
            block_part_size=128 * 1024,
            use_connection_pool=True,
            streaming_max_blocks_in_chunk=10,
        )

        ipc = IPC(mock_client, mock_conn, mock_ipc_instance, config)

        assert ipc.client == mock_client
        assert ipc.conn == mock_conn
        assert ipc.ipc == mock_ipc_instance
        assert ipc.max_concurrency == 5
        assert ipc.block_part_size == 128 * 1024
        assert ipc.max_blocks_in_chunk == 10


class TestCreateBucket:

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


        self.config = SDKConfig(address="test:5500")
        self.ipc = IPC(self.mock_client, self.mock_conn, self.mock_ipc, self.config)

    def test_create_bucket_invalid_name(self):
        with pytest.raises(SDKError, match="invalid bucket name"):
            self.ipc.create_bucket(None, "ab")

    def test_create_bucket_success(self):
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
