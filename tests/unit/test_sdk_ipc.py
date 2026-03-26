"""
Unit tests for IPC module.

These tests use mocks to validate all IPC operations without requiring
network access.
"""

import io
from unittest.mock import Mock, patch

import grpc
import pytest

from sdk.config import SDKConfig, SDKError
from sdk.model import (
    Chunk,
    FileBlockDownload,
    FileChunkDownload,
    IPCBucket,
    IPCBucketCreateResult,
    IPCFileDownload,
    IPCFileListItem,
    IPCFileMeta,
    IPCFileUpload,
)
from sdk.sdk_ipc import IPC

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_ipc_instance():
    mock_ipc = Mock()
    mock_ipc.auth = Mock()
    mock_ipc.auth.address = "0x1234567890abcdef1234567890abcdef12345678"
    mock_ipc.auth.key = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
    mock_ipc.storage = Mock()
    mock_ipc.eth = Mock()
    mock_ipc.eth.eth = Mock()
    mock_ipc.access_manager = Mock()
    return mock_ipc


def _make_config(**overrides):
    defaults = dict(
        address="test:5500",
        max_concurrency=2,
        block_part_size=1048576,
        use_connection_pool=True,
    )
    defaults.update(overrides)
    return SDKConfig(**defaults)


def _make_ipc(mock_client=None, mock_conn=None, mock_ipc=None, config=None):
    return IPC(
        client=mock_client or Mock(),
        conn=mock_conn or Mock(),
        ipc_instance=mock_ipc or _make_ipc_instance(),
        config=config or _make_config(),
    )


# ===================================================================
# Bucket operations
# ===================================================================


class TestCreateBucket:
    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

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

    def test_create_bucket_invalid_short_name(self):
        with pytest.raises(SDKError, match="invalid bucket name"):
            self.ipc.create_bucket(None, "ab")

    def test_create_bucket_transaction_failed(self):
        mock_receipt = Mock()
        mock_receipt.status = 0
        mock_receipt.blockNumber = 100

        self.mock_ipc.storage.create_bucket.return_value = "0xtx"
        self.mock_ipc.eth.eth.wait_for_transaction_receipt.return_value = mock_receipt

        with pytest.raises(SDKError, match="bucket creation failed"):
            self.ipc.create_bucket(None, "test-bucket")

    def test_create_bucket_storage_exception(self):
        self.mock_ipc.storage.create_bucket.side_effect = Exception("rpc failure")

        with pytest.raises(SDKError, match="bucket creation failed"):
            self.ipc.create_bucket(None, "test-bucket")


class TestViewBucket:
    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc)

    def test_view_bucket_success(self):
        mock_response = Mock()
        mock_response.id = "0xbucket_id"
        mock_response.name = "my-bucket"
        mock_response.created_at = Mock(seconds=1700000000)
        self.mock_client.BucketView.return_value = mock_response

        result = self.ipc.view_bucket(None, "my-bucket")
        assert isinstance(result, IPCBucket)
        assert result.name == "my-bucket"
        assert result.created_at == 1700000000

    def test_view_bucket_empty_name(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.view_bucket(None, "")

    def test_view_bucket_not_found_grpc(self):
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.NOT_FOUND
        rpc_error.details = lambda: "not found"
        self.mock_client.BucketView.side_effect = rpc_error

        assert self.ipc.view_bucket(None, "missing") is None

    def test_view_bucket_not_found_in_details(self):
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.INTERNAL
        rpc_error.details = lambda: "bucket not found in storage"
        self.mock_client.BucketView.side_effect = rpc_error

        assert self.ipc.view_bucket(None, "missing") is None

    def test_view_bucket_grpc_other_error(self):
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.INTERNAL
        rpc_error.details = lambda: "server exploded"
        self.mock_client.BucketView.side_effect = rpc_error

        with pytest.raises(SDKError, match="failed to view bucket"):
            self.ipc.view_bucket(None, "my-bucket")

    def test_view_bucket_none_response(self):
        self.mock_client.BucketView.return_value = None
        assert self.ipc.view_bucket(None, "my-bucket") is None

    def test_view_bucket_no_created_at(self):
        mock_response = Mock(spec=["id", "name"])
        mock_response.id = "0x123"
        mock_response.name = "no-time"
        self.mock_client.BucketView.return_value = mock_response

        result = self.ipc.view_bucket(None, "no-time")
        assert result.created_at == 0

    def test_view_bucket_generic_not_found(self):
        self.mock_client.BucketView.side_effect = Exception("not found in db")
        assert self.ipc.view_bucket(None, "x") is None

    def test_view_bucket_generic_other(self):
        self.mock_client.BucketView.side_effect = Exception("disk full")
        with pytest.raises(SDKError, match="failed to get bucket"):
            self.ipc.view_bucket(None, "x")


class TestListBuckets:
    def setup_method(self):
        self.mock_client = Mock()
        self.ipc = _make_ipc(mock_client=self.mock_client)

    def test_list_buckets_success(self):
        b1 = Mock(name="a", created_at=Mock(seconds=1000))
        b1.name = "bucket-a"
        b2 = Mock(name="b", created_at=Mock(seconds=2000))
        b2.name = "bucket-b"

        self.mock_client.BucketList.return_value = Mock(buckets=[b1, b2])

        result = self.ipc.list_buckets(None)
        assert len(result) == 2
        assert result[0].name == "bucket-a"

    def test_list_buckets_empty(self):
        self.mock_client.BucketList.return_value = Mock(buckets=[])
        assert self.ipc.list_buckets(None) == []

    def test_list_buckets_no_buckets_field(self):
        self.mock_client.BucketList.return_value = Mock(spec=[])
        assert self.ipc.list_buckets(None) == []

    def test_list_buckets_zero_limit_uses_default(self):
        self.mock_client.BucketList.return_value = Mock(buckets=[])
        self.ipc.list_buckets(None, offset=0, limit=0)
        request = self.mock_client.BucketList.call_args[0][0]
        assert request.limit == 10000

    def test_list_buckets_grpc_error(self):
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.UNAVAILABLE
        rpc_error.details = lambda: "connection refused"
        self.mock_client.BucketList.side_effect = rpc_error

        with pytest.raises(SDKError, match="failed to list buckets"):
            self.ipc.list_buckets(None)

    def test_list_buckets_generic_exception(self):
        self.mock_client.BucketList.side_effect = Exception("timeout")
        with pytest.raises(SDKError, match="failed to list buckets"):
            self.ipc.list_buckets(None)


class TestDeleteBucket:
    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc)

    def test_delete_bucket_empty_name(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.delete_bucket(None, "")

    def test_delete_bucket_success(self):
        self.mock_client.BucketView.return_value = Mock(id="0xbucket_id_hex", name="my-bucket")
        self.mock_ipc.storage.delete_bucket.return_value = "0xtx"

        assert self.ipc.delete_bucket(None, "my-bucket") is None
        self.mock_ipc.storage.delete_bucket.assert_called_once()

    def test_delete_bucket_not_found(self):
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.NOT_FOUND
        rpc_error.details = lambda: "not found"
        self.mock_client.BucketView.side_effect = rpc_error

        with pytest.raises(SDKError, match="bucket .* not found"):
            self.ipc.delete_bucket(None, "ghost")

    def test_delete_bucket_no_id(self):
        self.mock_client.BucketView.return_value = Mock(spec=["name"], name="x")
        with pytest.raises(SDKError):
            self.ipc.delete_bucket(None, "x")

    def test_delete_bucket_blockchain_failure(self):
        self.mock_client.BucketView.return_value = Mock(id="0xabc", name="x")
        self.mock_ipc.storage.delete_bucket.side_effect = Exception("gas limit")
        with pytest.raises(SDKError, match="failed to delete bucket"):
            self.ipc.delete_bucket(None, "x")


# ===================================================================
# File operations
# ===================================================================


class TestFileInfo:
    def setup_method(self):
        self.mock_client = Mock()
        self.ipc = _make_ipc(mock_client=self.mock_client)

    def test_file_info_success(self):
        mock_response = Mock(
            root_cid="bafyabc",
            file_name="data.bin",
            bucket_name="my-bucket",
            encoded_size=2048,
            actual_size=1024,
            is_public=False,
            created_at=Mock(seconds=1700000000),
        )
        self.mock_client.FileView.return_value = mock_response

        result = self.ipc.file_info(None, "my-bucket", "data.bin")
        assert isinstance(result, IPCFileMeta)
        assert result.root_cid == "bafyabc"
        assert result.actual_size == 1024

    def test_file_info_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.file_info(None, "", "file.txt")

    def test_file_info_empty_file(self):
        with pytest.raises(SDKError, match="empty file name"):
            self.ipc.file_info(None, "bucket", "")

    def test_file_info_not_found_grpc(self):
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.NOT_FOUND
        rpc_error.details = lambda: "file not found"
        self.mock_client.FileView.side_effect = rpc_error

        assert self.ipc.file_info(None, "bucket", "missing.txt") is None

    def test_file_info_not_exist_grpc(self):
        """Server returns 'file not exists' with UNKNOWN status code."""
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.UNKNOWN
        rpc_error.details = lambda: "file not exists: ghost.bin"
        self.mock_client.FileView.side_effect = rpc_error

        assert self.ipc.file_info(None, "bucket", "ghost.bin") is None

    def test_file_info_none_response(self):
        self.mock_client.FileView.return_value = None
        assert self.ipc.file_info(None, "bucket", "x") is None

    def test_file_info_grpc_other_error(self):
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.INTERNAL
        rpc_error.details = lambda: "internal server error"
        self.mock_client.FileView.side_effect = rpc_error

        with pytest.raises(SDKError, match="failed to get file info"):
            self.ipc.file_info(None, "bucket", "file.txt")

    def test_file_info_generic_not_exist(self):
        self.mock_client.FileView.side_effect = Exception("file not exists")
        assert self.ipc.file_info(None, "bucket", "x") is None

    def test_file_info_generic_other(self):
        self.mock_client.FileView.side_effect = Exception("network error")
        with pytest.raises(SDKError, match="failed to get file info"):
            self.ipc.file_info(None, "bucket", "file.txt")


class TestListFiles:
    def setup_method(self):
        self.mock_client = Mock()
        self.ipc = _make_ipc(mock_client=self.mock_client)

    def test_list_files_success(self):
        f1 = Mock(
            name="f1",
            root_cid="bafya",
            encoded_size=1024,
            actual_size=512,
            created_at=Mock(seconds=1000),
        )
        f1.name = "file-a.txt"
        f2 = Mock(
            name="f2",
            root_cid="bafyb",
            encoded_size=2048,
            actual_size=1024,
            created_at=Mock(seconds=2000),
        )
        f2.name = "file-b.bin"
        self.mock_client.FileList.return_value = Mock(list=[f1, f2])

        result = self.ipc.list_files(None, "my-bucket")
        assert len(result) == 2
        assert isinstance(result[0], IPCFileListItem)

    def test_list_files_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.list_files(None, "")

    def test_list_files_empty_result(self):
        self.mock_client.FileList.return_value = Mock(list=[])
        assert self.ipc.list_files(None, "empty") == []

    def test_list_files_no_list_field(self):
        self.mock_client.FileList.return_value = Mock(spec=[])
        assert self.ipc.list_files(None, "bucket") == []

    def test_list_files_grpc_error(self):
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.UNAVAILABLE
        rpc_error.details = lambda: "connection refused"
        self.mock_client.FileList.side_effect = rpc_error
        with pytest.raises(SDKError, match="failed to list files"):
            self.ipc.list_files(None, "bucket")


class TestFileDelete:
    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config(max_concurrency=1, block_part_size=1024, use_connection_pool=False)
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

    def test_file_delete_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket or file name"):
            self.ipc.file_delete(None, "", "file.txt")

    def test_file_delete_empty_filename(self):
        with pytest.raises(SDKError, match="empty bucket or file name"):
            self.ipc.file_delete(None, "bucket", "")

    def test_file_delete_whitespace_only(self):
        with pytest.raises(SDKError, match="empty bucket or file name"):
            self.ipc.file_delete(None, "   ", "file.txt")

    def test_file_delete_success(self):
        self.mock_ipc.storage.get_bucket_by_name.return_value = (b"bucket_id", "bucket")
        self.mock_ipc.storage.get_file_by_name.return_value = (b"file_id", "file")
        self.mock_ipc.storage.get_full_file_info.return_value = (
            (b"file_id", "file"),
            2,
            True,
        )
        self.mock_ipc.storage.delete_file.return_value = "0xtx"

        self.ipc.file_delete(None, "test-bucket", "test-file.txt")
        self.mock_ipc.storage.delete_file.assert_called_once()

    def test_file_delete_bucket_not_found(self):
        self.mock_ipc.storage.get_bucket_by_name.return_value = None
        with pytest.raises(SDKError, match="bucket .* not found"):
            self.ipc.file_delete(None, "missing", "file.txt")

    def test_file_delete_file_not_found(self):
        self.mock_ipc.storage.get_bucket_by_name.return_value = (b"id", "b")
        self.mock_ipc.storage.get_file_by_name.return_value = None
        with pytest.raises(SDKError, match="file does not exist"):
            self.ipc.file_delete(None, "bucket", "ghost.txt")

    def test_file_delete_full_file_info_not_exists(self):
        self.mock_ipc.storage.get_bucket_by_name.return_value = (b"id", "b")
        self.mock_ipc.storage.get_file_by_name.return_value = (b"fid", "f")
        self.mock_ipc.storage.get_full_file_info.return_value = (None, 0, False)
        with pytest.raises(SDKError, match="not found"):
            self.ipc.file_delete(None, "bucket", "file.txt")

    def test_file_delete_index_failure(self):
        self.mock_ipc.storage.get_bucket_by_name.return_value = (b"id", "b")
        self.mock_ipc.storage.get_file_by_name.return_value = (b"fid", "f")
        self.mock_ipc.storage.get_full_file_info.side_effect = Exception("rpc failure")
        with pytest.raises(SDKError, match="failed to determine file index"):
            self.ipc.file_delete(None, "bucket", "file.txt")


# ===================================================================
# Access control
# ===================================================================


class TestFileSetPublicAccess:
    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc)

    def test_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.file_set_public_access(None, "", "file.txt", True)

    def test_empty_file(self):
        with pytest.raises(SDKError, match="empty file name"):
            self.ipc.file_set_public_access(None, "bucket", "", True)

    def test_bucket_not_found(self):
        self.mock_client.BucketView.return_value = None
        with pytest.raises(SDKError):
            self.ipc.file_set_public_access(None, "bucket", "file.txt", True)

    def test_no_access_manager(self):
        self.mock_client.BucketView.return_value = Mock(
            id="abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            created_at=Mock(seconds=1000),
        )
        self.mock_ipc.storage.get_file_by_name.return_value = (b"file_id", "file.txt")
        self.mock_ipc.access_manager = None

        with pytest.raises(SDKError, match="access manager not available"):
            self.ipc.file_set_public_access(None, "bucket", "file.txt", True)

    def test_success(self):
        self.mock_client.BucketView.return_value = Mock(
            id="abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            created_at=Mock(seconds=1000),
        )
        self.mock_ipc.storage.get_file_by_name.return_value = (b"file_id", "file.txt")
        self.mock_ipc.access_manager.change_public_access.return_value = "0xtx"

        assert self.ipc.file_set_public_access(None, "bucket", "file.txt", True) is None
        self.mock_ipc.access_manager.change_public_access.assert_called_once()


# ===================================================================
# Download flow
# ===================================================================


class TestCreateFileDownload:
    def setup_method(self):
        self.mock_client = Mock()
        self.ipc = _make_ipc(mock_client=self.mock_client)

    def test_success(self):
        c1 = Mock(cid="bafychunk1", encoded_size=2048, size=1024)
        c2 = Mock(cid="bafychunk2", encoded_size=1024, size=512)
        self.mock_client.FileDownloadCreate.return_value = Mock(bucket_name="b", chunks=[c1, c2])

        result = self.ipc.create_file_download(None, "b", "f.bin")
        assert isinstance(result, IPCFileDownload)
        assert len(result.chunks) == 2
        assert result.chunks[0].index == 0
        assert result.chunks[1].index == 1

    def test_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.create_file_download(None, "", "f")

    def test_empty_file(self):
        with pytest.raises(SDKError, match="empty file name"):
            self.ipc.create_file_download(None, "b", "")

    def test_failure(self):
        self.mock_client.FileDownloadCreate.side_effect = Exception("timeout")
        with pytest.raises(SDKError, match="failed to create file download"):
            self.ipc.create_file_download(None, "b", "f")


class TestCreateRangeFileDownload:
    def setup_method(self):
        self.mock_client = Mock()
        self.ipc = _make_ipc(mock_client=self.mock_client)

    def test_success(self):
        c = Mock(cid="bafychunk", encoded_size=1024, size=512)
        self.mock_client.FileDownloadRangeCreate.return_value = Mock(bucket_name="b", chunks=[c])

        result = self.ipc.create_range_file_download(None, "b", "f", 2, 5)
        assert result.chunks[0].index == 2

    def test_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.create_range_file_download(None, "", "f", 0, 5)

    def test_empty_file(self):
        with pytest.raises(SDKError, match="empty file name"):
            self.ipc.create_range_file_download(None, "b", "", 0, 5)


class TestCreateChunkDownload:
    def setup_method(self):
        self.mock_client = Mock()
        self.ipc = _make_ipc(mock_client=self.mock_client)

    def test_success(self):
        blk = Mock(cid="bafyblock", permit="p", node_address="node:5500", node_id="n")
        self.mock_client.FileDownloadChunkCreate.return_value = Mock(blocks=[blk])

        chunk = Chunk(cid="bafychunk", encoded_size=2048, size=1024, index=0)
        result = self.ipc.create_chunk_download(None, "b", "f", chunk)

        assert isinstance(result, FileChunkDownload)
        assert len(result.blocks) == 1
        assert isinstance(result.blocks[0], FileBlockDownload)

    def test_failure(self):
        self.mock_client.FileDownloadChunkCreate.side_effect = Exception("timeout")
        chunk = Chunk(cid="c", encoded_size=1, size=1, index=0)
        with pytest.raises(SDKError, match="failed to create chunk download"):
            self.ipc.create_chunk_download(None, "b", "f", chunk)


class TestDownloadChunkBlocks:
    def setup_method(self):
        self.mock_client = Mock()
        self.ipc = _make_ipc(mock_client=self.mock_client)

    @patch("sdk.dag.extract_block_data")
    def test_success(self, mock_extract):
        mock_extract.side_effect = [b"block0_data", b"block1_data"]

        pool = Mock()
        pool.create_ipc_client.return_value = (
            Mock(FileDownloadBlock=Mock(return_value=[Mock(data=b"raw")])),
            Mock(),
            None,
        )

        block0 = FileBlockDownload(cid="b0", data=b"", permit="p", node_address="n:5500", node_id="n")
        block1 = FileBlockDownload(cid="b1", data=b"", permit="p", node_address="n:5500", node_id="n")
        chunk_dl = FileChunkDownload(cid="c", index=0, encoded_size=2048, size=1024, blocks=[block0, block1])

        writer = io.BytesIO()
        self.ipc.download_chunk_blocks(None, pool, "b", "f", "0xaddr", chunk_dl, b"", writer)

        assert writer.getvalue() == b"block0_datablock1_data"

    @patch("private.encryption.decrypt", return_value=b"decrypted")
    @patch("sdk.dag.extract_block_data", return_value=b"encrypted")
    def test_with_decryption(self, mock_extract, mock_decrypt):
        pool = Mock()
        pool.create_ipc_client.return_value = (
            Mock(FileDownloadBlock=Mock(return_value=[Mock(data=b"raw")])),
            Mock(),
            None,
        )

        block = FileBlockDownload(cid="b", data=b"", permit="p", node_address="n:5500", node_id="n")
        chunk_dl = FileChunkDownload(cid="c", index=0, encoded_size=100, size=50, blocks=[block])

        writer = io.BytesIO()
        self.ipc.download_chunk_blocks(None, pool, "b", "f", "0xaddr", chunk_dl, b"a" * 32, writer)
        mock_decrypt.assert_called_once_with(b"a" * 32, b"encrypted", b"0")
        assert writer.getvalue() == b"decrypted"


class TestFetchBlockData:
    def setup_method(self):
        self.ipc = _make_ipc()

    def test_success(self):
        pool = Mock()
        client = Mock()
        client.FileDownloadBlock.return_value = [
            Mock(data=b"hello "),
            Mock(data=b"world"),
        ]
        pool.create_ipc_client.return_value = (client, Mock(), None)

        block = Mock(node_address="node:5500", cid="bafyblock")
        result = self.ipc.fetch_block_data(None, pool, "c", "b", "f", "0x", 0, 0, block)
        assert result == b"hello world"

    def test_missing_metadata(self):
        block = Mock(spec=[])
        with pytest.raises(SDKError, match="missing block metadata"):
            self.ipc.fetch_block_data(None, Mock(), "c", "b", "f", "0x", 0, 0, block)

    def test_client_creation_failure(self):
        pool = Mock()
        pool.create_ipc_client.return_value = (None, None, Exception("conn failed"))
        block = Mock(node_address="bad:5500", cid="b")
        with pytest.raises(SDKError, match="failed to create client"):
            self.ipc.fetch_block_data(None, pool, "c", "b", "f", "0x", 0, 0, block)

    def test_empty_node_address(self):
        block = Mock(node_address="", cid="b")
        with pytest.raises(SDKError, match="missing block metadata"):
            self.ipc.fetch_block_data(None, Mock(), "c", "b", "f", "0x", 0, 0, block)


class TestDownload:
    def setup_method(self):
        self.ipc = _make_ipc()

    @patch.object(IPC, "download_chunk_blocks")
    @patch.object(IPC, "create_chunk_download")
    def test_iterates_chunks(self, mock_create, mock_dl_blocks):
        c1 = Chunk(cid="c1", encoded_size=100, size=50, index=0)
        c2 = Chunk(cid="c2", encoded_size=100, size=50, index=1)
        fd = IPCFileDownload(bucket_name="b", name="f", chunks=[c1, c2])

        self.ipc.download(None, fd, io.BytesIO())
        assert mock_create.call_count == 2
        assert mock_dl_blocks.call_count == 2

    @patch.object(IPC, "download_chunk_blocks")
    @patch.object(IPC, "create_chunk_download")
    def test_context_cancelled(self, mock_create, mock_dl_blocks):
        ctx = Mock()
        ctx.done.return_value = True
        fd = IPCFileDownload(
            bucket_name="b",
            name="f",
            chunks=[Chunk(cid="c", encoded_size=1, size=1, index=0)],
        )

        with pytest.raises(SDKError, match="failed to download file"):
            self.ipc.download(ctx, fd, io.BytesIO())


# ===================================================================
# Upload flow
# ===================================================================


class TestCreateFileUpload:
    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc)

    def test_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.create_file_upload(None, "", "f")

    def test_empty_file(self):
        with pytest.raises(SDKError, match="empty file name"):
            self.ipc.create_file_upload(None, "b", "")

    def test_bucket_not_found(self):
        self.mock_client.BucketView.return_value = None
        with pytest.raises(SDKError, match="failed to create file upload"):
            self.ipc.create_file_upload(None, "missing", "f")

    def test_file_already_exists(self):
        self.mock_client.BucketView.return_value = Mock(
            id="abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            created_at=Mock(seconds=1000),
        )
        self.mock_ipc.storage.create_file.side_effect = Exception("0x6891dde0 FileAlreadyExists")

        with pytest.raises(SDKError, match="file already exists"):
            self.ipc.create_file_upload(None, "b", "existing.txt")

    def test_success(self):
        self.mock_client.BucketView.return_value = Mock(
            id="abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            created_at=Mock(seconds=1000),
        )
        self.mock_ipc.storage.create_file.return_value = "0xtx"
        del self.mock_ipc.wait_for_tx
        del self.mock_ipc.web3

        result = self.ipc.create_file_upload(None, "b", "new.txt")
        assert isinstance(result, IPCFileUpload)
        assert result.name == "new.txt"


# ===================================================================
# Encryption helpers
# ===================================================================


class TestEncryptionHelpers:
    def test_encryption_key_empty_parent(self):
        from sdk.sdk_ipc import encryption_key

        assert encryption_key(b"", "b", "f") == b""

    def test_encryption_key_with_parent(self):
        from sdk.sdk_ipc import encryption_key

        result = encryption_key(b"secret_parent_key_32bytes_long!!", "b", "f")
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_maybe_encrypt_empty_key(self):
        from sdk.sdk_ipc import maybe_encrypt_metadata

        assert maybe_encrypt_metadata("my-file.txt", "b/f", b"") == "my-file.txt"

    def test_maybe_encrypt_with_key(self):
        from sdk.sdk_ipc import maybe_encrypt_metadata

        result = maybe_encrypt_metadata("my-file.txt", "b/f", b"a" * 32)
        assert result != "my-file.txt"
        bytes.fromhex(result)


# ===================================================================
# Internal helpers
# ===================================================================


class TestConvertCidToBytes:
    def setup_method(self):
        self.ipc = _make_ipc()

    def test_object_with_bytes(self):
        class FakeCID:
            def __bytes__(self):
                return b"\x01\x02\x03"

        assert self.ipc._convert_cid_to_bytes(FakeCID()) == b"\x01\x02\x03"


class TestCalculateFileId:
    def setup_method(self):
        self.ipc = _make_ipc()

    def test_deterministic(self):
        bucket_id = b"\x00" * 32
        assert self.ipc._calculate_file_id(bucket_id, "f.txt") == self.ipc._calculate_file_id(bucket_id, "f.txt")
        assert len(self.ipc._calculate_file_id(bucket_id, "f.txt")) == 32

    def test_different_inputs(self):
        bucket_id = b"\x00" * 32
        assert self.ipc._calculate_file_id(bucket_id, "a.txt") != self.ipc._calculate_file_id(bucket_id, "b.txt")


# ===================================================================
# IPC constructor & SDKConfig defaults
# ===================================================================


class TestIPCInit:
    def test_default_config(self):
        config = _make_config()
        ipc = _make_ipc(config=config)
        assert ipc.max_concurrency == 2
        assert ipc.block_part_size == 1048576
        assert ipc.use_connection_pool is True
        assert ipc.encryption_key == b""

    def test_config_with_encryption_key(self):
        ipc = _make_ipc(config=_make_config(encryption_key=b"my_key"))
        assert ipc.encryption_key == b"my_key"

    def test_config_streaming_max_blocks(self):
        ipc = _make_ipc(config=_make_config(streaming_max_blocks_in_chunk=64))
        assert ipc.max_blocks_in_chunk == 64

    def test_custom_retry(self):
        mock_retry = Mock()
        ipc = IPC(Mock(), Mock(), _make_ipc_instance(), _make_config(), with_retry=mock_retry)
        assert ipc.with_retry is mock_retry
