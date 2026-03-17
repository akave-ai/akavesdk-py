import io
from unittest.mock import MagicMock, Mock, patch

import grpc
import pytest

from sdk.config import SDKConfig, SDKError
from sdk.model import (
    Chunk,
    FileBlockDownload,
    FileChunkDownload,
    IPCBucket,
    IPCBucketCreateResult,
    IPCFileChunkUploadV2,
    IPCFileDownload,
    IPCFileListItem,
    IPCFileMeta,
    IPCFileMetaV2,
    IPCFileUpload,
)
from sdk.sdk_ipc import IPC

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_ipc_instance():
    """Create a standard mock IPC instance with auth, storage, eth."""
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
    """Create a standard SDKConfig with optional overrides."""
    defaults = dict(
        address="test:5500",
        max_concurrency=2,
        block_part_size=1048576,
        use_connection_pool=True,
    )
    defaults.update(overrides)
    return SDKConfig(**defaults)


def _make_ipc(mock_client=None, mock_conn=None, mock_ipc=None, config=None):
    """Assemble an IPC object from mocks."""
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
    """Test create bucket functionality."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

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

    def test_create_bucket_invalid_short_name(self):
        """Bucket name shorter than MIN_BUCKET_NAME_LENGTH should fail."""
        with pytest.raises(SDKError, match="invalid bucket name"):
            self.ipc.create_bucket(None, "ab")

    def test_create_bucket_transaction_failed(self):
        """Transaction receipt with status != 1 should raise."""
        mock_receipt = Mock()
        mock_receipt.status = 0
        mock_receipt.blockNumber = 100

        self.mock_ipc.storage.create_bucket.return_value = "0xtx"
        self.mock_ipc.eth.eth.wait_for_transaction_receipt.return_value = mock_receipt

        with pytest.raises(SDKError, match="bucket creation failed"):
            self.ipc.create_bucket(None, "test-bucket")

    def test_create_bucket_storage_exception(self):
        """Storage contract throwing should propagate as SDKError."""
        self.mock_ipc.storage.create_bucket.side_effect = Exception("rpc failure")

        with pytest.raises(SDKError, match="bucket creation failed"):
            self.ipc.create_bucket(None, "test-bucket")


class TestViewBucket:
    """Test view bucket functionality."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

    def test_view_bucket_success(self):
        mock_response = Mock()
        mock_response.id = "0xbucket_id"
        mock_response.name = "my-bucket"
        mock_response.created_at = Mock()
        mock_response.created_at.seconds = 1700000000

        self.mock_client.BucketView.return_value = mock_response

        result = self.ipc.view_bucket(None, "my-bucket")
        assert isinstance(result, IPCBucket)
        assert result.name == "my-bucket"
        assert result.created_at == 1700000000

    def test_view_bucket_empty_name(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.view_bucket(None, "")

    def test_view_bucket_not_found_grpc(self):
        """gRPC NOT_FOUND should return None, not raise."""
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.NOT_FOUND
        rpc_error.details = lambda: "not found"
        self.mock_client.BucketView.side_effect = rpc_error

        result = self.ipc.view_bucket(None, "missing-bucket")
        assert result is None

    def test_view_bucket_not_found_in_details(self):
        """gRPC error with 'not found' in details should return None."""
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.INTERNAL
        rpc_error.details = lambda: "bucket not found in storage"
        self.mock_client.BucketView.side_effect = rpc_error

        result = self.ipc.view_bucket(None, "missing-bucket")
        assert result is None

    def test_view_bucket_grpc_other_error(self):
        """Non-not-found gRPC errors should raise."""
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.INTERNAL
        rpc_error.details = lambda: "server exploded"
        self.mock_client.BucketView.side_effect = rpc_error

        with pytest.raises(SDKError, match="failed to view bucket"):
            self.ipc.view_bucket(None, "my-bucket")

    def test_view_bucket_none_response(self):
        self.mock_client.BucketView.return_value = None

        result = self.ipc.view_bucket(None, "my-bucket")
        assert result is None

    def test_view_bucket_no_created_at(self):
        """Response without created_at should default to 0."""
        mock_response = Mock(spec=["id", "name"])
        mock_response.id = "0x123"
        mock_response.name = "no-time-bucket"

        self.mock_client.BucketView.return_value = mock_response

        result = self.ipc.view_bucket(None, "no-time-bucket")
        assert result.created_at == 0


class TestListBuckets:
    """Test list buckets functionality."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

    def test_list_buckets_success(self):
        bucket1 = Mock()
        bucket1.name = "bucket-a"
        bucket1.created_at = Mock()
        bucket1.created_at.seconds = 1000

        bucket2 = Mock()
        bucket2.name = "bucket-b"
        bucket2.created_at = Mock()
        bucket2.created_at.seconds = 2000

        mock_response = Mock()
        mock_response.buckets = [bucket1, bucket2]
        self.mock_client.BucketList.return_value = mock_response

        result = self.ipc.list_buckets(None)
        assert len(result) == 2
        assert result[0].name == "bucket-a"
        assert result[1].name == "bucket-b"

    def test_list_buckets_empty(self):
        mock_response = Mock()
        mock_response.buckets = []
        self.mock_client.BucketList.return_value = mock_response

        result = self.ipc.list_buckets(None)
        assert result == []

    def test_list_buckets_no_buckets_field(self):
        """Response with no 'buckets' attribute should return empty list."""
        mock_response = Mock(spec=[])
        self.mock_client.BucketList.return_value = mock_response

        result = self.ipc.list_buckets(None)
        assert result == []

    def test_list_buckets_with_offset_and_limit(self):
        mock_response = Mock()
        mock_response.buckets = []
        self.mock_client.BucketList.return_value = mock_response

        self.ipc.list_buckets(None, offset=5, limit=10)

        call_args = self.mock_client.BucketList.call_args
        request = call_args[0][0]
        assert request.offset == 5
        assert request.limit == 10

    def test_list_buckets_zero_limit_uses_default(self):
        """limit=0 should be translated to 10000 (the default)."""
        mock_response = Mock()
        mock_response.buckets = []
        self.mock_client.BucketList.return_value = mock_response

        self.ipc.list_buckets(None, offset=0, limit=0)

        call_args = self.mock_client.BucketList.call_args
        request = call_args[0][0]
        assert request.limit == 10000

    def test_list_buckets_grpc_error(self):
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.UNAVAILABLE
        rpc_error.details = lambda: "connection refused"
        self.mock_client.BucketList.side_effect = rpc_error

        with pytest.raises(SDKError, match="failed to list buckets"):
            self.ipc.list_buckets(None)


class TestDeleteBucket:
    """Test delete bucket functionality."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

    def test_delete_bucket_empty_name(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.delete_bucket(None, "")

    def test_delete_bucket_success(self):
        mock_view_response = Mock()
        mock_view_response.id = "0xbucket_id_hex"
        mock_view_response.name = "my-bucket"
        self.mock_client.BucketView.return_value = mock_view_response

        self.mock_ipc.storage.delete_bucket.return_value = "0xtx"

        result = self.ipc.delete_bucket(None, "my-bucket")
        assert result is None
        self.mock_ipc.storage.delete_bucket.assert_called_once()

    def test_delete_bucket_not_found(self):
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.NOT_FOUND
        rpc_error.details = lambda: "not found"
        self.mock_client.BucketView.side_effect = rpc_error

        with pytest.raises(SDKError, match="bucket .* not found"):
            self.ipc.delete_bucket(None, "ghost-bucket")

    def test_delete_bucket_no_id_in_response(self):
        """If the IPC response has no bucket ID, should raise."""
        mock_response = Mock(spec=["name"])
        mock_response.name = "my-bucket"
        self.mock_client.BucketView.return_value = mock_response

        with pytest.raises(SDKError):
            self.ipc.delete_bucket(None, "my-bucket")

    def test_delete_bucket_blockchain_failure(self):
        mock_response = Mock()
        mock_response.id = "0xabc123"
        mock_response.name = "my-bucket"
        self.mock_client.BucketView.return_value = mock_response

        self.mock_ipc.storage.delete_bucket.side_effect = Exception("gas limit exceeded")

        with pytest.raises(SDKError, match="failed to delete bucket"):
            self.ipc.delete_bucket(None, "my-bucket")


# ===================================================================
# File operations
# ===================================================================


class TestFileInfo:
    """Test file_info functionality."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

    def test_file_info_success(self):
        mock_response = Mock()
        mock_response.root_cid = "bafyabc"
        mock_response.file_name = "data.bin"
        mock_response.bucket_name = "my-bucket"
        mock_response.encoded_size = 2048
        mock_response.actual_size = 1024
        mock_response.is_public = False
        mock_response.created_at = Mock()
        mock_response.created_at.seconds = 1700000000

        self.mock_client.FileView.return_value = mock_response

        result = self.ipc.file_info(None, "my-bucket", "data.bin")
        assert isinstance(result, IPCFileMeta)
        assert result.root_cid == "bafyabc"
        assert result.name == "data.bin"
        assert result.encoded_size == 2048
        assert result.actual_size == 1024
        assert result.is_public is False

    def test_file_info_empty_bucket_name(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.file_info(None, "", "file.txt")

    def test_file_info_empty_file_name(self):
        with pytest.raises(SDKError, match="empty file name"):
            self.ipc.file_info(None, "bucket", "")

    def test_file_info_not_found_grpc(self):
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.NOT_FOUND
        rpc_error.details = lambda: "file not found"
        self.mock_client.FileView.side_effect = rpc_error

        result = self.ipc.file_info(None, "bucket", "missing.txt")
        assert result is None

    def test_file_info_none_response(self):
        self.mock_client.FileView.return_value = None

        result = self.ipc.file_info(None, "bucket", "missing.txt")
        assert result is None

    def test_file_info_grpc_other_error(self):
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.INTERNAL
        rpc_error.details = lambda: "internal server error"
        self.mock_client.FileView.side_effect = rpc_error

        with pytest.raises(SDKError, match="failed to get file info"):
            self.ipc.file_info(None, "bucket", "file.txt")


class TestListFiles:
    """Test list_files functionality."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

    def test_list_files_success(self):
        file1 = Mock()
        file1.name = "file-a.txt"
        file1.root_cid = "bafya"
        file1.encoded_size = 1024
        file1.actual_size = 512
        file1.created_at = Mock()
        file1.created_at.seconds = 1000

        file2 = Mock()
        file2.name = "file-b.bin"
        file2.root_cid = "bafyb"
        file2.encoded_size = 2048
        file2.actual_size = 1024
        file2.created_at = Mock()
        file2.created_at.seconds = 2000

        mock_response = Mock()
        mock_response.list = [file1, file2]
        self.mock_client.FileList.return_value = mock_response

        result = self.ipc.list_files(None, "my-bucket")
        assert len(result) == 2
        assert isinstance(result[0], IPCFileListItem)
        assert result[0].name == "file-a.txt"
        assert result[1].root_cid == "bafyb"

    def test_list_files_empty_bucket_name(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.list_files(None, "")

    def test_list_files_empty_result(self):
        mock_response = Mock()
        mock_response.list = []
        self.mock_client.FileList.return_value = mock_response

        result = self.ipc.list_files(None, "empty-bucket")
        assert result == []

    def test_list_files_no_list_field(self):
        """Response without 'list' attribute should return empty list."""
        mock_response = Mock(spec=[])
        self.mock_client.FileList.return_value = mock_response

        result = self.ipc.list_files(None, "bucket")
        assert result == []

    def test_list_files_grpc_error(self):
        rpc_error = grpc.RpcError()
        rpc_error.code = lambda: grpc.StatusCode.UNAVAILABLE
        rpc_error.details = lambda: "connection refused"
        self.mock_client.FileList.side_effect = rpc_error

        with pytest.raises(SDKError, match="failed to list files"):
            self.ipc.list_files(None, "bucket")


class TestFileDelete:
    """Test file delete functionality - Issue #55 fix."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config(
            max_concurrency=1,
            block_part_size=1024,
            use_connection_pool=False,
            streaming_max_blocks_in_chunk=10,
        )
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

    def test_file_delete_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket or file name"):
            self.ipc.file_delete(None, "", "file.txt")

    def test_file_delete_empty_filename(self):
        with pytest.raises(SDKError, match="empty bucket or file name"):
            self.ipc.file_delete(None, "bucket", "")

    def test_file_delete_success(self):
        mock_receipt = Mock()
        mock_receipt.status = 1

        self.mock_ipc.storage.get_bucket_by_name.return_value = (b"mock_bucket_id", "mock_bucket_name")
        self.mock_ipc.storage.get_file_by_name.return_value = (b"mock_file_id", "mock_file_name")
        self.mock_ipc.storage.get_full_file_info.return_value = ((b"mock_file_id", "mock_file_name"), 2, True)

        self.mock_ipc.storage.delete_file.return_value = "0xtx"
        self.mock_ipc.eth.eth.wait_for_transaction_receipt.return_value = mock_receipt

        self.ipc.file_delete(None, "test-bucket", "test-file.txt")
        self.mock_ipc.storage.delete_file.assert_called_once()

    def test_file_delete_bucket_not_found(self):
        self.mock_ipc.storage.get_bucket_by_name.return_value = None

        with pytest.raises(SDKError, match="bucket .* not found"):
            self.ipc.file_delete(None, "missing-bucket", "file.txt")

    def test_file_delete_file_not_found(self):
        self.mock_ipc.storage.get_bucket_by_name.return_value = (b"bucket_id", "bucket")
        self.mock_ipc.storage.get_file_by_name.return_value = None

        with pytest.raises(SDKError, match="file does not exist"):
            self.ipc.file_delete(None, "bucket", "ghost.txt")

    def test_file_delete_full_file_info_not_exists(self):
        """get_full_file_info returns exists=False."""
        self.mock_ipc.storage.get_bucket_by_name.return_value = (b"bucket_id", "bucket")
        self.mock_ipc.storage.get_file_by_name.return_value = (b"file_id", "file")
        self.mock_ipc.storage.get_full_file_info.return_value = (None, 0, False)

        with pytest.raises(SDKError, match="not found"):
            self.ipc.file_delete(None, "bucket", "file.txt")

    def test_file_delete_index_lookup_failure(self):
        """Exception during get_full_file_info should raise SDKError."""
        self.mock_ipc.storage.get_bucket_by_name.return_value = (b"bucket_id", "bucket")
        self.mock_ipc.storage.get_file_by_name.return_value = (b"file_id", "file")
        self.mock_ipc.storage.get_full_file_info.side_effect = Exception("rpc failure")

        with pytest.raises(SDKError, match="failed to determine file index"):
            self.ipc.file_delete(None, "bucket", "file.txt")

    def test_file_delete_whitespace_only_names(self):
        """Whitespace-only bucket or file names should be rejected."""
        with pytest.raises(SDKError, match="empty bucket or file name"):
            self.ipc.file_delete(None, "   ", "file.txt")

        with pytest.raises(SDKError, match="empty bucket or file name"):
            self.ipc.file_delete(None, "bucket", "   ")


# ===================================================================
# File access control
# ===================================================================


class TestFileSetPublicAccess:
    """Test file_set_public_access functionality."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

    def test_set_public_access_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.file_set_public_access(None, "", "file.txt", True)

    def test_set_public_access_empty_file(self):
        with pytest.raises(SDKError, match="empty file name"):
            self.ipc.file_set_public_access(None, "bucket", "", True)

    def test_set_public_access_bucket_not_found(self):
        """When the bucket doesn't exist, should raise."""
        self.mock_client.BucketView.return_value = None

        with pytest.raises(SDKError, match="failed to set public access"):
            self.ipc.file_set_public_access(None, "bucket", "file.txt", True)

    def test_set_public_access_no_access_manager(self):
        """When access_manager is not available, should raise."""
        mock_view_response = Mock()
        mock_view_response.id = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        mock_view_response.name = "bucket"
        mock_view_response.created_at = Mock(seconds=1000)
        self.mock_client.BucketView.return_value = mock_view_response

        self.mock_ipc.storage.get_file_by_name.return_value = (b"file_id", "file.txt")
        self.mock_ipc.access_manager = None

        with pytest.raises(SDKError, match="access manager not available"):
            self.ipc.file_set_public_access(None, "bucket", "file.txt", True)

    def test_set_public_access_success(self):
        mock_view_response = Mock()
        mock_view_response.id = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        mock_view_response.name = "bucket"
        mock_view_response.created_at = Mock(seconds=1000)
        self.mock_client.BucketView.return_value = mock_view_response

        self.mock_ipc.storage.get_file_by_name.return_value = (b"file_id", "file.txt")
        self.mock_ipc.access_manager.change_public_access.return_value = "0xtx"

        result = self.ipc.file_set_public_access(None, "bucket", "file.txt", True)
        assert result is None
        self.mock_ipc.access_manager.change_public_access.assert_called_once()

    def test_set_public_access_disable(self):
        """Setting is_public=False should call access_manager with False."""
        mock_view_response = Mock()
        mock_view_response.id = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        mock_view_response.name = "bucket"
        mock_view_response.created_at = Mock(seconds=1000)
        self.mock_client.BucketView.return_value = mock_view_response

        self.mock_ipc.storage.get_file_by_name.return_value = (b"file_id", "file.txt")
        self.mock_ipc.access_manager.change_public_access.return_value = "0xtx"

        self.ipc.file_set_public_access(None, "bucket", "file.txt", False)

        call_args = self.mock_ipc.access_manager.change_public_access.call_args
        assert call_args[0][2] is False


# ===================================================================
# Download flow
# ===================================================================


class TestCreateFileDownload:
    """Test create_file_download functionality."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

    def test_create_file_download_success(self):
        mock_chunk1 = Mock()
        mock_chunk1.cid = "bafychunk1"
        mock_chunk1.encoded_size = 2048
        mock_chunk1.size = 1024

        mock_chunk2 = Mock()
        mock_chunk2.cid = "bafychunk2"
        mock_chunk2.encoded_size = 1024
        mock_chunk2.size = 512

        mock_response = Mock()
        mock_response.bucket_name = "my-bucket"
        mock_response.chunks = [mock_chunk1, mock_chunk2]
        self.mock_client.FileDownloadCreate.return_value = mock_response

        result = self.ipc.create_file_download(None, "my-bucket", "file.bin")

        assert isinstance(result, IPCFileDownload)
        assert result.bucket_name == "my-bucket"
        assert len(result.chunks) == 2
        assert result.chunks[0].cid == "bafychunk1"
        assert result.chunks[0].index == 0
        assert result.chunks[1].index == 1

    def test_create_file_download_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.create_file_download(None, "", "file.bin")

    def test_create_file_download_empty_file(self):
        with pytest.raises(SDKError, match="empty file name"):
            self.ipc.create_file_download(None, "bucket", "")

    def test_create_file_download_grpc_failure(self):
        self.mock_client.FileDownloadCreate.side_effect = Exception("connection lost")

        with pytest.raises(SDKError, match="failed to create file download"):
            self.ipc.create_file_download(None, "bucket", "file.bin")


class TestCreateRangeFileDownload:
    """Test create_range_file_download functionality."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

    def test_range_download_success(self):
        mock_chunk = Mock()
        mock_chunk.cid = "bafychunk_range"
        mock_chunk.encoded_size = 1024
        mock_chunk.size = 512

        mock_response = Mock()
        mock_response.bucket_name = "my-bucket"
        mock_response.chunks = [mock_chunk]
        self.mock_client.FileDownloadRangeCreate.return_value = mock_response

        result = self.ipc.create_range_file_download(None, "my-bucket", "file.bin", 2, 5)

        assert isinstance(result, IPCFileDownload)
        assert len(result.chunks) == 1
        # Index should be offset by start (2)
        assert result.chunks[0].index == 2

    def test_range_download_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.create_range_file_download(None, "", "file.bin", 0, 5)

    def test_range_download_empty_file(self):
        with pytest.raises(SDKError, match="empty file name"):
            self.ipc.create_range_file_download(None, "bucket", "", 0, 5)


class TestCreateChunkDownload:
    """Test create_chunk_download functionality."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

    def test_create_chunk_download_success(self):
        mock_block1 = Mock()
        mock_block1.cid = "bafyblock1"
        mock_block1.permit = "permit1"
        mock_block1.node_address = "node1:5500"
        mock_block1.node_id = "12D3node1"

        mock_block2 = Mock()
        mock_block2.cid = "bafyblock2"
        mock_block2.permit = "permit2"
        mock_block2.node_address = "node2:5500"
        mock_block2.node_id = "12D3node2"

        mock_response = Mock()
        mock_response.blocks = [mock_block1, mock_block2]
        self.mock_client.FileDownloadChunkCreate.return_value = mock_response

        chunk = Chunk(cid="bafychunk", encoded_size=2048, size=1024, index=0)
        result = self.ipc.create_chunk_download(None, "bucket", "file.bin", chunk)

        assert isinstance(result, FileChunkDownload)
        assert result.cid == "bafychunk"
        assert len(result.blocks) == 2
        assert isinstance(result.blocks[0], FileBlockDownload)
        assert result.blocks[0].node_address == "node1:5500"
        assert result.blocks[1].cid == "bafyblock2"

    def test_create_chunk_download_failure(self):
        self.mock_client.FileDownloadChunkCreate.side_effect = Exception("timeout")

        chunk = Chunk(cid="bafychunk", encoded_size=2048, size=1024, index=0)
        with pytest.raises(SDKError, match="failed to create chunk download"):
            self.ipc.create_chunk_download(None, "bucket", "file.bin", chunk)


class TestDownloadChunkBlocks:
    """Test download_chunk_blocks functionality."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

    @patch("sdk.dag.extract_block_data")
    def test_download_chunk_blocks_success(self, mock_extract):
        """Download two blocks, reassemble, write to writer."""
        mock_extract.side_effect = [b"block0_data", b"block1_data"]

        pool = Mock()
        mock_node_client = Mock()
        mock_node_client.FileDownloadBlock.return_value = [Mock(data=b"raw_block_data")]
        pool.create_ipc_client.return_value = (mock_node_client, Mock(), None)

        block0 = FileBlockDownload(cid="bafyblock0", data=b"", permit="p0", node_address="node0:5500", node_id="node0")
        block1 = FileBlockDownload(cid="bafyblock1", data=b"", permit="p1", node_address="node1:5500", node_id="node1")
        chunk_download = FileChunkDownload(
            cid="bafychunk", index=0, encoded_size=2048, size=1024, blocks=[block0, block1]
        )

        writer = io.BytesIO()
        self.ipc.download_chunk_blocks(None, pool, "bucket", "file.bin", "0xaddr", chunk_download, b"", writer)

        assert writer.getvalue() == b"block0_datablock1_data"

    @patch("private.encryption.decrypt", return_value=b"decrypted_data")
    @patch("sdk.dag.extract_block_data", return_value=b"encrypted_data")
    def test_download_chunk_blocks_with_decryption(self, mock_extract, mock_decrypt):
        """When file_encryption_key is provided, data should be decrypted."""
        pool = Mock()
        mock_node_client = Mock()
        mock_node_client.FileDownloadBlock.return_value = [Mock(data=b"raw")]
        pool.create_ipc_client.return_value = (mock_node_client, Mock(), None)

        block = FileBlockDownload(cid="bafyblock", data=b"", permit="p", node_address="node:5500", node_id="n")
        chunk_download = FileChunkDownload(cid="bafychunk", index=0, encoded_size=100, size=50, blocks=[block])

        writer = io.BytesIO()
        fake_key = b"a" * 32

        self.ipc.download_chunk_blocks(None, pool, "bucket", "file.bin", "0xaddr", chunk_download, fake_key, writer)
        mock_decrypt.assert_called_once_with(fake_key, b"encrypted_data", b"0")

        assert writer.getvalue() == b"decrypted_data"


class TestFetchBlockData:
    """Test fetch_block_data functionality."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

    def test_fetch_block_data_success(self):
        pool = Mock()
        mock_node_client = Mock()

        part1 = Mock()
        part1.data = b"hello "
        part2 = Mock()
        part2.data = b"world"
        mock_node_client.FileDownloadBlock.return_value = [part1, part2]
        pool.create_ipc_client.return_value = (mock_node_client, Mock(), None)

        block = Mock()
        block.node_address = "node:5500"
        block.cid = "bafyblock"

        result = self.ipc.fetch_block_data(None, pool, "bafychunk", "bucket", "file.bin", "0xaddr", 0, 0, block)
        assert result == b"hello world"

    def test_fetch_block_data_missing_metadata(self):
        pool = Mock()
        block = Mock(spec=[])  # No node_address attribute

        with pytest.raises(SDKError, match="missing block metadata"):
            self.ipc.fetch_block_data(None, pool, "bafychunk", "bucket", "file.bin", "0xaddr", 0, 0, block)

    def test_fetch_block_data_client_creation_failure(self):
        pool = Mock()
        pool.create_ipc_client.return_value = (None, None, Exception("connection failed"))

        block = Mock()
        block.node_address = "bad-node:5500"
        block.cid = "bafyblock"

        with pytest.raises(SDKError, match="failed to create client"):
            self.ipc.fetch_block_data(None, pool, "bafychunk", "bucket", "file.bin", "0xaddr", 0, 0, block)

    def test_fetch_block_data_empty_node_address(self):
        pool = Mock()
        block = Mock()
        block.node_address = ""
        block.cid = "bafyblock"

        with pytest.raises(SDKError, match="missing block metadata"):
            self.ipc.fetch_block_data(None, pool, "bafychunk", "bucket", "file.bin", "0xaddr", 0, 0, block)


class TestDownload:
    """Test the full download flow."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

    @patch.object(IPC, "download_chunk_blocks")
    @patch.object(IPC, "create_chunk_download")
    def test_download_iterates_chunks(self, mock_create_chunk, mock_download_blocks):
        """download() should process each chunk in the file_download."""
        chunk1 = Chunk(cid="c1", encoded_size=100, size=50, index=0)
        chunk2 = Chunk(cid="c2", encoded_size=100, size=50, index=1)
        file_download = IPCFileDownload(bucket_name="bucket", name="file.bin", chunks=[chunk1, chunk2])

        mock_chunk_dl = Mock()
        mock_create_chunk.return_value = mock_chunk_dl

        writer = io.BytesIO()
        self.ipc.download(None, file_download, writer)

        assert mock_create_chunk.call_count == 2
        assert mock_download_blocks.call_count == 2

    @patch.object(IPC, "download_chunk_blocks")
    @patch.object(IPC, "create_chunk_download")
    def test_download_context_cancelled(self, mock_create_chunk, mock_download_blocks):
        """If context is done, download should raise."""
        ctx = Mock()
        ctx.done.return_value = True

        chunk = Chunk(cid="c1", encoded_size=100, size=50, index=0)
        file_download = IPCFileDownload(bucket_name="bucket", name="file.bin", chunks=[chunk])

        writer = io.BytesIO()
        with pytest.raises(SDKError, match="failed to download file"):
            self.ipc.download(ctx, file_download, writer)


# ===================================================================
# Upload flow
# ===================================================================


class TestCreateFileUpload:
    """Test create_file_upload functionality."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

    def test_create_file_upload_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.create_file_upload(None, "", "file.txt")

    def test_create_file_upload_empty_file(self):
        with pytest.raises(SDKError, match="empty file name"):
            self.ipc.create_file_upload(None, "bucket", "")

    def test_create_file_upload_bucket_not_found(self):
        """When view_bucket returns None, should raise."""
        # view_bucket calls BucketView via gRPC
        self.mock_client.BucketView.return_value = None

        with pytest.raises(SDKError, match="failed to create file upload"):
            self.ipc.create_file_upload(None, "missing-bucket", "file.txt")

    def test_create_file_upload_file_already_exists(self):
        """When storage.create_file raises FileAlreadyExists, should propagate."""
        mock_view = Mock()
        mock_view.id = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        mock_view.name = "bucket"
        mock_view.created_at = Mock(seconds=1000)
        self.mock_client.BucketView.return_value = mock_view

        self.mock_ipc.storage.create_file.side_effect = Exception("0x6891dde0 FileAlreadyExists")

        with pytest.raises(SDKError, match="file already exists"):
            self.ipc.create_file_upload(None, "bucket", "existing.txt")

    def test_create_file_upload_success(self):
        mock_view = Mock()
        mock_view.id = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        mock_view.name = "bucket"
        mock_view.created_at = Mock(seconds=1000)
        self.mock_client.BucketView.return_value = mock_view

        self.mock_ipc.storage.create_file.return_value = "0xtx"
        # No wait_for_tx, no web3 attribute
        del self.mock_ipc.wait_for_tx
        del self.mock_ipc.web3

        result = self.ipc.create_file_upload(None, "bucket", "new-file.txt")
        assert isinstance(result, IPCFileUpload)
        assert result.bucket_name == "bucket"
        assert result.name == "new-file.txt"


# ===================================================================
# Encryption helpers
# ===================================================================


class TestEncryptionHelpers:
    """Test encryption_key and maybe_encrypt_metadata."""

    def test_encryption_key_empty_parent(self):
        from sdk.sdk_ipc import encryption_key

        result = encryption_key(b"", "bucket", "file")
        assert result == b""

    def test_encryption_key_with_parent(self):
        from sdk.sdk_ipc import encryption_key

        result = encryption_key(b"secret_parent_key_32bytes_long!!", "bucket", "file")
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_maybe_encrypt_metadata_empty_key(self):
        from sdk.sdk_ipc import maybe_encrypt_metadata

        result = maybe_encrypt_metadata("my-file.txt", "bucket/file", b"")
        assert result == "my-file.txt"

    def test_maybe_encrypt_metadata_with_key(self):
        from sdk.sdk_ipc import maybe_encrypt_metadata

        key = b"a" * 32
        result = maybe_encrypt_metadata("my-file.txt", "bucket/file", key)
        # Should return a hex-encoded encrypted string
        assert result != "my-file.txt"
        # Should be valid hex
        bytes.fromhex(result)


# ===================================================================
# Internal helper methods
# ===================================================================


class TestConvertCidToBytes:
    """Test _convert_cid_to_bytes."""

    def setup_method(self):
        self.ipc = _make_ipc()

    def test_convert_cid_object_with_bytes(self):
        """Objects with __bytes__ should use that method."""

        class FakeCID:
            def __bytes__(self):
                return b"\x01\x02\x03"

        result = self.ipc._convert_cid_to_bytes(FakeCID())
        assert result == b"\x01\x02\x03"


class TestCalculateFileId:
    """Test _calculate_file_id."""

    def setup_method(self):
        self.ipc = _make_ipc()

    def test_calculate_file_id_deterministic(self):
        """Same inputs should always produce the same file ID."""
        bucket_id = b"\x00" * 32
        result1 = self.ipc._calculate_file_id(bucket_id, "file.txt")
        result2 = self.ipc._calculate_file_id(bucket_id, "file.txt")
        assert result1 == result2
        assert len(result1) == 32

    def test_calculate_file_id_different_inputs(self):
        """Different inputs should produce different IDs."""
        bucket_id = b"\x00" * 32
        id1 = self.ipc._calculate_file_id(bucket_id, "file1.txt")
        id2 = self.ipc._calculate_file_id(bucket_id, "file2.txt")
        assert id1 != id2


# ===================================================================
# Error scenarios
# ===================================================================


class TestErrorScenarios:
    """Test various error handling paths."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_ipc = _make_ipc_instance()
        self.config = _make_config()
        self.ipc = _make_ipc(mock_client=self.mock_client, mock_ipc=self.mock_ipc, config=self.config)

    def test_view_bucket_generic_not_found_exception(self):
        """Generic exception with 'not found' should return None."""
        self.mock_client.BucketView.side_effect = Exception("something not found in db")

        result = self.ipc.view_bucket(None, "my-bucket")
        assert result is None

    def test_view_bucket_generic_other_exception(self):
        """Generic exception without 'not found' should raise."""
        self.mock_client.BucketView.side_effect = Exception("disk full")

        with pytest.raises(SDKError, match="failed to get bucket"):
            self.ipc.view_bucket(None, "my-bucket")

    def test_list_buckets_generic_exception(self):
        self.mock_client.BucketList.side_effect = Exception("timeout")

        with pytest.raises(SDKError, match="failed to list buckets"):
            self.ipc.list_buckets(None)

    def test_file_info_generic_exception(self):
        self.mock_client.FileView.side_effect = Exception("network error")

        with pytest.raises(SDKError, match="failed to get file info"):
            self.ipc.file_info(None, "bucket", "file.txt")

    def test_list_files_generic_exception(self):
        self.mock_client.FileList.side_effect = Exception("connection reset")

        with pytest.raises(SDKError, match="failed to list files"):
            self.ipc.list_files(None, "bucket")


# ===================================================================
# IPC constructor
# ===================================================================


class TestIPCInit:
    """Test IPC initialization."""

    def test_default_config_values(self):
        config = _make_config()
        ipc = _make_ipc(config=config)

        assert ipc.max_concurrency == 2
        assert ipc.block_part_size == 1048576
        assert ipc.use_connection_pool is True
        assert ipc.encryption_key == b""

    def test_config_with_encryption_key(self):
        config = _make_config(encryption_key=b"my_secret_key")
        ipc = _make_ipc(config=config)
        assert ipc.encryption_key == b"my_secret_key"

    def test_config_streaming_max_blocks(self):
        config = _make_config(streaming_max_blocks_in_chunk=64)
        ipc = _make_ipc(config=config)
        assert ipc.max_blocks_in_chunk == 64

    def test_custom_with_retry(self):
        mock_retry = Mock()
        ipc = IPC(Mock(), Mock(), _make_ipc_instance(), _make_config(), with_retry=mock_retry)
        assert ipc.with_retry is mock_retry
