"""
Real integration tests for IPC module against the Akave blockchain.

These tests connect to the live Akave network and exercise:
- Bucket operations (create, view, list, delete)
- File operations (upload, info, list, download, delete)
- Access control (set public access)
- Erasure coding (encode, decode, roundtrip)
- Download flows with data integrity verification

Requirements:
  - Network access to connect.akave.ai:5500
  - A valid private key with funds on the Akave subnet

Environment variables (optional overrides):
  AKAVE_PRIVATE_KEY   - hex private key (no 0x prefix)
  AKAVE_NODE_ADDRESS  - node endpoint
"""

import io
import os
import time

import pytest

from akavesdk import SDK, SDKConfig
from sdk.config import SDKError

PRIVATE_KEY = os.environ.get(
    "AKAVE_PRIVATE_KEY",
    "a5c223e956644f1ba11f0dcc6f3df4992184ff3c919223744d0cf1db33dab4d6",
)
NODE_ADDRESS = os.environ.get("AKAVE_NODE_ADDRESS", "connect.akave.ai:5500")

# Unique bucket name per test run to avoid collisions
TIMESTAMP = int(time.time())
BUCKET_NAME = f"ipc-test-{TIMESTAMP}"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sdk_instance():
    """Create a real SDK instance connected to the Akave network."""
    config = SDKConfig(
        address=NODE_ADDRESS,
        private_key=PRIVATE_KEY,
        max_concurrency=5,
        block_part_size=128 * 1024,
        use_connection_pool=True,
        chunk_buffer=10,
    )
    sdk = SDK(config)
    yield sdk
    sdk.close()


@pytest.fixture(scope="module")
def ipc(sdk_instance):
    """Create a real IPC instance."""
    return sdk_instance.ipc()


@pytest.fixture(scope="module")
def bucket(ipc):
    """Create the test bucket once for the entire module."""
    result = ipc.create_bucket(None, BUCKET_NAME)
    time.sleep(2)  # wait for on-chain propagation
    return result


# ===================================================================
# Bucket operations
# ===================================================================


class TestCreateBucket:
    def test_create_bucket_success(self, bucket):
        """Bucket creation should return a valid result with the right name."""
        assert bucket is not None
        assert bucket.name == BUCKET_NAME
        assert bucket.created_at > 0

    def test_create_bucket_invalid_short_name(self, ipc):
        """Bucket name shorter than 3 chars should be rejected."""
        with pytest.raises(SDKError, match="invalid bucket name"):
            ipc.create_bucket(None, "ab")


class TestViewBucket:
    def test_view_bucket_success(self, ipc, bucket):
        """Viewing an existing bucket should return its metadata."""
        result = ipc.view_bucket(None, BUCKET_NAME)
        assert result is not None
        assert result.name == BUCKET_NAME
        assert result.created_at > 0

    def test_view_bucket_empty_name(self, ipc):
        with pytest.raises(SDKError, match="empty bucket name"):
            ipc.view_bucket(None, "")

    def test_view_bucket_nonexistent(self, ipc):
        """Viewing a bucket that doesn't exist should return None."""
        result = ipc.view_bucket(None, f"nonexistent-bucket-{TIMESTAMP}")
        assert result is None


class TestListBuckets:
    def test_list_buckets_contains_ours(self, ipc, bucket):
        """list_buckets should include the bucket we just created."""
        buckets = ipc.list_buckets(None)
        assert isinstance(buckets, list)
        names = [b.name for b in buckets]
        assert BUCKET_NAME in names

    def test_list_buckets_returns_list(self, ipc):
        result = ipc.list_buckets(None)
        assert isinstance(result, list)


# ===================================================================
# File upload & info
# ===================================================================


class TestFileUpload:
    def test_upload_small_file(self, ipc, bucket):
        """Upload a small file and verify metadata comes back."""
        file_name = f"small_{TIMESTAMP}.bin"
        data = b"Hello Akave IPC integration test!" * 100  # ~3.3KB

        meta = ipc.upload(None, BUCKET_NAME, file_name, io.BytesIO(data))

        assert meta is not None
        assert meta.root_cid
        assert meta.size == len(data)
        assert meta.encoded_size > 0

    def test_upload_empty_bucket_name(self, ipc, bucket):
        with pytest.raises(SDKError):
            ipc.upload(None, "", "file.bin", io.BytesIO(b"data"))

    def test_upload_empty_file_name(self, ipc, bucket):
        with pytest.raises(SDKError):
            ipc.upload(None, BUCKET_NAME, "", io.BytesIO(b"data"))


class TestFileInfo:
    def test_file_info_success(self, ipc, bucket):
        """Upload a file, then retrieve its info."""
        file_name = f"info_{TIMESTAMP}.bin"
        data = b"file info test data" * 50

        ipc.upload(None, BUCKET_NAME, file_name, io.BytesIO(data))
        time.sleep(1)

        info = ipc.file_info(None, BUCKET_NAME, file_name)
        assert info is not None
        assert info.name == file_name
        assert info.bucket_name == BUCKET_NAME
        assert info.root_cid != ""
        assert info.actual_size > 0

    def test_file_info_empty_bucket(self, ipc):
        with pytest.raises(SDKError, match="empty bucket name"):
            ipc.file_info(None, "", "file.txt")

    def test_file_info_empty_file(self, ipc):
        with pytest.raises(SDKError, match="empty file name"):
            ipc.file_info(None, "bucket", "")

    def test_file_info_nonexistent(self, ipc, bucket):
        """file_info for a file that doesn't exist should return None."""
        result = ipc.file_info(None, BUCKET_NAME, f"ghost_{TIMESTAMP}.bin")
        assert result is None


class TestListFiles:
    def test_list_files_contains_uploaded(self, ipc, bucket):
        """Upload a file, then verify it appears in list_files."""
        file_name = f"listf_{TIMESTAMP}.bin"
        data = b"list files test" * 100

        ipc.upload(None, BUCKET_NAME, file_name, io.BytesIO(data))
        time.sleep(1)

        files = ipc.list_files(None, BUCKET_NAME)
        assert isinstance(files, list)
        assert any(f.name == file_name for f in files)

    def test_list_files_empty_bucket_name(self, ipc):
        with pytest.raises(SDKError, match="empty bucket name"):
            ipc.list_files(None, "")


# ===================================================================
# Download flow
# ===================================================================


class TestDownloadFlow:
    def test_upload_download_roundtrip(self, ipc, bucket):
        """Upload data, download it back, verify byte-for-byte match."""
        file_name = f"roundtrip_{TIMESTAMP}.bin"
        original_data = bytes(range(256)) * 40  # ~10KB varied binary

        # Upload
        ipc.upload(None, BUCKET_NAME, file_name, io.BytesIO(original_data))
        time.sleep(1)

        # Download
        file_download = ipc.create_file_download(None, BUCKET_NAME, file_name)
        writer = io.BytesIO()
        ipc.download(None, file_download, writer)

        downloaded = writer.getvalue()
        assert (
            downloaded == original_data
        ), f"Data mismatch: uploaded {len(original_data)} bytes, downloaded {len(downloaded)} bytes"

    def test_create_file_download_empty_bucket(self, ipc):
        with pytest.raises(SDKError, match="empty bucket name"):
            ipc.create_file_download(None, "", "file.bin")

    def test_create_file_download_empty_file(self, ipc):
        with pytest.raises(SDKError, match="empty file name"):
            ipc.create_file_download(None, "bucket", "")


# ===================================================================
# File delete
# ===================================================================


class TestFileDelete:
    def test_file_delete_empty_bucket(self, ipc):
        with pytest.raises(SDKError, match="empty bucket or file name"):
            ipc.file_delete(None, "", "file.txt")

    def test_file_delete_empty_filename(self, ipc):
        with pytest.raises(SDKError, match="empty bucket or file name"):
            ipc.file_delete(None, "bucket", "")

    def test_file_delete_whitespace_only(self, ipc):
        with pytest.raises(SDKError, match="empty bucket or file name"):
            ipc.file_delete(None, "   ", "file.txt")

    def test_file_delete_success(self, ipc, bucket):
        """Upload a file, delete it, verify it's gone."""
        file_name = f"todelete_{TIMESTAMP}.bin"
        data = b"delete me" * 100

        ipc.upload(None, BUCKET_NAME, file_name, io.BytesIO(data))
        time.sleep(1)

        # Verify exists
        info = ipc.file_info(None, BUCKET_NAME, file_name)
        assert info is not None

        # Delete
        ipc.file_delete(None, BUCKET_NAME, file_name)
        time.sleep(1)

        # Verify gone
        info_after = ipc.file_info(None, BUCKET_NAME, file_name)
        assert info_after is None


# ===================================================================
# Access control
# ===================================================================


class TestFilePublicAccess:
    def test_set_public_access_empty_bucket(self, ipc):
        with pytest.raises(SDKError, match="empty bucket name"):
            ipc.file_set_public_access(None, "", "file.txt", True)

    def test_set_public_access_empty_file(self, ipc):
        with pytest.raises(SDKError, match="empty file name"):
            ipc.file_set_public_access(None, "bucket", "", True)


# ===================================================================
# Erasure coding (standalone, no network needed)
# ===================================================================


class TestErasureCoding:
    def test_encode_decode_roundtrip(self):
        from private.erasure_code.erasure_code import ErasureCode

        ec = ErasureCode(4, 2)
        data = b"Erasure coding roundtrip test data for IPC"

        encoded = ec.encode(data)
        decoded = ec.extract_data(encoded, len(data))

        assert decoded == data

    def test_recover_missing_blocks(self):
        from private.erasure_code.erasure_code import ErasureCode, split_into_blocks

        ec = ErasureCode(4, 2)
        data = b"Recovery test with missing blocks"

        encoded = ec.encode(data)
        shard_size = len(encoded) // 6
        blocks = split_into_blocks(encoded, shard_size)

        # Lose 2 blocks (within parity tolerance)
        blocks[0] = None
        blocks[3] = None

        recovered = ec.extract_data_blocks(blocks, len(data))
        assert recovered == data

    def test_invalid_init(self):
        from private.erasure_code.erasure_code import ErasureCode

        with pytest.raises(ValueError):
            ErasureCode(0, 2)
        with pytest.raises(ValueError):
            ErasureCode(4, 0)


# ===================================================================
# SDKConfig defaults
# ===================================================================


class TestSDKConfigDefaults:
    def test_defaults_are_sensible(self):
        config = SDKConfig(address="test:5500")
        assert config.max_concurrency == 10
        assert config.block_part_size == 1024 * 1024
        assert config.use_connection_pool is True
        assert config.erasure_code is None
