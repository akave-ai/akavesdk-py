#!/usr/bin/env python3
"""Integration tests for IPC module against the live Akave blockchain.

These tests connect to the real Akave network and exercise:
- Bucket operations (create, view, list)
- File operations (upload, info, list, download, delete)
- Access control validation
- Download roundtrip with data integrity verification

Requirements:
  - Network access to connect.akave.ai:5500
  - A valid private key with funds on the Akave subnet

Run with: pytest tests/integration/test_ipc_upload.py -v -m integration
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

TIMESTAMP = int(time.time())
BUCKET_NAME = f"ipc-integ-{TIMESTAMP}"


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
    time.sleep(2)
    return result


@pytest.mark.integration
@pytest.mark.network
@pytest.mark.blockchain
class TestBucketOps:
    def test_create_bucket_success(self, bucket):
        assert bucket is not None
        assert bucket.name == BUCKET_NAME
        assert bucket.created_at > 0

    def test_create_bucket_invalid_short_name(self, ipc):
        with pytest.raises(SDKError, match="invalid bucket name"):
            ipc.create_bucket(None, "ab")

    def test_view_bucket_success(self, ipc, bucket):
        result = ipc.view_bucket(None, BUCKET_NAME)
        assert result is not None
        assert result.name == BUCKET_NAME

    def test_view_bucket_nonexistent(self, ipc):
        result = ipc.view_bucket(None, f"ghost-bucket-{TIMESTAMP}")
        assert result is None

    def test_list_buckets_contains_ours(self, ipc, bucket):
        buckets = ipc.list_buckets(None)
        assert isinstance(buckets, list)
        names = [b.name for b in buckets]
        assert BUCKET_NAME in names


@pytest.mark.integration
@pytest.mark.network
@pytest.mark.blockchain
class TestFileOps:
    def test_upload_small_file(self, ipc, bucket):
        file_name = f"small_{TIMESTAMP}.bin"
        data = b"Hello Akave IPC integration test!" * 100

        meta = ipc.upload(None, BUCKET_NAME, file_name, io.BytesIO(data))

        assert meta is not None
        assert meta.root_cid
        assert meta.size == len(data)

    def test_file_info_success(self, ipc, bucket):
        file_name = f"info_{TIMESTAMP}.bin"
        data = b"file info test data" * 50

        ipc.upload(None, BUCKET_NAME, file_name, io.BytesIO(data))
        time.sleep(1)

        info = ipc.file_info(None, BUCKET_NAME, file_name)
        assert info is not None
        assert info.name == file_name
        assert info.actual_size > 0

    def test_file_info_nonexistent(self, ipc, bucket):
        result = ipc.file_info(None, BUCKET_NAME, f"ghost_{TIMESTAMP}.bin")
        assert result is None

    def test_list_files_contains_uploaded(self, ipc, bucket):
        file_name = f"listf_{TIMESTAMP}.bin"
        data = b"list files test" * 100

        ipc.upload(None, BUCKET_NAME, file_name, io.BytesIO(data))
        time.sleep(1)

        files = ipc.list_files(None, BUCKET_NAME)
        assert isinstance(files, list)
        assert any(f.name == file_name for f in files)

    def test_upload_download_roundtrip(self, ipc, bucket):
        file_name = f"roundtrip_{TIMESTAMP}.bin"
        original_data = bytes(range(256)) * 40

        ipc.upload(None, BUCKET_NAME, file_name, io.BytesIO(original_data))
        time.sleep(1)

        file_download = ipc.create_file_download(None, BUCKET_NAME, file_name)
        writer = io.BytesIO()
        ipc.download(None, file_download, writer)

        assert writer.getvalue() == original_data

    def test_file_delete_success(self, ipc, bucket):
        file_name = f"todelete_{TIMESTAMP}.bin"
        data = b"delete me" * 100

        ipc.upload(None, BUCKET_NAME, file_name, io.BytesIO(data))
        time.sleep(1)

        info = ipc.file_info(None, BUCKET_NAME, file_name)
        assert info is not None

        ipc.file_delete(None, BUCKET_NAME, file_name)
        time.sleep(1)

        info_after = ipc.file_info(None, BUCKET_NAME, file_name)
        assert info_after is None
