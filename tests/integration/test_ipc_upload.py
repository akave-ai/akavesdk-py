#!/usr/bin/env python3
"""Integration test for IPC upload/download with actual blockchain.

This test requires:
  - Network access to connect.akave.ai:5500
  - A valid private key with funds on the Akave subnet

Run with: pytest tests/integration/test_ipc_upload.py -v -m integration
"""

import os
import time

import pytest

from sdk.config import SDKConfig

PRIVATE_KEY = os.environ.get(
    "AKAVE_PRIVATE_KEY",
    "a5c223e956644f1ba11f0dcc6f3df4992184ff3c919223744d0cf1db33dab4d6",
)
NODE_ADDRESS = os.environ.get("AKAVE_NODE_ADDRESS", "connect.akave.ai:5500")
BUCKET_NAME = os.environ.get("AKAVE_TEST_BUCKET", "pytest-ipc-integ")


@pytest.fixture(scope="module")
def sdk_instance():
    """Create a real SDK instance connected to the Akave network."""
    from akavesdk import SDK

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
def ipc_instance(sdk_instance):
    """Create a real IPC instance."""
    return sdk_instance.ipc()


@pytest.fixture(scope="module")
def test_bucket(ipc_instance):
    """Ensure the test bucket exists, creating it if needed."""
    existing = ipc_instance.view_bucket(None, BUCKET_NAME)
    if existing is None:
        result = ipc_instance.create_bucket(None, BUCKET_NAME)
        time.sleep(2)
        return result
    return existing


@pytest.mark.integration
@pytest.mark.network
@pytest.mark.blockchain
class TestIPCUploadIntegration:
    """Integration tests for IPC upload flow with real blockchain."""

    def test_bucket_view(self, ipc_instance, test_bucket):
        """Verify the test bucket can be viewed."""
        bucket = ipc_instance.view_bucket(None, BUCKET_NAME)
        assert bucket is not None
        assert bucket.name == BUCKET_NAME

    def test_bucket_list(self, ipc_instance, test_bucket):
        """Verify list_buckets returns at least our test bucket."""
        buckets = ipc_instance.list_buckets(None)
        assert isinstance(buckets, list)
        names = [b.name for b in buckets]
        assert BUCKET_NAME in names

    def test_upload_small_file(self, ipc_instance, test_bucket, tmp_path):
        """Upload a small file and verify metadata."""
        import io

        timestamp = int(time.time())
        file_name = f"test_small_{timestamp}.bin"
        data = b"Hello Akave SDK integration test!" * 100  # ~3.2KB

        file_meta = ipc_instance.upload(None, BUCKET_NAME, file_name, io.BytesIO(data))

        assert file_meta is not None
        assert file_meta.root_cid
        assert file_meta.size == len(data)

        # Verify via file_info
        info = ipc_instance.file_info(None, BUCKET_NAME, file_name)
        assert info is not None
        assert info.name == file_name

    def test_upload_and_list_file(self, ipc_instance, test_bucket):
        """Upload a file and verify it appears in list_files."""
        import io

        timestamp = int(time.time())
        file_name = f"test_list_{timestamp}.bin"
        data = b"x" * 1024  # 1KB

        ipc_instance.upload(None, BUCKET_NAME, file_name, io.BytesIO(data))

        files = ipc_instance.list_files(None, BUCKET_NAME)
        assert isinstance(files, list)
        found = any(f.name == file_name for f in files)
        assert found, f"Uploaded file '{file_name}' not found in file listing"

    def test_upload_and_download_roundtrip(self, ipc_instance, test_bucket):
        """Upload a file, then download it and verify data integrity."""
        import io

        timestamp = int(time.time())
        file_name = f"test_roundtrip_{timestamp}.bin"
        original_data = bytes(range(256)) * 40  # ~10KB of varied binary data

        # Upload
        ipc_instance.upload(None, BUCKET_NAME, file_name, io.BytesIO(original_data))

        # Download
        file_download = ipc_instance.create_file_download(None, BUCKET_NAME, file_name)
        writer = io.BytesIO()
        ipc_instance.download(None, file_download, writer)

        downloaded_data = writer.getvalue()
        assert downloaded_data == original_data, (
            f"Data mismatch: uploaded {len(original_data)} bytes, " f"downloaded {len(downloaded_data)} bytes"
        )

    def test_file_info_nonexistent(self, ipc_instance, test_bucket):
        """file_info for a nonexistent file should return None."""
        result = ipc_instance.file_info(None, BUCKET_NAME, "nonexistent_file_xyz.bin")
        assert result is None
