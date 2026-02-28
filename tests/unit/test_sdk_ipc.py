import pytest
from unittest.mock import Mock, MagicMock, patch, call
import io
from datetime import datetime

from sdk.sdk_ipc import (
    IPC, encryption_key, maybe_encrypt_metadata, to_ipc_proto_chunk, TxWaitSignal
)
from sdk.config import SDKConfig, SDKError
from sdk.model import (
    IPCBucketCreateResult, IPCBucket, IPCFileMeta, IPCFileUpload,
    IPCFileDownload, FileBlockUpload, Chunk
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
    
    @patch('sdk.sdk_ipc.derive_key')
    def test_encryption_key_with_data(self, mock_derive):
        parent = b"parent_key_32bytes_test123456789"
        mock_derive.return_value = b"derived"
        
        result = encryption_key(parent, "bucket", "file")
        
        assert result == b"derived"
        mock_derive.assert_called_once_with(parent, b"bucket/file")
    
    @patch('sdk.sdk_ipc.derive_key')
    def test_encryption_key_multiple_info(self, mock_derive):
        parent = b"key"
        mock_derive.return_value = b"result"
        
        result = encryption_key(parent, "a", "b", "c")
        
        mock_derive.assert_called_once_with(parent, b"a/b/c")


class TestMaybeEncryptMetadata:
    
    def test_maybe_encrypt_metadata_no_key(self):
        result = maybe_encrypt_metadata("plain_value", "path", b"")
        assert result == "plain_value"
    
    @patch('sdk.sdk_ipc.derive_key')
    @patch('sdk.sdk_ipc.encrypt')
    def test_maybe_encrypt_metadata_with_key(self, mock_encrypt, mock_derive):
        key = b"encryption_key_32bytes_test12345"
        mock_derive.return_value = b"file_key"
        mock_encrypt.return_value = b"\x01\x02\x03"
        
        result = maybe_encrypt_metadata("value", "path/to/file", key)
        
        assert result == "010203"
        mock_derive.assert_called_once_with(key, b"path/to/file")
        mock_encrypt.assert_called_once_with(b"file_key", b"value", b"metadata")
    
    @patch('sdk.sdk_ipc.derive_key')
    @patch('sdk.sdk_ipc.encrypt')
    def test_maybe_encrypt_metadata_error(self, mock_encrypt, mock_derive):
        key = b"encryption_key_32bytes_test12345"
        mock_derive.side_effect = Exception("Derive failed")
        
        with pytest.raises(SDKError, match="failed to encrypt metadata"):
            maybe_encrypt_metadata("value", "path", key)


class TestToIPCProtoChunk:
    
    @patch('sdk.sdk_ipc.ipcnodeapi_pb2')
    def test_to_ipc_proto_chunk_basic(self, mock_pb2):
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
            block_part_size=128*1024,
            streaming_max_blocks_in_chunk=10
        )
        
        ipc = IPC(mock_client, mock_conn, mock_ipc_instance, config)
        
        assert ipc.client == mock_client
        assert ipc.conn == mock_conn
        assert ipc.ipc == mock_ipc_instance
        assert ipc.max_concurrency == 5
        assert ipc.block_part_size == 128*1024
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
    
    def test_create_bucket_transaction_failed(self):
        mock_receipt = Mock()
        mock_receipt.status = 0
        
        self.mock_ipc.storage.create_bucket.return_value = "0xtx"
        self.mock_ipc.eth.eth.wait_for_transaction_receipt.return_value = mock_receipt
        
        with pytest.raises(SDKError, match="bucket creation transaction failed"):
            self.ipc.create_bucket(None, "test-bucket")


class TestViewBucket:
    
    def setup_method(self):
        self.mock_client = Mock()
        self.mock_conn = Mock()
        self.mock_ipc = Mock()
        self.mock_ipc.auth = Mock()
        self.mock_ipc.auth.address = "0x123"
        
        self.config = SDKConfig(address="test:5500")
        self.ipc = IPC(self.mock_client, self.mock_conn, self.mock_ipc, self.config)
    
    def test_view_bucket_empty_name(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.view_bucket(None, "")
    
    @patch('sdk.sdk_ipc.ipcnodeapi_pb2')
    def test_view_bucket_success(self, mock_pb2):
        mock_response = Mock()
        mock_response.id = "bucket_id"
        mock_response.name = "test-bucket"
        mock_response.created_at = Mock()
        mock_response.created_at.seconds = 1234567890
        
        self.mock_client.BucketView.return_value = mock_response
        
        result = self.ipc.view_bucket(None, "test-bucket")
        
        assert isinstance(result, IPCBucket)
        assert result.name == "test-bucket"
    
    @patch('sdk.sdk_ipc.ipcnodeapi_pb2')
    def test_view_bucket_not_found(self, mock_pb2):
        self.mock_client.BucketView.return_value = None
        
        result = self.ipc.view_bucket(None, "nonexistent")
        
        assert result is None


class TestListBuckets:
    
    def setup_method(self):
        self.mock_client = Mock()
        self.mock_conn = Mock()
        self.mock_ipc = Mock()
        self.mock_ipc.auth = Mock()
        self.mock_ipc.auth.address = "0x123"
        
        self.config = SDKConfig(address="test:5500")
        self.ipc = IPC(self.mock_client, self.mock_conn, self.mock_ipc, self.config)
    
    @patch('sdk.sdk_ipc.ipcnodeapi_pb2')
    def test_list_buckets_success(self, mock_pb2):
        mock_bucket1 = Mock()
        mock_bucket1.id = "id1"
        mock_bucket1.name = "bucket1"
        mock_bucket1.created_at = Mock()
        mock_bucket1.created_at.seconds = 100
        
        mock_bucket2 = Mock()
        mock_bucket2.id = "id2"
        mock_bucket2.name = "bucket2"
        mock_bucket2.created_at = Mock()
        mock_bucket2.created_at.seconds = 200
        
        mock_response = Mock()
        mock_response.buckets = [mock_bucket1, mock_bucket2]
        
        self.mock_client.BucketList.return_value = mock_response
        
        result = self.ipc.list_buckets(None)
        
        assert isinstance(result, list)
        assert len(result) == 2


class TestFileInfo:
    
    def setup_method(self):
        self.mock_client = Mock()
        self.mock_conn = Mock()
        self.mock_ipc = Mock()
        self.mock_ipc.auth = Mock()
        self.mock_ipc.auth.address = "0x123"
        
        self.config = SDKConfig(address="test:5500")
        self.ipc = IPC(self.mock_client, self.mock_conn, self.mock_ipc, self.config)
    
    def test_file_info_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.file_info(None, "", "file.txt")
    
    def test_file_info_empty_filename(self):
        with pytest.raises(SDKError, match="empty file name"):
            self.ipc.file_info(None, "bucket", "")
    
    @patch('sdk.sdk_ipc.ipcnodeapi_pb2')
    def test_file_info_success(self, mock_pb2):
        mock_response = Mock()
        mock_response.root_cid = "root_cid"
        mock_response.name = "file.txt"
        mock_response.size = 1024
        mock_response.encoded_size = 2048
        mock_response.created_at = Mock()
        mock_response.created_at.seconds = 1234567890
        
        self.mock_client.FileView.return_value = mock_response
        
        result = self.ipc.file_info(None, "bucket", "file.txt")
        
        assert isinstance(result, IPCFileMeta)
        assert result.name == "file.txt"
        assert result.size == 1024


class TestListFiles:
    
    def setup_method(self):
        self.mock_client = Mock()
        self.mock_conn = Mock()
        self.mock_ipc = Mock()
        self.mock_ipc.auth = Mock()
        self.mock_ipc.auth.address = "0x123"
        
        self.config = SDKConfig(address="test:5500")
        self.ipc = IPC(self.mock_client, self.mock_conn, self.mock_ipc, self.config)
    
    def test_list_files_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.list_files(None, "")
    
    @patch('sdk.sdk_ipc.ipcnodeapi_pb2')
    def test_list_files_success(self, mock_pb2):
        mock_file = Mock()
        mock_file.root_cid = "cid"
        mock_file.name = "file1.txt"
        mock_file.size = 512
        mock_file.encoded_size = 1024
        mock_file.created_at = Mock()
        mock_file.created_at.seconds = 100
        
        mock_response = Mock()
        mock_response.files = [mock_file]
        
        self.mock_client.FileList.return_value = mock_response
        
        result = self.ipc.list_files(None, "bucket")
        
        assert isinstance(result, list)
        assert len(result) == 1


class TestDeleteBucket:
    
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
        
        self.config = SDKConfig(address="test:5500")
        self.ipc = IPC(self.mock_client, self.mock_conn, self.mock_ipc, self.config)
    
    def test_delete_bucket_empty_name(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.delete_bucket(None, "")
    
    def test_delete_bucket_success(self):
        mock_receipt = Mock()
        mock_receipt.status = 1
        
        self.mock_ipc.storage.delete_bucket.return_value = "0xtx"
        self.mock_ipc.eth.eth.wait_for_transaction_receipt.return_value = mock_receipt
        
        self.ipc.delete_bucket(None, "test-bucket")
        
        self.mock_ipc.storage.delete_bucket.assert_called_once()


class TestCreateFileUpload:
    
    def setup_method(self):
        self.mock_client = Mock()
        self.mock_conn = Mock()
        self.mock_ipc = Mock()
        self.config = SDKConfig(address="test:5500")
        self.ipc = IPC(self.mock_client, self.mock_conn, self.mock_ipc, self.config)
    
    def test_create_file_upload_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.create_file_upload(None, "", "file.txt")
    
    def test_create_file_upload_empty_filename(self):
        with pytest.raises(SDKError, match="empty file name"):
            self.ipc.create_file_upload(None, "bucket", "")
    
    @patch('sdk.sdk_ipc.new_ipc_file_upload')
    def test_create_file_upload_success(self, mock_new_upload):
        mock_upload = Mock()
        mock_new_upload.return_value = mock_upload
        
        result = self.ipc.create_file_upload(None, "bucket", "file.txt")
        
        assert result == mock_upload
        mock_new_upload.assert_called_once_with("bucket", "file.txt")


class TestFileDelete:
    
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
            streaming_max_blocks_in_chunk=10
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
        self.mock_ipc.storage.get_file_index_by_id.return_value = 2  # mock returning index 2
        
        self.mock_ipc.storage.delete_file.return_value = "0xtx"
        self.mock_ipc.eth.eth.wait_for_transaction_receipt.return_value = mock_receipt
        
        self.ipc.file_delete(None, "bucket", "file.txt")
        
        self.mock_ipc.storage.delete_file.assert_called_once()


class TestCreateFileDownload:
    
    def setup_method(self):
        self.mock_client = Mock()
        self.mock_conn = Mock()
        self.mock_ipc = Mock()
        self.config = SDKConfig(address="test:5500")
        self.ipc = IPC(self.mock_client, self.mock_conn, self.mock_ipc, self.config)
    
    def test_create_file_download_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.create_file_download(None, "", "file.txt")
    
    def test_create_file_download_empty_filename(self):
        with pytest.raises(SDKError, match="empty file name"):
            self.ipc.create_file_download(None, "bucket", "")
    
    @patch('sdk.sdk_ipc.ipcnodeapi_pb2')
    def test_create_file_download_success(self, mock_pb2):
        mock_chunk = Mock()
        mock_chunk.cid = "chunk_cid"
        mock_chunk.size = 1024
        mock_chunk.encoded_size = 2048
        
        mock_response = Mock()
        mock_response.bucket_name = "bucket"
        mock_response.file_name = "file.txt"
        mock_response.chunks = [mock_chunk]
        
        self.mock_client.FileDownloadCreate.return_value = mock_response
        
        result = self.ipc.create_file_download(None, "bucket", "file.txt")
        
        assert isinstance(result, IPCFileDownload)
        assert result.bucket_name == "bucket"
        assert result.name == "file.txt"


class TestHelperMethods:
    
    def setup_method(self):
        self.mock_client = Mock()
        self.mock_conn = Mock()
        self.mock_ipc = Mock()
        self.config = SDKConfig(address="test:5500")
        self.ipc = IPC(self.mock_client, self.mock_conn, self.mock_ipc, self.config)
    
    def test_validate_bucket_name_valid(self):
        self.ipc._validate_bucket_name("valid-bucket")
    
    def test_validate_bucket_name_empty(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc._validate_bucket_name("")
    
    def test_validate_bucket_name_too_short(self):
        with pytest.raises(SDKError, match="invalid bucket name"):
            self.ipc._validate_bucket_name("ab")


@pytest.mark.integration
class TestIPCIntegration:
    
    def test_ipc_full_lifecycle(self):
        mock_client = Mock()
        mock_conn = Mock()
        mock_ipc = Mock()
        mock_ipc.auth = Mock()
        mock_ipc.auth.address = "0x123"
        
        config = SDKConfig(address="test:5500")
        
        ipc = IPC(mock_client, mock_conn, mock_ipc, config)
        
        assert ipc.client == mock_client
        assert ipc.ipc == mock_ipc
    
    @patch('sdk.sdk_ipc.ipcnodeapi_pb2')
    def test_bucket_workflow(self, mock_pb2):
        mock_client = Mock()
        mock_conn = Mock()
        mock_ipc = Mock()
        mock_ipc.auth = Mock()
        mock_ipc.auth.address = "0x123"
        mock_ipc.auth.key = "key"
        mock_ipc.storage = Mock()
        mock_ipc.eth = Mock()
        mock_ipc.eth.eth = Mock()
        
        config = SDKConfig(address="test:5500")
        ipc = IPC(mock_client, mock_conn, mock_ipc, config)
        
        mock_receipt = Mock()
        mock_receipt.status = 1
        mock_receipt.blockNumber = 100
        mock_receipt.transactionHash = Mock()
        mock_receipt.transactionHash.hex.return_value = "0xabc"
        
        mock_block = Mock()
        mock_block.timestamp = 1234567890
        
        mock_ipc.storage.create_bucket.return_value = "0xtx"
        mock_ipc.eth.eth.wait_for_transaction_receipt.return_value = mock_receipt
        mock_ipc.eth.eth.get_block.return_value = mock_block
        
        result = ipc.create_bucket(None, "test-bucket")
        
        assert isinstance(result, IPCBucketCreateResult)
        
        mock_view_response = Mock()
        mock_view_response.id = "id"
        mock_view_response.name = "test-bucket"
        mock_view_response.created_at = Mock()
        mock_view_response.created_at.seconds = 1234567890
        
        mock_client.BucketView.return_value = mock_view_response
        
        bucket = ipc.view_bucket(None, "test-bucket")
        
        assert bucket.name == "test-bucket"

