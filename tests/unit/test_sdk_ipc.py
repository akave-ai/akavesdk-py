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
    
    @patch('sdk.sdk_ipc.CID')
    @patch('sdk.sdk_ipc.ipcnodeapi_pb2')
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
            block_part_size=128*1024,
            use_connection_pool=True,
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
        mock_response.file_name = "file.txt"
        mock_response.actual_size = 1024
        mock_response.encoded_size = 2048
        mock_response.created_at = Mock()
        mock_response.created_at.seconds = 1234567890
        
        self.mock_client.FileView.return_value = mock_response
        
        result = self.ipc.file_info(None, "bucket", "file.txt")
        
        assert isinstance(result, IPCFileMeta)
        assert result.name == "file.txt"
        assert result.actual_size == 1024


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
        mock_response.list = [mock_file]
        
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
        self.mock_ipc.auth = Mock()
        self.mock_ipc.auth.address = "0x123"
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
        mock_upload.state.encoded_file_size = 1000
        mock_upload.state.actual_file_size = 500
        mock_new_upload.return_value = mock_upload
        
        # Mock view_bucket response
        mock_bucket_response = Mock()
        mock_bucket_response.id = "0x1234"
        mock_bucket_response.name = "bucket"
        mock_bucket_response.created_at = Mock()
        mock_bucket_response.created_at.seconds = 123
        self.mock_client.BucketView.return_value = mock_bucket_response
        
        # Mock storage create_file
        self.mock_ipc.storage.create_file.return_value = "0xtx"
        
        # Mock wait for tx
        mock_receipt = Mock()
        mock_receipt.status = 1
        self.mock_ipc.eth.eth.wait_for_transaction_receipt.return_value = mock_receipt
        
        result = self.ipc.create_file_upload(None, "bucket", "file.txt")
        
        assert result == mock_upload
        mock_new_upload.assert_called_once_with("bucket", "file.txt")
        self.mock_ipc.storage.create_file.assert_called_once()


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
        
        self.config = SDKConfig(address="test:5500")
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
        
        # Mock bucket lookup
        self.mock_ipc.storage.get_bucket_by_name.return_value = (b"bucket_id_bytes", "bucket_name")
        
        # Mock file lookup
        self.mock_ipc.storage.get_file_by_name.return_value = (b"file_id_bytes",)
        
        self.mock_ipc.storage.delete_file.return_value = "0xtx"
        self.mock_ipc.eth.eth.wait_for_transaction_receipt.return_value = mock_receipt
        
        self.ipc.file_delete(None, "bucket", "file.txt")
        
        self.mock_ipc.storage.delete_file.assert_called_once()


class TestCreateFileDownload:
    
    def setup_method(self):
        self.mock_client = Mock()
        self.mock_conn = Mock()
        self.mock_ipc = Mock()
        self.mock_ipc.auth = Mock()
        self.mock_ipc.auth.address = "0x123"
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




@pytest.mark.integration
class TestIPCIntegration:
    
    def test_ipc_full_lifecycle(self):
        mock_client = Mock()
        mock_conn = Mock()
        mock_ipc = Mock()
        mock_ipc.auth = Mock()
        mock_ipc.auth.address = "0x123"
        
        config = SDKConfig(
            address="test:5500",
            max_concurrency=10,
            block_part_size=1024,
            use_connection_pool=False
        )
        
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
        
        config = SDKConfig(
            address="test:5500",
            max_concurrency=10,
            block_part_size=1024,
            use_connection_pool=False
        )
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


class TestFilePublicAccess:
    
    def setup_method(self):
        self.mock_client = Mock()
        self.mock_conn = Mock()
        self.mock_ipc = Mock()
        self.mock_ipc.auth = Mock()
        self.mock_ipc.auth.address = "0x1234" # Even length address
        self.mock_ipc.storage = Mock()
        self.mock_ipc.access_manager = Mock()
        self.mock_ipc.eth = Mock()
        self.mock_ipc.eth.eth = Mock()
        
        self.config = SDKConfig(address="test:5500")
        self.ipc = IPC(self.mock_client, self.mock_conn, self.mock_ipc, self.config)
    
    def test_set_public_access_empty_bucket(self):
        with pytest.raises(SDKError, match="empty bucket name"):
            self.ipc.file_set_public_access(None, "", "file.txt", True)
            
    def test_set_public_access_empty_file(self):
        with pytest.raises(SDKError, match="empty file name"):
            self.ipc.file_set_public_access(None, "bucket", "", True)
            
    def test_set_public_access_bucket_not_found(self):
        # view_bucket returns None
        self.mock_client.BucketView.return_value = None
        
        with pytest.raises(SDKError, match="bucket 'bucket' not found"):
            self.ipc.file_set_public_access(None, "bucket", "file.txt", True)
            
    def test_set_public_access_file_not_found(self):
        # Bucket exists
        mock_bucket_response = Mock()
        mock_bucket_response.id = "0x1234" # Even length
        mock_bucket_response.created_at = Mock()
        mock_bucket_response.created_at.seconds = 123
        self.mock_client.BucketView.return_value = mock_bucket_response
        
        # File not found in storage contract
        self.mock_ipc.storage.get_file_by_name.side_effect = Exception("file not found")
        
        with pytest.raises(SDKError, match="failed to get file"):
            self.ipc.file_set_public_access(None, "bucket", "file.txt", True)

    def test_set_public_access_no_manager(self):
        # Remove access manager
        self.ipc.ipc.access_manager = None
        
        # Bucket exists
        mock_bucket_response = Mock()
        mock_bucket_response.id = "0x1234" # Even length
        mock_bucket_response.created_at = Mock()
        mock_bucket_response.created_at.seconds = 123
        self.mock_client.BucketView.return_value = mock_bucket_response
        
        # File exists
        self.mock_ipc.storage.get_file_by_name.return_value = ["file_id"]
        
        with pytest.raises(SDKError, match="access manager not available"):
            self.ipc.file_set_public_access(None, "bucket", "file.txt", True)
            
    def test_set_public_access_success(self):
        # Bucket exists
        mock_bucket_response = Mock()
        mock_bucket_response.id = "0x1234" # Even length
        mock_bucket_response.created_at = Mock()
        mock_bucket_response.created_at.seconds = 123
        self.mock_client.BucketView.return_value = mock_bucket_response
        
        # File exists in storage contract
        self.mock_ipc.storage.get_file_by_name.return_value = ["file_id"]
        
        # Transaction succeeds
        self.mock_ipc.access_manager.change_public_access.return_value = "0xtx"
        mock_receipt = Mock()
        mock_receipt.status = 1
        self.mock_ipc.eth.eth.wait_for_transaction_receipt.return_value = mock_receipt
        
        self.ipc.file_set_public_access(None, "bucket", "file.txt", True)
        
        self.mock_ipc.access_manager.change_public_access.assert_called_once_with(
            self.mock_ipc.auth, "file_id", True
        )


class TestDownload:
    
    def setup_method(self):
        self.mock_client = Mock()
        self.mock_conn = Mock()
        self.mock_ipc = Mock()
        self.mock_ipc.auth = Mock()
        self.mock_ipc.auth.address = "0x123"
        
        self.config = SDKConfig(address="test:5500")
        self.ipc = IPC(self.mock_client, self.mock_conn, self.mock_ipc, self.config)

    @patch('sdk.sdk_ipc.ipcnodeapi_pb2')
    def test_create_chunk_download_success(self, mock_pb2):
        chunk = Mock()
        chunk.cid = "chunk_cid"
        chunk.index = 0
        chunk.size = 100
        chunk.encoded_size = 120

        mock_response = Mock()
        mock_block = Mock()
        mock_block.cid = "block_cid"
        mock_block.node_id = "node_id"
        mock_block.node_address = "node_addr"
        mock_block.permit = "permit"
        mock_response.blocks = [mock_block]

        self.mock_client.FileDownloadChunkCreate.return_value = mock_response

        result = self.ipc.create_chunk_download(None, "bucket", "file", chunk)

        self.mock_client.FileDownloadChunkCreate.assert_called_once()
        assert result.cid == "chunk_cid"
        assert len(result.blocks) == 1
        assert result.blocks[0].cid == "block_cid"

    @patch('sdk.sdk_ipc.extract_block_data')
    @patch('sdk.sdk_ipc.ConnectionPool')
    def test_download_chunk_blocks_success(self, mock_pool_cls, mock_extract):
        mock_pool = Mock()
        mock_pool_cls.return_value = mock_pool
        
        # Mock client creation
        mock_client = Mock()
        mock_pool.create_ipc_client.return_value = (mock_client, Mock(), None)
        
        # Mock stream response
        mock_stream_item = Mock()
        mock_stream_item.data = b"block_data"
        mock_client.FileDownloadBlock.return_value = [mock_stream_item]

        # Mock chunk download object
        chunk_download = Mock()
        chunk_download.cid = "chunk_cid"
        chunk_download.index = 0
        chunk_download.size = 10
        
        block = Mock()
        block.cid = "block_cid"
        block.akave.node_address = "addr"
        chunk_download.blocks = [block]

        writer = io.BytesIO()
        mock_extract.return_value = b"block_data"

        self.ipc.download_chunk_blocks(None, "bucket", "file", "addr", chunk_download, None, writer)

        assert writer.getvalue() == b"block_data"

    def test_download_success(self):
        # Mock create_file_download
        chunk = Mock()
        chunk.cid = "chunk_cid"
        
        file_download = Mock()
        file_download.bucket_name = "bucket"
        file_download.name = "file"
        file_download.chunks = [chunk]

        writer = io.BytesIO()

        # Mock internal methods
        self.ipc.create_chunk_download = Mock()
        
        # Mock download_chunk_blocks to write to writer
        def side_effect(*args):
            args[-1].write(b"content")
            return None
        self.ipc.download_chunk_blocks = Mock(side_effect=side_effect)

        self.ipc.download(None, file_download, writer)

        assert writer.getvalue() == b"content"
        self.ipc.create_chunk_download.assert_called_once()
        self.ipc.download_chunk_blocks.assert_called_once()

    @patch('sdk.sdk_ipc.ipcnodeapi_pb2')
    def test_create_range_file_download(self, mock_pb2):
        mock_response = Mock()
        mock_response.bucket_name = "bucket"
        mock_response.chunks = []
        
        self.mock_client.FileDownloadRangeCreate.return_value = mock_response

        result = self.ipc.create_range_file_download(None, "bucket", "file", 0, 10)
        
        self.mock_client.FileDownloadRangeCreate.assert_called_once()
        assert result.bucket_name == "bucket"


class TestUploadLogic:

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_conn = Mock()
        self.mock_ipc = Mock()
        self.mock_ipc.auth = Mock()
        self.mock_ipc.auth.address = "0x123"
        self.mock_ipc.auth.key = b"key"
        self.mock_ipc.storage = Mock()
        self.mock_ipc.eth = Mock()
        self.mock_ipc.eth.eth = Mock()
        
        self.config = SDKConfig(address="test:5500", max_concurrency=1)
        self.ipc = IPC(self.mock_client, self.mock_conn, self.mock_ipc, self.config)

    @patch('sdk.sdk_ipc.to_ipc_proto_chunk')
    @patch('sdk.sdk_ipc.ConnectionPool')
    def test_upload_chunk_success(self, mock_pool_cls, mock_to_proto):
        mock_pool = Mock()
        mock_pool_cls.return_value = mock_pool
        mock_pool.close.return_value = None
        
        mock_client = Mock()
        mock_closer = Mock()
        mock_pool.create_ipc_client.return_value = (mock_client, mock_closer, None)

        # Mock proto chunk
        mock_proto = Mock()
        mock_proto.cid = "chunk_cid"
        mock_proto.index = 0
        mock_to_proto.return_value = ([], [], mock_proto, None)

        chunk_upload = Mock()
        chunk_upload.chunk_cid = "chunk_cid"
        chunk_upload.bucket_id = b"bucket_id_32bytes_1234567890ab"
        chunk_upload.file_name = "file"
        chunk_upload.actual_size = 100
        chunk_upload.index = 0
        
        # Mock blocks with proper data attribute
        block_mock = Mock()
        block_mock.cid = "block_cid"
        block_mock.data = b"test_data"
        block_mock.node_address = "addr"
        block_mock.node_id = "node_id"
        chunk_upload.blocks = [block_mock]
        
        # Mock the signature creation to avoid complex EIP712 logic
        with patch.object(self.ipc, '_create_storage_signature', return_value=("0xsig", b"nonce")):
            self.ipc.upload_chunk(None, chunk_upload)
        
        mock_pool.create_ipc_client.assert_called()
        mock_client.FileUploadBlock.assert_called()

    @patch('sdk.sdk_ipc.derive_key')
    @patch('sdk.sdk_ipc.encrypt')
    def test_upload_with_encryption(self, mock_encrypt, mock_derive):
        # Setup for upload
        self.ipc.encryption_key = b"root_key"
        mock_derive.return_value = b"derived_key"
        mock_encrypt.return_value = b"encrypted"

        # Mock dependent methods to bypass full flow
        self.ipc.create_file_upload = Mock()
        file_upload = Mock()
        file_upload.name = "file"
        file_upload.bucket_name = "bucket"
        file_upload.state = Mock()
        file_upload.state.chunk_count = 0
        file_upload.state.actual_file_size = 0
        self.ipc.create_file_upload.return_value = file_upload
        
        self.ipc.ipc.storage.get_bucket_by_name.return_value = [b"bucket_id"]
        
        with patch.object(self.ipc, '_upload_with_comprehensive_debug') as mock_inner:
            reader = io.BytesIO(b"data")
            self.ipc.upload(None, "bucket", "file", reader)
            
            mock_derive.assert_called()
            mock_inner.assert_called()
            args, _ = mock_inner.call_args
            assert args[6] == b"derived_key"


class TestErasureCoding:

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_conn = Mock()
        self.mock_ipc = Mock()
        self.mock_ipc.auth = Mock()
        self.mock_ipc.auth.address = "0x123"
        self.mock_ipc.auth.key = b"key"
        self.mock_ipc.storage = Mock()
        
        self.config = SDKConfig(address="test:5500")
        self.ipc = IPC(self.mock_client, self.mock_conn, self.mock_ipc, self.config)

    @patch('sdk.sdk_ipc.extract_block_data')
    @patch('sdk.sdk_ipc.ConnectionPool')
    def test_download_with_erasure_coding(self, mock_pool_cls, mock_extract):
        from sdk.erasure_code import ErasureCode
        self.ipc.erasure_code = ErasureCode(data_blocks=4, parity_blocks=2)
        
        chunk_download = Mock()
        chunk_download.cid = "chunk_cid"
        chunk_download.index = 0
        chunk_download.size = 100
        chunk_download.blocks = [Mock(cid="b1", akave=Mock(node_address="addr"))]
        
        mock_pool = Mock()
        mock_pool_cls.return_value = mock_pool
        mock_client = Mock()
        mock_stream = [Mock(data=b"block_data")]
        mock_client.FileDownloadBlock.return_value = mock_stream
        mock_pool.create_ipc_client.return_value = (mock_client, Mock(), None)
        
        mock_extract.return_value = b"block_data"
        
        writer = io.BytesIO()
        
        with patch.object(self.ipc.erasure_code, 'extract_data_blocks', return_value=b"decoded_data"):
            self.ipc.download_chunk_blocks(None, "bucket", "file", "addr", chunk_download, None, writer)
            
            self.ipc.erasure_code.extract_data_blocks.assert_called_once()
            assert writer.getvalue() == b"decoded_data"


class TestErrorScenarios:

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

    def test_view_bucket_grpc_not_found(self):
        import grpc
        error = grpc.RpcError()
        error.code = Mock(return_value=grpc.StatusCode.NOT_FOUND)
        error.details = Mock(return_value="not found")
        self.mock_client.BucketView.side_effect = error
        
        result = self.ipc.view_bucket(None, "nonexistent")
        assert result is None

    def test_create_bucket_transaction_reverted(self):
        mock_receipt = Mock()
        mock_receipt.status = 0
        self.mock_ipc.storage.create_bucket.return_value = "0xtx"
        self.mock_ipc.eth.eth.wait_for_transaction_receipt.return_value = mock_receipt
        
        with pytest.raises(SDKError, match="transaction failed"):
            self.ipc.create_bucket(None, "test-bucket")

    def test_file_info_grpc_unavailable(self):
        import grpc
        error = grpc.RpcError()
        error.code = Mock(return_value=grpc.StatusCode.UNAVAILABLE)
        error.details = Mock(return_value="service unavailable")
        self.mock_client.FileView.side_effect = error
        
        with pytest.raises(SDKError, match="failed to get file info"):
            self.ipc.file_info(None, "bucket", "file")

    def test_delete_bucket_not_found(self):
        import grpc
        error = grpc.RpcError()
        error.code = Mock(return_value=grpc.StatusCode.NOT_FOUND)
        error.details = Mock(return_value="bucket not found")
        self.mock_client.BucketView.side_effect = error
        
        with pytest.raises(SDKError, match="not found"):
            self.ipc.delete_bucket(None, "nonexistent")

    def test_upload_file_already_exists(self):
        mock_bucket = Mock()
        mock_bucket.id = "0x1234"
        mock_bucket.created_at = Mock()
        mock_bucket.created_at.seconds = 123
        self.mock_client.BucketView.return_value = mock_bucket
        
        self.mock_ipc.storage.create_file.side_effect = Exception("0x6891dde0 FileAlreadyExists")
        
        with pytest.raises(SDKError, match="file already exists"):
            self.ipc.create_file_upload(None, "bucket", "existing_file.txt")

        with pytest.raises(SDKError, match="empty file name"):
            self.ipc.create_file_download(None, "bucket", "")
