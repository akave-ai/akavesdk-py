import unittest
from unittest.mock import patch, MagicMock, Mock
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sdk.sdk_streaming import StreamingAPI, FileUpload, FileDownload, FileMeta
from sdk.common import SDKError
import grpc


class TestStreamingAPI(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_conn = MagicMock()
        self.mock_client = MagicMock()
        self.mock_erasure_code = None
        self.test_max_concurrency = 10
        self.test_block_part_size = 1024
        self.test_use_connection_pool = True
        self.test_encryption_key = b'a' * 32
        self.test_max_blocks_in_chunk = 32
        
        self.streaming_api = StreamingAPI(
            conn=self.mock_conn,
            client=self.mock_client,
            erasure_code=self.mock_erasure_code,
            max_concurrency=self.test_max_concurrency,
            block_part_size=self.test_block_part_size,
            use_connection_pool=self.test_use_connection_pool,
            encryption_key=self.test_encryption_key,
            max_blocks_in_chunk=self.test_max_blocks_in_chunk
        )
    
    def test_streaming_api_initialization(self):
        """Test StreamingAPI initialization."""
        self.assertEqual(self.streaming_api.max_concurrency, self.test_max_concurrency)
        self.assertEqual(self.streaming_api.block_part_size, self.test_block_part_size)
        self.assertEqual(self.streaming_api.use_connection_pool, self.test_use_connection_pool)
        self.assertEqual(self.streaming_api.encryption_key, self.test_encryption_key)
        self.assertEqual(self.streaming_api.max_blocks_in_chunk, self.test_max_blocks_in_chunk)
    
    def test_create_file_upload_success(self):
        """Test successful file upload creation."""
        # Arrange
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        
        mock_response = MagicMock()
        mock_response.bucket_name = bucket_name
        mock_response.file_name = file_name
        mock_response.stream_id = "stream123"
        mock_response.created_at = MagicMock()
        
        self.mock_client.FileUploadCreate.return_value = mock_response
        
        # Act
        result = self.streaming_api.create_file_upload(None, bucket_name, file_name)
        
        # Assert
        self.assertIsInstance(result, FileUpload)
        self.assertEqual(result.bucket_name, bucket_name)
        self.assertEqual(result.name, file_name)
        self.assertEqual(result.stream_id, "stream123")
        self.mock_client.FileUploadCreate.assert_called_once()
    
    def test_create_file_upload_empty_bucket_name(self):
        """Test file upload creation with empty bucket name."""
        with self.assertRaises(SDKError) as context:
            self.streaming_api.create_file_upload(None, "", "test-file.txt")
        
        self.assertIn("empty bucket name", str(context.exception))
    
    def test_create_file_upload_grpc_error(self):
        """Test file upload creation with gRPC error."""
        # Arrange
        self.mock_client.FileUploadCreate.side_effect = Exception("gRPC error")
        
        # Act & Assert
        with self.assertRaises(SDKError) as context:
            self.streaming_api.create_file_upload(None, "test-bucket", "test-file.txt")
        
        self.assertIn("failed to create file upload", str(context.exception))
    
    def test_create_file_download_success(self):
        """Test successful file download creation."""
        # Arrange
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        root_cid = "QmTest123"
        
        mock_chunk = MagicMock()
        mock_chunk.cid = "chunk_cid_123"
        mock_chunk.encoded_size = 1024
        mock_chunk.size = 512
        
        mock_response = MagicMock()
        mock_response.stream_id = "stream123"
        mock_response.bucket_name = bucket_name
        mock_response.chunks = [mock_chunk]
        
        self.mock_client.FileDownloadCreate.return_value = mock_response
        
        # Act
        result = self.streaming_api.create_file_download(None, bucket_name, file_name, root_cid)
        
        # Assert
        self.assertIsInstance(result, FileDownload)
        self.assertEqual(result.stream_id, "stream123")
        self.assertEqual(result.bucket_name, bucket_name)
        self.assertEqual(result.name, file_name)
        self.assertEqual(len(result.chunks), 1)
        self.assertEqual(result.chunks[0].cid, "chunk_cid_123")
        self.mock_client.FileDownloadCreate.assert_called_once()
    
    def test_create_file_download_grpc_error(self):
        """Test file download creation with gRPC error."""
        # Arrange
        self.mock_client.FileDownloadCreate.side_effect = Exception("gRPC error")
        
        # Act & Assert
        with self.assertRaises(SDKError) as context:
            self.streaming_api.create_file_download(None, "test-bucket", "test-file.txt")
        
        self.assertIn("failed to create file download", str(context.exception))
    
    def test_create_range_file_download_success(self):
        """Test successful range file download creation."""
        # Arrange
        bucket_name = "test-bucket"
        file_name = "test-file.txt"
        start = 0
        end = 10
        
        mock_chunk = MagicMock()
        mock_chunk.cid = "chunk_cid_123"
        mock_chunk.encoded_size = 1024
        mock_chunk.size = 512
        
        mock_response = MagicMock()
        mock_response.stream_id = "stream123"
        mock_response.bucket_name = bucket_name
        mock_response.chunks = [mock_chunk]
        
        self.mock_client.FileDownloadRangeCreate.return_value = mock_response
        
        # Act
        result = self.streaming_api.create_range_file_download(None, bucket_name, file_name, start, end)
        
        # Assert
        self.assertIsInstance(result, FileDownload)
        self.assertEqual(result.stream_id, "stream123")
        self.assertEqual(result.bucket_name, bucket_name)
        self.assertEqual(result.name, file_name)
        self.assertEqual(len(result.chunks), 1)
        self.mock_client.FileDownloadRangeCreate.assert_called_once()
    
    @patch('sdk.sdk_streaming.nodeapi_pb2.StreamFileUploadChunkCreateRequest')
    @patch('sdk.sdk_streaming.build_dag')
    @patch('sdk.sdk_streaming.to_proto_chunk')
    def test_create_chunk_upload_success(self, mock_to_proto_chunk, mock_build_dag, mock_request_class):
        """Test successful chunk upload creation."""
        # Arrange
        file_upload = FileUpload(
            bucket_name="test-bucket",
            name="test-file.txt",
            stream_id="stream123",
            created_at=None
        )
        index = 0
        file_encryption_key = b"test_key" * 4  # 32 bytes
        data = b"test data"
        
        # Mock DAG building
        mock_dag = MagicMock()
        mock_dag.cid = "chunk_cid_123"
        mock_dag.raw_data_size = len(data)
        mock_dag.encoded_size = 100
        mock_dag.blocks = []
        mock_build_dag.return_value = mock_dag
        
        # Mock proto chunk conversion - create a proper mock with CopyFrom method
        mock_proto_chunk = MagicMock()
        mock_proto_chunk.__class__ = MagicMock()
        mock_proto_chunk.__class__.__name__ = 'Chunk'
        mock_to_proto_chunk.return_value = mock_proto_chunk
        
        # Mock client response
        mock_response = MagicMock()
        mock_response.blocks = []
        self.mock_client.FileUploadChunkCreate.return_value = mock_response
        
        # Mock the request creation to avoid CopyFrom issues
        mock_request = MagicMock()
        mock_request.chunk = MagicMock()
        mock_request.chunk.CopyFrom = MagicMock()
        mock_request_class.return_value = mock_request
        
        # Act
        result = self.streaming_api._create_chunk_upload(None, file_upload, index, file_encryption_key, data)
        
        # Assert
        self.assertIsNotNone(result)
        mock_build_dag.assert_called_once()
        mock_to_proto_chunk.assert_called_once()
        self.mock_client.FileUploadChunkCreate.assert_called_once()
    
    @patch('sdk.sdk_streaming.build_dag')
    def test_create_chunk_upload_build_dag_error(self, mock_build_dag):
        """Test chunk upload creation with DAG building error."""
        # Arrange
        file_upload = FileUpload(
            bucket_name="test-bucket",
            name="test-file.txt",
            stream_id="stream123",
            created_at=None
        )
        index = 0
        file_encryption_key = b"test_key" * 4  # 32 bytes
        data = b"test data"
        
        mock_build_dag.side_effect = Exception("DAG building failed")
        
        # Act & Assert
        with self.assertRaises(Exception) as context:
            self.streaming_api._create_chunk_upload(None, file_upload, index, file_encryption_key, data)
        
        self.assertIn("DAG building failed", str(context.exception))

    def test_download_v2_success(self):
        """Test successful download_v2 operation."""
        file_download = MagicMock()
        file_download.chunks = [MagicMock(), MagicMock()]
        file_download.stream_id = 'stream123'
        ctx = MagicMock()
        ctx.done.return_value = False
        writer = MagicMock()
        with patch.object(self.streaming_api, '_create_chunk_download_v2', return_value=MagicMock()) as mock_create_chunk_download_v2, \
             patch.object(self.streaming_api, '_download_chunk_blocks') as mock_download_chunk_blocks:
            self.streaming_api.download_v2(ctx, file_download, writer)
            self.assertEqual(mock_create_chunk_download_v2.call_count, 2)
            self.assertEqual(mock_download_chunk_blocks.call_count, 2)

    def test_download_v2_context_cancel(self):
        """Test download_v2 stops on context cancellation."""
        file_download = MagicMock()
        chunk1 = MagicMock()
        chunk2 = MagicMock()
        file_download.chunks = [chunk1, chunk2]
        file_download.stream_id = 'stream123'
        ctx = MagicMock()
        ctx.done.side_effect = [False, True]
        writer = MagicMock()
        with patch.object(self.streaming_api, '_create_chunk_download_v2', return_value=MagicMock()) as mock_create_chunk_download_v2, \
             patch.object(self.streaming_api, '_download_chunk_blocks') as mock_download_chunk_blocks:
            self.streaming_api.download_v2(ctx, file_download, writer)
            self.assertEqual(mock_create_chunk_download_v2.call_count, 1)
            self.assertEqual(mock_download_chunk_blocks.call_count, 1)

    def test_download_random_success(self):
        """Test successful download_random operation with erasure coding enabled."""
        self.streaming_api.erasure_code = MagicMock()
        file_download = MagicMock()
        file_download.chunks = [MagicMock(), MagicMock()]
        file_download.stream_id = 'stream123'
        ctx = MagicMock()
        ctx.done.return_value = False
        writer = MagicMock()
        with patch.object(self.streaming_api, '_create_chunk_download', return_value=MagicMock()) as mock_create_chunk_download, \
             patch.object(self.streaming_api, '_download_random_chunk_blocks') as mock_download_random_chunk_blocks:
            self.streaming_api.download_random(ctx, file_download, writer)
            self.assertEqual(mock_create_chunk_download.call_count, 2)
            self.assertEqual(mock_download_random_chunk_blocks.call_count, 2)

    def test_download_random_erasure_coding_disabled(self):
        """Test download_random raises error if erasure coding is not enabled."""
        self.streaming_api.erasure_code = None
        file_download = MagicMock()
        file_download.chunks = [MagicMock()]
        ctx = MagicMock()
        writer = MagicMock()
        with self.assertRaises(SDKError) as context:
            self.streaming_api.download_random(ctx, file_download, writer)
        self.assertIn('erasure coding is not enabled', str(context.exception))

    def test_download_random_context_cancel(self):
        """Test download_random stops on context cancellation."""
        self.streaming_api.erasure_code = MagicMock()
        file_download = MagicMock()
        chunk1 = MagicMock()
        chunk2 = MagicMock()
        file_download.chunks = [chunk1, chunk2]
        file_download.stream_id = 'stream123'
        ctx = MagicMock()
        ctx.done.side_effect = [False, True]
        writer = MagicMock()
        with patch.object(self.streaming_api, '_create_chunk_download', return_value=MagicMock()) as mock_create_chunk_download, \
             patch.object(self.streaming_api, '_download_random_chunk_blocks') as mock_download_random_chunk_blocks:
            self.streaming_api.download_random(ctx, file_download, writer)
            self.assertEqual(mock_create_chunk_download.call_count, 1)
            self.assertEqual(mock_download_random_chunk_blocks.call_count, 1)

    def test_download_v2_error_handling(self):
        """Test download_v2 raises SDKError on exception."""
        file_download = MagicMock()
        file_download.chunks = [MagicMock()]
        file_download.stream_id = 'stream123'
        ctx = MagicMock()
        ctx.done.return_value = False
        writer = MagicMock()
        with patch.object(self.streaming_api, '_create_chunk_download_v2', side_effect=Exception('fail')):
            with self.assertRaises(SDKError) as context:
                self.streaming_api.download_v2(ctx, file_download, writer)
            self.assertIn('failed to download file', str(context.exception))

    def test_download_random_error_handling(self):
        """Test download_random raises SDKError on exception."""
        self.streaming_api.erasure_code = MagicMock()
        file_download = MagicMock()
        file_download.chunks = [MagicMock()]
        file_download.stream_id = 'stream123'
        ctx = MagicMock()
        ctx.done.return_value = False
        writer = MagicMock()
        with patch.object(self.streaming_api, '_create_chunk_download', side_effect=Exception('fail')):
            with self.assertRaises(SDKError) as context:
                self.streaming_api.download_random(ctx, file_download, writer)
            self.assertIn('failed to download file', str(context.exception))


if __name__ == '__main__':
    unittest.main() 