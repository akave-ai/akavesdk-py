import unittest
from unittest.mock import MagicMock, patch
import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from sdk.sdk import SDK, SDKError, BucketCreateResult, Bucket, encryption_key_derivation
from sdk.sdk_streaming import StreamingAPI
from sdk.erasure_code import ErasureCode
from sdk.dag import DAG
from sdk.sdk_ipc import IPCClient
import tempfile
import os
from sdk.model import Block, File, Directory, FileType
from sdk.connection import ConnectionPool, Connection
import hashlib
import threading
import queue


class TestSDK(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.mock_conn = MagicMock()

        with patch("private.pb.NodeAPIClient", return_value=self.mock_client):
            self.sdk = SDK(
                address="localhost:50051",
                max_concurrency=5,
                block_part_size=1024,
                use_connection_pool=False,
                encryption_key=b"0123456789abcdef0123456789abcdef",
                private_key="some_private_key",
                streaming_max_blocks_in_chunk=32,
                parity_blocks_count=2
            )

    def test_create_bucket_valid(self):
        mock_response = MagicMock()
        mock_response.name = "test_bucket"
        mock_response.created_at = Timestamp()
        self.mock_client.bucket_create.return_value = mock_response

        result = self.sdk.create_bucket("test_bucket")

        self.assertEqual(result.name, "test_bucket")
        self.assertIsInstance(result.created_at, Timestamp)

    def test_create_bucket_invalid_name(self):
        with self.assertRaises(SDKError):
            self.sdk.create_bucket("ab")  # Less than MIN_BUCKET_NAME_LENGTH

    def test_view_bucket_valid(self):
        mock_response = MagicMock()
        mock_response.name = "test_bucket"
        mock_response.created_at = Timestamp()
        self.mock_client.bucket_view.return_value = mock_response

        result = self.sdk.view_bucket("test_bucket")

        self.assertEqual(result.name, "test_bucket")
        self.assertIsInstance(result.created_at, Timestamp)

    def test_view_bucket_invalid_name(self):
        with self.assertRaises(SDKError):
            self.sdk.view_bucket("")

    def test_delete_bucket(self):
  
        self.mock_client.bucket_delete.return_value = None
        self.sdk.delete_bucket("test_bucket")  

       
        with self.assertRaises(SDKError) as context:
            self.sdk.delete_bucket("ab")  
        self.assertIn("Invalid bucket name", str(context.exception))

        self.mock_client.bucket_delete.side_effect = Exception("gRPC error")
        with self.assertRaises(SDKError) as context:
            self.sdk.delete_bucket("test_bucket")
        self.assertIn("Failed to delete bucket", str(context.exception))

    def test_encryption_key_derivation(self):
        key = encryption_key_derivation(b"parent_key", "info1", "info2")
        self.assertIsNotNone(key)
        self.assertIsInstance(key, bytes)


class TestStreamingAPI(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.mock_conn = MagicMock()
        self.erasure_code = ErasureCode(2)  
        self.streaming_api = StreamingAPI(
            conn=self.mock_conn,
            client=self.mock_client,
            erasure_code=self.erasure_code,
            max_concurrency=5,
            block_part_size=1024,
            use_connection_pool=False,
            encryption_key=b"0123456789abcdef0123456789abcdef",
            max_blocks_in_chunk=32
        )

    def test_streaming_api_initialization(self):
        self.assertEqual(self.streaming_api.max_concurrency, 5)
        self.assertEqual(self.streaming_api.block_part_size, 1024)
        self.assertEqual(self.streaming_api.max_blocks_in_chunk, 32)
        self.assertFalse(self.streaming_api.use_connection_pool)
        self.assertEqual(self.streaming_api.encryption_key, 
                        b"0123456789abcdef0123456789abcdef")
        self.assertIsInstance(self.streaming_api.erasure_code, ErasureCode)


class TestErasureCode(unittest.TestCase):
    def setUp(self):
        self.erasure_code = ErasureCode(parity_blocks=2)

    def test_erasure_code_initialization(self):
        self.assertEqual(self.erasure_code.parity_blocks, 2)

    def test_invalid_parity_blocks(self):
        with self.assertRaises(ValueError):
            ErasureCode(parity_blocks=0)  
        with self.assertRaises(ValueError):
            ErasureCode(parity_blocks=-1)


class TestDAG(unittest.TestCase):
    def setUp(self):
        self.dag = DAG()

    def test_add_node(self):
        self.dag.add_node("node1", {"data": "value1"})
        self.assertTrue(self.dag.has_node("node1"))
        self.assertEqual(self.dag.get_node_data("node1"), {"data": "value1"})

    def test_add_edge(self):
        self.dag.add_node("node1", {"data": "value1"})
        self.dag.add_node("node2", {"data": "value2"})
        self.dag.add_edge("node1", "node2")
        self.assertTrue(self.dag.has_edge("node1", "node2"))

    def test_cycle_detection(self):
        self.dag.add_node("node1", {})
        self.dag.add_node("node2", {})
        self.dag.add_node("node3", {})
        
        self.dag.add_edge("node1", "node2")
        self.dag.add_edge("node2", "node3")
        
        # Adding this edge would create a cycle
        with self.assertRaises(ValueError):
            self.dag.add_edge("node3", "node1")

    def test_topological_sort(self):
        self.dag.add_node("task1", {})
        self.dag.add_node("task2", {})
        self.dag.add_node("task3", {})
        
        self.dag.add_edge("task1", "task2")
        self.dag.add_edge("task2", "task3")
        
        order = self.dag.topological_sort()
        self.assertEqual(order, ["task1", "task2", "task3"])


class TestStreamingAPIOperations(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.mock_conn = MagicMock()
        self.erasure_code = ErasureCode(2)
        self.streaming_api = StreamingAPI(
            conn=self.mock_conn,
            client=self.mock_client,
            erasure_code=self.erasure_code,
            max_concurrency=5,
            block_part_size=1024,
            use_connection_pool=False,
            encryption_key=b"0123456789abcdef0123456789abcdef",
            max_blocks_in_chunk=32
        )
        self.test_data = b"Hello, World!" * 1000  # Create some test data

    @patch('sdk.sdk_streaming.grpc.ClientConn')
    def test_connection_pool(self, mock_grpc_conn):
        streaming_api = StreamingAPI(
            conn=self.mock_conn,
            client=self.mock_client,
            erasure_code=self.erasure_code,
            max_concurrency=5,
            block_part_size=1024,
            use_connection_pool=True,  # Test with connection pool enabled
            encryption_key=None,
            max_blocks_in_chunk=32
        )
        self.assertTrue(streaming_api.use_connection_pool)
        self.assertEqual(streaming_api.max_concurrency, 5)

    def test_block_size_validation(self):
        with self.assertRaises(ValueError):
            StreamingAPI(
                conn=self.mock_conn,
                client=self.mock_client,
                erasure_code=self.erasure_code,
                max_concurrency=5,
                block_part_size=0,  # Invalid block size
                use_connection_pool=False,
                encryption_key=None,
                max_blocks_in_chunk=32
            )


class TestIPCClient(unittest.TestCase):
    def setUp(self):
        self.mock_socket = MagicMock()
        self.ipc_client = IPCClient()
        
    @patch('socket.socket')
    def test_connect(self, mock_socket):
        mock_socket.return_value = self.mock_socket
        self.ipc_client.connect("localhost", 8080)
        mock_socket.assert_called_once()
        self.mock_socket.connect.assert_called_once_with(("localhost", 8080))

    def test_invalid_connection(self):
        with self.assertRaises(ConnectionError):
            self.ipc_client.connect("invalid_host", -1)  # Invalid port

    @patch('socket.socket')
    def test_send_receive(self, mock_socket):
        mock_socket.return_value = self.mock_socket
        self.mock_socket.recv.return_value = b"response"
        
        self.ipc_client.connect("localhost", 8080)
        response = self.ipc_client.send_and_receive(b"request")
        
        self.mock_socket.send.assert_called_once_with(b"request")
        self.assertEqual(response, b"response")

    def test_close_connection(self):
        self.ipc_client._socket = self.mock_socket
        self.ipc_client.close()
        self.mock_socket.close.assert_called_once()


class TestSDKErrorHandling(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        with patch("private.pb.NodeAPIClient", return_value=self.mock_client):
            self.sdk = SDK(
                address="localhost:50051",
                max_concurrency=5,
                block_part_size=1024,
                use_connection_pool=False,
                encryption_key=b"0123456789abcdef0123456789abcdef",
                private_key="some_private_key",
                streaming_max_blocks_in_chunk=32,
                parity_blocks_count=2
            )

    def test_connection_timeout(self):
        self.mock_client.bucket_view.side_effect = grpc.RpcError("Connection timeout")
        with self.assertRaises(SDKError) as context:
            self.sdk.view_bucket("test_bucket")
        self.assertIn("Connection timeout", str(context.exception))

    def test_invalid_encryption_key(self):
        with self.assertRaises(ValueError):
            SDK(
                address="localhost:50051",
                max_concurrency=5,
                block_part_size=1024,
                use_connection_pool=False,
                encryption_key=b"too_short",  # Invalid key length
                private_key="some_private_key",
                streaming_max_blocks_in_chunk=32,
                parity_blocks_count=2
            )

    def test_invalid_max_concurrency(self):
        with self.assertRaises(ValueError):
            SDK(
                address="localhost:50051",
                max_concurrency=0,  # Invalid concurrency
                block_part_size=1024,
                use_connection_pool=False,
                encryption_key=b"0123456789abcdef0123456789abcdef",
                private_key="some_private_key",
                streaming_max_blocks_in_chunk=32,
                parity_blocks_count=2
            )


class TestModel(unittest.TestCase):
    def setUp(self):
        self.test_block = Block(
            hash="abc123",
            size=1024,
            data=b"test data",
            index=0
        )
        self.test_file = File(
            name="test.txt",
            size=1024,
            created_at=123456789,
            modified_at=123456789,
            blocks=[self.test_block]
        )

    def test_block_validation(self):
        # Test valid block creation
        block = Block(hash="def456", size=512, data=b"data", index=1)
        self.assertEqual(block.hash, "def456")
        self.assertEqual(block.size, 512)
        
        # Test invalid block size
        with self.assertRaises(ValueError):
            Block(hash="xyz789", size=-1, data=b"data", index=0)

        # Test invalid block index
        with self.assertRaises(ValueError):
            Block(hash="xyz789", size=512, data=b"data", index=-1)

    def test_file_operations(self):
        # Test file type detection
        self.assertEqual(self.test_file.get_file_type(), FileType.REGULAR)
        
        # Test file with no extension
        file_no_ext = File(
            name="testfile",
            size=1024,
            created_at=123456789,
            modified_at=123456789,
            blocks=[]
        )
        self.assertEqual(file_no_ext.get_extension(), "")

        # Test file size calculation
        self.assertEqual(self.test_file.calculate_total_size(), 1024)

    def test_directory_operations(self):
        dir1 = Directory(
            name="dir1",
            created_at=123456789,
            modified_at=123456789
        )
        
        # Test adding files to directory
        dir1.add_file(self.test_file)
        self.assertEqual(len(dir1.files), 1)
        
        # Test adding subdirectories
        subdir = Directory(
            name="subdir",
            created_at=123456789,
            modified_at=123456789
        )
        dir1.add_subdirectory(subdir)
        self.assertEqual(len(dir1.subdirectories), 1)

        # Test directory size calculation
        self.assertEqual(dir1.calculate_total_size(), 1024)


class TestConnection(unittest.TestCase):
    def setUp(self):
        self.address = "localhost:50051"
        self.connection = Connection(self.address)

    @patch('grpc.insecure_channel')
    def test_connection_creation(self, mock_channel):
        mock_channel.return_value = MagicMock()
        conn = Connection(self.address)
        self.assertEqual(conn.address, self.address)
        mock_channel.assert_called_once_with(self.address)

    def test_connection_close(self):
        mock_channel = MagicMock()
        self.connection._channel = mock_channel
        self.connection.close()
        mock_channel.close.assert_called_once()

    def test_connection_context_manager(self):
        mock_channel = MagicMock()
        self.connection._channel = mock_channel
        with self.connection:
            pass
        mock_channel.close.assert_called_once()


class TestConnectionPool(unittest.TestCase):
    def setUp(self):
        self.address = "localhost:50051"
        self.pool_size = 3
        self.pool = ConnectionPool(self.address, self.pool_size)

    def test_pool_initialization(self):
        self.assertEqual(self.pool.address, self.address)
        self.assertEqual(self.pool.pool_size, self.pool_size)
        self.assertIsInstance(self.pool._pool, queue.Queue)
        self.assertEqual(self.pool._pool.maxsize, self.pool_size)

    @patch('sdk.connection.Connection')
    def test_get_connection(self, mock_connection):
        mock_connection.return_value = MagicMock()
        conn = self.pool.get_connection()
        self.assertIsNotNone(conn)
        mock_connection.assert_called_once_with(self.address)

    def test_return_connection(self):
        mock_conn = MagicMock()
        self.pool._pool.put(mock_conn)
        self.assertEqual(self.pool._pool.qsize(), 1)

    def test_pool_exhaustion(self):
        # Fill the pool
        connections = []
        for _ in range(self.pool_size):
            connections.append(self.pool.get_connection())
        
        # Try to get one more connection
        with self.assertRaises(queue.Empty):
            self.pool.get_connection(timeout=0.1)


class TestErasureCodeExtended(unittest.TestCase):
    def setUp(self):
        self.erasure_code = ErasureCode(parity_blocks=2)
        self.test_data = b"Test data for erasure coding" * 100

    def test_encode_decode(self):
        # Test encoding
        encoded_blocks = self.erasure_code.encode(self.test_data)
        self.assertIsNotNone(encoded_blocks)
        self.assertEqual(len(encoded_blocks), len(self.test_data) + 2)  # Original + parity

        # Test decoding
        decoded_data = self.erasure_code.decode(encoded_blocks)
        self.assertEqual(decoded_data, self.test_data)

    def test_partial_recovery(self):
        # Encode data
        encoded_blocks = self.erasure_code.encode(self.test_data)
        
        # Simulate loss of some blocks (but within recovery threshold)
        corrupted_blocks = encoded_blocks[:-1]  # Remove last block
        
        # Should still be able to recover
        decoded_data = self.erasure_code.decode(corrupted_blocks)
        self.assertEqual(decoded_data, self.test_data)

    def test_excessive_loss(self):
        # Encode data
        encoded_blocks = self.erasure_code.encode(self.test_data)
        
        # Simulate loss of too many blocks
        too_corrupted = encoded_blocks[:-3]  # Remove more blocks than parity can handle
        
        # Should raise error
        with self.assertRaises(ValueError):
            self.erasure_code.decode(too_corrupted)

    def test_empty_data(self):
        with self.assertRaises(ValueError):
            self.erasure_code.encode(b"")

    def test_large_data(self):
        large_data = b"X" * 1024 * 1024  # 1MB of data
        encoded = self.erasure_code.encode(large_data)
        decoded = self.erasure_code.decode(encoded)
        self.assertEqual(decoded, large_data)


class TestModelValidation(unittest.TestCase):
    def test_file_name_validation(self):
        # Test invalid characters in filename
        with self.assertRaises(ValueError):
            File(
                name="test/file.txt",  # Contains path separator
                size=1024,
                created_at=123456789,
                modified_at=123456789,
                blocks=[]
            )

        # Test empty filename
        with self.assertRaises(ValueError):
            File(
                name="",
                size=1024,
                created_at=123456789,
                modified_at=123456789,
                blocks=[]
            )

    def test_timestamp_validation(self):
        # Test invalid timestamps
        with self.assertRaises(ValueError):
            File(
                name="test.txt",
                size=1024,
                created_at=-1,  # Invalid timestamp
                modified_at=123456789,
                blocks=[]
            )

        with self.assertRaises(ValueError):
            File(
                name="test.txt",
                size=1024,
                created_at=123456789,
                modified_at=-1,  # Invalid timestamp
                blocks=[]
            )

    def test_block_hash_validation(self):
        # Test invalid hash format
        with self.assertRaises(ValueError):
            Block(
                hash="invalid#hash",  # Invalid characters
                size=512,
                data=b"data",
                index=0
            )

        # Test empty hash
        with self.assertRaises(ValueError):
            Block(
                hash="",
                size=512,
                data=b"data",
                index=0
            )


if __name__ == "__main__":
    unittest.main()
