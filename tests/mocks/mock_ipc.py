from unittest.mock import Mock, MagicMock
from typing import Any, Dict, List, Optional

class MockIPCClient:
    
    def __init__(self):
        self.auth = Mock()
        self.auth.address = "0x1234567890123456789012345678901234567890"
        self.auth.key = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        
        self.storage = MockStorageContract()
        self.access_manager = Mock()
        self.eth = Mock()
        
        # Mock wait methods
        self.wait_for_tx = Mock()
        
    def close(self):
        pass

class MockStorageContract:
    
    def __init__(self):
        self.contract_address = "0x1111111111111111111111111111111111111111"
        self._chain_id = 21207
        self._buckets = {}
        self._files = {}
        self._filled_files = set()
        
    def get_chain_id(self) -> int:
        return self._chain_id
    
    def create_bucket(self, **kwargs) -> str:
        bucket_id = f"bucket_{len(self._buckets)}"
        self._buckets[kwargs.get('bucket_name', 'test')] = {
            'id': bucket_id,
            'name': kwargs.get('bucket_name', 'test'),
            'owner': kwargs.get('from_address', self.contract_address)
        }
        return f"0x{bucket_id}"
    
    def get_bucket_by_name(self, call_opts: dict, bucket_name: str, **kwargs):
        if bucket_name in self._buckets:
            bucket = self._buckets[bucket_name]
            return (
                bytes.fromhex(bucket['id'].replace('bucket_', '').zfill(64)),
                bucket['name'],
                1234567890,  # created_at
                bucket['owner'],
                []  # files
            )
        raise Exception(f"Bucket '{bucket_name}' not found")
    
    def create_file(self, from_address: str, private_key: str, bucket_id: bytes, 
                   file_name: str, **kwargs) -> str:
        file_id = f"file_{len(self._files)}"
        self._files[file_name] = {
            'id': file_id,
            'name': file_name,
            'bucket_id': bucket_id,
            'created_at': 1234567890
        }
        return f"0x{file_id}"
    
    def add_file_chunk(self, from_address: str, private_key: str, cid: bytes,
                      bucket_id: bytes, name: str, encoded_chunk_size: int,
                      cids: list, chunk_blocks_sizes: list, chunk_index: int,
                      **kwargs) -> str:
        return f"0xchunk_{chunk_index}"
    
    def is_file_filled(self, file_id: bytes) -> bool:
        return file_id in self._filled_files
    
    def mark_file_filled(self, file_id: bytes):
        self._filled_files.add(file_id)
    
    def get_file_by_name(self, call_opts: dict, bucket_id: bytes, file_name: str):
        if file_name in self._files:
            file_info = self._files[file_name]
            return (
                bytes.fromhex(file_info['id'].replace('file_', '').zfill(64)),
                b"mock_cid",
                bucket_id,
                file_name,
                1024,  # encoded_size
                file_info['created_at'],
                1000   # actual_size
            )
        raise Exception(f"File '{file_name}' not found")
    
    def commit_file(self, bucket_name: str, file_name: str, size: int,
                   root_cid: bytes, from_address: str, private_key: str) -> str:
        if file_name in self._files:
            file_id = bytes.fromhex(self._files[file_name]['id'].replace('file_', '').zfill(64))
            self.mark_file_filled(file_id)
        return "0xcommit_tx"

class MockGRPCClient:
    
    def __init__(self):
        self.BucketView = Mock()
        self.BucketList = Mock()
        self.FileView = Mock()
        self.FileList = Mock()
        self.FileUploadChunkCreate = Mock()
        self.FileUploadBlock = Mock()
        self.FileDownloadCreate = Mock()
        self.FileDownloadChunkCreate = Mock()
        self.FileDownloadBlock = Mock()
        
        self._setup_default_responses()
    
    def _setup_default_responses(self):
        mock_bucket = Mock()
        mock_bucket.id = "test_bucket_id"
        mock_bucket.name = "test-bucket"
        mock_bucket.created_at.seconds = 1234567890
        self.BucketView.return_value = mock_bucket
        
        mock_chunk_response = Mock()
        mock_chunk_response.blocks = []
        self.FileUploadChunkCreate.return_value = mock_chunk_response

class MockConnectionPool:
    
    def __init__(self):
        self.connections = {}
        self.closed = False
    
    def create_ipc_client(self, node_address: str, use_pool: bool = True):
        mock_client = Mock()
        mock_closer = Mock()
        return (mock_client, mock_closer, None)
    
    def close(self):
        self.closed = True
        return None
