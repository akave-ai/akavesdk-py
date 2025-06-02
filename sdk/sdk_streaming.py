import io
import logging
import concurrent.futures
from typing import List, Optional, Dict, Any, Callable, BinaryIO
import time
from datetime import datetime
import threading
import os
import random
import base64
from dataclasses import dataclass

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from .sdk import SDKError
from .model import FileMeta, FileListItem
from .erasure_code import ErasureCode
from private.pb import nodeapi_pb2
from private.spclient.spclient import SPClient
from private.encryption import encrypt, decrypt, derive_key
from sdk.dag import build_dag, extract_block_data

try:
    from multiformats import cid as cidlib
except ImportError:
    pass

logger = logging.getLogger(__name__)

# Constants
BlockSize = 1 << 20  # 1MiB blocks
EncryptionOverhead = 16  # 16 bytes overhead from encryption

@dataclass
class Chunk:
    """Represents a chunk of a file for download"""
    CID: str
    EncodedSize: int
    Size: int
    Index: int

@dataclass
class FileUpload:
    """Represents a file upload request"""
    BucketName: str
    Name: str
    StreamID: str
    CreatedAt: datetime

@dataclass
class FileDownload:
    """Represents a file download request"""
    StreamID: str
    BucketName: str
    Name: str
    Chunks: List[Chunk]

@dataclass
class FileBlockUpload:
    """Represents a block for upload"""
    CID: str
    Data: bytes
    NodeAddress: str = ""
    NodeID: str = ""
    Permit: bytes = b""

@dataclass
class FileChunkUpload:
    """Represents a chunk upload"""
    StreamID: str
    Index: int
    ChunkCID: Any  # CID object
    ActualSize: int
    RawDataSize: int
    ProtoNodeSize: int
    Blocks: List[FileBlockUpload]

@dataclass
class AkaveBlockData:
    """Represents Akave block data for download"""
    NodeID: str
    NodeAddress: str
    Permit: bytes = b""

@dataclass
class FilecoinBlockData:
    """Represents Filecoin block data for download"""
    BaseURL: str

@dataclass
class FileBlockDownload:
    """Represents a block for download"""
    CID: str
    Akave: Optional[AkaveBlockData] = None
    Filecoin: Optional[FilecoinBlockData] = None

@dataclass
class FileChunkDownload:
    """Represents a chunk download"""
    CID: str
    Index: int
    EncodedSize: int
    Size: int
    Blocks: List[FileBlockDownload]

class ConnectionPool:
    """Manages a pool of gRPC connections"""
    def __init__(self):
        self.connections = {}
        self.lock = threading.Lock()
        self._closed = False
    
    def create_streaming_client(self, address: str, use_pool: bool):
        """Creates a streaming client for the given address"""
        if self._closed:
            raise RuntimeError("ConnectionPool has been closed")
            
        if not use_pool:
            channel = grpc.insecure_channel(address)
            client = nodeapi_pb2.StreamAPIStub(channel)
            return client, lambda: channel.close()
        
        with self.lock:
            if address in self.connections:
                client, _ = self.connections[address]
                return client, None
            
            channel = grpc.insecure_channel(address)
            client = nodeapi_pb2.StreamAPIStub(channel)
            self.connections[address] = (client, channel)
            return client, None
    
    def close(self):
        """Closes all connections in the pool"""
        if self._closed:
            return
            
        with self.lock:
            for _, channel in self.connections.values():
                try:
                    channel.close()
                except Exception as e:
                    logger.error(f"Error closing channel: {e}")
            self.connections = {}
            self._closed = True
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class DAGRoot:
    """Simplified DAG Root implementation"""
    def __init__(self):
        self.links = []
        self._cid = None
    
    @classmethod
    def new(cls):
        return cls()
    
    def add_link(self, chunk_cid, raw_data_size: int, proto_node_size: int):
        if not chunk_cid:
            raise ValueError("Invalid chunk CID")
        if raw_data_size < 0 or proto_node_size < 0:
            raise ValueError("Invalid size values")
            
        self.links.append({
            "cid": chunk_cid,
            "raw_data_size": raw_data_size,
            "proto_node_size": proto_node_size
        })
        self._cid = None  # Reset cached CID
        return None
    
    def build(self):
        if self._cid is not None:
            return self._cid
            
        if not self.links:
            raise ValueError("Cannot build DAG root with no links")
            
        if not hasattr(cidlib, 'make_cid'):
            logger.warning("CID library not available, using fallback CID generation")
            # Generate a deterministic fallback CID based on links
            link_data = "".join(str(link["cid"]) for link in self.links)
            hash_input = link_data.encode()
            cid_str = "Qm" + base64.b32encode(hash_input[:32]).decode('utf-8')
            self._cid = type('CID', (), {'string': lambda: cid_str})()
            return self._cid
        
        try:
            # Build a proper CID using the library
            root_cid = cidlib.make_cid(f"dag_root_{len(self.links)}")
            self._cid = root_cid
            return root_cid
        except Exception as e:
            logger.error(f"Failed to generate CID: {e}")
            # Fallback to deterministic CID generation
            link_data = "".join(str(link["cid"]) for link in self.links)
            hash_input = link_data.encode()
            cid_str = "Qm" + base64.b32encode(hash_input[:32]).decode('utf-8')
            self._cid = type('CID', (), {'string': lambda: cid_str})()
            return self._cid

def encryption_key(parent_key: bytes, *info_data: str):
    """Derive an encryption key from a parent key and information data"""
    if len(parent_key) == 0:
        return b''
    
    info = "/".join(info_data)
    return derive_key(parent_key, info.encode())

def to_proto_chunk(stream_id: str, cid: str, index: int, size: int, blocks: List[FileBlockUpload]):
    """Convert block data to protobuf chunk format"""
    pb_blocks = []
    for block in blocks:
        pb_blocks.append({
            "cid": block.CID,
            "size": len(block.Data) if hasattr(block, 'Data') and block.Data is not None else 0
        })
    
    return {
        "stream_id": stream_id,
        "cid": cid,
        "index": index,
        "size": size,
        "blocks": pb_blocks
    }

class StreamingAPI:
    """
    Exposes SDK file streaming API in Python.
    This is the Python equivalent of the Go StreamingAPI.
    """
    def __init__(self, conn: grpc.Channel, client: Any,
                 erasure_code: Optional[ErasureCode] = None, 
                 max_concurrency: int = 10,
                 block_part_size: int = 1024 * 1024, 
                 use_connection_pool: bool = True,
                 encryption_key: Optional[bytes] = None, 
                 max_blocks_in_chunk: int = 32):
        """
        Initialize a new StreamingAPI instance.
        
        Args:
            conn: gRPC connection
            client: StreamAPI client
            erasure_code: Erasure coding configuration
            max_concurrency: Maximum concurrency for operations
            block_part_size: Size of block parts
            use_connection_pool: Whether to use connection pooling
            encryption_key: Optional encryption key
            max_blocks_in_chunk: Maximum blocks in one chunk
        """
        self.client = client
        self.conn = conn
        self.sp_client = SPClient()
        self.erasure_code = erasure_code
        self.max_concurrency = max_concurrency
        self.block_part_size = block_part_size
        self.use_connection_pool = use_connection_pool
        self.encryption_key = encryption_key if encryption_key else b''
        self.max_blocks_in_chunk = max_blocks_in_chunk
    
    def file_info(self, ctx, bucket_name: str, file_name: str) -> FileMeta:
        """
        Returns meta information for a single file by bucket and file name.
        
        Args:
            ctx: Context object
            bucket_name: Name of the bucket
            file_name: Name of the file
            
        Returns:
            FileMeta: File metadata
            
        Raises:
            SDKError: If there's an error fetching file information
        """
        if bucket_name == "":
            raise SDKError("empty bucket name")
        if file_name == "":
            raise SDKError("empty file name")
        
        try:
            request = nodeapi_pb2.StreamFileViewRequest(
                bucket_name=bucket_name,
                file_name=file_name
            )
            
            res = self.client.FileView(ctx, request)
            
            return FileMeta(
                stream_id=res.stream_id,
                root_cid=res.root_cid,
                bucket_name=res.bucket_name, 
                name=res.file_name,
                encoded_size=res.encoded_size,
                size=res.size,
                created_at=res.created_at.ToDatetime() if hasattr(res.created_at, 'ToDatetime') else res.created_at,
                committed_at=res.committed_at.ToDatetime() if hasattr(res.committed_at, 'ToDatetime') else res.committed_at
            )
        except Exception as err:
            raise SDKError(f"failed to get file info: {str(err)}")
    
    def list_files(self, ctx, bucket_name: str) -> List[FileMeta]:
        """
        Returns list of files in a particular bucket.
        
        Args:
            ctx: Context object
            bucket_name: Name of the bucket
            
        Returns:
            List[FileMeta]: List of file metadata
            
        Raises:
            SDKError: If there's an error listing files
        """
        if bucket_name == "":
            raise SDKError("empty bucket name")
        
        try:
            request = nodeapi_pb2.StreamFileListRequest(
                bucket_name=bucket_name
            )
            
            resp = self.client.FileList(ctx, request)
            
            files = []
            for file_meta in resp.files:
                files.append(self._to_file_meta(file_meta, bucket_name))
            
            return files
        except Exception as err:
            raise SDKError(f"failed to list files: {str(err)}")
    
    def file_versions(self, ctx, bucket_name: str, file_name: str) -> List[FileMeta]:
        """
        Returns list of file versions in a particular bucket.
        
        Args:
            ctx: Context object
            bucket_name: Name of the bucket
            file_name: Name of the file
            
        Returns:
            List[FileMeta]: List of file versions metadata
            
        Raises:
            SDKError: If there's an error listing file versions
        """
        if bucket_name == "":
            raise SDKError("empty bucket name")
        
        try:
            request = nodeapi_pb2.StreamFileListVersionsRequest(
                bucket_name=bucket_name,
                file_name=file_name
            )
            
            resp = self.client.FileVersions(ctx, request)
            
            files = []
            for file_meta in resp.versions:
                files.append(self._to_file_meta(file_meta, bucket_name))
            
            return files
        except Exception as err:
            raise SDKError(f"failed to list file versions: {str(err)}")
    
    def create_file_upload(self, ctx, bucket_name: str, file_name: str) -> FileUpload:
        """
        Creates a new file upload request.
        
        Args:
            ctx: Context object
            bucket_name: Name of the bucket
            file_name: Name of the file
            
        Returns:
            FileUpload: File upload information
            
        Raises:
            SDKError: If there's an error creating file upload
        """
        if bucket_name == "":
            raise SDKError("empty bucket name")
        
        try:
            request = nodeapi_pb2.StreamFileUploadCreateRequest(
                bucket_name=bucket_name,
                file_name=file_name
            )
            
            res = self.client.FileUploadCreate(ctx, request)
            
            return FileUpload(
                BucketName=res.bucket_name,
                Name=res.file_name,
                StreamID=res.stream_id,
                CreatedAt=res.created_at.ToDatetime() if hasattr(res.created_at, 'ToDatetime') else res.created_at
            )
        except Exception as err:
            raise SDKError(f"failed to create file upload: {str(err)}")
    
    def upload(self, ctx, upload: FileUpload, reader: BinaryIO) -> FileMeta:
        """
        Uploads a file using streaming api.
        
        Args:
            ctx: Context object
            upload: File upload information
            reader: File reader
            
        Returns:
            FileMeta: File metadata
            
        Raises:
            SDKError: If there's an error uploading file
        """
        try:
            chunk_enc_overhead = 0
            file_enc_key = b''
            if self.encryption_key:
                # TODO: implement key derivation
                file_enc_key = self.encryption_key
                chunk_enc_overhead = EncryptionOverhead
            
            is_empty_file = True
            
            buffer_size = self.max_blocks_in_chunk * BlockSize
            if self.erasure_code is not None:
                buffer_size = self.erasure_code.data_blocks * BlockSize
            buffer_size -= chunk_enc_overhead
            buf = bytearray(buffer_size)
            
            # TODO: Implement DAGRoot
            dag_root = self._create_dag_root()
            
            chunk_index = 0
            while True:
                # Check for context cancellation
                if hasattr(ctx, 'done') and ctx.done():
                    return None
                
                # Read data from reader
                n = reader.readinto(buf)
                if n == 0:
                    if is_empty_file:
                        raise SDKError("empty file")
                    break
                is_empty_file = False
                
                # Create chunk upload
                chunk_upload = self._create_chunk_upload(ctx, upload, chunk_index, file_enc_key, buf[:n])
                
                # Add link to DAG root
                self._add_dag_link(dag_root, chunk_upload.ChunkCID, chunk_upload.RawDataSize, chunk_upload.ProtoNodeSize)
                
                # Upload chunk
                self._upload_chunk(ctx, chunk_upload)
                
                chunk_index += 1
            
            # Build DAG root and get CID
            root_cid = self._build_dag_root(dag_root)
            
            # Commit stream
            file_meta = self._commit_stream(ctx, upload, root_cid, chunk_index)
            
            return file_meta
            
        except Exception as err:
            raise SDKError(f"failed to upload file: {str(err)}")
    
    def create_file_download(self, ctx, bucket_name: str, file_name: str, root_cid: str = "") -> FileDownload:
        """
        Creates a new download request.
        
        Args:
            ctx: Context object
            bucket_name: Name of the bucket
            file_name: Name of the file
            root_cid: Root CID (optional, for specific version)
            
        Returns:
            FileDownload: File download information
            
        Raises:
            SDKError: If there's an error creating file download
        """
        try:
            request = nodeapi_pb2.StreamFileDownloadCreateRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                root_cid=root_cid
            )
            
            res = self.client.FileDownloadCreate(ctx, request)
            
            chunks = []
            for chunk in res.chunks:
                chunks.append(Chunk(
                    CID=chunk.cid,
                    EncodedSize=chunk.encoded_size,
                    Size=chunk.size,
                    Index=chunk.index
                ))
            
            return FileDownload(
                StreamID=res.stream_id,
                BucketName=res.bucket_name,
                Name=file_name,
                Chunks=chunks
            )
        except Exception as err:
            raise SDKError(f"failed to create file download: {str(err)}")
    
    def create_range_file_download(self, ctx, bucket_name: str, file_name: str, start: int, end: int) -> FileDownload:
        """
        Creates a new download request with block ranges.
        
        Args:
            ctx: Context object
            bucket_name: Name of the bucket
            file_name: Name of the file
            start: Start index
            end: End index
            
        Returns:
            FileDownload: File download information
            
        Raises:
            SDKError: If there's an error creating file download
        """
        try:
            request = nodeapi_pb2.StreamFileDownloadRangeCreateRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                start_index=start,
                end_index=end
            )
            
            res = self.client.FileDownloadRangeCreate(ctx, request)
            
            chunks = []
            for chunk in res.chunks:
                chunks.append(Chunk(
                    CID=chunk.cid,
                    EncodedSize=chunk.encoded_size,
                    Size=chunk.size,
                    Index=chunk.index + start
                ))
            
            return FileDownload(
                StreamID=res.stream_id,
                BucketName=res.bucket_name,
                Name=file_name,
                Chunks=chunks
            )
        except Exception as err:
            raise SDKError(f"failed to create file download range: {str(err)}")
    
    def download(self, ctx, file_download: FileDownload, writer: BinaryIO) -> None:
        """
        Downloads a file using streaming api.
        
        Args:
            ctx: Context object
            file_download: File download information
            writer: File writer
            
        Raises:
            SDKError: If there's an error downloading file
        """
        try:
            file_enc_key = b''
            if self.encryption_key:
                # TODO: implement key derivation
                file_enc_key = self.encryption_key
            
            for chunk in file_download.Chunks:
                # Check for context cancellation
                if hasattr(ctx, 'done') and ctx.done():
                    return
                
                # Create chunk download
                chunk_download = self._create_chunk_download(ctx, file_download.StreamID, chunk)
                
                # Download chunk blocks
                self._download_chunk_blocks(ctx, file_download.StreamID, chunk_download, file_enc_key, writer)
            
        except Exception as err:
            raise SDKError(f"failed to download file: {str(err)}")
    
    def download_v2(self, ctx, file_download: FileDownload, writer: BinaryIO) -> None:
        """
        Downloads a file using streaming api V2.
        This version can download blocks from either filecoin or akave nodes.
        
        Args:
            ctx: Context object
            file_download: File download information
            writer: File writer
            
        Raises:
            SDKError: If there's an error downloading file
        """
        try:
            file_enc_key = b''
            if self.encryption_key:
                # TODO: implement key derivation
                file_enc_key = self.encryption_key
            
            for chunk in file_download.Chunks:
                # Check for context cancellation
                if hasattr(ctx, 'done') and ctx.done():
                    return
                
                # Create chunk download v2
                chunk_download = self._create_chunk_download_v2(ctx, file_download.StreamID, chunk)
                
                # Download chunk blocks
                self._download_chunk_blocks(ctx, file_download.StreamID, chunk_download, file_enc_key, writer)
            
        except Exception as err:
            raise SDKError(f"failed to download file: {str(err)}")
    
    def download_random(self, ctx, file_download: FileDownload, writer: BinaryIO) -> None:
        """
        Downloads a file using streaming api and fetches only randomly half of the blocks.
        Only works with erasure coding enabled.
        
        Args:
            ctx: Context object
            file_download: File download information
            writer: File writer
            
        Raises:
            SDKError: If there's an error downloading file
        """
        if self.erasure_code is None:
            raise SDKError("erasure coding is not enabled")
        
        try:
            file_enc_key = b''
            if self.encryption_key:
                # TODO: implement key derivation
                file_enc_key = self.encryption_key
            
            for chunk in file_download.Chunks:
                # Check for context cancellation
                if hasattr(ctx, 'done') and ctx.done():
                    return
                
                # Create chunk download
                chunk_download = self._create_chunk_download(ctx, file_download.StreamID, chunk)
                
                # Download random chunk blocks
                self._download_random_chunk_blocks(ctx, file_download.StreamID, chunk_download, file_enc_key, writer)
            
        except Exception as err:
            raise SDKError(f"failed to download file: {str(err)}")
    
    def file_delete(self, ctx, bucket_name: str, file_name: str) -> None:
        """
        Deletes a file from a bucket.
        
        Args:
            ctx: Context object
            bucket_name: Name of the bucket
            file_name: Name of the file
            
        Raises:
            SDKError: If there's an error deleting file
        """
        try:
            request = nodeapi_pb2.StreamFileDeleteRequest(
                bucket_name=bucket_name,
                file_name=file_name
            )
            
            self.client.FileDelete(ctx, request)
        except Exception as err:
            raise SDKError(f"failed to delete file: {str(err)}")
    
    # Helper methods
    def _to_file_meta(self, file_meta, bucket_name: str) -> FileMeta:
        """
        Converts protobuf file meta to FileMeta model.
        
        Args:
            file_meta: Protobuf file metadata
            bucket_name: Name of the bucket
            
        Returns:
            FileMeta: Converted file metadata
        """
        return FileMeta(
            stream_id=file_meta.stream_id,
            root_cid=file_meta.root_cid,
            bucket_name=bucket_name,
            name=file_meta.file_name,
            encoded_size=file_meta.encoded_size,
            size=file_meta.size,
            created_at=file_meta.created_at.ToDatetime() if hasattr(file_meta.created_at, 'ToDatetime') else file_meta.created_at,
            committed_at=file_meta.committed_at.ToDatetime() if hasattr(file_meta.committed_at, 'ToDatetime') else file_meta.committed_at
        )
    
    # Updated DAG operation methods
    def _create_dag_root(self):
        """Create a new DAG root"""
        return DAGRoot.new()
    
    def _add_dag_link(self, dag_root, chunk_cid, raw_size, proto_node_size):
        """Add a link to the DAG root"""
        return dag_root.add_link(chunk_cid, raw_size, proto_node_size)
    
    def _build_dag_root(self, dag_root):
        """Build the DAG root and return its CID"""
        root_cid = dag_root.build()
        if hasattr(root_cid, 'string'):
            return root_cid.string()
        return str(root_cid)
    
    def _create_chunk_upload(self, ctx, file_upload, index, file_encryption_key, data):
        """
        Create a chunk upload for the given data.
        
        Args:
            ctx: Context object
            file_upload: FileUpload object
            index: Chunk index
            file_encryption_key: Encryption key for the file
            data: Raw data to upload
            
        Returns:
            FileChunkUpload: Created chunk upload
            
        Raises:
            SDKError: If there's an error creating the chunk upload
        """
        try:
            # Encrypt the data if an encryption key is provided
            if len(file_encryption_key) > 0:
                data = encrypt(file_encryption_key, data, str(index).encode())
            
            size = len(data)
            
            # Apply erasure coding if configured
            block_size = BlockSize
            if self.erasure_code is not None:
                data = self.erasure_code.encode(data)
                # Equivalent to shard size in erasure coding terminology
                block_size = len(data) // (self.erasure_code.data_blocks + self.erasure_code.parity_blocks)
            
            # Build the DAG for the chunk
            chunk_dag = build_dag(ctx, io.BytesIO(data), block_size)
            
            # Convert to protobuf chunk format
            proto_chunk = to_proto_chunk(
                file_upload.StreamID,
                chunk_dag.cid.string() if hasattr(chunk_dag.cid, 'string') else str(chunk_dag.cid),
                index,
                size,
                [FileBlockUpload(CID=block["cid"], Data=block["data"]) for block in chunk_dag.blocks]
            )
            
            # Create the chunk upload request
            request = nodeapi_pb2.StreamFileUploadChunkCreateRequest(chunk=proto_chunk)
            
            # Send the request
            res = self.client.FileUploadChunkCreate(ctx, request)
            
            # Verify the response
            if len(res.blocks) != len(chunk_dag.blocks):
                raise SDKError(f"received unexpected amount of blocks {len(res.blocks)}, expected {len(chunk_dag.blocks)}")
            
            # Update block metadata with node information
            blocks = []
            for i, upload in enumerate(res.blocks):
                if chunk_dag.blocks[i]["cid"] != upload.cid:
                    raise SDKError(f"block CID mismatch at position {i}")
                
                blocks.append(FileBlockUpload(
                    CID=chunk_dag.blocks[i]["cid"],
                    Data=chunk_dag.blocks[i]["data"],
                    NodeAddress=upload.node_address,
                    NodeID=upload.node_id,
                    Permit=upload.permit
                ))
            
            # Create and return the chunk upload
            return FileChunkUpload(
                StreamID=file_upload.StreamID,
                Index=index,
                ChunkCID=chunk_dag.cid,
                ActualSize=size,
                RawDataSize=chunk_dag.raw_data_size,
                ProtoNodeSize=chunk_dag.proto_node_size,
                Blocks=blocks
            )
        except Exception as err:
            raise SDKError(f"failed to create chunk upload: {str(err)}")
    
    def _upload_chunk(self, ctx, file_chunk_upload: FileChunkUpload):
        """
        Upload a chunk to the storage nodes.
        
        Args:
            ctx: Context object
            file_chunk_upload: FileChunkUpload object with chunk data
            
        Raises:
            SDKError: If there's an error uploading the chunk
        """
        try:
            pool = ConnectionPool()
            
            try:
                # Convert to protobuf chunk format for sending
                proto_chunk = to_proto_chunk(
                    file_chunk_upload.StreamID,
                    file_chunk_upload.ChunkCID.string() if hasattr(file_chunk_upload.ChunkCID, 'string') else str(file_chunk_upload.ChunkCID),
                    file_chunk_upload.Index,
                    file_chunk_upload.ActualSize,
                    file_chunk_upload.Blocks
                )
                
                # Upload blocks in parallel
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                    futures = []
                    
                    for i, block in enumerate(file_chunk_upload.Blocks):
                        futures.append(executor.submit(
                            self._upload_block,
                            ctx, pool, i, block, proto_chunk
                        ))
                    
                    # Wait for all futures to complete and raise any errors
                    for future in concurrent.futures.as_completed(futures):
                        future.result()
                        
            finally:
                # Close the connection pool
                pool.close()
            
            return None
        except Exception as err:
            raise SDKError(f"failed to upload chunk: {str(err)}")
    
    def _upload_block(self, ctx, pool: ConnectionPool, block_index: int, block: FileBlockUpload, proto_chunk):
        """
        Upload a block to a storage node.
        
        Args:
            ctx: Context object
            pool: Connection pool
            block_index: Index of the block in the chunk
            block: Block data to upload
            proto_chunk: Protobuf chunk data
            
        Raises:
            SDKError: If there's an error uploading the block
        """
        try:
            # Create a client for the node
            client, closer = pool.create_streaming_client(block.NodeAddress, self.use_connection_pool)
            
            try:
                # Create a streaming connection for the block upload
                sender = client.FileUploadBlock(ctx)
                
                # Upload the block data
                err = self._upload_block_segments(
                    ctx,
                    {
                        "data": block.Data,
                        "cid": block.CID,
                        "index": block_index,
                        "chunk": proto_chunk
                    },
                    sender.send
                )
                
                if err:
                    raise err
                
                # Close the streaming connection
                _, err = sender.close_and_recv()
                if err:
                    raise err
                
            finally:
                # Close the client connection if not using the pool
                if closer:
                    closer()
                    
        except Exception as err:
            raise SDKError(f"failed to upload block {block.CID}: {str(err)}")
    
    def _upload_block_segments(self, ctx, block_data, send_func: Callable):
        """
        Upload a block in segments to avoid memory issues with large blocks.
        
        Args:
            ctx: Context object
            block_data: Block data to upload
            send_func: Function to send data
            
        Returns:
            Optional[Exception]: Any error that occurred during upload
        """
        try:
            data = block_data.get("data", b"")
            data_len = len(data)
            
            if data_len == 0:
                return None
                
            i = 0
            while i < data_len:
                # Check for context cancellation
                if hasattr(ctx, 'done') and ctx.done():
                    return SDKError("context cancelled")
                    
                # Calculate the end of this segment
                end = i + self.block_part_size
                if end > data_len:
                    end = data_len
                
                # Create a segment with just the relevant slice of data
                segment_data = dict(block_data)
                segment_data["data"] = data[i:end]
                
                # Send the segment
                send_func(segment_data)
                
                # Clear non-essential data for subsequent segments
                segment_data["chunk"] = None
                segment_data["cid"] = ""
                
                # Move to the next segment
                i += self.block_part_size
                
            return None
        except Exception as err:
            return SDKError(f"failed to upload block segments: {str(err)}")
    
    def _commit_stream(self, ctx, upload, root_cid, chunk_count):
        """Placeholder for committing stream"""
        # TODO: Implement stream commit
        request = nodeapi_pb2.StreamFileUploadCommitRequest(
            stream_id=upload.StreamID,
            root_cid=root_cid,
            chunk_count=chunk_count
        )
        
        res = self.client.FileUploadCommit(ctx, request)
        
        return FileMeta(
            stream_id=res.stream_id,
            root_cid=root_cid,
            bucket_name=res.bucket_name,
            name=res.file_name,
            encoded_size=res.encoded_size,
            size=res.size,
            created_at=upload.CreatedAt,
            committed_at=res.committed_at.ToDatetime() if hasattr(res.committed_at, 'ToDatetime') else res.committed_at
        )
    
    def _create_chunk_download(self, ctx, stream_id, chunk):
        """Placeholder for creating chunk download"""
        # TODO: Implement chunk download creation
        request = nodeapi_pb2.StreamFileDownloadChunkCreateRequest(
            stream_id=stream_id,
            chunk_cid=chunk.CID
        )
        
        res = self.client.FileDownloadChunkCreate(ctx, request)
        
        blocks = []
        for block in res.blocks:
            blocks.append(FileBlockDownload(
                CID=block.cid,
                Akave=AkaveBlockData(
                    NodeID=block.node_id,
                    NodeAddress=block.node_address,
                    Permit=block.permit
                )
            ))
        
        return FileChunkDownload(
            CID=chunk.CID,
            Index=chunk.Index,
            EncodedSize=chunk.EncodedSize,
            Size=chunk.Size,
            Blocks=blocks
        )
    
    def _create_chunk_download_v2(self, ctx, stream_id, chunk):
        """Placeholder for creating chunk download v2"""
        # TODO: Implement chunk download v2 creation
        request = nodeapi_pb2.StreamFileDownloadChunkCreateRequest(
            stream_id=stream_id,
            chunk_cid=chunk.CID
        )
        
        res = self.client.FileDownloadChunkCreateV2(ctx, request)
        
        blocks = []
        for block in res.blocks:
            block_download = FileBlockDownload(CID=block.cid)
            
            if hasattr(block, 'akave') and block.akave:
                block_download.Akave = AkaveBlockData(
                    NodeID=block.akave.node_id,
                    NodeAddress=block.akave.node_address
                )
            elif hasattr(block, 'filecoin') and block.filecoin:
                block_download.Filecoin = FilecoinBlockData(
                    BaseURL=block.filecoin.sp_address
                )
            
            blocks.append(block_download)
        
        return FileChunkDownload(
            CID=chunk.CID,
            Index=chunk.Index,
            EncodedSize=chunk.EncodedSize,
            Size=chunk.Size,
            Blocks=blocks
        )
    
    def _download_chunk_blocks(self, ctx, stream_id, chunk_download, file_encryption_key, writer):
        """
        Download all blocks for a chunk and write the combined data to the writer.
        
        Args:
            ctx: Context object
            stream_id: Stream ID
            chunk_download: FileChunkDownload object
            file_encryption_key: Encryption key for the file
            writer: Writer to write the data to
            
        Raises:
            SDKError: If there's an error downloading the blocks
        """
        try:
            # Create a connection pool
            pool = ConnectionPool()
            
            try:
                # Download blocks in parallel
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                    futures = {}
                    
                    # Submit download tasks for each block
                    for i, block in enumerate(chunk_download.Blocks):
                        futures[executor.submit(
                            self._fetch_block_data,
                            ctx, pool, stream_id, chunk_download.CID, 
                            chunk_download.Index, i, block
                        )] = i
                    
                    # Collect results and organize them by position
                    blocks = [None] * len(chunk_download.Blocks)
                    for future in concurrent.futures.as_completed(futures):
                        index = futures[future]
                        try:
                            # Extract data from the block
                            data = future.result()
                            blocks[index] = extract_block_data(chunk_download.Blocks[index].CID, data)
                        except Exception as e:
                            raise SDKError(f"failed to download block: {str(e)}")
                
                # Combine blocks based on whether erasure coding is used
                if self.erasure_code is not None:
                    # Use erasure coding to extract data
                    data = self.erasure_code.extract_data(blocks, int(chunk_download.Size))
                else:
                    # Simple concatenation of blocks
                    data = b"".join([b for b in blocks if b is not None])
                
                # Decrypt the data if an encryption key is provided
                if file_encryption_key:
                    data = decrypt(file_encryption_key, data, str(chunk_download.Index).encode())
                
                # Write the data
                writer.write(data)
                
            finally:
                # Close the connection pool
                pool.close()
            
            return None
        except Exception as err:
            raise SDKError(f"failed to download chunk blocks: {str(err)}")
    
    def _download_random_chunk_blocks(self, ctx, stream_id, chunk_download, file_encryption_key, writer):
        """
        Download a random subset of blocks for a chunk (works only with erasure coding).
        
        Args:
            ctx: Context object
            stream_id: Stream ID
            chunk_download: FileChunkDownload object
            file_encryption_key: Encryption key for the file
            writer: Writer to write the data to
            
        Raises:
            SDKError: If there's an error downloading the blocks
        """
        try:
            # Create a connection pool
            pool = ConnectionPool()
            
            try:
                # Download blocks in parallel
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                    futures = {}
                    
                    # Create a map of all blocks
                    blocks_map = {i: block for i, block in enumerate(chunk_download.Blocks)}
                    
                    # Get the block indexes and randomize them
                    block_indexes = list(blocks_map.keys())
                    random.shuffle(block_indexes)
                    
                    # Take only the necessary number of blocks (data blocks for erasure coding)
                    for i in block_indexes[:self.erasure_code.data_blocks]:
                        del blocks_map[i]
                    
                    # Submit download tasks for each selected block
                    for index, block in blocks_map.items():
                        futures[executor.submit(
                            self._fetch_block_data,
                            ctx, pool, stream_id, chunk_download.CID, 
                            chunk_download.Index, index, block
                        )] = index
                    
                    # Collect results and organize them by position
                    blocks = [None] * len(chunk_download.Blocks)
                    for future in concurrent.futures.as_completed(futures):
                        index = futures[future]
                        try:
                            # Extract data from the block
                            data = future.result()
                            blocks[index] = extract_block_data(chunk_download.Blocks[index].CID, data)
                        except Exception as e:
                            raise SDKError(f"failed to download block: {str(e)}")
                
                # Use erasure coding to extract data
                data = self.erasure_code.extract_data(blocks, int(chunk_download.Size))
                
                # Decrypt the data if an encryption key is provided
                if file_encryption_key:
                    data = decrypt(file_encryption_key, data, str(chunk_download.Index).encode())
                
                # Write the data
                writer.write(data)
                
            finally:
                # Close the connection pool
                pool.close()
            
            return None
        except Exception as err:
            raise SDKError(f"failed to download random chunk blocks: {str(err)}")
    
    def _fetch_block_data(self, ctx, pool, stream_id, chunk_cid, chunk_index, block_index, block):
        """
        Fetch block data from either Akave or Filecoin.
        
        Args:
            ctx: Context object
            pool: Connection pool
            stream_id: Stream ID
            chunk_cid: Chunk CID
            chunk_index: Chunk index
            block_index: Block index
            block: FileBlockDownload object
            
        Returns:
            bytes: Block data
            
        Raises:
            SDKError: If there's an error fetching the block data
        """
        try:
            # Check if block metadata is available
            if block.Akave is None and block.Filecoin is None:
                raise SDKError("missing block metadata")
            
            # If Filecoin data is available, fetch from Filecoin
            if block.Filecoin is not None:
                try:
                    # Decode the CID
                    cid_obj = cidlib.decode(block.CID)
                    # Fetch the block from Filecoin
                    data = self.sp_client.fetch_block(ctx, block.Filecoin.BaseURL, cid_obj)
                    # Return the raw data
                    return data.raw_data()
                except Exception as e:
                    raise SDKError(f"failed to fetch block from Filecoin: {str(e)}")
            
            # Otherwise, fetch from Akave
            client, closer = pool.create_streaming_client(block.Akave.NodeAddress, self.use_connection_pool)
            
            try:
                # Create download request
                download_req = nodeapi_pb2.StreamFileDownloadBlockRequest(
                    stream_id=stream_id,
                    chunk_cid=chunk_cid,
                    chunk_index=chunk_index,
                    block_cid=block.CID,
                    block_index=block_index
                )
                
                # Send the request
                download_client = client.FileDownloadBlock(ctx, download_req)
                
                # Receive the data
                buffer = io.BytesIO()
                while True:
                    try:
                        # Receive a block
                        block_data = download_client.recv()
                        if not block_data:
                            break
                        # Write the data to the buffer
                        buffer.write(block_data.data)
                    except Exception as e:
                        # Break on EOF
                        if isinstance(e, io.EOF) or "EOF" in str(e):
                            break
                        # Raise other errors
                        raise SDKError(f"error receiving block data: {str(e)}")
                
                # Return the complete data
                return buffer.getvalue()
                
            finally:
                # Close the client connection if not using the pool
                if closer:
                    closer()
                    
        except Exception as e:
            raise SDKError(f"failed to fetch block data: {str(e)}")
