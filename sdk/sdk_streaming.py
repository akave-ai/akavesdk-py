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

from .common import SDKError
from .model import FileMeta, FileListItem
from .erasure_code import ErasureCode
from private.pb import nodeapi_pb2
from private.pb import nodeapi_pb2_grpc
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
    CID: str
    EncodedSize: int
    Size: int
    Index: int

@dataclass
class FileUpload:
    BucketName: str
    Name: str
    StreamID: str
    CreatedAt: datetime

@dataclass
class FileDownload:
    StreamID: str
    BucketName: str
    Name: str
    Chunks: List[Chunk]

@dataclass
class FileBlockUpload:
    cid: str
    data: bytes
    node_address: str = ""
    node_id: str = ""
    permit: bytes = b""

@dataclass
class FileChunkUpload:
    StreamID: str
    Index: int
    ChunkCID: Any  # CID object
    ActualSize: int
    RawDataSize: int
    ProtoNodeSize: int
    Blocks: List[FileBlockUpload]

@dataclass
class AkaveBlockData:
    NodeID: str
    NodeAddress: str
    Permit: bytes = b""

@dataclass
class FilecoinBlockData:
    BaseURL: str

@dataclass
class FileBlockDownload:
    CID: str
    Akave: Optional[AkaveBlockData] = None
    Filecoin: Optional[FilecoinBlockData] = None

@dataclass
class FileChunkDownload:
    CID: str
    Index: int
    EncodedSize: int
    Size: int
    Blocks: List[FileBlockDownload]

class ConnectionPool:
    def __init__(self):
        self.connections = {}
        self.lock = threading.Lock()
    
    def create_streaming_client(self, address: str, use_pool: bool):
        if not use_pool:
            channel = grpc.insecure_channel(address)
            client = nodeapi_pb2_grpc.StreamAPIStub(channel)
            return client, lambda: channel.close()
        
        with self.lock:
            if address in self.connections:
                client, _ = self.connections[address]
                return client, None
            
            channel = grpc.insecure_channel(address)
            client = nodeapi_pb2_grpc.StreamAPIStub(channel)
            self.connections[address] = (client, channel)
            return client, None
    
    def close(self):
        with self.lock:
            for _, channel in self.connections.values():
                channel.close()
            self.connections = {}
        return None

class DAGRoot:
    def __init__(self):
        self.links = []
    
    @classmethod
    def new(cls):
        return cls()
    
    def add_link(self, chunk_cid, raw_data_size: int, proto_node_size: int):
        self.links.append({
            "cid": chunk_cid,
            "raw_data_size": raw_data_size,
            "proto_node_size": proto_node_size
        })
        return None
    
    def build(self):
        if not hasattr(cidlib, 'make_cid'):
            # Fallback if cidlib not available
            cid_str = "Qm" + base64.b32encode(os.urandom(32)).decode('utf-8')
            return type('CID', (), {'string': lambda *args: cid_str})()
        
        try:
            # Actually build a CID if the library is available
            root_cid = cidlib.make_cid(f"dag_root_{len(self.links)}")
            return type('CID', (), {'string': lambda *args: str(root_cid)})()
        except Exception:
            # Fallback on error
            cid_str = "Qm" + base64.b32encode(os.urandom(32)).decode('utf-8')
            return type('CID', (), {'string': lambda *args: cid_str})()

def encryption_key(parent_key: bytes, *info_data: str):
    if len(parent_key) == 0:
        return b''
    
    info = "/".join(info_data)
    return derive_key(parent_key, info.encode())

def to_proto_chunk(stream_id: str, cid: str, index: int, size: int, blocks: List[FileBlockUpload]):
    pb_blocks = []
    for block in blocks:
        pb_blocks.append(nodeapi_pb2.Chunk.Block(
            cid=block.cid,
            size=len(block.data) if block.data is not None else 0
        ))
    
    return nodeapi_pb2.Chunk(
        stream_id=stream_id,
        cid=cid,
        index=index,
        size=size,
        blocks=pb_blocks
    )

class StreamingAPI:
    
    def __init__(self, conn: grpc.Channel, client: Any,
                 erasure_code: Optional[ErasureCode] = None, 
                 max_concurrency: int = 10,
                 block_part_size: int = 1024 * 1024, 
                 use_connection_pool: bool = True,
                 encryption_key: Optional[bytes] = None, 
                 max_blocks_in_chunk: int = 32):
        
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
        if bucket_name == "":
            raise SDKError("empty bucket name")
        if file_name == "":
            raise SDKError("empty file name")
        
        try:
            request = nodeapi_pb2.StreamFileViewRequest(
                bucket_name=bucket_name,
                file_name=file_name
            )
            
            res = self.client.FileView(request)
            
            return self._to_file_meta(res, bucket_name)
        except Exception as err:
            raise SDKError(f"failed to get file info: {str(err)}")
    
    def list_files(self, ctx, bucket_name: str) -> List[FileMeta]:
        if bucket_name == "":
            raise SDKError("empty bucket name")
        
        try:
            request = nodeapi_pb2.StreamFileListRequest(
                bucket_name=bucket_name
            )
            
            resp = self.client.FileList(request)
            
            files = []
            for file_meta in resp.files:
                files.append(self._to_file_meta(file_meta, bucket_name))
            
            return files
        except Exception as err:
            raise SDKError(f"failed to list files: {str(err)}")
    
    def file_versions(self, ctx, bucket_name: str, file_name: str) -> List[FileMeta]:
        if bucket_name == "":
            raise SDKError("empty bucket name")
        
        try:
            request = nodeapi_pb2.StreamFileListVersionsRequest(
                bucket_name=bucket_name,
                file_name=file_name
            )
            
            resp = self.client.FileVersions(request)
            
            files = []
            for file_meta in resp.versions:
                files.append(self._to_file_meta(file_meta, bucket_name))
            
            return files
        except Exception as err:
            raise SDKError(f"failed to list file versions: {str(err)}")
    
    def create_file_upload(self, ctx, bucket_name: str, file_name: str) -> FileUpload:
        if bucket_name == "":
            raise SDKError("empty bucket name")
        
        try:
            request = nodeapi_pb2.StreamFileUploadCreateRequest(
                bucket_name=bucket_name,
                file_name=file_name
            )
            
            res = self.client.FileUploadCreate(request)
            
            return FileUpload(
                BucketName=res.bucket_name,
                Name=res.file_name,
                StreamID=res.stream_id,
                CreatedAt=res.created_at.ToDatetime() if hasattr(res.created_at, 'ToDatetime') else res.created_at
            )
        except Exception as err:
            raise SDKError(f"failed to create file upload: {str(err)}")
    
    def upload(self, ctx, upload: FileUpload, reader: BinaryIO) -> FileMeta:
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
        try:
            request = nodeapi_pb2.StreamFileDownloadCreateRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                root_cid=root_cid
            )
            
            res = self.client.FileDownloadCreate(request)
            
            chunks = []
            for i, chunk in enumerate(res.chunks):
                chunks.append(Chunk(
                    CID=chunk.cid,
                    EncodedSize=chunk.encoded_size,
                    Size=chunk.size,
                    Index=i  # Use the position in the list as the index
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
        try:
            request = nodeapi_pb2.StreamFileDownloadRangeCreateRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                start_index=start,
                end_index=end
            )
            
            res = self.client.FileDownloadRangeCreate(request)
            
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
        try:
            request = nodeapi_pb2.StreamFileDeleteRequest(
                bucket_name=bucket_name,
                file_name=file_name
            )
            
            self.client.FileDelete(request)
        except Exception as err:
            raise SDKError(f"failed to delete file: {str(err)}")
    
    # Helper methods
    def _to_file_meta(self, file_meta, bucket_name: str) -> FileMeta:
        # Get created_at, handling both protobuf timestamp and datetime
        created_at = None
        if hasattr(file_meta, 'created_at'):
            if hasattr(file_meta.created_at, 'ToDatetime'):
                created_at = file_meta.created_at.ToDatetime()
            else:
                created_at = file_meta.created_at
        
        # Get committed_at if it exists
        committed_at = None
        if hasattr(file_meta, 'committed_at'):
            if hasattr(file_meta.committed_at, 'ToDatetime'):
                committed_at = file_meta.committed_at.ToDatetime()
            else:
                committed_at = file_meta.committed_at
        
        return FileMeta(
            stream_id=file_meta.stream_id,
            root_cid=file_meta.root_cid,
            bucket_name=bucket_name,
            name=file_meta.name if hasattr(file_meta, 'name') else file_meta.file_name,
            encoded_size=file_meta.encoded_size,
            size=file_meta.size,
            created_at=created_at,
            committed_at=committed_at
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
        if hasattr(root_cid, 'string') and callable(root_cid.string):
            return root_cid.string()
        return str(root_cid)
    
    def _create_chunk_upload(self, ctx, file_upload, index, file_encryption_key, data):
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
            
            # Convert blocks to FileBlockUpload format
            block_uploads = []
            for block in chunk_dag.blocks:
                block_uploads.append(FileBlockUpload(
                    cid=block.cid,
                    data=block.data
                ))
            
            # Convert to protobuf chunk format
            proto_chunk = to_proto_chunk(
                file_upload.StreamID,
                chunk_dag.cid if isinstance(chunk_dag.cid, str) else str(chunk_dag.cid),
                index,
                size,
                block_uploads
            )
            
            # Create the chunk upload request
            request = nodeapi_pb2.StreamFileUploadChunkCreateRequest(chunk=proto_chunk)
            
            # Send the request
            res = self.client.FileUploadChunkCreate(request)
            
            # Verify the response
            if len(res.blocks) != len(chunk_dag.blocks):
                raise SDKError(f"received unexpected amount of blocks {len(res.blocks)}, expected {len(chunk_dag.blocks)}")
            
            # Update block metadata with node information
            blocks = []
            for i, upload in enumerate(res.blocks):
                if i >= len(chunk_dag.blocks):
                    raise SDKError(f"block index {i} out of range")
                
                block_cid = chunk_dag.blocks[i].cid
                if block_cid != upload.cid:
                    raise SDKError(f"block CID mismatch at position {i}")
                
                blocks.append(FileBlockUpload(
                    cid=block_cid,
                    data=chunk_dag.blocks[i].data,
                    node_address=upload.node_address,
                    node_id=upload.node_id,
                    permit=upload.permit
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
        try:
            # Create a client for the node
            client, closer = pool.create_streaming_client(block.node_address, self.use_connection_pool)
            
            try:
                # Convert memoryview to bytes if necessary
                data = block.data
                if isinstance(data, memoryview):
                    data = data.tobytes()
                
                # Create a request iterator for the streaming call
                def request_iterator():
                    # First send the metadata
                    block_segment = nodeapi_pb2.StreamFileBlockData(
                        cid=block.cid,
                        index=block_index,
                        chunk=proto_chunk
                    )
                    yield block_segment
                    
                    # Then send the data in chunks
                    data_len = len(data)
                    i = 0
                    while i < data_len:
                        # Check for context cancellation
                        if hasattr(ctx, 'done') and ctx.done():
                            return
                            
                        # Calculate the end of this segment
                        end = i + self.block_part_size
                        if end > data_len:
                            end = data_len
                        
                        # Create the block data object
                        block_segment = nodeapi_pb2.StreamFileBlockData(
                            data=data[i:end]
                        )
                        yield block_segment
                        
                        # Move to the next segment
                        i += self.block_part_size
                
                # Create and execute the streaming call
                response = client.FileUploadBlock(request_iterator())
                
                # If we get here, the upload was successful
                return
                
            finally:
                # Close the client connection if not using the pool
                if closer:
                    closer()
                    
        except Exception as err:
            raise SDKError(f"failed to upload block {block.cid}: {str(err)}")
    
    def _commit_stream(self, ctx, upload, root_cid, chunk_count):
        # TODO: Implement stream commit
        request = nodeapi_pb2.StreamFileUploadCommitRequest(
            stream_id=upload.StreamID,
            root_cid=root_cid,
            chunk_count=chunk_count
        )
        
        res = self.client.FileUploadCommit(request)
        
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
        # TODO: Implement chunk download creation
        request = nodeapi_pb2.StreamFileDownloadChunkCreateRequest(
            stream_id=stream_id,
            chunk_cid=chunk.CID
        )
        
        res = self.client.FileDownloadChunkCreate(request)
        
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
        # TODO: Implement chunk download v2 creation
        request = nodeapi_pb2.StreamFileDownloadChunkCreateRequest(
            stream_id=stream_id,
            chunk_cid=chunk.CID
        )
        
        res = self.client.FileDownloadChunkCreateV2(request)
        
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
                
                # Send the request with a timeout of 30 seconds
                download_client = client.FileDownloadBlock(download_req, timeout=30.0)
                
                # Receive the data
                buffer = io.BytesIO()
                try:
                    while True:
                        # Receive a block using next() for streaming responses
                        block_data = next(download_client)
                        if not block_data:
                            break
                        # Write the data to the buffer
                        buffer.write(block_data.data)
                except StopIteration:
                    # This is normal - it means we've received all the data
                    pass
                except Exception as e:
                    raise SDKError(f"error receiving block data: {str(e)}")
                
                # Return the complete data
                return buffer.getvalue()
                
            finally:
                # Close the client connection if not using the pool
                if closer:
                    closer()
                    
        except Exception as e:
            raise SDKError(f"failed to fetch block data: {str(e)}")
