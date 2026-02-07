import io
import logging
import concurrent.futures
from typing import List, Optional, Dict, Any, Callable, BinaryIO, Tuple
import time
from datetime import datetime
import threading
import os
import random
import base64
from dataclasses import dataclass

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from .config import SDKError, SDKConfig, BlockSize, EncryptionOverhead
from .connection import ConnectionPool
from .model import (
    FileMeta, FileListItem, Chunk, FileUpload, FileDownload, FileBlockUpload, 
    FileChunkUpload, AkaveBlockData, FilecoinBlockData, FileBlockDownload, FileChunkDownload,
)
from .erasure_code import ErasureCode
from private.pb import nodeapi_pb2
from private.pb import nodeapi_pb2_grpc
from private.spclient.spclient import SPClient
from private.encryption import encrypt, decrypt, derive_key
from sdk.dag import build_dag, extract_block_data

from multiformats import cid as cidlib


logger = logging.getLogger(__name__)




class DAGRoot:
    def __init__(self) -> None:
        self.links: List[Dict[str, Any]] = []
    
    @classmethod
    def new(cls) -> 'DAGRoot':
        """Create a new DAG root instance."""
        return cls()
    
    def add_link(self, chunk_cid: Any, raw_data_size: int, proto_node_size: int) -> None:
        self.links.append({
            "cid": chunk_cid,
            "raw_data_size": raw_data_size,
            "proto_node_size": proto_node_size
        })
        return None
    
    def build(self) -> Any:
        cid_str = "Qm" + base64.b32encode(os.urandom(32)).decode('utf-8')
        return type('CID', (), {'string': lambda *args: cid_str})()

def encryption_key(parent_key: bytes, *info_data: str) -> bytes:
    if len(parent_key) == 0:
        return b''
    
    info = "/".join(info_data)
    return derive_key(parent_key, info.encode())

def to_proto_chunk(stream_id: str, cid: str, index: int, size: int, blocks: List[FileBlockUpload]) -> nodeapi_pb2.Chunk:
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
    
    def __init__(
        self, 
        conn: grpc.Channel, 
        client: Any,
        config: SDKConfig
    ) -> None:
        
        self.client = client
        self.conn = conn
        self.sp_client: SPClient = SPClient()
        self.erasure_code = config.erasure_code
        self.max_concurrency = config.max_concurrency
        self.block_part_size = config.block_part_size
        self.use_connection_pool = config.use_connection_pool
        self.encryption_key = config.encryption_key if config.encryption_key else b''
        self.max_blocks_in_chunk = config.streaming_max_blocks_in_chunk
    
    def file_info(self, ctx: Any, bucket_name: str, file_name: str) -> FileMeta:
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
    

    def list_files(self, ctx: Any, bucket_name: str) -> List[FileMeta]:
        if bucket_name == "":
            raise SDKError("empty bucket name")
        
        try:
            request = nodeapi_pb2.StreamFileListRequest(
                bucket_name=bucket_name
            ) # type: ignore[attr-defined]
            
            resp = self.client.FileList(request)
            
            files = []
            for file_meta in resp.files:
                files.append(self._to_file_meta(file_meta, bucket_name))
            
            return files
        except Exception as err:
            raise SDKError(f"failed to list files: {str(err)}")
    
    def file_versions(self, ctx: Any, bucket_name: str, file_name: str) -> List[FileMeta]:
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

    def create_file_upload(self, ctx: Any, bucket_name: str, file_name: str) -> FileUpload:
        if bucket_name == "":
            raise SDKError("empty bucket name")
        
        try:
            request = nodeapi_pb2.StreamFileUploadCreateRequest(
                bucket_name=bucket_name,
                file_name=file_name
            )
            
            res = self.client.FileUploadCreate(request)
            
            return FileUpload(
                bucket_name=res.bucket_name,
                name=res.file_name,
                stream_id=res.stream_id,
                created_at=res.created_at.ToDatetime() if hasattr(res.created_at, 'ToDatetime') else datetime.now()
            )
        except Exception as err:
            raise SDKError(f"failed to create file upload: {str(err)}")

    def upload(self, ctx: Any, upload: FileUpload, reader: BinaryIO) -> FileMeta:
        try:
            chunk_enc_overhead = 0
            file_enc_key = b''
            if self.encryption_key:
                # Derive file-specific encryption key using bucket and file name
                file_enc_key = encryption_key(self.encryption_key, upload.bucket_name, upload.name)
                chunk_enc_overhead = EncryptionOverhead
            
            is_empty_file = True
            
            buffer_size = self.max_blocks_in_chunk * BlockSize
            if self.erasure_code is not None:
                buffer_size = self.erasure_code.data_blocks * BlockSize
            buffer_size -= chunk_enc_overhead
            buf = bytearray(buffer_size)
            
            # Create DAG root for tracking file chunks
            dag_root = self._create_dag_root()
            
            chunk_index = 0
            while True:
                # Check for context cancellation
                if hasattr(ctx, 'done') and ctx.done():
                    raise SDKError("upload cancelled by context")
                
                # Read data from reader
                n = reader.readinto(buf) # type: ignore[attr-defined]
                if n == 0:
                    if is_empty_file:
                        raise SDKError("empty file")
                    break
                is_empty_file = False
                
                # Create chunk upload
                chunk_upload = self._create_chunk_upload(ctx, upload, chunk_index, file_enc_key, buf[:n])
                
                # Add link to DAG root
                self._add_dag_link(dag_root, chunk_upload.chunk_cid, chunk_upload.raw_data_size, chunk_upload.encoded_size)
                
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
    
    def create_file_download(self, ctx: Any, bucket_name: str, file_name: str, root_cid: str = "") -> FileDownload:
        try:
            request = nodeapi_pb2.StreamFileDownloadCreateRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                root_cid=root_cid
            )
            
            res = self.client.FileDownloadCreate(request)
            
            chunks = [Chunk(cid=c.cid, encoded_size=c.encoded_size, size=c.size, index=i) for i, c in enumerate(res.chunks)]
            return FileDownload(stream_id=res.stream_id, bucket_name=res.bucket_name, name=file_name, chunks=chunks)

        except Exception as err:
            raise SDKError(f"failed to create file download: {str(err)}")
    
    def create_range_file_download(self, ctx: Any, bucket_name: str, file_name: str, start: int, end: int) -> FileDownload:
        try:
            request = nodeapi_pb2.StreamFileDownloadRangeCreateRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                start_index=start,
                end_index=end
            )
            
            res = self.client.FileDownloadRangeCreate(request)
            
            chunks = [Chunk(cid=c.cid, encoded_size=c.encoded_size, size=c.size, index=c.index + start) for c in res.chunks]
            return FileDownload(
                stream_id=res.stream_id,
                bucket_name=res.bucket_name,
                name=file_name,
                chunks=chunks
            )
        except Exception as err:
            raise SDKError(f"failed to create file download range: {str(err)}")
    
    def download(self, ctx, file_download: FileDownload, writer: BinaryIO) -> None:
        try:
            file_enc_key = b''
            if self.encryption_key:
                # Derive file-specific encryption key using bucket and file name
                file_enc_key = encryption_key(self.encryption_key, file_download.bucket_name, file_download.name)
            
            for chunk in file_download.chunks:
                # Check for context cancellation
                if hasattr(ctx, 'done') and ctx.done():
                    return
                
                # Create chunk download
                chunk_download = self._create_chunk_download(ctx, file_download.stream_id, chunk)

                # Download chunk blocks
                self._download_chunk_blocks(ctx, file_download.stream_id, chunk_download, file_enc_key, writer)

        except Exception as err:
            raise SDKError(f"failed to download file: {str(err)}")
    

    def download_v2(self, ctx, file_download: FileDownload, writer: BinaryIO) -> None:
        try:
            file_enc_key = b''
            if self.encryption_key:
                # Derive file-specific encryption key using bucket and file name
                file_enc_key = encryption_key(self.encryption_key, file_download.bucket_name, file_download.name)
            
            for chunk in file_download.chunks:
                # Check for context cancellation
                if hasattr(ctx, 'done') and ctx.done():
                    return
                
                # Create chunk download v2
                chunk_download = self._create_chunk_download_v2(ctx, file_download.stream_id, chunk)

                # Download chunk blocks
                self._download_chunk_blocks(ctx, file_download.stream_id, chunk_download, file_enc_key, writer)

        except Exception as err:
            raise SDKError(f"failed to download file: {str(err)}")
    

    def download_random(self, ctx, file_download: FileDownload, writer: BinaryIO) -> None:
        if self.erasure_code is None:
            raise SDKError("erasure coding is not enabled")
        
        try:
            file_enc_key = b''
            if self.encryption_key:
                # Derive file-specific encryption key using bucket and file name
                file_enc_key = encryption_key(self.encryption_key, file_download.bucket_name, file_download.name)

            for chunk in file_download.chunks:
                # Check for context cancellation
                if hasattr(ctx, 'done') and ctx.done():
                    return

                # Create chunk download
                chunk_download = self._create_chunk_download(ctx, file_download.stream_id, chunk)

                # Download random chunk blocks
                self._download_random_chunk_blocks(ctx, file_download.stream_id, chunk_download, file_enc_key, writer)

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
    def _to_file_meta(self, file_meta: Any, bucket_name: str) -> FileMeta:
        # Get created_at, handling both protobuf timestamp and datetime
        created_at_dt = datetime.fromtimestamp(0)
        if hasattr(file_meta, 'created_at') and file_meta.created_at.seconds > 0:
            created_at_dt = file_meta.created_at.ToDatetime()
        
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
            created_at=created_at_dt,
            committed_at=committed_at
        )
    
    # Updated DAG operation methods
    def _create_dag_root(self) -> DAGRoot:
        """Create a new DAG root"""
        return DAGRoot.new()
    
    def _add_dag_link(self, dag_root: DAGRoot, chunk_cid: Any, raw_size: int, proto_node_size: int) -> None:
        """Add a link to the DAG root"""
        try:
            dag_root.add_link(chunk_cid, raw_size, proto_node_size)
        except Exception as err:
            raise SDKError(f"failed to add DAG link: {str(err)}")
    
    def _build_dag_root(self, dag_root: DAGRoot) -> str:
        """Build the DAG root and return its CID"""
        root_cid = dag_root.build()
        if hasattr(root_cid, 'string') and callable(root_cid.string):
            return str(root_cid.string())
        return str(root_cid)
    
    def _create_chunk_upload(self, ctx: Any, file_upload: FileUpload, index: int, file_encryption_key: bytes, data: bytes) -> FileChunkUpload:
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
                file_upload.stream_id,
                chunk_dag.cid if isinstance(chunk_dag.cid, str) else str(chunk_dag.cid),
                index,
                size,
                block_uploads
            )
            
            # Create the chunk upload request
            request = nodeapi_pb2.StreamFileUploadChunkCreateRequest()
            request.chunk.CopyFrom(proto_chunk)
            
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
                stream_id=file_upload.stream_id,
                index=index,
                chunk_cid=chunk_dag.cid,
                raw_data_size=chunk_dag.raw_data_size,
                encoded_size=chunk_dag.proto_node_size,  # proto_node_size maps to encoded_size
                blocks=blocks
            )
        except Exception as err:
            raise SDKError(f"failed to create chunk upload: {str(err)}")
    
    def _upload_chunk(self, ctx: Any, file_chunk_upload: FileChunkUpload) -> None:
        try:
            pool = ConnectionPool()
            
            try:
                # Convert to protobuf chunk format for sending
                proto_chunk = to_proto_chunk(
                    file_chunk_upload.stream_id,
                    file_chunk_upload.chunk_cid.string() if hasattr(file_chunk_upload.chunk_cid, 'string') else str(file_chunk_upload.chunk_cid),
                    file_chunk_upload.index,
                    file_chunk_upload.encoded_size,  # Use encoded_size instead of actual_size
                    file_chunk_upload.blocks
                )
                
                # Upload blocks in parallel
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                    futures = []

                    for i, block in enumerate(file_chunk_upload.blocks):
                        futures.append(executor.submit(
                            self._upload_block,
                            ctx, pool, i, block, proto_chunk
                        ))
                    
                    # Wait for all futures to complete and raise any errors
                    for future in concurrent.futures.as_completed(futures):
                        future.result()
                        
            finally:
                # Close the connection pool
                try:
                    err = pool.close()
                    if err:
                        logging.warning(f"failed to close connection pool: {err}")
                except Exception as e:
                    logging.warning(f"failed to close connection pool: {e}")
            
            return None
        except Exception as err:
            raise SDKError(f"failed to upload chunk: {str(err)}")

    def _upload_block(self, ctx: Any, pool: ConnectionPool, block_index: int, block: FileBlockUpload, proto_chunk: Any) -> None:
        try:
            # Create a client for the node
            client, closer, err = pool.create_streaming_client(block.node_address, self.use_connection_pool)
            if err:
                raise SDKError(f"Failed to create streaming client: {str(err)}")
            
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

    def _commit_stream(self, ctx: Any, upload: FileUpload, root_cid: str, chunk_count: int) -> FileMeta:
        """Commit the upload stream and finalize the file metadata."""
        request = nodeapi_pb2.StreamFileUploadCommitRequest(
            stream_id=upload.stream_id,
            root_cid=root_cid,
            chunk_count=chunk_count
        )
        
        res = self.client.FileUploadCommit(request)

        created_at_dt: datetime
        if isinstance(upload.created_at, datetime):
            created_at_dt = upload.created_at
        elif isinstance(upload.created_at, (float, int)):
            created_at_dt = datetime.fromtimestamp(upload.created_at)
        else:
            raise TypeError(f"Unexpected type for upload.created_at: {type(upload.created_at)}")
        
        return FileMeta(
            stream_id=res.stream_id,
            root_cid=root_cid,
            bucket_name=res.bucket_name,
            name=res.file_name,
            encoded_size=res.encoded_size,
            size=res.size,
            created_at=created_at_dt,
            committed_at=res.committed_at.ToDatetime() if hasattr(res.committed_at, 'ToDatetime') else res.committed_at
        )

    def _create_chunk_download(self, ctx: Any, stream_id: str, chunk: Chunk) -> FileChunkDownload:
        """Create a chunk download request and parse the response blocks."""
        request = nodeapi_pb2.StreamFileDownloadChunkCreateRequest(
            stream_id=stream_id,
            chunk_cid=chunk.cid,
        )
        
        res = self.client.FileDownloadChunkCreate(request)
        
        blocks = []
        for block in res.blocks:
            blocks.append(FileBlockDownload(
                cid=block.cid,
                akave=AkaveBlockData(
                    node_id=block.node_id,
                    node_address=block.node_address,
                    permit=block.permit
                ),
                filecoin=FilecoinBlockData(
                    base_url=block.filecoin.sp_address
                ),
                data=block.data if hasattr(block, 'data') and block.data is not None else b''
            ))
        
        return FileChunkDownload(
            cid=chunk.cid,
            index=chunk.index,
            encoded_size=chunk.encoded_size,
            size=chunk.size,
            blocks=blocks
        )
    
    def _create_chunk_download_v2(self, ctx: Any, stream_id: str, chunk: Chunk) -> FileChunkDownload:
        """Create a chunk download request (v2) and parse the response blocks with provider metadata."""
        request = nodeapi_pb2.StreamFileDownloadChunkCreateRequest(
            stream_id=stream_id,
            chunk_cid=chunk.cid
        )

        res = self.client.FileDownloadChunkCreateV2(request)

        blocks = []
        for block in res.blocks:
            # Handle optional data field - may not be present in all responses
            block_data = block.data if hasattr(block, 'data') and block.data is not None else b''
            block_download = FileBlockDownload(
                cid=block.cid,
                data=block_data,
            )
            if hasattr(block, 'akave') and block.akave:
                block_download.akave = AkaveBlockData(
                    node_id=block.akave.node_id,
                    node_address=block.akave.node_address,
                    permit=block.akave.permit
                )
            elif hasattr(block, 'filecoin') and block.filecoin:
                block_download.filecoin = FilecoinBlockData(
                    base_url=block.filecoin.sp_address
                )

            blocks.append(block_download)
        
        return FileChunkDownload(
            cid=chunk.cid,
            index=chunk.index,
            encoded_size=chunk.encoded_size,
            size=chunk.size,
            blocks=blocks
        )
    
    def _download_chunk_blocks(self, ctx: Any, stream_id: str, chunk_download: FileChunkDownload, file_encryption_key: bytes, writer: BinaryIO) -> None:
        try:
            # Create a connection pool
            pool = ConnectionPool()
            
            try:
                # Download blocks in parallel
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                    futures = {}
                    
                    # Submit download tasks for each block
                    for i, block in enumerate(chunk_download.blocks):
                        futures[executor.submit(
                            self._fetch_block_data,
                            ctx, pool, stream_id, chunk_download.cid,
                            chunk_download.index, i, block
                        )] = i
                    
                    # Collect results and organize them by position
                    blocks: List[Optional[bytes]] = [None] * len(chunk_download.blocks)
                    for future in concurrent.futures.as_completed(futures):
                        index = futures[future]
                        try:
                            # Extract data from the block
                            data = future.result()
                            blocks[index] = extract_block_data(chunk_download.blocks[index].cid, data)
                        except Exception as e:
                            raise SDKError(f"failed to download block: {str(e)}")
                
                # Combine blocks based on whether erasure coding is used
                if self.erasure_code is not None:
                    # Use erasure coding to extract data
                    data = self.erasure_code.extract_data(blocks, int(chunk_download.size)) # type: ignore[arg-type]
                else:
                    # Simple concatenation of blocks
                    data = b"".join([b for b in blocks if b is not None])
                
                # Decrypt the data if an encryption key is provided
                if file_encryption_key:
                    data = decrypt(file_encryption_key, data, str(chunk_download.index).encode())

                # Write the data
                writer.write(data)
                
            finally:
                # Close the connection pool
                try:
                    err = pool.close()
                    if err:
                        logging.warning(f"failed to close connection pool: {err}")
                except Exception as e:
                    logging.warning(f"failed to close connection pool: {e}")
            
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
                            ctx, pool, stream_id, chunk_download.cid,
                            chunk_download.index, index, block
                        )] = index
                    
                    # Collect results and organize them by position
                    blocks: List[Optional[bytes]] = [None] * len(chunk_download.blocks)
                    for future in concurrent.futures.as_completed(futures):
                        index = futures[future]
                        try:
                            # Extract data from the block
                            data = future.result()
                            blocks[index] = extract_block_data(chunk_download.blocks[index].cid, data)
                        except Exception as e:
                            raise SDKError(f"failed to download block: {str(e)}")
                
                if self.erasure_code is not None:
                    # Use erasure coding to extract data
                    data = self.erasure_code.extract_data(blocks, int(chunk_download.size)) # type: ignore[arg-type]
                else:
                    # Simple concatenation of blocks
                    data = b"".join([b for b in blocks if b is not None])

                # Decrypt the data if an encryption key is provided
                if file_encryption_key:
                    data = decrypt(file_encryption_key, data, str(chunk_download.index).encode())
                
                # Write the data
                writer.write(data)
                
            finally:
                # Close the connection pool
                try:
                    err = pool.close()
                    if err:
                        logging.warning(f"failed to close connection pool: {err}")
                except Exception as e:
                    logging.warning(f"failed to close connection pool: {e}")
            
            return None
        except Exception as err:
            raise SDKError(f"failed to download random chunk blocks: {str(err)}")
    
    def _fetch_block_data(self, ctx: Any, pool: ConnectionPool, stream_id: str, chunk_cid: str, chunk_index: int, block_index: int, block: FileBlockDownload) -> bytes:
        try:
            # Check if block metadata is available
            if block.akave is None and block.filecoin is None:
                raise SDKError("missing block metadata")
            
            # If Filecoin data is available, fetch from Filecoin
            if block.filecoin is not None:
                try:
                    # Decode the CID
                    cid_obj = cidlib.CID.decode(block.cid)
                    # Fetch the block from Filecoin
                    data = self.sp_client.fetch_block(block.filecoin.base_url, str(cid_obj))
                    # Return the raw data
                    return data
                except Exception as e:
                    raise SDKError(f"failed to fetch block from Filecoin: {str(e)}")
            
            if block.akave is None:
                raise SDKError("missing Akave block metadata")
            # Otherwise, fetch from Akave
            client, closer, err = pool.create_streaming_client(block.akave.node_address, self.use_connection_pool)
            if err:
                raise SDKError(f"Failed to create streaming client: {str(err)}")
            
            try:
                # Create download request
                download_req = nodeapi_pb2.StreamFileDownloadBlockRequest(
                    stream_id=stream_id,
                    chunk_cid=chunk_cid,
                    chunk_index=chunk_index,
                    block_cid=block.cid,
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
