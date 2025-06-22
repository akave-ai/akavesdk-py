import time
import logging
import binascii
import io
import math
import concurrent.futures
from hashlib import sha256
from typing import List, Optional, Callable, Dict, Any, Union, Tuple
from google.protobuf.timestamp_pb2 import Timestamp
from datetime import datetime
import grpc # Add grpc import for error handling
import json
import structlog
from tqdm import tqdm

from .common import MIN_BUCKET_NAME_LENGTH, SDKError, BLOCK_SIZE, ENCRYPTION_OVERHEAD
from .erasure_code import ErasureCode
from .dag import build_dag, extract_block_data
from .connection import ConnectionPool
from .model import (
    IPCBucketCreateResult, IPCBucket, IPCFileMeta, IPCFileListItem,
    IPCFileMetaV2, IPCFileChunkUploadV2, AkaveBlockData, FileBlockUpload,
    FileBlockDownload, Chunk, IPCFileDownload, FileChunkDownload
)
from private.encryption import encrypt, derive_key, decrypt
from private.pb import ipcnodeapi_pb2, ipcnodeapi_pb2_grpc

try:
    from multiformats import cid as cidlib
except ImportError:
    pass

# Configure structlog for JSON formatted logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

BlockSize = BLOCK_SIZE
EncryptionOverhead = ENCRYPTION_OVERHEAD

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
        root_cid = cidlib.make_cid(f"dag_root_{len(self.links)}")
        return root_cid

def encryption_key(parent_key: bytes, *info_data: str):
    if len(parent_key) == 0:
        return b''
    
    info = "/".join(info_data)
    return derive_key(parent_key, info.encode())

def to_ipc_proto_chunk(chunk_cid: str, index: int, size: int, blocks):
    cids = []
    sizes = []
    
    pb_blocks = []
    for block in blocks:
        pb_block = {
            "cid": block["cid"],
            "size": len(block["data"])
        }
        pb_blocks.append(pb_block)
        
        try:
            c = cidlib.decode(block["cid"])
            b_cid = bytearray(32)
            cid_bytes = c.buffer if hasattr(c, 'buffer') else c._buffer
            copy_len = min(len(cid_bytes) - 4, 32)
            b_cid[:copy_len] = cid_bytes[4:4+copy_len]
            cids.append(b_cid)
            sizes.append(len(block["data"]))
        except Exception as e:
            return None, None, None, SDKError(f"failed to decode CID: {str(e)}")
    
    proto_chunk = {
        "cid": chunk_cid,
        "index": index,
        "size": size,
        "blocks": pb_blocks
    }
    
    return cids, sizes, proto_chunk, None

class IPC:
    def __init__(self, client, conn, ipc_instance, max_concurrency, block_part_size, 
                 use_connection_pool, encryption_key=None, max_blocks_in_chunk=32, 
                 erasure_code=None, progress_callback: Optional[Callable[[float, int, int], None]] = None,
                 log_level: str = "INFO", enable_progress_bar: bool = False):
        self.client = client
        self.conn = conn
        self.ipc = ipc_instance
        self.max_concurrency = max_concurrency
        self.block_part_size = block_part_size
        self.use_connection_pool = use_connection_pool
        self.encryption_key = encryption_key if encryption_key else b''
        self.max_blocks_in_chunk = max_blocks_in_chunk
        self.erasure_code = erasure_code
        self.progress_callback = progress_callback
        self.enable_progress_bar = enable_progress_bar
        self.logger = structlog.get_logger()
        self.logger = self.logger.bind(component="IPC")
        self._configure_logging(log_level)

    def _configure_logging(self, log_level: str):
        """Configure logging level and handlers"""
        numeric_level = getattr(logging, log_level.upper(), None)
        if not isinstance(numeric_level, int):
            numeric_level = logging.INFO
        
        logging.basicConfig(
            level=numeric_level,
            format="%(message)s",
            handlers=[logging.StreamHandler()]
        )
        
    def create_bucket(self, ctx, name: str) -> IPCBucketCreateResult:
        start_time = time.time()
        self.logger.info("Starting bucket creation", bucket_name=name)
        
        if len(name) < MIN_BUCKET_NAME_LENGTH:
            raise SDKError("invalid bucket name")

        try:
            # Create bucket using the storage contract
            tx = self.ipc.storage.create_bucket(
                bucket_name=name,
                from_address=self.ipc.auth.address,
                private_key=self.ipc.auth.key,
                gas_limit=500000
            )
            
            # Get transaction receipt
            receipt = self.ipc.web3.eth.wait_for_transaction_receipt(tx)
            
            # Check if transaction was successful
            if receipt.status != 1:
                raise SDKError("bucket creation transaction failed")
                
            # Get creation timestamp from block
            block = self.ipc.web3.eth.get_block(receipt.blockNumber)
            created_at = block.timestamp
            
            result = IPCBucketCreateResult(
                name=name,
                created_at=created_at
            )
            
            self.logger.info(
                "Bucket created successfully",
                bucket_name=name,
                duration_ms=(time.time() - start_time) * 1000,
                tx_hash=receipt.transactionHash.hex()
            )
            return result
            
        except Exception as e:
            self.logger.error(
                "Bucket creation failed",
                bucket_name=name,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000
            )
            raise SDKError(f"bucket creation failed: {e}")

    def view_bucket(self, ctx, bucket_name: str) -> Optional[IPCBucket]:
        start_time = time.time()
        self.logger.debug("Viewing bucket", bucket_name=bucket_name)
        
        if not bucket_name:
            raise SDKError("empty bucket name")

        try:
            request = ipcnodeapi_pb2.IPCBucketViewRequest(
                name=bucket_name,      # Using lowercase as per protobuf definition
                address=self.ipc.auth.address.lower()  # Ensure address is lowercase
            )
            response = self.client.BucketView(request)
            
            if not response:
                self.logger.info(
                    "Bucket not found",
                    bucket_name=bucket_name,
                    duration_ms=(time.time() - start_time) * 1000
                )
                return None
            created_at = 0
            if hasattr(response, 'created_at') and response.created_at:
                created_at = int(response.created_at.seconds)

            return IPCBucket(
                id=response.id if hasattr(response, 'id') else '',
                name=response.name if hasattr(response, 'name') else bucket_name,
                created_at=created_at
            )
            
            self.logger.info(
                "Bucket viewed successfully",
                bucket_name=bucket_name,
                bucket_id=result.id,
                duration_ms=(time.time() - start_time) * 1000
            )
            return result
            
        except grpc.RpcError as e:
            self.logger.error(
                "Bucket view failed",
                bucket_name=bucket_name,
                error_code=e.code(),
                error_details=e.details(),
                duration_ms=(time.time() - start_time) * 1000
            )
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return None
            logging.error(f"IPC view_bucket gRPC failed: {e.code()} - {e.details()}")
            raise SDKError(f"failed to view bucket: {e.details()}")
        except Exception as err:
            self.logger.error(
                "Bucket view failed unexpectedly",
                bucket_name=bucket_name,
                error=str(err),
                duration_ms=(time.time() - start_time) * 1000
            )
            raise SDKError(f"failed to view bucket: {err}")

    def list_buckets(self, ctx) -> list[IPCBucket]:
        start_time = time.time()
        self.logger.debug("Listing buckets")
        
        try:
            request = ipcnodeapi_pb2.IPCBucketListRequest(
                address=self.ipc.auth.address.lower()
            )
            
            response = self.client.BucketList(request)
            buckets = []
            if response and hasattr(response, 'buckets'):
                for bucket in response.buckets:
                    created_at = 0
                    if hasattr(bucket, 'created_at') and bucket.created_at:
                        created_at = int(bucket.created_at.seconds)
                    
                    bucket_name = bucket.name if hasattr(bucket, 'name') else ''
                    bucket_id = bucket.id if hasattr(bucket, 'id') else ''
                    
                    buckets.append(IPCBucket(
                        name=bucket_name,
                        created_at=created_at,
                        id=bucket_id
                    ))
            
            self.logger.info(
                "Buckets listed successfully",
                bucket_count=len(buckets),
                duration_ms=(time.time() - start_time) * 1000
            )
            return buckets
            
        except grpc.RpcError as e:
            self.logger.error(
                "Bucket list failed",
                error_code=e.code(),
                error_details=e.details(),
                duration_ms=(time.time() - start_time) * 1000
            )
            raise SDKError(f"failed to list buckets: {e.details()}")
        except Exception as err:
            self.logger.error(
                "Bucket list failed unexpectedly",
                error=str(err),
                duration_ms=(time.time() - start_time) * 1000
            )
            raise SDKError(f"failed to list buckets: {err}")

    def delete_bucket(self, ctx, name: str) -> None:
        start_time = time.time()
        self.logger.info("Starting bucket deletion", bucket_name=name)
        
        if not name:
            raise SDKError("empty bucket name")
            
        try:
            request = ipcnodeapi_pb2.IPCBucketViewRequest(
                name=name,
                address=self.ipc.auth.address.lower()
            )
            response = self.client.BucketView(request)
            
            if not response:
                self.logger.warning(
                    "Bucket not found for deletion",
                    bucket_name=name,
                    duration_ms=(time.time() - start_time) * 1000
                )
                raise SDKError(f"bucket '{name}' not found")
                
            # If we get here, bucket exists - proceed with deletion
            # Get bucket ID from IPC response like Go SDK does
            bucket_id_hex = response.id if hasattr(response, 'id') and response.id else None
            if not bucket_id_hex:
                raise SDKError(f"bucket ID not available from IPC response")
                
            tx_hash = self.ipc.storage.delete_bucket(
                bucket_name=name,
                from_address=self.ipc.auth.address,
                private_key=self.ipc.auth.key,
                bucket_id_hex=bucket_id_hex  # Pass the bucket ID from IPC response
            )
                
                
            # The storage contract delete_bucket method already handles receipt verification
            # If we get here, the transaction was successful
            self.logger.info(
                "Bucket deleted successfully",
                bucket_name=name,
                tx_hash=tx_hash,
                duration_ms=(time.time() - start_time) * 1000
            )
            return None
                
        except grpc.RpcError as e:
            self.logger.error(
                "Bucket deletion failed",
                bucket_name=name,
                error_code=e.code(),
                error_details=e.details(),
                duration_ms=(time.time() - start_time) * 1000
            )
            if e.code() == grpc.StatusCode.NOT_FOUND:
                raise SDKError(f"bucket '{name}' not found")
            raise SDKError(f"failed to check bucket existence: {e.details()}")
        except Exception as err:
            self.logger.error(
                "Bucket deletion failed unexpectedly",
                bucket_name=name,
                error=str(err),
                duration_ms=(time.time() - start_time) * 1000
            )
            raise SDKError(f"failed to delete bucket: {err}")

    def file_info(self, ctx, bucket_name: str, file_name: str) -> Optional[IPCFileMeta]:
        start_time = time.time()
        self.logger.debug("Fetching file info", bucket_name=bucket_name, file_name=file_name)
        
        if not bucket_name or not file_name:
            raise SDKError("empty bucket name or file name")

        try:
            request = ipcnodeapi_pb2.IPCFileViewRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                address=self.ipc.auth.address.lower()
            )
            response = self.client.FileView(request)
            
            if not response:
                self.logger.info(
                    "File not found",
                    bucket_name=bucket_name,
                    file_name=file_name,
                    duration_ms=(time.time() - start_time) * 1000
                )
                return None
            
            created_at = 0
            if hasattr(response, 'created_at') and response.created_at:
                created_at = int(response.created_at.seconds)
            
            result = IPCFileMeta(
                root_cid=response.root_cid if hasattr(response, 'root_cid') else '',
                name=response.file_name if hasattr(response, 'file_name') else file_name,
                bucket_name=response.bucket_name if hasattr(response, 'bucket_name') else bucket_name,
                encoded_size=response.encoded_size if hasattr(response, 'encoded_size') else 0,
                created_at=created_at
            )
            
            self.logger.info(
                "File info retrieved successfully",
                bucket_name=bucket_name,
                file_name=file_name,
                root_cid=result.root_cid,
                size=result.encoded_size,
                duration_ms=(time.time() - start_time) * 1000
            )
            return result
            
        except grpc.RpcError as e:
            self.logger.error(
                "File info fetch failed",
                bucket_name=bucket_name,
                file_name=file_name,
                error_code=e.code(),
                error_details=e.details(),
                duration_ms=(time.time() - start_time) * 1000
            )
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise SDKError(f"failed to get file info: {e.details()}")
        except Exception as err:
            self.logger.error(
                "File info fetch failed unexpectedly",
                bucket_name=bucket_name,
                file_name=file_name,
                error=str(err),
                duration_ms=(time.time() - start_time) * 1000
            )
            raise SDKError(f"failed to get file info: {err}")

    def list_files(self, ctx, bucket_name: str) -> list[IPCFileListItem]:
        start_time = time.time()
        self.logger.debug("Listing files", bucket_name=bucket_name)
        
        if not bucket_name:
            raise SDKError("empty bucket name")

        try:
            request = ipcnodeapi_pb2.IPCFileListRequest(
                bucket_name=bucket_name,
                address=self.ipc.auth.address.lower()
            )
            response = self.client.FileList(request)
            
            files = []
            if response and hasattr(response, 'list'):
                for file_item in response.list:
                    created_at = 0
                    if hasattr(file_item, 'created_at') and file_item.created_at:
                        created_at = int(file_item.created_at.seconds)
                    
                    file_name = file_item.name if hasattr(file_item, 'name') else ''
                    root_cid = file_item.root_cid if hasattr(file_item, 'root_cid') else ''
                    encoded_size = file_item.encoded_size if hasattr(file_item, 'encoded_size') else 0
                    
                    files.append(IPCFileListItem(
                        name=file_name,
                        root_cid=root_cid,
                        encoded_size=encoded_size,
                        created_at=created_at
                    ))
            
            self.logger.info(
                "Files listed successfully",
                bucket_name=bucket_name,
                file_count=len(files),
                duration_ms=(time.time() - start_time) * 1000
            )
            return files
            
        except grpc.RpcError as e:
            self.logger.error(
                "File list failed",
                bucket_name=bucket_name,
                error_code=e.code(),
                error_details=e.details(),
                duration_ms=(time.time() - start_time) * 1000
            )
            raise SDKError(f"failed to list files: {e.details()}")
        except Exception as err:
            self.logger.error(
                "File list failed unexpectedly",
                bucket_name=bucket_name,
                error=str(err),
                duration_ms=(time.time() - start_time) * 1000
            )
            raise SDKError(f"failed to list files: {err}")

    def file_delete(self, ctx, bucket_name: str, file_name: str) -> None:
        start_time = time.time()
        self.logger.info("Starting file deletion", bucket_name=bucket_name, file_name=file_name)
        
        if not bucket_name.strip() or not file_name.strip():
            raise SDKError(f"empty bucket or file name. Bucket: '{bucket_name}', File: '{file_name}'")

        try:
            # Delete file using storage contract
            self.ipc.storage.delete_file(
                bucket_name,
                file_name,
                self.ipc.auth.address, 
                self.ipc.auth.key
            )
            
            self.logger.info(
                "File deleted successfully",
                bucket_name=bucket_name,
                file_name=file_name,
                duration_ms=(time.time() - start_time) * 1000
            )
            return None
            
        except Exception as err:
            self.logger.error(
                "File deletion failed",
                bucket_name=bucket_name,
                file_name=file_name,
                error=str(err),
                duration_ms=(time.time() - start_time) * 1000
            )
            raise SDKError(f"failed to delete file: {err}")

    def create_file_upload(self, ctx, bucket_name: str, file_name: str) -> None:
        start_time = time.time()
        self.logger.info("Creating file upload", bucket_name=bucket_name, file_name=file_name)
        
        if not bucket_name:
            raise SDKError("empty bucket name")

        try:
            # Use web3 instance from ipc_instance for keccak
            if not hasattr(self.ipc, 'web3'):
                 raise SDKError("Web3 instance not available in IPC client")
            file_id = self.ipc.web3.keccak(text=f"{bucket_name}/{file_name}")
            
            
            # Create file record using storage contract
            self.ipc.storage.create_file(
                bucket_name,
                file_name,
                file_id,
                0,  # Initial size is 0
                self.ipc.auth.address, 
                self.ipc.auth.key
            )
            
            self.logger.info(
                "File upload created successfully",
                bucket_name=bucket_name,
                file_name=file_name,
                file_id=file_id.hex(),
                duration_ms=(time.time() - start_time) * 1000
            )
            return None
        except Exception as err:
            self.logger.error(
                "File upload creation failed",
                bucket_name=bucket_name,
                file_name=file_name,
                error=str(err),
                duration_ms=(time.time() - start_time) * 1000
            )
            raise SDKError(f"failed to create file upload: {err}")

    def upload(self, ctx, bucket_name: str, file_name: str, reader: io.IOBase, total_size: Optional[int] = None) -> IPCFileMetaV2:
        start_time = time.time()
        self.logger.info("Starting file upload", bucket_name=bucket_name, file_name=file_name)
        
        progress_bar = None
        if self.enable_progress_bar and total_size:
            progress_bar = tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Uploading {file_name}")
        
        try:
            bucket = self.ipc.storage.get_bucket_by_name(
                {"from": self.ipc.auth.address},
                bucket_name
            )
            if not bucket:
                raise SDKError("failed to retrieve bucket")
            
            chunk_enc_overhead = 0
            file_enc_key = encryption_key(self.encryption_key, bucket_name, file_name)
            if len(file_enc_key) > 0:
                chunk_enc_overhead = EncryptionOverhead
            
            is_empty_file = True
            
            buffer_size = self.max_blocks_in_chunk * int(BlockSize)
            if self.erasure_code:
                buffer_size = self.erasure_code.data_blocks * int(BlockSize)
            buffer_size -= chunk_enc_overhead
            buf = bytearray(buffer_size)
            
            dag_root = DAGRoot.new()
            
            i = 0
            file_size = 0
            processed_bytes = 0
            
            while True:
                if hasattr(ctx, 'done') and ctx.done():
                    raise SDKError("context cancelled")
                
                try:
                    n = reader.readinto(buf)
                    if n == 0:
                        if is_empty_file:
                            raise SDKError("empty file")
                        break
                    is_empty_file = False
                except Exception as e:
                    if isinstance(e, EOFError):
                        if is_empty_file:
                            raise SDKError("empty file")
                        break
                    raise SDKError(f"failed to read file: {str(e)}")
                
                chunk_upload = self.create_chunk_upload(ctx, i, file_enc_key, buf[:n], bucket.id, file_name)
                file_size += chunk_upload.actual_size
                processed_bytes += n
                
                dag_root.add_link(chunk_upload.chunk_cid, chunk_upload.raw_data_size, chunk_upload.proto_node_size)
                
                self.upload_chunk(ctx, chunk_upload)
                
                if self.progress_callback and total_size:
                    progress = (processed_bytes / total_size) * 100
                    self.progress_callback(progress, processed_bytes, total_size)
                
                if progress_bar:
                    progress_bar.update(n)
                
                i += 1
            
            root_cid = dag_root.build()
            
            self.ipc.storage.commit_file(
                bucket_name,
                file_name,
                file_size, 
                root_cid.toBytes(),
                self.ipc.auth.address,
                self.ipc.auth.key
            )
            committed_at_ts = time.time()
            
            file_meta_info = self.file_info(ctx, bucket_name, file_name)
            result = IPCFileMetaV2(
                root_cid=file_meta_info.root_cid if file_meta_info else root_cid.toString(),
                bucket_name=bucket_name,
                name=file_name,
                encoded_size=file_size,
                created_at=file_meta_info.created_at if file_meta_info else 0,
                committed_at=committed_at_ts
            )
            
            self.logger.info(
                "File uploaded successfully",
                bucket_name=bucket_name,
                file_name=file_name,
                root_cid=result.root_cid,
                size=file_size,
                duration_ms=(time.time() - start_time) * 1000
            )
            return result
            
        except Exception as err:
            self.logger.error(
                "File upload failed",
                bucket_name=bucket_name,
                file_name=file_name,
                error=str(err),
                duration_ms=(time.time() - start_time) * 1000
            )
            raise SDKError(f"failed to upload file: {str(err)}")
        finally:
            if progress_bar:
                progress_bar.close()
                {"from": self.ipc.auth.address},
                bucket_name
    

    def create_chunk_upload(self, ctx, index: int, file_encryption_key: bytes, data: bytes, bucket_id, file_name: str) -> IPCFileChunkUploadV2:
        start_time = time.time()
        self.logger.debug("Creating chunk upload", chunk_index=index, file_name=file_name)
        
        try:
            if len(file_encryption_key) > 0:
                data = encrypt(file_encryption_key, data, str(index).encode())
            
            size = len(data)
            
            block_size = BlockSize
            if self.erasure_code:
                data = self.erasure_code.encode(data)
                blocks_count = self.erasure_code.data_blocks + self.erasure_code.parity_blocks
                block_size = len(data) // blocks_count
            
            chunk_dag = build_dag(ctx, io.BytesIO(data), block_size)
            
            cids, sizes, proto_chunk, _ = to_ipc_proto_chunk(
                chunk_dag.cid.string(),
                index,
                size,
                chunk_dag.blocks
            )
            
            request = ipcnodeapi_pb2.IPCFileUploadChunkCreateRequest(
                chunk=ipcnodeapi_pb2.IPCChunk(
                    cid=proto_chunk["cid"],
                    index=proto_chunk["index"],
                    size=proto_chunk["size"],
                    blocks=[
                        ipcnodeapi_pb2.IPCChunk.Block(
                            cid=block["cid"],
                            size=block["size"]
                        ) for block in proto_chunk["blocks"]
                    ]
                ),
                bucket_id=bucket_id,
                file_name=file_name
            )
            
            response = self.client.FileUploadChunkCreate(request)
            
            if len(response.blocks) != len(chunk_dag.blocks):
                raise SDKError(f"received unexpected amount of blocks {len(response.blocks)}, expected {len(chunk_dag.blocks)}")
            
            for i, upload in enumerate(response.blocks):
                if chunk_dag.blocks[i]["cid"] != upload.cid:
                    raise SDKError(f"block CID mismatch at position {i}")
                chunk_dag.blocks[i]["node_address"] = upload.node_address
                chunk_dag.blocks[i]["node_id"] = upload.node_id
                chunk_dag.blocks[i]["permit"] = upload.permit
            
            result = IPCFileChunkUploadV2(
                index=index,
                chunk_cid=chunk_dag.cid,
                actual_size=size,
                raw_data_size=chunk_dag.raw_data_size,
                proto_node_size=chunk_dag.proto_node_size,
                blocks=chunk_dag.blocks,
                bucket_id=bucket_id,
                file_name=file_name
            )
            
            self.logger.debug(
                "Chunk upload created successfully",
                chunk_index=index,
                file_name=file_name,
                chunk_size=size,
                duration_ms=(time.time() - start_time) * 1000
            )
            return result
            
        except Exception as err:
            self.logger.error(
                "Chunk upload creation failed",
                chunk_index=index,
                file_name=file_name,
                error=str(err),
                duration_ms=(time.time() - start_time) * 1000
            )
            raise SDKError(f"failed to create chunk upload: {str(err)}")

    def upload_chunk(self, ctx, file_chunk_upload: IPCFileChunkUploadV2) -> None:
        start_time = time.time()
        self.logger.debug("Uploading chunk", chunk_index=file_chunk_upload.index, file_name=file_chunk_upload.file_name)
        
        try:
            pool = ConnectionPool()
            
            try:
                _, _, proto_chunk, _ = to_ipc_proto_chunk(
                    file_chunk_upload.chunk_cid.string(),
                    file_chunk_upload.index,
                    file_chunk_upload.actual_size,
                    file_chunk_upload.blocks
                )
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                    futures = []
                    
                    for i, block in enumerate(file_chunk_upload.blocks):
                        futures.append(executor.submit(
                            self._upload_block,
                            ctx, pool, i, block, proto_chunk, 
                            file_chunk_upload.bucket_id, file_chunk_upload.file_name
                        ))
                    
                    for future in concurrent.futures.as_completed(futures):
                        future.result()
                        
            finally:
                err = pool.close()
                if err:
                    self.logger.warning(
                        "Connection pool closure warning",
                        error=str(err),
                        duration_ms=(time.time() - start_time) * 1000
                    )

            self.logger.debug(
                "Chunk uploaded successfully",
                chunk_index=file_chunk_upload.index,
                file_name=file_chunk_upload.file_name,
                duration_ms=(time.time() - start_time) * 1000
            )
            return None
        except Exception as err:
            self.logger.error(
                "Chunk upload failed",
                chunk_index=file_chunk_upload.index,
                file_name=file_chunk_upload.file_name,
                error=str(err),
                duration_ms=(time.time() - start_time) * 1000
            )
            raise SDKError(f"failed to upload chunk: {str(err)}")

    def _upload_block(self, ctx, pool: ConnectionPool, block_index: int, block, proto_chunk, bucket_id, file_name: str) -> None:
        start_time = time.time()
        self.logger.debug("Uploading block", block_index=block_index, file_name=file_name)
        
        try:
            client, closer, err = pool.create_ipc_client(block["node_address"], self.use_connection_pool)
            if err:
                raise SDKError(f"failed to create client: {str(err)}")
            
            try:
                block_data = ipcnodeapi_pb2.IPCFileBlockData(
                    data=block["data"],
                    cid=block["cid"],
                    index=block_index,
                    chunk=proto_chunk,
                    bucket_id=bucket_id,
                    file_name=file_name
                )
                
                response = client.FileUploadBlock(iter([block_data]))
                if not response:
                    raise SDKError("failed to upload block")
                    
                self.logger.debug(
                    "Block uploaded successfully",
                    block_index=block_index,
                    file_name=file_name,
                    block_cid=block["cid"],
                    duration_ms=(time.time() - start_time) * 1000
                )
                
            finally:
                if closer:
                    closer()
                    
        except Exception as err:
            self.logger.error(
                "Block upload failed",
                block_index=block_index,
                file_name=file_name,
                block_cid=block["cid"],
                error=str(err),
                duration_ms=(time.time() - start_time) * 1000
            )
            raise SDKError(f"failed to upload block {block['cid']}: {str(err)}")

    def fetch_block_data(
        self, 
        ctx, 
        pool: ConnectionPool,
        chunk_cid: str, 
        bucket_name: str, 
        file_name: str, 
        address: str,
        chunk_index: int, 
        block_index: int,
        block
    ) -> bytes:
        start_time = time.time()
        self.logger.debug("Fetching block data", block_index=block_index, file_name=file_name)
        
        try:
            if not hasattr(block, 'akave') or not block.akave and not hasattr(block, 'filecoin') or not block.filecoin:
                raise SDKError("missing block metadata")
            
            client, closer, err = pool.create_ipc_client(block.akave.node_address, self.use_connection_pool)
            if err:
                raise SDKError(f"failed to create client: {str(err)}")
            
            try:
                download_req = {
                    "chunk_cid": chunk_cid,
                    "chunk_index": chunk_index,
                    "block_cid": block.cid,
                    "block_index": block_index,
                    "bucket_name": bucket_name,
                    "file_name": file_name,
                    "address": address
                }
                
                download_client = client.file_download_block(ctx, download_req)
                if not download_client:
                    raise SDKError("failed to get download client")
                
                buffer = io.BytesIO()
                
                while True:
                    try:
                        block_data = download_client.recv()
                        if not block_data:
                            break
                        buffer.write(block_data.data)
                    except EOFError:
                        break
                    except Exception as e:
                        if isinstance(e, io.EOF):
                            break
                        raise SDKError(f"error receiving block data: {str(e)}")
                
                result = buffer.getvalue()
                
                self.logger.debug(
                    "Block data fetched successfully",
                    block_index=block_index,
                    file_name=file_name,
                    block_cid=block.cid,
                    size=len(result),
                    duration_ms=(time.time() - start_time) * 1000
                )
                return result
                
            finally:
                if closer:
                    try:
                        closer()
                    except Exception as e:
                        self.logger.warning(
                            "Connection closure warning",
                            block_cid=block.cid,
                            error=str(e),
                            duration_ms=(time.time() - start_time) * 1000
                        )
                        
        except Exception as e:
            self.logger.error(
                "Block data fetch failed",
                block_index=block_index,
                file_name=file_name,
                block_cid=block.cid,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000
            )
            raise SDKError(f"failed to fetch block data: {str(e)}")

    def create_file_download(self, ctx, bucket_name: str, file_name: str):
        try:
            if not bucket_name:
                raise SDKError("empty bucket name")
                
            if not file_name:
                raise SDKError("empty file name")
                
            request = ipcnodeapi_pb2.IPCFileDownloadCreateRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                address=self.ipc.auth.address
            )
            
            response = self.client.FileDownloadCreate(request)
            
            chunks = []
            for chunk in response.chunks:
                chunks.append(Chunk(
                    cid=chunk.cid,
                    encoded_size=chunk.encoded_size,
                    size=chunk.size,
                    index=chunk.index
                ))
            
            return IPCFileDownload(
                bucket_name=response.bucket_name,
                name=file_name,
                chunks=chunks
            )
        except Exception as err:
            raise SDKError(f"failed to create file download: {str(err)}")
            
    def download(self, ctx, file_download, writer: io.IOBase):
        try:
            file_enc_key = encryption_key(
                self.encryption_key, 
                file_download.bucket_name, 
                file_download.name
            )
            
            for chunk in file_download.chunks:
                if hasattr(ctx, 'done') and ctx.done():
                    raise SDKError("context cancelled")
                
                chunk_download = self.create_chunk_download(
                    ctx, 
                    file_download.bucket_name, 
                    file_download.name, 
                    chunk
                )
                
                self.download_chunk_blocks(
                    ctx,
                    file_download.bucket_name,
                    file_download.name,
                    self.ipc.auth.address,
                    chunk_download,
                    file_enc_key,
                    writer
                )
            
            return None
        except Exception as err:
            raise SDKError(f"failed to download file: {str(err)}")
            
    def create_chunk_download(self, ctx, bucket_name: str, file_name: str, chunk):
        try:
            request = ipcnodeapi_pb2.IPCFileDownloadChunkCreateRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                chunk_cid=chunk.cid,
                address=self.ipc.auth.address
            )
            
            response = self.client.FileDownloadChunkCreate(request)
            
            blocks = []
            for block in response.blocks:
                blocks.append(FileBlockDownload(
                    cid=block.cid,
                    data=b"",
                    akave=AkaveBlockData(
                        node_id=block.node_id,
                        node_address=block.node_address,
                        permit=block.permit
                    ),
                    filecoin=None
                ))
            
            return FileChunkDownload(
                cid=chunk.cid,
                index=chunk.index,
                encoded_size=chunk.encoded_size,
                size=chunk.size,
                blocks=blocks
            )
        except Exception as err:
            raise SDKError(f"failed to create chunk download: {str(err)}")
            
    def download_chunk_blocks(self, ctx, bucket_name: str, file_name: str, address: str, 
                             chunk_download, file_encryption_key: bytes, writer: io.IOBase):
        try:
            pool = ConnectionPool()
            
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                    futures = {}
                    
                    for i, block in enumerate(chunk_download.blocks):
                        futures[executor.submit(
                            self.fetch_block_data,
                            ctx, pool, chunk_download.cid, bucket_name, file_name, 
                            address, chunk_download.index, i, block
                        )] = i
                    
                    blocks = [None] * len(chunk_download.blocks)
                    for future in concurrent.futures.as_completed(futures):
                        index = futures[future]
                        try:
                            data = future.result()
                            from .dag import extract_block_data
                            blocks[index] = extract_block_data(chunk_download.blocks[index].cid, data)
                        except Exception as e:
                            raise SDKError(f"failed to download block: {str(e)}")
                
                if self.erasure_code:
                    data = self.erasure_code.extract_data_blocks(blocks, int(chunk_download.size))
                else:
                    data = b"".join([b for b in blocks if b is not None])
                
                if file_encryption_key:
                    from private.encryption import decrypt
                    data = decrypt(file_encryption_key, data, str(chunk_download.index).encode())
                
                writer.write(data)
            finally:
                err = pool.close()
                if err:
                    logging.warning(f"Error closing connection pool: {str(err)}")
            
            return None
        except Exception as err:
            raise SDKError(f"failed to download chunk blocks: {str(err)}")