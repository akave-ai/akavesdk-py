import time
import logging
import binascii
import io
import math
import concurrent.futures
import secrets
from hashlib import sha256
from typing import List, Optional, Callable, Dict, Any, Union, Tuple, cast, Sequence
from google.protobuf.timestamp_pb2 import Timestamp
from datetime import datetime
import grpc 

from .common import MIN_BUCKET_NAME_LENGTH, SDKError, BLOCK_SIZE, ENCRYPTION_OVERHEAD
from .erasure_code import ErasureCode
from .dag import DAGRoot, build_dag, extract_block_data
from .connection import ConnectionPool
from .model import (
    IPCBucketCreateResult, IPCBucket, IPCFileMeta, IPCFileListItem,
    IPCFileMetaV2, IPCFileChunkUploadV2, AkaveBlockData, FileBlockUpload,
    FileBlockDownload, Chunk, IPCFileDownload, FileChunkDownload
)
from private.encryption import encrypt, derive_key, decrypt
from private.pb import ipcnodeapi_pb2, ipcnodeapi_pb2_grpc

try:
    from multiformats.cid import CID
except ImportError:
    pass

try:
    from eth_account import Account
    from eth_account.messages import encode_structured_data
    from eth_utils import to_checksum_address
except ImportError:
    pass

BlockSize = BLOCK_SIZE
EncryptionOverhead = ENCRYPTION_OVERHEAD

def encryption_key(parent_key: bytes, *info_data: str):
    if len(parent_key) == 0:
        return b''
    
    info = "/".join(info_data)
    return derive_key(parent_key, info.encode())

def maybe_encrypt_metadata(value: str, derivation_path: str, encryption_key: bytes) -> str:
    """Encrypt metadata if encryption key is provided, matching Go implementation"""
    if len(encryption_key) == 0:
        return value
    
    try:
        file_enc_key = derive_key(encryption_key, derivation_path.encode())
        encrypted_data = encrypt(file_enc_key, value.encode(), b"metadata")
        return encrypted_data.hex()
    except Exception as e:
        raise SDKError(f"failed to encrypt metadata: {str(e)}")

def to_ipc_proto_chunk(chunk_cid: str, index: int, size: int, blocks):
    """Convert to IPC proto chunk format matching Go implementation"""
    from private.pb import ipcnodeapi_pb2
    
    cids = []  # [][32]byte
    sizes = []  # []*big.Int (but we'll use regular ints)
    
    pb_blocks = []
    for block in blocks:
        block_cid = block.cid if hasattr(block, 'cid') else block["cid"]
        block_data = block.data if hasattr(block, 'data') else block["data"]
        
        # Create protobuf block
        pb_block = ipcnodeapi_pb2.IPCChunk.Block(
            cid=block_cid,
            size=len(block_data)
        )
        pb_blocks.append(pb_block)
        
        # Convert CID to [32]byte for blockchain
        try:
            # Try to decode as proper CID first
            try:
                c = CID.decode(block_cid)
                # Get CID bytes - try different methods based on library version
                if hasattr(c, 'bytes'):
                    cid_bytes = c.bytes
                elif hasattr(c, 'encode'):
                    cid_bytes = c.encode()
                elif hasattr(c, 'buffer'):
                    cid_bytes = c.buffer
                else:
                    # Fallback: encode the string representation
                    cid_bytes = block_cid.encode()
            except Exception:
                # If CID decoding fails, treat as string and hash it
                import hashlib
                cid_bytes = hashlib.sha256(block_cid.encode()).digest()
            
            bcid = bytearray(32)
            
            # Ensure we have bytes, not string
            if isinstance(cid_bytes, str):
                cid_bytes = cid_bytes.encode()
            elif not isinstance(cid_bytes, (bytes, bytearray)):
                cid_bytes = bytes(cid_bytes)
                
            # Copy bytes, handling both proper CIDs and our simplified format
            if len(cid_bytes) > 4:
                copy_len = min(len(cid_bytes) - 4, 32)
                bcid[:copy_len] = cid_bytes[4:4+copy_len]
            else:
                # If CID is too short, just copy what we have
                copy_len = min(len(cid_bytes), 32)
                bcid[:copy_len] = cid_bytes[:copy_len]
                
            cids.append(bytes(bcid))
            sizes.append(len(block_data))
        except Exception as e:
            return None, None, None, SDKError(f"failed to process CID: {str(e)}")
    
    # Create protobuf chunk matching Go version
    proto_chunk = ipcnodeapi_pb2.IPCChunk(
        cid=chunk_cid,
        index=index,
        size=size,
        blocks=pb_blocks
    )
    
    return cids, sizes, proto_chunk, None

class IPC:
    def __init__(self, client, conn, ipc_instance, max_concurrency, block_part_size, use_connection_pool, encryption_key=None, max_blocks_in_chunk=32, erasure_code=None, chunk_buffer=10):
        self.client = client
        self.conn = conn
        self.ipc = ipc_instance
        self.max_concurrency = max_concurrency
        self.block_part_size = block_part_size
        self.use_connection_pool = use_connection_pool
        self.encryption_key = encryption_key or b''
        self.max_blocks_in_chunk = max_blocks_in_chunk
        self.erasure_code = erasure_code
        self.chunk_buffer = chunk_buffer


    def create_bucket(self, ctx: Any, name: str) -> IPCBucketCreateResult:
        if len(name) < MIN_BUCKET_NAME_LENGTH:
            raise SDKError("invalid bucket name")
        try:
            tx = self.ipc.storage.create_bucket(
                bucket_name=name,
                from_address=self.ipc.auth.address,
                private_key=self.ipc.auth.key,
                gas_limit=500000
            )
            receipt = self.ipc.web3.eth.wait_for_transaction_receipt(tx)
            
            if receipt.status != 1:
                raise SDKError("bucket creation transaction failed")
                
            block = self.ipc.web3.eth.get_block(receipt.blockNumber)
            created_at = block.timestamp
            
            return IPCBucketCreateResult(
                id=tx,
                name=name,
                created_at=created_at
            )
            
        except Exception as e:
            raise SDKError(f"bucket creation failed: {e}")
        

    def view_bucket(self, ctx: Any, bucket_name: str) -> Optional[IPCBucket]:
        if not bucket_name:
            raise SDKError("empty bucket name")
        try:
            request = ipcnodeapi_pb2.IPCBucketViewRequest(
                name=bucket_name,     
                address=self.ipc.auth.address.lower() 
            )
            response = self.client.BucketView(request)
            if not response:
                return None
            created_at = int(response.created_at.seconds) if response.created_at else 0
            return IPCBucket(
                id=response.id,
                name=response.name,
                created_at=created_at
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND: # type: ignore[union-attr]
                return None
            raise SDKError(f"failed to view bucket: {e.details()}") # type: ignore[return-value]


    def list_buckets(self, ctx: Any) -> List[IPCBucket]:
        try:
            request = ipcnodeapi_pb2.IPCBucketListRequest(
                address=self.ipc.auth.address.lower()
            )
            response = self.client.BucketList(request)
            buckets = []
            if response and hasattr(response, 'buckets'):
                logging.info(f"Received BucketList response with {len(response.buckets)} buckets")
                for bucket in response.buckets:
                    created_at = 0
                    if hasattr(bucket, 'created_at') and bucket.created_at:
                        created_at = int(bucket.created_at.seconds)
                    
                    bucket_name = bucket.name if hasattr(bucket, 'name') else ''
                    bucket_id = bucket.id if hasattr(bucket, 'id') else ''
                    logging.info(f"Processing bucket: name={bucket_name}, id={bucket_id}, created_at={created_at}")
                    buckets.append(IPCBucket(
                        name=bucket_name,
                        created_at=created_at,
                        id=bucket_id
                    ))
            else:
                logging.warning("BucketList response has no 'buckets' field or is empty")
            return buckets
        except grpc.RpcError as e:
            raise SDKError(f"failed to list buckets: {e.details()}") # type: ignore[return-value]


    def delete_bucket(self, ctx: Any, name: str) -> None:
        if not name:
            raise SDKError("empty bucket name")
        try:
            # ensure exists
            req = ipcnodeapi_pb2.IPCBucketViewRequest(
                name=name,
                address=self.ipc.auth.address.lower()
            )
            try:
                response = self.client.BucketView(request)
                if not response:
                    logging.warning(f"Bucket '{name}' not found, cannot delete")
                    raise SDKError(f"bucket '{name}' not found")
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.NOT_FOUND:
                    logging.warning(f"Bucket '{name}' not found, cannot delete")
                    raise SDKError(f"bucket '{name}' not found")
                logging.error(f"IPC bucket view failed during delete: {e.code()} - {e.details()}")
                raise SDKError(f"failed to check bucket existence: {e.details()}")

            # If we get here, bucket exists - proceed with deletion
            bucket_id_hex = response.id if hasattr(response, 'id') and response.id else None
            if not bucket_id_hex:
                logging.error(f"No bucket ID returned from IPC for bucket '{name}'")
                raise SDKError(f"bucket ID not available from IPC response")
                
            logging.info(f"Got bucket ID from IPC: {bucket_id_hex}")
            
            try:
                tx_hash = self.ipc.storage.delete_bucket(
                    bucket_name=name,
                    from_address=self.ipc.auth.address,
                    private_key=self.ipc.auth.key,
                    bucket_id_hex=bucket_id_hex  # bucket ID from IPC response
                )
                logging.info(f"IPC delete_bucket transaction sent for '{name}', tx_hash: {tx_hash}")
                return None
                
            except Exception as e:
                logging.error(f"Failed to delete bucket '{name}' on blockchain: {str(e)}")
                raise SDKError(f"blockchain transaction failed: {str(e)}")
                
        except Exception as err:
            logging.error(f"IPC delete_bucket failed: {err}")
            raise SDKError(f"failed to delete bucket: {err}")

    def file_info(self, ctx: Any, bucket_name: str, file_name: str) -> Optional[IPCFileMeta]:
        if not bucket_name or not file_name:
            raise SDKError("empty bucket or file name")
        try:
            request = ipcnodeapi_pb2.IPCFileViewRequest(
                file_name=file_name,
                bucket_name=bucket_name,
                address=self.ipc.auth.address.lower()
            )
            resp = self.client.FileView(request)
            if not resp:
                return None
            created_at = int(resp.created_at.seconds) if resp.created_at else 0
            return IPCFileMeta(
                root_cid=resp.root_cid,
                name=resp.file_name,
                bucket_name=resp.bucket_name,
                encoded_size=resp.encoded_size,
                created_at=created_at
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND: # type: ignore[union-attr]
                return None
            raise SDKError(f"failed to get file info: {e.details()}") # type: ignore[return]


    def list_files(self, ctx: Any, bucket_name: str) -> List[IPCFileListItem]:
        if not bucket_name:
            raise SDKError("empty bucket name")
        try:
            request = ipcnodeapi_pb2.IPCFileListRequest(
                bucket_name=bucket_name,
                address=self.ipc.auth.address.lower()
            )
            resp = self.client.FileList(request)
            items: List[IPCFileListItem] = []
            for f in resp.list:
                items.append(IPCFileListItem(
                    root_cid=f.root_cid,
                    name=f.name,
                    encoded_size=f.encoded_size,
                    created_at=int(f.created_at.seconds) if f.created_at else 0
                ))
            return items
        except grpc.RpcError as e:
            raise SDKError(f"failed to list files: {e.details()}") # type: ignore[return-value]


    def file_delete(self, ctx: Any, bucket_name: str, file_name: str) -> None:
        if not bucket_name.strip() or not file_name.strip():
            raise SDKError(f"empty bucket or file name. Bucket: '{bucket_name}', File: '{file_name}'")

        try:
            encrypted_file_name = file_name
            encrypted_bucket_name = bucket_name
            
            bucket = self.ipc.storage.get_bucket_by_name(
                {"from": self.ipc.auth.address},
                encrypted_bucket_name
            )
            if not bucket:
                raise SDKError("failed to retrieve bucket")
            
            bucket_id = bucket[0]  # bytes32 id
            
            file_info = self.ipc.storage.get_file_by_name(
                {}, 
                bucket_id,
                encrypted_file_name
            )
            if not file_info:
                raise SDKError("failed to retrieve file - file does not exist")
            
            file_id = file_info[0]  # bytes32 file ID
            file_index = self.ipc.storage.get_file_index_by_id(
                {},
                bucket_name,
                file_id 
            )
            
            logging.info(f"Deleting file with file_id: {file_id.hex() if isinstance(file_id, bytes) else file_id}, bucket_id: {bucket_id.hex() if isinstance(bucket_id, bytes) else bucket_id}, name: {encrypted_file_name}, index: {file_index}")
            tx_hash = self.ipc.storage.delete_file(
                self.ipc.auth,          
                file_id,                
                bucket_id,              
                encrypted_file_name,    
                file_index             
            )
            
            logging.info(f"IPC file_delete transaction successful for '{file_name}' in bucket '{bucket_name}', tx_hash: {tx_hash}")
            return None
            
        except Exception as err:
            logging.error(f"IPC file_delete failed: {err}")
            raise SDKError(f"failed to delete file: {err}")

    def create_file_upload(self, ctx, bucket_name: str, file_name: str) -> None:
        if not bucket_name:
            raise SDKError("empty bucket name")
        try:
            bucket = self.ipc.storage.get_bucket_by_name(
                {"from": self.ipc.auth.address},
                bucket_name
            )
            if not bucket:
                raise SDKError("failed to retrieve bucket")
            
            bucket_id = bucket[0]  
            
            tx_hash = self.ipc.storage.create_file(
                self.ipc.auth.address, 
                self.ipc.auth.key,
                bucket_id,
                file_name
            )
            logging.info(f"IPC create_file_upload transaction sent, tx_hash: {tx_hash}")
            return None    
        except Exception as err:
            logging.error(f"IPC create_file_upload failed: {err}")
            raise SDKError(f"failed to create file upload: {err}")

    def upload(self, ctx, bucket_name: str, file_name: str, reader: io.IOBase) -> IPCFileMetaV2:
        """Upload a file to the storage network, following the exact Go implementation flow"""
        try:
            if not bucket_name:
                raise SDKError("empty bucket name")
            if not file_name:
                raise SDKError("empty file name")
            
            # Apply metadata encryption if enabled (matching Go maybeEncryptMetadata)
            original_file_name = file_name
            original_bucket_name = bucket_name
            
            file_name = maybe_encrypt_metadata(file_name, bucket_name + "/" + file_name, self.encryption_key)
            bucket_name = maybe_encrypt_metadata(bucket_name, bucket_name, self.encryption_key)
            
            # Get bucket info
            try:
                bucket = self.ipc.storage.get_bucket_by_name(
                    {"from": self.ipc.auth.address},
                    bucket_name
                )
                if not bucket:
                    raise SDKError("failed to retrieve bucket")
            except Exception as e:
                raise SDKError(f"failed to get bucket: {str(e)}")
            
            # Calculate encryption overhead
            chunk_enc_overhead = 0
            try:
                file_enc_key = encryption_key(self.encryption_key, bucket_name, file_name)
                if len(file_enc_key) > 0:
                    chunk_enc_overhead = EncryptionOverhead
            except Exception as e:
                raise SDKError(f"encryption key derivation failed: {str(e)}")
            
            # Calculate buffer size - matching Go implementation
            buffer_size = self.max_blocks_in_chunk * int(BlockSize)
            if self.erasure_code:  # erasure coding enabled
                buffer_size = self.erasure_code.data_blocks * int(BlockSize)
            buffer_size -= chunk_enc_overhead
            
            # Create DAG root using proper constructor matching Go NewDAGRoot()
            dag_root = DAGRoot.new()
            
            # Use concurrent processing like Go - create channel equivalent
            import queue
            file_upload_chunks_queue = queue.Queue(maxsize=self.chunk_buffer)
            
            # Start concurrent chunk processing
            chunk_processing_done = False
            chunk_processing_error = None
            
            def chunk_reader():
                nonlocal chunk_processing_done, chunk_processing_error
                try:
                    buf = bytearray(buffer_size)
                    index = 0
                    
                    while True:
                        try:
                            n = reader.readinto(buf)
                            if n == 0 or n is None:
                                if index == 0:
                                    chunk_processing_error = SDKError("empty file")
                                    return
                                break
                        except Exception as e:
                            if isinstance(e, EOFError):
                                if index == 0:
                                    chunk_processing_error = SDKError("empty file")
                                    return
                                break
                            chunk_processing_error = SDKError(f"failed to read file: {str(e)}")
                            return
                        
                        if n > 0:
                            # Create chunk upload
                            try:
                                chunk_upload = self.create_chunk_upload(ctx, index, file_enc_key, buf[:n], bucket[0], file_name)
                                file_upload_chunks_queue.put(chunk_upload)
                                index += 1
                            except Exception as e:
                                chunk_processing_error = SDKError(f"failed to create chunk upload: {str(e)}")
                                return
                    
                    chunk_processing_done = True
                    
                except Exception as e:
                    chunk_processing_error = SDKError(f"chunk reader error: {str(e)}")
                finally:
                    # Signal completion by putting None
                    file_upload_chunks_queue.put(None)
            
            # Start chunk reader in separate thread
            import threading
            chunk_reader_thread = threading.Thread(target=chunk_reader)
            chunk_reader_thread.start()
            
            # Process chunks as they become available
            file_size = 0
            actual_file_size = 0
            chunk_count = 0
            
            while True:
                try:
                    chunk_upload = file_upload_chunks_queue.get(timeout=30)  # 30 second timeout
                    if chunk_upload is None:  # Signal for completion
                        break
                    
                    # Add to DAG using proper add_link method matching Go AddLink()
                    dag_root.add_link(chunk_upload.chunk_cid, chunk_upload.raw_data_size, chunk_upload.proto_node_size)
                    
                    # Upload chunk
                    self.upload_chunk(ctx, chunk_upload)
                    
                    file_size += chunk_upload.proto_node_size
                    actual_file_size += chunk_upload.actual_size
                    chunk_count += 1
                    
                except queue.Empty:
                    if chunk_processing_error:
                        raise chunk_processing_error
                    if chunk_processing_done:
                        break
                    continue
                except Exception as e:
                    raise SDKError(f"chunk processing error: {str(e)}")
            
            # Wait for chunk reader to complete
            chunk_reader_thread.join()
            
            # Check for any errors from chunk processing
            if chunk_processing_error:
                raise chunk_processing_error
            
            # Build DAG root using proper build() method matching Go Build()
            root_cid = dag_root.build()
            
            # Get file metadata (required before commit)
            try:
                file_meta = self.ipc.storage.get_file_by_name(
                    {"from": self.ipc.auth.address},
                    bucket[0],  # bucket[0] is the bucket ID
                    file_name
                )
            except Exception as e:
                raise SDKError(f"failed to get file metadata: {str(e)}")
            
            # Calculate file ID and wait for fill completion like Go implementation
            file_id = self._calculate_file_id(bucket[0], file_name)
            
            # Wait for file to be filled - matching Go IsFileFilled loop
            is_filled = False
            max_wait_time = 300  # 5 minutes max wait
            wait_start = time.time()
            
            while not is_filled:
                try:
                    is_filled = self.ipc.storage.is_file_filled({"from": self.ipc.auth.address}, file_id)
                    if is_filled:
                        break
                except Exception as e:
                    # If is_file_filled method doesn't exist, wait a bit and assume filled
                    logging.warning(f"IsFileFilled check failed: {e}, assuming file is filled")
                    time.sleep(2)
                    break
                
                if time.time() - wait_start > max_wait_time:
                    raise SDKError("timeout waiting for file to be filled")
                
                time.sleep(1)  # Wait 1 second like Go implementation
            
            # Commit file
            try:
                # Convert CID to bytes for storage contract
                if hasattr(root_cid, 'bytes'):
                    root_cid_bytes = root_cid.bytes
                elif hasattr(root_cid, 'encode'):
                    root_cid_bytes = root_cid.encode()
                else:
                    root_cid_bytes = str(root_cid).encode()
                
                tx = self.ipc.storage.commit_file(
                    self.ipc.auth.address,
                    self.ipc.auth.key,
                    bucket[0],  # bucket ID
                    file_name,
                    file_size,
                    actual_file_size,
                    root_cid_bytes
                )
                
                # Wait for commit transaction
                receipt = self.ipc.web3.eth.wait_for_transaction_receipt(tx)
                if receipt.status != 1:
                    raise SDKError("CommitFile transaction failed")
                
                return IPCFileMetaV2(
                    root_cid=str(root_cid),
                    bucket_name=bucket_name,
                    name=file_name,
                    encoded_size=file_size,
                    created_at=time.time(),
                    committed_at=time.time()
                )
                
            except Exception as e:
                raise SDKError(f"failed to commit file: {str(e)}")
            
        except Exception as err:
            logging.error(f"IPC upload failed: {err}")
            raise SDKError(f"upload failed: {str(err)}")
    
    def _calculate_file_id(self, bucket_id: bytes, file_name: str) -> bytes:
        """Calculate file ID matching the Go implementation"""
        # This should match the Go ipc.CalculateFileID function
        from hashlib import sha256
        combined = bucket_id + file_name.encode()
        return sha256(combined).digest()

    def create_chunk_upload(self, ctx, index: int, file_encryption_key: bytes, data: bytes, bucket_id: bytes, file_name: str) -> IPCFileChunkUploadV2:
        try:
            # Encryption step - matching Go implementation
            if len(file_encryption_key) > 0:
                data = encrypt(file_encryption_key, data, str(index).encode())
            
            size = len(data)
            block_size = int(BlockSize)
            
            # Calculate block size - matching Go implementation
            block_size = BlockSize
            if self.erasure_code:
                # Apply erasure coding first
                data = self.erasure_code.encode(data)
                # Calculate shard size (equivalent to Go's blockSize calculation)
                blocks_count = self.erasure_code.data_blocks + self.erasure_code.parity_blocks
                block_size = len(data) // blocks_count
            
            # Build chunk DAG using the proper build_dag function
            chunk_dag = build_dag(ctx, io.BytesIO(data), block_size, None)
            if chunk_dag is None:
                raise SDKError("build_dag returned None")
            
            # Convert chunk CID to string if needed
            chunk_cid_str = chunk_dag.cid if isinstance(chunk_dag.cid, str) else str(chunk_dag.cid)
            
            # Convert to IPC proto chunk format
            cids, sizes, proto_chunk, error = to_ipc_proto_chunk(
                chunk_cid_str,
                index,
                size,
                chunk_dag.blocks
            )
            
            if error is not None:
                raise error
            if proto_chunk is None:
                raise SDKError("to_ipc_proto_chunk returned None proto_chunk")
            
            # Create gRPC request
            request = ipcnodeapi_pb2.IPCFileUploadChunkCreateRequest(
                chunk=proto_chunk,
                bucket_id=bucket_id,  # bytes
                file_name=file_name
            )
            
            # Call gRPC API
            response = self.client.FileUploadChunkCreate(request)
            if response is None:
                raise SDKError("gRPC response is None")
            
            # Validate response
            if len(response.blocks) != len(chunk_dag.blocks):
                raise SDKError(f"received unexpected amount of blocks {len(response.blocks)}, expected {len(chunk_dag.blocks)}")
            
            # Update blocks with upload information from response
            for i, upload in enumerate(response.blocks):
                if chunk_dag.blocks[i].cid != upload.cid:
                    raise SDKError(f"block CID mismatch at position {i}")
                # Update block with node information
                chunk_dag.blocks[i].node_address = upload.node_address
                chunk_dag.blocks[i].node_id = upload.node_id
                chunk_dag.blocks[i].permit = upload.permit
            
            # Call blockchain AddFileChunk like Go version  
            try:
                # Convert chunk CID to bytes for blockchain
                chunk_cid_obj = CID.decode(chunk_cid_str)
                if hasattr(chunk_cid_obj, 'bytes'):
                    chunk_cid_bytes = chunk_cid_obj.bytes
                elif hasattr(chunk_cid_obj, 'encode'):
                    chunk_cid_bytes = chunk_cid_obj.encode()
                elif hasattr(chunk_cid_obj, 'buffer'):
                    chunk_cid_bytes = chunk_cid_obj.buffer
                else:
                    # Fallback: encode CID string as bytes
                    chunk_cid_bytes = chunk_cid_str.encode()
            except Exception as e:
                logging.warning(f"Failed to decode CID {chunk_cid_str}: {e}, using string encoding")
                # Fallback: encode CID string as bytes
                chunk_cid_bytes = chunk_cid_str.encode()
            
            # Add file chunk to blockchain (like Go's AddFileChunk)
            tx = self.ipc.storage.add_file_chunk(
                self.ipc.auth.address,
                self.ipc.auth.key,
                chunk_cid_bytes,
                bucket_id,
                file_name,
                chunk_dag.proto_node_size,
                cids,
                sizes, 
                index
            )
            
            # Wait for transaction completion like Go version
            receipt = self.ipc.web3.eth.wait_for_transaction_receipt(tx)
            if receipt.status != 1:
                raise SDKError("AddFileChunk transaction failed")
            
            return IPCFileChunkUploadV2(
                index=index, chunk_cid=cast(CIDType, chunk_dag.cid),
                actual_size=size, raw_data_size=chunk_dag.raw_data_size,
                proto_node_size=chunk_dag.proto_node_size,
                blocks=chunk_dag.blocks, bucket_id=bucket_id, file_name=file_name
            )
        except Exception as err:
            raise SDKError(f"failed to create chunk upload: {err}")


    def upload_chunk(self, ctx, file_chunk_upload: IPCFileChunkUploadV2) -> None:
        """Upload chunk blocks to storage nodes, matching Go implementation"""
        try:
            pool = ConnectionPool()
            
            try:
                # Handle CID string conversion
                chunk_cid_str = file_chunk_upload.chunk_cid
                if hasattr(file_chunk_upload.chunk_cid, 'string'):
                    chunk_cid_str = file_chunk_upload.chunk_cid.string()
                elif hasattr(file_chunk_upload.chunk_cid, 'toString'):
                    chunk_cid_str = file_chunk_upload.chunk_cid.toString()
                elif not isinstance(file_chunk_upload.chunk_cid, str):
                    chunk_cid_str = str(file_chunk_upload.chunk_cid)
                
                cids, sizes, proto_chunk, err = to_ipc_proto_chunk(
                    chunk_cid_str,
                    file_chunk_upload.index,
                    file_chunk_upload.actual_size,
                    file_chunk_upload.blocks
                )
                if err:
                    raise err
                
                # Upload blocks concurrently with proper error handling matching Go
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                    futures = {}
                    
                    for i, block in enumerate(file_chunk_upload.blocks):
                        future = executor.submit(
                            self._upload_block,
                            ctx, pool, i, block, proto_chunk, 
                            file_chunk_upload.bucket_id, file_chunk_upload.file_name
                        )
                        futures[future] = i
                    
                    # Wait for all uploads to complete and handle errors
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            future.result()  # This will raise exception if upload failed
                        except Exception as e:
                            # Cancel remaining futures
                            for f in futures:
                                f.cancel()
                            raise e
                            
            finally:
                try:
                    pool.close()
                except Exception as e:
                    logging.warning(f"Error closing connection pool: {str(e)}")
            
        except Exception as err:
            raise SDKError(f"failed to upload chunk: {err}")
        finally:
            # Always clean up the pool
            pool.close()


    def _upload_block(self, ctx, pool: ConnectionPool, block_index: int, block, proto_chunk, bucket_id, file_name: str) -> None:
        """Upload a single block with proper cryptographic signing matching Go implementation"""
        try:
            # Handle both dict and object formats for block
            node_address = block.node_address if hasattr(block, 'node_address') else block["node_address"]
            block_data_bytes = block.data if hasattr(block, 'data') else block["data"]
            block_cid = block.cid if hasattr(block, 'cid') else block["cid"]
            node_id = block.node_id if hasattr(block, 'node_id') else block["node_id"]
            
            client, closer, err = pool.create_ipc_client(node_address, self.use_connection_pool)
            if err:
                raise SDKError(f"failed to create client: {str(err)}")
            
            try:
                # Create streaming uploader matching Go implementation
                def upload_streaming():
                    # Start streaming upload
                    def message_generator():
                        # Generate nonce
                        nonce = secrets.randbits(256)
                        
                        # Create signature following EIP712 standard like Go version
                        signature_hex, nonce_bytes = self._create_storage_signature(
                            proto_chunk.cid, block_cid, proto_chunk.index, 
                            block_index, node_id, nonce
                        )
                        
                        # Upload block data in segments matching Go uploadIPCBlockSegments
                        data_len = len(block_data_bytes)
                        if data_len == 0:
                            return
                        
                        i = 0
                        is_first_part = True
                        
                        while i < data_len:
                            end = min(i + self.block_part_size, data_len)
                            segment_data = block_data_bytes[i:end]
                            
                            # Create block data matching Go IPCFileBlockData structure
                            block_data = ipcnodeapi_pb2.IPCFileBlockData(
                                data=segment_data,
                                cid=block_cid if is_first_part else "",  # Only send CID in first part
                                index=block_index,
                                chunk=proto_chunk if is_first_part else None,  # Only send chunk in first part
                                bucket_id=bucket_id,
                                file_name=file_name,
                                signature=signature_hex,
                                nonce=nonce_bytes,
                                node_id=node_id.encode() if isinstance(node_id, str) else node_id
                            )
                            
                            yield block_data
                            
                            i = end
                            is_first_part = False  # Clear fields for subsequent parts like Go
                    
                    try:
                        # Execute streaming upload
                        response = client.FileUploadBlock(message_generator())
                        return response
                    except Exception as e:
                        raise SDKError(f"streaming upload failed: {str(e)}")
                
                # Execute streaming upload
                upload_streaming()
                    
            finally:
                if closer:
                    closer()
                    
        except Exception as err:
            block_cid = block.cid if hasattr(block, 'cid') else block["cid"]
            raise SDKError(f"failed to upload block {block_cid}: {str(err)}")
    
    def _create_storage_signature(self, chunk_cid: str, block_cid: str, chunk_index: int, 
                                 block_index: int, node_id: str, nonce: int) -> tuple:
        """Create EIP712 signature for storage operation matching Go implementation"""
        try:
            # Generate nonce bytes
            nonce_bytes = nonce.to_bytes(32, byteorder='big')
            
            # Decode CIDs to get proper byte representations
            chunk_cid_obj = CID.decode(chunk_cid)
            block_cid_obj = CID.decode(block_cid)
            
            # Get chunk CID bytes
            if hasattr(chunk_cid_obj, 'bytes'):
                chunk_cid_bytes = chunk_cid_obj.bytes
            elif hasattr(chunk_cid_obj, 'encode'):
                chunk_cid_bytes = chunk_cid_obj.encode()
            elif hasattr(chunk_cid_obj, 'buffer'):
                chunk_cid_bytes = chunk_cid_obj.buffer
            else:
                chunk_cid_bytes = chunk_cid.encode()
            
            # Get block CID bytes and create 32-byte block CID (matching Go bcid [32]byte)
            if hasattr(block_cid_obj, 'bytes'):
                block_cid_bytes = block_cid_obj.bytes
            elif hasattr(block_cid_obj, 'encode'):
                block_cid_bytes = block_cid_obj.encode()
            elif hasattr(block_cid_obj, 'buffer'):
                block_cid_bytes = block_cid_obj.buffer
            else:
                block_cid_bytes = block_cid.encode()
            
            # Create 32-byte block CID (matching Go bcid [32]byte)
            bcid = bytearray(32)
            
            # Ensure we have bytes, not string
            if isinstance(block_cid_bytes, str):
                block_cid_bytes = block_cid_bytes.encode()
            elif not isinstance(block_cid_bytes, (bytes, bytearray)):
                block_cid_bytes = bytes(block_cid_bytes)
            
            # Copy bytes from position 4 to match Go: copy(bcid[:], blockCid.Bytes()[4:])
            if len(block_cid_bytes) > 4:
                copy_len = min(len(block_cid_bytes) - 4, 32)
                bcid[:copy_len] = block_cid_bytes[4:4+copy_len]
            else:
                copy_len = min(len(block_cid_bytes), 32)
                bcid[:copy_len] = block_cid_bytes[:copy_len]
            
            # Prepare node ID bytes - handle libp2p peer ID properly
            try:
                # Try to decode as libp2p peer ID first
                from libp2p.peer.id import ID as PeerID
                peer_id = PeerID.from_base58(node_id) if isinstance(node_id, str) else node_id
                node_id_bytes = peer_id.to_bytes() if hasattr(peer_id, 'to_bytes') else node_id.encode()
            except:
                # Fallback to simple encoding
                node_id_bytes = node_id.encode() if isinstance(node_id, str) else node_id
            
            # Ensure chunk_cid_bytes is proper bytes format
            if isinstance(chunk_cid_bytes, str):
                chunk_cid_bytes = chunk_cid_bytes.encode()
            elif not isinstance(chunk_cid_bytes, (bytes, bytearray)):
                chunk_cid_bytes = bytes(chunk_cid_bytes)
            
            # Create EIP712 message matching Go version
            message = {
                "chunkCID": chunk_cid_bytes,
                "blockCID": bytes(bcid),
                "chunkIndex": chunk_index,
                "blockIndex": block_index,
                "nodeId": node_id_bytes,
                "nonce": nonce
            }
            
            # EIP712 types matching Go version
            types = {
                "StorageData": [
                    {"name": "chunkCID", "type": "bytes"},
                    {"name": "blockCID", "type": "bytes32"},
                    {"name": "chunkIndex", "type": "uint256"},
                    {"name": "blockIndex", "type": "uint8"},
                    {"name": "nodeId", "type": "bytes"},
                    {"name": "nonce", "type": "uint256"}
                ]
            }
            
            # EIP712 domain matching Go version
            domain = {
                "name": "Storage",
                "version": "1", 
                "chainId": int(self.ipc.chain_id) if hasattr(self.ipc, 'chain_id') else 1,
                "verifyingContract": self.ipc.storage_address if hasattr(self.ipc, 'storage_address') else "0x0000000000000000000000000000000000000000"
            }
            
            # Create structured data
            structured_data = {
                "types": types,
                "primaryType": "StorageData",
                "domain": domain,
                "message": message
            }
            
            # Sign with private key
            try:
                encoded_data = encode_structured_data(structured_data)
                account = Account.from_key(self.ipc.auth.key)
                signed_message = account.sign_message(encoded_data)
                signature_hex = signed_message.signature.hex()
            except Exception as e:
                raise SDKError(f"EIP712 signing failed: {str(e)}")
            
            return signature_hex, nonce_bytes
            
        except Exception as e:
            raise SDKError(f"signature creation failed: {str(e)}")

    def fetch_block_data(
        self,
        ctx: Any,
        pool: ConnectionPool,
        chunk_cid: str,
        bucket_name: str,
        file_name: str,
        address: str,
        chunk_index: int,
        block_index: int,
        block
    ) -> bytes:
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
                
                return buffer.getvalue()
            finally:
                if closer:
                    try:
                        closer()
                    except Exception as e:
                        logging.warning(f"Failed to close connection for block {block.cid}: {str(e)}")
        except Exception as e:
            raise SDKError(f"failed to fetch block data: {str(e)}")

    def create_file_download(self, ctx, bucket_name: str, file_name: str):
        try:
            if not bucket_name:
                raise SDKError("empty bucket name")
                
            if not file_name:
                raise SDKError("empty file name")
            
            if not self.client:
                raise SDKError("IPC client has not been initialized.")
                
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
            

    def download(
        self, ctx: Any, file_download: IPCFileDownload, writer: io.IOBase
    ) -> None:
        try:
            file_enc_key = encryption_key(self.encryption_key, file_download.bucket_name, file_download.name)
            for chunk in file_download.chunks:
                if hasattr(ctx, "done") and ctx.done():
                    raise SDKError("context cancelled")
                chunk_dl = self.create_chunk_download(ctx, file_download.bucket_name, file_download.name, chunk)
                self.download_chunk_blocks(
                    ctx,
                    file_download.bucket_name,
                    file_download.name,
                    self.ipc.auth.address,
                    chunk_dl,
                    file_enc_key,
                    writer
                )
        except Exception as err:
            raise SDKError(f"failed to download file: {str(err)}")



    def create_chunk_download(
        self, ctx: Any, bucket_name: str, file_name: str, chunk: Chunk
    ) -> FileChunkDownload:
        request = ipcnodeapi_pb2.IPCFileDownloadChunkCreateRequest(
            bucket_name=bucket_name,
            file_name=file_name,
            chunk_cid=chunk.cid,
            address=self.ipc.auth.address.lower()
        )
        response = self.client.FileDownloadChunkCreate(request)
        blocks: List[FileBlockDownload] = []
        for b in response.blocks:
            blocks.append(FileBlockDownload(
                cid=b.cid,
                data=b"" ,
                akave=AkaveBlockData(
                    permit=b.permit,
                    node_address=b.node_address,
                    node_id=b.node_id
                ) if b.node_id else None,
                filecoin=None
            ))
        return FileChunkDownload(
            cid=chunk.cid,
            index=chunk.index,
            encoded_size=chunk.encoded_size,
            size=chunk.size,
            blocks=blocks
        )
            

    def download_chunk_blocks(
        self,
        ctx: Any,
        bucket_name: str,
        file_name: str,
        address: str,
        chunk_download: FileChunkDownload,
        file_encryption_key: bytes,
        writer: io.IOBase
    ) -> None:
        pool = ConnectionPool()
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                futures: Dict[concurrent.futures.Future, int] = {}
                for i, block in enumerate(chunk_download.blocks):
                    futures[executor.submit(
                        self.fetch_block_data,
                        ctx, pool,
                        chunk_download.cid,
                        bucket_name,
                        file_name,
                        address,
                        chunk_download.index,
                        i,
                        block
                    )] = i
                blocks: List[Optional[bytes]] = [None for _ in range(len(chunk_download.blocks))]
                for fut in concurrent.futures.as_completed(futures):
                    idx = futures[fut]
                    data = fut.result()
                    blocks[idx] = extract_block_data(chunk_download.blocks[idx].cid, data)
            # assemble
            if self.erasure_code:
                # Filter out None values and concatenate blocks for erasure code
                non_null_blocks = [b for b in blocks if b is not None]
                encoded_data = b"".join(non_null_blocks)
                data = self.erasure_code.extract_data(encoded_data, int(chunk_download.size))
            else:
                data = b"".join(b for b in blocks if b is not None)
            if file_encryption_key:
                data = decrypt(file_encryption_key, data, str(chunk_download.index).encode())
            writer.write(data)
        finally:
            pool.close()
    
    