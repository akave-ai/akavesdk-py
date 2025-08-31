import time
import logging
import binascii
import io
import math
import concurrent.futures
import secrets
from hashlib import sha256
from typing import List, Optional, Callable, Dict, Any, Union, Tuple
from google.protobuf.timestamp_pb2 import Timestamp
from datetime import datetime
import grpc 

from .config import MIN_BUCKET_NAME_LENGTH, SDKError, SDKConfig, BLOCK_SIZE, ENCRYPTION_OVERHEAD
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
    from eth_account.messages import encode_typed_data
    from eth_utils import to_checksum_address
    from eth_keys import keys
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

def to_ipc_proto_chunk(chunk_cid, index: int, size: int, blocks):
    """Convert to IPC proto chunk format matching Go implementation"""
    from private.pb import ipcnodeapi_pb2
    
    cids = []  # [][32]byte
    sizes = []  # []*big.Int (but we'll use regular ints)
    
    # Convert chunk CID to string if it's a CID object
    if hasattr(chunk_cid, '__str__'):
        chunk_cid_str = str(chunk_cid)
    else:
        chunk_cid_str = chunk_cid
    
    pb_blocks = []
    for block in blocks:
        block_cid = block.cid if hasattr(block, 'cid') else block["cid"]
        block_data = block.data if hasattr(block, 'data') else block["data"]
        
        if hasattr(block_cid, '__str__'):
            block_cid_str = str(block_cid)
        else:
            block_cid_str = block_cid
        
        pb_block = ipcnodeapi_pb2.IPCChunk.Block(
            cid=block_cid_str,
            size=len(block_data)
        )
        pb_blocks.append(pb_block)
        
        try:
            try:
                if hasattr(block_cid, 'bytes'):
                    cid_bytes = block_cid.bytes if callable(block_cid.bytes) else block_cid.bytes()
                else:
                    c = CID.decode(block_cid_str)
                    cid_bytes = c.bytes if callable(c.bytes) else c.bytes()
            except Exception:
                import hashlib
                cid_bytes = hashlib.sha256(block_cid_str.encode()).digest()
            
            bcid = bytearray(32)
            
            if isinstance(cid_bytes, str):
                cid_bytes = cid_bytes.encode()
            elif not isinstance(cid_bytes, (bytes, bytearray)):
                cid_bytes = bytes(cid_bytes)
                
            if len(cid_bytes) > 4:
                copy_len = min(len(cid_bytes) - 4, 32)
                bcid[:copy_len] = cid_bytes[4:4+copy_len]
            else:
                copy_len = min(len(cid_bytes), 32)
                bcid[:copy_len] = cid_bytes[:copy_len]
                
            cids.append(bytes(bcid))
            sizes.append(len(block_data))
        except Exception as e:
            return None, None, None, SDKError(f"failed to process CID: {str(e)}")
    
    proto_chunk = ipcnodeapi_pb2.IPCChunk(
        cid=chunk_cid_str,
        index=index,
        size=size,
        blocks=pb_blocks
    )
    
    return cids, sizes, proto_chunk, None

class IPC:
    def __init__(self, client, conn, ipc_instance, config: SDKConfig):
        self.client = client
        self.conn = conn
        self.ipc = ipc_instance
        self.max_concurrency = config.max_concurrency
        self.block_part_size = config.block_part_size
        self.use_connection_pool = config.use_connection_pool
        self.encryption_key = config.encryption_key if config.encryption_key else b''
        self.max_blocks_in_chunk = config.streaming_max_blocks_in_chunk
        self.erasure_code = config.erasure_code
        self.chunk_buffer = config.chunk_buffer

    def create_bucket(self, ctx, name: str) -> IPCBucketCreateResult:
        if len(name) < MIN_BUCKET_NAME_LENGTH:
            raise SDKError("invalid bucket name")

        try:
            tx = self.ipc.storage.create_bucket(
                bucket_name=name,
                from_address=self.ipc.auth.address,
                private_key=self.ipc.auth.key,
                gas_limit=500000,
                nonce_manager=self.ipc.nonce_manager
            )
            receipt = self.ipc.web3.eth.wait_for_transaction_receipt(tx)
            
            if receipt.status != 1:
                raise SDKError("bucket creation transaction failed")
                
            block = self.ipc.web3.eth.get_block(receipt.blockNumber)
            created_at = block.timestamp
            
            return IPCBucketCreateResult(
                name=name,
                created_at=created_at
            )
            
        except Exception as e:
            logging.error(f"IPC create_bucket failed: {e}")
            raise SDKError(f"bucket creation failed: {e}")

    def view_bucket(self, ctx, bucket_name: str) -> Optional[IPCBucket]:
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
            created_at = 0
            if hasattr(response, 'created_at') and response.created_at:
                created_at = int(response.created_at.seconds)

            return IPCBucket(
                id=response.id if hasattr(response, 'id') else '',
                name=response.name if hasattr(response, 'name') else bucket_name,
                created_at=created_at
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return None
            logging.error(f"IPC view_bucket gRPC failed: {e.code()} - {e.details()}")
            raise SDKError(f"failed to view bucket: {e.details()}")
        except Exception as err:
            logging.error(f"IPC view_bucket unexpected error: {err}")
            raise SDKError(f"failed to view bucket: {err}")

    def list_buckets(self, ctx) -> list[IPCBucket]:
        try:
            request = ipcnodeapi_pb2.IPCBucketListRequest(
                address=self.ipc.auth.address.lower()
            )
            logging.info(f"Sending BucketList request with address: {self.ipc.auth.address.lower()}")
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
            logging.error(f"IPC list_buckets gRPC failed: {e.code()} - {e.details()}")
            raise SDKError(f"failed to list buckets: {e.details()}")
        except Exception as err:
            logging.error(f"IPC list_buckets unexpected error: {err}")
            raise SDKError(f"failed to list buckets: {err}")

    def delete_bucket(self, ctx, name: str) -> None:
        if not name:
            raise SDKError("empty bucket name")
        try:
            request = ipcnodeapi_pb2.IPCBucketViewRequest(
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

    def file_info(self, ctx, bucket_name: str, file_name: str) -> Optional[IPCFileMeta]:
        if not bucket_name:
            raise SDKError("empty bucket name")
        
        if not file_name:
            raise SDKError("empty file name")

        try:
            request = ipcnodeapi_pb2.IPCFileViewRequest(
                file_name=file_name,
                bucket_name=bucket_name,
                address=self.ipc.auth.address.lower()
            )
            response = self.client.FileView(request)
            
            if not response:
                logging.info(f"File '{file_name}' in bucket '{bucket_name}' not found.")
                return None
            
            created_at = 0
            if hasattr(response, 'created_at') and response.created_at:
                created_at = int(response.created_at.seconds)
            
            return IPCFileMeta(
                root_cid=response.root_cid if hasattr(response, 'root_cid') else '',
                name=response.file_name if hasattr(response, 'file_name') else file_name,
                bucket_name=response.bucket_name if hasattr(response, 'bucket_name') else bucket_name,
                encoded_size=response.encoded_size if hasattr(response, 'encoded_size') else 0,
                created_at=created_at
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                logging.info(f"File '{file_name}' in bucket '{bucket_name}' not found via gRPC.")
                return None
            logging.error(f"IPC file_info gRPC failed: {e.code()} - {e.details()}")
            raise SDKError(f"failed to get file info: {e.details()}")
        except Exception as err:
            logging.error(f"IPC file_info unexpected error: {err}")
            raise SDKError(f"failed to get file info: {err}")

    def list_files(self, ctx, bucket_name: str) -> list[IPCFileListItem]:
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
                logging.info(f"Received FileList response with {len(response.list)} files")
                for file_item in response.list:
                    created_at = 0
                    if hasattr(file_item, 'created_at') and file_item.created_at:
                        created_at = int(file_item.created_at.seconds)
                    
                    file_name = file_item.name if hasattr(file_item, 'name') else ''
                    root_cid = file_item.root_cid if hasattr(file_item, 'root_cid') else ''
                    encoded_size = file_item.encoded_size if hasattr(file_item, 'encoded_size') else 0
                    
                    logging.info(f"Processing file: name={file_name}, root_cid={root_cid}, size={encoded_size}, created_at={created_at}")
                    
                    files.append(IPCFileListItem(
                        name=file_name,
                        root_cid=root_cid,
                        encoded_size=encoded_size,
                        created_at=created_at
                    ))
            else:
                logging.warning("FileList response has no 'list' field or is empty")
            
            return files
        except grpc.RpcError as e:
            logging.error(f"IPC list_files gRPC failed: {e.code()} - {e.details()}")
            raise SDKError(f"failed to list files: {e.details()}")
        except Exception as err:
            logging.error(f"IPC list_files unexpected error: {err}")
            raise SDKError(f"failed to list files: {err}")

    def file_delete(self, ctx, bucket_name: str, file_name: str) -> None:
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
        try:
            if not bucket_name:
                raise SDKError("empty bucket name")
            if not file_name:
                raise SDKError("empty file name")
            
            original_file_name = file_name
            original_bucket_name = bucket_name
            
            file_name = maybe_encrypt_metadata(file_name, bucket_name + "/" + file_name, self.encryption_key)
            bucket_name = maybe_encrypt_metadata(bucket_name, bucket_name, self.encryption_key)
            
            try:
                bucket = self.ipc.storage.get_bucket_by_name(
                    {"from": self.ipc.auth.address},
                    bucket_name
                )
                if not bucket:
                    raise SDKError("failed to retrieve bucket")
            except Exception as e:
                raise SDKError(f"failed to get bucket: {str(e)}")
            
            try:
                tx_hash = self.ipc.storage.create_file(
                    self.ipc.auth.address, 
                    self.ipc.auth.key,
                    bucket[0],  # bucket_id
                    file_name,
                    nonce_manager=self.ipc.nonce_manager
                )
                logging.info(f"File creation transaction sent, tx_hash: {tx_hash}")
                
                if hasattr(self.ipc, 'wait_for_tx'):
                    self.ipc.wait_for_tx(tx_hash)
                elif hasattr(self.ipc, 'web3') and hasattr(self.ipc.web3.eth, 'wait_for_transaction_receipt'):
                    receipt = self.ipc.web3.eth.wait_for_transaction_receipt(tx_hash)
                    if receipt.status != 1:
                        raise SDKError("CreateFile transaction failed")
                
            except Exception as e:
                raise SDKError(f"failed to create file in storage contract: {str(e)}")
            
            chunk_enc_overhead = 0
            try:
                file_enc_key = encryption_key(self.encryption_key, bucket_name, file_name)
                if len(file_enc_key) > 0:
                    chunk_enc_overhead = EncryptionOverhead
            except Exception as e:
                raise SDKError(f"encryption key derivation failed: {str(e)}")
            
            buffer_size = self.max_blocks_in_chunk * int(BlockSize)
            if self.erasure_code:  
                buffer_size = self.erasure_code.data_blocks * int(BlockSize)
            buffer_size -= chunk_enc_overhead
            
            dag_root = DAGRoot.new()
            
            import queue
            file_upload_chunks_queue = queue.Queue(maxsize=self.chunk_buffer)
            
            chunk_processing_done = False
            chunk_processing_error = None
            
            def chunk_reader():
                nonlocal chunk_processing_done, chunk_processing_error
                try:
                    buf = bytearray(buffer_size)
                    index = 0
                    
                    while True:
                        try:
                            bytes_read = 0
                            temp_buf = bytearray(buffer_size)
                            
                            while bytes_read < buffer_size:
                                n = reader.readinto(memoryview(temp_buf)[bytes_read:])
                                if n == 0 or n is None:
                                    break  
                                bytes_read += n
                            
                            if bytes_read == 0:
                                if index == 0:
                                    chunk_processing_error = SDKError("empty file")
                                    return
                                break 
                            
                            chunk_data = temp_buf[:bytes_read]
                            
                        except Exception as e:
                            chunk_processing_error = SDKError(f"failed to read file: {str(e)}")
                            return
                        
                        if bytes_read > 0:
                            try:
                                chunk_upload = self.create_chunk_upload(ctx, index, file_enc_key, chunk_data, bucket[0], file_name)
                                
                                if chunk_processing_error:
                                    return
                                    
                                file_upload_chunks_queue.put(chunk_upload, timeout=30)
                                index += 1
                            except Exception as e:
                                chunk_processing_error = SDKError(f"failed to create chunk upload: {str(e)}")
                                return
                        
                        if bytes_read < buffer_size:
                            break
                    
                    chunk_processing_done = True
                    
                except Exception as e:
                    chunk_processing_error = SDKError(f"chunk reader error: {str(e)}")
                finally:
                    file_upload_chunks_queue.put(None)  
            
            import threading
            chunk_reader_thread = threading.Thread(target=chunk_reader)
            chunk_reader_thread.start()
            
            file_size = 0
            actual_file_size = 0
            chunk_count = 0
            
            while True:
                try:
                    if chunk_processing_error:
                        break
                    
                    try:
                        chunk_upload = file_upload_chunks_queue.get(timeout=1)  # Short timeout for responsiveness
                    except queue.Empty:
                        if chunk_processing_done:
                            break
                        continue
                    
                    if chunk_upload is None: 
                        break
                    
                    try:
                        dag_root.add_link(chunk_upload.chunk_cid, chunk_upload.raw_data_size, chunk_upload.encoded_size)
                    except Exception as e:
                        raise SDKError(f"failed to add DAG link: {str(e)}")
                    
                    try:
                        self.upload_chunk(ctx, chunk_upload)
                    except Exception as e:
                        raise SDKError(f"failed to upload chunk: {str(e)}")
                    
                    file_size += chunk_upload.encoded_size
                    actual_file_size += chunk_upload.actual_size
                    chunk_count += 1
                    
                except Exception as e:
                    chunk_processing_error = e
                    break
            
            chunk_reader_thread.join(timeout=300)  #
            
            if chunk_reader_thread.is_alive():
                chunk_processing_error = SDKError("chunk reader thread timeout")
            
            if chunk_processing_error:
                raise chunk_processing_error
            
            root_cid = dag_root.build()
            
            try:
                file_meta = self.ipc.storage.get_file_by_name(
                    {"from": self.ipc.auth.address},
                    bucket[0],  # bucket[0] is the bucket ID
                    file_name
                )
            except Exception as e:
                raise SDKError(f"failed to get file metadata: {str(e)}")
            
            file_id = self._calculate_file_id(bucket[0], file_name)
            
            is_filled = False
            max_wait_time = 300  
            wait_start = time.time()
            
            while not is_filled:
                try:
                    is_filled = self.ipc.storage.is_file_filled({"from": self.ipc.auth.address}, file_id)
                    if is_filled:
                        break
                except Exception as e:
                    logging.warning(f"IsFileFilled check failed: {e}, assuming file is filled")
                    time.sleep(2)
                    break
                
                if time.time() - wait_start > max_wait_time:
                    raise SDKError("timeout waiting for file to be filled")
                
                time.sleep(1)  
            
            try:
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
        try:
            from Crypto.Hash import keccak
            combined = bucket_id + file_name.encode()
            hash_obj = keccak.new(digest_bits=256)
            hash_obj.update(combined)
            return hash_obj.digest()
        except ImportError:
                raise SDKError("Failed to import required modules for file ID calculation")
        except Exception as e:
            raise SDKError(f"Failed to calculate file ID: {str(e)}")

    def _convert_cid_to_bytes(self, cid_input) -> bytes:      
        if hasattr(cid_input, 'bytes'):
            try:
                if callable(cid_input.bytes):
                    return cid_input.bytes()
                else:
                    return bytes(cid_input.bytes)
            except Exception as e:
                logging.warning(f"Failed to access .bytes on CID object: {e}")
        elif hasattr(cid_input, '__bytes__'):
            try:
                return bytes(cid_input)
            except Exception as e:
                logging.warning(f"Failed to call __bytes__ on CID object: {e}")
        elif hasattr(cid_input, 'encode') and callable(cid_input.encode):
            try:
                result = cid_input.encode()
                if isinstance(result, bytes):
                    return result
                return result.encode() if isinstance(result, str) else bytes(result)
            except Exception as e:
                logging.warning(f"Failed to call .encode() on CID object: {e}")
        
        if not isinstance(cid_input, str):
            cid_str = str(cid_input)
        else:
            cid_str = cid_input
            
        try:
            from multiformats import CID as CIDLib
            cid_obj = CIDLib.decode(cid_str)
            return self._convert_cid_to_bytes(cid_obj)
        except Exception as e:
            logging.warning(f"Failed to decode CID using library: {e}")
            
            import hashlib
            return hashlib.sha256(cid_str.encode()).digest()

    def create_chunk_upload(self, ctx, index: int, file_encryption_key: bytes, data: bytes, bucket_id: bytes, file_name: str) -> IPCFileChunkUploadV2:
        try:
            if len(file_encryption_key) > 0:
                data = encrypt(file_encryption_key, data, str(index).encode())
            
            size = len(data)
            
            block_size = BlockSize
            if self.erasure_code:
                data = self.erasure_code.encode(data)
                blocks_count = self.erasure_code.data_blocks + self.erasure_code.parity_blocks
                block_size = len(data) // blocks_count
            
            chunk_dag = build_dag(ctx, io.BytesIO(data), block_size, None)
            if chunk_dag is None:
                raise SDKError("build_dag returned None")
            
            chunk_cid_str = chunk_dag.cid if isinstance(chunk_dag.cid, str) else str(chunk_dag.cid)
            
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
            
            request = ipcnodeapi_pb2.IPCFileUploadChunkCreateRequest(
                chunk=proto_chunk,
                bucket_id=bucket_id,  # bytes
                file_name=file_name
            )
            
            response = self.client.FileUploadChunkCreate(request)
            if response is None:
                raise SDKError("gRPC response is None")
            
            if len(response.blocks) != len(chunk_dag.blocks):
                raise SDKError(f"received unexpected amount of blocks {len(response.blocks)}, expected {len(chunk_dag.blocks)}")
            
            for i, upload in enumerate(response.blocks):
                if chunk_dag.blocks[i].cid != upload.cid:
                    raise SDKError(f"block CID mismatch at position {i}")
                chunk_dag.blocks[i].node_address = upload.node_address
                chunk_dag.blocks[i].node_id = upload.node_id
                chunk_dag.blocks[i].permit = upload.permit
            
            chunk_cid_bytes = self._convert_cid_to_bytes(chunk_dag.cid)
            
            tx = self.ipc.storage.add_file_chunk(
                self.ipc.auth.address,      # from_address: HexAddress
                self.ipc.auth.key,          # private_key: str
                chunk_cid_bytes,            # cid: bytes (chunk CID) - MUST be bytes (converted from chunk_dag.cid)
                bucket_id,                  # bucket_id: bytes (bucket ID as bytes32)
                file_name,                  # name: str (file name)
                chunk_dag.encoded_size,  # encoded_chunk_size: int (proto node size)
                cids,                       # cids: list (array of block CIDs as bytes32[])
                sizes,                      # chunk_blocks_sizes: list (array of block sizes)
                index,                      # chunk_index: int (chunk index)
                nonce_manager=self.ipc.nonce_manager  # nonce_manager for coordinated transactions
            )
            
            if hasattr(self.ipc, 'wait_for_tx') and hasattr(tx, 'hex'):
                self.ipc.wait_for_tx(tx.hex())
            elif hasattr(self.ipc, 'web3') and hasattr(self.ipc.web3.eth, 'wait_for_transaction_receipt'):
                receipt = self.ipc.web3.eth.wait_for_transaction_receipt(tx)
                if receipt.status != 1:
                    raise SDKError("AddFileChunk transaction failed")
            
            return IPCFileChunkUploadV2(
                index=index,
                chunk_cid=chunk_dag.cid,
                actual_size=size,
                raw_data_size=chunk_dag.raw_data_size,
                encoded_size=chunk_dag.encoded_size,
                blocks=chunk_dag.blocks,
                bucket_id=bucket_id,
                file_name=file_name
            )
        except Exception as err:
            raise SDKError(f"failed to create chunk upload: {str(err)}")

    def upload_chunk(self, ctx, file_chunk_upload: IPCFileChunkUploadV2) -> None:
        try:
            pool = ConnectionPool()
            
            try:
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
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                    futures = {}
                    
                    for i, block in enumerate(file_chunk_upload.blocks):
                        future = executor.submit(
                            self._upload_block,
                            ctx, pool, i, block, proto_chunk, 
                            file_chunk_upload.bucket_id, file_chunk_upload.file_name
                        )
                        futures[future] = i
                    
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            future.result()  # This will raise exception if upload failed
                        except Exception as e:
                            for f in futures:
                                f.cancel()
                            raise e
                            
            finally:
                try:
                    pool.close()
                except Exception as e:
                    logging.warning(f"Error closing connection pool: {str(e)}")
            
        except Exception as err:
            raise SDKError(f"failed to upload chunk: {str(err)}")

    def _upload_block(self, ctx, pool: ConnectionPool, block_index: int, block, proto_chunk, bucket_id, file_name: str) -> None:
        try:
            node_address = block.node_address if hasattr(block, 'node_address') else block["node_address"]
            block_data_bytes = block.data if hasattr(block, 'data') else block["data"]
            block_cid = block.cid if hasattr(block, 'cid') else block["cid"]
            node_id = block.node_id if hasattr(block, 'node_id') else block["node_id"]
            
            if isinstance(block_data_bytes, memoryview):
                block_data_bytes = bytes(block_data_bytes)
            
            if not node_address or not node_address.strip():
                raise SDKError(f"Invalid node address for block {block_cid}: '{node_address}'")
            
            logging.debug(f"Uploading block {block_index}: CID={block_cid}, node={node_address}")
            
            result = pool.create_ipc_client(node_address, self.use_connection_pool)
            
            if len(result) == 3:
                client, closer, err = result
                if err:
                    raise SDKError(f"failed to create client: {str(err)}")
                if client is None:
                    raise SDKError(f"failed to create client: client is None")
            else:
                raise SDKError(f"Unexpected return format from create_ipc_client: {result}")
            
            try:
                def upload_streaming():
                    def message_generator():
                        nonce = secrets.randbits(256) 
                        
                        signature_hex, nonce_bytes = self._create_storage_signature(
                            proto_chunk.cid, block_cid, proto_chunk.index, 
                            block_index, node_id, nonce
                        )
                        
                        data_len = len(block_data_bytes)
                        if data_len == 0:
                            return
                        
                        i = 0
                        is_first_part = True
                        
                        while i < data_len:
                            end = min(i + self.block_part_size, data_len)
                            segment_data = block_data_bytes[i:end]
                            
                            if isinstance(segment_data, memoryview):
                                segment_data = bytes(segment_data)
                            
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
                            is_first_part = False  
                    
                    try:
                        response = client.FileUploadBlock(message_generator())
                        logging.debug(f"Block {block_cid} upload completed successfully")
                        return response
                    except Exception as e:
                        raise SDKError(f"streaming upload failed: {str(e)}")
                
                upload_streaming()
                    
            finally:
                if closer:
                    try:
                        closer()
                    except Exception as e:
                        logging.warning(f"Failed to close connection for block {block_cid}: {e}")
                        
        except Exception as err:
            block_cid = block.cid if hasattr(block, 'cid') else block["cid"]
            raise SDKError(f"failed to upload block {block_cid}: {str(err)}")
    
    def _create_storage_signature(self, chunk_cid: str, block_cid: str, chunk_index: int, 
                                 block_index: int, node_id: str, nonce: int) -> tuple:
        try:
            from private.eip712 import sign, Domain, TypedData
            nonce_bytes = nonce.to_bytes(32, byteorder='big')
            chunk_cid_obj = CID.decode(chunk_cid)
            block_cid_obj = CID.decode(block_cid)
            
            chunk_cid_bytes = self._convert_cid_to_bytes(chunk_cid_obj)
            
            block_cid_bytes = self._convert_cid_to_bytes(block_cid_obj)
            
            bcid = bytearray(32)
            if len(block_cid_bytes) > 4:
                copy_len = min(len(block_cid_bytes) - 4, 32)
                bcid[:copy_len] = block_cid_bytes[4:4+copy_len]
            else:
                copy_len = min(len(block_cid_bytes), 32)
                bcid[:copy_len] = block_cid_bytes[:copy_len]
           
            try:
                import base58
                if node_id.startswith('12D3'):
                    decoded = base58.b58decode(node_id)
                    node_id_bytes = decoded
                else:
                    node_id_bytes = node_id.encode() if isinstance(node_id, str) else node_id
            except ImportError:
                node_id_bytes = node_id.encode() if isinstance(node_id, str) else node_id
            
            chain_id = 78964  
            contract_address = "0x9Aa8ff1604280d66577ecB5051a3833a983Ca3aF"  
            
            domain = Domain(
                name="Storage",
                version="1",
                chain_id=chain_id,
                verifying_contract=contract_address
            )
            
            data_types = {
                "StorageData": [
                    TypedData("chunkCID", "bytes"),
                    TypedData("blockCID", "bytes32"), 
                    TypedData("chunkIndex", "uint256"),
                    TypedData("blockIndex", "uint8"),
                    TypedData("nodeId", "bytes"),
                    TypedData("nonce", "uint256")
                ]
            }
            
            from eth_utils import to_int
            
            data_message = {
                "chunkCID": bytes(chunk_cid_bytes),     
                "blockCID": bytes(bcid),                
                "chunkIndex": to_int(chunk_index),  # Ensure int for uint256
                "blockIndex": int(block_index) & 0xFF,  # Ensure uint8 range
                "nodeId": bytes(node_id_bytes),       
                "nonce": to_int(nonce)  # Ensure int for uint256
            }
            
            # Debug logging 
            print(f"[PYTHON_UPLOAD_SIGNATURE] Chunk CID: {chunk_cid}")
            print(f"[PYTHON_UPLOAD_SIGNATURE] Block CID: {block_cid}")
            print(f"[PYTHON_UPLOAD_SIGNATURE] Chunk Index: {chunk_index}")
            print(f"[PYTHON_UPLOAD_SIGNATURE] Block Index: {block_index}")
            print(f"[PYTHON_UPLOAD_SIGNATURE] Node ID: {node_id}")
            print(f"[PYTHON_UPLOAD_SIGNATURE] Nonce: {nonce}")
            print(f"[PYTHON_UPLOAD_SIGNATURE] Chain ID: {chain_id}")
            print(f"[PYTHON_UPLOAD_SIGNATURE] Contract Address: {contract_address}")
            print(f"[PYTHON_UPLOAD_SIGNATURE] Chunk CID bytes: {chunk_cid_bytes.hex()}")
            print(f"[PYTHON_UPLOAD_SIGNATURE] Block CID bytes (bcid): {bytes(bcid).hex()}")
            print(f"[PYTHON_UPLOAD_SIGNATURE] Node ID bytes: {node_id_bytes.hex()}")
            
            if isinstance(self.ipc.auth.key, bytes):
                private_key_bytes = self.ipc.auth.key
            else:
                key_str = str(self.ipc.auth.key).replace('0x', '')
                private_key_bytes = bytes.fromhex(key_str)
            
            print(f"[PYTHON_UPLOAD_SIGNATURE] Private key bytes length: {len(private_key_bytes)}")
            
            signature_bytes = sign(private_key_bytes, domain, data_message, data_types)
            signature_hex = signature_bytes.hex()
            
            print(f"[PYTHON_UPLOAD_SIGNATURE] Generated signature: {signature_hex}")
            print(f"[PYTHON_UPLOAD_SIGNATURE] Signature length: {len(signature_hex)}")
            
            return signature_hex, nonce_bytes
            
        except Exception as e:
            raise SDKError(f"signature creation failed: {str(e)}")

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
        try:
            if (not hasattr(block, 'akave') or not block.akave) and (not hasattr(block, 'filecoin') or not block.filecoin):
                raise SDKError("missing block metadata")
            
            client, closer, err = pool.create_ipc_client(block.akave.node_address, self.use_connection_pool)
            if err:
                raise SDKError(f"failed to create client: {str(err)}")
            
            try:
                download_req = ipcnodeapi_pb2.IPCFileDownloadBlockRequest(
                    chunk_cid=chunk_cid,
                    chunk_index=chunk_index,
                    block_cid=block.cid,
                    block_index=block_index,
                    bucket_name=bucket_name,
                    file_name=file_name,
                    address=address
                )
                
                download_client = client.FileDownloadBlock(download_req)
                if not download_client:
                    raise SDKError("failed to get download client")
                
                buffer = io.BytesIO()
                
                for block_data in download_client:
                    if not block_data:
                        break
                    buffer.write(block_data.data)
                
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
            
            file_name = maybe_encrypt_metadata(file_name, bucket_name + "/" + file_name, self.encryption_key)
            bucket_name = maybe_encrypt_metadata(bucket_name, bucket_name, self.encryption_key)
                
            request = ipcnodeapi_pb2.IPCFileDownloadCreateRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                address=self.ipc.auth.address
            )
            
            response = self.client.FileDownloadCreate(request)
            
            chunks = []
            for i, chunk in enumerate(response.chunks):
                chunks.append(Chunk(
                    cid=chunk.cid,
                    encoded_size=chunk.encoded_size,
                    size=chunk.size,
                    index=i 
                ))
            
            return IPCFileDownload(
                bucket_name=response.bucket_name,
                name=file_name,
                chunks=chunks
            )
        except Exception as err:
            raise SDKError(f"failed to create file download: {str(err)}")
    
    def create_range_file_download(self, ctx, bucket_name: str, file_name: str, start: int, end: int):
        try:
            if not bucket_name:
                raise SDKError("empty bucket name")
                
            if not file_name:
                raise SDKError("empty file name")
                
            file_name = maybe_encrypt_metadata(file_name, bucket_name + "/" + file_name, self.encryption_key)
            bucket_name = maybe_encrypt_metadata(bucket_name, bucket_name, self.encryption_key)
                
            request = ipcnodeapi_pb2.IPCFileDownloadRangeCreateRequest(
                bucket_name=bucket_name,
                file_name=file_name,
                address=self.ipc.auth.address,
                start_index=start,
                end_index=end
            )
            
            response = self.client.FileDownloadRangeCreate(request)
            
            chunks = []
            for i, chunk in enumerate(response.chunks):
                chunks.append(Chunk(
                    cid=chunk.cid,
                    encoded_size=chunk.encoded_size,
                    size=chunk.size,
                    index=i + start  
                ))
            
            return IPCFileDownload(
                bucket_name=response.bucket_name,
                name=file_name,
                chunks=chunks
            )
        except Exception as err:
            raise SDKError(f"failed to create range file download: {str(err)}")

    def file_set_public_access(self, ctx, bucket_name: str, file_name: str, is_public: bool) -> None:
        try:
            if not bucket_name:
                raise SDKError("empty bucket name")
            if not file_name:
                raise SDKError("empty file name")
                
            original_file_name = file_name
            original_bucket_name = bucket_name
            
            file_name = maybe_encrypt_metadata(file_name, bucket_name + "/" + file_name, self.encryption_key)
            bucket_name = maybe_encrypt_metadata(bucket_name, bucket_name, self.encryption_key)
            
            try:
                bucket = self.ipc.storage.get_bucket_by_name(
                    {"from": self.ipc.auth.address},
                    bucket_name
                )
                if not bucket:
                    raise SDKError("failed to retrieve bucket")
            except Exception as e:
                raise SDKError(f"failed to get bucket: {str(e)}")
            
            try:
                file = self.ipc.storage.get_file_by_name(
                    {},
                    bucket[0],  # bucket ID
                    file_name
                )
                if not file:
                    raise SDKError("failed to retrieve file")
            except Exception as e:
                raise SDKError(f"failed to get file: {str(e)}")
            
            if not hasattr(self.ipc, 'access_manager') or self.ipc.access_manager is None:
                raise SDKError("access manager not available")
            
            try:
                tx_hash = self.ipc.access_manager.change_public_access(
                    self.ipc.auth,  # auth object
                    file[0],        # file ID
                    is_public       # is_public flag
                )
                
                if hasattr(self.ipc, 'wait_for_tx'):
                    self.ipc.wait_for_tx(tx_hash)
                elif hasattr(self.ipc, 'web3') and hasattr(self.ipc.web3.eth, 'wait_for_transaction_receipt'):
                    receipt = self.ipc.web3.eth.wait_for_transaction_receipt(tx_hash)
                    if receipt.status != 1:
                        raise SDKError("ChangePublicAccess transaction failed")
                
                logging.info(f"Successfully {'enabled' if is_public else 'disabled'} public access for file '{original_file_name}' in bucket '{original_bucket_name}', tx_hash: {tx_hash}")
                return None
                
            except Exception as e:
                raise SDKError(f"failed to change public access: {str(e)}")
                
        except Exception as err:
            logging.error(f"IPC file_set_public_access failed: {err}")
            raise SDKError(f"failed to set public access: {str(err)}")
            
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