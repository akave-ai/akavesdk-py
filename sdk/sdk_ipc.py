import binascii
import concurrent.futures
import io
import logging
import math
import queue
import secrets
import threading
import time
from datetime import datetime
from hashlib import sha256
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from .config import BLOCK_SIZE, ENCRYPTION_OVERHEAD, MIN_BUCKET_NAME_LENGTH, SDKConfig, SDKError
from .connection import ConnectionPool
from .dag import DAGRoot, build_dag, extract_block_data
from .model import (
    Chunk,
    FileBlockDownload,
    FileBlockUpload,
    FileChunkDownload,
    IPCBucket,
    IPCBucketCreateResult,
    IPCFileChunkUploadV2,
    IPCFileDownload,
    IPCFileListItem,
    IPCFileMeta,
    IPCFileMetaV2,
    IPCFileUpload,
    UploadState,
    new_ipc_file_upload,
)


class TxWaitSignal:
    def __init__(self, FileUploadChunk, Transaction):
        self.FileUploadChunk = FileUploadChunk
        self.Transaction = Transaction


from private.encryption import decrypt, derive_key, encrypt
from private.pb import ipcnodeapi_pb2, ipcnodeapi_pb2_grpc

try:
    from multiformats.cid import CID
except ImportError:
    pass

try:
    from eth_account import Account
    from eth_account.messages import encode_typed_data
    from eth_keys import keys
    from eth_utils import to_checksum_address
except ImportError:
    pass

BlockSize = BLOCK_SIZE
EncryptionOverhead = ENCRYPTION_OVERHEAD


def encryption_key(parent_key: bytes, *info_data: str):
    if len(parent_key) == 0:
        return b""

    info = "/".join(info_data)
    return derive_key(parent_key, info.encode())


def maybe_encrypt_metadata(value: str, derivation_path: str, encryption_key: bytes) -> str:
    if len(encryption_key) == 0:
        return value

    try:
        file_enc_key = derive_key(encryption_key, derivation_path.encode())
        encrypted_data = encrypt(file_enc_key, value.encode(), b"metadata")
        return encrypted_data.hex()
    except Exception as e:
        raise SDKError(f"failed to encrypt metadata: {str(e)}")


def to_ipc_proto_chunk(chunk_cid, index: int, size: int, blocks):
    from private.pb import ipcnodeapi_pb2

    cids = []
    sizes = []

    if hasattr(chunk_cid, "__str__"):
        chunk_cid_str = str(chunk_cid)
    else:
        chunk_cid_str = chunk_cid

    pb_blocks = []
    for block in blocks:
        block_cid = block.cid if hasattr(block, "cid") else block["cid"]
        block_data = block.data if hasattr(block, "data") else block["data"]

        if hasattr(block_cid, "__str__"):
            block_cid_str = str(block_cid)
        else:
            block_cid_str = block_cid

        pb_block = ipcnodeapi_pb2.IPCChunk.Block(cid=block_cid_str, size=len(block_data))
        pb_blocks.append(pb_block)

        try:
            try:
                if hasattr(block_cid, "__bytes__"):
                    cid_bytes = bytes(block_cid)
                else:
                    c = CID.decode(block_cid_str)
                    cid_bytes = bytes(c)
            except Exception as e:
                raise SDKError(f"failed to get CID bytes for block {block_cid_str}: {str(e)}")

            bcid = bytearray(32)

            if isinstance(cid_bytes, str):
                cid_bytes = cid_bytes.encode()
            elif not isinstance(cid_bytes, (bytes, bytearray)):
                cid_bytes = bytes(cid_bytes)

            if len(cid_bytes) > 4:
                copy_len = min(len(cid_bytes) - 4, 32)
                bcid[:copy_len] = cid_bytes[4 : 4 + copy_len]
            else:
                copy_len = min(len(cid_bytes), 32)
                bcid[:copy_len] = cid_bytes[:copy_len]

            cids.append(bytes(bcid))
            sizes.append(len(block_data))
        except Exception as e:
            return None, None, None, SDKError(f"failed to process CID: {str(e)}")

    proto_chunk = ipcnodeapi_pb2.IPCChunk(cid=chunk_cid_str, index=index, size=size, blocks=pb_blocks)

    return cids, sizes, proto_chunk, None


class IPC:
    def __init__(self, client, conn, ipc_instance, config: SDKConfig, http_client=None, batch_size=1, with_retry=None):
        self.client = client
        self.conn = conn
        self.ipc = ipc_instance
        self.max_concurrency = config.max_concurrency
        self.block_part_size = config.block_part_size
        self.use_connection_pool = config.use_connection_pool
        self.encryption_key = config.encryption_key if config.encryption_key else b""
        self.max_blocks_in_chunk = config.streaming_max_blocks_in_chunk
        self.chunk_buffer = config.chunk_buffer
        self.http_client = http_client
        self.batch_size = batch_size

        from private.retry.retry import WithRetry

        self.with_retry = with_retry if with_retry is not None else WithRetry(max_attempts=5, base_delay=0.1)

    def create_bucket(self, ctx, name: str) -> IPCBucketCreateResult:
        if len(name) < MIN_BUCKET_NAME_LENGTH:
            raise SDKError("invalid bucket name")

        try:
            tx = self.ipc.storage.create_bucket(
                bucket_name=name,
                from_address=self.ipc.auth.address,
                private_key=self.ipc.auth.key,
                gas_limit=500000,
                nonce_manager=None,
            )
            receipt = self.ipc.eth.eth.wait_for_transaction_receipt(tx)

            if receipt.status != 1:
                raise SDKError("bucket creation transaction failed")

            block = self.ipc.eth.eth.get_block(receipt.blockNumber)
            created_at = block.timestamp

            bucket_id = receipt.transactionHash.hex() if receipt.transactionHash else "unknown"

            return IPCBucketCreateResult(id=bucket_id, name=name, created_at=created_at)

        except Exception as e:
            logging.error(f"IPC create_bucket failed: {e}")
            raise SDKError(f"bucket creation failed: {e}")

    def view_bucket(self, ctx, bucket_name: str) -> Optional[IPCBucket]:
        if not bucket_name:
            raise SDKError("empty bucket name")
        try:
            request = ipcnodeapi_pb2.IPCBucketViewRequest(name=bucket_name, address=self.ipc.auth.address.lower())
            response = self.client.BucketView(request)

            if not response:
                return None
            created_at = 0
            if hasattr(response, "created_at") and response.created_at:
                created_at = int(response.created_at.seconds)

            return IPCBucket(
                id=response.id if hasattr(response, "id") else "",
                name=response.name if hasattr(response, "name") else bucket_name,
                created_at=created_at,
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                logging.info(f"Bucket '{bucket_name}' not found")
                return None
            error_details = str(e.details()).lower() if e.details() else ""
            if "not found" in error_details:
                logging.info(f"Bucket '{bucket_name}' not found")
                return None
            logging.error(f"IPC view_bucket gRPC failed: {e.code()} - {e.details()}")
            raise SDKError(f"failed to view bucket: {e.details()}")
        except Exception as err:
            error_str = str(err).lower()
            if "not found" in error_str:
                logging.info(f"Bucket '{bucket_name}' not found")
                return None
            logging.error(f"IPC view_bucket unexpected error: {err}")
            raise SDKError(f"failed to get bucket: {err}")

    def list_buckets(self, ctx, offset: int = 0, limit: int = 0) -> list[IPCBucket]:
        try:
            actual_limit = limit if limit > 0 else 10000

            request = ipcnodeapi_pb2.IPCBucketListRequest(
                address=self.ipc.auth.address, offset=offset, limit=actual_limit
            )
            logging.info(
                f"Sending BucketList request - address: {self.ipc.auth.address}, offset: {offset}, limit: {actual_limit} (user limit: {limit})"
            )
            response = self.client.BucketList(request)

            buckets = []
            if response and hasattr(response, "buckets"):
                logging.info(f"Received BucketList response with {len(response.buckets)} buckets")
                for bucket in response.buckets:
                    created_at = 0
                    if hasattr(bucket, "created_at") and bucket.created_at:
                        created_at = int(bucket.created_at.seconds)

                    bucket_name = bucket.name if hasattr(bucket, "name") else ""
                    buckets.append(IPCBucket(name=bucket_name, created_at=created_at, id=""))
            else:
                logging.info("BucketList response is empty or has no 'buckets' field")

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
            request = ipcnodeapi_pb2.IPCBucketViewRequest(name=name, address=self.ipc.auth.address.lower())
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
            bucket_id_hex = response.id if hasattr(response, "id") and response.id else None
            if not bucket_id_hex:
                logging.error(f"No bucket ID returned from IPC for bucket '{name}'")
                raise SDKError(f"bucket ID not available from IPC response")

            logging.info(f"Got bucket ID from IPC: {bucket_id_hex}")

            try:
                tx_hash = self.ipc.storage.delete_bucket(
                    bucket_name=name,
                    from_address=self.ipc.auth.address,
                    private_key=self.ipc.auth.key,
                    bucket_id_hex=bucket_id_hex,  # bucket ID from IPC response
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
                file_name=file_name, bucket_name=bucket_name, address=self.ipc.auth.address.lower()
            )
            response = self.client.FileView(request)

            if not response:
                logging.info(f"File '{file_name}' in bucket '{bucket_name}' not found.")
                return None

            created_at = 0
            if hasattr(response, "created_at") and response.created_at:
                created_at = int(response.created_at.seconds)

            return IPCFileMeta(
                root_cid=response.root_cid if hasattr(response, "root_cid") else "",
                name=response.file_name if hasattr(response, "file_name") else file_name,
                bucket_name=response.bucket_name if hasattr(response, "bucket_name") else bucket_name,
                encoded_size=response.encoded_size if hasattr(response, "encoded_size") else 0,
                actual_size=response.actual_size if hasattr(response, "actual_size") else 0,
                is_public=response.is_public if hasattr(response, "is_public") else False,
                created_at=created_at,
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
                address=self.ipc.auth.address.lower(),
                offset=0,
                limit=1000,  # Default limit to get up to 1000 files
            )
            response = self.client.FileList(request)

            files = []
            if response and hasattr(response, "list"):
                logging.info(f"Received FileList response with {len(response.list)} files")
                for file_item in response.list:
                    created_at = 0
                    if hasattr(file_item, "created_at") and file_item.created_at:
                        created_at = int(file_item.created_at.seconds)

                    file_name = file_item.name if hasattr(file_item, "name") else ""
                    root_cid = file_item.root_cid if hasattr(file_item, "root_cid") else ""
                    encoded_size = file_item.encoded_size if hasattr(file_item, "encoded_size") else 0
                    actual_size = file_item.actual_size if hasattr(file_item, "actual_size") else 0

                    logging.info(
                        f"Processing file: name={file_name}, root_cid={root_cid}, encoded_size={encoded_size}, actual_size={actual_size}, created_at={created_at}"
                    )

                    files.append(
                        IPCFileListItem(
                            name=file_name,
                            root_cid=root_cid,
                            encoded_size=encoded_size,
                            actual_size=actual_size,
                            created_at=created_at,
                        )
                    )
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
        """Delete a file by bucket name and file name.

        This implementation matches the Go SDK's IPC FileDelete method.
        """
        if not bucket_name.strip() or not file_name.strip():
            raise SDKError(f"empty bucket or file name. Bucket: '{bucket_name}', File: '{file_name}'")

        try:
            # Note: Metadata encryption would be applied here if implemented
            # For now, using the names as-is like in the current implementation
            encrypted_file_name = file_name
            encrypted_bucket_name = bucket_name

            # Step 1: Get bucket by name from storage contract
            # Go SDK: bucket, err := sdk.ipc.Storage.GetBucketByName(...)
            bucket = self.ipc.storage.get_bucket_by_name(
                {"from": self.ipc.auth.address},
                encrypted_bucket_name,
                self.ipc.auth.address,
                0,  # file_offset - no need to fetch file ids
                0,  # file_limit - no need to fetch file ids
            )
            if not bucket:
                raise SDKError(f"bucket '{bucket_name}' not found")

            bucket_id = bucket[0]  # bytes32 bucket ID
            bucket_name_from_contract = bucket[1]  # Bucket name from contract

            # Step 2: Get file by name from storage contract
            # Go SDK: file, err := sdk.ipc.Storage.GetFileByName(&bind.CallOpts{Context: ctx}, bucket.Id, fileName)
            file_info = self.ipc.storage.get_file_by_name(
                {"from": self.ipc.auth.address}, bucket_id, encrypted_file_name
            )
            if not file_info:
                raise SDKError(f"failed to retrieve file - file does not exist")

            file_id = file_info[0]
            file_index = None
            try:
                file_index = self.ipc.storage.get_file_index_by_id(
                    {"from": self.ipc.auth.address}, encrypted_bucket_name, file_id
                )
                logging.info(f"Got file index from contract: {file_index}")
            except Exception as index_err:
                logging.warning(f"Failed to get file index from contract: {index_err}")
                logging.info("Falling back to manual index calculation via file listing...")

                try:
                    files = self.list_files(ctx, bucket_name)
                    for i, f in enumerate(files):
                        if f.name == encrypted_file_name:
                            file_index = i
                            logging.info(f"Calculated manual file index: {file_index} (position in list)")
                            break

                    if file_index is None:
                        raise SDKError(f"file '{file_name}' not found in bucket file list")

                except Exception as list_err:
                    logging.error(f"Failed to calculate manual index: {list_err}")
                    raise SDKError(
                        f"failed to determine file index: contract method failed and manual calculation failed: {index_err}"
                    )

            # Validate that we got a valid index
            if file_index is None or (isinstance(file_index, int) and file_index < 0):
                raise SDKError(f"invalid file index: {file_index}")

            # Step 4: Delete file via storage contract transaction
            # Go SDK: tx, err := sdk.ipc.Storage.DeleteFile(sdk.ipc.Auth, file.Id, bucket.Id, fileName, fileIdx.Index)
            logging.info(
                f"Deleting file with file_id: {file_id.hex() if isinstance(file_id, bytes) else file_id}, bucket_id: {bucket_id.hex() if isinstance(bucket_id, bytes) else bucket_id}, name: {encrypted_file_name}, index: {file_index}"
            )

            tx_hash = self.ipc.storage.delete_file(self.ipc.auth, file_id, bucket_id, encrypted_file_name, file_index)

            # Go SDK waits for transaction: return errSDK.Wrap(sdk.ipc.WaitForTx(ctx, tx.Hash()))
            # Python SDK doesn't have WaitForTx implemented yet, so we just return
            logging.info(
                f"IPC file_delete transaction successful for '{file_name}' in bucket '{bucket_name}', tx_hash: {tx_hash}"
            )
            return None

        except SDKError:
            # Re-raise SDKError as-is
            raise
        except Exception as err:
            logging.error(f"IPC file_delete failed: {err}")
            raise SDKError(f"failed to delete file: {err}")

    def create_file_upload(self, ctx, bucket_name: str, file_name: str) -> IPCFileUpload:
        if not bucket_name:
            raise SDKError("empty bucket name")

        if not file_name:
            raise SDKError("empty file name")

        try:
            encrypted_file_name = maybe_encrypt_metadata(file_name, bucket_name + "/" + file_name, self.encryption_key)
            encrypted_bucket_name = maybe_encrypt_metadata(bucket_name, file_name, self.encryption_key)

            file_upload = new_ipc_file_upload(bucket_name, file_name)

            bucket_info = self.view_bucket(None, encrypted_bucket_name)
            if not bucket_info:
                raise SDKError(f"bucket '{bucket_name}' not found")

            bucket_id_hex = bucket_info.id
            if bucket_id_hex.startswith("0x"):
                bucket_id_hex = bucket_id_hex[2:]
            bucket_id = bytes.fromhex(bucket_id_hex)

            tx_hash = None
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    tx_hash = self.ipc.storage.create_file(
                        self.ipc.auth.address, self.ipc.auth.key, bucket_id, encrypted_file_name, nonce_manager=None
                    )
                    break
                except Exception as e:
                    error_msg = str(e).lower()

                    if "0x6891dde0" in error_msg or "filealreadyexists" in error_msg:
                        logging.info(f"File '{file_name}' already exists in bucket '{bucket_name}'")
                        raise SDKError(f"file already exists")

                    if "transaction reverted" in error_msg:
                        from private.ipc.errors import error_hash_to_error

                        parsed_error = error_hash_to_error(e)
                        error_name = str(parsed_error)
                        if error_name != str(e):
                            logging.error(f"Transaction reverted: {error_name}")
                            raise SDKError(f"upload failed: {error_name}")

                    is_retryable = any(
                        retry_phrase in error_msg
                        for retry_phrase in ["nonce too low", "replacement transaction underpriced", "eof"]
                    )
                    if is_retryable and attempt < max_retries - 1:
                        time.sleep(0.5 * (2**attempt))  # Exponential backoff
                        continue
                    else:
                        raise SDKError(f"failed to create file transaction: {str(e)}")

            if hasattr(self.ipc, "wait_for_tx") and tx_hash:
                self.ipc.wait_for_tx(tx_hash)
            elif hasattr(self.ipc, "web3") and hasattr(self.ipc.eth.eth, "wait_for_transaction_receipt") and tx_hash:
                receipt = self.ipc.eth.eth.wait_for_transaction_receipt(tx_hash)
                if receipt.status != 1:
                    raise SDKError("CreateFile transaction failed")

            logging.info(
                f"IPC create_file_upload completed successfully for '{file_name}' in bucket '{bucket_name}', tx_hash: {tx_hash}"
            )
            return file_upload

        except Exception as err:
            logging.error(f"IPC create_file_upload failed: {err}")
            raise SDKError(f"failed to create file upload: {err}")

    def upload(self, ctx, bucket_name: str, file_name: str, reader: io.IOBase) -> IPCFileMetaV2:
        try:
            file_upload = self.create_file_upload(ctx, bucket_name, file_name)

            is_continuation = file_upload.state.chunk_count > 0

            encrypted_file_name = maybe_encrypt_metadata(
                file_upload.name, file_upload.bucket_name + "/" + file_upload.name, self.encryption_key
            )
            encrypted_bucket_name = maybe_encrypt_metadata(
                file_upload.bucket_name, file_upload.name, self.encryption_key
            )

            bucket = None

            def get_bucket_call():
                nonlocal bucket
                try:
                    bucket = self.ipc.storage.get_bucket_by_name(
                        call_opts={"from": self.ipc.auth.address},
                        bucket_name=encrypted_bucket_name,
                        owner_address=self.ipc.auth.address,
                        file_offset=0,
                        file_limit=0,
                    )
                    return (False, None)
                except Exception as e:
                    return (True, e)

            retry_err = self.with_retry.do(get_bucket_call)
            if retry_err:
                raise SDKError(f"failed to get bucket: {str(retry_err)}")

            if not bucket or len(bucket) == 0 or not bucket[0]:
                raise SDKError(f"bucket '{file_upload.bucket_name}' not found")

            bucket_id = bucket[0]

            chunk_enc_overhead = 0
            file_enc_key = encryption_key(self.encryption_key, encrypted_bucket_name, encrypted_file_name)
            if len(file_enc_key) > 0:
                chunk_enc_overhead = EncryptionOverhead

            buffer_size = self.max_blocks_in_chunk * int(BlockSize)
            buffer_size -= chunk_enc_overhead

            if is_continuation:
                from .sdk import skip_to_position

                skip_to_position(reader, file_upload.state.actual_file_size)

            return self._upload_with_comprehensive_debug(
                ctx,
                file_upload,
                reader,
                bucket_id,
                encrypted_file_name,
                encrypted_bucket_name,
                file_enc_key,
                buffer_size,
            )

        except Exception as err:
            logging.error(f"IPC upload failed: {err}")
            raise SDKError(f"upload failed: {str(err)}")

    def _upload_with_comprehensive_debug(
        self,
        ctx,
        file_upload: IPCFileUpload,
        reader: io.IOBase,
        bucket_id: bytes,
        encrypted_file_name: str,
        encrypted_bucket_name: str,
        file_enc_key: bytes,
        buffer_size: int,
    ) -> IPCFileMetaV2:
        pool = ConnectionPool()
        try:
            chunk_index = file_upload.state.chunk_count
            total_chunks_uploaded = 0

            while True:
                chunk_data = bytearray(buffer_size)
                bytes_read = 0

                while bytes_read < buffer_size:
                    try:
                        n = reader.readinto(memoryview(chunk_data)[bytes_read:])
                        if n == 0 or n is None:
                            break
                        bytes_read += n
                    except Exception as e:
                        if bytes_read == 0:
                            raise SDKError(f"failed to read from reader: {str(e)}")
                        break

                if bytes_read == 0:
                    if chunk_index == 0:
                        raise SDKError("empty file")
                    break

                chunk_upload = self.create_chunk_upload(
                    ctx, chunk_index, file_enc_key, chunk_data[:bytes_read], bucket_id, encrypted_file_name
                )

                cids, sizes, proto_chunk, error = to_ipc_proto_chunk(
                    chunk_upload.chunk_cid, chunk_upload.index, chunk_upload.actual_size, chunk_upload.blocks
                )
                if error:
                    raise error

                tx_hash = None
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        tx_hash = self.ipc.storage.add_file_chunk(
                            self.ipc.auth.address,
                            self.ipc.auth.key,
                            self._convert_cid_to_bytes(chunk_upload.chunk_cid),
                            bucket_id,
                            encrypted_file_name,
                            chunk_upload.encoded_size,
                            cids,
                            sizes,
                            chunk_upload.index,
                            nonce_manager=None,
                        )
                        break
                    except Exception as e:
                        error_msg = str(e).lower()
                        is_retryable = any(
                            retry_phrase in error_msg
                            for retry_phrase in ["nonce too low", "replacement transaction underpriced", "eof"]
                        )
                        if is_retryable and attempt < max_retries - 1:
                            time.sleep(0.5 * (2**attempt))
                            continue
                        else:
                            raise SDKError(f"failed to add file chunk: {str(e)}")

                if hasattr(self.ipc, "wait_for_tx") and tx_hash:
                    self.ipc.wait_for_tx(tx_hash)
                elif hasattr(self.ipc, "web3") and tx_hash:
                    receipt = self.ipc.eth.eth.wait_for_transaction_receipt(tx_hash)
                    if receipt.status != 1:
                        raise SDKError(f"AddFileChunk transaction failed: {tx_hash}")

                file_upload.state.pre_create_chunk(chunk_upload, tx_hash)

                self.upload_chunk(ctx, chunk_upload, pool)

                file_upload.state.chunk_uploaded(chunk_upload)
                total_chunks_uploaded += 1
                chunk_index += 1

                time.sleep(0.1)

            root_cid = file_upload.state.dag_root.build()
            file_meta = self.ipc.storage.get_file_by_name(
                {"from": self.ipc.auth.address}, bucket_id, encrypted_file_name
            )
            file_id = self._calculate_file_id(bucket_id, encrypted_file_name)

            is_filled = False
            max_wait_time = 60
            wait_start = time.time()

            while not is_filled:
                try:
                    is_filled = self.ipc.storage.is_file_filled(file_id)
                    if is_filled:
                        break
                except Exception as e:
                    logging.warning(f"IsFileFilled check failed: {e}")
                    time.sleep(2)
                    break

                if time.time() - wait_start > max_wait_time:
                    raise SDKError("timeout waiting for file to be filled")

                time.sleep(1)

            root_cid_bytes = self._convert_cid_to_bytes(root_cid)
            bucket_info = self.view_bucket(None, encrypted_bucket_name)
            if not bucket_info:
                raise SDKError(f"bucket '{file_upload.bucket_name}' not found during commit")

            bucket_id_bytes = bytes.fromhex(bucket_info.id) if isinstance(bucket_info.id, str) else bucket_info.id

            tx_hash = self.ipc.storage.commit_file(
                bucket_id_bytes,
                encrypted_file_name,
                file_upload.state.encoded_file_size,
                file_upload.state.actual_file_size,
                root_cid_bytes,
                self.ipc.auth.address,
                self.ipc.auth.key,
            )

            if hasattr(self.ipc, "wait_for_tx") and tx_hash:
                self.ipc.wait_for_tx(tx_hash)
            elif hasattr(self.ipc, "web3") and tx_hash:
                receipt = self.ipc.eth.eth.wait_for_transaction_receipt(tx_hash)
                if receipt.status != 1:
                    raise SDKError("CommitFile transaction failed")

            file_upload.state.is_committed = True

            return IPCFileMetaV2(
                root_cid=str(root_cid),
                bucket_name=encrypted_bucket_name,
                name=encrypted_file_name,
                encoded_size=file_upload.state.encoded_file_size,
                size=file_upload.state.actual_file_size,
                created_at=time.time() if file_meta else None,
                committed_at=time.time(),
            )

        except Exception as err:
            raise SDKError(f"upload failed: {str(err)}")
        finally:
            try:
                pool.close()
            except Exception as e:
                logging.warning(f"Error closing connection pool: {str(e)}")

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
        if hasattr(cid_input, "__bytes__"):
            try:
                return bytes(cid_input)
            except Exception as e:
                logging.warning(f"Failed to call __bytes__ on CID object: {e}")

        # Convert to string if needed
        if not isinstance(cid_input, str):
            cid_str = str(cid_input)
        else:
            cid_str = cid_input

        try:
            from multiformats import CID as CIDLib

            cid_obj = CIDLib.decode(cid_str)
            return bytes(cid_obj)

        except Exception as e:
            logging.error(f"Failed to decode CID using multiformats library: {e}")
            raise SDKError(f"Failed to convert CID to binary format: {e}")

    def create_chunk_upload(
        self, ctx, index: int, file_encryption_key: bytes, data: bytes, bucket_id: bytes, file_name: str
    ) -> IPCFileChunkUploadV2:
        try:
            if len(file_encryption_key) > 0:
                data = encrypt(file_encryption_key, data, str(index).encode())

            size = len(data)

            block_size = BlockSize

            chunk_dag = build_dag(ctx, io.BytesIO(data), block_size)
            if chunk_dag is None:
                raise SDKError("build_dag returned None")

            chunk_cid_str = chunk_dag.cid if isinstance(chunk_dag.cid, str) else str(chunk_dag.cid)

            cids, sizes, proto_chunk, error = to_ipc_proto_chunk(chunk_cid_str, index, size, chunk_dag.blocks)

            if error is not None:
                raise error
            if proto_chunk is None:
                raise SDKError("to_ipc_proto_chunk returned None proto_chunk")

            request = ipcnodeapi_pb2.IPCFileUploadChunkCreateRequest(
                chunk=proto_chunk, bucket_id=bucket_id, file_name=file_name  # bytes
            )

            response = self.client.FileUploadChunkCreate(request)
            if response is None:
                raise SDKError("gRPC response is None")

            if len(response.blocks) != len(chunk_dag.blocks):
                raise SDKError(
                    f"received unexpected amount of blocks {len(response.blocks)}, expected {len(chunk_dag.blocks)}"
                )

            for i, upload in enumerate(response.blocks):
                if chunk_dag.blocks[i].cid != upload.cid:
                    raise SDKError(f"block CID mismatch at position {i}")
                chunk_dag.blocks[i].node_address = upload.node_address
                chunk_dag.blocks[i].node_id = upload.node_id
                chunk_dag.blocks[i].permit = upload.permit

            return IPCFileChunkUploadV2(
                index=index,
                chunk_cid=chunk_dag.cid,
                actual_size=size,
                raw_data_size=chunk_dag.raw_data_size,
                encoded_size=chunk_dag.encoded_size,  # proto_node_size maps to encoded_size
                blocks=chunk_dag.blocks,
                bucket_id=bucket_id,
                file_name=file_name,
            )
        except Exception as err:
            raise SDKError(f"failed to create chunk upload: {str(err)}")

    def upload_chunk(self, ctx, file_chunk_upload: IPCFileChunkUploadV2, pool: ConnectionPool) -> None:
        try:
            chunk_cid_str = file_chunk_upload.chunk_cid
            if hasattr(file_chunk_upload.chunk_cid, "string"):
                chunk_cid_str = file_chunk_upload.chunk_cid.string()
            elif hasattr(file_chunk_upload.chunk_cid, "toString"):
                chunk_cid_str = file_chunk_upload.chunk_cid.toString()
            elif not isinstance(file_chunk_upload.chunk_cid, str):
                chunk_cid_str = str(file_chunk_upload.chunk_cid)

            cids, sizes, proto_chunk, err = to_ipc_proto_chunk(
                chunk_cid_str, file_chunk_upload.index, file_chunk_upload.actual_size, file_chunk_upload.blocks
            )
            if err:
                raise err

            # Upload all blocks in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                futures = {}

                for i, block in enumerate(file_chunk_upload.blocks):
                    future = executor.submit(
                        self._upload_block,
                        ctx,
                        pool,
                        i,
                        block,
                        proto_chunk,
                        file_chunk_upload.bucket_id,
                        file_chunk_upload.file_name,
                    )
                    futures[future] = i

                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        for f in futures:
                            f.cancel()
                        raise e

        except Exception as err:
            raise SDKError(f"failed to upload chunk: {str(err)}")

    def _upload_block(
        self, ctx, pool: ConnectionPool, block_index: int, block, proto_chunk, bucket_id, file_name: str
    ) -> None:
        try:
            node_address = block.node_address if hasattr(block, "node_address") else block["node_address"]
            block_data_bytes = block.data if hasattr(block, "data") else block["data"]
            block_cid = block.cid if hasattr(block, "cid") else block["cid"]
            node_id = block.node_id if hasattr(block, "node_id") else block["node_id"]

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
                    import random

                    nonce_bytes = random.randbytes(32)
                    nonce = int.from_bytes(nonce_bytes, byteorder="big")
                    deadline = int(time.time() + 24 * 60 * 60)

                    try:
                        import base58

                        if node_id.startswith("12D3"):
                            full_node_id = base58.b58decode(node_id)
                        else:
                            full_node_id = node_id.encode() if isinstance(node_id, str) else node_id
                    except ImportError:
                        full_node_id = node_id.encode() if isinstance(node_id, str) else node_id

                    signature_hex, _ = self._create_storage_signature(
                        proto_chunk.cid, block_cid, proto_chunk.index, block_index, node_id, nonce, deadline, bucket_id
                    )

                    data_len = len(block_data_bytes)
                    if data_len == 0:
                        return

                    def message_generator():
                        i = 0
                        is_first_part = True

                        while i < data_len:
                            end = min(i + self.block_part_size, data_len)
                            segment_data = block_data_bytes[i:end]

                            if isinstance(segment_data, memoryview):
                                segment_data = bytes(segment_data)

                            block_data = ipcnodeapi_pb2.IPCFileBlockData(
                                data=segment_data,
                                cid=block_cid if is_first_part else "",
                                index=block_index,
                                chunk=proto_chunk if is_first_part else None,
                                bucket_id=bucket_id,
                                file_name=file_name,
                                signature=signature_hex,
                                nonce=nonce_bytes,
                                node_id=full_node_id,
                                deadline=deadline,
                            )

                            yield block_data
                            i = end
                            is_first_part = False

                    try:
                        response = client.FileUploadBlock(message_generator())
                        return response
                    except Exception as e:
                        if "BlockAlreadyFilled" in str(e):
                            return None
                        raise SDKError(f"streaming upload failed: {str(e)}")

                upload_streaming()

            finally:
                if closer:
                    try:
                        closer()
                    except Exception as e:
                        logging.warning(f"Failed to close connection for block {block_cid}: {e}")

        except Exception as err:
            block_cid = block.cid if hasattr(block, "cid") else block["cid"]
            raise SDKError(f"failed to upload block {block_cid}: {str(err)}")

    def _create_storage_signature(
        self,
        chunk_cid: str,
        block_cid: str,
        chunk_index: int,
        block_index: int,
        node_id: str,
        nonce: int,
        deadline: int,
        bucket_id: bytes,
    ) -> tuple:
        try:
            from private.eip712 import Domain, TypedData, sign

            nonce_bytes = nonce.to_bytes(32, byteorder="big")
            chunk_cid_obj = CID.decode(chunk_cid)
            block_cid_obj = CID.decode(block_cid)

            chunk_cid_bytes = self._convert_cid_to_bytes(chunk_cid_obj)
            block_cid_bytes = self._convert_cid_to_bytes(block_cid_obj)

            bcid = bytearray(32)
            if len(block_cid_bytes) > 4:
                copy_len = min(len(block_cid_bytes) - 4, 32)
                bcid[:copy_len] = block_cid_bytes[4 : 4 + copy_len]
            else:
                copy_len = min(len(block_cid_bytes), 32)
                bcid[:copy_len] = block_cid_bytes[:copy_len]

            try:
                import base58

                if node_id.startswith("12D3"):
                    decoded = base58.b58decode(node_id)
                    node_id_bytes = decoded
                else:
                    node_id_bytes = node_id.encode() if isinstance(node_id, str) else node_id
            except ImportError:
                node_id_bytes = node_id.encode() if isinstance(node_id, str) else node_id

            chain_id = self.ipc.storage.get_chain_id()
            contract_address = self.ipc.storage.contract_address

            domain = Domain(name="Storage", version="1", chain_id=chain_id, verifying_contract=contract_address)

            data_types = {
                "StorageData": [
                    TypedData("chunkCID", "bytes"),
                    TypedData("blockCID", "bytes32"),
                    TypedData("chunkIndex", "uint256"),
                    TypedData("blockIndex", "uint8"),
                    TypedData("nodeId", "bytes32"),
                    TypedData("nonce", "uint256"),
                    TypedData("deadline", "uint256"),
                    TypedData("bucketId", "bytes32"),
                ]
            }

            from eth_utils import to_int

            node_id_32 = bytearray(32)
            if len(node_id_bytes) > 6:
                copy_len = min(len(node_id_bytes) - 6, 32)
                node_id_32[:copy_len] = node_id_bytes[6 : 6 + copy_len]

            data_message = {
                "chunkCID": bytes(chunk_cid_bytes),
                "blockCID": bytes(bcid),
                "chunkIndex": to_int(chunk_index),
                "blockIndex": int(block_index),
                "nodeId": bytes(node_id_32),
                "nonce": to_int(nonce),
                "deadline": to_int(deadline),
                "bucketId": (
                    bytes(bucket_id) if len(bucket_id) == 32 else bytes(bucket_id) + b"\x00" * (32 - len(bucket_id))
                ),
            }

            if isinstance(self.ipc.auth.key, bytes):
                private_key_bytes = self.ipc.auth.key
            else:
                key_str = str(self.ipc.auth.key).replace("0x", "")
                private_key_bytes = bytes.fromhex(key_str)

            signature_bytes = sign(private_key_bytes, domain, "StorageData", data_types, data_message)
            signature_hex = signature_bytes.hex()

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
        block,
    ) -> bytes:
        try:
            if not hasattr(block, "node_address") or not block.node_address:
                raise SDKError("missing block metadata")

            client, closer, err = pool.create_ipc_client(block.node_address, self.use_connection_pool)
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
                    address=address,
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
                bucket_name=bucket_name, file_name=file_name, address=self.ipc.auth.address
            )

            response = self.client.FileDownloadCreate(request)

            chunks = []
            for i, chunk in enumerate(response.chunks):
                chunks.append(Chunk(cid=chunk.cid, encoded_size=chunk.encoded_size, size=chunk.size, index=i))

            return IPCFileDownload(bucket_name=response.bucket_name, name=file_name, chunks=chunks)
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
                end_index=end,
            )

            response = self.client.FileDownloadRangeCreate(request)

            chunks = []
            for i, chunk in enumerate(response.chunks):
                chunks.append(Chunk(cid=chunk.cid, encoded_size=chunk.encoded_size, size=chunk.size, index=i + start))

            return IPCFileDownload(bucket_name=response.bucket_name, name=file_name, chunks=chunks)
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

            bucket_info = self.view_bucket(None, bucket_name)
            if not bucket_info:
                raise SDKError(f"bucket '{bucket_name}' not found")

            bucket_id_hex = bucket_info.id
            if bucket_id_hex.startswith("0x"):
                bucket_id_hex = bucket_id_hex[2:]
            bucket_id = bytes.fromhex(bucket_id_hex)

            try:
                file = self.ipc.storage.get_file_by_name({}, bucket_id, file_name)  # bucket ID
                if not file:
                    raise SDKError("failed to retrieve file")
            except Exception as e:
                raise SDKError(f"failed to get file: {str(e)}")

            if not hasattr(self.ipc, "access_manager") or self.ipc.access_manager is None:
                raise SDKError("access manager not available")

            try:
                tx_hash = self.ipc.access_manager.change_public_access(
                    self.ipc.auth, file[0], is_public  # auth object  # file ID  # is_public flag
                )

                if hasattr(self.ipc, "wait_for_tx"):
                    self.ipc.wait_for_tx(tx_hash)
                elif hasattr(self.ipc, "web3") and hasattr(self.ipc.eth.eth, "wait_for_transaction_receipt"):
                    receipt = self.ipc.eth.eth.wait_for_transaction_receipt(tx_hash)
                    if receipt.status != 1:
                        raise SDKError("ChangePublicAccess transaction failed")

                logging.info(
                    f"Successfully {'enabled' if is_public else 'disabled'} public access for file '{original_file_name}' in bucket '{original_bucket_name}', tx_hash: {tx_hash}"
                )
                return None

            except Exception as e:
                raise SDKError(f"failed to change public access: {str(e)}")

        except Exception as err:
            logging.error(f"IPC file_set_public_access failed: {err}")
            raise SDKError(f"failed to set public access: {str(err)}")

    def download(self, ctx, file_download, writer: io.IOBase):
        pool = ConnectionPool()
        try:
            file_enc_key = encryption_key(self.encryption_key, file_download.bucket_name, file_download.name)

            for chunk in file_download.chunks:
                if hasattr(ctx, "done") and ctx.done():
                    raise SDKError("context cancelled")

                chunk_download = self.create_chunk_download(ctx, file_download.bucket_name, file_download.name, chunk)

                self.download_chunk_blocks(
                    ctx,
                    pool,
                    file_download.bucket_name,
                    file_download.name,
                    self.ipc.auth.address,
                    chunk_download,
                    file_enc_key,
                    writer,
                )

            return None
        except Exception as err:
            raise SDKError(f"failed to download file: {str(err)}")
        finally:
            try:
                pool.close()
            except Exception as e:
                logging.warning(f"Error closing connection pool: {str(e)}")

    def create_chunk_download(self, ctx, bucket_name: str, file_name: str, chunk):
        try:
            request = ipcnodeapi_pb2.IPCFileDownloadChunkCreateRequest(
                bucket_name=bucket_name, file_name=file_name, chunk_cid=chunk.cid, address=self.ipc.auth.address
            )

            response = self.client.FileDownloadChunkCreate(request)

            blocks = []
            for block in response.blocks:
                blocks.append(
                    FileBlockDownload(
                        cid=block.cid,
                        data=b"",
                        permit=block.permit,
                        node_address=block.node_address,
                        node_id=block.node_id,
                    )
                )

            return FileChunkDownload(
                cid=chunk.cid, index=chunk.index, encoded_size=chunk.encoded_size, size=chunk.size, blocks=blocks
            )
        except Exception as err:
            raise SDKError(f"failed to create chunk download: {str(err)}")

    def download_chunk_blocks(
        self,
        ctx,
        pool: ConnectionPool,
        bucket_name: str,
        file_name: str,
        address: str,
        chunk_download,
        file_encryption_key: bytes,
        writer: io.IOBase,
    ):
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                futures = {}

                for i, block in enumerate(chunk_download.blocks):
                    futures[
                        executor.submit(
                            self.fetch_block_data,
                            ctx,
                            pool,
                            chunk_download.cid,
                            bucket_name,
                            file_name,
                            address,
                            chunk_download.index,
                            i,
                            block,
                        )
                    ] = i

                blocks = [None] * len(chunk_download.blocks)
                for future in concurrent.futures.as_completed(futures):
                    index = futures[future]
                    try:
                        data = future.result()
                        from .dag import extract_block_data

                        blocks[index] = extract_block_data(chunk_download.blocks[index].cid, data)
                    except Exception as e:
                        raise SDKError(f"failed to download block: {str(e)}")

            data = b"".join([b for b in blocks if b is not None])

            if file_encryption_key:
                from private.encryption import decrypt

                data = decrypt(file_encryption_key, data, str(chunk_download.index).encode())

            writer.write(data)

            return None
        except Exception as err:
            raise SDKError(f"failed to download chunk blocks: {str(err)}")
