import grpc
import ipfshttpclient
from google.protobuf.timestamp_pb2 import Timestamp
import logging
from private.memory.memory import Size
from private.pb import nodeapi_pb2_grpc, ipcnodeapi_pb2_grpc
from private.ipc.client import Client
from private.spclient.spclient import SPClient
from private.encryption import derive_key
from typing import List, Optional, Union, Any
from multiformats import cid


BLOCK_SIZE = 1 * Size.MB
ENCRYPTION_OVERHEAD = 28  # 16 bytes for AES-GCM tag, 12 bytes for nonce
MIN_BUCKET_NAME_LENGTH = 3
MIN_FILE_SIZE = 127  # 127 bytes

from .sdk_ipc import IPC
from .sdk_streaming import StreamingAPI
from .erasure_code import ErasureCode

class SDKError(Exception):
    pass

class SDK:
    def __init__(self, address: str, max_concurrency: int, block_part_size: int, use_connection_pool: bool,
                 encryption_key: Optional[bytes] = None, private_key: Optional[str] = None,
                 streaming_max_blocks_in_chunk: int = 32, parity_blocks_count: int = 0) -> None:
        self.client: Optional[nodeapi_pb2_grpc.NodeAPIStub] = None
        self.conn: Optional[grpc.Channel] = None
        self.sp_client: Optional[SPClient] = None
        self.streaming_erasure_code: Optional[ErasureCode] = None

        # Initializing variables
        self.max_concurrency = max_concurrency
        self.block_part_size = block_part_size
        self.use_connection_pool = use_connection_pool
        self.private_key = private_key
        self.encryption_key = encryption_key or []
        self.streaming_max_blocks_in_chunk = streaming_max_blocks_in_chunk
        self.parity_blocks_count = parity_blocks_count

        if self.block_part_size <= 0 or self.block_part_size > BLOCK_SIZE:
            raise SDKError(f"Invalid blockPartSize: {block_part_size}. Valid range is 1-{BLOCK_SIZE}")

        # gRPC client connection
        self.conn = grpc.insecure_channel(address)
        self.client = nodeapi_pb2_grpc.NodeAPIStub(self.conn)

        # Additional configurations (erasure coding, encryption validation)
        if len(self.encryption_key) != 0 and len(self.encryption_key) != 32:
            raise SDKError("Encryption key length should be 32 bytes long")

        if self.parity_blocks_count > self.streaming_max_blocks_in_chunk // 2:
            raise SDKError(f"Parity blocks count {self.parity_blocks_count} should be <= {self.streaming_max_blocks_in_chunk // 2}")

        if self.parity_blocks_count > 0:
            self.streaming_erasure_code = ErasureCode(self.streaming_max_blocks_in_chunk - self.parity_blocks_count, self.parity_blocks_count)

        self.sp_client = SPClient()

    def close(self) -> None:
        if self.conn:
            self.conn.close()

    def streaming_api(self) -> StreamingAPI:
        return StreamingAPI(self.conn, self.client, self.streaming_erasure_code, self.max_concurrency,
                            self.block_part_size, self.use_connection_pool, self.encryption_key,
                            self.streaming_max_blocks_in_chunk)

    def ipc(self) -> IPC:
        client = ipcnodeapi_pb2_grpc(self.conn)
        ipc_instance = Client.dial(self.conn, self.private_key, client)
        return IPC(client, self.conn, self.max_concurrency, self.block_part_size, self.use_connection_pool, self.encryption_key, ipc_instance)

    def create_bucket(self, name: str) -> 'BucketCreateResult':
        if len(name) < MIN_BUCKET_NAME_LENGTH:
            raise SDKError("Invalid bucket name")

        # Call to gRPC API
        response = self.client.bucket_create(nodeapi_pb2_grpc.NodeAPIStub.BucketCreate(name=name))
        return BucketCreateResult(name=response.name, created_at=response.created_at)

    def view_bucket(self, name: str) -> 'Bucket':
        if name == "":
            raise SDKError("Invalid bucket name")

        # Call to gRPC API
        response = self.client.bucket_view(nodeapi_pb2_grpc.NodeAPIStub.BucketView(name=name))
        return Bucket(name=response.name, created_at=response.created_at)

    def delete_bucket(self, name: str) -> None:
        if len(name) < MIN_BUCKET_NAME_LENGTH:
            raise SDKError(f"Invalid bucket name. Must be at least {MIN_BUCKET_NAME_LENGTH} characters long")

        try:
            self.client.bucket_delete(nodeapi_pb2_grpc.NodeAPIStub.BucketDelete(name=name))
        except Exception as e:
            logging.error(f"Error deleting bucket '{name}': {str(e)}")
            raise SDKError(f"Failed to delete bucket: {str(e)}")

    @staticmethod
    def extract_block_data(id_str: str, data: bytes) -> bytes:
        try:
            block_cid = cid.decode(id_str)
        except Exception as e:
            raise ValueError(f"Invalid CID: {e}")

        if block_cid.codec == "dag-pb":
            try:
                dag_node = ipfshttpclient.codec.decode("dag-pb", data)  
                unixfs_data = dag_node["Data"]
                return unixfs_data
            except Exception as e:
                raise ValueError(f"Failed to decode DAG node: {e}")

        elif block_cid.codec == "raw":
            return data

        else:
            raise ValueError(f"Unknown CID type: {block_cid.codec}")


class BucketCreateResult:
    def __init__(self, name: str, created_at: Timestamp) -> None:
        self.name = name
        self.created_at = created_at


class Bucket:
    def __init__(self, name: str, created_at: Timestamp) -> None:
        self.name = name
        self.created_at = created_at


def encryption_key_derivation(parent_key: bytes, *info_data: str) -> Optional[bytes]:
    if len(parent_key) == 0:
        return None

    info = "/".join(info_data)
    key = derive_key(parent_key, info.encode())
    return key
