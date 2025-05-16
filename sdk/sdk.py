import grpc
import ipfshttpclient
from google.protobuf.timestamp_pb2 import Timestamp
import logging
from private.memory.memory import Size
from private.pb import nodeapi_pb2, nodeapi_pb2_grpc, ipcnodeapi_pb2_grpc
from private.ipc.client import Client
from private.spclient.spclient import SPClient
from private.encryption import derive_key
from typing import List, Optional
from multiformats import cid
import warnings

BLOCK_SIZE = 1 * Size.MB
ENCRYPTION_OVERHEAD = 28  # 16 bytes for AES-GCM tag, 12 bytes for nonce
MIN_BUCKET_NAME_LENGTH = 3
MIN_FILE_SIZE = 127  # 127 bytes

from .sdk_ipc import IPC
from .sdk_streaming import StreamingAPI
from .erasure_code import ErasureCode
from .connection import ConnectionPool, ConnectionError

class SDKError(Exception):
    pass

class SDK:
    def __init__(self, address: str, max_concurrency: int, block_part_size: int, use_connection_pool: bool,
                 encryption_key: Optional[bytes] = None, private_key: Optional[str] = None,
                 streaming_max_blocks_in_chunk: int = 32, parity_blocks_count: int = 0,
                 ssl_credentials: Optional[grpc.ChannelCredentials] = None,
                 connection_pool_config: Optional[dict] = None):
        """
        Initialize the SDK with enhanced security and connection management.
        
        Args:
            address: The address of the gRPC server
            max_concurrency: Maximum number of concurrent operations
            block_part_size: Size of block parts
            use_connection_pool: Whether to use connection pooling
            encryption_key: Optional encryption key
            private_key: Optional private key
            streaming_max_blocks_in_chunk: Maximum blocks in one chunk
            parity_blocks_count: Number of parity blocks
            ssl_credentials: Optional SSL credentials for secure communication
            connection_pool_config: Optional configuration for connection pool
        """
        self.client = None
        self.conn = None
        self.sp_client = None
        self.streaming_erasure_code = None
        self._connection_pool = None

        # Validate and set basic parameters
        self.max_concurrency = max_concurrency
        self.block_part_size = block_part_size
        self.use_connection_pool = use_connection_pool
        self.private_key = private_key
        self.encryption_key = encryption_key or []
        self.streaming_max_blocks_in_chunk = streaming_max_blocks_in_chunk
        self.parity_blocks_count = parity_blocks_count

        # Validate block size
        if self.block_part_size <= 0 or self.block_part_size > BLOCK_SIZE:
            raise SDKError(f"Invalid blockPartSize: {block_part_size}. Valid range is 1-{BLOCK_SIZE}")

        # Validate encryption key
        if len(self.encryption_key) != 0 and len(self.encryption_key) != 32:
            raise SDKError("Encryption key length should be 32 bytes long")

        # Validate parity blocks
        if self.parity_blocks_count > self.streaming_max_blocks_in_chunk // 2:
            raise SDKError(f"Parity blocks count {self.parity_blocks_count} should be <= {self.streaming_max_blocks_in_chunk // 2}")

        # Initialize connection pool with configuration
        pool_config = connection_pool_config or {}
        self._connection_pool = ConnectionPool(
            ssl_credentials=ssl_credentials,
            max_retries=pool_config.get('max_retries', 3),
            max_concurrent=pool_config.get('max_concurrent', 100),
            max_idle=pool_config.get('max_idle', 5),
            max_age=pool_config.get('max_age', 300)
        )

        # Create secure connection
        try:
            client, closer, err = self._connection_pool.create_client(address, use_connection_pool)
            if err:
                raise SDKError(f"Failed to create connection: {err}")
            self.client = client
            if closer:
                self._connection_closer = closer
            # Store the channel reference for streaming and IPC
            self.conn = client._channel
        except Exception as e:
            raise SDKError(f"Failed to initialize SDK: {str(e)}")

        # Initialize erasure coding if needed
        if self.parity_blocks_count > 0:
            self.streaming_erasure_code = ErasureCode(
                self.streaming_max_blocks_in_chunk - self.parity_blocks_count,
                self.parity_blocks_count
            )

        self.sp_client = SPClient()

    def close(self):
        """Safely close all connections"""
        if hasattr(self, '_connection_closer'):
            try:
                self._connection_closer()
            except Exception as e:
                logging.warning(f"Error closing connection: {e}")
        
        if self._connection_pool:
            err = self._connection_pool.close()
            if err:
                logging.warning(f"Error closing connection pool: {err}")

    def __enter__(self):
        """Support for context manager protocol"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure resources are cleaned up"""
        self.close()

    def streaming_api(self):
        """Get a streaming API instance with the current connection"""
        return StreamingAPI(
            self.conn,
            self.client,
            self.streaming_erasure_code,
            self.max_concurrency,
            self.block_part_size,
            self.use_connection_pool,
            self.encryption_key,
            self.streaming_max_blocks_in_chunk
        )

    def ipc(self):
        """Get an IPC instance with the current connection"""
        try:
            client = ipcnodeapi_pb2_grpc.IPCNodeAPIStub(self.conn)
            ipc_instance = Client.dial(self.conn, self.private_key, client)
            return IPC(
                client,
                self.conn,
                self.max_concurrency,
                self.block_part_size,
                self.use_connection_pool,
                self.encryption_key,
                ipc_instance
            )
        except Exception as e:
            raise SDKError(f"Failed to create IPC instance: {str(e)}")

    def create_bucket(self, ctx, name: str):
        if len(name) < MIN_BUCKET_NAME_LENGTH:
            raise SDKError("Invalid bucket name")

        request = nodeapi_pb2.BucketCreateRequest(name=name)
        response = self.client.bucket_create(ctx, request)
        return BucketCreateResult(name=response.name, created_at=response.created_at.AsTime() if hasattr(response.created_at, 'AsTime') else response.created_at)

    def view_bucket(self, ctx, name: str):
        if name == "":
            raise SDKError("Invalid bucket name")

        request = nodeapi_pb2.BucketViewRequest(name=name)
        response = self.client.bucket_view(ctx, request)
        return Bucket(
            name=response.name, 
            created_at=response.created_at.AsTime() if hasattr(response.created_at, 'AsTime') else response.created_at
        )

    def delete_bucket(self, ctx, name: str):
        if name == "":
            raise SDKError("Invalid bucket name")
           
        try:
            request = nodeapi_pb2.BucketDeleteRequest(name=name)
            self.client.bucket_delete(ctx, request)
            return True
        except Exception as err:
            logging.error(f"Error deleting bucket: {err}")
            raise SDKError(f"Failed to delete bucket: {err}")

    def extract_block_data(id_str: str, data: bytes) -> bytes:
        try:
         block_cid = cid.decode(id_str)
        except Exception as e:
          raise ValueError(f"Invalid CID: {e}")

        if block_cid.codec == "dag-pb":
          try:
            dag_node = ipfshttpclient.codec.decode("dag-pb", data) #Decoding the DAG node
            unixfs_data = dag_node["Data"] 
            return unixfs_data
          except Exception as e:
            raise ValueError(f"Failed to decode DAG node: {e}")
    
        elif block_cid.codec == "raw":
         return data 
     
        else:
         raise ValueError(f"Unknown CID type: {block_cid.codec}")


class BucketCreateResult:
    def __init__(self, name: str, created_at: Timestamp):
        self.name = name
        self.created_at = created_at

class Bucket:
    def __init__(self, name: str, created_at: Timestamp):
        self.name = name
        self.created_at = created_at

def encryption_key_derivation(parent_key: bytes, *info_data: str):
    """Derive an encryption key from a parent key and information data"""
    if len(parent_key) == 0:
        return None

    info = "/".join(info_data)
    key = derive_key(parent_key, info.encode())
    return key
