import grpc
from google.protobuf.timestamp_pb2 import Timestamp
import logging
from private.pb import nodeapi_pb2, nodeapi_pb2_grpc, ipcnodeapi_pb2, ipcnodeapi_pb2_grpc
from private.ipc.client import Client, Config as IPCConfig
from private.spclient.spclient import SPClient
from private.encryption import derive_key
from typing import List, Optional
from multiformats.cid import CID
from .sdk_ipc import IPC
from .sdk_streaming import StreamingAPI
from .erasure_code import ErasureCode
from .common import SDKError, BLOCK_SIZE, MIN_BUCKET_NAME_LENGTH
import os
import time

from .config import config

try:
    from ipld_dag_pb import decode as decode_dag_pb
    DAG_PB_AVAILABLE = True
except ImportError:
    DAG_PB_AVAILABLE = False

class AkaveContractFetcher:
    """Fetches contract addresses from Akave node"""
    
    def __init__(self, node_address: str):
        self.node_address = node_address
        self.channel = None
        self.stub = None
    
    def connect(self) -> bool:
        """Connect to the Akave node"""
        try:
            logging.info(f"🔗 Connecting to {self.node_address}...")
            self.channel = grpc.insecure_channel(self.node_address)
            self.stub = ipcnodeapi_pb2_grpc.IPCNodeAPIStub(self.channel)
            return True
            
        except grpc.RpcError as e:
            logging.error(f"❌ gRPC error: {e.code()} - {e.details()}")
            return False
        except Exception as e:
            logging.error(f"❌ Connection error: {type(e).__name__}: {str(e)}")
            return False
    
    def fetch_contract_addresses(self) -> Optional[dict]:
        """Fetch contract addresses from the node"""
        if not self.stub:
            return None
        
        try:
            request = ipcnodeapi_pb2.ConnectionParamsRequest()
            response = self.stub.ConnectionParams(request)
            
            contract_info = {
                'dial_uri': response.dial_uri if hasattr(response, 'dial_uri') else None,
                'contract_address': response.storage_address if hasattr(response, 'storage_address') else None,
            }
            
            if hasattr(response, 'access_address'):
                contract_info['access_address'] = response.access_address
            
            return contract_info
        except Exception as e:
            logging.error(f"❌ Error fetching contract info: {e}")
            return None
    
    def close(self):
        """Close the gRPC connection"""
        if self.channel:
            self.channel.close()

class SDK:
    def __init__(self, address: str, max_concurrency: int, block_part_size: int, use_connection_pool: bool,
                 encryption_key: Optional[bytes] = None, private_key: Optional[str] = None,
                 streaming_max_blocks_in_chunk: int = 32, parity_blocks_count: int = 0,
                 ipc_address: Optional[str] = None):
        
        if not address:
            address = config.akave_sdk_node
        
        self.client = None
        self.conn = None
        self.ipc_conn = None
        self.ipc_client = None
        self.sp_client = None
        self.streaming_erasure_code = None
        self.max_concurrency = max_concurrency
        self.block_part_size = block_part_size
        self.use_connection_pool = use_connection_pool
        self.private_key = private_key or config.private_key
        self.encryption_key = encryption_key or []
        self.streaming_max_blocks_in_chunk = streaming_max_blocks_in_chunk
        self.parity_blocks_count = parity_blocks_count
        self.ipc_address = ipc_address or config.akave_ipc_node or address  # Use provided IPC address or fallback to main address
        
        self._contract_info = None

        if self.block_part_size <= 0 or self.block_part_size > BLOCK_SIZE:
            raise SDKError(f"Invalid blockPartSize: {block_part_size}. Valid range is 1-{BLOCK_SIZE}")

        # Create gRPC channel and clients for SDK operations
        self.conn = grpc.insecure_channel(address)
        self.client = nodeapi_pb2_grpc.NodeAPIStub(self.conn)
        
        # Create separate gRPC channel for IPC operations if needed
        if self.ipc_address == address:
            # Reuse main connection for IPC
            self.ipc_conn = self.conn
        else:
            # Create separate connection for IPC
            self.ipc_conn = grpc.insecure_channel(self.ipc_address)
        
        self.ipc_client = ipcnodeapi_pb2_grpc.IPCNodeAPIStub(self.ipc_conn)

        if len(self.encryption_key) != 0 and len(self.encryption_key) != 32:
            raise SDKError("Encryption key length should be 32 bytes long")

        if self.parity_blocks_count > self.streaming_max_blocks_in_chunk // 2:
            raise SDKError(f"Parity blocks count {self.parity_blocks_count} should be <= {self.streaming_max_blocks_in_chunk // 2}")

        if self.parity_blocks_count > 0:
            self.streaming_erasure_code = ErasureCode(self.streaming_max_blocks_in_chunk - self.parity_blocks_count, self.parity_blocks_count)

        self.sp_client = SPClient()

    def _fetch_contract_info(self) -> Optional[dict]:
        """Dynamically fetch contract information using multiple endpoints"""
        if self._contract_info:
            return self._contract_info
            
        endpoints = [
            'yucca.akave.ai:5500',
            # 'connect.akave.ai:5500'  # DNS resolution failing
        ]
        
        for endpoint in endpoints:
            logging.info(f"🔄 Trying endpoint: {endpoint}")
            fetcher = AkaveContractFetcher(endpoint)
            
            if fetcher.connect():
                logging.info("✅ Connected successfully!")
                
                info = fetcher.fetch_contract_addresses()
                fetcher.close()
                
                if info and info.get('contract_address') and info.get('dial_uri'):
                    logging.info("✅ Successfully fetched contract information!")
                    logging.info(f"📍 Contract Details: dial_uri={info.get('dial_uri')}, contract_address={info.get('contract_address')}")
                    self._contract_info = info
                    return info
                else:
                    logging.warning("❌ Failed to fetch complete contract information")
            else:
                logging.warning(f"❌ Failed to connect to {endpoint}")
                fetcher.close()
        
        logging.error("❌ All endpoints failed for contract fetching")
        return None

    def close(self):
        """Close the gRPC channels."""
        if self.conn:
            self.conn.close()
        if self.ipc_conn and self.ipc_conn != self.conn:
            self.ipc_conn.close()

    def streaming_api(self):
        """Returns SDK streaming API."""
        return StreamingAPI(
            conn=self.conn,
            client=nodeapi_pb2_grpc.StreamAPIStub(self.conn),
            erasure_code=self.streaming_erasure_code,
            max_concurrency=self.max_concurrency,
            block_part_size=self.block_part_size,
            use_connection_pool=self.use_connection_pool,
            encryption_key=self.encryption_key,
            max_blocks_in_chunk=self.streaming_max_blocks_in_chunk
        )

    def ipc(self):
        """Returns SDK IPC API."""
        try:
            # Get connection parameters dynamically
            conn_params = self._fetch_contract_info()
            
            if not conn_params:
                raise SDKError("Could not fetch contract information from any Akave node")
            
            if not self.private_key:
                raise SDKError("Private key is required for IPC operations")
            
            config = IPCConfig(
                dial_uri=conn_params['dial_uri'],
                private_key=self.private_key,
                storage_contract_address=conn_params['contract_address'],
                access_contract_address=conn_params.get('access_address', '')
            )
            
            # Create IPC instance with retries
            max_retries = 3
            retry_delay = 1  
            last_error = None
            
            for attempt in range(max_retries):
                try:
                    ipc_instance = Client.dial(config)
                    if ipc_instance:
                        logging.info("Successfully connected to Ethereum node")
                        break
                except Exception as e:
                    last_error = e
                    logging.warning(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    continue
            else:
                raise SDKError(f"Failed to dial IPC client after {max_retries} attempts: {str(last_error)}")
            
            return IPC(
                client=self.ipc_client,
                conn=self.ipc_conn,  # Use the IPC connection
                ipc_instance=ipc_instance,
                max_concurrency=self.max_concurrency,
                block_part_size=self.block_part_size,
                use_connection_pool=self.use_connection_pool,
                encryption_key=self.encryption_key,
                max_blocks_in_chunk=self.streaming_max_blocks_in_chunk
            )
        except Exception as e:
            raise SDKError(f"Failed to initialize IPC API: {str(e)}")

    def create_bucket(self, ctx, name: str):
        if len(name) < MIN_BUCKET_NAME_LENGTH:
            raise SDKError("Invalid bucket name")

        request = nodeapi_pb2.BucketCreateRequest(name=name)
        response = self.client.BucketCreate(request)
        return BucketCreateResult(name=response.name, created_at=response.created_at.AsTime() if hasattr(response.created_at, 'AsTime') else response.created_at)

    def view_bucket(self, ctx, name: str):
        if name == "":
            raise SDKError("Invalid bucket name")

        request = nodeapi_pb2.BucketViewRequest(bucket_name=name)
        response = self.client.BucketView(request)
        return Bucket(
            name=response.name, 
            created_at=response.created_at.AsTime() if hasattr(response.created_at, 'AsTime') else response.created_at
        )

    def delete_bucket(self, ctx, name: str):
        if name == "":
            raise SDKError("Invalid bucket name")
           
        try:
            request = nodeapi_pb2.BucketDeleteRequest(name=name)
            self.client.BucketDelete(request)
            return True
        except Exception as err:
            logging.error(f"Error deleting bucket: {err}")
            raise SDKError(f"Failed to delete bucket: {err}")

    def extract_block_data(id_str: str, data: bytes) -> bytes:
        try:
            block_cid = CID.decode(id_str)
        except Exception as e:
            raise ValueError(f"Invalid CID: {e}")
        codec_name = getattr(block_cid.codec, 'name', str(block_cid.codec))
        
        if codec_name == "dag-pb":
            if not DAG_PB_AVAILABLE:
                raise ValueError("DAG-PB decoding requires ipld_dag_pb library. Install with: pip install ipld_dag_pb")
            try:
                decoded_node = decode_dag_pb(data)
                return decoded_node.data if decoded_node.data else b''
            except Exception as e:
                raise ValueError(f"Failed to decode DAG-PB node: {e}")
        elif codec_name == "raw":
            return data 
        else:
            raise ValueError(f"Unknown CID codec: {codec_name}")

class BucketCreateResult:
    def __init__(self, name: str, created_at: Timestamp):
        self.name = name
        self.created_at = created_at

class Bucket:
    def __init__(self, name: str, created_at: Timestamp):
        self.name = name
        self.created_at = created_at

def encryption_key_derivation(parent_key: bytes, *info_data: str):
    if len(parent_key) == 0:
        return None

    info = "/".join(info_data)
    key = derive_key(parent_key, info.encode())
    return key
