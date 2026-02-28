import grpc
import logging
import io
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Callable, TypeVar, List, Dict, Any
from private.pb import ipcnodeapi_pb2, ipcnodeapi_pb2_grpc
from private.ipc.client import Client
from private.retry.retry import WithRetry
from .sdk_ipc import IPC
from .config import Config, SDKConfig, SDKError, BLOCK_SIZE, MIN_BUCKET_NAME_LENGTH
from private.encryption import derive_key
from .shared.grpc_base import GrpcClientBase

try:
    import requests
    HTTP_CLIENT_AVAILABLE = True
except ImportError:
    HTTP_CLIENT_AVAILABLE = False

ENCRYPTION_OVERHEAD = 28  # 16 bytes for AES-GCM tag, 12 bytes for nonce
MIN_FILE_SIZE = 127  # 127 bytes

class AkaveContractFetcher:
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


T = TypeVar("T")  # Generic return type for gRPC calls

@dataclass
class BucketCreateResult:
    name: str
    created_at: datetime


@dataclass
class Bucket:
    name: str
    created_at: datetime

@dataclass
class MonkitStats:
    name: str
    successes: int
    errors: Dict[str, int]  
    highwater: int  
    success_times: Optional[List[float]] = None  
    failure_times: Optional[List[float]] = None  


class SDKOption:
    def apply(self, sdk: 'SDK'):
        pass

class WithMetadataEncryption(SDKOption):
    def apply(self, sdk: 'SDK'):
        sdk.use_metadata_encryption = True

class WithEncryptionKey(SDKOption):
    def __init__(self, key: bytes):
        self.key = key
    
    def apply(self, sdk: 'SDK'):
        sdk.encryption_key = self.key

class WithPrivateKey(SDKOption):
    def __init__(self, key: str):
        self.key = key
    
    def apply(self, sdk: 'SDK'):
        sdk.private_key = self.key

class WithStreamingMaxBlocksInChunk(SDKOption):
    def __init__(self, max_blocks_in_chunk: int):
        self.max_blocks_in_chunk = max_blocks_in_chunk
    
    def apply(self, sdk: 'SDK'):
        sdk.streaming_max_blocks_in_chunk = self.max_blocks_in_chunk

class WithErasureCoding(SDKOption):
    def __init__(self, parity_blocks: int):
        self.parity_blocks = parity_blocks
    
    def apply(self, sdk: 'SDK'):
        sdk.parity_blocks_count = self.parity_blocks

class WithChunkBuffer(SDKOption):
    def __init__(self, buffer_size: int):
        self.buffer_size = buffer_size
    
    def apply(self, sdk: 'SDK'):
        sdk.chunk_buffer = self.buffer_size

class WithBatchSize(SDKOption):
    def __init__(self, batch_size: int):
        self.batch_size = max(1, batch_size)  # Ensure at least 1
    
    def apply(self, sdk: 'SDK'):
        sdk.batch_size = self.batch_size

class WithCustomHttpClient(SDKOption):
    def __init__(self, client):
        self.client = client
    
    def apply(self, sdk: 'SDK'):
        sdk.http_client = self.client

class WithoutRetry(SDKOption):
    def apply(self, sdk: 'SDK'):
        sdk.with_retry = WithRetry(max_attempts=0, base_delay=0.1)

class SDK():
    def __init__(self, config: SDKConfig):
        self.conn = None
        self.ipc_conn = None
        self.ipc_client = None
        self.config = config
        self._grpc_base = GrpcClientBase(self.config.connection_timeout)

        self.config.encryption_key = config.encryption_key or []
        self.ipc_address = config.ipc_address or config.address  # Use provided IPC address or fallback to main address
        self._contract_info = None
        
        # Initialize HTTP client
        if HTTP_CLIENT_AVAILABLE:
            self.http_client = requests.Session()
        else:
            self.http_client = None
        
        # Initialize batch size and retry settings
        self.batch_size = 1
        self.with_retry = WithRetry(max_attempts=5, base_delay=0.1)

        if self.config.block_part_size <= 0 or self.config.block_part_size > BLOCK_SIZE:
            raise SDKError(f"Invalid blockPartSize: {config.block_part_size}. Valid range is 1-{BLOCK_SIZE}")

        self.conn = grpc.insecure_channel(config.address)
       
        if self.ipc_address == config.address:
            self.ipc_conn = self.conn
        else:
            self.ipc_conn = grpc.insecure_channel(self.ipc_address)
        
        self.ipc_client = ipcnodeapi_pb2_grpc.IPCNodeAPIStub(self.ipc_conn)

        if len(self.config.encryption_key) != 0 and len(self.config.encryption_key) != 32:
            raise SDKError("Encryption key length should be 32 bytes long")
        
        # Sanitize retry params
        if self.with_retry.max_attempts < 0:
            self.with_retry.max_attempts = 0
        if self.with_retry.base_delay <= 0.1:
            self.with_retry.base_delay = 0.1

    def _fetch_contract_info(self) -> Optional[dict]:
        """Dynamically fetch contract information using multiple endpoints"""
        if self._contract_info:
            return self._contract_info
            
        endpoints = [
             'connect.akave.ai:5500'  
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
        """Close the gRPC channels and HTTP client."""
        if self.http_client and HTTP_CLIENT_AVAILABLE:
            self.http_client.close()
        if self.conn:
            self.conn.close()
        if self.ipc_conn and self.ipc_conn != self.conn:
            self.ipc_conn.close()

    def ipc(self):
        """Returns SDK IPC API."""
        try:
            # Get connection parameters dynamically
            conn_params = self._fetch_contract_info()
            
            if not conn_params:
                raise SDKError("Could not fetch contract information from any Akave node")
            
            if not self.config.private_key:
                raise SDKError("Private key is required for IPC operations")
            
            config = Config(
                dial_uri=conn_params['dial_uri'],
                private_key=self.config.private_key,
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
                config=self.config,
                http_client=self.http_client,
                batch_size=self.batch_size,
                with_retry=self.with_retry
            )
        except Exception as e:
            raise SDKError(f"Failed to initialize IPC API: {str(e)}")
    


def get_monkit_stats() -> List[MonkitStats]:
    stats = []
    return stats

def extract_block_data(id_str: str, data: bytes) -> bytes:
    try:
        from multiformats import CID
        cid = CID.decode(id_str)
    except Exception as e:
        raise ValueError(f"failed to decode CID: {e}")
    
    codec_name = getattr(cid.codec, 'name', str(cid.codec))
    
    if codec_name == "dag-pb":
        try:
            from .dag import extract_block_data as dag_extract
            return dag_extract(id_str, data)
        except Exception as e:
            raise ValueError(f"failed to decode DAG-PB node: {e}")
    elif codec_name == "raw":
        return data
    else:
        raise ValueError(f"unknown cid type: {codec_name}")

def encryption_key_derivation(parent_key: bytes, *info_data: str) -> bytes:
    if not parent_key:
        return b""
    
    info = "/".join(info_data)
    try:
        key = derive_key(parent_key, info.encode())
        return key
    except Exception as e:
        raise SDKError(f"failed to derive key: {e}")

def is_retryable_tx_error(err: Exception) -> bool:
    if err is None:
        return False
    
    msg = str(err).lower()
    retryable_errors = [
        "nonce too low",
        "replacement transaction underpriced", 
        "eof"
    ]
    
    return any(error in msg for error in retryable_errors)

def skip_to_position(reader: io.IOBase, position: int) -> None:
    if position > 0:
        if hasattr(reader, 'seek'):
            try:
                reader.seek(position, io.SEEK_SET)
                return
            except (OSError, io.UnsupportedOperation):
                pass
        
        if hasattr(reader, 'read'):
            remaining = position
            while remaining > 0:
                chunk_size = min(remaining, 8192)  # Read in 8KB chunks
                chunk = reader.read(chunk_size)
                if not chunk:  # EOF reached
                    break
                remaining -= len(chunk)
        else:
            raise SDKError("reader does not support seek or read operations")

def parse_timestamp(ts) -> Optional[datetime]:
    if ts is None:
        return None
    return ts.AsTime() if hasattr(ts, "AsTime") else ts

def get_monkit_stats() -> List[MonkitStats]:
    stats = []
    
    placeholder_stats = [
        MonkitStats(
            name="sdk.upload",
            successes=0,
            errors={},
            highwater=0,
            success_times=None,
            failure_times=None
        ),
        MonkitStats(
            name="sdk.download", 
            successes=0,
            errors={},
            highwater=0,
            success_times=None,
            failure_times=None
        )
    ]
    
    for stat in placeholder_stats:
        if stat.successes > 0 or len(stat.errors) > 0:
            stats.append(stat)
    
    return stats