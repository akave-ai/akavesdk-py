from typing import Optional, List
from private.memory.memory import Size
from dataclasses import dataclass
from .erasure_code import ErasureCode

BLOCK_SIZE = 1 * Size.MB
ENCRYPTION_OVERHEAD = 28  # 16 bytes for AES-GCM tag, 12 bytes for nonce
MIN_BUCKET_NAME_LENGTH = 3
MIN_FILE_SIZE = 127  # 127 bytes



# Default CID version and codecs for IPFS
# used in the DAG operations

DEFAULT_CID_VERSION = 1
DAG_PB_CODEC = 0x70
RAW_CODEC = 0x55


# Constants
BlockSize = 1 << 20  # 1MiB blocks
EncryptionOverhead = 16  # 16 bytes overhead from encryption



## [Base Config Class]

class Config:
    """Configuration for the Ethereum storage contract client."""
    def __init__(self, dial_uri: str, private_key: str, storage_contract_address: str, access_contract_address: Optional[str] = None, policy_factory_contract_address: Optional[str] = None):
        self.dial_uri = dial_uri
        self.private_key = private_key
        self.storage_contract_address = storage_contract_address
        self.access_contract_address = access_contract_address
        self.policy_factory_contract_address = policy_factory_contract_address or ""

    @staticmethod
    def default():
        return Config(dial_uri="", private_key="", storage_contract_address="", access_contract_address="", policy_factory_contract_address="")


## [SDK Error Class]

class SDKError(Exception):
    pass 



## [Validation Functions]

# Basic validation: expect hex string like '0x' + 8 hex chars (4 bytes) minimum
def validate_hex_string(hex_string: str) -> bool:
    if not hex_string.startswith("0x"):
        return False
    if len(hex_string) < 10:
        return False
    return True


## [Test Configurations]

DEFAULT_CONFIG_TEST_STREAMING_CONN = {
    'AKAVE_SDK_NODE': 'connect.akave.ai:5000',  
    'ENCRYPTION_KEY': '',  
}

DEFAULT_CONFIG_TEST_SDK_CONN = {
    'AKAVE_SDK_NODE': 'connect.akave.ai:5000',  # For streaming operations
    'AKAVE_IPC_NODE': 'connect.akave.ai:5500',  # For IPC operations
    'ETHEREUM_NODE_URL': 'https://n3-us.akave.ai/ext/bc/2JMWNmZbYvWcJRPPy1siaDBZaDGTDAaqXoY5UBKh4YrhNFzEce/rpc',
    'STORAGE_CONTRACT_ADDRESS': '0x9Aa8ff1604280d66577ecB5051a3833a983Ca3aF',  # Will be obtained from node
    'ACCESS_CONTRACT_ADDRESS': '',   # Will be obtained from node
}


## [Error Handling Functions]

# List of known error strings from the smart contracts
# Replace these with the actual error strings from your contracts

KNOWN_ERROR_STRINGS: List[str] = [
    "Storage: bucket doesn't exist",
    "Storage: bucket exists",
    "Storage: file doesn't exist",
    "Storage: file exists",
    "AccessManager: caller is not the owner",
    "AccessManager: caller is not authorized",
    # Add all other known error strings here...
]

@dataclass
class SDKConfig:
    address: str
    max_concurrency: int = 10
    block_part_size: int = 1024 * 1024
    use_connection_pool: bool = True
    parity_blocks_count: int = 0
    chunk_buffer: int = 10
    encryption_key: Optional[bytes] = None
    private_key: Optional[str] = None
    streaming_max_blocks_in_chunk: int = 32
    connection_timeout: Optional[int] = 30
    max_retries: Optional[int] = 3
    backoff_delay: Optional[int] = 1
    ipc_address: Optional[str] = None
    erasure_code: Optional[ErasureCode] = None
