import sys
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PRIVATE_PATH = os.path.join(PROJECT_ROOT, "private")
if PRIVATE_PATH not in sys.path:
    sys.path.append(PRIVATE_PATH)

try:
    # Import WithRetry from retry module
    from private.retry.retry import WithRetry
    
    from .sdk import (
        # Core SDK class
        SDK,
        
        # Data classes
        BucketCreateResult,
        Bucket,
        MonkitStats,
        AkaveContractFetcher,
        
        # SDK Options
        SDKOption,
        WithMetadataEncryption,
        WithEncryptionKey,
        WithPrivateKey,
        WithStreamingMaxBlocksInChunk,
        WithErasureCoding,
        WithChunkBuffer,
        WithBatchSize,
        WithCustomHttpClient,
        WithoutRetry,
        
        # Utility functions
        get_monkit_stats,
        extract_block_data,
        encryption_key_derivation,
        is_retryable_tx_error,
        skip_to_position,
        parse_timestamp,
        
        # Constants
        ENCRYPTION_OVERHEAD,
        MIN_FILE_SIZE,
    )
    
    # Import model classes
    from .model import (
        IPCFileUpload,
        new_ipc_file_upload,
        UploadState,
        TxWaitSignal,
        IPCFileChunkUploadV2,
        IPCFileMetaV2,
        IPCBucketCreateResult,
        IPCBucket,
        IPCFileMeta,
        IPCFileListItem,
        IPCFileDownload,
        FileChunkDownload,
        Chunk,
        FileBlockUpload,
        FileBlockDownload,
        ArchivalMetadata,
        ArchivalChunk,
        ArchivalBlock,
        PDPBlockData,
        ErrMissingArchivalBlock
    )
    
    _SDK_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import SDK core modules: {e}")
    _SDK_AVAILABLE = False
    
    class SDK:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class BucketCreateResult:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class Bucket:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class MonkitStats:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class WithRetry:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class AkaveContractFetcher:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class SDKOption:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class WithMetadataEncryption:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class WithEncryptionKey:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class WithPrivateKey:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class WithStreamingMaxBlocksInChunk:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class WithErasureCoding:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class WithChunkBuffer:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class WithoutRetry:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    def get_monkit_stats(*args, **kwargs):
        raise ImportError("SDK not available due to missing dependencies")
    
    def extract_block_data(*args, **kwargs):
        raise ImportError("SDK not available due to missing dependencies")
    
    def encryption_key_derivation(*args, **kwargs):
        raise ImportError("SDK not available due to missing dependencies")
    
    def is_retryable_tx_error(*args, **kwargs):
        raise ImportError("SDK not available due to missing dependencies")
    
    def skip_to_position(*args, **kwargs):
        raise ImportError("SDK not available due to missing dependencies")
    
    def parse_timestamp(*args, **kwargs):
        raise ImportError("SDK not available due to missing dependencies")
    
    ENCRYPTION_OVERHEAD = 28
    MIN_FILE_SIZE = 127
    
    class IPCFileUpload:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    def new_ipc_file_upload(*args, **kwargs):
        raise ImportError("SDK not available due to missing dependencies")
    
    class UploadState:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class TxWaitSignal:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class IPCFileChunkUploadV2:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class IPCFileMetaV2:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class IPCBucketCreateResult:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class IPCBucket:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class IPCFileMeta:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class IPCFileListItem:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class IPCFileDownload:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class FileChunkDownload:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class Chunk:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class FileBlockUpload:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class FileBlockDownload:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class ArchivalMetadata:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class ArchivalChunk:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class ArchivalBlock:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class PDPBlockData:
        def __init__(self, *args, **kwargs):
            raise ImportError("SDK not available due to missing dependencies")
    
    class ErrMissingArchivalBlock(Exception):
        pass

try:
    from .config import SDKConfig, SDKError, Config
except ImportError as e:
    print(f"Warning: Could not import config modules: {e}")
    SDKConfig = None
    SDKError = Exception
    Config = None

try:
    from .sdk_ipc import IPC
except ImportError:
    IPC = None

try:
    from private.cids import verify_raw, verify, CIDError
except ImportError:
    def verify_raw(*args, **kwargs):
        raise ImportError("CID utilities not available due to missing dependencies")
    def verify(*args, **kwargs):
        raise ImportError("CID utilities not available due to missing dependencies")
    CIDError = Exception


__all__ = [
    # Core SDK
    'SDK',
    
    # Data classes
    'BucketCreateResult',
    'Bucket', 
    'MonkitStats',
    'WithRetry',
    'AkaveContractFetcher',
    
    # SDK Options
    'SDKOption',
    'WithMetadataEncryption',
    'WithEncryptionKey',
    'WithPrivateKey',
    'WithStreamingMaxBlocksInChunk',
    'WithErasureCoding',
    'WithChunkBuffer',
    'WithoutRetry',
    
    # Utility functions
    'get_monkit_stats',
    'extract_block_data',
    'encryption_key_derivation',
    'is_retryable_tx_error',
    'skip_to_position',
    'parse_timestamp',
    
    # Constants
    'ENCRYPTION_OVERHEAD',
    'MIN_FILE_SIZE',
    
    # Configuration
    'SDKConfig',
    'SDKError',
    'Config',
    
    # APIs
    'IPC',
    
    # CID utilities
    'verify_raw',
    'verify',
    'CIDError',
    
    # Model classes
    'IPCFileUpload',
    'new_ipc_file_upload',
    'UploadState',
    'TxWaitSignal',
    'IPCFileChunkUploadV2',
    'IPCFileMetaV2',
    'IPCBucketCreateResult',
    'IPCBucket',
    'IPCFileMeta',
    'IPCFileListItem',
    'IPCFileDownload',
    'FileChunkDownload',
    'Chunk',
    'FileBlockUpload',
    'FileBlockDownload',
    'ArchivalMetadata',
    'ArchivalChunk',
    'ArchivalBlock',
    'PDPBlockData',
    'ErrMissingArchivalBlock',
]
