import time
from typing import List, Optional, Union, Any, NewType
from dataclasses import dataclass
from datetime import datetime

from multiformats.cid import CID as CIDType

TimestampType = Union[datetime, float, int]

@dataclass
class BucketCreateResult:
    """Result of bucket creation."""
    name: str
    created_at: TimestampType


@dataclass
class Bucket:
    """A bucket."""
    name: str
    created_at: TimestampType


@dataclass
class Chunk:
    """A piece of metadata of some file."""
    cid: str
    encoded_size: int
    size: int
    index: int


@dataclass
class FileBlockUpload:
    """A piece of metadata of some file used for upload."""
    cid: str
    data: bytes
    permit: str = ""
    node_address: str = ""
    node_id: str = ""

    # Alias properties for backwards compatibility with uppercase naming
    @property
    def CID(self):
        return self.cid
        
    @property
    def Data(self):
        return self.data
        
    @property
    def NodeAddress(self):
        return self.node_address
        
    @property
    def NodeID(self):
        return self.node_id
        
    @property
    def Permit(self):
        return self.permit


@dataclass
class FileBlockDownload:
    """A piece of metadata of some file used for download."""
    cid: str
    data: bytes
    permit: str = ""
    node_address: str = ""
    node_id: str = ""


@dataclass
class FileListItem:
    """Contains bucket file list file meta information."""
    root_cid: str
    name: str
    size: int
    created_at: TimestampType
    data_blocks: int = 0
    total_blocks: int = 0


@dataclass
class FileUpload:
    """Contains single file meta information."""
    bucket_name: str
    name: str
    stream_id: str
    created_at: TimestampType
    data_blocks: int = 0
    total_blocks: int = 0


@dataclass
class FileChunkUpload:
    """Contains single file chunk meta information."""
    stream_id: str
    index: int
    chunk_cid: CIDType
    raw_data_size: int  # uint64 in Go
    encoded_size: int   # uint64 in Go
    blocks: List[FileBlockUpload]


@dataclass
class FileDownload:
    """Contains single file meta information."""
    stream_id: str
    bucket_name: str
    name: str
    chunks: List[Chunk]
    data_blocks: int = 0
    total_blocks: int = 0


@dataclass
class FileChunkDownload:
    """Contains single file chunk meta information."""
    cid: str
    index: int
    encoded_size: int
    size: int
    blocks: List[FileBlockDownload]


@dataclass
class FileMeta:
    """Contains single file meta information."""
    stream_id: str
    root_cid: str
    bucket_name: str
    name: str
    encoded_size: int
    size: int
    created_at: datetime
    committed_at: Optional[datetime] = None
    data_blocks: int = 0
    total_blocks: int = 0


@dataclass
class IPCBucketCreateResult:
    """Result of IPC bucket creation."""
    id: str
    name: str
    created_at: TimestampType


@dataclass
class IPCBucket:
    """An IPC bucket."""
    id: str
    name: str
    created_at: TimestampType


@dataclass
class IPCFileDownload:
    """Represents an IPC file download and some metadata."""
    bucket_name: str
    name: str
    chunks: List[Chunk]


@dataclass
class IPCFileListItem:
    """Contains IPC bucket file list file meta information."""
    root_cid: str
    name: str
    encoded_size: int
    actual_size: int
    created_at: TimestampType


@dataclass
class IPCFileMeta:
    """Contains single IPC file meta information."""
    root_cid: str
    name: str
    bucket_name: str
    encoded_size: int
    actual_size: int
    is_public: bool
    created_at: TimestampType


@dataclass
class IPCFileMetaV2:
    """Contains single file meta information."""
    root_cid: str
    bucket_name: str
    name: str
    encoded_size: int
    size: int = 0
    created_at: Optional[TimestampType] = None
    committed_at: Optional[TimestampType] = None


@dataclass
class IPCFileChunkUploadV2:
    """Contains single file chunk meta information."""
    index: int
    chunk_cid: CIDType
    actual_size: int
    raw_data_size: int  # uint64 in Go
    encoded_size: int   # uint64 in Go
    blocks: List[FileBlockUpload]
    bucket_id: bytes  # 32-byte array in Go, using bytes in Python
    file_name: str


@dataclass
class TxWaitSignal:
    file_upload_chunk: IPCFileChunkUploadV2
    transaction: Any  


class UploadState:    
    def __init__(self, dag_root):
        from threading import RLock
        self.dag_root = dag_root
        self.mutex = RLock()
        self.pre_created_chunks = {}  # map[int]chunkWithTx
        self.is_committed = False
        self.chunk_count = 0
        self.actual_file_size = 0
        self.encoded_file_size = 0
    
    def pre_create_chunk(self, chunk: IPCFileChunkUploadV2, tx) -> None:
        with self.mutex:
            self.pre_created_chunks[chunk.index] = {
                'chunk': chunk,
                'tx': tx
            }
            self.chunk_count += 1
            self.actual_file_size += chunk.actual_size
            self.encoded_file_size += chunk.encoded_size
            if hasattr(self.dag_root, 'add_link'):
                self.dag_root.add_link(chunk.chunk_cid, chunk.raw_data_size, chunk.encoded_size)
    
    def chunk_uploaded(self, chunk: IPCFileChunkUploadV2) -> None:
        with self.mutex:
            if chunk.index in self.pre_created_chunks:
                del self.pre_created_chunks[chunk.index]
    
    def list_pre_created_chunks(self) -> List[dict]:
        with self.mutex:
            return list(self.pre_created_chunks.values())


@dataclass
class IPCFileUpload:
    bucket_name: str
    name: str
    state: UploadState
    blocks_counter: int = 0
    bytes_counter: int = 0
    chunks_counter: int = 0


def new_ipc_file_upload(bucket_name: str, name: str) -> IPCFileUpload:
    from .dag import DAGRoot
    dag_root = DAGRoot.new()
    state = UploadState(dag_root)
    
    return IPCFileUpload(
        bucket_name=bucket_name,
        name=name,
        state=state,
        blocks_counter=0,
        bytes_counter=0,
        chunks_counter=0
    )


@dataclass
class ArchivalMetadata:
    """Contains file metadata with chunks and blocks including PDP data."""
    bucket_name: str
    name: str
    chunks: List['ArchivalChunk']


@dataclass
class ArchivalChunk:
    """Contains chunk metadata with blocks."""
    chunk: Chunk
    blocks: List['ArchivalBlock']


@dataclass
class ArchivalBlock:
    """Contains block metadata with PDP data."""
    cid: str
    size: int
    pdp_data: Optional['PDPBlockData'] = None


@dataclass
class PDPBlockData:
    """Contains PDP data for a block."""
    url: str
    offset: int
    size: int
    data_set_id: int


class ErrMissingArchivalBlock(Exception):
    """Error returned when archival block metadata is missing."""
    
    def __init__(self, block_cid: str):
        self.block_cid = block_cid
        super().__init__(f"missing archival block metadata for block CID {block_cid}")
