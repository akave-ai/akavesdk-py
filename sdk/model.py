# Copyright (C) 2024 Akave
# See LICENSE for copying information.

"""
Module containing data model classes for the Akave SDK.
These classes are the Python equivalent of Go structs from model.go.
"""

import time
from typing import List, Optional, Union, Any, NewType
from dataclasses import dataclass
from datetime import datetime

# Type definitions for domain-specific types
try:
    from multiformats.cid import CID as CIDType
except ImportError:
    CIDType = NewType('CID', str)

# Type for timestamp fields that could be different formats
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
class AkaveBlockData:
    """Akavenode block metadata."""
    permit: str
    node_address: str
    node_id: str


@dataclass
class FilecoinBlockData:
    """Filecoin block metadata."""
    base_url: str


@dataclass
class FileBlockUpload:
    """A piece of metadata of some file used for upload."""
    cid: str
    data: bytes
    node_address: str = ""
    node_id: str = ""
    permit: bytes = b""

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
    filecoin: Optional[FilecoinBlockData] = None
    akave: Optional[AkaveBlockData] = None


@dataclass
class FileListItem:
    """Contains bucket file list file meta information."""
    root_cid: str
    name: str
    size: int
    created_at: TimestampType


@dataclass
class FileUpload:
    """Contains single file meta information."""
    bucket_name: str
    name: str
    stream_id: str
    created_at: TimestampType


@dataclass
class FileChunkUpload:
    """Contains single file chunk meta information."""
    stream_id: str
    index: int
    chunk_cid: Any  # Now using proper CID type
    actual_size: int
    raw_data_size: int
    proto_node_size: int
    blocks: List[FileBlockUpload]


@dataclass
class FileDownload:
    """Contains single file meta information."""
    stream_id: str
    bucket_name: str
    name: str
    chunks: List[Chunk]


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


@dataclass
class IPCBucketCreateResult:
    """Result of IPC bucket creation."""
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
    created_at: TimestampType


@dataclass
class IPCFileMeta:
    """Contains single IPC file meta information."""
    root_cid: str
    name: str
    bucket_name: str
    encoded_size: int
    created_at: TimestampType


@dataclass
class IPCFileMetaV2:
    """Contains single file meta information."""
    root_cid: str
    bucket_name: str
    encoded_size: int
    size: int = 0
    created_at: TimestampType = None
    committed_at: Optional[TimestampType] = None


@dataclass
class IPCFileChunkUploadV2:
    """Contains single file chunk meta information."""
    index: int
    chunk_cid: CIDType  # Now using proper CID type
    actual_size: int
    raw_data_size: int
    proto_node_size: int
    blocks: List[FileBlockUpload]
    bucket_id: bytes  # 32-byte array in Go, using bytes in Python
    file_name: str
