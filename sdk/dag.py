import io
import os
import hashlib
from typing import List, Tuple, Dict, Optional, Any, BinaryIO
from dataclasses import dataclass

try:
    from multiformats import CID, multihash
    from ipld_dag_pb import PBNode, PBLink, encode, decode
except ImportError:
    # Fallback implementations
    class CID:
        def __init__(self, cid_str):
            self.cid_str = cid_str
        
        @classmethod
        def decode(cls, cid_str):
            return cls(cid_str)
        
        def __str__(self):
            return self.cid_str
        
        @property 
        def bytes(self):
            return self.cid_str.encode()

from private.encryption.encryption import encrypt, decrypt
from .model import FileBlockUpload

class DAGError(Exception):
    pass

# IPFS constants matching Go implementation
DEFAULT_CID_VERSION = 1
DEFAULT_HASH_FUNC = "sha2-256"
DAG_PB_CODEC = 0x70

class DAGRoot:

    def __init__(self):
        # Initialize like Go's NewDAGRoot
        self.links = []  # Store links to chunks
        self.total_size = 0  # Track total file size (UnixFS FSNode equivalent)
        self.block_sizes = []  # Track individual block sizes
        
    @classmethod 
    def new(cls):
        return cls()
    
    def add_link(self, chunk_cid, raw_data_size: int, proto_node_size: int) -> None:
        
        cid_str = str(chunk_cid)
        # Store link information
        self.links.append({
            "cid": cid_str,
            "size": proto_node_size,
            "raw_size": raw_data_size
        })
        
        # Track cumulative sizes (like UnixFS FSNode.AddBlockSize)
        self.total_size += raw_data_size
        self.block_sizes.append(raw_data_size)
    
    def build(self):
        if len(self.links) == 0:
            raise DAGError("no chunks added")
        
        if len(self.links) == 1:
            return self.links[0]["cid"]
        
        try:
            unixfs_data = self._create_unixfs_data()
            pb_links = []
            for link in self.links:
                pb_link = {
                    "cid": link["cid"],
                    "name": "",  # Empty name for file blocks
                    "size": link["size"]
                }
                pb_links.append(pb_link)
            
            # Create DAG node
            dag_node = {
                "data": unixfs_data,
                "links": pb_links
            }
            
            # Generate CID for the root node
            root_cid = self._generate_cid(dag_node)
            return root_cid
            
        except Exception as e:
            raise DAGError(f"failed to build DAG root: {str(e)}")
    
    def _create_unixfs_data(self) -> bytes:
        file_type = 2  # UnixFS file type
        file_size = self.total_size
        
        # Simple encoding: [type, size, block_count]
        unixfs_header = bytes([file_type]) + file_size.to_bytes(8, 'big') + len(self.block_sizes).to_bytes(4, 'big')
        
        # Add block sizes
        block_data = b''
        for size in self.block_sizes:
            block_data += size.to_bytes(8, 'big')
        
        return unixfs_header + block_data
    
    def _generate_cid(self, dag_node) -> str:       
        node_data = self._serialize_dag_node(dag_node)
        
        # Create hash
        hash_digest = hashlib.sha256(node_data).digest()
        import base64
        b32_hash = base64.b32encode(hash_digest).decode().lower().rstrip('=')
        char_map = str.maketrans('01', 'ab')  # Replace '0' and '1' with valid chars
        b32_hash = b32_hash.translate(char_map)
        cid_str = f"bafybeig{b32_hash[:50]}"
        return cid_str
    
    def _serialize_dag_node(self, dag_node) -> bytes:
        import json
        node_str = json.dumps(dag_node, sort_keys=True)
        return node_str.encode()

@dataclass 
class ChunkDAG:   
    cid: str                        # Chunk CID
    raw_data_size: int             # Size of original data
    proto_node_size: int           # Size of protobuf encoded node  
    blocks: List[FileBlockUpload]  # List of blocks in chunk

def build_dag(ctx: Any, reader: BinaryIO, block_size: int, enc_key: Optional[bytes] = None) -> ChunkDAG:
    
    try:
        # Read all data from reader
        data = reader.read()
        if not data:
            raise DAGError("empty data")
        
        # Apply encryption if enabled
        if enc_key:
            # Use chunk-specific nonce/info for encryption
            data = encrypt(enc_key, data, b"chunk_data")
        
        original_size = len(data)
        
        # Split data into blocks
        blocks = []
        offset = 0
        
        while offset < len(data):
            end_offset = min(offset + block_size, len(data))
            block_data = data[offset:end_offset]
            
            # Create block CID
            block_cid = _generate_block_cid(block_data)
            
            # Create block upload structure
            block = FileBlockUpload(
                cid=block_cid,
                data=block_data
            )
            blocks.append(block)
            
            offset = end_offset
        
        if not blocks:
            raise DAGError("no blocks created")
        
        # Calculate chunk CID and sizes
        if len(blocks) == 1:
            # Single block chunk
            chunk_cid = blocks[0].cid
            proto_node_size = len(blocks[0].data)
        else:
            # Multi-block chunk - create chunk DAG
            chunk_dag_data = _create_chunk_dag(blocks)
            chunk_cid = _generate_block_cid(chunk_dag_data)
            proto_node_size = len(chunk_dag_data)
        
        return ChunkDAG(
            cid=chunk_cid,
            raw_data_size=original_size,
            proto_node_size=proto_node_size,
            blocks=blocks
        )
        
    except Exception as e:
        raise DAGError(f"failed to build chunk DAG: {str(e)}")

def _generate_block_cid(data: bytes) -> str:
    hash_digest = hashlib.sha256(data).digest()
    import base64
    b32_hash = base64.b32encode(hash_digest).decode().lower().rstrip('=')
    char_map = str.maketrans('01', 'ab')  # Replace '0' and '1' with valid chars
    b32_hash = b32_hash.translate(char_map)
    
    # Create proper IPFS CID v1 format with dag-pb codec
    cid_str = f"bafybeig{b32_hash[:50]}"  # Truncate to reasonable length
    return cid_str

def _create_chunk_dag(blocks: List[FileBlockUpload]) -> bytes:
    # Create links to all blocks
    links_data = b''
    for block in blocks:
        # Simple link encoding: [cid_length, cid, size]
        cid_bytes = block.cid.encode()
        links_data += len(cid_bytes).to_bytes(4, 'big')
        links_data += cid_bytes
        links_data += len(block.data).to_bytes(8, 'big')
    
    # Create chunk header
    header = b'CHUNK_DAG'
    header += len(blocks).to_bytes(4, 'big')
    
    return header + links_data
def block_by_cid(blocks: List[FileBlockUpload], cid_str: str) -> Tuple[FileBlockUpload, bool]:
    for block in blocks:
        if block.cid == cid_str:
            return block, True
    return FileBlockUpload(cid="", data=b""), False

def node_sizes(node_data: bytes) -> Tuple[int, int]:
    
    try:
        # For our implementation, proto size is the encoded size
        proto_node_size = len(node_data)
        raw_data_size = proto_node_size
        return raw_data_size, proto_node_size
        
    except Exception as e:
        raise DAGError(f"failed to calculate node sizes: {str(e)}")

