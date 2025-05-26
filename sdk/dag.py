import io
import os
from typing import List, Tuple, Dict, Optional, Any, BinaryIO
from dataclasses import dataclass
from ipld_dag_pb import PBNode, PBLink, encode, decode, code
from multiformats import multihash, CID
from ..private.encryption.encryption import encrypt, decrypt

from .model import FileBlockUpload

class DAGError(Exception):
    pass

DEFAULT_CID_VERSION = 1
DEFAULT_HASH_FUNC = "sha2-256"

class DAGRoot:
    
    def __init__(self) -> None:
        self.links: List[PBLink] = []  # Format: [PBLink objects]
        self.data_size: int = 0  # Total raw data size
        
    def add_link(self, cid_str: str, raw_data_size: int, proto_node_size: int) -> None:
        self.data_size += raw_data_size
        
        cid_obj = CID.decode(cid_str)
        
        link = PBLink(
            name="",  
            size=proto_node_size,
            hash=cid_obj
        )
        
        self.links.append(link)
        
    def build(self) -> str:
        if not self.links:
            raise DAGError("No chunks added")
            
        if len(self.links) == 1:
            # If there's only one link, just return its CID
            # Note: Here hash is actually the CID of the single node
            return str(self.links[0].hash)
            
        root_node = PBNode(data=None, links=self.links)
        encoded_node = encode(root_node)
        digest = multihash.digest(encoded_node, DEFAULT_HASH_FUNC)
        root_cid = CID("base32", DEFAULT_CID_VERSION, code, digest)
        return str(root_cid)

@dataclass
class ChunkDAG:
    cid: str
    raw_data_size: int  
    proto_node_size: int  
    blocks: List[FileBlockUpload]

def chunk_data(reader: BinaryIO, block_size: int) -> List[bytes]:
    chunks: List[bytes] = []
    while True:
        chunk = reader.read(block_size)
        if not chunk:
            break
        chunks.append(chunk)
    return chunks

def build_dag(ctx: Any, reader: BinaryIO, block_size: int, enc_key: Optional[bytes] = None) -> ChunkDAG:
    chunks: List[bytes] = chunk_data(reader, block_size)
    blocks: List[FileBlockUpload] = []

    total_raw_size: int = 0
    total_proto_size: int = 0

    for i, chunk in enumerate(chunks):
        if enc_key:
            nonce = os.urandom(12)
            chunk = encrypt(enc_key, chunk, nonce)
        
        node = PBNode(data=chunk)
        encoded_node = encode(node)
        digest = multihash.digest(encoded_node, DEFAULT_HASH_FUNC)
        block_cid = CID("base32", DEFAULT_CID_VERSION, code, digest)
        chunk_size = len(chunk)
        total_raw_size += chunk_size
        total_proto_size += len(encoded_node)
        blocks.append(FileBlockUpload(
            cid=str(block_cid),
            data=encoded_node
        ))
    
    if not blocks:
        raise DAGError("No blocks created, file may be empty")
    
    if len(blocks) == 1:
        root_cid = blocks[0].cid
        proto_node_size = len(blocks[0].data)
    else:
        dag_root = DAGRoot()
        for block in blocks:
            raw_size, proto_size = node_sizes(block.data)
            dag_root.add_link(block.cid, raw_size, proto_size)
        
        root_cid = dag_root.build()
        proto_node_size = total_proto_size
    
    return ChunkDAG(
        cid=root_cid,
        raw_data_size=total_raw_size,
        proto_node_size=proto_node_size,
        blocks=blocks
    )

def extract_block_data(id_str: str, data: bytes) -> bytes:
    try:
        cid_obj = CID.decode(id_str)
    except Exception as e:
        raise ValueError(f"Invalid CID: {e}")

    if cid_obj.codec == code:  # dag-pb code
        try:
            node = decode(data)
            return node.data if node.data is not None else b""
        except Exception as e:
            raise ValueError(f"Failed to decode DAG node: {e}")
    elif cid_obj.codec == 0x55:  # raw codec
        return data
    else:
        raise ValueError(f"Unknown CID codec: {cid_obj.codec}")

def block_by_cid(blocks: List[FileBlockUpload], cid_str: str) -> Tuple[FileBlockUpload, bool]:
    for block in blocks:
        if block.cid == cid_str:
            return block, True
    return FileBlockUpload(cid="", data=b""), False

def node_sizes(node_data: bytes) -> Tuple[int, int]:
    try:
        node = decode(node_data)
        raw_data_size = len(node.data) if node.data is not None else 0
        proto_node_size = len(node_data)
        return raw_data_size, proto_node_size
    except Exception as e:
        raise DAGError(f"Failed to calculate node sizes: {str(e)}")

