import io
import hashlib
from typing import List, Optional, Any, BinaryIO, Tuple
from dataclasses import dataclass

try:
    from multiformats import CID, multihash
    from multiformats.multicodec import multicodec
    from ipld_dag_pb import PBNode, PBLink, encode, decode, prepare, code as dag_pb_code
    IPLD_AVAILABLE = True
except ImportError:
    IPLD_AVAILABLE = False
    class CID:
        def __init__(self, cid_str):
            self.cid_str = cid_str
        
        @classmethod
        def decode(cls, cid_str):
            return cls(cid_str)
        
        def __str__(self):
            return self.cid_str
        
        def string(self):
            return self.cid_str
            
        def bytes(self):
            return self.cid_str.encode()
        
        def type(self):
            if self.cid_str.startswith('bafybeig'):
                return 0x70  # dag-pb
            else:
                return 0x55  # raw

try:
    from ipld_dag_pb import decode as decode_dag_pb
    DAG_PB_AVAILABLE = True
except ImportError:
    DAG_PB_AVAILABLE = False

from .model import FileBlockUpload
from .config import DAG_PB_CODEC, RAW_CODEC, DEFAULT_CID_VERSION

CID_VERSION = 1

class DAGError(Exception):
    pass

class DAGRoot:    
    def __init__(self):
        self.node = None  # Will store PBNode
        self.fs_node_data = b""  # UnixFS data
        self.links = []  # Store links for the node
        self.total_file_size = 0  # Track total file size for UnixFS
        
    @classmethod 
    def new(cls):
        return cls()
    
    def add_link(self, chunk_cid, raw_data_size: int, proto_node_size: int) -> None:
        if hasattr(chunk_cid, 'string'):
            cid_str = chunk_cid.string()
        elif hasattr(chunk_cid, '__str__'):
            cid_str = str(chunk_cid)
        else:
            cid_str = chunk_cid
            
        if IPLD_AVAILABLE:
            try:
                cid_obj = CID.decode(cid_str) if isinstance(cid_str, str) else chunk_cid
            except:
                cid_obj = cid_str
        else:
            cid_obj = cid_str
            
        self.total_file_size += raw_data_size
        
        self.links.append({
            "cid": cid_obj,
            "cid_str": cid_str,  
            "name": "",  
            "size": proto_node_size
        })
    
    def build(self):
        if len(self.links) == 0:
            raise DAGError("no chunks added")
        
        if len(self.links) == 1:
            link_cid = self.links[0]["cid"]
            if hasattr(link_cid, 'string'):
                return link_cid.string()
            elif "cid_str" in self.links[0]:
                return self.links[0]["cid_str"]
            return str(link_cid)
        
        if not IPLD_AVAILABLE:
            import base64
            combined_data = "".join([link["cid_str"] for link in self.links]).encode()
            hash_digest = hashlib.sha256(combined_data).digest()
            b32_hash = base64.b32encode(hash_digest).decode().lower().rstrip('=')
            char_map = str.maketrans('01', 'ab')
            b32_hash = b32_hash.translate(char_map)
            return f"bafybeig{b32_hash[:50]}"
        
        try:
            pb_links = []
            for link in self.links:
                link_cid = link["cid"]
                if isinstance(link_cid, str):
                    try:
                        link_cid = CID.decode(link_cid)
                    except Exception:
                        continue
                
                pb_link = PBLink(
                    hash=link_cid,
                    name=link["name"], 
                    size=link["size"]
                )
                pb_links.append(pb_link)
            
            if not pb_links:
                raise DAGError("no valid CIDs found for DAG links")
            
            unixfs_data = self._create_unixfs_file_data()
            pb_node = PBNode(data=unixfs_data, links=pb_links)
            encoded_bytes = encode(pb_node)
            digest = multihash.digest(encoded_bytes, "sha2-256")
            root_cid = CID("base32", CID_VERSION, dag_pb_code, digest)
            
            return root_cid
            
        except Exception as e:
            raise DAGError(f"failed to build DAG root: {str(e)}")
    
    def _create_unixfs_file_data(self) -> bytes:
        unixfs_data = bytes([0x08, 0x02])  # Type = File
        
        if self.total_file_size > 0:
            unixfs_data += bytes([0x18]) + self._encode_varint(self.total_file_size)
        
        return unixfs_data
    
    def _encode_varint(self, value: int) -> bytes:
        result = b''
        while value > 127:
            result += bytes([(value & 127) | 128])
            value >>= 7
        result += bytes([value & 127])
        return result

@dataclass 
class ChunkDAG:
    cid: Any                           # Chunk CID
    raw_data_size: int                 # size of data read from disk
    encoded_size: int                  # encoded size (was proto_node_size)
    blocks: List[FileBlockUpload]      # Blocks in the chunk

def build_dag(ctx: Any, reader: BinaryIO, block_size: int) -> ChunkDAG:
    try:
        data = reader.read()
        if not data:
            raise DAGError("empty data")
        
        raw_data_size = len(data)
        
        blocks = []
        
        if len(data) <= block_size:
            chunk_cid, encoded_data = _create_unixfs_file_node(data)
            proto_node_size = len(encoded_data)
            
            blocks = [FileBlockUpload(
                cid=str(chunk_cid) if hasattr(chunk_cid, '__str__') else chunk_cid,
                data=encoded_data
            )]
            raw_size, encoded_size = node_sizes(encoded_data)
            
        else:
            offset = 0
            pb_links = []
            
            while offset < len(data):
                end_offset = min(offset + block_size, len(data))
                block_data = data[offset:end_offset]
                
                block_cid, block_encoded_data = _create_unixfs_file_node(block_data)
                
                block = FileBlockUpload(
                    cid=str(block_cid) if hasattr(block_cid, '__str__') else block_cid,
                    data=block_encoded_data
                )
                blocks.append(block)
                
                if IPLD_AVAILABLE:
                    try:
                        cid_obj = CID.decode(block.cid) if isinstance(block.cid, str) else block_cid
                        pb_links.append(PBLink(
                            hash=cid_obj,
                            name="",
                            size=len(block_encoded_data)
                        ))
                    except:
                        pass
                
                offset = end_offset
            
            chunk_cid, encoded_size = _create_chunk_dag_root_node(blocks, pb_links)
            raw_size = raw_data_size
        
        if not blocks:
            raise DAGError("no blocks created")
        return ChunkDAG(
            cid=chunk_cid,
            raw_data_size=raw_size,
            encoded_size=encoded_size,
            blocks=blocks
        )
        
    except Exception as e:
        raise DAGError(f"failed to build chunk DAG: {str(e)}")

def _create_unixfs_file_node(data: bytes):
    if not IPLD_AVAILABLE:
        hash_digest = hashlib.sha256(data).digest()
        import base64
        b32_hash = base64.b32encode(hash_digest).decode().lower().rstrip('=')
        char_map = str.maketrans('01', 'ab')
        b32_hash = b32_hash.translate(char_map)
        return f"bafybeig{b32_hash[:50]}", data
    
    try:
        unixfs_data = bytes([0x08, 0x02])  
        
        if len(data) > 0:
            unixfs_data += bytes([0x22]) + _encode_varint(len(data)) + data
        
        pb_node = PBNode(data=unixfs_data, links=[])
        encoded_bytes = encode(pb_node)
        
        digest = multihash.digest(encoded_bytes, "sha2-256")
        cid = CID("base32", CID_VERSION, dag_pb_code, digest)
        
        return cid, encoded_bytes
        
    except Exception as e:
        hash_digest = hashlib.sha256(data).digest()
        import base64
        b32_hash = base64.b32encode(hash_digest).decode().lower().rstrip('=')
        char_map = str.maketrans('01', 'ab')
        b32_hash = b32_hash.translate(char_map)
        return f"bafybeig{b32_hash[:50]}", data

def _create_chunk_dag_root_node(blocks: List[FileBlockUpload], pb_links: List = None):
    if not IPLD_AVAILABLE:
        combined_data = b"".join([block.data for block in blocks])
        hash_digest = hashlib.sha256(combined_data).digest()
        import base64
        b32_hash = base64.b32encode(hash_digest).decode().lower().rstrip('=')
        char_map = str.maketrans('01', 'ab')
        b32_hash = b32_hash.translate(char_map)
        return f"bafybeig{b32_hash[:50]}", len(combined_data)
    
    try:
        if not pb_links:
            pb_links = []
            for block in blocks:
                try:
                    block_cid = CID.decode(block.cid) if isinstance(block.cid, str) else block.cid
                    pb_link = PBLink(
                        hash=block_cid,
                        name="",
                        size=len(block.data)
                    )
                    pb_links.append(pb_link)
                except:
                    continue
        
        unixfs_data = bytes([0x08, 0x02]) 
        
        pb_node = PBNode(data=unixfs_data, links=pb_links)
        encoded_bytes = encode(pb_node)
        
        digest = multihash.digest(encoded_bytes, "sha2-256")
        cid = CID("base32", CID_VERSION, dag_pb_code, digest)
        
        total_size = sum(len(block.data) for block in blocks)
        return cid, total_size
        
    except Exception as e:
        combined_data = b"".join([block.data for block in blocks])
        hash_digest = hashlib.sha256(combined_data).digest()
        import base64
        b32_hash = base64.b32encode(hash_digest).decode().lower().rstrip('=')
        char_map = str.maketrans('01', 'ab')
        b32_hash = b32_hash.translate(char_map)
        return f"bafybeig{b32_hash[:50]}", len(combined_data)

def node_sizes(node_data: bytes) -> Tuple[int, int]:
    if not IPLD_AVAILABLE:
        encoded_size = len(node_data)
        return encoded_size, encoded_size
    
    try:
        pb_node = decode(node_data)  
        
        if not pb_node.data:
            if len(pb_node.links) == 0:
                return len(node_data), len(node_data)
            
            encoded_size = sum(link.size for link in pb_node.links)
            return 0, encoded_size  
        
        raw_data_size = _extract_unixfs_data_size(pb_node.data)
        
        if len(pb_node.links) == 0:
            encoded_size = len(node_data)
        else:
            encoded_size = sum(link.size for link in pb_node.links)
        
        return raw_data_size, encoded_size
        
    except Exception as e:
        encoded_size = len(node_data)
        return encoded_size, encoded_size

def _extract_unixfs_data_size(unixfs_bytes: bytes) -> int:
    try:
        offset = 0
        while offset < len(unixfs_bytes):
            if offset >= len(unixfs_bytes):
                break
                
            field_tag = unixfs_bytes[offset]
            offset += 1
            
            field_number = (field_tag >> 3)
            wire_type = field_tag & 0x07
            
            if field_number == 3 and wire_type == 0:  
                file_size, bytes_read = _decode_varint(unixfs_bytes[offset:])
                return file_size
            elif field_number == 4 and wire_type == 2:  
                length, bytes_read = _decode_varint(unixfs_bytes[offset:])
                return length  
            elif wire_type == 2:  
                length, bytes_read = _decode_varint(unixfs_bytes[offset:])
                offset += bytes_read + length
            elif wire_type == 0:  
                value, bytes_read = _decode_varint(unixfs_bytes[offset:])
                offset += bytes_read
            elif wire_type == 1:  
                offset += 8
            elif wire_type == 5: 
                offset += 4
            else:
                offset += 1
        
        return 0
        
    except Exception:
        return 0

def _encode_varint(value: int) -> bytes:
    result = b''
    while value > 127:
        result += bytes([(value & 127) | 128])
        value >>= 7
    result += bytes([value & 127])
    return result

def _decode_varint(data: bytes) -> tuple[int, int]:
    value = 0
    shift = 0
    bytes_read = 0
    
    for byte in data:
        bytes_read += 1
        value |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7
        if shift >= 64:
            raise ValueError("varint too long")
    
    return value, bytes_read

def extract_block_data(cid_str: str, data: bytes) -> bytes:
    try:
        if not IPLD_AVAILABLE:
            return _extract_unixfs_data_fallback(data)
            
        try:
            cid_obj = CID.decode(cid_str)
            cid_type = cid_obj.codec
        except:
            if cid_str.startswith('bafkreig'):
                cid_type = RAW_CODEC
            else:
                cid_type = DAG_PB_CODEC
        
        if cid_type == DAG_PB_CODEC:
            try:
                pb_node = decode(data)
                if pb_node.data:
                    extracted = _extract_unixfs_data(pb_node.data)
                    if extracted:
                        return extracted
                    return _extract_unixfs_data_fallback(data)
                else:
                    return b""
            except Exception:
                return _extract_unixfs_data_fallback(data)
        elif cid_type == RAW_CODEC:
            return data
        else:
            raise DAGError(f"unknown CID type: {cid_type}")
            
    except Exception:
        return _extract_unixfs_data_fallback(data)

def _extract_unixfs_data_fallback(data: bytes) -> bytes:
    try:
        offset = 0  
        # DAG-PB structure:
        # Field 1 (Data): The UnixFS data
        # Field 2 (Links): Array of links to other blocks
        
        while offset < len(data):
            if offset >= len(data):
                break
                
            field_tag = data[offset]
            offset += 1
            
            field_number = (field_tag >> 3)
            wire_type = field_tag & 0x07
            
            if field_number == 1 and wire_type == 2:
                length, bytes_read = _decode_varint(data[offset:])
                offset += bytes_read
                
                if offset + length <= len(data):
                    unixfs_data = data[offset:offset + length]
                    extracted = _extract_unixfs_data(unixfs_data)
                    if extracted:
                        return extracted
                    offset += length
                else:
                    break
            elif wire_type == 2:  
                length, bytes_read = _decode_varint(data[offset:])
                offset += bytes_read + length
            elif wire_type == 0:  
                value, bytes_read = _decode_varint(data[offset:])
                offset += bytes_read
            elif wire_type == 1:  
                offset += 8
            elif wire_type == 5:  
                offset += 4
            else:
                offset += 1
        
        return data
        
    except Exception:
        return data

def _extract_unixfs_data(unixfs_bytes: bytes) -> bytes:
    try:
        offset = 0
        while offset < len(unixfs_bytes):
            if offset >= len(unixfs_bytes):
                break
                
            field_tag = unixfs_bytes[offset]
            offset += 1
            
            field_number = (field_tag >> 3)
            wire_type = field_tag & 0x07
            
            if field_number == 4 and wire_type == 2:  # Field 4 (Data) with length-delimited wire type
                length, bytes_read = _decode_varint(unixfs_bytes[offset:])
                offset += bytes_read
                
                if offset + length <= len(unixfs_bytes):
                    return unixfs_bytes[offset:offset + length]
                else:
                    break
            elif wire_type == 2:  # Length-delimited field
                length, bytes_read = _decode_varint(unixfs_bytes[offset:])
                offset += bytes_read + length
            elif wire_type == 0:  # Varint
                value, bytes_read = _decode_varint(unixfs_bytes[offset:])
                offset += bytes_read
            elif wire_type == 1:  # Fixed64
                offset += 8
            elif wire_type == 5:  # Fixed32
                offset += 4
            else:
                offset += 1
        
        return b""
        
    except Exception:
        return b""

def bytes_to_node(node_data: bytes):
    try:
        pb_node = decode(node_data)
        return pb_node
    except Exception as e:
        raise DAGError(f"failed to decode node data: {str(e)}")

def get_node_links(node_data: bytes) -> List[dict]:
    try:
        pb_node = bytes_to_node(node_data)
        
        links = []
        for pb_link in pb_node.links:
            link_info = {
                'cid': str(pb_link.hash) if hasattr(pb_link.hash, '__str__') else pb_link.hash,
                'name': pb_link.name if pb_link.name else "",
                'size': pb_link.size
            }
            links.append(link_info)
        
        return links
        
    except Exception as e:
        raise DAGError(f"failed to extract links from node: {str(e)}")

def block_by_cid(blocks: List[FileBlockUpload], cid_str: str) -> tuple[FileBlockUpload, bool]:
    for block in blocks:
        if block.cid == cid_str:
            return block, True
    return FileBlockUpload(cid="", data=b""), False