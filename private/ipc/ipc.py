from __future__ import annotations

import secrets
from dataclasses import dataclass
from typing import Dict, List, Any

from eth_utils import keccak, to_bytes, to_checksum_address

try:
    from multiformats import CID
    MULTIFORMATS_AVAILABLE = True
except ImportError:
    MULTIFORMATS_AVAILABLE = False
    CID = None

try:
    from ..eip712 import Domain as EIP712Domain, TypedData as EIP712TypedData, sign as eip712_sign
except ImportError:
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from eip712 import Domain as EIP712Domain, TypedData as EIP712TypedData, sign as eip712_sign


@dataclass
class StorageData:
    chunk_cid: bytes
    block_cid: bytes  
    chunk_index: int
    block_index: int  
    node_id: bytes   
    nonce: int
    deadline: int
    bucket_id: bytes  

    def to_message_dict(self) -> Dict[str, Any]:
        return {
            "chunkCID": self.chunk_cid,
            "blockCID": self.block_cid,
            "chunkIndex": self.chunk_index,
            "blockIndex": self.block_index,
            "nodeId": self.node_id,
            "nonce": self.nonce,
            "deadline": self.deadline,
            "bucketId": self.bucket_id,
        }


def generate_nonce() -> int:
    return int.from_bytes(secrets.token_bytes(32), byteorder="big")


def calculate_file_id(bucket_id: bytes, name: str) -> bytes:
    if not isinstance(bucket_id, (bytes, bytearray)):
        raise TypeError("bucket_id must be bytes")
    
    data = bucket_id + name.encode('utf-8')
    return keccak(data)


def calculate_bucket_id(bucket_name: str, address: str) -> bytes:
    data = bucket_name.encode('utf-8')
    
    addr = address.lower()
    if addr.startswith("0x"):
        addr = addr[2:]
    if len(addr) != 40:
        raise ValueError("address must be a 20-byte hex string")
    
    address_bytes = bytes.fromhex(addr)
    data += address_bytes
    
    return keccak(data)


def from_byte_array_cid(data: bytes) -> 'CID':
    if not MULTIFORMATS_AVAILABLE:
        raise ImportError("multiformats library is required")
    
    if len(data) != 32:
        raise ValueError(f"expected 32 bytes, got {len(data)}")
    
    # CID v1, dag-pb (0x70), sha2-256 (0x12)
    # Construct CID bytes: version + codec + multihash
    # multihash format: hash_type (0x12 for sha2-256) + length (0x20 = 32) + hash_data
    multicodec_dagpb = bytes([0x70])
    multihash_sha256_header = bytes([0x12, 0x20])
    cid_bytes = bytes([0x01]) + multicodec_dagpb + multihash_sha256_header + data
    
    return CID.decode(cid_bytes)


def sign_block(private_key_hex: str, storage_address: str, chain_id: int, data: StorageData) -> bytes:
    key_hex = private_key_hex.lower()
    if key_hex.startswith("0x"):
        key_hex = key_hex[2:]
    private_key_bytes = bytes.fromhex(key_hex)

    checksum_addr = to_checksum_address(storage_address)

    domain = EIP712Domain(
        name="Storage",
        version="1",
        chain_id=chain_id,
        verifying_contract=checksum_addr,
    )

    storage_data_types: Dict[str, List[EIP712TypedData]] = {
        "StorageData": [
            EIP712TypedData("chunkCID", "bytes"),
            EIP712TypedData("blockCID", "bytes32"),
            EIP712TypedData("chunkIndex", "uint256"),
            EIP712TypedData("blockIndex", "uint256"),
            EIP712TypedData("nodeId", "bytes32"),
            EIP712TypedData("nonce", "uint256"),
            EIP712TypedData("deadline", "uint256"),
            EIP712TypedData("bucketId", "bytes32"),
        ]
    }

    message = data.to_message_dict()

    return eip712_sign(private_key_bytes, domain, "StorageData", storage_data_types, message)