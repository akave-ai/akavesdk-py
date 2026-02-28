# Copyright (C) 2025 Akave
# See LICENSE for copying information.

from dataclasses import dataclass
from typing import List

try:
    from multiformats import CID
    MULTIFORMATS_AVAILABLE = True
except ImportError:
    MULTIFORMATS_AVAILABLE = False
    CID = None


CONTRACT_METHOD_SIGNATURE_LEN = 4


@dataclass
class AddChunkTransactionData:
    cid: 'CID'
    bucket_id: bytes
    file_name: str
    encoded_size: int
    block_cids: List['CID']
    block_sizes: List[int]
    index: int


def from_byte_array_cid(data: bytes) -> 'CID':
    if not MULTIFORMATS_AVAILABLE:
        raise ImportError("multiformats library is required")
    
    if len(data) != 32:
        raise ValueError(f"expected 32 bytes, got {len(data)}")
    
    multicodec_dagpb = bytes([0x70])
    multihash_sha256_header = bytes([0x12, 0x20])
    cid_bytes = bytes([0x01]) + multicodec_dagpb + multihash_sha256_header + data
    
    return CID.decode(cid_bytes)


def parse_add_chunk_tx(storage_contract_abi: dict, tx_data: bytes) -> AddChunkTransactionData:
    if len(tx_data) < CONTRACT_METHOD_SIGNATURE_LEN:
        raise ValueError("invalid transaction data length")
    
    method_abi = None
    for item in storage_contract_abi:
        if item.get('type') == 'function' and item.get('name') == 'addFileChunk':
            method_abi = item
            break
    
    if not method_abi:
        raise ValueError("method addFileChunk not found in ABI")
    
    from web3 import Web3
    contract = Web3().eth.contract(abi=[method_abi])
    
    fn, params = contract.decode_function_input(tx_data)
    
    chunk_cid_bytes = params['chunkCID']
    if not MULTIFORMATS_AVAILABLE:
        raise ImportError("multiformats library is required")
    chunk_cid = CID.decode(chunk_cid_bytes)
    
    bucket_id = params['bucketId']
    file_name = params['fileName']
    encoded_size = int(params['encodedChunkSize'])
    block_cid_arrays = params['cids']
    block_sizes_bigint = params['chunkBlocksSizes']
    chunk_index = int(params['chunkIndex'])
    
    block_cids = []
    for b in block_cid_arrays:
        block_cids.append(from_byte_array_cid(b))
    
    block_sizes = [int(size) for size in block_sizes_bigint]
    
    if len(block_cids) != len(block_sizes):
        raise ValueError("mismatched block CIDs and sizes lengths")
    
    return AddChunkTransactionData(
        cid=chunk_cid,
        bucket_id=bucket_id,
        file_name=file_name,
        encoded_size=encoded_size,
        block_cids=block_cids,
        block_sizes=block_sizes,
        index=chunk_index,
    )


def parse_add_chunks_tx(storage_contract_abi: dict, tx_data: bytes) -> List[AddChunkTransactionData]:
    if len(tx_data) < CONTRACT_METHOD_SIGNATURE_LEN:
        raise ValueError("invalid transaction data length")
    
    method_abi = None
    for item in storage_contract_abi:
        if item.get('type') == 'function' and item.get('name') == 'addFileChunks':
            method_abi = item
            break
    
    if not method_abi:
        raise ValueError("method addFileChunks not found in ABI")
    
    from web3 import Web3
    contract = Web3().eth.contract(abi=[method_abi])
    
    fn, params = contract.decode_function_input(tx_data)
    
    chunk_cids_bytes = params['cids']
    bucket_id = params['bucketId']
    file_name = params['fileName']
    encoded_chunk_sizes = params['encodedChunkSizes']
    chunk_blocks_cids = params['chunkBlocksCIDs']
    chunk_blocks_sizes = params['chunkBlockSizes']
    starting_chunk_index = int(params['startingChunkIndex'])
    
    num_chunks = len(chunk_cids_bytes)
    chunks = []
    
    for i in range(num_chunks):
        if not MULTIFORMATS_AVAILABLE:
            raise ImportError("multiformats library is required")
        chunk_cid = CID.decode(chunk_cids_bytes[i])
        
        block_cids = []
        for b in chunk_blocks_cids[i]:
            block_cids.append(from_byte_array_cid(b))
        
        block_sizes = [int(size) for size in chunk_blocks_sizes[i]]
        
        if len(block_cids) != len(block_sizes):
            raise ValueError(f"mismatched block CIDs and sizes for chunk {i}")
        
        chunks.append(AddChunkTransactionData(
            cid=chunk_cid,
            bucket_id=bucket_id,
            file_name=file_name,
            encoded_size=int(encoded_chunk_sizes[i]),
            block_cids=block_cids,
            block_sizes=block_sizes,
            index=starting_chunk_index + i,
        ))
    
    return chunks
