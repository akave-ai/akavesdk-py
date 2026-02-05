import math
from reedsolo import RSCodec, ReedSolomonError
from itertools import combinations

def missing_shards_idx(n, k):
    return [list(combo) for combo in combinations(range(n), k)]

def split_into_blocks(encoded: bytes, shard_size: int):
    blocks = []
    for offset in range(0, len(encoded), shard_size):
        block = encoded[offset: offset + shard_size]
        if len(block) < shard_size:
            block = block.ljust(shard_size, b'\x00')
        blocks.append(block)
    return blocks

class ErasureCode:
    def __init__(self, data_blocks: int, parity_blocks: int):
        if data_blocks <= 0 or parity_blocks <= 0:
            raise ValueError("Data and parity shards must be > 0")
        self.data_blocks = data_blocks
        self.parity_blocks = parity_blocks
        self.total_shards = data_blocks + parity_blocks

    @classmethod
    def new(cls, data_blocks: int, parity_blocks: int):
        return cls(data_blocks, parity_blocks)

    def encode(self, data: bytes) -> bytes:
        total_size = len(data)
        shard_size = math.ceil(total_size / self.data_blocks)
        padded_data = data.ljust(self.data_blocks * shard_size, b'\x00')
        
        shards = [bytearray(padded_data[i * shard_size : (i + 1) * shard_size]) 
                  for i in range(self.data_blocks)]
        parity_shards = [bytearray(shard_size) for _ in range(self.parity_blocks)]
        rsc = RSCodec(self.parity_blocks)
        
        for j in range(shard_size):
            message = bytearray(self.data_blocks)
            for i in range(self.data_blocks):
                message[i] = shards[i][j]
            
            encoded_msg = rsc.encode(message)
            parity_bytes = encoded_msg[self.data_blocks:]
            
            for i in range(self.parity_blocks):
                parity_shards[i][j] = parity_bytes[i]
                
        all_shards = shards + parity_shards
        return b"".join(all_shards)

    def extract_data(self, encoded: bytes, original_data_size: int, erase_pos=None) -> bytes:
        shard_size = len(encoded) // self.total_shards
        
        shards = [bytearray(encoded[i * shard_size : (i + 1) * shard_size]) 
                  for i in range(self.total_shards)]
        
        rsc = RSCodec(self.parity_blocks)
        decoded_shards = [bytearray(shard_size) for _ in range(self.data_blocks)]
        
        erased_shard_indices = set()
        if erase_pos:
            for pos in erase_pos:
                erased_shard_indices.add(pos // shard_size)
        
        erased_shard_indices_list = list(erased_shard_indices)
        
        for j in range(shard_size):
            message = bytearray(self.total_shards)
            for i in range(self.total_shards):
                message[i] = shards[i][j]
            
            try:
                decoded_msg, _, _ = rsc.decode(message, erase_pos=erased_shard_indices_list)
                for i in range(self.data_blocks):
                    decoded_shards[i][j] = decoded_msg[i]
            except ReedSolomonError as e:
                raise ValueError(f"Decoding error at byte {j}: {str(e)}")
                
        full_data = b"".join(decoded_shards)
        return full_data[:original_data_size]

    def extract_data_blocks(self, blocks, original_data_size: int) -> bytes:
        if not blocks:
            raise ValueError("No blocks provided")
        
        valid_block = next((b for b in blocks if b is not None), None)
        if valid_block is None:
            raise ValueError("All blocks are missing")
            
        shard_size = len(valid_block)
        if len(blocks) != self.total_shards:
            raise ValueError(f"Expected {self.total_shards} blocks, got {len(blocks)}")
            
        erase_pos = [i for i, b in enumerate(blocks) if b is None]
        shards = [bytearray(b) if b is not None else bytearray(shard_size) for b in blocks]
        
        rsc = RSCodec(self.parity_blocks)
        decoded_shards = [bytearray(shard_size) for _ in range(self.data_blocks)]
        
        for j in range(shard_size):
            stripe = bytearray(self.total_shards)
            for i in range(self.total_shards):
                stripe[i] = shards[i][j]
            
            try:
                decoded_stripe, _, _ = rsc.decode(stripe, erase_pos=erase_pos)
                for i in range(self.data_blocks):
                    decoded_shards[i][j] = decoded_stripe[i]
            except ReedSolomonError as e:
                 raise ValueError("Decoding error: " + str(e))
                 
        full_data = b"".join(decoded_shards)
        return full_data[:original_data_size]
