import numpy as np
from reedsolo import RSCodec, ReedSolomonError

class ErasureCode:
    def __init__(self, data_blocks: int, parity_blocks: int):
        if data_blocks <= 0 or parity_blocks <= 0:
            raise ValueError("Data and parity shards must be > 0")

        self.data_blocks = data_blocks
        self.parity_blocks = parity_blocks
        self.total_blocks = data_blocks + parity_blocks
        self.encoder = RSCodec(parity_blocks * 2)

    def encode(self, data: bytes) -> list:
        """Splits data into shards, encodes them, and returns encoded shards."""
        try:
            shards = np.array_split(data, self.data_blocks)
            shards = [shard.tobytes() for shard in shards]

            max_shard_size = max(len(shard) for shard in shards)
            shards = [shard.ljust(max_shard_size, b'\0') for shard in shards]

            encoded_shards = [self.encoder.encode(shard) for shard in shards]

            return encoded_shards
        except ReedSolomonError as e:
            raise RuntimeError(f"Erasure coding failed: {str(e)}")

    def verify(self, encoded_shards: list) -> bool:
        """Verifies if encoded shards are correct."""
        try:
            for shard in encoded_shards:
                self.encoder.decode(shard, only_erasures=True)  
            return True
        except ReedSolomonError:
            return False  

    def reconstruct(self, encoded_shards: list) -> list:
        """Attempts to reconstruct missing/corrupted shards."""
        try:
            recovered_shards = []
            for shard in encoded_shards:
                try:
                    recovered_shards.append(self.encoder.decode(shard))  
                except ReedSolomonError:
                    recovered_shards.append(b'\0' * len(shard))  

            return recovered_shards
        except ReedSolomonError as e:
            raise RuntimeError(f"Reconstruction failed: {str(e)}")

    def extract_data(self, encoded_shards: list, original_data_size: int) -> bytes:
        """Extracts the original data from encoded shards, verifying and reconstructing if needed."""
        if not self.verify(encoded_shards):
            encoded_shards = self.reconstruct(encoded_shards)

        try:
            decoded_shards = [self.encoder.decode(shard) for shard in encoded_shards]

            return b"".join(decoded_shards)[:original_data_size]
        except ReedSolomonError as e:
            raise RuntimeError(f"Data extraction failed: {str(e)}")
