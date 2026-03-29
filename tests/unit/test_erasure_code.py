from itertools import combinations

import pytest

from private.erasurecode import ErasureCode, ErasureCodeError


def _split_into_blocks(encoded: bytes, shard_size: int):
    blocks = []
    for offset in range(0, len(encoded), shard_size):
        block = bytearray(shard_size)
        chunk = encoded[offset : offset + shard_size]
        block[: len(chunk)] = chunk
        blocks.append(bytes(block))
    return blocks


def _missing_shards_idx(n: int, k: int):
    return [list(combo) for combo in combinations(range(n), k)]


DATA = b"Quick brown fox jumps over the lazy dog"
DATA_SHARDS = 5
PARITY_SHARDS = 3


class TestErasureCodeInvalidParams:
    def test_both_zero(self):
        with pytest.raises(ErasureCodeError):
            ErasureCode(0, 0)

    def test_zero_parity(self):
        with pytest.raises(ErasureCodeError):
            ErasureCode(16, 0)

    def test_zero_data(self):
        with pytest.raises(ErasureCodeError):
            ErasureCode(0, 4)


class TestErasureCode:
    def setup_method(self):
        self.encoder = ErasureCode(DATA_SHARDS, PARITY_SHARDS)
        assert self.encoder.data_blocks == DATA_SHARDS
        assert self.encoder.parity_blocks == PARITY_SHARDS

        self.encoded = self.encoder.encode(DATA)
        self.shard_size = len(self.encoded) // (DATA_SHARDS + PARITY_SHARDS)

    def test_no_missing_shards(self):
        blocks = _split_into_blocks(self.encoded, self.shard_size)
        result = self.encoder.extract_data(blocks, 0)
        assert result == DATA

    def test_missing_up_to_parity_count(self):
        """All combinations of up to parity_shards missing shards should recover."""
        all_combos = []
        for k in range(1, PARITY_SHARDS + 1):
            all_combos.extend(_missing_shards_idx(DATA_SHARDS + PARITY_SHARDS, k))

        for missing_idxs in all_combos:
            encoded = self.encoder.encode(DATA)
            blocks = _split_into_blocks(encoded, self.shard_size)
            for idx in missing_idxs:
                blocks[idx] = None
            result = self.encoder.extract_data(blocks, 0)
            assert result == DATA, f"failed for missing shards: {missing_idxs}"

    def test_missing_more_than_parity_count(self):
        blocks = _split_into_blocks(self.encoded, self.shard_size)
        for i in range(PARITY_SHARDS + 1):
            blocks[i] = None
        with pytest.raises(ErasureCodeError):
            self.encoder.extract_data(blocks, 0)

    def test_non_erasure_coded_data(self):
        """Garbage blocks with correct size should fail magic suffix check."""
        blocks = [bytes([(i + j) % 256 for j in range(self.shard_size)]) for i in range(DATA_SHARDS + PARITY_SHARDS)]
        with pytest.raises(ErasureCodeError):
            self.encoder.extract_data(blocks, 0)


class TestReconstructAll:
    def setup_method(self):
        self.encoder = ErasureCode(DATA_SHARDS, PARITY_SHARDS)
        self.encoded = self.encoder.encode(DATA)
        self.shard_size = len(self.encoded) // (DATA_SHARDS + PARITY_SHARDS)

    def test_reconstruct_all_blocks(self):
        original_blocks = _split_into_blocks(self.encoded, self.shard_size)

        blocks = list(original_blocks)
        blocks[0] = None
        blocks[1] = None
        blocks[-1] = None

        self.encoder.reconstruct_all(blocks)

        assert blocks == original_blocks
