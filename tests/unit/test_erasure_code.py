import pytest
from unittest.mock import Mock, patch
import math

from sdk.erasure_code import ErasureCode, missing_shards_idx, split_into_blocks


class TestMissingShardsIdx:
    
    def test_missing_shards_idx_basic(self):
        result = missing_shards_idx(5, 2)
        assert isinstance(result, list)
        assert len(result) == 10
        assert [0, 1] in result
        assert [4, 3] in result or [3, 4] in result
    
    def test_missing_shards_idx_single(self):
        result = missing_shards_idx(3, 1)
        assert len(result) == 3
        assert [0] in result
        assert [1] in result
        assert [2] in result
    
    def test_missing_shards_idx_none(self):
        result = missing_shards_idx(5, 0)
        assert len(result) == 1
        assert result[0] == []


class TestSplitIntoBlocks:
    
    def test_split_into_blocks_exact(self):
        data = b"123456789012"
        blocks = split_into_blocks(data, 4)
        assert len(blocks) == 3
        assert blocks[0] == b"1234"
        assert blocks[1] == b"5678"
        assert blocks[2] == b"9012"
    
    def test_split_into_blocks_with_padding(self):
        data = b"12345"
        blocks = split_into_blocks(data, 3)
        assert len(blocks) == 2
        assert blocks[0] == b"123"
        assert blocks[1] == b"45\x00"
    
    def test_split_into_blocks_empty(self):
        data = b""
        blocks = split_into_blocks(data, 4)
        assert len(blocks) == 0
    
    def test_split_into_blocks_single(self):
        data = b"ab"
        blocks = split_into_blocks(data, 5)
        assert len(blocks) == 1
        assert blocks[0] == b"ab\x00\x00\x00"


class TestErasureCodeInit:
    
    def test_init_valid(self):
        ec = ErasureCode(4, 2)
        assert ec.data_blocks == 4
        assert ec.parity_blocks == 2
        assert ec.total_shards == 6
    
    def test_init_invalid_data_blocks_zero(self):
        with pytest.raises(ValueError, match="Data and parity shards must be > 0"):
            ErasureCode(0, 2)
    
    def test_init_invalid_data_blocks_negative(self):
        with pytest.raises(ValueError, match="Data and parity shards must be > 0"):
            ErasureCode(-1, 2)
    
    def test_init_invalid_parity_blocks_zero(self):
        with pytest.raises(ValueError, match="Data and parity shards must be > 0"):
            ErasureCode(4, 0)
    
    def test_init_invalid_parity_blocks_negative(self):
        with pytest.raises(ValueError, match="Data and parity shards must be > 0"):
            ErasureCode(4, -2)
    
    def test_new_classmethod(self):
        ec = ErasureCode.new(3, 2)
        assert isinstance(ec, ErasureCode)
        assert ec.data_blocks == 3
        assert ec.parity_blocks == 2


class TestErasureCodeEncode:
    
    def test_encode_basic(self):
        ec = ErasureCode(4, 2)
        data = b"Hello, World! This is test data."
        
        encoded = ec.encode(data)
        
        assert isinstance(encoded, bytes)
        assert len(encoded) >= len(data)
        expected_shard_size = math.ceil(len(data) / 4)
        expected_total = 6 * expected_shard_size
        assert len(encoded) == expected_total
    
    def test_encode_empty_data(self):
        ec = ErasureCode(2, 1)
        data = b""
        
        encoded = ec.encode(data)
        
        assert isinstance(encoded, bytes)
    
    def test_encode_small_data(self):
        ec = ErasureCode(2, 1)
        data = b"Hi"
        
        encoded = ec.encode(data)
        
        assert len(encoded) >= len(data)
        expected_shard_size = math.ceil(len(data) / 2)
        assert len(encoded) == 3 * expected_shard_size
    
    def test_encode_large_data(self):
        ec = ErasureCode(8, 4)
        data = b"x" * 10000
        
        encoded = ec.encode(data)
        
        expected_shard_size = math.ceil(len(data) / 8)
        expected_total = 12 * expected_shard_size
        assert len(encoded) == expected_total


class TestErasureCodeExtractData:
    
    def test_extract_data_no_errors(self):
        ec = ErasureCode(4, 2)
        data = b"Test data for erasure coding"
        
        encoded = ec.encode(data)
        decoded = ec.extract_data(encoded, len(data))
        
        assert decoded == data
    
    def test_extract_data_with_erase_pos(self):
        ec = ErasureCode(4, 2)
        data = b"Test data with erasure"
        
        encoded = ec.encode(data)
        shard_size = len(encoded) // 6
        
        erase_pos = list(range(0, shard_size))
        
        corrupted = bytearray(encoded)
        for pos in erase_pos:
            corrupted[pos] = 0
        
        decoded = ec.extract_data(bytes(corrupted), len(data), erase_pos=erase_pos)
        
        assert decoded == data
    
    def test_extract_data_too_many_errors(self):
        ec = ErasureCode(3, 1)
        data = b"Test"
        
        encoded = ec.encode(data)
        shard_size = len(encoded) // 4
        
        corrupted = bytearray(encoded)
        for i in range(0, shard_size * 2):
            corrupted[i] = 0xFF
        
        with pytest.raises(ValueError, match="Decoding error"):
            ec.extract_data(bytes(corrupted), len(data))
    
    def test_extract_data_empty(self):
        ec = ErasureCode(2, 1)
        data = b""
        
        encoded = ec.encode(data)
        decoded = ec.extract_data(encoded, 0)
        
        assert decoded == b""


class TestErasureCodeExtractDataBlocks:
    
    def test_extract_data_blocks_all_present(self):
        ec = ErasureCode(4, 2)
        data = b"Hello, erasure coding blocks!"
        
        encoded = ec.encode(data)
        shard_size = len(encoded) // 6
        blocks = split_into_blocks(encoded, shard_size)
        
        decoded = ec.extract_data_blocks(blocks, len(data))
        
        assert decoded == data
    
    def test_extract_data_blocks_with_missing(self):
        ec = ErasureCode(4, 2)
        data = b"Test missing blocks"
        
        encoded = ec.encode(data)
        shard_size = len(encoded) // 6
        blocks = split_into_blocks(encoded, shard_size)
        
        blocks[0] = None
        blocks[2] = None
        
        decoded = ec.extract_data_blocks(blocks, len(data))
        
        assert decoded == data
    
    def test_extract_data_blocks_no_blocks(self):
        ec = ErasureCode(3, 2)
        
        with pytest.raises(ValueError, match="No blocks provided"):
            ec.extract_data_blocks([], 100)
    
    def test_extract_data_blocks_all_missing(self):
        ec = ErasureCode(3, 2)
        blocks = [None, None, None, None, None]
        
        with pytest.raises(ValueError, match="All blocks are missing"):
            ec.extract_data_blocks(blocks, 100)
    
    def test_extract_data_blocks_wrong_count(self):
        ec = ErasureCode(4, 2)
        blocks = [b"block1", b"block2"]
        
        with pytest.raises(ValueError, match="Expected 6 blocks"):
            ec.extract_data_blocks(blocks, 100)
    
    def test_extract_data_blocks_partial_missing(self):
        ec = ErasureCode(3, 1)
        data = b"Partial missing test"
        
        encoded = ec.encode(data)
        shard_size = len(encoded) // 4
        blocks = split_into_blocks(encoded, shard_size)
        
        blocks[1] = None
        
        decoded = ec.extract_data_blocks(blocks, len(data))
        
        assert decoded == data


class TestErasureCodeRoundtrip:
    
    def test_roundtrip_simple(self):
        ec = ErasureCode(5, 3)
        data = b"Simple roundtrip test"
        
        encoded = ec.encode(data)
        decoded = ec.extract_data(encoded, len(data))
        
        assert decoded == data
    
    def test_roundtrip_with_unicode(self):
        ec = ErasureCode(4, 2)
        data = "Hello 世界! 🌍".encode('utf-8')
        
        encoded = ec.encode(data)
        decoded = ec.extract_data(encoded, len(data))
        
        assert decoded == data
        assert decoded.decode('utf-8') == "Hello 世界! 🌍"
    
    def test_roundtrip_binary_data(self):
        ec = ErasureCode(6, 2)
        data = bytes(range(256))
        
        encoded = ec.encode(data)
        decoded = ec.extract_data(encoded, len(data))
        
        assert decoded == data
    
    def test_roundtrip_large_file(self):
        ec = ErasureCode(10, 4)
        data = b"x" * 50000
        
        encoded = ec.encode(data)
        decoded = ec.extract_data(encoded, len(data))
        
        assert decoded == data
        assert len(decoded) == 50000


class TestErasureCodeEdgeCases:
    
    def test_single_byte_data(self):
        ec = ErasureCode(2, 1)
        data = b"x"
        
        encoded = ec.encode(data)
        decoded = ec.extract_data(encoded, len(data))
        
        assert decoded == data
    
    def test_high_redundancy(self):
        ec = ErasureCode(2, 10)
        data = b"High redundancy test"
        
        encoded = ec.encode(data)
        decoded = ec.extract_data(encoded, len(data))
        
        assert decoded == data
    
    def test_many_data_blocks(self):
        ec = ErasureCode(20, 5)
        data = b"y" * 1000
        
        encoded = ec.encode(data)
        decoded = ec.extract_data(encoded, len(data))
        
        assert decoded == data
    
    def test_equal_data_parity(self):
        ec = ErasureCode(5, 5)
        data = b"Equal data and parity"
        
        encoded = ec.encode(data)
        decoded = ec.extract_data(encoded, len(data))
        
        assert decoded == data


@pytest.mark.integration
class TestErasureCodeIntegration:
    
    def test_full_workflow(self):
        ec = ErasureCode(4, 2)
        original_data = b"Integration test data for erasure coding"
        
        encoded_data = ec.encode(original_data)
        
        shard_size = len(encoded_data) // 6
        blocks = split_into_blocks(encoded_data, shard_size)
        
        assert len(blocks) == 6
        
        blocks[1] = None
        
        recovered_data = ec.extract_data_blocks(blocks, len(original_data))
        
        assert recovered_data == original_data
    
    def test_multiple_missing_blocks(self):
        ec = ErasureCode(6, 3)
        data = b"Test recovery with multiple missing blocks"
        
        encoded = ec.encode(data)
        shard_size = len(encoded) // 9
        blocks = split_into_blocks(encoded, shard_size)
        
        blocks[0] = None
        blocks[4] = None
        blocks[7] = None
        
        recovered = ec.extract_data_blocks(blocks, len(data))
        
        assert recovered == data
    
    def test_stress_test(self):
        ec = ErasureCode(8, 4)
        
        for size in [10, 100, 1000, 5000]:
            data = b"z" * size
            encoded = ec.encode(data)
            decoded = ec.extract_data(encoded, len(data))
            assert decoded == data

