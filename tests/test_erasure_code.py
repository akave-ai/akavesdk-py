import unittest
from sdk.erasure_code import ErasureCode, missing_shards_idx, split_into_blocks

class TestErasureCode(unittest.TestCase):

    def test_invalid_params(self) -> None:
        with self.assertRaises(ValueError):
            _ = ErasureCode.new(0, 0)
        with self.assertRaises(ValueError):
            _ = ErasureCode.new(16, 0)

    def test_erasure_code(self) -> None:
        data = b"Quick brown fox jumps over the lazy dog"
        data_shards = 5
        parity_shards = 3
        total_shards = data_shards + parity_shards

        encoder = ErasureCode.new(data_shards, parity_shards)
        self.assertEqual(encoder.data_blocks, data_shards)
        self.assertEqual(encoder.parity_blocks, parity_shards)

        encoded = encoder.encode(data)
        shard_size = len(encoded) // total_shards

        with self.subTest("no missing shards"):
            blocks = split_into_blocks(encoded, shard_size)
            extracted = encoder.extract_data_blocks(blocks, len(data))
            self.assertEqual(extracted, data)

        with self.subTest("missing up to parity shards"):
            all_combos = []
            for k in range(1, parity_shards + 1):
                all_combos.extend(missing_shards_idx(total_shards, k))
            for missing_idxs in all_combos:
                blocks = split_into_blocks(encoded, shard_size)
                for idx in missing_idxs:
                    blocks[idx] = None
                extracted = encoder.extract_data_blocks(blocks, len(data))
                self.assertEqual(extracted, data)

        with self.subTest("missing more than parity shards"):
            blocks = split_into_blocks(encoded, shard_size)
            for i in range(parity_shards + 1):
                blocks[i] = None
            with self.assertRaises(ValueError):
                _ = encoder.extract_data_blocks(blocks, len(data))

if __name__ == '__main__':
    unittest.main()
