# Copyright (C) 2025 Akave
# See LICENSE for copying information.

import pytest
from .ipc import generate_nonce, calculate_file_id, calculate_bucket_id, from_byte_array_cid

try:
    from multiformats import CID
    MULTIFORMATS_AVAILABLE = True
except ImportError:
    MULTIFORMATS_AVAILABLE = False
    CID = None


def test_generate_nonce():
    for i in range(10):
        nonce = generate_nonce()
        
        if i > 0:
            print(f"retrying to sample nonce {i}")
        
        nonce_bytes = nonce.to_bytes((nonce.bit_length() + 7) // 8, byteorder='big')
        if len(nonce_bytes) == 32:
            break
    
    nonce_bytes = nonce.to_bytes((nonce.bit_length() + 7) // 8, byteorder='big')
    assert len(nonce_bytes) == 32


def test_calculate_file_id():
    test_cases = [
        {
            "bucket_id": bytes.fromhex("c10fad62c0224052065576135ed2ae4d85d34432b4fb40796eadd9a991f064b9"),
            "name": "file1",
            "expected": bytes.fromhex("eea1eddf9f4be315e978c6d0d25d1b870ec0162ebf0acf173f47b738ff0cb421"),
        },
        {
            "bucket_id": bytes.fromhex("f855c5499b442e6b57ea3ec0c260d1e23a74415451ec5a4796560cc9b7d89be0"),
            "name": "file2",
            "expected": bytes.fromhex("f8d6d1f6e7ba4f69f00df4e4b53b3e51eb8610f0774f16ea1f02162e0034487b"),
        },
        {
            "bucket_id": bytes.fromhex("f06eac67910341b595f1ef319ca12713a79f180b96a685430d806701dc42e9aa"),
            "name": "file3",
            "expected": bytes.fromhex("3eb92385cd986662e9885c47364fa5b2f154cd6fca8d99f4aed68160064991cb"),
        },
    ]
    
    for tc in test_cases:
        file_id = calculate_file_id(tc["bucket_id"], tc["name"])
        assert file_id == tc["expected"]


def test_calculate_bucket_id():
    test_cases = [
        {
            "bucket_name": "test1",
            "address": "eea1eddf9f4be315e978c6d0d25d1b870ec0162ebf0acf173f47b738ff0cb421",
            "expected": "7d8b15e57405638fe772de6bb73b94345deb1f41fa1850654bc1f587a5a6afa7",
        },
        {
            "bucket_name": "bucket new",
            "address": "eea1eddf9f4be315e978c6d0d25d1b870ec0162ebf0acf173f47b738ff0cb421",
            "expected": "ca7b393db299deee1bf58fcb9670b9e6e6079cba1e85bca7c62dbd889caba925",
        },
        {
            "bucket_name": "random name",
            "address": "eea1eddf9f4be315e978c6d0d25d1b870ec0162ebf0acf173f47b738ff0cb421",
            "expected": "8f92db9fde643ed88b4dc2e238e329bafdff4a172b34d0501c2f46a0d2c36696",
        },
    ]
    
    for tc in test_cases:
        bucket_id = calculate_bucket_id(tc["bucket_name"], tc["address"])
        assert bucket_id.hex() == tc["expected"]


def test_from_byte_array_cid():
    if not MULTIFORMATS_AVAILABLE:
        pytest.skip("multiformats library not available")
    
    import secrets
    data = secrets.token_bytes(32)
    
    reconstructed_cid = from_byte_array_cid(data)
    assert reconstructed_cid is not None
    
    # Check that the CID is valid
    assert reconstructed_cid.version == 1
    assert reconstructed_cid.codec.name == 'dag-pb'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
