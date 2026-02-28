# Copyright (C) 2025 Akave
# See LICENSE for copying information.

import sys
import os
import pytest
from typing import Dict, List, Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from private.eip712.eip712 import sign, recover_signer_address, Domain, TypedData


class TestSignatureAgainstContract:
    
    test_cases = [
        {
            "chunkCID": "86b258127d599eb74c729f97",
            "blockCID": "c00612ae8af29b5437ba40df50c46c0175c69b6dc3b3014ed19bda51e318f0f3",
            "nodeID": "5a604f924e185f6ec5754156e331e9d52df8a669de7e1a060b90e636e0e9e818",
            "nonce": 3456789012,
            "deadline": 1759859212,
            "bucketID": "930c2de1e6a9a0726f2d7bde19428453d9fdc11fa5c98205ce9b9e794bbd93a2",
            "storageAddress": "0x4e7B1E9c3214C973Ff2fc680A9789E8579a5eD9d",
            "signature": "726683359604ffe042e73afd7adef9b7f6e13ffd0078999d31bd1cc8c119e1e8324d44cffdc2f771912e500c522082ee94e5f30ac5844c06497e3c49dab8b6de1b",
        },
        {
            "chunkCID": "edf5fb5fdd325e462cd806f2",
            "blockCID": "fbeeb197dd90574c97d5993fab0610403197db0f18133033755ec39cab7596c9",
            "nodeID": "3a59ed631290287c86c90777b2d45926c1a860b1e90828963358d72fa8834389",
            "nonce": 2345678901,
            "deadline": 1759862780,
            "bucketID": "95f7f023dbf92b2ab036280c44037485c0deec1d854046443bae8ae16c37bc86",
            "storageAddress": "0x23618e81E3f5cdF7f54C3d65f7FBc0aBf5B21E8f",
            "signature": "47569b36d69bde9e8953cc8c6a01599f0a307850d25e9101c4b1338fbf562d58017bd4ecae535eb330ea7c7ca710fb0055d9d3697e2ebc18902aa32d252eb7361c",
        },
        {
            "chunkCID": "2e3adffef0437b35f247022b",
            "blockCID": "fc785a432d1c6d45671f60ed36f44378f63ae4fbbf4ef2a9f0d4951e77e81272",
            "nodeID": "050f9e0347ebfbdcf50fddf89713b7f37e667d19279d9f550fa7b93237ce29fa",
            "nonce": 1234567890,
            "deadline": 1759866325,
            "bucketID": "a928e74732b6ca5fd1bf7f3eedfdca3c578a05297157e239e7f7861de2b40f42",
            "storageAddress": "0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc",
            "signature": "8ccd5143f4b87e898021c4b3a4bf73e3e8d6e8b97e39106374fac72be610629463a0ba6fc4c975c41fbb1ad3940f76a30e6cb916a8e01d09afbe24538ce151ca1b",
        }
    ]
    
    @pytest.mark.parametrize("tc", test_cases)
    def test_signature_against_contract(self, tc):
        
        data_types = {
            "StorageData": [
                TypedData("chunkCID", "bytes"),
                TypedData("blockCID", "bytes32"),
                TypedData("chunkIndex", "uint256"),
                TypedData("blockIndex", "uint8"),
                TypedData("nodeId", "bytes32"),
                TypedData("nonce", "uint256"),
                TypedData("deadline", "uint256"),
                TypedData("bucketId", "bytes32"),
            ]
        }
        
        domain = Domain(
            name="Storage",
            version="1",
            chain_id=31337,
            verifying_contract=tc["storageAddress"]
        )
        
        chunk_cid = bytes.fromhex(tc["chunkCID"])
        block_cid = bytes.fromhex(tc["blockCID"])
        node_id = bytes.fromhex(tc["nodeID"])
        bucket_id = bytes.fromhex(tc["bucketID"])
        
        data_message = {
            "chunkCID": chunk_cid,
            "blockCID": block_cid,
            "chunkIndex": 0,
            "blockIndex": 0,
            "nodeId": node_id,
            "nonce": tc["nonce"],
            "deadline": tc["deadline"],
            "bucketId": bucket_id,
        }
        
        private_key_hex = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
        private_key_bytes = bytes.fromhex(private_key_hex)
        
        signature = sign(private_key_bytes, domain, "StorageData", data_types, data_message)
        
        assert signature.hex() == tc["signature"], \
            f"Signature mismatch for test case with nonce {tc['nonce']}"


def test_signature_recovery():
    
    private_key_hex = "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    private_key_bytes = bytes.fromhex(private_key_hex)
    
    from eth_keys import keys
    private_key_obj = keys.PrivateKey(private_key_bytes)
    expected_address = private_key_obj.public_key.to_checksum_address()
    
    block_cid = bytearray(32)
    block_cid[:9] = b"blockCID1"
    
    node_id = bytearray(32)
    node_id[:7] = b"node id"
    
    bucket_id = bytearray(32)
    bucket_id[:9] = b"bucket id"
    
    data_types = {
        "StorageData": [
            TypedData("chunkCID", "bytes"),
            TypedData("blockCID", "bytes32"),
            TypedData("chunkIndex", "uint256"),
            TypedData("blockIndex", "uint8"),
            TypedData("nodeId", "bytes32"),
            TypedData("nonce", "uint256"),
            TypedData("deadline", "uint256"),
            TypedData("bucketId", "bytes32"),
        ]
    }
    
    domain = Domain(
        name="Storage",
        version="1",
        chain_id=31337,
        verifying_contract="0x1234567890123456789012345678901234567890"
    )
    
    data_message = {
        "chunkCID": b"rootCID1",
        "blockCID": bytes(block_cid),
        "chunkIndex": 0,
        "blockIndex": 0,
        "nodeId": bytes(node_id),
        "nonce": 1234567890,
        "deadline": 12345,
        "bucketId": bytes(bucket_id),
    }
    
    signature = sign(private_key_bytes, domain, "StorageData", data_types, data_message)
    
    recovered_address = recover_signer_address(
        signature, domain, "StorageData", data_types, data_message
    )
    
    assert recovered_address.lower() == expected_address.lower(), \
        f"Address mismatch: expected {expected_address}, got {recovered_address}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

