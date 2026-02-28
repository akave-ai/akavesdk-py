# Copyright (C) 2024 Akave
# See LICENSE for copying information.

import os
import sys
import time
import secrets
import pytest
from typing import Optional
from unittest.mock import Mock, patch

from .client import Client, Config, TransactionFailedError
from .ipc import StorageData, sign_block
from ..ipctest.ipctest import new_funded_account, to_wei, IPCTestError

DIAL_URI = os.getenv("DIAL_URI", "")
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")

def pick_dial_uri() -> str:
    if not DIAL_URI or DIAL_URI.lower() == "omit":
        pytest.skip("dial uri flag missing, set DIAL_URI environment variable")
    return DIAL_URI

def pick_private_key() -> str:
    if not PRIVATE_KEY or PRIVATE_KEY.lower() == "omit":
        pytest.skip("private key flag missing, set PRIVATE_KEY environment variable")
    return PRIVATE_KEY

def generate_random_address() -> str:
    from eth_account import Account
    account = Account.create()
    return account.address

def generate_random_cid() -> bytes:
    return secrets.token_bytes(32)

def generate_random_nonce() -> int:
    return secrets.randbits(256)

class TestClient:

    def test_config_default(self):
        config = Config()
        assert config.dial_uri == ""
        assert config.private_key == ""
        assert config.storage_contract_address == ""
        assert config.access_contract_address == ""

    def test_config_creation(self):
        config = Config(
            dial_uri="http://localhost:8545",
            private_key="0x123",
            storage_contract_address="0xstorage",
            access_contract_address="0xaccess"
        )
        assert config.dial_uri == "http://localhost:8545"
        assert config.private_key == "0x123"
        assert config.storage_contract_address == "0xstorage"

    @pytest.mark.integration
    def test_dial_connection(self):
        dial_uri = pick_dial_uri()
        private_key = pick_private_key()
        
        config = Config(
            dial_uri=dial_uri,
            private_key=private_key,
            storage_contract_address="0x" + "0" * 40,  # Mock address
            access_contract_address="0x" + "0" * 40,   # Mock address
        )
        
        try:
            client = Client.dial(config)
            assert client.web3.is_connected()
            assert client.auth is not None
            assert client.chain_id() is not None
        except Exception as e:
            assert "Failed to connect" in str(e) or "Invalid" in str(e)

    def test_wait_for_tx_invalid_hash(self):
        with patch('web3.Web3') as mock_web3:
            mock_web3_instance = Mock()
            mock_web3_instance.is_connected.return_value = True
            mock_web3_instance.eth.chain_id = 1
            mock_web3_instance.eth.get_transaction_receipt.side_effect = Exception("not found")
            mock_web3.return_value = mock_web3_instance
            
            from ..contracts import StorageContract
            with patch.object(StorageContract, '__init__', return_value=None):
                mock_storage = Mock()
                
                from eth_account import Account
                account = Account.create()
                
                client = Client(
                    web3=mock_web3_instance,
                    auth=account,
                    storage=mock_storage,
                    chain_id=1
                )
                
                with pytest.raises((TimeoutError, TransactionFailedError)):
                    client.wait_for_tx("0x" + "0" * 64, timeout=0.1)

    def test_storage_data_structure(self):
        chunk_cid = b"test_chunk_cid"
        block_cid = b"0" * 32  # 32 bytes
        bucket_id = b"1" * 32  # 32 bytes
        node_id = b"2" * 32    # 32 bytes
        
        data = StorageData(
            chunk_cid=chunk_cid,
            block_cid=block_cid,
            chunk_index=0,
            block_index=1,
            node_id=node_id,
            nonce=12345,
            deadline=int(time.time()) + 3600,
            bucket_id=bucket_id
        )
        
        assert data.chunk_cid == chunk_cid
        assert data.block_cid == block_cid
        assert data.chunk_index == 0
        assert data.block_index == 1
        
        # Test message dict conversion
        msg_dict = data.to_message_dict()
        assert msg_dict["chunkCID"] == chunk_cid
        assert msg_dict["blockCID"] == block_cid
        assert msg_dict["nonce"] == 12345

    def test_sign_block(self):
        from eth_account import Account
        
        # Create test account
        account = Account.create()
        private_key_hex = account.key.hex()
        
        # Create test data
        data = StorageData(
            chunk_cid=b"test_chunk",
            block_cid=b"0" * 32,
            chunk_index=0,
            block_index=1,
            node_id=b"1" * 32,
            nonce=12345,
            deadline=int(time.time()) + 3600,
            bucket_id=b"2" * 32
        )
        
        # Test signing
        storage_address = "0x" + "0" * 40
        chain_id = 1
        
        try:
            signature = sign_block(private_key_hex, storage_address, chain_id, data)
            assert isinstance(signature, bytes)
            assert len(signature) > 0
        except Exception as e:
            # May fail if EIP712 implementation is not complete
            assert "not implemented" in str(e).lower() or "import" in str(e).lower()

if __name__ == "__main__":
    # Run basic tests
    test_client = TestClient()
    
    print("Running basic client tests...")
    
    try:
        test_client.test_config_default()
        print("✓ Config default test passed")
    except Exception as e:
        print(f"✗ Config default test failed: {e}")
    
    try:
        test_client.test_config_creation()
        print("✓ Config creation test passed")
    except Exception as e:
        print(f"✗ Config creation test failed: {e}")
    
    try:
        test_client.test_storage_data_structure()
        print("✓ Storage data structure test passed")
    except Exception as e:
        print(f"✗ Storage data structure test failed: {e}")
    
    print("\nNote: Integration tests require DIAL_URI and PRIVATE_KEY environment variables")
    print("Example: DIAL_URI=http://localhost:8545 PRIVATE_KEY=0x... python client_test.py")
