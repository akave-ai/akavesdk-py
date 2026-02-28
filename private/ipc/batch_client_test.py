# Copyright (C) 2025 Akave
# See LICENSE for copying information.

import os
import time
import pytest
from eth_account import Account
from web3 import Web3
from web3.exceptions import BlockNotFound

from .batch_client import BatchClient, BatchReceiptRequest, BatchBlockResponse
from .client import Client, Config
from ..ipctest.ipctest import new_funded_account, to_wei


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


def test_get_transaction_receipts_batch():
    dial_uri = pick_dial_uri()
    private_key = pick_private_key()
    
    pk = new_funded_account(private_key, dial_uri, to_wei(10))
    pk_hex = pk.key.hex()
    
    # Deploy contracts
    client = Client.deploy_contracts(Config(
        dial_uri=dial_uri,
        private_key=pk_hex
    ))
    
    batch_client = BatchClient(client.eth)
    
    num_txs = 55
    requests = []
    
    from_address = pk.address
    nonce = client.eth.eth.get_transaction_count(from_address, 'pending')
    gas_price = client.eth.eth.gas_price
    chain_id = client.eth.eth.chain_id
    
    for i in range(num_txs):
        to_address = "0x000000000000000000000000000000000000dead"
        
        tx = {
            'chainId': chain_id,
            'nonce': nonce + i,
            'to': to_address,
            'value': 0,
            'gas': 21000,
            'gasPrice': gas_price,
        }
        
        signed_tx = pk.sign_transaction(tx)
        tx_hash = client.eth.eth.send_raw_transaction(signed_tx.rawTransaction)
        
        requests.append(BatchReceiptRequest(
            hash=tx_hash.hex(),
            key=f"tx-{i}"
        ))
    
    result = batch_client.get_transaction_receipts_batch(requests, timeout=10.0)
    
    assert result is not None
    assert len(result.responses) == len(requests)
    
    receipts_found = 0
    receipts_not_found = 0
    individual_errors = 0
    
    for i, response in enumerate(result.responses):
        expected_key = f"tx-{i}"
        assert response.key == expected_key
        
        if response.error is not None:
            individual_errors += 1
            print(f"Transaction {requests[i].hash} has error: {response.error}")
        elif response.receipt is None:
            receipts_not_found += 1
            print(f"Transaction {requests[i].hash} not yet mined (receipt is None)")
        else:
            receipts_found += 1
            tx_hash = response.receipt['transactionHash']
            if isinstance(tx_hash, bytes):
                tx_hash = tx_hash.hex()
            if not tx_hash.startswith('0x'):
                tx_hash = '0x' + tx_hash
            
            req_hash = requests[i].hash
            if not req_hash.startswith('0x'):
                req_hash = '0x' + req_hash
            
            assert tx_hash == req_hash
            assert response.receipt['status'] in (0, 1)
            
            block_number = response.receipt['blockNumber']
            if isinstance(block_number, str):
                block_number = int(block_number, 16)
            assert block_number > 0
            
            print(f"Transaction {tx_hash} mined in block {block_number} "
                  f"with status {response.receipt['status']}")
    
    assert len(requests) == receipts_found + receipts_not_found + individual_errors


def test_get_blocks_batch():
    dial_uri = pick_dial_uri()
    private_key = pick_private_key()
    
    pk = new_funded_account(private_key, dial_uri, to_wei(10))
    pk_hex = pk.key.hex()
    
    client = Client.deploy_contracts(Config(
        dial_uri=dial_uri,
        private_key=pk_hex
    ))
    
    batch_client = BatchClient(client.eth)
    
    from_address = pk.address
    nonce = client.eth.eth.get_transaction_count(from_address, 'pending')
    gas_price = client.eth.eth.gas_price
    chain_id = client.eth.eth.chain_id
    
    num_txs = 5
    tx_hashes = []
    
    for i in range(num_txs):
        to_address = "0x0000000000000000000000000000000000000000"
        
        tx = {
            'chainId': chain_id,
            'nonce': nonce + i,
            'to': to_address,
            'value': 0,
            'gas': 21000,
            'gasPrice': gas_price,
        }
        
        signed_tx = pk.sign_transaction(tx)
        tx_hash = client.eth.eth.send_raw_transaction(signed_tx.rawTransaction)
        tx_hashes.append(tx_hash)
        
        client.wait_for_tx(tx_hash)
    
    block_numbers = []
    for tx_hash in tx_hashes:
        receipt = client.eth.eth.get_transaction_receipt(tx_hash)
        block_num = receipt['blockNumber']
        if isinstance(block_num, str):
            block_num = int(block_num, 16)
        block_numbers.append(block_num)
    
    non_existent_block = 99999999
    block_numbers.append(non_existent_block)
    
    responses = batch_client.get_blocks_batch(block_numbers)
    
    assert len(responses) == len(block_numbers)
    
    found_blocks = 0
    not_found_blocks = 0
    
    for i, resp in enumerate(responses):
        if resp.error is not None:
            assert isinstance(resp.error, BlockNotFound)
            assert resp.block is None
            not_found_blocks += 1
        else:
            assert resp.block is not None
            
            block_num = resp.block.get('number')
            if isinstance(block_num, str):
                block_num = int(block_num, 16)
            
            assert block_num == block_numbers[i]
            found_blocks += 1
    
    assert found_blocks >= 1
    assert not_found_blocks == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
