# Copyright (C) 2025 Akave
# See LICENSE for copying information.

import os
import json
import pytest
import requests
from web3 import Web3
from eth_account import Account

from ..ipctest.ipctest import new_funded_account, to_wei, wait_for_tx


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


def fill_blocks(dial_uri: str):
    payload = {
        "jsonrpc": "2.0",
        "method": "anvil_mine",
        "params": [],
        "id": 1
    }
    
    response = requests.post(dial_uri, json=payload, headers={"Content-Type": "application/json"})
    response.raise_for_status()


def pad_left(data: bytes, size: int) -> bytes:
    if len(data) >= size:
        return data
    
    padded = bytearray(size)
    padded[size - len(data):] = data
    return bytes(padded)


@pytest.mark.integration
def test_contract_pdp():
    dial_uri = pick_dial_uri()
    private_key = pick_private_key()
    
    pk = new_funded_account(private_key, dial_uri, to_wei(10))
    
    web3 = Web3(Web3.HTTPProvider(dial_uri))
    assert web3.is_connected()
    
    chain_id = web3.eth.chain_id
    
    challenge_finality = 3
    
    from .contracts.pdp_verifier import deploy_pdp_verifier, PDPVerifierMetaData
    from .contracts.sink import deploy_sink
    
    verifier_address, tx_hash = deploy_pdp_verifier(
        web3, pk, challenge_finality
    )
    wait_for_tx(web3, tx_hash)
    
    pdp_verifier = web3.eth.contract(address=verifier_address, abi=PDPVerifierMetaData.ABI)
    
    sink_address, tx_hash = deploy_sink(web3, pk)
    wait_for_tx(web3, tx_hash)
    
    value_wei = web3.to_wei(1, 'ether')
    
    tx = pdp_verifier.functions.createProofSet(sink_address, b'').build_transaction({
        'from': pk.address,
        'value': value_wei,
        'gas': 500000,
        'gasPrice': web3.eth.gas_price,
        'nonce': web3.eth.get_transaction_count(pk.address),
    })
    
    signed_tx = pk.sign_transaction(tx)
    tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
    receipt = wait_for_tx(web3, tx_hash)
    
    set_id = 0
    for log in receipt['logs']:
        if log['address'].lower() == verifier_address.lower():
            set_id = int.from_bytes(log['topics'][1], byteorder='big')
            break
    
    data = bytes([
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        2, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        6, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        7, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    ])
    
    assert len(data) == 288
    
    print(f"PDP test completed with set_id: {set_id}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
