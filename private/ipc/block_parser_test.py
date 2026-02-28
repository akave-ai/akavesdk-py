# Copyright (C) 2025 Akave
# See LICENSE for copying information.

import json
import pytest
from eth_account import Account
from web3 import Web3
from web3.exceptions import BlockNotFound

from .block_parser import block_from_json


def test_parse_block_from_json_valid_block():
    account = Account.create()
    
    tx = {
        'nonce': 0,
        'to': '0x1234567890123456789012345678901234567891',
        'value': 1000,
        'gas': 21000,
        'gasPrice': 1000000000,
        'data': '0x',
        'chainId': 1
    }
    
    signed_tx = account.sign_transaction(tx)
    
    tx_data = {
        'hash': signed_tx.hash.hex(),
        'nonce': '0x0',
        'from': account.address,
        'to': '0x1234567890123456789012345678901234567891',
        'value': '0x3e8',
        'gas': '0x5208',
        'gasPrice': '0x3b9aca00',
        'input': '0x',
        'v': hex(signed_tx.v),
        'r': hex(signed_tx.r),
        's': hex(signed_tx.s),
        'type': '0x0',
        'chainId': '0x1'
    }
    
    uncle_hash = '0xdef7890123456789012345678901234567890123456789012345678901234567'
    
    block_data = {
        'number': '0x1',
        'hash': '0xabc1230000000000000000000000000000000000000000000000000000000001',
        'parentHash': '0x0000000000000000000000000000000000000000000000000000000000000000',
        'nonce': '0x0000000000000000',
        'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
        'logsBloom': '0x' + '00' * 256,
        'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'stateRoot': '0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544',
        'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'miner': '0x0000000000000000000000000000000000000000',
        'difficulty': '0x0',
        'totalDifficulty': '0x0',
        'extraData': '0x',
        'size': '0x219',
        'gasLimit': '0x1c9c380',
        'gasUsed': '0x5208',
        'timestamp': '0x61bc6b5d',
        'transactions': [tx_data],
        'uncles': [uncle_hash]
    }
    
    block_json = json.dumps(block_data).encode('utf-8')
    
    parsed_block = block_from_json(block_json)
    
    assert parsed_block is not None
    assert len(parsed_block['transactions']) == 1
    assert parsed_block['transactions'][0]['hash'] == signed_tx.hash.hex()
    assert len(parsed_block['uncles']) == 1
    assert parsed_block['uncles'][0] == uncle_hash


def test_parse_block_from_json_null_block():
    with pytest.raises(BlockNotFound):
        block_from_json(b'null')


def test_parse_block_from_json_invalid_json():
    with pytest.raises(ValueError):
        block_from_json(b'{invalid json')


def test_parse_block_from_json_empty_transactions_and_uncles():
    block_data = {
        'number': '0x2',
        'hash': '0xabc1230000000000000000000000000000000000000000000000000000000002',
        'parentHash': '0x0000000000000000000000000000000000000000000000000000000000000000',
        'nonce': '0x0000000000000000',
        'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
        'logsBloom': '0x' + '00' * 256,
        'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'stateRoot': '0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544',
        'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'miner': '0x0000000000000000000000000000000000000000',
        'difficulty': '0x0',
        'totalDifficulty': '0x0',
        'extraData': '0x',
        'size': '0x219',
        'gasLimit': '0x1c9c380',
        'gasUsed': '0x0',
        'timestamp': '0x61bc6b5d',
        'transactions': [],
        'uncles': []
    }
    
    block_json = json.dumps(block_data).encode('utf-8')
    
    block = block_from_json(block_json)
    
    assert block is not None
    assert block['hash'] == '0xabc1230000000000000000000000000000000000000000000000000000000002'
    assert len(block['transactions']) == 0
    assert len(block['uncles']) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
