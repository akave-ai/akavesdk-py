# Copyright (C) 2025 Akave
# See LICENSE for copying information.

from __future__ import annotations

import json
from typing import Dict, Any, List, Optional
from web3.exceptions import BlockNotFound


def block_from_json(raw_json: bytes) -> Dict[str, Any]:
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}")
    
    if data is None:
        raise BlockNotFound("Block not found")
    
    block = {}
    
    if 'hash' in data:
        block['hash'] = data['hash']
    if 'number' in data:
        if isinstance(data['number'], str):
            block['number'] = int(data['number'], 16)
        else:
            block['number'] = data['number']
    if 'parentHash' in data:
        block['parentHash'] = data['parentHash']
    if 'nonce' in data:
        block['nonce'] = data['nonce']
    if 'sha3Uncles' in data:
        block['sha3Uncles'] = data['sha3Uncles']
    if 'logsBloom' in data:
        block['logsBloom'] = data['logsBloom']
    if 'transactionsRoot' in data:
        block['transactionsRoot'] = data['transactionsRoot']
    if 'stateRoot' in data:
        block['stateRoot'] = data['stateRoot']
    if 'receiptsRoot' in data:
        block['receiptsRoot'] = data['receiptsRoot']
    if 'miner' in data:
        block['miner'] = data['miner']
    if 'difficulty' in data:
        if isinstance(data['difficulty'], str):
            block['difficulty'] = int(data['difficulty'], 16)
        else:
            block['difficulty'] = data['difficulty']
    if 'totalDifficulty' in data:
        if isinstance(data['totalDifficulty'], str):
            block['totalDifficulty'] = int(data['totalDifficulty'], 16)
        else:
            block['totalDifficulty'] = data['totalDifficulty']
    if 'extraData' in data:
        block['extraData'] = data['extraData']
    if 'size' in data:
        if isinstance(data['size'], str):
            block['size'] = int(data['size'], 16)
        else:
            block['size'] = data['size']
    if 'gasLimit' in data:
        if isinstance(data['gasLimit'], str):
            block['gasLimit'] = int(data['gasLimit'], 16)
        else:
            block['gasLimit'] = data['gasLimit']
    if 'gasUsed' in data:
        if isinstance(data['gasUsed'], str):
            block['gasUsed'] = int(data['gasUsed'], 16)
        else:
            block['gasUsed'] = data['gasUsed']
    if 'timestamp' in data:
        if isinstance(data['timestamp'], str):
            block['timestamp'] = int(data['timestamp'], 16)
        else:
            block['timestamp'] = data['timestamp']
    if 'baseFeePerGas' in data:
        if isinstance(data['baseFeePerGas'], str):
            block['baseFeePerGas'] = int(data['baseFeePerGas'], 16)
        else:
            block['baseFeePerGas'] = data['baseFeePerGas']
    if 'mixHash' in data:
        block['mixHash'] = data['mixHash']
    
    transactions = []
    if 'transactions' in data:
        for tx_data in data['transactions']:
            if isinstance(tx_data, dict):
                tx = _parse_transaction(tx_data)
                transactions.append(tx)
            else:
                transactions.append(tx_data)
    block['transactions'] = transactions
    
    uncles = []
    if 'uncles' in data:
        uncles = data['uncles']
    block['uncles'] = uncles
    
    withdrawals = []
    if 'withdrawals' in data:
        withdrawals = data['withdrawals']
    block['withdrawals'] = withdrawals
    
    return block


def _parse_transaction(tx_data: Dict[str, Any]) -> Dict[str, Any]:
    tx = {}
    
    if 'hash' in tx_data:
        tx['hash'] = tx_data['hash']
    if 'nonce' in tx_data:
        if isinstance(tx_data['nonce'], str):
            tx['nonce'] = int(tx_data['nonce'], 16)
        else:
            tx['nonce'] = tx_data['nonce']
    if 'blockHash' in tx_data:
        tx['blockHash'] = tx_data['blockHash']
    if 'blockNumber' in tx_data:
        if isinstance(tx_data['blockNumber'], str):
            tx['blockNumber'] = int(tx_data['blockNumber'], 16)
        else:
            tx['blockNumber'] = tx_data['blockNumber']
    if 'transactionIndex' in tx_data:
        if isinstance(tx_data['transactionIndex'], str):
            tx['transactionIndex'] = int(tx_data['transactionIndex'], 16)
        else:
            tx['transactionIndex'] = tx_data['transactionIndex']
    if 'from' in tx_data:
        tx['from'] = tx_data['from']
    if 'to' in tx_data:
        tx['to'] = tx_data['to']
    if 'value' in tx_data:
        if isinstance(tx_data['value'], str):
            tx['value'] = int(tx_data['value'], 16)
        else:
            tx['value'] = tx_data['value']
    if 'gas' in tx_data:
        if isinstance(tx_data['gas'], str):
            tx['gas'] = int(tx_data['gas'], 16)
        else:
            tx['gas'] = tx_data['gas']
    if 'gasPrice' in tx_data:
        if isinstance(tx_data['gasPrice'], str):
            tx['gasPrice'] = int(tx_data['gasPrice'], 16)
        else:
            tx['gasPrice'] = tx_data['gasPrice']
    if 'input' in tx_data:
        tx['input'] = tx_data['input']
    if 'v' in tx_data:
        if isinstance(tx_data['v'], str):
            tx['v'] = int(tx_data['v'], 16)
        else:
            tx['v'] = tx_data['v']
    if 'r' in tx_data:
        tx['r'] = tx_data['r']
    if 's' in tx_data:
        tx['s'] = tx_data['s']
    if 'type' in tx_data:
        if isinstance(tx_data['type'], str):
            tx['type'] = int(tx_data['type'], 16)
        else:
            tx['type'] = tx_data['type']
    if 'chainId' in tx_data:
        if isinstance(tx_data['chainId'], str):
            tx['chainId'] = int(tx_data['chainId'], 16)
        else:
            tx['chainId'] = tx_data['chainId']
    if 'accessList' in tx_data:
        tx['accessList'] = tx_data['accessList']
    if 'maxPriorityFeePerGas' in tx_data:
        if isinstance(tx_data['maxPriorityFeePerGas'], str):
            tx['maxPriorityFeePerGas'] = int(tx_data['maxPriorityFeePerGas'], 16)
        else:
            tx['maxPriorityFeePerGas'] = tx_data['maxPriorityFeePerGas']
    if 'maxFeePerGas' in tx_data:
        if isinstance(tx_data['maxFeePerGas'], str):
            tx['maxFeePerGas'] = int(tx_data['maxFeePerGas'], 16)
        else:
            tx['maxFeePerGas'] = tx_data['maxFeePerGas']
    
    return tx
