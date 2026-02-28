# Copyright (C) 2025 Akave
# See LICENSE for copying information.

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import List, Optional, Any, Dict
from web3 import Web3
from web3.exceptions import BlockNotFound, TransactionNotFound
from eth_typing import HexStr

from .block_parser import block_from_json


@dataclass
class BatchReceiptRequest:
    hash: str
    key: str


@dataclass
class BatchReceiptResponse:
    receipt: Optional[Dict[str, Any]]
    error: Optional[Exception]
    key: str


@dataclass
class BatchReceiptResult:
    responses: List[BatchReceiptResponse]


@dataclass
class BatchBlockResponse:
    block_number: int
    block: Optional[Dict[str, Any]]
    error: Optional[Exception]


class BatchClient:

    def __init__(self, web3: Web3):
        self.web3 = web3

    def get_transaction_receipts_batch(
        self, 
        requests: List[BatchReceiptRequest], 
        timeout: float = 30.0
    ) -> BatchReceiptResult:
        # Prepare batch requests
        batch_requests = []
        for req in requests:
            tx_hash = req.hash
            if not tx_hash.startswith('0x'):
                tx_hash = '0x' + tx_hash
            batch_requests.append(('eth_getTransactionReceipt', [tx_hash]))
        
        try:
            raw_responses = self.web3.manager.request_blocking_batch(batch_requests)
            
            responses = []
            for i, (req, raw_response) in enumerate(zip(requests, raw_responses)):
                if 'error' in raw_response:
                    error_msg = raw_response['error'].get('message', 'Unknown error')
                    response = BatchReceiptResponse(
                        receipt=None,
                        error=Exception(error_msg),
                        key=req.key
                    )
                elif raw_response.get('result') is None:
                    response = BatchReceiptResponse(
                        receipt=None,
                        error=TransactionNotFound(f"Transaction {req.hash} not found"),
                        key=req.key
                    )
                else:
                    response = BatchReceiptResponse(
                        receipt=raw_response['result'],
                        error=None,
                        key=req.key
                    )
                responses.append(response)
            
            return BatchReceiptResult(responses=responses)
            
        except Exception as e:
            # Fallback: try individual requests
            responses = []
            for req in requests:
                try:
                    tx_hash = req.hash
                    if not tx_hash.startswith('0x'):
                        tx_hash = '0x' + tx_hash
                    receipt = self.web3.eth.get_transaction_receipt(tx_hash)
                    response = BatchReceiptResponse(
                        receipt=dict(receipt),
                        error=None,
                        key=req.key
                    )
                except TransactionNotFound:
                    response = BatchReceiptResponse(
                        receipt=None,
                        error=TransactionNotFound(f"Transaction {req.hash} not found"),
                        key=req.key
                    )
                except Exception as err:
                    response = BatchReceiptResponse(
                        receipt=None,
                        error=err,
                        key=req.key
                    )
                responses.append(response)
            
            return BatchReceiptResult(responses=responses)

    def get_blocks_batch(
        self, 
        block_numbers: List[int]
    ) -> List[BatchBlockResponse]:
        batch_requests = []
        for block_number in block_numbers:
            block_hex = hex(block_number) if block_number >= 0 else 'latest'
            batch_requests.append(('eth_getBlockByNumber', [block_hex, True]))
        
        try:
            raw_responses = self.web3.manager.request_blocking_batch(batch_requests)
            
            responses = []
            for block_number, raw_response in zip(block_numbers, raw_responses):
                if 'error' in raw_response:
                    error_msg = raw_response['error'].get('message', 'Unknown error')
                    response = BatchBlockResponse(
                        block_number=block_number,
                        block=None,
                        error=Exception(error_msg)
                    )
                elif raw_response.get('result') is None:
                    # Block not found
                    response = BatchBlockResponse(
                        block_number=block_number,
                        block=None,
                        error=BlockNotFound(f"Block {block_number} not found")
                    )
                else:
                    try:
                        block_json = json.dumps(raw_response['result']).encode('utf-8')
                        block_data = block_from_json(block_json)
                        response = BatchBlockResponse(
                            block_number=block_number,
                            block=block_data,
                            error=None
                        )
                    except Exception as e:
                        response = BatchBlockResponse(
                            block_number=block_number,
                            block=None,
                            error=e
                        )
                responses.append(response)
            
            return responses
            
        except Exception as e:
            responses = []
            for block_number in block_numbers:
                try:
                    block = self.web3.eth.get_block(block_number, full_transactions=True)
                    block_json = json.dumps(dict(block)).encode('utf-8')
                    block_data = block_from_json(block_json)
                    response = BatchBlockResponse(
                        block_number=block_number,
                        block=block_data,
                        error=None
                    )
                except BlockNotFound:
                    response = BatchBlockResponse(
                        block_number=block_number,
                        block=None,
                        error=BlockNotFound(f"Block {block_number} not found")
                    )
                except Exception as err:
                    response = BatchBlockResponse(
                        block_number=block_number,
                        block=None,
                        error=err
                    )
                responses.append(response)
            
            return responses
