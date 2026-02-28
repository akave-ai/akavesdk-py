"""
Sink contract bindings for Python.
This file is auto-generated - DO NOT EDIT manually.
"""

from __future__ import annotations

from typing import Optional, Dict, Any, List, Tuple
from eth_typing import Address, HexStr, HexAddress
from web3 import Web3
from web3.contract import Contract
from eth_account.signers.local import LocalAccount
from eth_account import Account


class SinkMetaData:
    """Metadata for the Sink contract."""
    
    ABI = [
        {
            "stateMutability": "nonpayable",
            "type": "fallback"
        }
    ]

    BIN = "0x6080604052348015600e575f5ffd5b50604680601a5f395ff3fe6080604052348015600e575f5ffd5b00fea2646970667358221220f43799cb6e28e32500f5eb3784cc07778d26ab2be04f4ee9fd27d581ad2138f464736f6c634300081c0033"


class SinkContract:
    """Main class for interacting with the Sink contract."""
    
    def __init__(self, w3: Web3, address: str):
        """Initialize Sink contract interface."""
        self.w3 = w3
        self.address = Web3.to_checksum_address(address)
        self.contract = w3.eth.contract(
            address=self.address,
            abi=SinkMetaData.ABI
        )
    
    # Fallback function
    def fallback(self, account: LocalAccount, data: bytes = b'', value: int = 0, gas_limit: int = 500000) -> str:
        """Call the fallback function with optional data and value."""
        tx = {
            'to': self.address,
            'from': account.address,
            'value': value,
            'gas': gas_limit,
            'nonce': self.w3.eth.get_transaction_count(account.address),
            'data': data.hex() if isinstance(data, bytes) else data
        }
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()


def new_sink(w3: Web3, address: str) -> SinkContract:
    """Create a new Sink contract interface."""
    return SinkContract(w3, address)


def deploy_sink(w3: Web3, account: LocalAccount, gas_limit: int = 500000) -> Tuple[str, str]:
    """
    Deploy a new Sink contract.
    
    Returns:
        Tuple of (contract_address, transaction_hash)
    """
    contract_factory = w3.eth.contract(
        abi=SinkMetaData.ABI,
        bytecode=SinkMetaData.BIN
    )
    
    tx = contract_factory.constructor().build_transaction({
        'from': account.address,
        'gas': gas_limit,
        'nonce': w3.eth.get_transaction_count(account.address),
    })
    
    signed_tx = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
    
    # Wait for transaction receipt
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
    
    return receipt.contractAddress, tx_hash.hex()