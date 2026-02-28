"""
ListPolicy contract bindings for Python.
This file is auto-generated - DO NOT EDIT manually.
"""

from __future__ import annotations

from typing import Optional, Dict, Any, List, Tuple
from eth_typing import Address, HexStr, HexAddress
from web3 import Web3
from web3.contract import Contract
from eth_account.signers.local import LocalAccount
from eth_account import Account


class ListPolicyMetaData:
    """Metadata for the ListPolicy contract."""
    
    ABI = [
        {
            "inputs": [],
            "name": "AlreadyWhitelisted",
            "type": "error"
        },
        {
            "inputs": [],
            "name": "InvalidAddress",
            "type": "error"
        },
        {
            "inputs": [],
            "name": "NotThePolicyOwner",
            "type": "error"
        },
        {
            "inputs": [],
            "name": "NotWhitelisted",
            "type": "error"
        },
        {
            "inputs": [{"internalType": "address", "name": "user", "type": "address"}],
            "name": "assignRole",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function"
        },
        {
            "inputs": [{"internalType": "address", "name": "_owner", "type": "address"}],
            "name": "initialize",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "owner",
            "outputs": [{"internalType": "address", "name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [{"internalType": "address", "name": "user", "type": "address"}],
            "name": "revokeRole",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "address", "name": "user", "type": "address"},
                {"internalType": "bytes", "name": "data", "type": "bytes"}
            ],
            "name": "validateAccess",
            "outputs": [{"internalType": "bool", "name": "hasAccess", "type": "bool"}],
            "stateMutability": "view",
            "type": "function"
        }
    ]

    BIN = "0x6080604052348015600e575f5ffd5b506103a78061001c5f395ff3fe608060405234801561000f575f5ffd5b5060043610610055575f3560e01c80635c110a741461005957806365e88c5a1461008157806380e52e3f146100965780638da5cb5b146100a9578063c4d66de8146100d3575b5f5ffd5b61006c6100673660046102d3565b6100e6565b60405190151581526020015b60405180910390f35b61009461008f366004610351565b61012e565b005b6100946100a4366004610351565b6101bd565b5f546100bb906001600160a01b031681565b6040516001600160a01b039091168152602001610078565b6100946100e1366004610351565b610245565b5f6001600160a01b03841661010e5760405163e6c4247b60e01b815260040160405180910390fd5b5050506001600160a01b03165f9081526001602052604090205460ff1690565b5f546001600160a01b03163314610158576040516351604ff560e11b815260040160405180910390fd5b6001600160a01b0381165f9081526001602081905260409091205460ff16151590036101975760405163b73e95e160e01b815260040160405180910390fd5b6001600160a01b03165f908152600160208190526040909120805460ff19169091179055565b5f546001600160a01b031633146101e7576040516351604ff560e11b815260040160405180910390fd5b6001600160a01b0381165f9081526001602081905260409091205460ff1615151461022557604051630b094f2760e31b815260040160405180910390fd5b6001600160a01b03165f908152600160205260409020805460ff19169055565b5f546001600160a01b0316156102975760405162461bcd60e51b8152602060048201526013602482015272105b1c9958591e481a5b9a5d1a585b1a5e9959606a1b604482015260640160405180910390fd5b5f80546001600160a01b0319166001600160a01b0392909216919091179055565b80356001600160a01b03811681146102ce575f5ffd5b919050565b5f5f5f604084860312156102e5575f5ffd5b6102ee846102b8565b9250602084013567ffffffffffffffff811115610309575f5ffd5b8401601f81018613610319575f5ffd5b803567ffffffffffffffff81111561032f575f5ffd5b866020828401011115610340575f5ffd5b939660209190910195509293505050565b5f60208284031215610361575f5ffd5b61036a826102b8565b939250505056fea264697066735822122047472e4c391eccdb2c4a9389b8901ff8ee4eac8e7f69a48928e3085c565ad9aa64736f6c634300081c0033"


class ListPolicyContract:
    """Main class for interacting with the ListPolicy contract."""
    
    def __init__(self, w3: Web3, address: str):
        """Initialize ListPolicy contract interface."""
        self.w3 = w3
        self.address = Web3.to_checksum_address(address)
        self.contract = w3.eth.contract(
            address=self.address,
            abi=ListPolicyMetaData.ABI
        )
    
    # View functions
    def owner(self) -> str:
        """Get the owner of the policy."""
        return self.contract.functions.owner().call()
    
    def validate_access(self, user: str, data: bytes) -> bool:
        """Validate access for a user."""
        return self.contract.functions.validateAccess(user, data).call()
    
    # Transaction functions
    def initialize(self, account: LocalAccount, owner: str, gas_limit: int = 500000) -> str:
        """Initialize the policy with an owner."""
        tx = self.contract.functions.initialize(owner).build_transaction({
            'from': account.address,
            'gas': gas_limit,
            'nonce': self.w3.eth.get_transaction_count(account.address),
        })
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()
    
    def assign_role(self, account: LocalAccount, user: str, gas_limit: int = 500000) -> str:
        """Assign role (whitelist) to a user."""
        tx = self.contract.functions.assignRole(user).build_transaction({
            'from': account.address,
            'gas': gas_limit,
            'nonce': self.w3.eth.get_transaction_count(account.address),
        })
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()
    
    def revoke_role(self, account: LocalAccount, user: str, gas_limit: int = 500000) -> str:
        """Revoke role (remove from whitelist) from a user."""
        tx = self.contract.functions.revokeRole(user).build_transaction({
            'from': account.address,
            'gas': gas_limit,
            'nonce': self.w3.eth.get_transaction_count(account.address),
        })
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()


def new_list_policy(w3: Web3, address: str) -> ListPolicyContract:
    """Create a new ListPolicy contract interface."""
    return ListPolicyContract(w3, address)


def deploy_list_policy(w3: Web3, account: LocalAccount, gas_limit: int = 2000000) -> Tuple[str, str]:
    """
    Deploy a new ListPolicy contract.
    
    Returns:
        Tuple of (contract_address, transaction_hash)
    """
    contract_factory = w3.eth.contract(
        abi=ListPolicyMetaData.ABI,
        bytecode=ListPolicyMetaData.BIN
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