from typing import List, Tuple, Optional
from eth_typing import HexAddress, HexStr
from web3 import Web3
from web3.contract import Contract
import json

class PolicyContract:
    """Python bindings for the Policy smart contract."""
    
    def __init__(self, web3: Web3, contract_address: HexAddress):
        """Initialize the Policy contract interface.
        
        Args:
            web3: Web3 instance
            contract_address: Address of the deployed Policy contract
        """
        self.web3 = web3
        self.contract_address = contract_address
        
        # Contract ABI from the Go bindings
        self.abi = [
            {
                "inputs": [
                    {
                        "internalType": "address",
                        "name": "_accessManager",
                        "type": "address"
                    }
                ],
                "stateMutability": "nonpayable",
                "type": "constructor"
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes32",
                        "name": "fileId",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "address",
                        "name": "user",
                        "type": "address"
                    }
                ],
                "name": "addUserAccess",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "accessManager",
                "outputs": [
                    {
                        "internalType": "address",
                        "name": "",
                        "type": "address"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes32",
                        "name": "fileId",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "address",
                        "name": "user",
                        "type": "address"
                    }
                ],
                "name": "hasAccess",
                "outputs": [
                    {
                        "internalType": "bool",
                        "name": "",
                        "type": "bool"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes32",
                        "name": "fileId",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "address",
                        "name": "user",
                        "type": "address"
                    }
                ],
                "name": "removeUserAccess",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            }
        ]
        
        self.contract = web3.eth.contract(address=contract_address, abi=self.abi)

    def add_user_access(self, file_id: bytes, user: HexAddress, from_address: HexAddress) -> None:
        """Grants access to a file for a specific user.
        
        Args:
            file_id: ID of the file
            user: Address of the user to grant access to
            from_address: Address granting the access
        """
        tx_hash = self.contract.functions.addUserAccess(file_id, user).transact({'from': from_address})
        self.web3.eth.wait_for_transaction_receipt(tx_hash)

    def get_access_manager(self) -> HexAddress:
        """Gets the address of the associated access manager contract.
        
        Returns:
            Address of the access manager contract
        """
        return self.contract.functions.accessManager().call()

    def has_access(self, file_id: bytes, user: HexAddress) -> bool:
        """Checks if a user has access to a file.
        
        Args:
            file_id: ID of the file
            user: Address of the user to check access for
            
        Returns:
            True if the user has access, False otherwise
        """
        return self.contract.functions.hasAccess(file_id, user).call()

    def remove_user_access(self, file_id: bytes, user: HexAddress, from_address: HexAddress) -> None:
        """Revokes access to a file for a specific user.
        
        Args:
            file_id: ID of the file
            user: Address of the user to revoke access from
            from_address: Address revoking the access
        """
        tx_hash = self.contract.functions.removeUserAccess(file_id, user).transact({'from': from_address})
        self.web3.eth.wait_for_transaction_receipt(tx_hash) 