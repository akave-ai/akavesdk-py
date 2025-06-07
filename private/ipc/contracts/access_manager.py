from typing import List, Tuple, Optional, Dict, Any, cast
from eth_typing import HexAddress, HexStr, ChecksumAddress
from web3 import Web3
from web3.types import TxParams, ABI, TxReceipt
from web3.contract import Contract # type: ignore[import-untyped]
import json

class AccessManagerContract:
    """Python bindings for the AccessManager smart contract."""
    
    def __init__(self, web3: Web3, contract_address: HexAddress):
        """Initialize the AccessManager contract interface.
        
        Args:
            web3: Web3 instance
            contract_address: Address of the deployed AccessManager contract
        """
        self.web3 = web3
        self.contract_address = contract_address
        self.checksum_address: ChecksumAddress = self.web3.to_checksum_address(self.contract_address)
        if not self.web3.is_checksum_address(self.checksum_address):
            raise ValueError(f"Invalid contract address: {contract_address}. Must be a valid checksum address.")
        
        # Contract ABI from the Go bindings
        self.abi = [
            {
                "inputs": [
                    {
                        "internalType": "address",
                        "name": "_storageContract",
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
                        "internalType": "bool",
                        "name": "isPublic",
                        "type": "bool"
                    }
                ],
                "name": "changePublicAccess",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes32",
                        "name": "fileId",
                        "type": "bytes32"
                    }
                ],
                "name": "getFileAccessInfo",
                "outputs": [
                    {
                        "internalType": "address",
                        "name": "",
                        "type": "address"
                    },
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
                    }
                ],
                "name": "getPolicy",
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
                        "name": "policyContract",
                        "type": "address"
                    }
                ],
                "name": "setPolicy",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "storageContract",
                "outputs": [
                    {
                        "internalType": "address",
                        "name": "",
                        "type": "address"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            }
        ]

        self.contract: Contract = web3.eth.contract(address=self.checksum_address, abi=self.abi)

    def change_public_access(self, file_id: bytes, is_public: bool, from_address: HexAddress) -> None:
        """Changes the public access status of a file.
        
        Args:
            file_id: ID of the file
            is_public: Whether the file should be publicly accessible
            from_address: Address changing the access
        """
        checksum_from_address: ChecksumAddress = self.web3.to_checksum_address(from_address)
        tx_hash = self.contract.functions.changePublicAccess(file_id, is_public).transact({'from': checksum_from_address})
        self.web3.eth.wait_for_transaction_receipt(tx_hash)

    def get_file_access_info(self, file_id: bytes) -> Tuple[HexAddress, bool]:
        """Gets access information for a file.
        
        Args:
            file_id: ID of the file
            
        Returns:
            Tuple containing (policy contract address, is public)
        """
        result = self.contract.functions.getFileAccessInfo(file_id).call()
        return cast(Tuple[HexAddress, bool], result)

    def get_policy(self, file_id: bytes) -> HexAddress:
        """Gets the policy contract address for a file.
        
        Args:
            file_id: ID of the file
            
        Returns:
            Address of the policy contract
        """
        result = self.contract.functions.getPolicy(file_id).call()
        return cast(HexAddress, result)

    def set_policy(self, file_id: bytes, policy_contract: HexAddress, from_address: HexAddress) -> None:
        """Sets the policy contract for a file.
        
        Args:
            file_id: ID of the file
            policy_contract: Address of the policy contract
            from_address: Address setting the policy
        """
        checksum_from_address: ChecksumAddress = self.web3.to_checksum_address(from_address)
        tx_hash = self.contract.functions.setPolicy(file_id, policy_contract).transact({'from': checksum_from_address})
        self.web3.eth.wait_for_transaction_receipt(tx_hash)

    def get_storage_contract(self) -> HexAddress:
        """Gets the address of the associated storage contract.
        
        Returns:
            Address of the storage contract
        """
        result = self.contract.functions.storageContract().call()
        return cast(HexAddress, result)
