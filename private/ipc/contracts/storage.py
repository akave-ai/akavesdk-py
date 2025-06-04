from typing import List, Tuple, Optional
from eth_typing import HexAddress, HexStr
from web3 import Web3
from web3.contract import Contract
from eth_account import Account
import json

class StorageContract:
    """Python bindings for the Storage smart contract."""
    
    def __init__(self, web3: Web3, contract_address: HexAddress):
        """Initialize the Storage contract interface.
        
        Args:
            web3: Web3 instance
            contract_address: Address of the deployed Storage contract
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
                        "internalType": "string",
                        "name": "bucketName",
                        "type": "string"
                    }
                ],
                "name": "createBucket",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "string",
                        "name": "bucketName",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "fileName",
                        "type": "string"
                    },
                    {
                        "internalType": "bytes32",
                        "name": "fileId",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "uint256",
                        "name": "size",
                        "type": "uint256"
                    }
                ],
                "name": "createFile",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes32",
                        "name": "id",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "string",
                        "name": "name",
                        "type": "string"
                    },
                    {
                        "internalType": "uint256",
                        "name": "index",
                        "type": "uint256"
                    }
                ],
                "name": "deleteBucket",
                "outputs": [
                    {
                        "internalType": "bool",
                        "name": "",
                        "type": "bool"
                    }
                ],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "string",
                        "name": "bucketName",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "fileName",
                        "type": "string"
                    }
                ],
                "name": "deleteFile",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "string",
                        "name": "bucketName",
                        "type": "string"
                    }
                ],
                "name": "getBucket",
                "outputs": [
                    {
                        "components": [
                            {
                                "internalType": "string",
                                "name": "name",
                                "type": "string"
                            },
                            {
                                "internalType": "uint256",
                                "name": "createdAt",
                                "type": "uint256"
                            },
                            {
                                "internalType": "address",
                                "name": "owner",
                                "type": "address"
                            }
                        ],
                        "internalType": "struct Storage.Bucket",
                        "name": "",
                        "type": "tuple"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "string",
                        "name": "bucketName",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "fileName",
                        "type": "string"
                    }
                ],
                "name": "getFile",
                "outputs": [
                    {
                        "components": [
                            {
                                "internalType": "string",
                                "name": "name",
                                "type": "string"
                            },
                            {
                                "internalType": "bytes32",
                                "name": "id",
                                "type": "bytes32"
                            },
                            {
                                "internalType": "uint256",
                                "name": "size",
                                "type": "uint256"
                            },
                            {
                                "internalType": "uint256",
                                "name": "createdAt",
                                "type": "uint256"
                            }
                        ],
                        "internalType": "struct Storage.File",
                        "name": "",
                        "type": "tuple"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "string",
                        "name": "name",
                        "type": "string"
                    },
                    {
                        "internalType": "address",
                        "name": "owner",
                        "type": "address"
                    }
                ],
                "name": "getBucketIndexByName",
                "outputs": [
                    {
                        "internalType": "uint256",
                        "name": "index",
                        "type": "uint256"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            }
        ]
        
        self.contract = web3.eth.contract(address=contract_address, abi=self.abi)

    def get_access_manager(self) -> HexAddress:
        """Gets the address of the associated access manager contract.
        
        Returns:
            Address of the access manager contract
        """
        return self.contract.functions.accessManager().call()

    def create_bucket(self, bucket_name: str, from_address: HexAddress, private_key: str, gas_limit: int = None) -> HexStr:
        """Creates a new bucket.
        
        Args:
            bucket_name: Name of the bucket to create
            from_address: Address creating the bucket
            private_key: Private key for signing the transaction
            gas_limit: Optional gas limit for the transaction. If not provided, will use default.
            
        Returns:
            Transaction hash of the create operation
        """
        # Build transaction
        tx_params = {
            'from': from_address,
            'gasPrice': self.web3.eth.gas_price,
            'nonce': self.web3.eth.get_transaction_count(from_address)
        }
        
        if gas_limit:
            tx_params['gas'] = gas_limit
        else:
            tx_params['gas'] = 500000  # Default gas limit
            
        tx = self.contract.functions.createBucket(bucket_name).build_transaction(tx_params)
        
        # Sign transaction
        signed_tx = Account.sign_transaction(tx, private_key)
        
        # Send raw transaction
        tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        
        # Wait for receipt
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status != 1:
            # Get revert reason if possible
            try:
                self.contract.functions.createBucket(bucket_name).call({
                    'from': from_address
                })
            except Exception as e:
                raise Exception(f"Transaction reverted: {str(e)}")
            raise Exception(f"Transaction failed. Receipt: {receipt}")
        
        return tx_hash.hex()

    def create_file(self, bucket_name: str, file_name: str, file_id: bytes, size: int, from_address: HexAddress, private_key: str) -> None:
        """Creates a new file entry.
        
        Args:
            bucket_name: Name of the bucket containing the file
            file_name: Name of the file
            file_id: Unique ID of the file
            size: Size of the file in bytes
            from_address: Address creating the file
            private_key: Private key for signing the transaction
        """
        # Build transaction
        tx = self.contract.functions.createFile(bucket_name, file_name, file_id, size).build_transaction({
            'from': from_address,
            'gas': 500000,  # Gas limit
            'gasPrice': self.web3.eth.gas_price,
            'nonce': self.web3.eth.get_transaction_count(from_address)
        })
        
        # Sign transaction
        signed_tx = Account.sign_transaction(tx, private_key)
        
        # Send raw transaction
        tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        
        # Wait for receipt
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status != 1:
            raise Exception("Transaction failed")

    def commit_file(self, bucket_name: str, file_name: str, size: int, root_cid: bytes, from_address: HexAddress, private_key: str) -> None:
        """Updates the file metadata after upload (size, root CID).
        
        Args:
            bucket_name: Name of the bucket containing the file
            file_name: Name of the file
            size: Final size of the file in bytes
            root_cid: Root CID of the uploaded file
            from_address: Address committing the file
            private_key: Private key for signing the transaction
        """
        # Assume a contract function like 'commitFile' or 'updateFileMetadata' exists
        # Adding 'commitFile' based on Go SDK patterns
        function_name = 'commitFile' # Adjust if contract ABI uses a different name
        
        try:
            contract_function = getattr(self.contract.functions, function_name)
        except AttributeError:
            raise NotImplementedError(f"Contract function '{function_name}' not found in ABI")
            
        # Build transaction
        tx = contract_function(bucket_name, file_name, size, root_cid).build_transaction({
            'from': from_address,
            'gas': 500000,  # Gas limit (adjust as needed)
            'gasPrice': self.web3.eth.gas_price,
            'nonce': self.web3.eth.get_transaction_count(from_address)
        })
        
        # Sign transaction
        signed_tx = Account.sign_transaction(tx, private_key)
        
        # Send raw transaction
        tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        
        # Wait for receipt
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status != 1:
            raise Exception(f"Transaction failed for {function_name}")

    def delete_bucket(self, bucket_name: str, from_address: HexAddress, private_key: str, bucket_id_hex: str = None) -> HexStr:
        """Deletes a bucket.
        
        Args:
            bucket_name: Name of the bucket to delete
            from_address: Address deleting the bucket
            private_key: Private key for signing the transaction
            bucket_id_hex: Hex string bucket ID from IPC BucketView response (e.g., "a1b2c3d4...")
            
        Returns:
            Transaction hash of the delete operation
            
        Raises:
            Exception: If the transaction fails or is reverted
        """
        if not bucket_id_hex:
            raise Exception("bucket_id_hex is required - get it from IPC BucketView response")
            
        try:
            # Convert hex string to bytes32 like Go SDK does: hex.DecodeString(bucket.Id)
            if bucket_id_hex.startswith('0x'):
                bucket_id_hex = bucket_id_hex[2:]
            
            # Ensure we have exactly 32 bytes (64 hex chars)
            if len(bucket_id_hex) != 64:
                bucket_id_hex = bucket_id_hex.ljust(64, '0')  # Pad with zeros if needed
                
            bucket_id_bytes = bytes.fromhex(bucket_id_hex)
            print(f"Using bucket_id from IPC: 0x{bucket_id_hex}")
            
            # Get bucket index by name and owner (like Go SDK)
            try:
                bucket_index = self.contract.functions.getBucketIndexByName(bucket_name, from_address).call()
                print(f"Got bucket_index: {bucket_index}")
            except Exception as e:
                print(f"getBucketIndexByName failed: {e}")
                raise Exception(f"Failed to get bucket index: {str(e)}")
            
        except Exception as e:
            raise Exception(f"Failed to prepare bucket deletion: {str(e)}")

        # Build transaction parameters - use standard legacy transaction
        tx_params = {
            'from': from_address,
            'gas': 500000,  # Gas limit
            'gasPrice': self.web3.eth.gas_price,
            'nonce': self.web3.eth.get_transaction_count(from_address),
        }
        
        try:
            print(f"Calling deleteBucket with:")
            print(f"  bucket_id: 0x{bucket_id_hex}")
            print(f"  bucket_name: {bucket_name}")
            print(f"  bucket_index: {bucket_index}")
            print(f"  from_address: {from_address}")
            
            # Try to call the function first to see if it would revert
            try:
                self.contract.functions.deleteBucket(
                    bucket_id_bytes,      # bytes32 id (from IPC BucketView response)
                    bucket_name,          # string name  
                    bucket_index          # uint256 index
                ).call({'from': from_address})
                print("deleteBucket call simulation succeeded")
            except Exception as call_error:
                print(f"deleteBucket call simulation failed: {call_error}")
                
                # Try to decode the revert reason
                error_str = str(call_error)
                if "execution reverted" in error_str.lower():
                    # Extract any hex data from the error
                    import re
                    hex_match = re.search(r'0x[a-fA-F0-9]+', error_str)
                    if hex_match:
                        error_data = hex_match.group()
                        print(f"Revert data: {error_data}")
                        
                        # Try to decode common error selectors
                        if error_data.startswith('0x938a92b7'):
                            print("Error: Bucket not found or doesn't exist")
                        elif error_data.startswith('0x08c379a0'):
                            # Standard revert reason
                            try:
                                from eth_abi import decode_single
                                reason = decode_single('string', bytes.fromhex(error_data[10:]))
                                print(f"Revert reason: {reason}")
                            except:
                                print(f"Could not decode revert reason from: {error_data}")
                
                raise Exception(f"Contract call simulation failed: {str(call_error)}")
            
            # Build and send the transaction
            tx = self.contract.functions.deleteBucket(
                bucket_id_bytes,      # bytes32 id (from IPC BucketView response)
                bucket_name,          # string name  
                bucket_index          # uint256 index
            ).build_transaction(tx_params)
            
            print(f"Built transaction")
            
            # Sign transaction
            signed_tx = self.web3.eth.account.sign_transaction(tx, private_key)
            
            # Send transaction
            tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
            print(f"Transaction sent: {tx_hash.hex()}")
            
            # Wait for receipt
            receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"Transaction receipt: status={receipt.status}, gasUsed={receipt.gasUsed}")
            
            if receipt.status != 1:
                raise Exception(f"Transaction failed with status: {receipt.status}")
                
            return tx_hash.hex()
            
        except Exception as e:
            raise Exception(f"Failed to delete bucket: {str(e)}")

    def delete_file(self, bucket_name: str, file_name: str, from_address: HexAddress, private_key: str) -> None:
        """Deletes a file.
        
        Args:
            bucket_name: Name of the bucket containing the file
            file_name: Name of the file to delete
            from_address: Address deleting the file
            private_key: Private key for signing the transaction
        """
        # Build transaction
        tx = self.contract.functions.deleteFile(bucket_name, file_name).build_transaction({
            'from': from_address,
            'gas': 500000,  # Gas limit
            'gasPrice': self.web3.eth.gas_price,
            'nonce': self.web3.eth.get_transaction_count(from_address)
        })
        
        # Sign transaction
        signed_tx = Account.sign_transaction(tx, private_key)
        
        # Send raw transaction
        tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        
        # Wait for receipt
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status != 1:
            raise Exception("Transaction failed")

    def get_bucket(self, bucket_name: str) -> Tuple[str, int, HexAddress]:
        """Gets bucket information.
        
        Args:
            bucket_name: Name of the bucket
            
        Returns:
            Tuple containing (bucket_name, created_at_timestamp, owner_address)
        """
        return self.contract.functions.getBucket(bucket_name).call()

    def get_file(self, bucket_name: str, file_name: str) -> Tuple[str, bytes, int, int]:
        """Gets file information.
        
        Args:
            bucket_name: Name of the bucket containing the file
            file_name: Name of the file
            
        Returns:
            Tuple containing (file_name, file_id, size, created_at_timestamp)
        """
        return self.contract.functions.getFile(bucket_name, file_name).call()
