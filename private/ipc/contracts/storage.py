from typing import List, Tuple, Optional, cast, Dict
from eth_typing import HexAddress, HexStr, ChecksumAddress
from web3 import Web3
from web3.types import TxParams, TxReceipt
from web3.contract import Contract # type: ignore[import-untyped]
from eth_account import Account

def get_raw_transaction(signed_tx):
    if hasattr(signed_tx, 'raw_transaction'):
        return signed_tx.raw_transaction  # web3 v7+
    elif hasattr(signed_tx, 'rawTransaction'):
        return signed_tx.rawTransaction   # web3 v6
    else:
        raise AttributeError("SignedTransaction has neither raw_transaction nor rawTransaction attribute")
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

        # Convert to checksum address for web3.py contract interaction
        self.checksum_address: ChecksumAddress = self.web3.to_checksum_address(self.contract_address)
        
        # Contract ABI from the Go bindings
        self.abi: List[Dict] = [
            {
                "inputs": [
                    {
                        "internalType": "address",
                        "name": "tokenAddress",
                        "type": "address"
                    }
                ],
                "stateMutability": "nonpayable",
                "type": "constructor"
            },
            {
                "inputs": [],
                "name": "BlockAlreadyExists",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "BlockAlreadyFilled",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "BlockInvalid",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "BlockNonexists",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "BucketAlreadyExists",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "BucketInvalid",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "BucketInvalidOwner",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "BucketNonempty",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "BucketNonexists",
                "type": "error"
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes",
                        "name": "fileCID",
                        "type": "bytes"
                    }
                ],
                "name": "ChunkCIDMismatch",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "ECDSAInvalidSignature",
                "type": "error"
            },
            {
                "inputs": [
                    {
                        "internalType": "uint256",
                        "name": "length",
                        "type": "uint256"
                    }
                ],
                "name": "ECDSAInvalidSignatureLength",
                "type": "error"
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes32",
                        "name": "s",
                        "type": "bytes32"
                    }
                ],
                "name": "ECDSAInvalidSignatureS",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "FileAlreadyExists",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "FileChunkDuplicate",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "FileFullyUploaded",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "FileInvalid",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "FileNameDuplicate",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "FileNonempty",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "FileNotExists",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "FileNotFilled",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "IndexMismatch",
                "type": "error"
            },
            {
                "inputs": [
                    {
                        "internalType": "uint256",
                        "name": "cidsLength",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "sizesLength",
                        "type": "uint256"
                    }
                ],
                "name": "InvalidArrayLength",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "InvalidBlockIndex",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "InvalidBlocksAmount",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "InvalidEncodedSize",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "InvalidFileBlocksCount",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "InvalidFileCID",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "InvalidLastBlockSize",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "InvalidShortString",
                "type": "error"
            },
            {
                "inputs": [],
                "name": "LastChunkDuplicate",
                "type": "error"
            },
            {
                "inputs": [
                    {
                        "internalType": "string",
                        "name": "str",
                        "type": "string"
                    }
                ],
                "name": "StringTooLong",
                "type": "error"
            },
            {
                "anonymous": False,
                "inputs": [
                    {
                        "indexed": True,
                        "internalType": "bytes32",
                        "name": "id",
                        "type": "bytes32"
                    },
                    {
                        "indexed": True,
                        "internalType": "bytes32",
                        "name": "bucketId",
                        "type": "bytes32"
                    },
                    {
                        "indexed": True,
                        "internalType": "string",
                        "name": "name",
                        "type": "string"
                    },
                    {
                        "indexed": False,
                        "internalType": "address",
                        "name": "owner",
                        "type": "address"
                    }
                ],
                "name": "AddFile",
                "type": "event"
            },
            {
                "inputs": [],
                "name": "accessManager",
                "outputs": [
                    {
                        "internalType": "contract IAccessManager",
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
                        "name": "name",
                        "type": "string"
                    }
                ],
                "name": "createBucket",
                "outputs": [
                    {
                        "internalType": "bytes32",
                        "name": "id",
                        "type": "bytes32"
                    }
                ],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes32",
                        "name": "bucketId",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "string",
                        "name": "name",
                        "type": "string"
                    }
                ],
                "name": "createFile",
                "outputs": [
                    {
                        "internalType": "bytes32",
                        "name": "",
                        "type": "bytes32"
                    }
                ],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes32",
                        "name": "bucketId",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "string",
                        "name": "name",
                        "type": "string"
                    },
                    {
                        "internalType": "uint256",
                        "name": "encodedFileSize",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "actualSize",
                        "type": "uint256"
                    },
                    {
                        "internalType": "bytes",
                        "name": "fileCID",
                        "type": "bytes"
                    }
                ],
                "name": "commitFile",
                "outputs": [
                    {
                        "internalType": "bytes32",
                        "name": "",
                        "type": "bytes32"
                    }
                ],
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
                        "internalType": "bytes32",
                        "name": "fileID",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "bytes32",
                        "name": "bucketId",
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
                "name": "deleteFile",
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
                        "name": "name",
                        "type": "string"
                    }
                ],
                "name": "getBucketByName",
                "outputs": [
                    {
                        "components": [
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
                                "name": "createdAt",
                                "type": "uint256"
                            },
                            {
                                "internalType": "address",
                                "name": "owner",
                                "type": "address"
                            },
                            {
                                "internalType": "bytes32[]",
                                "name": "files",
                                "type": "bytes32[]"
                            }
                        ],
                        "internalType": "struct IStorage.Bucket",
                        "name": "bucket",
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
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes32",
                        "name": "bucketId",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "string",
                        "name": "name",
                        "type": "string"
                    }
                ],
                "name": "getFileByName",
                "outputs": [
                    {
                        "components": [
                            {
                                "internalType": "bytes32",
                                "name": "id",
                                "type": "bytes32"
                            },
                            {
                                "internalType": "bytes",
                                "name": "fileCID",
                                "type": "bytes"
                            },
                            {
                                "internalType": "bytes32",
                                "name": "bucketId",
                                "type": "bytes32"
                            },
                            {
                                "internalType": "string",
                                "name": "name",
                                "type": "string"
                            },
                            {
                                "internalType": "uint256",
                                "name": "encodedSize",
                                "type": "uint256"
                            },
                            {
                                "internalType": "uint256",
                                "name": "createdAt",
                                "type": "uint256"
                            },
                            {
                                "internalType": "uint256",
                                "name": "actualSize",
                                "type": "uint256"
                            },
                            {
                                "components": [
                                    {
                                        "internalType": "bytes[]",
                                        "name": "chunkCIDs",
                                        "type": "bytes[]"
                                    },
                                    {
                                        "internalType": "uint256[]",
                                        "name": "chunkSize",
                                        "type": "uint256[]"
                                    }
                                ],
                                "internalType": "struct IStorage.Chunk",
                                "name": "chunks",
                                "type": "tuple"
                            }
                        ],
                        "internalType": "struct IStorage.File",
                        "name": "file",
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
                        "internalType": "bytes32",
                        "name": "fileId",
                        "type": "bytes32"
                    }
                ],
                "name": "getFileIndexById",
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
        
        self.contract = web3.eth.contract(address=self.checksum_address, abi=self.abi)

    def get_access_manager(self) -> HexAddress:
        """Gets the address of the associated access manager contract.
        
        Returns:
            Address of the access manager contract
        """
        result = self.contract.functions.accessManager().call()
        return cast(HexAddress, result)

    def create_bucket(self, bucket_name: str, from_address: HexAddress, private_key: str, gas_limit: Optional[int] = None) -> HexStr:
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
        checksum_from_address: ChecksumAddress = self.web3.to_checksum_address(from_address)
        tx_params: TxParams = {
            'from': checksum_from_address,
            'gasPrice': self.web3.eth.gas_price,
            'nonce': self.web3.eth.get_transaction_count(checksum_from_address)
        }
        
        if gas_limit:
            tx_params['gas'] = gas_limit
        else:
            tx_params['gas'] = 500000  # Default gas limit
            
        tx = self.contract.functions.createBucket(bucket_name).build_transaction(tx_params)
        
        # Sign transaction
        signed_tx = self.web3.eth.account.sign_transaction(tx, private_key)
        
        # Send raw transaction
        tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        
        # Wait for receipt
        receipt: TxReceipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt['status'] != 1:
            # Get revert reason if possible
            try:
                self.contract.functions.createBucket(bucket_name).call({
                    'from': from_address
                })
            except Exception as e:
                raise Exception(f"Transaction reverted: {str(e)}")
            raise Exception(f"Transaction failed. Receipt: {receipt}")
        
        return cast(HexStr, tx_hash.hex())

    def create_file(self, bucket_name: str, file_name: str, file_id: bytes, size: int, from_address: HexAddress, private_key: str) -> None:
        """Creates a new file entry using the new ABI signature.
        
        Args:
            bucket_name: Name of the bucket containing the file
            file_name: Name of the file
            file_id: Unique ID of the file (not used in new ABI)
            size: Size of the file in bytes (not used in new ABI)
            from_address: Address creating the file
            private_key: Private key for signing the transaction
        """
        # First get bucket to get bucket ID
        bucket = self.contract.functions.getBucketByName(bucket_name).call()
        bucket_id = bucket[0]  # bytes32 id
        
        # Build transaction with new signature: createFile(bucketId, name)
        checksum_from_address: ChecksumAddress = self.web3.to_checksum_address(from_address)
        tx = self.contract.functions.createFile(bucket_id, file_name).build_transaction({
            'from': from_address,
            'gas': 500000,  # Gas limit
            'gasPrice': self.web3.eth.gas_price,
            'nonce': self.web3.eth.get_transaction_count(checksum_from_address)
        })
        
        # Sign transaction
        signed_tx = self.web3.eth.account.sign_transaction(tx, private_key)
        
        # Send raw transaction
        tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        
        # Wait for receipt
        receipt: TxReceipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt['status'] != 1:
            raise Exception("Transaction failed")

    def commit_file(self, bucket_name: str, file_name: str, size: int, root_cid: bytes, from_address: HexAddress, private_key: str) -> None:
        """Updates the file metadata after upload using new ABI signature.
        
        Args:
            bucket_name: Name of the bucket containing the file
            file_name: Name of the file
            size: Final size of the file in bytes
            root_cid: Root CID of the uploaded file
            from_address: Address committing the file
            private_key: Private key for signing the transaction
        """
        # First get bucket to get bucket ID
        bucket = self.contract.functions.getBucketByName(bucket_name).call()
        bucket_id = bucket[0]  # bytes32 id
        
        # commitFile signature: commitFile(bucketId, name, encodedFileSize, actualSize, fileCID)
        # We'll use size for both encodedFileSize and actualSize
        checksum_from_address: ChecksumAddress = self.web3.to_checksum_address(from_address)
        tx = self.contract.functions.commitFile(bucket_id, file_name, size, size, root_cid).build_transaction({
            'from': from_address,
            'gas': 500000,  # Gas limit (adjust as needed)
            'gasPrice': self.web3.eth.gas_price,
            'nonce': self.web3.eth.get_transaction_count(checksum_from_address)
        })
        
        # Sign transaction
        signed_tx = self.web3.eth.account.sign_transaction(tx, private_key)

        # Send raw transaction
        tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        
        # Wait for receipt
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt['status'] != 1:
            raise Exception("Transaction failed for commitFile")

    def delete_bucket(self, bucket_name: str, from_address: HexAddress, private_key: str, bucket_id_hex: Optional[str] = None) -> HexStr:
        if not bucket_id_hex:
            raise Exception("bucket_id_hex is required - get it from IPC BucketView response")
            
        try:
            if bucket_id_hex.startswith('0x'):
                bucket_id_hex = bucket_id_hex[2:]
            
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
        checksum_from_address: ChecksumAddress = self.web3.to_checksum_address(from_address)
        tx_params: TxParams = {
            'from': checksum_from_address,
            'gas': 500000,  # Gas limit
            'gasPrice': self.web3.eth.gas_price,
            'nonce': self.web3.eth.get_transaction_count(checksum_from_address),
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
                                from eth_abi import decode as decode_single # type: ignore[attr-defined]
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
            tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
            print(f"Transaction sent: {tx_hash.hex()}")
            
            # Wait for receipt
            receipt: TxReceipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"Transaction receipt: status={receipt['status']}, gasUsed={receipt['gasUsed']}")

            if receipt['status'] != 1:
                raise Exception(f"Transaction failed with status: {receipt['status']}")

            return cast(HexStr, tx_hash.hex())
            
        except Exception as e:
            raise Exception(f"Failed to delete bucket: {str(e)}")

    def delete_file(self, auth, file_id: bytes, bucket_id: bytes, file_name: str, file_index: int) -> str:
        
        # Build transaction
        tx = self.contract.functions.deleteFile(file_id, bucket_id, file_name, file_index).build_transaction({
            'from': auth.address,
            'gas': 500000,  # Gas limit
            'gasPrice': self.web3.eth.gas_price,
            'nonce': self.web3.eth.get_transaction_count(auth.address)
        })
        signed_tx = self.web3.eth.account.sign_transaction(tx, auth.key)
        
        tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt['status'] != 1:
            raise Exception("Transaction failed")
            
        return tx_hash.hex()

    def get_bucket(self, bucket_name: str) -> Tuple[str, int, HexAddress]:
        bucket = self.contract.functions.getBucketByName(bucket_name).call()
        return (bucket[1], bucket[2], bucket[3])  # (name, createdAt, owner)

    def get_file(self, bucket_name: str, file_name: str) -> Tuple[str, bytes, int, int]:
        
        bucket = self.contract.functions.getBucketByName(bucket_name).call()
        bucket_id = bucket[0]  # bytes32 id
        file_info = self.contract.functions.getFileByName(bucket_id, file_name).call()
        return (file_info[3], file_info[0], file_info[4], file_info[5])  # (name, id, encodedSize, createdAt)

    def get_bucket_by_name(self, call_opts: dict, bucket_name: str):
        if call_opts:
            return self.contract.functions.getBucketByName(bucket_name).call(call_opts)
        else:
            return self.contract.functions.getBucketByName(bucket_name).call()

    def get_file_by_name(self, call_opts: dict, bucket_id: bytes, file_name: str):
        if call_opts:
            return self.contract.functions.getFileByName(bucket_id, file_name).call(call_opts)
        else:
            return self.contract.functions.getFileByName(bucket_id, file_name).call()

    def get_file_index_by_id(self, call_opts: dict, file_name: str, bucket_id: bytes):
        if call_opts:
            return self.contract.functions.getFileIndexById(file_name, bucket_id).call(call_opts)
        else:
            return self.contract.functions.getFileIndexById(file_name, bucket_id).call()
