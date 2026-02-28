from typing import List, Tuple, Optional
from eth_typing import HexAddress, HexStr
from web3 import Web3
from web3.contract import Contract
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
        
        # Contract ABI from the Go bindings
        self.abi = [
            {
                "inputs": [],
                "stateMutability": "nonpayable",
                "type": "constructor"
            },
            {
                "inputs": [
                    {
                        "internalType": "address",
                        "name": "target",
                        "type": "address"
                    }
                ],
                "name": "AddressEmptyCode",
                "type": "error"
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
                        "internalType": "string",
                        "name": "name",
                        "type": "string"
                    },
                    {
                        "indexed": True,
                        "internalType": "address",
                        "name": "owner",
                        "type": "address"
                    }
                ],
                "name": "CreateBucket",
                "type": "event"
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
                "name": "CreateFile",
                "type": "event"
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
                        "internalType": "string",
                        "name": "name",
                        "type": "string"
                    },
                    {
                        "indexed": True,
                        "internalType": "address",
                        "name": "owner",
                        "type": "address"
                    }
                ],
                "name": "DeleteBucket",
                "type": "event"
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
                "name": "DeleteFile",
                "type": "event"
            },
            {
                "anonymous": False,
                "inputs": [
                    {
                        "indexed": True,
                        "internalType": "bytes32",
                        "name": "blockId",
                        "type": "bytes32"
                    },
                    {
                        "indexed": True,
                        "internalType": "bytes",
                        "name": "peerId",
                        "type": "bytes"
                    }
                ],
                "name": "DeletePeerBlock",
                "type": "event"
            },
            {
                "anonymous": False,
                "inputs": [],
                "name": "EIP712DomainChanged",
                "type": "event"
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
                "name": "FileUploaded",
                "type": "event"
            },
            {
                "inputs": [],
                "name": "MAX_BLOCKS_PER_FILE",
                "outputs": [
                    {
                        "internalType": "uint64",
                        "name": "",
                        "type": "uint64"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "MAX_BLOCK_SIZE",
                "outputs": [
                    {
                        "internalType": "uint64",
                        "name": "",
                        "type": "uint64"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
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
                        "internalType": "bytes",
                        "name": "cid",
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
                        "name": "encodedChunkSize",
                        "type": "uint256"
                    },
                    {
                        "internalType": "bytes32[]",
                        "name": "cids",
                        "type": "bytes32[]"
                    },
                    {
                        "internalType": "uint256[]",
                        "name": "chunkBlocksSizes",
                        "type": "uint256[]"
                    },
                    {
                        "internalType": "uint256",
                        "name": "chunkIndex",
                        "type": "uint256"
                    }
                ],
                "name": "addFileChunk",
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
                        "internalType": "bytes",
                        "name": "peerId",
                        "type": "bytes"
                    },
                    {
                        "internalType": "bytes32",
                        "name": "cid",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "bool",
                        "name": "isReplica",
                        "type": "bool"
                    }
                ],
                "name": "addPeerBlock",
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
                        "name": "bucketName",
                        "type": "string"
                    },
                    {
                        "internalType": "address",
                        "name": "owner",
                        "type": "address"
                    },
                    {
                        "internalType": "uint256",
                        "name": "fileOffset",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "fileLimit",
                        "type": "uint256"
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
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes32",
                        "name": "fileId",
                        "type": "bytes32"
                    }
                ],
                "name": "isFileFilled",
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
                    }
                ],
                "name": "isFileFilledV2",
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
                "inputs": [],
                "name": "UPGRADE_INTERFACE_VERSION",
                "outputs": [
                    {
                        "internalType": "string",
                        "name": "",
                        "type": "string"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes[]",
                        "name": "cids",
                        "type": "bytes[]"
                    },
                    {
                        "internalType": "bytes32",
                        "name": "bucketId",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "string",
                        "name": "fileName",
                        "type": "string"
                    },
                    {
                        "internalType": "uint256[]",
                        "name": "encodedChunkSizes",
                        "type": "uint256[]"
                    },
                    {
                        "internalType": "bytes32[][]",
                        "name": "chunkBlocksCIDs",
                        "type": "bytes32[][]"
                    },
                    {
                        "internalType": "uint256[][]",
                        "name": "chunkBlockSizes",
                        "type": "uint256[][]"
                    },
                    {
                        "internalType": "uint256",
                        "name": "startingChunkIndex",
                        "type": "uint256"
                    }
                ],
                "name": "addFileChunks",
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
                    }
                ],
                "name": "getFileById",
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
                        "name": "bucketName",
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
                    },
                    {
                        "internalType": "bool",
                        "name": "exists",
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
                        "internalType": "uint256",
                        "name": "chunkIndex",
                        "type": "uint256"
                    }
                ],
                "name": "isChunkFilled",
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
                        "internalType": "uint256",
                        "name": "blockIndex",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "chunkIndex",
                        "type": "uint256"
                    }
                ],
                "name": "isBlockFilled",
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
                "inputs": [],
                "name": "version",
                "outputs": [
                    {
                        "internalType": "string",
                        "name": "",
                        "type": "string"
                    }
                ],
                "stateMutability": "pure",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "getChainID",
                "outputs": [
                    {
                        "internalType": "uint256",
                        "name": "",
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
                        "name": "id",
                        "type": "bytes32"
                    }
                ],
                "name": "getFileOwner",
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
                        "internalType": "bytes32[]",
                        "name": "ids",
                        "type": "bytes32[]"
                    }
                ],
                "name": "getBucketsByIds",
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
                        "internalType": "struct IStorage.Bucket[]",
                        "name": "",
                        "type": "tuple[]"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "address",
                        "name": "owner",
                        "type": "address"
                    },
                    {
                        "internalType": "uint256",
                        "name": "offset",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "limit",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "fileOffset",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "fileLimit",
                        "type": "uint256"
                    }
                ],
                "name": "getOwnerBuckets",
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
                        "internalType": "struct IStorage.Bucket[]",
                        "name": "buckets",
                        "type": "tuple[]"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "address",
                        "name": "tokenAddress",
                        "type": "address"
                    }
                ],
                "name": "initialize",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "timestamp",
                "outputs": [
                    {
                        "internalType": "uint256",
                        "name": "",
                        "type": "uint256"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "token",
                "outputs": [
                    {
                        "internalType": "contractIAkaveToken",
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
                        "name": "peerId",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "bytes32",
                        "name": "cid",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "string",
                        "name": "fileName",
                        "type": "string"
                    },
                    {
                        "internalType": "bool",
                        "name": "isReplica",
                        "type": "bool"
                    }
                ],
                "name": "addPeerBlock",
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
                        "name": "id",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "bytes32",
                        "name": "peerId",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "bytes32",
                        "name": "cid",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "string",
                        "name": "fileName",
                        "type": "string"
                    },
                    {
                        "internalType": "uint256",
                        "name": "index",
                        "type": "uint256"
                    }
                ],
                "name": "deletePeerBlock",
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
                        "components": [
                            {
                                "internalType": "bytes32",
                                "name": "blockCID",
                                "type": "bytes32"
                            },
                            {
                                "internalType": "bytes32",
                                "name": "nodeId",
                                "type": "bytes32"
                            },
                            {
                                "internalType": "bytes32",
                                "name": "bucketId",
                                "type": "bytes32"
                            },
                            {
                                "internalType": "uint256",
                                "name": "chunkIndex",
                                "type": "uint256"
                            },
                            {
                                "internalType": "uint256",
                                "name": "nonce",
                                "type": "uint256"
                            },
                            {
                                "internalType": "uint256",
                                "name": "blockIndex",
                                "type": "uint256"
                            },
                            {
                                "internalType": "string",
                                "name": "fileName",
                                "type": "string"
                            },
                            {
                                "internalType": "bytes",
                                "name": "signature",
                                "type": "bytes"
                            },
                            {
                                "internalType": "uint256",
                                "name": "deadline",
                                "type": "uint256"
                            }
                        ],
                        "internalType": "struct IStorage.FillChunkBlockArgs",
                        "name": "args",
                        "type": "tuple"
                    }
                ],
                "name": "fillChunkBlock",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "components": [
                            {
                                "internalType": "bytes32",
                                "name": "blockCID",
                                "type": "bytes32"
                            },
                            {
                                "internalType": "bytes32",
                                "name": "nodeId",
                                "type": "bytes32"
                            },
                            {
                                "internalType": "bytes32",
                                "name": "bucketId",
                                "type": "bytes32"
                            },
                            {
                                "internalType": "uint256",
                                "name": "chunkIndex",
                                "type": "uint256"
                            },
                            {
                                "internalType": "uint256",
                                "name": "nonce",
                                "type": "uint256"
                            },
                            {
                                "internalType": "uint256",
                                "name": "blockIndex",
                                "type": "uint256"
                            },
                            {
                                "internalType": "string",
                                "name": "fileName",
                                "type": "string"
                            },
                            {
                                "internalType": "bytes",
                                "name": "signature",
                                "type": "bytes"
                            },
                            {
                                "internalType": "uint256",
                                "name": "deadline",
                                "type": "uint256"
                            }
                        ],
                        "internalType": "struct IStorage.FillChunkBlockArgs[]",
                        "name": "args",
                        "type": "tuple[]"
                    }
                ],
                "name": "fillChunkBlocks",
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
                        "internalType": "uint256",
                        "name": "index",
                        "type": "uint256"
                    }
                ],
                "name": "getChunkByIndex",
                "outputs": [
                    {
                        "internalType": "bytes",
                        "name": "",
                        "type": "bytes"
                    },
                    {
                        "internalType": "uint256",
                        "name": "",
                        "type": "uint256"
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
                    },
                    {
                        "internalType": "bytes32",
                        "name": "bucketId",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "address",
                        "name": "owner",
                        "type": "address"
                    }
                ],
                "name": "getFullFileInfo",
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
                    },
                    {
                        "internalType": "uint256",
                        "name": "index",
                        "type": "uint256"
                    },
                    {
                        "internalType": "bool",
                        "name": "exists",
                        "type": "bool"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes32[]",
                        "name": "ids",
                        "type": "bytes32[]"
                    },
                    {
                        "internalType": "uint256",
                        "name": "bucketOffset",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "bucketLimit",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "fileOffset",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "fileLimit",
                        "type": "uint256"
                    }
                ],
                "name": "getBucketsByIdsWithFiles",
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
                        "internalType": "struct IStorage.Bucket[]",
                        "name": "buckets",
                        "type": "tuple[]"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes32",
                        "name": "peerId",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "bytes32",
                        "name": "cid",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "string",
                        "name": "fileName",
                        "type": "string"
                    }
                ],
                "name": "getPeerBlockIndexById",
                "outputs": [
                    {
                        "internalType": "uint256",
                        "name": "index",
                        "type": "uint256"
                    },
                    {
                        "internalType": "bool",
                        "name": "exists",
                        "type": "bool"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes32[]",
                        "name": "cids",
                        "type": "bytes32[]"
                    },
                    {
                        "internalType": "string",
                        "name": "fileName",
                        "type": "string"
                    }
                ],
                "name": "getPeersArrayByPeerBlockCid",
                "outputs": [
                    {
                        "internalType": "bytes32[][]",
                        "name": "peers",
                        "type": "bytes32[][]"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes32",
                        "name": "cid",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "string",
                        "name": "fileName",
                        "type": "string"
                    }
                ],
                "name": "getPeersByPeerBlockCid",
                "outputs": [
                    {
                        "internalType": "bytes32[]",
                        "name": "peers",
                        "type": "bytes32[]"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes32",
                        "name": "cid",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "bytes32",
                        "name": "peerId",
                        "type": "bytes32"
                    }
                ],
                "name": "isPeerBlockReplica",
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
                        "name": "",
                        "type": "bytes32"
                    }
                ],
                "name": "fileFillCounter",
                "outputs": [
                    {
                        "internalType": "uint16",
                        "name": "",
                        "type": "uint16"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "bytes32",
                        "name": "",
                        "type": "bytes32"
                    }
                ],
                "name": "fileRewardClaimed",
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
                        "name": "",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "uint256",
                        "name": "",
                        "type": "uint256"
                    }
                ],
                "name": "fulfilledBlocks",
                "outputs": [
                    {
                        "internalType": "uint32",
                        "name": "",
                        "type": "uint32"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "eip712Domain",
                "outputs": [
                    {
                        "internalType": "bytes1",
                        "name": "fields",
                        "type": "bytes1"
                    },
                    {
                        "internalType": "string",
                        "name": "name",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "version",
                        "type": "string"
                    },
                    {
                        "internalType": "uint256",
                        "name": "chainId",
                        "type": "uint256"
                    },
                    {
                        "internalType": "address",
                        "name": "verifyingContract",
                        "type": "address"
                    },
                    {
                        "internalType": "bytes32",
                        "name": "salt",
                        "type": "bytes32"
                    },
                    {
                        "internalType": "uint256[]",
                        "name": "extensions",
                        "type": "uint256[]"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "proxiableUUID",
                "outputs": [
                    {
                        "internalType": "bytes32",
                        "name": "",
                        "type": "bytes32"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "address",
                        "name": "accessManagerAddress",
                        "type": "address"
                    }
                ],
                "name": "setAccessManager",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "address",
                        "name": "newImplementation",
                        "type": "address"
                    },
                    {
                        "internalType": "bytes",
                        "name": "data",
                        "type": "bytes"
                    }
                ],
                "name": "upgradeToAndCall",
                "outputs": [],
                "stateMutability": "payable",
                "type": "function"
            }
        ]
        
        try:
            self.contract = web3.eth.contract(address=contract_address, abi=self.abi)
        except Exception as e:
            # Hide the ABI details from error messages
            error_msg = str(e)
            if "Could not format invalid value" in error_msg and "as field 'abi'" in error_msg:
                raise ValueError(f"Failed to initialize storage contract at {contract_address}: Invalid ABI format")
            else:
                raise ValueError(f"Failed to initialize storage contract at {contract_address}: {type(e).__name__}")from e

    def get_access_manager(self) -> HexAddress:
        """Gets the address of the associated access manager contract.
        
        Returns:
            Address of the access manager contract
        """
        return self.contract.functions.accessManager().call()

    def get_max_blocks_per_file(self) -> int:
        """Gets the maximum number of blocks per file.
        
        Returns:
            Maximum blocks per file
        """
        return self.contract.functions.MAX_BLOCKS_PER_FILE().call()

    def get_max_block_size(self) -> int:
        """Gets the maximum block size.
        
        Returns:
            Maximum block size in bytes
        """
        return self.contract.functions.MAX_BLOCK_SIZE().call()

    def create_bucket(self, bucket_name: str, from_address: HexAddress, private_key: str, gas_limit: int = None, nonce_manager=None) -> HexStr:
        """Creates a new bucket.
        
        Args:
            bucket_name: Name of the bucket to create
            from_address: Address creating the bucket
            private_key: Private key for signing the transaction
            gas_limit: Optional gas limit for the transaction. If not provided, will use default.
            nonce_manager: Optional nonce manager for coordinated transactions
            
        Returns:
            Transaction hash of the create operation
        """
        # Build transaction
        tx_params = {
            'from': from_address,
            'gasPrice': self.web3.eth.gas_price,
            'nonce': nonce_manager.get_nonce() if nonce_manager else self.web3.eth.get_transaction_count(from_address)
        }
        
        if gas_limit:
            tx_params['gas'] = gas_limit
        else:
            tx_params['gas'] = 500000  # Default gas limit
            
        tx = self.contract.functions.createBucket(bucket_name).build_transaction(tx_params)
        
        # Sign transaction
        signed_tx = Account.sign_transaction(tx, private_key)
        
        # Send raw transaction
        try:
            tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        except Exception as e:
            if nonce_manager and "nonce too low" in str(e):
                nonce_manager.reset_nonce()
            raise
        
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

    def create_file(self, from_address: HexAddress, private_key: str, bucket_id: bytes, file_name: str, nonce_manager=None) -> HexStr:
        """Creates a new file entry in the specified bucket.
        
        Args:
            from_address: Address creating the file
            private_key: Private key for signing the transaction
            bucket_id: ID of the bucket to create the file in (bytes32)
            file_name: Name of the file
            nonce_manager: Optional nonce manager for coordinated transactions
            
        Returns:
            Transaction hash of the create operation
        """
        # Build transaction with signature: createFile(bucketId, name)
        tx_params = {
            'from': from_address,
            'gas': 500000,  # Gas limit
            'gasPrice': self.web3.eth.gas_price,
            'nonce': nonce_manager.get_nonce() if nonce_manager else self.web3.eth.get_transaction_count(from_address)
        }
        
        tx = self.contract.functions.createFile(bucket_id, file_name).build_transaction(tx_params)
        
        # Sign transaction
        signed_tx = Account.sign_transaction(tx, private_key)
        
        # Send raw transaction
        try:
            tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        except Exception as e:
            if nonce_manager and "nonce too low" in str(e):
                nonce_manager.reset_nonce()
            raise
        
        # Wait for receipt
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status != 1:
            # Get revert reason if possible
            try:
                self.contract.functions.createFile(bucket_id, file_name).call({
                    'from': from_address
                })
            except Exception as e:
                raise Exception(f"Transaction reverted: {str(e)}")
            raise Exception(f"Transaction failed. Receipt: {receipt}")
        
        return tx_hash.hex()

    def add_file_chunk(self, from_address: HexAddress, private_key: str, cid: bytes, bucket_id: bytes, name: str, encoded_chunk_size: int, cids: list, chunk_blocks_sizes: list, chunk_index: int, nonce_manager=None) -> HexStr:
        """Adds a chunk to a file.
        
        Args:
            from_address: Address adding the chunk
            private_key: Private key for signing the transaction
            cid: CID of the chunk
            bucket_id: ID of the bucket containing the file
            name: Name of the file
            encoded_chunk_size: Size of the encoded chunk
            cids: List of block CIDs in the chunk
            chunk_blocks_sizes: List of block sizes
            chunk_index: Index of the chunk
            nonce_manager: Optional nonce manager for coordinated transactions
            
        Returns:
            Transaction hash of the add operation
        """
        # Build transaction
        tx_params = {
            'from': from_address,
            'gas': 1000000,  # Higher gas limit for chunk operations
            'gasPrice': self.web3.eth.gas_price,
            'nonce': nonce_manager.get_nonce() if nonce_manager else self.web3.eth.get_transaction_count(from_address)
        }
        
        tx = self.contract.functions.addFileChunk(
            cid, bucket_id, name, encoded_chunk_size, cids, chunk_blocks_sizes, chunk_index
        ).build_transaction(tx_params)
        
        # Sign transaction
        signed_tx = Account.sign_transaction(tx, private_key)
        
        # Send raw transaction
        try:
            tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        except Exception as e:
            if nonce_manager and "nonce too low" in str(e):
                nonce_manager.reset_nonce()
            raise
        
        # Wait for receipt
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status != 1:
            # Get revert reason if possible
            try:
                self.contract.functions.addFileChunk(
                    cid, bucket_id, name, encoded_chunk_size, cids, chunk_blocks_sizes, chunk_index
                ).call({'from': from_address})
            except Exception as e:
                raise Exception(f"Transaction reverted: {str(e)}")
            raise Exception(f"Transaction failed. Receipt: {receipt}")
        
        return tx_hash.hex()

    def commit_file(self, bucket_id: bytes, file_name: str, encoded_size: int, actual_size: int, root_cid: bytes, from_address: HexAddress, private_key: str) -> None:
        """Updates the file metadata after upload using new ABI signature.
        
        Args:
            bucket_id: ID of the bucket (bytes32)
            file_name: Name of the file
            encoded_size: Size after erasure coding in bytes
            actual_size: Original file size in bytes
            root_cid: Root CID of the uploaded file
            from_address: Address committing the file
            private_key: Private key for signing the transaction
        """
        # Ensure bucket_id is bytes32
        if isinstance(bucket_id, str):
            if bucket_id.startswith('0x'):
                bucket_id = bytes.fromhex(bucket_id[2:])
            else:
                bucket_id = bytes.fromhex(bucket_id)
        
        if len(bucket_id) != 32:
            raise ValueError(f"bucket_id must be 32 bytes, got {len(bucket_id)}")
        
        # commitFile signature: commitFile(bucketId, name, encodedFileSize, actualSize, fileCID)
        tx = self.contract.functions.commitFile(bucket_id, file_name, encoded_size, actual_size, root_cid).build_transaction({
            'from': from_address,
            'gas': 500000,  # Gas limit (adjust as needed)
            'gasPrice': self.web3.eth.gas_price,
            'nonce': self.web3.eth.get_transaction_count(from_address)
        })
        
        # Sign transaction
        signed_tx = Account.sign_transaction(tx, private_key)
        
        # Send raw transaction
        tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        
        # Wait for receipt
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status != 1:
            print(f"[COMMIT_FILE_ERROR] Transaction receipt: {receipt}")
            print(f"[COMMIT_FILE_ERROR] Status: {receipt.status}")
            print(f"[COMMIT_FILE_ERROR] Gas used: {receipt.gasUsed}")
            print(f"[COMMIT_FILE_ERROR] Bucket ID: {bucket_id.hex()}")
            print(f"[COMMIT_FILE_ERROR] File name: {file_name}")
            print(f"[COMMIT_FILE_ERROR] Encoded size: {encoded_size}")
            print(f"[COMMIT_FILE_ERROR] Actual size: {actual_size}")
            print(f"[COMMIT_FILE_ERROR] Root CID: {root_cid.hex()}")
            raise Exception(f"Transaction failed for commitFile. Status: {receipt.status}, Gas used: {receipt.gasUsed}")

    def delete_bucket(self, bucket_name: str, from_address: HexAddress, private_key: str, bucket_id_hex: str = None) -> HexStr:
        if not bucket_id_hex:
            raise Exception("bucket_id_hex is required - get it from IPC BucketView response")
            
        try:
            if bucket_id_hex.startswith('0x'):
                bucket_id_hex = bucket_id_hex[2:]
            
            if len(bucket_id_hex) != 64:
                bucket_id_hex = bucket_id_hex.ljust(64, '0')  # Pad with zeros if needed
                
            bucket_id_bytes = bytes.fromhex(bucket_id_hex)
            print(f"Using bucket_id from IPC: 0x{bucket_id_hex}")
            
            try:
                result = self.contract.functions.getBucketIndexByName(bucket_name, from_address).call()
                bucket_index = result[0] if isinstance(result, (list, tuple)) else result
                print(f"Got bucket_index result: {result}")
                print(f"Using bucket_index: {bucket_index}")
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
            tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
            print(f"Transaction sent: {tx_hash.hex()}")
            
            # Wait for receipt
            receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"Transaction receipt: status={receipt.status}, gasUsed={receipt.gasUsed}")
            
            if receipt.status != 1:
                raise Exception(f"Transaction failed with status: {receipt.status}")
                
            return tx_hash.hex()
            
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
        signed_tx = Account.sign_transaction(tx, auth.key)
        
        tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status != 1:
            raise Exception("Transaction failed")
            
        return tx_hash.hex()

    def get_bucket(self, bucket_name: str, owner_address: str = None) -> Tuple[str, int, HexAddress]:
        if owner_address is None:
            raise ValueError("Owner address must be provided for bucket lookup")
        bucket = self.contract.functions.getBucketByName(bucket_name, owner_address, 0, 10).call()
        return (bucket[1], bucket[2], bucket[3])  # (name, createdAt, owner)

    def get_file(self, bucket_name: str, file_name: str, owner_address: str = None) -> Tuple[str, bytes, int, int]:
        if owner_address is None:
            raise ValueError("Owner address must be provided for bucket lookup")
            
        bucket = self.contract.functions.getBucketByName(bucket_name, owner_address, 0, 10).call()
        bucket_id = bucket[0]  # bytes32 id
        file_info = self.contract.functions.getFileByName(bucket_id, file_name).call()
        return (file_info[3], file_info[0], file_info[4], file_info[5])  # (name, id, encodedSize, createdAt)

    def get_bucket_by_name(self, call_opts: dict, bucket_name: str, owner_address: str = None, file_offset: int = 0, file_limit: int = 10):
        if owner_address is None and call_opts and 'from' in call_opts:
            owner_address = call_opts['from']
        elif owner_address is None:
            raise ValueError("Owner address must be provided either as parameter or in call_opts['from']")
            
        if call_opts:
            return self.contract.functions.getBucketByName(bucket_name, owner_address, file_offset, file_limit).call(call_opts)
        else:
            return self.contract.functions.getBucketByName(bucket_name, owner_address, file_offset, file_limit).call()

    def get_file_by_name(self, call_opts: dict, bucket_id: bytes, file_name: str):
        if call_opts:
            return self.contract.functions.getFileByName(bucket_id, file_name).call(call_opts)
        else:
            return self.contract.functions.getFileByName(bucket_id, file_name).call()

    def get_file_index_by_id(self, call_opts: dict, bucket_name: str, file_id: bytes):
        if call_opts:
            return self.contract.functions.getFileIndexById(bucket_name, file_id).call(call_opts)
        else:
            return self.contract.functions.getFileIndexById(bucket_name, file_id).call()

    def is_file_filled(self, file_id: bytes) -> bool:
        """Returns info about file status.
        
        Args:
            file_id: ID of the file to check
            
        Returns:
            True if file is filled, False otherwise
        """
        return self.contract.functions.isFileFilled(file_id).call()

    def is_file_filled_v2(self, file_id: bytes) -> bool:
        """Returns info about file status (V2 - uses loop to iterate through each chunk).
        
        Args:
            file_id: ID of the file to check
            
        Returns:
            True if file is filled, False otherwise
        """
        return self.contract.functions.isFileFilledV2(file_id).call()

    def get_upgrade_interface_version(self) -> str:
        
        return self.contract.functions.UPGRADE_INTERFACE_VERSION().call()

    def add_file_chunks(self, from_address: HexAddress, private_key: str, cids: List[bytes], bucket_id: bytes, 
                       file_name: str, encoded_chunk_sizes: List[int], chunk_blocks_cids: List[List[bytes]], 
                       chunk_block_sizes: List[List[int]], starting_chunk_index: int, nonce_manager=None) -> HexStr:
        
        tx_params = {
            'from': from_address,
            'gas': 2000000,  # Higher gas limit for multiple chunks
            'gasPrice': self.web3.eth.gas_price,
            'nonce': nonce_manager.get_nonce() if nonce_manager else self.web3.eth.get_transaction_count(from_address)
        }
        
        tx = self.contract.functions.addFileChunks(
            cids, bucket_id, file_name, encoded_chunk_sizes, chunk_blocks_cids, chunk_block_sizes, starting_chunk_index
        ).build_transaction(tx_params)
        
        signed_tx = Account.sign_transaction(tx, private_key)
        
        try:
            tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        except Exception as e:
            if nonce_manager and "nonce too low" in str(e):
                nonce_manager.reset_nonce()
            raise
        
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status != 1:
            raise Exception(f"Transaction failed. Receipt: {receipt}")
        
        return tx_hash.hex()

    def get_file_by_id(self, file_id: bytes):       
        return self.contract.functions.getFileById(file_id).call()

    def get_bucket_index_by_name(self, bucket_name: str, owner_address: HexAddress) -> Tuple[int, bool]:
        return self.contract.functions.getBucketIndexByName(bucket_name, owner_address).call()

    def is_chunk_filled(self, file_id: bytes, chunk_index: int) -> bool:
        return self.contract.functions.isChunkFilled(file_id, chunk_index).call()

    def is_block_filled(self, file_id: bytes, block_index: int, chunk_index: int) -> bool:
        return self.contract.functions.isBlockFilled(file_id, block_index, chunk_index).call()

    def get_version(self) -> str:
        return self.contract.functions.version().call()

    def get_chain_id(self) -> int:
        return self.contract.functions.getChainID().call()

    def get_file_owner(self, file_id: bytes) -> HexAddress:
        return self.contract.functions.getFileOwner(file_id).call()

    def get_buckets_by_ids(self, bucket_ids: List[bytes]):
        return self.contract.functions.getBucketsByIds(bucket_ids).call()

    def get_owner_buckets(self, owner_address: HexAddress, offset: int = 0, limit: int = 10, 
                         file_offset: int = 0, file_limit: int = 10):
        return self.contract.functions.getOwnerBuckets(owner_address, offset, limit, file_offset, file_limit).call()

    def initialize_contract(self, from_address: HexAddress, private_key: str, token_address: HexAddress, nonce_manager=None) -> HexStr:
        tx_params = {
            'from': from_address,
            'gas': 500000,
            'gasPrice': self.web3.eth.gas_price,
            'nonce': nonce_manager.get_nonce() if nonce_manager else self.web3.eth.get_transaction_count(from_address)
        }
        
        tx = self.contract.functions.initialize(token_address).build_transaction(tx_params)
        signed_tx = Account.sign_transaction(tx, private_key)
        
        try:
            tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        except Exception as e:
            if nonce_manager and "nonce too low" in str(e):
                nonce_manager.reset_nonce()
            raise
        
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status != 1:
            raise Exception(f"Transaction failed. Receipt: {receipt}")
        
        return tx_hash.hex()

    def get_timestamp(self) -> int:
        return self.contract.functions.timestamp().call()

    def get_token(self) -> HexAddress:
        return self.contract.functions.token().call()

    def add_peer_block(self, from_address: HexAddress, private_key: str, peer_id: bytes, cid: bytes, 
                      file_name: str, is_replica: bool, nonce_manager=None) -> HexStr:
        tx_params = {
            'from': from_address,
            'gas': 500000,
            'gasPrice': self.web3.eth.gas_price,
            'nonce': nonce_manager.get_nonce() if nonce_manager else self.web3.eth.get_transaction_count(from_address)
        }
        
        tx = self.contract.functions.addPeerBlock(peer_id, cid, file_name, is_replica).build_transaction(tx_params)
        signed_tx = Account.sign_transaction(tx, private_key)
        
        try:
            tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        except Exception as e:
            if nonce_manager and "nonce too low" in str(e):
                nonce_manager.reset_nonce()
            raise
        
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status != 1:
            raise Exception(f"Transaction failed. Receipt: {receipt}")
        
        return tx_hash.hex()

    def delete_peer_block(self, from_address: HexAddress, private_key: str, block_id: bytes, 
                         peer_id: bytes, cid: bytes, file_name: str, index: int, nonce_manager=None) -> HexStr:
        tx_params = {
            'from': from_address,
            'gas': 500000,
            'gasPrice': self.web3.eth.gas_price,
            'nonce': nonce_manager.get_nonce() if nonce_manager else self.web3.eth.get_transaction_count(from_address)
        }
        
        tx = self.contract.functions.deletePeerBlock(block_id, peer_id, cid, file_name, index).build_transaction(tx_params)
        signed_tx = Account.sign_transaction(tx, private_key)
        
        try:
            tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        except Exception as e:
            if nonce_manager and "nonce too low" in str(e):
                nonce_manager.reset_nonce()
            raise
        
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status != 1:
            raise Exception(f"Transaction failed. Receipt: {receipt}")
        
        return tx_hash.hex()

    def fill_chunk_block(self, from_address: HexAddress, private_key: str, fill_args: dict, nonce_manager=None) -> HexStr:
        tx_params = {
            'from': from_address,
            'gas': 1000000,
            'gasPrice': self.web3.eth.gas_price,
            'nonce': nonce_manager.get_nonce() if nonce_manager else self.web3.eth.get_transaction_count(from_address)
        }
        
        args_tuple = (
            fill_args['blockCID'],
            fill_args['nodeId'], 
            fill_args['bucketId'],
            fill_args['chunkIndex'],
            fill_args['nonce'],
            fill_args['blockIndex'],
            fill_args['fileName'],
            fill_args['signature'],
            fill_args['deadline']
        )
        
        tx = self.contract.functions.fillChunkBlock(args_tuple).build_transaction(tx_params)
        signed_tx = Account.sign_transaction(tx, private_key)
        
        try:
            tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        except Exception as e:
            if nonce_manager and "nonce too low" in str(e):
                nonce_manager.reset_nonce()
            raise
        
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status != 1:
            raise Exception(f"Transaction failed. Receipt: {receipt}")
        
        return tx_hash.hex()

    def fill_chunk_blocks(self, from_address: HexAddress, private_key: str, fill_args_list: List[dict], nonce_manager=None) -> HexStr:
        tx_params = {
            'from': from_address,
            'gas': 2000000,  
            'gasPrice': self.web3.eth.gas_price,
            'nonce': nonce_manager.get_nonce() if nonce_manager else self.web3.eth.get_transaction_count(from_address)
        }
        
        args_tuples = []
        for fill_args in fill_args_list:
            args_tuple = (
                fill_args['blockCID'],
                fill_args['nodeId'],
                fill_args['bucketId'], 
                fill_args['chunkIndex'],
                fill_args['nonce'],
                fill_args['blockIndex'],
                fill_args['fileName'],
                fill_args['signature'],
                fill_args['deadline']
            )
            args_tuples.append(args_tuple)
        
        tx = self.contract.functions.fillChunkBlocks(args_tuples).build_transaction(tx_params)
        signed_tx = Account.sign_transaction(tx, private_key)
        
        try:
            tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        except Exception as e:
            if nonce_manager and "nonce too low" in str(e):
                nonce_manager.reset_nonce()
            raise
        
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status != 1:
            raise Exception(f"Transaction failed. Receipt: {receipt}")
        
        return tx_hash.hex()

    def get_chunk_by_index(self, file_id: bytes, index: int) -> Tuple[bytes, int]:
        return self.contract.functions.getChunkByIndex(file_id, index).call()

    def get_full_file_info(self, bucket_name: str, file_name: str, bucket_id: bytes, owner_address: HexAddress):
        return self.contract.functions.getFullFileInfo(bucket_name, file_name, bucket_id, owner_address).call()

    def get_buckets_by_ids_with_files(self, bucket_ids: List[bytes], bucket_offset: int = 0, 
                                     bucket_limit: int = 10, file_offset: int = 0, file_limit: int = 10):
        return self.contract.functions.getBucketsByIdsWithFiles(
            bucket_ids, bucket_offset, bucket_limit, file_offset, file_limit
        ).call()

    def get_peer_block_index_by_id(self, peer_id: bytes, cid: bytes, file_name: str) -> Tuple[int, bool]:
        return self.contract.functions.getPeerBlockIndexById(peer_id, cid, file_name).call()

    def get_peers_array_by_peer_block_cid(self, cids: List[bytes], file_name: str) -> List[List[bytes]]:
        return self.contract.functions.getPeersArrayByPeerBlockCid(cids, file_name).call()

    def get_peers_by_peer_block_cid(self, cid: bytes, file_name: str) -> List[bytes]:
        return self.contract.functions.getPeersByPeerBlockCid(cid, file_name).call()

    def is_peer_block_replica(self, cid: bytes, peer_id: bytes) -> bool:
        return self.contract.functions.isPeerBlockReplica(cid, peer_id).call()

    def get_file_fill_counter(self, file_id: bytes) -> int:
        return self.contract.functions.fileFillCounter(file_id).call()

    def get_file_reward_claimed(self, file_id: bytes) -> bool:
        return self.contract.functions.fileRewardClaimed(file_id).call()

    def get_fulfilled_blocks(self, file_id: bytes, index: int) -> int:
        return self.contract.functions.fulfilledBlocks(file_id, index).call()

    def get_eip712_domain(self):
        return self.contract.functions.eip712Domain().call()

    def get_proxiable_uuid(self) -> bytes:
        return self.contract.functions.proxiableUUID().call()

    def set_access_manager(self, from_address: HexAddress, private_key: str, access_manager_address: HexAddress, 
                          nonce_manager=None) -> HexStr:
        tx_params = {
            'from': from_address,
            'gas': 500000,
            'gasPrice': self.web3.eth.gas_price,
            'nonce': nonce_manager.get_nonce() if nonce_manager else self.web3.eth.get_transaction_count(from_address)
        }
        
        tx = self.contract.functions.setAccessManager(access_manager_address).build_transaction(tx_params)
        signed_tx = Account.sign_transaction(tx, private_key)
        
        try:
            tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        except Exception as e:
            if nonce_manager and "nonce too low" in str(e):
                nonce_manager.reset_nonce()
            raise
        
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status != 1:
            raise Exception(f"Transaction failed. Receipt: {receipt}")
        
        return tx_hash.hex()

    def upgrade_to_and_call(self, from_address: HexAddress, private_key: str, new_implementation: HexAddress, 
                           data: bytes, nonce_manager=None) -> HexStr:
        tx_params = {
            'from': from_address,
            'gas': 1000000,
            'gasPrice': self.web3.eth.gas_price,
            'nonce': nonce_manager.get_nonce() if nonce_manager else self.web3.eth.get_transaction_count(from_address)
        }
        
        tx = self.contract.functions.upgradeToAndCall(new_implementation, data).build_transaction(tx_params)
        signed_tx = Account.sign_transaction(tx, private_key)
        
        try:
            tx_hash = self.web3.eth.send_raw_transaction(get_raw_transaction(signed_tx))
        except Exception as e:
            if nonce_manager and "nonce too low" in str(e):
                nonce_manager.reset_nonce()
            raise
        
        receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status != 1:
            raise Exception(f"Transaction failed. Receipt: {receipt}")
        
        return tx_hash.hex()
