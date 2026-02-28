from __future__ import annotations

from typing import Optional, Dict, Any, List, Tuple, Union
from eth_typing import Address, HexStr, HexAddress
from web3 import Web3
from web3.contract import Contract
from eth_account.signers.local import LocalAccount
from eth_account import Account


class CidsCid:
    def __init__(self, data: bytes):
        self.data = data


class PDPVerifierProof:
    def __init__(self, leaf: bytes, proof: List[bytes]):
        self.leaf = leaf
        self.proof = proof


class PDPVerifierRootData:
    def __init__(self, root: CidsCid, raw_size: int):
        self.root = root
        self.raw_size = raw_size


class PDPVerifierRootIdAndOffset:
    def __init__(self, root_id: int, offset: int):
        self.root_id = root_id
        self.offset = offset


class PDPVerifierMetaData:    
    ABI = [
        {
            "inputs": [{"internalType": "uint256", "name": "_challengeFinality", "type": "uint256"}],
            "stateMutability": "nonpayable",
            "type": "constructor"
        },
        {
            "inputs": [
                {"internalType": "uint256", "name": "idx", "type": "uint256"},
                {"internalType": "string", "name": "msg", "type": "string"}
            ],
            "name": "IndexedError",
            "type": "error"
        },
        {
            "inputs": [{"internalType": "address", "name": "owner", "type": "address"}],
            "name": "OwnableInvalidOwner",
            "type": "error"
        },
        {
            "inputs": [{"internalType": "address", "name": "account", "type": "address"}],
            "name": "OwnableUnauthorizedAccount",
            "type": "error"
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": False, "internalType": "string", "name": "message", "type": "string"},
                {"indexed": False, "internalType": "uint256", "name": "value", "type": "uint256"}
            ],
            "name": "Debug",
            "type": "event"
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "uint256", "name": "setId", "type": "uint256"},
                {"indexed": False, "internalType": "uint256", "name": "challengeEpoch", "type": "uint256"},
                {"indexed": False, "internalType": "uint256", "name": "leafCount", "type": "uint256"}
            ],
            "name": "NextProvingPeriod",
            "type": "event"
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "address", "name": "previousOwner", "type": "address"},
                {"indexed": True, "internalType": "address", "name": "newOwner", "type": "address"}
            ],
            "name": "OwnershipTransferred",
            "type": "event"
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "uint256", "name": "setId", "type": "uint256"},
                {
                    "components": [
                        {"internalType": "uint256", "name": "rootId", "type": "uint256"},
                        {"internalType": "uint256", "name": "offset", "type": "uint256"}
                    ],
                    "indexed": False,
                    "internalType": "struct PDPVerifier.RootIdAndOffset[]",
                    "name": "challenges",
                    "type": "tuple[]"
                }
            ],
            "name": "PossessionProven",
            "type": "event"
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "uint256", "name": "setId", "type": "uint256"},
                {"indexed": False, "internalType": "uint256", "name": "fee", "type": "uint256"},
                {"indexed": False, "internalType": "uint64", "name": "price", "type": "uint64"},
                {"indexed": False, "internalType": "int32", "name": "expo", "type": "int32"}
            ],
            "name": "ProofFeePaid",
            "type": "event"
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "uint256", "name": "setId", "type": "uint256"},
                {"indexed": True, "internalType": "address", "name": "owner", "type": "address"}
            ],
            "name": "ProofSetCreated",
            "type": "event"
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "uint256", "name": "setId", "type": "uint256"},
                {"indexed": False, "internalType": "uint256", "name": "deletedLeafCount", "type": "uint256"}
            ],
            "name": "ProofSetDeleted",
            "type": "event"
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "uint256", "name": "setId", "type": "uint256"}
            ],
            "name": "ProofSetEmpty",
            "type": "event"
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "uint256", "name": "setId", "type": "uint256"},
                {"indexed": True, "internalType": "address", "name": "oldOwner", "type": "address"},
                {"indexed": True, "internalType": "address", "name": "newOwner", "type": "address"}
            ],
            "name": "ProofSetOwnerChanged",
            "type": "event"
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "uint256", "name": "setId", "type": "uint256"},
                {"indexed": False, "internalType": "uint256[]", "name": "rootIds", "type": "uint256[]"}
            ],
            "name": "RootsAdded",
            "type": "event"
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "uint256", "name": "setId", "type": "uint256"},
                {"indexed": False, "internalType": "uint256[]", "name": "rootIds", "type": "uint256[]"}
            ],
            "name": "RootsRemoved",
            "type": "event"
        },
        {
            "inputs": [],
            "name": "EXTRA_DATA_MAX_SIZE",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "LEAF_SIZE",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "MAX_ENQUEUED_REMOVALS",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "MAX_ROOT_SIZE",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "NO_CHALLENGE_SCHEDULED",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "NO_PROVEN_EPOCH",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "RANDOMNESS_PRECOMPILE",
            "outputs": [{"internalType": "address", "name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "SECONDS_IN_DAY",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "uint256", "name": "setId", "type": "uint256"},
                {
                    "components": [
                        {
                            "components": [
                                {"internalType": "bytes", "name": "data", "type": "bytes"}
                            ],
                            "internalType": "struct Cids.Cid",
                            "name": "root",
                            "type": "tuple"
                        },
                        {"internalType": "uint256", "name": "rawSize", "type": "uint256"}
                    ],
                    "internalType": "struct PDPVerifier.RootData[]",
                    "name": "rootData",
                    "type": "tuple[]"
                },
                {"internalType": "bytes", "name": "extraData", "type": "bytes"}
            ],
            "name": "addRoots",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "nonpayable",
            "type": "function"
        },
        {
            "inputs": [{"internalType": "uint256", "name": "setId", "type": "uint256"}],
            "name": "claimProofSetOwnership",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "address", "name": "listenerAddr", "type": "address"},
                {"internalType": "bytes", "name": "extraData", "type": "bytes"}
            ],
            "name": "createProofSet",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "payable",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "uint256", "name": "setId", "type": "uint256"},
                {"internalType": "bytes", "name": "extraData", "type": "bytes"}
            ],
            "name": "deleteProofSet",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "uint256", "name": "setId", "type": "uint256"},
                {"internalType": "uint256[]", "name": "leafIndexs", "type": "uint256[]"}
            ],
            "name": "findRootIds",
            "outputs": [
                {
                    "components": [
                        {"internalType": "uint256", "name": "rootId", "type": "uint256"},
                        {"internalType": "uint256", "name": "offset", "type": "uint256"}
                    ],
                    "internalType": "struct PDPVerifier.RootIdAndOffset[]",
                    "name": "",
                    "type": "tuple[]"
                }
            ],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "getChallengeFinality",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [{"internalType": "uint256", "name": "setId", "type": "uint256"}],
            "name": "getChallengeRange",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [{"internalType": "uint256", "name": "setId", "type": "uint256"}],
            "name": "getNextChallengeEpoch",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "getNextProofSetId",
            "outputs": [{"internalType": "uint64", "name": "", "type": "uint64"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [{"internalType": "uint256", "name": "setId", "type": "uint256"}],
            "name": "getNextRootId",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [{"internalType": "uint256", "name": "setId", "type": "uint256"}],
            "name": "getProofSetLastProvenEpoch",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [{"internalType": "uint256", "name": "setId", "type": "uint256"}],
            "name": "getProofSetLeafCount",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [{"internalType": "uint256", "name": "setId", "type": "uint256"}],
            "name": "getProofSetListener",
            "outputs": [{"internalType": "address", "name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [{"internalType": "uint256", "name": "setId", "type": "uint256"}],
            "name": "getProofSetOwner",
            "outputs": [
                {"internalType": "address", "name": "", "type": "address"},
                {"internalType": "address", "name": "", "type": "address"}
            ],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [{"internalType": "uint256", "name": "epoch", "type": "uint256"}],
            "name": "getRandomness",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "uint256", "name": "setId", "type": "uint256"},
                {"internalType": "uint256", "name": "rootId", "type": "uint256"}
            ],
            "name": "getRootCid",
            "outputs": [
                {
                    "components": [
                        {"internalType": "bytes", "name": "data", "type": "bytes"}
                    ],
                    "internalType": "struct Cids.Cid",
                    "name": "",
                    "type": "tuple"
                }
            ],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "uint256", "name": "setId", "type": "uint256"},
                {"internalType": "uint256", "name": "rootId", "type": "uint256"}
            ],
            "name": "getRootLeafCount",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [{"internalType": "uint256", "name": "setId", "type": "uint256"}],
            "name": "getScheduledRemovals",
            "outputs": [{"internalType": "uint256[]", "name": "", "type": "uint256[]"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "uint256", "name": "setId", "type": "uint256"},
                {"internalType": "uint256", "name": "rootId", "type": "uint256"}
            ],
            "name": "getSumTreeCounts",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [{"internalType": "uint256", "name": "index", "type": "uint256"}],
            "name": "heightFromIndex",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "pure",
            "type": "function"
        },
        {
            "inputs": [{"internalType": "uint256", "name": "setId", "type": "uint256"}],
            "name": "heightOfTree",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "bytes32[][]", "name": "tree", "type": "bytes32[][]"},
                {"internalType": "uint256", "name": "leafCount", "type": "uint256"}
            ],
            "name": "makeRoot",
            "outputs": [
                {
                    "components": [
                        {
                            "components": [
                                {"internalType": "bytes", "name": "data", "type": "bytes"}
                            ],
                            "internalType": "struct Cids.Cid",
                            "name": "root",
                            "type": "tuple"
                        },
                        {"internalType": "uint256", "name": "rawSize", "type": "uint256"}
                    ],
                    "internalType": "struct PDPVerifier.RootData",
                    "name": "",
                    "type": "tuple"
                }
            ],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "uint256", "name": "setId", "type": "uint256"},
                {"internalType": "uint256", "name": "challengeEpoch", "type": "uint256"},
                {"internalType": "bytes", "name": "extraData", "type": "bytes"}
            ],
            "name": "nextProvingPeriod",
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
            "inputs": [{"internalType": "uint256", "name": "setId", "type": "uint256"}],
            "name": "proofSetLive",
            "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "uint256", "name": "setId", "type": "uint256"},
                {"internalType": "address", "name": "newOwner", "type": "address"}
            ],
            "name": "proposeProofSetOwner",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "uint256", "name": "setId", "type": "uint256"},
                {
                    "components": [
                        {"internalType": "bytes32", "name": "leaf", "type": "bytes32"},
                        {"internalType": "bytes32[]", "name": "proof", "type": "bytes32[]"}
                    ],
                    "internalType": "struct PDPVerifier.Proof[]",
                    "name": "proofs",
                    "type": "tuple[]"
                }
            ],
            "name": "provePossession",
            "outputs": [],
            "stateMutability": "payable",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "renounceOwnership",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "uint256", "name": "setId", "type": "uint256"},
                {"internalType": "uint256", "name": "rootId", "type": "uint256"}
            ],
            "name": "rootChallengable",
            "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "uint256", "name": "setId", "type": "uint256"},
                {"internalType": "uint256", "name": "rootId", "type": "uint256"}
            ],
            "name": "rootLive",
            "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "uint256", "name": "setId", "type": "uint256"},
                {"internalType": "uint256[]", "name": "rootIds", "type": "uint256[]"},
                {"internalType": "bytes", "name": "extraData", "type": "bytes"}
            ],
            "name": "scheduleRemovals",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "uint256", "name": "", "type": "uint256"},
                {"internalType": "uint256", "name": "", "type": "uint256"}
            ],
            "name": "sumTreeCounts",
            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [{"internalType": "address", "name": "newOwner", "type": "address"}],
            "name": "transferOwnership",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function"
        }
    ]

    BIN = "0x6080604052348015600e575f5ffd5b50604051613bc9380380613bc9833981016040819052602b9160ad565b3380604f57604051631e4fbdf760e01b81525f600482015260240160405180910390fd5b605681605e565b5060015560c3565b5f80546001600160a01b038381166001600160a01b0319831681178455604051919092169283917f8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e09190a35050565b5f6020828403121560bc575f5ffd5b5051919050565b613af9806100d05f395ff3fe60806040526004361061023e575f3560e01c806371cf2a16116101345780639f8cb3bd116100b3578063f178b1be11610078578063f178b1be146103db578063f2fde38b146106ec578063f58f952b1461070b578063f5cac1ba1461071e578063f83758fe1461073d578063faa6716314610751575f5ffd5b80639f8cb3bd1461064f578063c0e1594914610664578063cbb147cd14610678578063d49245c1146106ae578063ee3dac65146106cd575f5ffd5b80638d101c68116100f95780638d101c68146105985780638da5cb5b146105b75780638ea417e5146105d35780639153e64b146105fa5780639a9a330f14610619575f5ffd5b806371cf2a16146104f0578063831604e11461050f578063847d1d061461053b578063869888841461055a57806389208ba914610579575f5ffd5b8063453f4f62116101c057806361a52a361161018557806361a52a361461045c5780636ba4608f146104725780636cb55c16146104915780636fa44692146104b0578063715018a6146104dc575f5ffd5b8063453f4f621461039f57806345c0b92d146103bc578063462dd449146103db5780634726075b146103ee578063473310501461042d575f5ffd5b806316e2bcd51161020657806316e2bcd5146102fa57806331601226146103145780633b68e4e9146103335780633b7ae913146103545780633f84135f14610380575f5ffd5b8063029b4646146102425780630528a55b1461026a5780630a4d79321461029657806311c0ee4a146102a957806315b17570146102c8575b5f5ffd5b34801561024d575f5ffd5b5061025761080081565b6040519081526020015b60405180910390f35b348015610275575f5ffd5b50610289610284366004613022565b610770565b6040516102619190613069565b6102576102a436600461310f565b610857565b3480156102b4575f5ffd5b506102576102c3366004613150565b610a47565b3480156102d3575f5ffd5b506102e26006607f60991b0181565b6040516001600160a01b039091168152602001610261565b348015610305575f5ffd5b50610257660400000000000081565b34801561031f575f5ffd5b506102e261032e3660046131c7565b610cf5565b34801561033e575f5ffd5b5061035261034d366004613150565b610d36565b005b34801561035f575f5ffd5b5061037361036e3660046131de565b610fe9565b6040516102619190613236565b34801561038b575f5ffd5b5061025761039a3660046131c7565b6110d9565b3480156103aa575f5ffd5b506102576103b93660046131c7565b90565b3480156103c7575f5ffd5b506103526103d6366004613248565b611111565b3480156103e6575f5ffd5b506102575f81565b3480156103f9575f5ffd5b5061040d6104083660046131c7565b61150c565b604080516001600160a01b03938416815292909116602083015201610261565b348015610438575f5ffd5b5061044c6104473660046131de565b611560565b6040519015158152602001610261565b348015610467575f5ffd5b506102576201518081565b34801561047d575f5ffd5b5061025761048c3660046131c7565b6115a8565b34801561049c575f5ffd5b506103526104ab366004613296565b6115e0565b3480156104bb575f5ffd5b506104cf6104ca3660046131c7565b6116e6565b60405161026191906132c0565b3480156104e7575f5ffd5b506103526117b5565b3480156104fb575f5ffd5b5061044c61050a3660046131de565b6117c8565b34801561051a575f5ffd5b5061052e61052936600461335d565b6118da565b6040516102619190613476565b348015610546575f5ffd5b506103526105553660046134a7565b611976565b348015610565575f5ffd5b506102576105743660046131c7565b611b5b565b348015610584575f5ffd5b506102576105933660046131c7565b611b6f565b3480156105a3575f5ffd5b506102576105b23660046131c7565b611ba7565b3480156105c2575f5ffd5b505f546001600160a01b03166102e2565b3480156105de575f5ffd5b506002546040516001600160401b039091168152602001610261565b348015610605575f5ffd5b506102576106143660046131de565b611bca565b348015610624575f5ffd5b506102576106333660046131de565b600560209081525f928352604080842090915290825290205481565b34801561065a575f5ffd5b506102576107d081565b34801561066f575f5ffd5b50610257602081565b348015610683575f5ffd5b506102576106923660046131de565b5f91825260056020908152604080842092845291905290205490565b3480156106b9575f5ffd5b506102576106c83660046131c7565b611c0d565b3480156106d8575f5ffd5b506103526106e73660046131c7565b611c45565b3480156106f7575f5ffd5b506103526107063660046134d5565b611d50565b610352610719366004613022565b611d8d565b348015610729575f5ffd5b5061044c6107383660046131c7565b612269565b348015610748575f5ffd5b50600154610257565b34801561075c575f5ffd5b5061025761076b3660046131c7565b61229d565b5f838152600660205260408120546060919061078b906122d5565b61079790610100613502565b90505f836001600160401b038111156107b2576107b26132f7565b6040519080825280602002602001820160405280156107f657816020015b604080518082019091525f80825260208201528152602001906001900390816107d05790505b5090505f5b8481101561084b576108268787878481811061081957610819613515565b90506020020135856123cc565b82828151811061083857610838613515565b60209081029190910101526001016107fb565b509150505b9392505050565b5f6108008211156108835760405162461bcd60e51b815260040161087a90613529565b60405180910390fd5b5f61088c6125ac565b9050803410156108d25760405162461bcd60e51b81526020600482015260116024820152701cde589a5b08199959481b9bdd081b595d607a1b604482015260640161087a565b8034111561090f57336108fc6108e88334613502565b6040518115909202915f818181858888f1935050505015801561090d573d5f5f3e3d5ffd5b505b600280545f916001600160401b03909116908261092b83613557565b82546001600160401b039182166101009390930a928302928202191691909117909155165f81815260076020908152604080832083905560088252808320839055600c825280832080546001600160a01b031990811633179091556009835281842080546001600160a01b038d16921682179055600e90925282209190915590915015610a1257604051634a6a0d9b60e11b81526001600160a01b038716906394d41b36906109e490849033908a908a906004016135a9565b5f604051808303815f87803b1580156109fb575f5ffd5b505af1158015610a0d573d5f5f3e3d5ffd5b505050505b604051339082907f017f0b33d96e8f9968590172013032c2346cf047787a5e17a44b0a1bb3cd0f01905f90a395945050505050565b5f610800821115610a6a5760405162461bcd60e51b815260040161087a90613529565b610a7386612269565b610a8f5760405162461bcd60e51b815260040161087a906135dd565b83610adc5760405162461bcd60e51b815260206004820152601a60248201527f4d75737420616464206174206c65617374206f6e6520726f6f74000000000000604482015260640161087a565b5f868152600c60205260409020546001600160a01b03163314610b415760405162461bcd60e51b815260206004820152601c60248201527f4f6e6c7920746865206f776e65722063616e2061646420726f6f747300000000604482015260640161087a565b5f8681526006602052604081205490856001600160401b03811115610b6857610b686132f7565b604051908082528060200260200182016040528015610b91578160200160208202803683370190505b5090505f5b86811015610c2e57610bfe89828a8a85818110610bb557610bb5613515565b9050602002810190610bc79190613609565b610bd19080613627565b8b8b86818110610be357610be3613515565b9050602002810190610bf59190613609565b602001356125d1565b50610c09818461363b565b828281518110610c1b57610c1b613515565b6020908102919091010152600101610b96565b50877f5ce51a8003915c377679ba533d9dafa0792058b254965697e674272f13f4fdd382604051610c5f91906132c0565b60405180910390a25f888152600960205260409020546001600160a01b03168015610ce8576040516312d5d66f60e01b81526001600160a01b038216906312d5d66f90610cba908c9087908d908d908d908d9060040161364e565b5f604051808303815f87803b158015610cd1575f5ffd5b505af1158015610ce3573d5f5f3e3d5ffd5b505050505b5090979650505050505050565b5f610cff82612269565b610d1b5760405162461bcd60e51b815260040161087a906135dd565b505f908152600960205260409020546001600160a01b031690565b610800811115610d585760405162461bcd60e51b815260040161087a90613529565b610d6185612269565b610d7d5760405162461bcd60e51b815260040161087a906135dd565b5f858152600c60205260409020546001600160a01b03163314610df75760405162461bcd60e51b815260206004820152602c60248201527f4f6e6c7920746865206f776e65722063616e207363686564756c652072656d6f60448201526b76616c206f6620726f6f747360a01b606482015260840161087a565b5f858152600b60205260409020546107d090610e13908561363b565b1115610e875760405162461bcd60e51b815260206004820152603a60248201527f546f6f206d616e792072656d6f76616c73207761697420666f72206e6578742060448201527f70726f76696e6720706572696f6420746f207363686564756c65000000000000606482015260840161087a565b5f5b83811015610f61575f86815260066020526040902054858583818110610eb157610eb1613515565b9050602002013510610f195760405162461bcd60e51b815260206004820152602b60248201527f43616e206f6e6c79207363686564756c652072656d6f76616c206f662065786960448201526a7374696e6720726f6f747360a81b606482015260840161087a565b5f868152600b60205260409020858583818110610f3857610f38613515565b8354600180820186555f9586526020958690209290950293909301359201919091555001610e89565b505f858152600960205260409020546001600160a01b03168015610fe15760405163257be8e960e11b81526001600160a01b03821690634af7d1d290610fb39089908990899089908990600401613755565b5f604051808303815f87803b158015610fca575f5ffd5b505af1158015610fdc573d5f5f3e3d5ffd5b505050505b505050505050565b60408051602081019091526060815261100183612269565b61101d5760405162461bcd60e51b815260040161087a906135dd565b5f8381526003602090815260408083208584528252918290208251918201909252815490919082908290611050906137ad565b80601f016020809104026020016040519081016040528092919081815260200182805461107c906137ad565b80156110c75780601f1061109e576101008083540402835291602001916110c7565b820191905f5260205f20905b8154815290600101906020018083116110aa57829003601f168201915b50505050508152505090505b92915050565b5f6110e382612269565b6110ff5760405162461bcd60e51b815260040161087a906135dd565b505f9081526007602052604090205490565b6108008111156111335760405162461bcd60e51b815260040161087a90613529565b5f848152600c60205260409020546001600160a01b031633146111685760405162461bcd60e51b815260040161087a906137e5565b5f848152600760205260409020546111d75760405162461bcd60e51b815260206004820152602c60248201527f63616e206f6e6c792073746172742070726f76696e67206f6e6365206c65617660448201526b195cc8185c9948185919195960a21b606482015260840161087a565b5f848152600e60205260409020546111fa575f848152600e602052604090204390555b5f848152600b6020526040812080549091906001600160401b03811115611223576112236132f7565b60405190808252806020026020018201604052801561124c578160200160208202803683370190505b5090505f5b81518110156112cf578254839061126a90600190613502565b8154811061127a5761127a613515565b905f5260205f20015482828151811061129557611295613515565b602002602001018181525050828054806112b1576112b1613833565b5f8281526020812082015f1990810191909155019055600101611251565b506112da8682612794565b857fd22bb0ee05b8ca92312459c76223d3b9bc1bd96fb6c9b18e637ededf92d811748260405161130a91906132c0565b60405180910390a25f86815260076020908152604080832054600a90925290912055600154611339904361363b565b8510156113be5760405162461bcd60e51b815260206004820152604760248201527f6368616c6c656e67652065706f6368206d757374206265206174206c6561737460448201527f206368616c6c656e676546696e616c6974792065706f63687320696e207468656064820152662066757475726560c81b608482015260a40161087a565b5f868152600860209081526040808320889055600790915281205490036114255760405186907f323c29bc8d678a5d987b90a321982d10b9a91bcad071a9e445879497bf0e68e7905f90a25f868152600e6020908152604080832083905560089091528120555b5f868152600960205260409020546001600160a01b031680156114bb575f87815260086020908152604080832054600790925291829020549151632a89faf360e21b81526001600160a01b0384169263aa27ebcc9261148d928c92908b908b90600401613847565b5f604051808303815f87803b1580156114a4575f5ffd5b505af11580156114b6573d5f5f3e3d5ffd5b505050505b5f878152600760209081526040918290205482518981529182015288917fc099ffec4e3e773644a4d1dda368c46af853a0eeb15babde217f53a657396e1e910160405180910390a250505050505050565b5f5f61151783612269565b6115335760405162461bcd60e51b815260040161087a906135dd565b50505f908152600c6020908152604080832054600d909252909120546001600160a01b0391821692911690565b5f61156a83612269565b801561158257505f8381526006602052604090205482105b80156108505750505f918252600460209081526040808420928452919052902054151590565b5f6115b282612269565b6115ce5760405162461bcd60e51b815260040161087a906135dd565b505f9081526008602052604090205490565b6115e982612269565b6116055760405162461bcd60e51b815260040161087a906135dd565b5f828152600c60205260409020546001600160a01b03163381146116825760405162461bcd60e51b815260206004820152602e60248201527f4f6e6c79207468652063757272656e74206f776e65722063616e2070726f706f60448201526d39b29030903732bb9037bbb732b960911b606482015260840161087a565b816001600160a01b0316816001600160a01b0316036116ba5750505f908152600d6020526040902080546001600160a01b0319169055565b5f838152600d6020526040902080546001600160a01b0319166001600160a01b0384161790555b505050565b60606116f182612269565b61170d5760405162461bcd60e51b815260040161087a906135dd565b5f828152600b6020526040812080549091906001600160401b03811115611736576117366132f7565b60405190808252806020026020018201604052801561175f578160200160208202803683370190505b5090505f5b82548110156117ad5782818154811061177f5761177f613515565b905f5260205f20015482828151811061179a5761179a613515565b6020908102919091010152600101611764565b509392505050565b6117bd612824565b6117c65f612850565b565b5f8281526006602052604081205481906117e1906122d5565b6117ed90610100613502565b5f858152600a60205260408120549192509061181790869061181190600190613502565b846123cc565b5f8681526004602090815260408083208451845290915290205490915061184090600190613502565b8160200151146118ba576040805162461bcd60e51b81526020600482015260248101919091527f6368616c6c656e676552616e6765202d312073686f756c6420616c69676e207760448201527f697468207468652076657279206c617374206c656166206f66206120726f6f74606482015260840161087a565b6118c48585611560565b80156118d1575080518411155b95945050505050565b60408051606080820183529181019182529081525f60208201525f6040518060400160405280600381526020016210d25160ea1b8152509050604051806040016040528061195a83875f8151811061193457611934613515565b60200260200101515f8151811061194d5761194d613515565b602002602001015161289f565b815260200184602061196c9190613877565b9052949350505050565b6108008111156119985760405162461bcd60e51b815260040161087a90613529565b6002546001600160401b031683106119f25760405162461bcd60e51b815260206004820152601a60248201527f70726f6f6620736574206964206f7574206f6620626f756e6473000000000000604482015260640161087a565b5f838152600c60205260409020546001600160a01b03163314611a635760405162461bcd60e51b8152602060048201526024808201527f4f6e6c7920746865206f776e65722063616e2064656c6574652070726f6f66206044820152637365747360e01b606482015260840161087a565b5f838152600760209081526040808320805490849055600c835281842080546001600160a01b031916905560088352818420849055600e83528184208490556009909252909120546001600160a01b03168015611b1a576040516326c249e360e01b81526001600160a01b038216906326c249e390611aec90889086908990899060040161388e565b5f604051808303815f87803b158015611b03575f5ffd5b505af1158015611b15573d5f5f3e3d5ffd5b505050505b847f589e9a441b5bddda77c4ab647b0108764a9cc1a7f655aa9b7bc50b8bdfab867383604051611b4c91815260200190565b60405180910390a25050505050565b5f6110d3611b6a83600161363b565b6129c5565b5f611b7982612269565b611b955760405162461bcd60e51b815260040161087a906135dd565b505f908152600a602052604090205490565b5f81815260066020526040812054611bbe906122d5565b6110d390610100613502565b5f611bd483612269565b611bf05760405162461bcd60e51b815260040161087a906135dd565b505f91825260046020908152604080842092845291905290205490565b5f611c1782612269565b611c335760405162461bcd60e51b815260040161087a906135dd565b505f9081526006602052604090205490565b611c4e81612269565b611c6a5760405162461bcd60e51b815260040161087a906135dd565b5f818152600d60205260409020546001600160a01b03163314611ce35760405162461bcd60e51b815260206004820152602b60248201527f4f6e6c79207468652070726f706f736564206f776e65722063616e20636c616960448201526a06d206f776e6572736869760ac1b606482015260840161087a565b5f818152600c602090815260408083208054336001600160a01b03198083168217909355600d909452828520805490921690915590516001600160a01b0390911692839185917fd3273037b635678293ef0c076bd77af13760e75e12806d1db237616d03c3a76691a45050565b611d58612824565b6001600160a01b038116611d8157604051631e4fbdf760e01b81525f600482015260240161087a565b611d8a81612850565b50565b5f5a5f858152600c60205260409020549091506001600160a01b03163314611dc75760405162461bcd60e51b815260040161087a906137e5565b5f8481526008602052604090205443811115611e175760405162461bcd60e51b815260206004820152600f60248201526e383932b6b0ba3ab93290383937b7b360891b604482015260640161087a565b82611e525760405162461bcd60e51b815260206004820152600b60248201526a32b6b83a3c90383937b7b360a91b604482015260640161087a565b80611e985760405162461bcd60e51b81526020600482015260166024820152751b9bc818da185b1b195b99d9481cd8da19591d5b195960521b604482015260640161087a565b5f836001600160401b03811115611eb157611eb16132f7565b604051908082528060200260200182016040528015611ef557816020015b604080518082019091525f8082526020820152815260200190600190039081611ecf5790505b5090505f611f0287612be5565b5f888152600a6020908152604080832054600690925282205492935091611f28906122d5565b611f3490610100613502565b90505f5b6001600160401b03811688111561213f5760408051602081018690529081018b90526001600160c01b031960c083901b1660608201525f9060680160405160208183030381529060405290505f8482805190602001205f1c611f9a91906138c1565b9050611fa78c82866123cc565b87846001600160401b031681518110611fc257611fc2613515565b60200260200101819052505f6120056120008e8a876001600160401b031681518110611ff057611ff0613515565b60200260200101515f0151610fe9565b612bf8565b90505f6120e28d8d876001600160401b031681811061202657612026613515565b90506020028101906120389190613609565b6120469060208101906138d4565b808060200260200160405190810160405280939291908181526020018383602002808284375f81840152601f19601f82011690508083019250505050505050838f8f896001600160401b03168181106120a1576120a1613515565b90506020028101906120b39190613609565b5f01358c896001600160401b0316815181106120d1576120d1613515565b602002602001015160200151612ce2565b9050806121285760405162461bcd60e51b815260206004820152601460248201527370726f6f6620646964206e6f742076657269667960601b604482015260640161087a565b50505050808061213790613557565b915050611f38565b505f61214b8989612cf9565b61215690602061363b565b61216290610514613877565b5a61216d9089613502565b612177919061363b565b5f8b8152600960205260409020549091506001600160a01b03168015612212575f8b8152600760205260409081902054905163356de02b60e01b8152600481018d9052602481019190915260448101869052606481018a90526001600160a01b0382169063356de02b906084015f604051808303815f87803b1580156121fb575f5ffd5b505af115801561220d573d5f5f3e3d5ffd5b505050505b5f8b8152600e602052604090819020439055518b907f1acf7df9f0c1b0208c23be6178950c0273f89b766805a2c0bd1e53d25c700e5090612254908990613069565b60405180910390a25050505050505050505050565b6002545f906001600160401b0316821080156110d35750505f908152600c60205260409020546001600160a01b0316151590565b5f6122a782612269565b6122c35760405162461bcd60e51b815260040161087a906135dd565b505f908152600e602052604090205490565b5f610100608083901c80156122f5576122ef608083613502565b91508093505b50604083901c80156123125761230c604083613502565b91508093505b50602083901c801561232f57612329602083613502565b91508093505b50601083901c801561234c57612346601083613502565b91508093505b50600883901c801561236957612363600883613502565b91508093505b50600483901c801561238657612380600483613502565b91508093505b50600283901c80156123a35761239d600283613502565b91508093505b50600183901c80156123c2576123ba600283613502565b949350505050565b6123ba8483613502565b604080518082019091525f80825260208201525f84815260076020526040902054831061243b5760405162461bcd60e51b815260206004820152601860248201527f4c65616620696e646578206f7574206f6620626f756e64730000000000000000604482015260640161087a565b5f612449600180851b613502565b90505f80845b8015612521575f88815260066020526040902054841061248957612474600182613502565b612482906001901b85613502565b935061250f565b5f8881526005602090815260408083208784529091529020546124ac908461363b565b91508682116124f3575f8881526005602090815260408083208784529091529020546124d8908461363b565b92506124e5600182613502565b612482906001901b8561363b565b6124fe600182613502565b61250c906001901b85613502565b93505b8061251981613919565b91505061244f565b505f878152600560209081526040808320868452909152902054612545908361363b565b9050858111612582576040518060400160405280846001612566919061363b565b81526020016125758389613502565b8152509350505050610850565b6040518060400160405280848152602001838861259f9190613502565b9052979650505050505050565b5f600a6125c26001670de0b6b3a7640000613877565b6125cc919061392e565b905090565b5f6125dd6020836138c1565b15612634578360405163c7b67cf360e01b815260040161087a918152604060208201819052601d908201527f53697a65206d7573742062652061206d756c7469706c65206f66203332000000606082015260800190565b815f0361268d578360405163c7b67cf360e01b815260040161087a918152604060208201819052601b908201527f53697a65206d7573742062652067726561746572207468616e20300000000000606082015260800190565b66040000000000008211156126ed578360405163c7b67cf360e01b815260040161087a91815260406020808301829052908201527f526f6f742073697a65206d757374206265206c657373207468616e20325e3530606082015260800190565b5f6126f960208461392e565b5f87815260066020526040812080549293509091908261271883613941565b919050559050612729878383612d62565b5f8781526003602090815260408083208484529091529020859061274d828261399d565b50505f878152600460209081526040808320848452825280832085905589835260079091528120805484929061278490849061363b565b9091555090979650505050505050565b61279d82612269565b6127b95760405162461bcd60e51b815260040161087a906135dd565b5f805b82518110156127fc576127e8848483815181106127db576127db613515565b6020026020010151612ddd565b6127f2908361363b565b91506001016127bc565b505f838152600760205260408120805483929061281a908490613502565b9091555050505050565b5f546001600160a01b031633146117c65760405163118cdaa760e01b815233600482015260240161087a565b5f80546001600160a01b038381166001600160a01b0319831681178455604051919092169283917f8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e09190a35050565b6040805160208101909152606081525f835160206128bd919061363b565b6001600160401b038111156128d4576128d46132f7565b6040519080825280601f01601f1916602001820160405280156128fe576020820181803683370190505b5090505f5b84518110156129595784818151811061291e5761291e613515565b602001015160f81c60f81b82828151811061293b5761293b613515565b60200101906001600160f81b03191690815f1a905350600101612903565b505f5b60208110156129af57612970816008613877565b84901b82865183612981919061363b565b8151811061299157612991613515565b60200101906001600160f81b03191690815f1a90535060010161295c565b5060408051602081019091529081529392505050565b5f6001600160ff1b03821115612a285760405162461bcd60e51b815260206004820152602260248201527f496e7075742065786365656473206d6178696d756d20696e743235362076616c604482015261756560f01b606482015260840161087a565b6101005f612a3584613a86565b841690508015612a4d5781612a4981613919565b9250505b6fffffffffffffffffffffffffffffffff811615612a7357612a70608083613502565b91505b77ffffffffffffffff0000000000000000ffffffffffffffff811615612aa157612a9e604083613502565b91505b7bffffffff00000000ffffffff00000000ffffffff00000000ffffffff811615612ad357612ad0602083613502565b91505b7dffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff811615612b0757612b04601083613502565b91505b7eff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff811615612b3c57612b39600883613502565b91505b7f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f811615612b7257612b6f600483613502565b91505b7f3333333333333333333333333333333333333333333333333333333333333333811615612ba857612ba5600283613502565b91505b7f5555555555555555555555555555555555555555555555555555555555555555811615612bde57612bdb600183613502565b91505b5092915050565b5f818152600860205260408120546110d3565b5f6020825f0151511015612c465760405162461bcd60e51b815260206004820152601560248201527410da590819185d18481a5cc81d1bdbc81cda1bdc9d605a1b604482015260640161087a565b6040805160208082528183019092525f916020820181803683370190505090505f5b6020811015612cd857835180518290612c8390602090613502565b612c8d919061363b565b81518110612c9d57612c9d613515565b602001015160f81c60f81b828281518110612cba57612cba613515565b60200101906001600160f81b03191690815f1a905350600101612c68565b5061085081613aa0565b5f83612cef868585612e43565b1495945050505050565b5f80805b838110156117ad57848482818110612d1757612d17613515565b9050602002810190612d299190613609565b612d379060208101906138d4565b612d4391506020613877565b612d4e90604061363b565b612d58908361363b565b9150600101612cfd565b805f612d6d82611b5b565b9050835f5b82811015612db9575f612d886001831b86613502565b5f898152600560209081526040808320848452909152902054909150612dae908461363b565b925050600101612d72565b505f9586526005602090815260408088209588529490529290942091909155505050565b5f828152600460209081526040808320848452909152812054612e01848483612ebd565b5f848152600460209081526040808320868452825280832083905586835260038252808320868452909152812090612e398282612f91565b5090949350505050565b5f82815b8551811015612eb4575f868281518110612e6357612e63613515565b60200260200101519050600285612e7a91906138c1565b5f03612e9157612e8a8382612f64565b9250612e9e565b612e9b8184612f64565b92505b612ea960028661392e565b945050600101612e47565b50949350505050565b5f83815260066020526040812054612ed4906122d5565b612ee090610100613502565b90505f612eec84611b5b565b90505b818111158015612f0b57505f8581526006602052604090205484105b15612f5d575f85815260056020908152604080832087845290915281208054859290612f38908490613502565b90915550612f4b90506001821b8561363b565b9350612f5684611b5b565b9050612eef565b5050505050565b5f61085083835f825f528160205260205f60405f60025afa612f84575f5ffd5b50505f5160c01916919050565b508054612f9d906137ad565b5f825580601f10612fac575050565b601f0160209004905f5260205f2090810190611d8a91905b80821115612fd7575f8155600101612fc4565b5090565b5f5f83601f840112612feb575f5ffd5b5081356001600160401b03811115613001575f5ffd5b6020830191508360208260051b850101111561301b575f5ffd5b9250929050565b5f5f5f60408486031215613034575f5ffd5b8335925060208401356001600160401b03811115613050575f5ffd5b61305c86828701612fdb565b9497909650939450505050565b602080825282518282018190525f918401906040840190835b818110156130ac578351805184526020908101518185015290930192604090920191600101613082565b509095945050505050565b80356001600160a01b03811681146130cd575f5ffd5b919050565b5f5f83601f8401126130e2575f5ffd5b5081356001600160401b038111156130f8575f5ffd5b60208301915083602082850101111561301b575f5ffd5b5f5f5f60408486031215613121575f5ffd5b61312a846130b7565b925060208401356001600160401b03811115613144575f5ffd5b61305c868287016130d2565b5f5f5f5f5f60608688031215613164575f5ffd5b8535945060208601356001600160401b03811115613180575f5ffd5b61318c88828901612fdb565b90955093505060408601356001600160401b038111156131aa575f5ffd5b6131b6888289016130d2565b969995985093965092949392505050565b5f602082840312156131d7575f5ffd5b5035919050565b5f5f604083850312156131ef575f5ffd5b50508035926020909101359150565b5f81516020845280518060208601528060208301604087015e5f604082870101526040601f19601f8301168601019250505092915050565b602081525f61085060208301846131fe565b5f5f5f5f6060858703121561325b575f5ffd5b843593506020850135925060408501356001600160401b0381111561327e575f5ffd5b61328a878288016130d2565b95989497509550505050565b5f5f604083850312156132a7575f5ffd5b823591506132b7602084016130b7565b90509250929050565b602080825282518282018190525f918401906040840190835b818110156130ac5783518352602093840193909201916001016132d9565b634e487b7160e01b5f52604160045260245ffd5b604051601f8201601f191681016001600160401b0381118282101715613333576133336132f7565b604052919050565b5f6001600160401b03821115613353576133536132f7565b5060051b60200190565b5f5f6040838503121561336e575f5ffd5b82356001600160401b03811115613383575f5ffd5b8301601f81018513613393575f5ffd5b80356133a66133a18261333b565b61330b565b8082825260208201915060208360051b8501019250878311156133c7575f5ffd5b602084015b838110156134645780356001600160401b038111156133e9575f5ffd5b8501603f81018a136133f9575f5ffd5b602081013561340a6133a18261333b565b808282526020820191506020808460051b8601010192508c83111561342d575f5ffd5b6040840193505b8284101561344f578335825260209384019390910190613434565b865250506020938401939190910190506133cc565b50976020969096013596505050505050565b602081525f82516040602084015261349160608401826131fe565b9050602084015160408401528091505092915050565b5f5f5f604084860312156134b9575f5ffd5b8335925060208401356001600160401b03811115613144575f5ffd5b5f602082840312156134e5575f5ffd5b610850826130b7565b634e487b7160e01b5f52601160045260245ffd5b818103818111156110d3576110d36134ee565b634e487b7160e01b5f52603260045260245ffd5b6020808252601490820152734578747261206461746120746f6f206c6172676560601b604082015260600190565b5f6001600160401b0382166001600160401b038103613578576135786134ee565b60010192915050565b81835281816020850137505f828201602090810191909152601f909101601f19169091010190565b8481526001600160a01b03841660208201526060604082018190525f906135d39083018486613581565b9695505050505050565b60208082526012908201527150726f6f6620736574206e6f74206c69766560701b604082015260600190565b5f8235603e1983360301811261361d575f5ffd5b9190910192915050565b5f8235601e1983360301811261361d575f5ffd5b808201808211156110d3576110d36134ee565b5f60808201888352876020840152608060408401528086825260a08401905060a08760051b8501019150875f603e198a3603015b8982101561373157868503609f1901845282358181126136a0575f5ffd5b8b01803536829003601e190181126136b6575f5ffd5b604087528101803536829003601e190181126136d0575f5ffd5b016020810190356001600160401b038111156136ea575f5ffd5b8036038213156136f8575f5ffd5b6020604089015261370d606089018284613581565b60209384013598840198909852505093840193929092019160019190910190613682565b505050508281036060840152613748818587613581565b9998505050505050505050565b85815260606020820181905281018490525f6001600160fb1b0385111561377a575f5ffd5b8460051b808760808501378201828103608090810160408501526137a19082018587613581565b98975050505050505050565b600181811c908216806137c157607f821691505b6020821081036137df57634e487b7160e01b5f52602260045260245ffd5b50919050565b6020808252602e908201527f6f6e6c7920746865206f776e65722063616e206d6f766520746f206e6578742060408201526d1c1c9bdd9a5b99c81c195c9a5bd960921b606082015260800190565b634e487b7160e01b5f52603160045260245ffd5b858152846020820152836040820152608060608201525f61386c608083018486613581565b979650505050505050565b80820281158282048414176110d3576110d36134ee565b848152836020820152606060408201525f6135d3606083018486613581565b634e487b7160e01b5f52601260045260245ffd5b5f826138cf576138cf6138ad565b500690565b5f5f8335601e198436030181126138e9575f5ffd5b8301803591506001600160401b03821115613902575f5ffd5b6020019150600581901b360382131561301b575f5ffd5b5f81613927576139276134ee565b505f190190565b5f8261393c5761393c6138ad565b500490565b5f60018201613952576139526134ee565b5060010190565b601f8211156116e157805f5260205f20601f840160051c8101602085101561397e5750805b601f840160051c820191505b81811015612f5d575f815560010161398a565b8135601e198336030181126139b0575f5ffd5b820180356001600160401b03811180156139c8575f5ffd5b8136036020840113156139d9575f5ffd5b5f9050506139f1816139eb85546137ad565b85613959565b5f601f821160018114613a25575f8315613a0e5750838201602001355b5f19600385901b1c1916600184901b178555610fe1565b5f85815260208120601f198516915b82811015613a5657602085880181013583559485019460019092019101613a34565b5084821015613a75575f1960f88660031b161c19602085880101351681555b50505050600190811b019091555050565b5f600160ff1b8201613a9a57613a9a6134ee565b505f0390565b805160208083015191908110156137df575f1960209190910360031b1b1691905056fea26469706673582212201e3514b1c3fabe754ed0d7e9cc3d506f10369864d5ab3e8889771c58b041754564736f6c634300081c0033"


class PDPVerifier:
    def __init__(self, w3: Web3, address: str):
        self.w3 = w3
        self.address = Web3.to_checksum_address(address)
        self.contract = w3.eth.contract(
            address=self.address,
            abi=PDPVerifierMetaData.ABI
        )
    
    # Constants
    def extra_data_max_size(self) -> int:
        return self.contract.functions.EXTRA_DATA_MAX_SIZE().call()
    
    def leaf_size(self) -> int:
        return self.contract.functions.LEAF_SIZE().call()
    
    def max_enqueued_removals(self) -> int:
        return self.contract.functions.MAX_ENQUEUED_REMOVALS().call()
    
    def max_root_size(self) -> int:
        return self.contract.functions.MAX_ROOT_SIZE().call()
    
    def no_challenge_scheduled(self) -> int:
        return self.contract.functions.NO_CHALLENGE_SCHEDULED().call()
    
    def no_proven_epoch(self) -> int:
        return self.contract.functions.NO_PROVEN_EPOCH().call()
    
    def randomness_precompile(self) -> str:
        return self.contract.functions.RANDOMNESS_PRECOMPILE().call()
    
    def seconds_in_day(self) -> int:
        return self.contract.functions.SECONDS_IN_DAY().call()
    
    def get_challenge_finality(self) -> int:
        return self.contract.functions.getChallengeFinality().call()
    
    def get_challenge_range(self, set_id: int) -> int:
        return self.contract.functions.getChallengeRange(set_id).call()
    
    def get_next_challenge_epoch(self, set_id: int) -> int:
        return self.contract.functions.getNextChallengeEpoch(set_id).call()
    
    def get_next_proof_set_id(self) -> int:
        return self.contract.functions.getNextProofSetId().call()
    
    def get_next_root_id(self, set_id: int) -> int:
        return self.contract.functions.getNextRootId(set_id).call()
    
    def get_proof_set_last_proven_epoch(self, set_id: int) -> int:
        return self.contract.functions.getProofSetLastProvenEpoch(set_id).call()
    
    def get_proof_set_leaf_count(self, set_id: int) -> int:
        return self.contract.functions.getProofSetLeafCount(set_id).call()
    
    def get_proof_set_listener(self, set_id: int) -> str:
        return self.contract.functions.getProofSetListener(set_id).call()
    
    def get_proof_set_owner(self, set_id: int) -> Tuple[str, str]:
        return self.contract.functions.getProofSetOwner(set_id).call()
    
    def get_randomness(self, epoch: int) -> int:
        return self.contract.functions.getRandomness(epoch).call()
    
    def get_root_cid(self, set_id: int, root_id: int) -> Tuple[bytes]:
        return self.contract.functions.getRootCid(set_id, root_id).call()
    
    def get_root_leaf_count(self, set_id: int, root_id: int) -> int:
        return self.contract.functions.getRootLeafCount(set_id, root_id).call()
    
    def get_scheduled_removals(self, set_id: int) -> List[int]:
        return self.contract.functions.getScheduledRemovals(set_id).call()
    
    def get_sum_tree_counts(self, set_id: int, root_id: int) -> int:
        return self.contract.functions.getSumTreeCounts(set_id, root_id).call()
    
    def height_from_index(self, index: int) -> int:
        return self.contract.functions.heightFromIndex(index).call()
    
    def height_of_tree(self, set_id: int) -> int:
        return self.contract.functions.heightOfTree(set_id).call()
    
    def owner(self) -> str:
        return self.contract.functions.owner().call()
    
    def proof_set_live(self, set_id: int) -> bool:
        return self.contract.functions.proofSetLive(set_id).call()
    
    def root_challengable(self, set_id: int, root_id: int) -> bool:
        return self.contract.functions.rootChallengable(set_id, root_id).call()
    
    def root_live(self, set_id: int, root_id: int) -> bool:
        return self.contract.functions.rootLive(set_id, root_id).call()
    
    def sum_tree_counts(self, set_id: int, root_id: int) -> int:
        return self.contract.functions.sumTreeCounts(set_id, root_id).call()
    
    def find_root_ids(self, set_id: int, leaf_indices: List[int]) -> List[Tuple[int, int]]:
        return self.contract.functions.findRootIds(set_id, leaf_indices).call()
    
    def make_root(self, tree: List[List[bytes]], leaf_count: int) -> Tuple[Tuple[bytes], int]:
        return self.contract.functions.makeRoot(tree, leaf_count).call()
    
    def add_roots(self, account: LocalAccount, set_id: int, root_data: List[Tuple], extra_data: bytes, gas_limit: int = 1000000) -> str:
        tx = self.contract.functions.addRoots(
            set_id, root_data, extra_data
        ).build_transaction({
            'from': account.address,
            'gas': gas_limit,
            'nonce': self.w3.eth.get_transaction_count(account.address),
        })
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()
    
    def claim_proof_set_ownership(self, account: LocalAccount, set_id: int, gas_limit: int = 1000000) -> str:
        tx = self.contract.functions.claimProofSetOwnership(set_id).build_transaction({
            'from': account.address,
            'gas': gas_limit,
            'nonce': self.w3.eth.get_transaction_count(account.address),
        })
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()
    
    def create_proof_set(self, account: LocalAccount, listener_addr: str, extra_data: bytes, value: int = 0, gas_limit: int = 1000000) -> str:
        tx = self.contract.functions.createProofSet(
            listener_addr, extra_data
        ).build_transaction({
            'from': account.address,
            'value': value,
            'gas': gas_limit,
            'nonce': self.w3.eth.get_transaction_count(account.address),
        })
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()
    
    def delete_proof_set(self, account: LocalAccount, set_id: int, extra_data: bytes, gas_limit: int = 1000000) -> str:
        tx = self.contract.functions.deleteProofSet(set_id, extra_data).build_transaction({
            'from': account.address,
            'gas': gas_limit,
            'nonce': self.w3.eth.get_transaction_count(account.address),
        })
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()
    
    def next_proving_period(self, account: LocalAccount, set_id: int, challenge_epoch: int, extra_data: bytes, gas_limit: int = 1000000) -> str:
        tx = self.contract.functions.nextProvingPeriod(
            set_id, challenge_epoch, extra_data
        ).build_transaction({
            'from': account.address,
            'gas': gas_limit,
            'nonce': self.w3.eth.get_transaction_count(account.address),
        })
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()
    
    def propose_proof_set_owner(self, account: LocalAccount, set_id: int, new_owner: str, gas_limit: int = 1000000) -> str:
        tx = self.contract.functions.proposeProofSetOwner(set_id, new_owner).build_transaction({
            'from': account.address,
            'gas': gas_limit,
            'nonce': self.w3.eth.get_transaction_count(account.address),
        })
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()
    
    def prove_possession(self, account: LocalAccount, set_id: int, proofs: List[Tuple], value: int = 0, gas_limit: int = 1000000) -> str:
        tx = self.contract.functions.provePossession(set_id, proofs).build_transaction({
            'from': account.address,
            'value': value,
            'gas': gas_limit,
            'nonce': self.w3.eth.get_transaction_count(account.address),
        })
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()
    
    def renounce_ownership(self, account: LocalAccount, gas_limit: int = 1000000) -> str:
        tx = self.contract.functions.renounceOwnership().build_transaction({
            'from': account.address,
            'gas': gas_limit,
            'nonce': self.w3.eth.get_transaction_count(account.address),
        })
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()
    
    def schedule_removals(self, account: LocalAccount, set_id: int, root_ids: List[int], extra_data: bytes, gas_limit: int = 1000000) -> str:
        tx = self.contract.functions.scheduleRemovals(
            set_id, root_ids, extra_data
        ).build_transaction({
            'from': account.address,
            'gas': gas_limit,
            'nonce': self.w3.eth.get_transaction_count(account.address),
        })
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()
    
    def transfer_ownership(self, account: LocalAccount, new_owner: str, gas_limit: int = 1000000) -> str:
        tx = self.contract.functions.transferOwnership(new_owner).build_transaction({
            'from': account.address,
            'gas': gas_limit,
            'nonce': self.w3.eth.get_transaction_count(account.address),
        })
        
        signed_tx = account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()


def new_pdp_verifier(w3: Web3, address: str) -> PDPVerifier:
    return PDPVerifier(w3, address)


def deploy_pdp_verifier(w3: Web3, account: LocalAccount, challenge_finality: int, gas_limit: int = 3000000) -> Tuple[str, str]:
    
    contract_factory = w3.eth.contract(
        abi=PDPVerifierMetaData.ABI,
        bytecode=PDPVerifierMetaData.BIN
    )
    
    tx = contract_factory.constructor(challenge_finality).build_transaction({
        'from': account.address,
        'gas': gas_limit,
        'nonce': w3.eth.get_transaction_count(account.address),
    })
    
    signed_tx = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
    
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
    
    return receipt.contractAddress, tx_hash.hex()
