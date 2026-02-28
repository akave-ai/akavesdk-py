from __future__ import annotations

from typing import Optional, Dict, Any

from eth_typing import Address, HexStr
from web3 import Web3
from web3.contract import Contract
from eth_account.signers.local import LocalAccount


class AkaveTokenMetaData:
    ABI = [
        {
            "inputs": [],
            "name": "MINTER_ROLE",
            "outputs": [{"internalType": "bytes32", "name": "", "type": "bytes32"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [
                {"internalType": "bytes32", "name": "role", "type": "bytes32"},
                {"internalType": "address", "name": "account", "type": "address"},
            ],
            "name": "grantRole",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function",
        },
    ]


class AkaveTokenCaller:
    def __init__(self, contract: Contract):
        self.contract = contract

    def MINTER_ROLE(self) -> bytes:
        return self.contract.functions.MINTER_ROLE().call()


class AkaveTokenTransactor:
    def __init__(self, contract: Contract, web3: Web3):
        self.contract = contract
        self.web3 = web3

    def grant_role(self, account: LocalAccount, role: bytes, grantee: Address, tx_params: Optional[Dict[str, Any]] = None) -> HexStr:
        params = {"from": account.address, "gasPrice": self.web3.eth.gas_price}
        if tx_params:
            params.update(tx_params)
        tx = self.contract.functions.grantRole(role, grantee).build_transaction(params)
        signed = account.sign_transaction(tx)
        tx_hash = self.web3.eth.send_raw_transaction(getattr(signed, "raw_transaction", getattr(signed, "rawTransaction")))
        return tx_hash.hex()


class AkaveToken:
    def __init__(self, w3: Web3, address: Address):
        self.web3 = w3
        self.contract = w3.eth.contract(address=address, abi=AkaveTokenMetaData.ABI)
        self.caller = AkaveTokenCaller(self.contract)
        self.transactor = AkaveTokenTransactor(self.contract, w3)

    @property
    def address(self) -> Address:
        return self.contract.address


def new_akave_token(w3: Web3, address: Address) -> AkaveToken:
    return AkaveToken(w3, address)


