import time
import threading
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union
from web3 import Web3
from web3.middleware.proof_of_authority import ExtraDataToPOAMiddleware
from web3.exceptions import TransactionNotFound
from eth_account import Account
from eth_account.signers.local import LocalAccount
from .contracts import StorageContract, AccessManagerContract
from .ipc import StorageData, sign_block

@dataclass
class Config:
    dial_uri: str = ""
    private_key: str = ""
    storage_contract_address: str = ""
    access_contract_address: str = ""

    @staticmethod
    def default_config() -> 'Config':
        return Config()


@dataclass 
class ContractsAddresses:
    storage: str = ""
    access_manager: str = ""


class TransactionFailedError(Exception):
    pass


class Client:    
    def __init__(self, web3: Web3, auth: LocalAccount, storage: StorageContract, 
                 access_manager: Optional[AccessManagerContract] = None,
                 list_policy_abi: Optional[dict] = None,
                 addresses: Optional[ContractsAddresses] = None,
                 chain_id: Optional[int] = None):
        self.storage = storage
        self.access_manager = access_manager
        self.list_policy_abi = list_policy_abi
        self.auth = auth
        self.eth = web3
        self.addresses = addresses or ContractsAddresses()
        self._chain_id = chain_id

    @classmethod
    def dial(cls, config: Config) -> 'Client':
        try:
            client = Web3(Web3.HTTPProvider(config.dial_uri))
            if not client.is_connected():
                raise ConnectionError(f"Failed to connect to {config.dial_uri}")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {config.dial_uri}: {e}")

        client.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        try:
            private_key = config.private_key
            if private_key.startswith('0x'):
                private_key = private_key[2:]
            account = Account.from_key(private_key)
        except Exception as e:
            raise ValueError(f"Failed to load private key: {e}")

        try:
            chain_id = client.eth.chain_id
        except Exception as e:
            raise ConnectionError(f"Failed to get chain ID: {e}")

        storage = StorageContract(client, config.storage_contract_address)
        
        access_manager = None
        if config.access_contract_address:
            access_manager = AccessManagerContract(client, config.access_contract_address)
        
        list_policy_abi = None
        try:
            from .contracts import ListPolicyMetaData
            list_policy_abi = ListPolicyMetaData.ABI
        except ImportError:
            pass  
        
        addresses = ContractsAddresses(
            storage=config.storage_contract_address,
            access_manager=config.access_contract_address
        )

        ipc_client = cls(
            web3=client,
            auth=account,
            storage=storage, 
            access_manager=access_manager,
            list_policy_abi=list_policy_abi,
            addresses=addresses,
            chain_id=chain_id
        )

        return ipc_client

    @classmethod
    def deploy_contracts(cls, config: Config) -> 'Client':
        eth_client = Web3(Web3.HTTPProvider(config.dial_uri))
        if not eth_client.is_connected():
            raise ConnectionError(f"Failed to connect to {config.dial_uri}")

        # Setup account
        private_key = config.private_key
        if private_key.startswith('0x'):
            private_key = private_key[2:]
        account = Account.from_key(private_key)
        
        chain_id = eth_client.eth.chain_id
        
        client = cls(
            web3=eth_client,
            auth=account,
            storage=None,  
            chain_id=chain_id
        )
        
        try:
            from .contracts import (
                deploy_erc1967_proxy, deploy_access_manager, deploy_list_policy, 
                StorageContract, AccessManagerContract, ListPolicyMetaData
            )
            
            try:
                from .contracts import deploy_akave_token, deploy_storage
            except ImportError:
                raise NotImplementedError(
                    "AkaveToken and Storage deployment functions are not yet implemented. "
                    "The following functions are missing: deploy_akave_token, deploy_storage"
                )
            
            akave_token_addr, tx_hash, token_contract = deploy_akave_token(eth_client, account)
            client.wait_for_tx(tx_hash)
            
            storage_impl_addr, tx_hash, _ = deploy_storage(eth_client, account)
            client.wait_for_tx(tx_hash)
            
            storage_abi = StorageContract.get_abi()
            init_data = StorageContract.encode_function_data("initialize", [akave_token_addr])
            
            storage_proxy_addr, tx_hash, _ = deploy_erc1967_proxy(
                eth_client, account, storage_impl_addr, init_data
            )
            client.wait_for_tx(tx_hash)
            
            storage = StorageContract(eth_client, storage_proxy_addr)
            client.storage = storage
            client.addresses.storage = storage_proxy_addr
            
            minter_role = token_contract.functions.MINTER_ROLE().call()
            tx_hash = token_contract.functions.grantRole(minter_role, storage_proxy_addr).transact({
                'from': account.address
            })
            client.wait_for_tx(tx_hash)
            
            access_addr, tx_hash, access_manager = deploy_access_manager(eth_client, account, storage_proxy_addr)
            client.wait_for_tx(tx_hash)
            
            client.access_manager = access_manager
            client.addresses.access_manager = access_addr
            
            tx_hash = storage.set_access_manager(account, access_addr)
            client.wait_for_tx(tx_hash)
            
            base_list_policy_addr, tx_hash, _ = deploy_list_policy(eth_client, account)
            client.wait_for_tx(tx_hash)
            
            client.list_policy_abi = ListPolicyMetaData.ABI
            
            return client
            
        except ImportError as e:
            raise NotImplementedError(
                f"Contract deployment functions not available: {e}. "
                "This feature requires complete contract bindings."
            )

    def chain_id(self) -> int:
        return self._chain_id

    def wait_for_tx(self, tx_hash: Union[str, bytes], timeout: float = 120.0) -> dict:
        if isinstance(tx_hash, bytes):
            tx_hash = tx_hash.hex()
        if not tx_hash.startswith('0x'):
            tx_hash = '0x' + tx_hash

        try:
            receipt = self.eth.eth.get_transaction_receipt(tx_hash)
            if receipt.status == 1:
                return receipt
            else:
                raise TransactionFailedError("Transaction failed")
        except TransactionNotFound:
            pass  
        except Exception as e:
            raise TransactionFailedError(f"Error checking transaction receipt: {e}")
        
        start_time = time.time()
        poll_interval = 0.2  
        
        while True:
            current_time = time.time()
            if current_time - start_time > timeout:
                raise TimeoutError(f"Timeout waiting for transaction {tx_hash}")
            
            try:
                receipt = self.eth.eth.get_transaction_receipt(tx_hash)
                if receipt.status == 1:
                    return receipt
                else:
                    raise TransactionFailedError("Transaction failed")
            except TransactionNotFound:
                time.sleep(poll_interval)
                continue
            except Exception as e:
                raise TransactionFailedError(f"Error checking transaction receipt: {e}")