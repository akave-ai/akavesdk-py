import time
from typing import Optional
from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_account import Account
from eth_account.signers.local import LocalAccount
from .contracts import StorageContract, AccessManagerContract

class TransactionFailedError(Exception):
    """Raised when a transaction fails (receipt status is 0)."""
    pass

class Config:
    """Configuration for the Ethereum storage contract client."""
    def __init__(self, dial_uri: str, private_key: str, storage_contract_address: str, access_contract_address: Optional[str] = None):
        self.dial_uri = dial_uri
        self.private_key = private_key
        self.storage_contract_address = storage_contract_address
        self.access_contract_address = access_contract_address

    @staticmethod
    def default():
        return Config(dial_uri="", private_key="", storage_contract_address="", access_contract_address="")

class Client:
    """Represents the Ethereum storage client."""
    def __init__(self, web3: Web3, auth: LocalAccount, storage: StorageContract, access_manager: Optional[AccessManagerContract] = None):
        self.web3 = web3
        self.auth = auth
        self.storage = storage
        self.access_manager = access_manager
        # self.ticker = 0.2  # 200ms polling interval (currently unused)

    @classmethod
    def dial(cls, config: Config) -> 'Client':
        """Creates a new IPC client with the given configuration.
        
        Args:
            config: Client configuration
            
        Returns:
            A new IPC client instance
        """
        # Initialize Web3 using HTTP provider
        web3 = Web3(Web3.HTTPProvider(config.dial_uri))
        if not web3.is_connected():
            raise ConnectionError(f"Failed to connect to Ethereum node at {config.dial_uri}")

        # Add POA middleware if needed (common for testnets like Goerli, Sepolia)
        # Consider making this optional based on chain type
        web3.middleware_onion.inject(geth_poa_middleware, layer=0) 

        # Create account from private key
        try:
            account = Account.from_key(config.private_key)
        except ValueError as e:
            raise ValueError(f"Invalid private key: {e}") from e

        # Initialize contracts
        storage = StorageContract(web3, config.storage_contract_address)
        access_manager = None
        if config.access_contract_address:
            access_manager = AccessManagerContract(web3, config.access_contract_address)

        return cls(web3, account, storage, access_manager)

    @staticmethod
    def _wait_for_tx_receipt(web3_instance: Web3, tx_hash: str, timeout: int = 120, poll_latency: float = 0.5):
        """Waits for a transaction receipt and raises an error if it failed."""
        try:
            receipt = web3_instance.eth.wait_for_transaction_receipt(
                tx_hash, timeout=timeout, poll_latency=poll_latency
            )
            if receipt.status == 0:
                # Consider adding more details from the receipt if available
                raise TransactionFailedError(f"Transaction {tx_hash} failed.")
            return receipt
        except Exception as e: # Catch specific web3 exceptions if needed
             raise TimeoutError(f"Timeout waiting for transaction {tx_hash}") from e

    @classmethod
    def deploy_storage(cls, config: Config):
        """Deploys Storage and AccessManager contracts.

        Requires ABI and Bytecode to be available. 
        Assumes they are located in the 'contracts' module/package.
        Example: from .contracts import STORAGE_ABI, STORAGE_BYTECODE, ...
        """
        # Reuse dial logic for setup? Maybe factor out Web3/Account setup?
        web3 = Web3(Web3.HTTPProvider(config.dial_uri))
        if not web3.is_connected():
            raise ConnectionError(f"Failed to connect to Ethereum node at {config.dial_uri}")
        
        # Add POA middleware
        web3.middleware_onion.inject(geth_poa_middleware, layer=0)

        try:
            account = Account.from_key(config.private_key)
        except ValueError as e:
            raise ValueError(f"Invalid private key: {e}") from e
            
        # --- Deployment requires ABI and Bytecode ---
        # Ensure these are correctly imported or defined
        try:
            # This assumes you have ABI/Bytecode defined in your contracts package/module
            from .contracts import storage_abi, storage_bytecode, access_manager_abi, access_manager_bytecode
        except ImportError:
            raise ImportError("Storage/AccessManager ABI and Bytecode not found. Ensure they are defined in akavesdk-py/private/ipc/contracts.")
            
        gas_price = web3.eth.gas_price # Consider using maxFeePerGas/maxPriorityFeePerGas for EIP-1559

        # Deploy Storage Contract
        Storage = web3.eth.contract(abi=storage_abi, bytecode=storage_bytecode)
        print(f"Deploying Storage contract from {account.address}...")
        construct_txn = Storage.constructor().build_transaction({
            'from': account.address,
            'gas': 5000000, # Estimate gas properly
            'gasPrice': gas_price, # Use EIP-1559 fields if applicable
            'nonce': web3.eth.get_transaction_count(account.address)
        })
        signed_tx = account.sign_transaction(construct_txn)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        print(f"Storage deployment transaction sent: {tx_hash.hex()}")
        storage_receipt = cls._wait_for_tx_receipt(web3, tx_hash)
        storage_address = storage_receipt.contractAddress
        print(f"Storage contract deployed at: {storage_address}")

        # Deploy Access Manager Contract
        AccessManager = web3.eth.contract(abi=access_manager_abi, bytecode=access_manager_bytecode)
        print(f"Deploying AccessManager contract...")
        # Pass the deployed storage address to the constructor
        construct_txn = AccessManager.constructor(storage_address).build_transaction({
            'from': account.address,
            'gas': 5000000, # Estimate gas properly
            'gasPrice': gas_price, # Use EIP-1559 fields if applicable
            # Fetch nonce again for the second transaction
            'nonce': web3.eth.get_transaction_count(account.address) 
        })
        signed_tx = account.sign_transaction(construct_txn)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        print(f"AccessManager deployment transaction sent: {tx_hash.hex()}")
        access_manager_receipt = cls._wait_for_tx_receipt(web3, tx_hash)
        access_manager_address = access_manager_receipt.contractAddress
        print(f"AccessManager contract deployed at: {access_manager_address}")

        # Create contract instances for the client
        storage_instance = StorageContract(web3, storage_address)
        access_manager_instance = AccessManagerContract(web3, access_manager_address)

        # Update config with deployed addresses if needed, or return them separately
        # config.storage_contract_address = storage_address
        # config.access_contract_address = access_manager_address

        # Return the client configured with the newly deployed contracts and the addresses
        deployed_client = cls(web3, account, storage_instance, access_manager_instance)
        return deployed_client, storage_address, access_manager_address # Returning addresses too

    # Instance method to wait for transactions initiated via this client instance
    def wait_for_tx(self, tx_hash: str, timeout: int = 120, poll_latency: float = 0.5) -> None:
        """Waits for a transaction initiated by this client to be mined.
        
        Args:
            tx_hash: Transaction hash to wait for.
            timeout: Time in seconds to wait for the transaction.
            poll_latency: Time in seconds to wait between polls.
            
        Raises:
            TransactionFailedError: If the transaction receipt indicates failure.
            TimeoutError: If the transaction is not mined within the timeout.
        """
        Client._wait_for_tx_receipt(self.web3, tx_hash, timeout, poll_latency)

# Example usage (illustrative):
# if __name__ == '__main__':
#     # Assumes you have a running node (e.g., Ganache) and contract ABI/Bytecode
#     cfg = Config.default()
#     cfg.dial_uri = "http://127.0.0.1:8545" 
#     # Replace with a valid private key with funds on the network
#     cfg.private_key = "0x..." # DO NOT COMMIT REAL PRIVATE KEYS

#     try:
#         print("Deploying contracts...")
#         # Deployment requires ABI/Bytecode in contracts module
#         # deployed_client, storage_addr, access_addr = Client.deploy_storage(cfg)
#         # print(f"Deployed Storage: {storage_addr}, AccessManager: {access_addr}")

#         # Example: Using an existing contract
#         cfg.storage_contract_address = "0x..." # Address of deployed Storage
#         cfg.access_contract_address = "0x..." # Address of deployed AccessManager
#         client = Client.dial(cfg)
#         print("Client dialed.")
        
#         # Example contract interaction (assuming StorageContract has create_bucket)
#         # tx_hash = client.storage.create_bucket("my-test-bucket", client.auth.address, cfg.private_key)
#         # print(f"Create bucket tx sent: {tx_hash}")
#         # client.wait_for_tx(tx_hash) 
#         # print("Bucket created successfully.")

#     except (ConnectionError, ValueError, TransactionFailedError, TimeoutError, ImportError) as e:
#         print(f"Error: {e}")
