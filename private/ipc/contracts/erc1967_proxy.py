"""
ERC1967Proxy contract bindings for Python.
This file is auto-generated - DO NOT EDIT manually.
"""

from __future__ import annotations

from typing import Optional, Dict, Any, List
from eth_typing import Address, HexStr, HexAddress
from web3 import Web3
from web3.contract import Contract
from eth_account.signers.local import LocalAccount
from eth_account import Account


class ERC1967ProxyMetaData:
    """Metadata for the ERC1967Proxy contract."""
    
    ABI = [
        {
            "inputs": [
                {"internalType": "address", "name": "implementation", "type": "address"},
                {"internalType": "bytes", "name": "_data", "type": "bytes"}
            ],
            "stateMutability": "nonpayable",
            "type": "constructor"
        },
        {
            "inputs": [
                {"internalType": "address", "name": "target", "type": "address"}
            ],
            "name": "AddressEmptyCode",
            "type": "error"
        },
        {
            "inputs": [
                {"internalType": "address", "name": "implementation", "type": "address"}
            ],
            "name": "ERC1967InvalidImplementation",
            "type": "error"
        },
        {
            "inputs": [],
            "name": "ERC1967NonPayable",
            "type": "error"
        },
        {
            "inputs": [],
            "name": "FailedCall",
            "type": "error"
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "address", "name": "implementation", "type": "address"}
            ],
            "name": "Upgraded",
            "type": "event"
        },
        {
            "stateMutability": "payable",
            "type": "fallback"
        }
    ]
    
    # Compiled bytecode for deploying new contracts
    BIN = "0x608060405234801561000f575f5ffd5b506040516103df3803806103df83398101604081905261002e9161024b565b818161003a8282610043565b50505050610330565b61004c826100a1565b6040516001600160a01b038316907fbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b905f90a280511561009557610090828261011c565b505050565b61009d61018f565b5050565b806001600160a01b03163b5f036100db57604051634c9c8ce360e01b81526001600160a01b03821660048201526024015b60405180910390fd5b7f360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc80546001600160a01b0319166001600160a01b0392909216919091179055565b60605f5f846001600160a01b031684604051610138919061031a565b5f60405180830381855af49150503d805f8114610170576040519150601f19603f3d011682016040523d82523d5f602084013e610175565b606091505b5090925090506101868583836101b0565b95945050505050565b34156101ae5760405163b398979f60e01b815260040160405180910390fd5b565b6060826101c5576101c08261020f565b610208565b81511580156101dc57506001600160a01b0384163b155b1561020557604051639996b31560e01b81526001600160a01b03851660048201526024016100d2565b50805b9392505050565b80511561021e57805160208201fd5b60405163d6bda27560e01b815260040160405180910390fd5b634e487b7160e01b5f52604160045260245ffd5b5f5f6040838503121561025c575f5ffd5b82516001600160a01b0381168114610272575f5ffd5b60208401519092506001600160401b0381111561028d575f5ffd5b8301601f8101851361029d575f5ffd5b80516001600160401b038111156102b6576102b6610237565b604051601f8201601f19908116603f011681016001600160401b03811182821017156102e4576102e4610237565b6040528181528282016020018710156102fb575f5ffd5b8160208401602083015e5f602083830101528093505050509250929050565b5f82518060208501845e5f920191825250919050565b60a38061033c5f395ff3fe6080604052600a600c565b005b60186014601a565b6050565b565b5f604b7f360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc546001600160a01b031690565b905090565b365f5f375f5f365f845af43d5f5f3e8080156069573d5ff35b3d5ffdfea2646970667358221220be6558ccf6c789c169daef452fea1c8eb936331c00f69faa3606b7da6fbbb3e464736f6c634300081c0033"


class ERC1967ProxyCaller:
    """Read-only binding to the ERC1967Proxy contract."""
    
    def __init__(self, contract: Contract):
        self.contract = contract
    
    # Note: ERC1967Proxy is primarily a proxy contract with minimal read-only functions
    # Most functionality is delegated to the implementation contract


class ERC1967ProxyTransactor:
    """Write-only binding to the ERC1967Proxy contract."""
    
    def __init__(self, contract: Contract, web3: Web3):
        self.contract = contract
        self.web3 = web3
    
    def fallback(self, account: LocalAccount, calldata: bytes, tx_params: Optional[Dict[str, Any]] = None) -> HexStr:
        """
        Execute the fallback function with custom calldata.
        
        Args:
            account: Account to sign the transaction
            calldata: Bytes to send to the fallback function
            tx_params: Additional transaction parameters
            
        Returns:
            Transaction hash
        """
        params = {
            "from": account.address,
            "gasPrice": self.web3.eth.gas_price,
            "data": calldata
        }
        if tx_params:
            params.update(tx_params)
        
        # Build transaction manually since this is a fallback call
        tx = {
            "to": self.contract.address,
            "from": account.address,
            "data": calldata,
            "gasPrice": self.web3.eth.gas_price
        }
        if tx_params:
            tx.update(tx_params)
        
        # Estimate gas if not provided
        if "gas" not in tx:
            tx["gas"] = self.web3.eth.estimate_gas(tx)
        
        # Sign and send transaction
        signed_tx = account.sign_transaction(tx)
        tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()


class ERC1967ProxyFilterer:
    """Log filtering binding for ERC1967Proxy contract events."""
    
    def __init__(self, contract: Contract):
        self.contract = contract
    
    def filter_upgraded(self, from_block: int = 0, to_block: str = "latest", 
                       implementation: Optional[List[Address]] = None):
        """
        Filter for Upgraded events.
        
        Args:
            from_block: Starting block number
            to_block: Ending block number or "latest"
            implementation: List of implementation addresses to filter by
            
        Returns:
            Event filter object
        """
        argument_filters = {}
        if implementation:
            argument_filters["implementation"] = implementation
        
        return self.contract.events.Upgraded.create_filter(
            fromBlock=from_block,
            toBlock=to_block,
            argument_filters=argument_filters
        )
    
    def get_upgraded_events(self, from_block: int = 0, to_block: str = "latest",
                           implementation: Optional[List[Address]] = None):
        """
        Get all past Upgraded events.
        
        Args:
            from_block: Starting block number
            to_block: Ending block number or "latest"
            implementation: List of implementation addresses to filter by
            
        Returns:
            List of event dictionaries
        """
        argument_filters = {}
        if implementation:
            argument_filters["implementation"] = implementation
        
        return self.contract.events.Upgraded.get_logs(
            fromBlock=from_block,
            toBlock=to_block,
            argument_filters=argument_filters
        )


class ERC1967Proxy:
    """Auto-generated Python binding for the ERC1967Proxy contract."""
    
    def __init__(self, web3: Web3, contract_address: HexAddress):
        """
        Initialize the ERC1967Proxy contract interface.
        
        Args:
            web3: Web3 instance
            contract_address: Address of the deployed proxy contract
        """
        self.web3 = web3
        self.contract_address = contract_address
        self.contract = web3.eth.contract(
            address=contract_address,
            abi=ERC1967ProxyMetaData.ABI
        )
        
        # Initialize component bindings
        self.caller = ERC1967ProxyCaller(self.contract)
        self.transactor = ERC1967ProxyTransactor(self.contract, web3)
        self.filterer = ERC1967ProxyFilterer(self.contract)
    
    @classmethod
    def deploy(cls, web3: Web3, account: LocalAccount, implementation: Address, 
               data: bytes, tx_params: Optional[Dict[str, Any]] = None) -> 'ERC1967Proxy':
        """
        Deploy a new ERC1967Proxy contract.
        
        Args:
            web3: Web3 instance
            account: Account to deploy the contract
            implementation: Address of the implementation contract
            data: Initialization data to send to the implementation
            tx_params: Additional transaction parameters
            
        Returns:
            ERC1967Proxy instance for the deployed contract
        """
        contract_factory = web3.eth.contract(
            abi=ERC1967ProxyMetaData.ABI,
            bytecode=ERC1967ProxyMetaData.BIN
        )
        
        # Build deployment transaction
        params = {
            "from": account.address,
            "gasPrice": web3.eth.gas_price
        }
        if tx_params:
            params.update(tx_params)
        
        tx = contract_factory.constructor(implementation, data).build_transaction(params)
        
        # Estimate gas if not provided
        if "gas" not in tx:
            tx["gas"] = web3.eth.estimate_gas(tx)
        
        # Sign and send transaction
        signed_tx = account.sign_transaction(tx)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        
        # Wait for transaction receipt
        tx_receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
        
        return cls(web3, tx_receipt.contractAddress)
    
    def call_implementation(self, method_sig: str, args: List[Any] = None, 
                           caller: Optional[Address] = None) -> Any:
        """
        Call a method on the implementation contract through the proxy.
        
        Args:
            method_sig: Method signature (e.g., "balanceOf(address)")
            args: Method arguments
            caller: Address to use for the call
            
        Returns:
            Result of the method call
        """
        if args is None:
            args = []
        
        # Encode function call
        method_id = self.web3.keccak(text=method_sig)[:4]
        encoded_args = self.web3.codec.encode(
            ['address'] + [type(arg).__name__ for arg in args],
            args
        ) if args else b''
        calldata = method_id + encoded_args
        
        # Make call through proxy
        call_params = {"to": self.contract_address, "data": calldata}
        if caller:
            call_params["from"] = caller
        
        result = self.web3.eth.call(call_params)
        return result
    
    def transact_implementation(self, account: LocalAccount, method_sig: str, 
                               args: List[Any] = None, tx_params: Optional[Dict[str, Any]] = None) -> HexStr:
        """
        Send a transaction to a method on the implementation contract through the proxy.
        
        Args:
            account: Account to sign the transaction
            method_sig: Method signature (e.g., "transfer(address,uint256)")
            args: Method arguments
            tx_params: Additional transaction parameters
            
        Returns:
            Transaction hash
        """
        if args is None:
            args = []
        
        # Encode function call
        method_id = self.web3.keccak(text=method_sig)[:4]
        encoded_args = self.web3.codec.encode(
            ['address'] + [type(arg).__name__ for arg in args],
            args
        ) if args else b''
        calldata = method_id + encoded_args
        
        return self.transactor.fallback(account, calldata, tx_params)


def new_erc1967_proxy(web3: Web3, contract_address: HexAddress) -> ERC1967Proxy:
    """
    Create a new instance of ERC1967Proxy bound to a specific deployed contract.
    
    Args:
        web3: Web3 instance
        contract_address: Address of the deployed contract
        
    Returns:
        ERC1967Proxy instance
    """
    return ERC1967Proxy(web3, contract_address)


def deploy_erc1967_proxy(web3: Web3, account: LocalAccount, implementation: Address,
                        data: bytes, tx_params: Optional[Dict[str, Any]] = None) -> ERC1967Proxy:
    """
    Deploy a new ERC1967Proxy contract.
    
    Args:
        web3: Web3 instance
        account: Account to deploy the contract
        implementation: Address of the implementation contract
        data: Initialization data to send to the implementation
        tx_params: Additional transaction parameters
        
    Returns:
        ERC1967Proxy instance for the deployed contract
    """
    return ERC1967Proxy.deploy(web3, account, implementation, data, tx_params)
