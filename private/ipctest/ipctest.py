import asyncio
import time
import threading
from typing import Optional, Union
from web3 import Web3
from web3.exceptions import TransactionNotFound
from eth_account import Account
from eth_account.signers.local import LocalAccount
import logging

_nonce_lock = threading.Lock()

class IPCTestError(Exception):
    pass

class TransactionFailedError(IPCTestError):
    pass

class NonceTooLowError(IPCTestError):
    pass

class ReplaceUnderpricedError(IPCTestError):
    pass

def new_funded_account(
    source_private_key: str,
    dial_uri: str,
    amount_wei: int,
    max_retries: int = 10,
    retry_delay: float = 0.01
) -> LocalAccount:
    try:
        if source_private_key.startswith('0x'):
            source_private_key = source_private_key[2:]
        source_account = Account.from_key(source_private_key)
    except Exception as e:
        raise IPCTestError(f"Failed to load private key: {e}")
    
    try:
        dest_account = Account.create()
    except Exception as e:
        raise IPCTestError(f"Failed to generate private key: {e}")
    
    try:
        web3 = Web3(Web3.HTTPProvider(dial_uri))
        if not web3.is_connected():
            raise IPCTestError(f"Failed to connect to {dial_uri}")
    except Exception as e:
        raise IPCTestError(f"Failed to connect to {dial_uri}: {e}")
    
    try:
        chain_id = web3.eth.chain_id
    except Exception as e:
        raise IPCTestError(f"Failed to get network ID: {e}")
    
    for attempt in range(max_retries):
        try:
            deposit(
                web3=web3,
                dest_address=dest_account.address,
                source_account=source_account,
                amount_wei=amount_wei,
                chain_id=chain_id
            )
            return dest_account
        except (NonceTooLowError, ReplaceUnderpricedError) as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            raise IPCTestError(f"Failed to deposit after {max_retries} attempts: {e}")
        except Exception as e:
            raise IPCTestError(f"Failed to deposit: {e}")
    
    raise IPCTestError(f"Failed to deposit account {max_retries} times")

def deposit(
    web3: Web3,
    dest_address: str,
    source_account: LocalAccount,
    amount_wei: int,
    chain_id: int
) -> None:
    source_address = source_account.address
    
    with _nonce_lock:
        try:
            nonce = web3.eth.get_transaction_count(source_address, 'pending')
        except Exception as e:
            raise IPCTestError(f"Failed to get nonce: {e}")
    
    try:
        gas_price = web3.eth.gas_price
        gas_limit = 21000
        
        transaction = {
            'chainId': chain_id,
            'nonce': nonce,
            'maxFeePerGas': gas_price * 2, 
            'maxPriorityFeePerGas': gas_price, 
            'gas': gas_limit,
            'to': dest_address,
            'value': amount_wei,
            'data': b'',
        }
        signed_txn = source_account.sign_transaction(transaction)  
        tx_hash = web3.eth.send_raw_transaction(signed_txn.rawTransaction)
        wait_for_tx(web3, tx_hash)
        
    except Exception as e:
        error_msg = str(e).lower()
        if 'nonce too low' in error_msg:
            raise NonceTooLowError(f"Nonce too low: {e}")
        elif 'replacement transaction underpriced' in error_msg:
            raise ReplaceUnderpricedError(f"Replacement transaction underpriced: {e}")
        else:
            raise IPCTestError(f"Transaction failed: {e}")

def to_wei(amount: Union[int, float]) -> int:
    return int(amount * 10**18)


def private_key_to_hex(private_key: LocalAccount) -> str:
    pk_bytes = private_key.key
    return pk_bytes.hex()


def wait_for_tx(
    web3: Web3,
    tx_hash: Union[str, bytes],
    timeout: float = 120.0,
    poll_interval: float = 0.2
):
    
    if isinstance(tx_hash, str):
        if tx_hash.startswith('0x'):
            tx_hash = bytes.fromhex(tx_hash[2:])
        else:
            tx_hash = bytes.fromhex(tx_hash)
    
    start_time = time.time()
    
    try:
        receipt = web3.eth.get_transaction_receipt(tx_hash)
        if receipt['status'] == 1:
            return receipt
        else:
            raise IPCTestError("Transaction failed")
    except TransactionNotFound:
        pass 
    except Exception as e:
        raise IPCTestError(f"Error checking transaction receipt: {e}")
    
    while True:
        current_time = time.time()
        if current_time - start_time > timeout:
            raise IPCTestError(f"Timeout waiting for transaction {tx_hash.hex()}")
        
        try:
            receipt = web3.eth.get_transaction_receipt(tx_hash)
            if receipt['status'] == 1:
                return receipt
            else:
                raise IPCTestError("Transaction failed")
        except TransactionNotFound:
            time.sleep(poll_interval)
            continue
        except Exception as e:
            raise IPCTestError(f"Error checking transaction receipt: {e}")
