import web3
from typing import Dict, Optional, List

# List of known error strings from the smart contracts
# Replace these with the actual error strings from your contracts
_KNOWN_ERROR_STRINGS: List[str] = [
    "Storage: bucket doesn't exist",
    "Storage: bucket exists",
    "Storage: file doesn't exist",
    "Storage: file exists",
    "AccessManager: caller is not the owner",
    "AccessManager: caller is not authorized",
    # Add all other known error strings here...
]

# Dictionary to store the mapping from error hash (selector) to error string
_error_hash_to_error_map: Dict[str, str] = {}

def parse_errors_to_hashes() -> None:
    """
    Computes the Keccak256 hash for known error strings and populates the 
    _error_hash_to_error_map. This should be called once on initialization.
    
    The hash calculation mimics Solidity's `keccak256(bytes("Error(string)"))`, 
    taking the first 4 bytes as the selector.
    """
    global _error_hash_to_error_map
    if _error_hash_to_error_map: # Avoid re-parsing if already done
        return

    temp_map = {}
    for error_string in _KNOWN_ERROR_STRINGS:
        # Construct the error signature string
        error_signature = f"Error({error_string})"
        # Compute Keccak256 hash
        hash_bytes = web3.Web3.keccak(text=error_signature)
        # Take the first 4 bytes as the selector (hex representation)
        selector = hash_bytes[:4].hex()
        # Store the mapping (e.g., '0xabcdef12' -> "Storage: bucket exists")
        temp_map[selector] = error_string
        
    _error_hash_to_error_map = temp_map
    print(f"Parsed {len(_error_hash_to_error_map)} error strings into hashes.") # Optional: logging

def error_hash_to_error(error_data: str) -> Optional[str]:
    """
    Attempts to map an error hash (typically from a transaction revert reason)
    to a known human-readable error string.

    Args:
        error_data: The error data string, usually starting with '0x' followed 
                    by the 4-byte error selector (e.g., "0x08c379a0...").

    Returns:
        The corresponding human-readable error string if found, otherwise None.
    """
    if not _error_hash_to_error_map:
        # Ensure hashes are parsed if accessed before explicit call
        print("Warning: Error hashes not parsed yet. Parsing now.") # Optional: logging
        parse_errors_to_hashes()
        
    if not isinstance(error_data, str) or not error_data.startswith('0x') or len(error_data) < 10:
        # Basic validation: expect hex string like '0x' + 8 hex chars (4 bytes) minimum
        return None 

    # Extract the selector (first 4 bytes after '0x')
    selector = error_data[:10] # Takes '0x' + 8 hex characters

    return _error_hash_to_error_map.get(selector)

# Automatically parse errors when the module is imported
parse_errors_to_hashes()

# --- How to use with web3.py ---
# 
# from web3.exceptions import ContractLogicError
# from .errors import error_hash_to_error
# 
# try:
#     # ... make a contract call that might revert ...
#     tx_hash = contract.functions.someFunction().transact({'from': ..., ...})
#     receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
#     if receipt.status == 0:
#         # Try to get revert reason (requires node support)
#         try:
#              tx = web3.eth.get_transaction(tx_hash)
#              revert_reason = web3.eth.call(tx, tx.blockNumber) # Re-call failed tx
#              # web3.py might raise ContractLogicError here with the reason
#         except ContractLogicError as cle:
#              print(f"ContractLogicError: {cle}")
#              # The error data might be in cle.args or similar, requires inspection
#              # Example: cle.args[0] might contain 'execution reverted: 0x...'
#              error_data = str(cle) # Or parse from args
#              human_readable_error = error_hash_to_error(error_data) 
#              if human_readable_error:
#                  print(f"Known revert reason: {human_readable_error}")
#              else:
#                  print(f"Unknown revert reason or data: {error_data}")
#         except Exception as call_exc:
#              print(f"Could not get revert reason: {call_exc}")
#         print("Transaction failed, but couldn't determine exact reason.")
#         
# except ContractLogicError as e:
#     print(f"ContractLogicError on send/call: {e}")
#     # Process error_hash_to_error(str(e)) or e.args as above
#     human_readable_error = error_hash_to_error(str(e))
#     if human_readable_error:
#         print(f"Known revert reason: {human_readable_error}")
#     else:
#         print(f"Unknown revert reason: {e}")
# except Exception as e:
#     print(f"An unexpected error occurred: {e}")
#