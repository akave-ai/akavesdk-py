import struct
import sys
import os
from typing import Dict, List, Any, Union
from Crypto.Hash import keccak
from eth_keys import keys
from eth_utils import to_checksum_address

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from sdk.common import SDKError

class TypedData:
    def __init__(self, name: str, type_name: str):
        self.name = name
        self.type = type_name


class Domain:
    def __init__(self, name: str, version: str, chain_id: int, verifying_contract: str):
        self.name = name
        self.version = version
        self.chain_id = chain_id
        self.verifying_contract = verifying_contract


def sign(private_key_bytes: bytes, domain: Domain, primary_type: str, 
         data_types: Dict[str, List[TypedData]], data_message: Dict[str, Any]) -> bytes:
    """Sign EIP-712 data according to the standard - properly hash bytes fields"""
    try:
        # Clone data_types and add EIP712Domain
        data_types_copy = data_types.copy()
        data_types_copy["EIP712Domain"] = [
            TypedData("name", "string"),
            TypedData("version", "string"),
            TypedData("chainId", "uint256"),
            TypedData("verifyingContract", "address"),
        ]
        
        typed_data_hash = hash_typed_data(domain, primary_type, data_message, data_types_copy)
        
        class EncodedMessage:
            def __init__(self, body):
                self.body = body
        
        encoded_message = EncodedMessage(typed_data_hash)
        
        private_key_obj = keys.PrivateKey(private_key_bytes)
        signature_obj = private_key_obj.sign_msg_hash(encoded_message.body)
        signature_bytes = signature_obj.to_bytes()
        v = signature_bytes[64]
        if v in (0, 1):
            v_out = v + 27
        else:
            v_out = v
        return signature_bytes[:64] + bytes([v_out])
        
    except Exception as e:
        raise SDKError(f"EIP-712 signing failed: {str(e)}")


def encode_type(primary_type: str, types: Dict[str, List[TypedData]]) -> str:
    result = primary_type + "("
    
    first = True
    for field in types[primary_type]:
        if not first:
            result += ","
        result += field.type + " " + field.name
        first = False
    
    result += ")"
    return result


def type_hash(primary_type: str, types: Dict[str, List[TypedData]]) -> bytes:
    encoded_type = encode_type(primary_type, types)
    hash_obj = keccak.new(digest_bits=256)
    hash_obj.update(encoded_type.encode())
    return hash_obj.digest()


def hash_typed_data(domain: Domain, primary_type: str, data_message: Dict[str, Any], 
                   data_types: Dict[str, List[TypedData]]) -> bytes:
    
    domain_message = {
        "name": domain.name,
        "version": domain.version,
        "chainId": domain.chain_id,
        "verifyingContract": domain.verifying_contract,
    }
    
    domain_hash = encode_data("EIP712Domain", domain_message, data_types)
    data_hash = encode_data(primary_type, data_message, data_types)
    raw_data = bytes([0x19, 0x01]) + domain_hash + data_hash
    
    hash_obj = keccak.new(digest_bits=256)
    hash_obj.update(raw_data)
    final_hash = hash_obj.digest()
    
    return final_hash


def encode_data(primary_type: str, data: Dict[str, Any], 
               types: Dict[str, List[TypedData]]) -> bytes:
    
    type_hash_bytes = type_hash(primary_type, types)
    
    encoded_data = [type_hash_bytes]
    
    for field in types[primary_type]:
        value = data[field.name]
        encoded_value = encode_value(value, field.type)
        encoded_data.append(encoded_value)
    
    combined = b''.join(encoded_data)
    hash_obj = keccak.new(digest_bits=256)
    hash_obj.update(combined)
    return hash_obj.digest()


def encode_value(value: Any, type_name: str) -> bytes:
    
    if type_name == "string":
        if not isinstance(value, str):
            raise ValueError(f"expected string, got {type(value)}")
        hash_obj = keccak.new(digest_bits=256)
        hash_obj.update(value.encode())
        return hash_obj.digest()
    
    elif type_name == "bytes":
        if not isinstance(value, (bytes, bytearray)):
            raise ValueError(f"expected bytes, got {type(value)}")
        hash_obj = keccak.new(digest_bits=256)
        hash_obj.update(bytes(value))
        return hash_obj.digest()
    
    elif type_name == "bytes32":
        if isinstance(value, (bytes, bytearray)):
            if len(value) != 32:
                raise ValueError(f"expected 32 bytes, got {len(value)}")
            return bytes(value)
        else:
            raise ValueError(f"expected bytes32, got {type(value)}")
    
    elif type_name == "uint8":
        if not isinstance(value, int):
            raise ValueError(f"expected int, got {type(value)}")
        if not (0 <= value <= 255):
            raise ValueError(f"uint8 value out of range: {value}")
        buf = bytearray(32)
        buf[31] = value
        return bytes(buf)
    
    elif type_name == "uint64":
        if not isinstance(value, int):
            raise ValueError(f"expected int, got {type(value)}")
        if not (0 <= value < 2**64):
            raise ValueError(f"uint64 value out of range: {value}")
        buf = bytearray(32)
        struct.pack_into('>Q', buf, 24, value)
        return bytes(buf)
    
    elif type_name == "uint256":
        if isinstance(value, int):
            if value < 0:
                raise ValueError(f"uint256 cannot be negative: {value}")
            buf = bytearray(32)
            value_bytes = value.to_bytes(32, byteorder='big')
            buf[:] = value_bytes
            return bytes(buf)
        else:
            raise ValueError(f"expected int for uint256, got {type(value)}")
    
    elif type_name == "address":
        if isinstance(value, str):
            addr_str = value.lower()
            if addr_str.startswith('0x'):
                addr_str = addr_str[2:]
            if len(addr_str) != 40:
                raise ValueError(f"invalid address length: {len(addr_str)}")
            addr_bytes = bytes.fromhex(addr_str)
        elif isinstance(value, (bytes, bytearray)):
            if len(value) != 20:
                raise ValueError(f"address must be 20 bytes, got {len(value)}")
            addr_bytes = bytes(value)
        else:
            raise ValueError(f"expected string or bytes for address, got {type(value)}")
        
        buf = bytearray(32)
        buf[12:32] = addr_bytes
        return bytes(buf)
    
    else:
        raise ValueError(f"unsupported type: {type_name}")


def recover_signer_address(signature: bytes, domain: Domain, primary_type: str,
                          data_types: Dict[str, List[TypedData]], data_message: Dict[str, Any]) -> str:
    
    # Clone data_types and add EIP712Domain
    data_types_copy = data_types.copy()
    data_types_copy["EIP712Domain"] = [
        TypedData("name", "string"),
        TypedData("version", "string"),
        TypedData("chainId", "uint256"),
        TypedData("verifyingContract", "address"),
    ]
    
    hash_bytes = hash_typed_data(domain, primary_type, data_message, data_types_copy)
    
    sig_copy = bytearray(signature)
    if sig_copy[64] >= 27:
        sig_copy[64] -= 27
    
    from eth_keys import keys
    signature_obj = keys.Signature(bytes(sig_copy))
    public_key = signature_obj.recover_public_key_from_msg_hash(hash_bytes)
    
    address = public_key.to_checksum_address()
    return address 
