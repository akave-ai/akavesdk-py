
from typing import Optional, Any, Dict
from web3.exceptions import ContractLogicError


def error_hash_to_error(error_data: Any) -> Exception:
    if hasattr(error_data, 'args') and error_data.args:
        error_str = str(error_data.args[0]) if error_data.args else str(error_data)
    else:
        error_str = str(error_data)
    hash_code = None
    if isinstance(error_str, str):
        import re
        hex_match = re.search(r'0x[a-fA-F0-9]{8}', error_str)
        if hex_match:
            hash_code = hex_match.group(0).lower()
    
    error_map = {
        "0x497ef2c2": "BucketAlreadyExists",
        "0x4f4b202a": "BucketInvalid", 
        "0xdc64d0ad": "BucketInvalidOwner",
        "0x938a92b7": "BucketNonexists",
        "0x89fddc00": "BucketNonempty",
        "0x6891dde0": "FileAlreadyExists",
        "0x77a3cbd8": "FileInvalid",
        "0x21584586": "FileNonexists",
        "0xc4a3b6f1": "FileNonempty",
        "0xd09ec7af": "FileNameDuplicate",
        "0xd96b03b1": "FileFullyUploaded",
        "0x702cf740": "FileChunkDuplicate",
        "0xc1edd16a": "BlockAlreadyExists",
        "0xcb20e88c": "BlockInvalid",
        "0x15123121": "BlockNonexists",
        "0x856b300d": "InvalidArrayLength",
        "0x17ec8370": "InvalidFileBlocksCount",
        "0x5660ebd2": "InvalidLastBlockSize",
        "0x1b6fdfeb": "InvalidEncodedSize",
        "0xfe33db92": "InvalidFileCID",
        "0x37c7f255": "IndexMismatch",
        "0xcefa6b05": "NoPolicy",
        "0x5c371e92": "FileNotFilled",
        "0xdad01942": "BlockAlreadyFilled",
        "0x4b6b8ec8": "ChunkCIDMismatch",
        "0x0d6b18f0": "NotBucketOwner",
        "0xc4c1a0c5": "BucketNotFound",
        "0x3bcbb0de": "FileDoesNotExist",
        "0xa2c09fea": "NotThePolicyOwner",
        "0x94289054": "CloneArgumentsTooLong",
        "0x4ca249dc": "Create2EmptyBytecode",
        "0xf3714a9b": "ECDSAInvalidSignatureS",
        "0x367e2e27": "ECDSAInvalidSignatureLength",
        "0xf645eedf": "ECDSAInvalidSignature",
        "0xb73e95e1": "AlreadyWhitelisted",
        "0xe6c4247b": "InvalidAddress",
        "0x584a7938": "NotWhitelisted",
        "0x227bc153": "MathOverflowedMulDiv",
        "0xe7b199a6": "InvalidBlocksAmount",
        "0x59b452ef": "InvalidBlockIndex",
        "0x55cbc831": "LastChunkDuplicate",
        "0x2abde339": "FileNotExists",
        "0x48e0ed68": "NotSignedByBucketOwner",
        "0x923b8cbb": "NonceAlreadyUsed",
        "0x9605a010": "OffsetOutOfBounds",
    }
    
    if hash_code and hash_code in error_map:
        return Exception(error_map[hash_code]) 
    return error_data if isinstance(error_data, Exception) else Exception(str(error_data))


def ignore_offset_error(error: Exception) -> Optional[Exception]:
    mapped_error = error_hash_to_error(error)
    if mapped_error and str(mapped_error) == "OffsetOutOfBounds":
        return None
    return error


def parse_errors_to_hashes() -> None:
    pass