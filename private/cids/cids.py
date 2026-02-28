from typing import Union

try:
    from multiformats import CID, multihash
    MULTIFORMATS_AVAILABLE = True
except ImportError:
    MULTIFORMATS_AVAILABLE = False
    CID = None


class CIDError(Exception):
    pass


def verify_raw(provided_cid: str, data: bytes) -> None:
    if not MULTIFORMATS_AVAILABLE:
        raise CIDError("multiformats library is not available")
    
    try:
        parsed_cid = CID.decode(provided_cid)
    except Exception as e:
        raise CIDError(f"failed to decode provided CID: {e}")
    
    verify(parsed_cid, data)


def verify(c: 'CID', data: bytes) -> None:
    if not MULTIFORMATS_AVAILABLE:
        raise CIDError("multiformats library is not available")
    
    calculated_cid = _calculate_standard_cid(c, data)
    
    if calculated_cid != c:
        raise CIDError(
            f"CID mismatch: provided {str(c)}, calculated {str(calculated_cid)}"
        )


def _calculate_standard_cid(c: 'CID', data: bytes) -> 'CID':
    
    if not MULTIFORMATS_AVAILABLE:
        raise CIDError("multiformats library is not available")
    
    version = c.version
    codec = c.codec
    
    if hasattr(c, 'hashfun'):
        # hashfun is a multicodec object, convert to string
        hash_code = str(c.hashfun).replace("multihash.get('", "").replace("')", "")
    else:
        hash_code = "sha2-256"
    
    try:
        digest = multihash.digest(data, hash_code)
    except Exception as e:
        raise CIDError(f"failed to create multihash: {e}")
    
    if version == 0:
        return CID("base58btc", 0, "dag-pb", digest)
    elif version == 1:
        base = "base32"  
        if hasattr(c, 'base'):
            base = c.base
        return CID(base, 1, codec, digest)
    else:
        raise CIDError(f"unsupported CID version: {version}")

