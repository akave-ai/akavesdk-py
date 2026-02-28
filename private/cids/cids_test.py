import pytest
import secrets
import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from multiformats import CID, multihash
    MULTIFORMATS_AVAILABLE = True
except ImportError:
    MULTIFORMATS_AVAILABLE = False
    pytest.skip("multiformats library not available", allow_module_level=True)

try:
    from private.cids.cids import verify_raw, verify, CIDError
except ImportError:
    from .cids import verify_raw, verify, CIDError


def test_verify_raw_valid_cidv0_matches():
    test_data = secrets.token_bytes(128)
    
    v0hash = multihash.digest(test_data, "sha2-256")
    cidv0 = CID("base58btc", 0, "dag-pb", v0hash)
    
    verify_raw(str(cidv0), test_data)


def test_verify_raw_valid_cidv1_matches():
    test_data = secrets.token_bytes(128)
    
    hash_digest = multihash.digest(test_data, "sha2-256")
    expected_cid = CID("base32", 1, "dag-pb", hash_digest)
    
    verify_raw(str(expected_cid), test_data)


def test_verify_raw_cid_mismatch():
    test_data = secrets.token_bytes(128)
    
    hash_digest = multihash.digest(test_data, "sha2-256")
    expected_cid = CID("base32", 1, "dag-pb", hash_digest)
    
    wrong_data = b"different data"
    
    with pytest.raises(CIDError) as exc_info:
        verify_raw(str(expected_cid), wrong_data)
    
    assert "CID mismatch" in str(exc_info.value)


def test_verify_raw_invalid_cid_format():
    test_data = secrets.token_bytes(128)
    
    with pytest.raises(CIDError) as exc_info:
        verify_raw("invalid-cid", test_data)
    
    assert "failed to decode provided CID" in str(exc_info.value)


def test_verify_raw_empty_data():
    empty_data = b""
    
    # Calculate CID for empty data
    hash_digest = multihash.digest(empty_data, "sha2-256")
    empty_cid = CID("base32", 1, "dag-pb", hash_digest)
    
    verify_raw(str(empty_cid), empty_data)


def test_verify_valid_cidv1_matches():
    test_data = secrets.token_bytes(127)
    
    hash_digest = multihash.digest(test_data, "sha2-256")
    expected_cid = CID("base32", 1, "dag-pb", hash_digest)
    
    verify(expected_cid, test_data)


def test_verify_valid_cidv0_matches():
    test_data = secrets.token_bytes(127)
    
    hash_digest = multihash.digest(test_data, "sha2-256")
    cidv0 = CID("base58btc", 0, "dag-pb", hash_digest)
    
    verify(cidv0, test_data)


def test_verify_cid_mismatch():
    test_data = secrets.token_bytes(127)
    
    hash_digest = multihash.digest(test_data, "sha2-256")
    expected_cid = CID("base32", 1, "dag-pb", hash_digest)
    
    wrong_data = b"different data"
    
    with pytest.raises(CIDError) as exc_info:
        verify(expected_cid, wrong_data)
    
    assert "CID mismatch" in str(exc_info.value)


def test_verify_different_hash_algorithms():
    test_data = secrets.token_bytes(64)
    
    hash_256 = multihash.digest(test_data, "sha2-256")
    cid_256 = CID("base32", 1, "dag-pb", hash_256)
    verify(cid_256, test_data)
    
    hash_512 = multihash.digest(test_data, "sha2-512")
    cid_512 = CID("base32", 1, "dag-pb", hash_512)
    verify(cid_512, test_data)


def test_verify_different_codecs():
    test_data = secrets.token_bytes(64)
    
    hash_digest = multihash.digest(test_data, "sha2-256")
    cid_dagpb = CID("base32", 1, "dag-pb", hash_digest)
    verify(cid_dagpb, test_data)
    
    cid_raw = CID("base32", 1, "raw", hash_digest)
    verify(cid_raw, test_data)


def test_verify_large_data():
    large_data = secrets.token_bytes(1024 * 1024)
    
    hash_digest = multihash.digest(large_data, "sha2-256")
    expected_cid = CID("base32", 1, "dag-pb", hash_digest)
    
    verify(expected_cid, large_data)

