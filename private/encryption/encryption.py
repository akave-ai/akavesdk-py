import os
import hashlib
from typing import Tuple
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
from typing import Tuple, cast

KEY_LENGTH = 32


def derive_key(key: bytes, info: bytes) -> bytes:
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=KEY_LENGTH,
        salt=None,
        info=info,
        backend=default_backend(),
    )
    return hkdf.derive(key)


def make_gcm_cipher(origin_key: bytes, info: bytes) -> Tuple[Cipher, bytes]
    """
    Creates a GCM cipher using the provided key and info.
    The key is derived using HKDF with SHA-256, and a random nonce is generated.
    :param origin_key: The original key to derive from, must be 32 bytes long.
    :param info: Additional information for key derivation.
    :return: A tuple containing the Cipher object and the nonce.
    """
    if len(origin_key) != KEY_LENGTH:
        raise ValueError(f"Key must be {KEY_LENGTH} bytes long")
    key = derive_key(origin_key, info)
    nonce = os.urandom(12)  # AES-GCM standard nonce size
    cipher = Cipher(algorithms.AES(key), modes.GCM(nonce), backend=default_backend())
    return cipher, nonce


def encrypt(key: bytes, data: bytes, info: bytes) -> bytes:
    cipher, nonce = make_gcm_cipher(key, info)
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(data) + encryptor.finalize()
    tag = encryptor.tag # type: ignore[union-attr]
    encrypted_data = nonce + ciphertext + tag
    return cast(bytes, encrypted_data)

def decrypt(key: bytes, encrypted_data: bytes, info: bytes) -> bytes:
    nonce_size = 12  
    tag_size = 16 
    if len(encrypted_data) < nonce_size + tag_size:
        raise ValueError("Invalid encrypted data: insufficient length")
    
    nonce = encrypted_data[:nonce_size]
    ciphertext = encrypted_data[nonce_size:-tag_size]
    tag = encrypted_data[-tag_size:]
    
    derived_key = derive_key(key, info)
    cipher = Cipher(algorithms.AES(derived_key), modes.GCM(nonce, tag), backend=default_backend())
    decryptor = cipher.decryptor()
    decrypted_data = decryptor.update(ciphertext) + decryptor.finalize()
    return cast(bytes, decrypted_data)