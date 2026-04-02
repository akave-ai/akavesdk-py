import pytest


from private.encryption.encryption import derive_key, make_gcm_cipher, encrypt, decrypt, KEY_LENGTH


class TestDeriveKey:

    def test_derive_key_basic(self):
        key = b"parent_key_32_bytes_test12345678"
        info = b"test_info"

        derived = derive_key(key, info)

        assert isinstance(derived, bytes)
        assert len(derived) == KEY_LENGTH

    def test_derive_key_deterministic(self):
        key = b"parent_key_32_bytes_test12345678"
        info = b"same_info"

        derived1 = derive_key(key, info)
        derived2 = derive_key(key, info)

        assert derived1 == derived2

    def test_derive_key_different_info(self):
        key = b"parent_key_32_bytes_test12345678"
        info1 = b"info_one"
        info2 = b"info_two"

        derived1 = derive_key(key, info1)
        derived2 = derive_key(key, info2)

        assert derived1 != derived2

    def test_derive_key_different_parent(self):
        key1 = b"parent_key_1_32bytes_test1234567"
        key2 = b"parent_key_2_32bytes_test1234567"
        info = b"same_info"

        derived1 = derive_key(key1, info)
        derived2 = derive_key(key2, info)

        assert derived1 != derived2

    def test_derive_key_empty_info(self):
        key = b"parent_key_32_bytes_test12345678"
        info = b""

        derived = derive_key(key, info)

        assert isinstance(derived, bytes)
        assert len(derived) == KEY_LENGTH


class TestMakeGCMCipher:

    def test_make_gcm_cipher_basic(self):
        key = b"test_key_32_bytes_for_gcm_cipher"
        info = b"cipher_info"

        cipher, nonce = make_gcm_cipher(key, info)

        assert cipher is not None
        assert isinstance(nonce, bytes)
        assert len(nonce) == 12

    def test_make_gcm_cipher_invalid_key_length_short(self):
        key = b"short_key"
        info = b"info"

        with pytest.raises(ValueError, match=f"Key must be {KEY_LENGTH} bytes long"):
            make_gcm_cipher(key, info)

    def test_make_gcm_cipher_invalid_key_length_long(self):
        key = b"x" * 64
        info = b"info"

        with pytest.raises(ValueError, match=f"Key must be {KEY_LENGTH} bytes long"):
            make_gcm_cipher(key, info)

    def test_make_gcm_cipher_nonce_randomness(self):
        key = b"test_key_32_bytes_for_gcm_cipher"
        info = b"cipher_info"

        cipher1, nonce1 = make_gcm_cipher(key, info)
        cipher2, nonce2 = make_gcm_cipher(key, info)

        assert nonce1 != nonce2

    def test_make_gcm_cipher_valid_32_byte_key(self):
        key = b"a" * 32
        info = b"test"

        cipher, nonce = make_gcm_cipher(key, info)

        assert cipher is not None
        assert len(nonce) == 12


class TestEncrypt:

    def test_encrypt_basic(self):
        key = b"encryption_key_32bytes_test12345"
        data = b"Hello, World!"
        info = b"test_encryption"

        encrypted = encrypt(key, data, info)

        assert isinstance(encrypted, bytes)
        assert len(encrypted) > len(data)
        assert encrypted != data

    def test_encrypt_empty_data(self):
        key = b"encryption_key_32bytes_test12345"
        data = b""
        info = b"empty_test"

        encrypted = encrypt(key, data, info)

        assert isinstance(encrypted, bytes)
        assert len(encrypted) == 12 + 16

    def test_encrypt_large_data(self):
        key = b"encryption_key_32bytes_test12345"
        data = b"x" * 10000
        info = b"large_data"

        encrypted = encrypt(key, data, info)

        assert len(encrypted) == len(data) + 12 + 16

    def test_encrypt_different_each_time(self):
        key = b"encryption_key_32bytes_test12345"
        data = b"Same data"
        info = b"test"

        encrypted1 = encrypt(key, data, info)
        encrypted2 = encrypt(key, data, info)

        assert encrypted1 != encrypted2

    def test_encrypt_binary_data(self):
        key = b"encryption_key_32bytes_test12345"
        data = bytes(range(256))
        info = b"binary"

        encrypted = encrypt(key, data, info)

        assert isinstance(encrypted, bytes)
        assert len(encrypted) == len(data) + 12 + 16


class TestDecrypt:

    def test_decrypt_basic(self):
        key = b"encryption_key_32bytes_test12345"
        data = b"Hello, World!"
        info = b"test_decryption"

        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)

        assert decrypted == data

    def test_decrypt_empty_data(self):
        key = b"encryption_key_32bytes_test12345"
        data = b""
        info = b"empty"

        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)

        assert decrypted == data

    def test_decrypt_large_data(self):
        key = b"encryption_key_32bytes_test12345"
        data = b"y" * 10000
        info = b"large"

        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)

        assert decrypted == data

    def test_decrypt_wrong_key(self):
        key1 = b"encryption_key_1_32bytes_test123"
        key2 = b"encryption_key_2_32bytes_test123"
        data = b"Secret data"
        info = b"test"

        encrypted = encrypt(key1, data, info)

        with pytest.raises(Exception):
            decrypt(key2, encrypted, info)

    def test_decrypt_wrong_info(self):
        key = b"encryption_key_32bytes_test12345"
        data = b"Secret data"
        info1 = b"info_one"
        info2 = b"info_two"

        encrypted = encrypt(key, data, info1)

        with pytest.raises(Exception):
            decrypt(key, encrypted, info2)

    def test_decrypt_corrupted_data(self):
        key = b"encryption_key_32bytes_test12345"
        data = b"Original data"
        info = b"test"

        encrypted = encrypt(key, data, info)

        corrupted = bytearray(encrypted)
        corrupted[20] ^= 0xFF

        with pytest.raises(Exception):
            decrypt(key, bytes(corrupted), info)

    def test_decrypt_insufficient_length(self):
        key = b"encryption_key_32bytes_test12345"
        info = b"test"
        invalid_data = b"short"

        with pytest.raises(ValueError, match="Invalid encrypted data: insufficient length"):
            decrypt(key, invalid_data, info)

    def test_decrypt_exactly_min_length(self):
        key = b"encryption_key_32bytes_test12345"
        info = b"test"
        min_data = b"x" * 28

        with pytest.raises(Exception):
            decrypt(key, min_data, info)


class TestEncryptionRoundtrip:

    def test_roundtrip_simple(self):
        key = b"roundtrip_key_32bytes_test123456"
        data = b"Roundtrip test data"
        info = b"roundtrip"

        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)

        assert decrypted == data

    def test_roundtrip_unicode(self):
        key = b"roundtrip_key_32bytes_test123456"
        data = "Hello 世界! 🌍".encode("utf-8")
        info = b"unicode"

        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)

        assert decrypted == data
        assert decrypted.decode("utf-8") == "Hello 世界! 🌍"

    def test_roundtrip_multiple_times(self):
        key = b"roundtrip_key_32bytes_test123456"
        data = b"Test data"
        info = b"multi"

        for _ in range(10):
            encrypted = encrypt(key, data, info)
            decrypted = decrypt(key, encrypted, info)
            assert decrypted == data

    def test_roundtrip_different_data_sizes(self):
        key = b"roundtrip_key_32bytes_test123456"
        info = b"sizes"

        for size in [1, 10, 100, 1000, 5000]:
            data = b"x" * size
            encrypted = encrypt(key, data, info)
            decrypted = decrypt(key, encrypted, info)
            assert decrypted == data
            assert len(decrypted) == size

    def test_roundtrip_special_characters(self):
        key = b"roundtrip_key_32bytes_test123456"
        data = b"\x00\x01\xff\xfe\n\r\t"
        info = b"special"

        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)

        assert decrypted == data


class TestEncryptionEdgeCases:

    def test_single_byte_encryption(self):
        key = b"edge_case_key_32bytes_test123456"
        data = b"x"
        info = b"single"

        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)

        assert decrypted == data

    def test_encryption_with_null_bytes(self):
        key = b"edge_case_key_32bytes_test123456"
        data = b"\x00" * 100
        info = b"nulls"

        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)

        assert decrypted == data
        assert len(decrypted) == 100

    def test_encryption_deterministic_with_same_nonce(self):
        key = b"edge_case_key_32bytes_test123456"
        data = b"Test data"
        info = b"test"

        encrypted1 = encrypt(key, data, info)
        encrypted2 = encrypt(key, data, info)

        nonce1 = encrypted1[:12]
        nonce2 = encrypted2[:12]

        assert nonce1 != nonce2

    def test_max_data_size(self):
        key = b"edge_case_key_32bytes_test123456"
        data = b"z" * 100000
        info = b"max"

        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)

        assert decrypted == data
        assert len(decrypted) == 100000


@pytest.mark.integration
class TestEncryptionIntegration:

    def test_full_encryption_workflow(self):
        master_key = b"master_key_32bytes_for_testing12"

        file_info = b"bucket/file.txt"
        derived_key = derive_key(master_key, file_info)

        original_data = b"Important file content that needs encryption"

        encrypted_data = encrypt(derived_key, original_data, b"file_encryption")

        decrypted_data = decrypt(derived_key, encrypted_data, b"file_encryption")

        assert decrypted_data == original_data

    def test_multiple_files_with_different_keys(self):
        master_key = b"master_key_32bytes_for_testing12"

        files = [
            (b"bucket1/file1.txt", b"Content of file 1"),
            (b"bucket1/file2.txt", b"Content of file 2"),
            (b"bucket2/file1.txt", b"Content of file 3"),
        ]

        encrypted_files = []
        for file_path, content in files:
            file_key = derive_key(master_key, file_path)
            encrypted = encrypt(file_key, content, b"metadata")
            encrypted_files.append((file_path, encrypted))

        for (file_path, content), (_, encrypted) in zip(files, encrypted_files):
            file_key = derive_key(master_key, file_path)
            decrypted = decrypt(file_key, encrypted, b"metadata")
            assert decrypted == content


class TestKeyLengthConstant:

    def test_key_length_value(self):
        assert KEY_LENGTH == 32

    def test_derived_key_matches_key_length(self):
        key = os.urandom(32)
        info = b"test"
        derived = derive_key(key, info)
        assert len(derived) == KEY_LENGTH


class TestDeriveKeyAdvanced:

    def test_derive_key_with_short_input_key(self):
        short_key = b"short"
        info = b"test"
        derived = derive_key(short_key, info)
        assert len(derived) == KEY_LENGTH

    def test_derive_key_with_long_input_key(self):
        long_key = b"x" * 1000
        info = b"test"
        derived = derive_key(long_key, info)
        assert len(derived) == KEY_LENGTH

    def test_derive_key_with_long_info(self):
        key = b"parent_key_32_bytes_test12345678"
        info = b"x" * 10000
        derived = derive_key(key, info)
        assert len(derived) == KEY_LENGTH

    def test_derive_key_isolation(self):
        key = b"parent_key_32_bytes_test12345678"
        derived1 = derive_key(key, b"context_A")
        derived2 = derive_key(key, b"context_B")

        # Check that they share no common bytes at same positions (probabilistic)
        differences = sum(1 for a, b in zip(derived1, derived2) if a != b)
        assert differences > 20

    @pytest.mark.parametrize("key_size", [1, 16, 32, 64, 128, 256])
    def test_derive_key_various_input_sizes(self, key_size):
        key = b"k" * key_size
        info = b"test_info"
        derived = derive_key(key, info)
        assert len(derived) == KEY_LENGTH
        assert isinstance(derived, bytes)


class TestMakeGCMCipherAdvanced:

    def test_make_gcm_cipher_empty_key(self):
        with pytest.raises(ValueError, match=f"Key must be {KEY_LENGTH} bytes long"):
            make_gcm_cipher(b"", b"info")

    def test_make_gcm_cipher_31_bytes(self):
        with pytest.raises(ValueError, match=f"Key must be {KEY_LENGTH} bytes long"):
            make_gcm_cipher(b"x" * 31, b"info")

    def test_make_gcm_cipher_33_bytes(self):
        with pytest.raises(ValueError, match=f"Key must be {KEY_LENGTH} bytes long"):
            make_gcm_cipher(b"x" * 33, b"info")

    def test_make_gcm_cipher_nonce_entropy(self):
        key = b"test_key_32_bytes_for_gcm_cipher"
        nonces = [make_gcm_cipher(key, b"info")[1] for _ in range(100)]

        # All nonces should be unique
        assert len(set(nonces)) == 100

    def test_make_gcm_cipher_produces_usable_cipher(self):
        key = b"test_key_32_bytes_for_gcm_cipher"
        cipher, nonce = make_gcm_cipher(key, b"info")

        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(b"test data") + encryptor.finalize()

        assert len(ciphertext) > 0
        assert encryptor.tag is not None
        assert len(encryptor.tag) == 16


class TestSecurityProperties:

    def test_tag_tampering_detected(self):
        key = b"security_test_key_32bytes_12345!"
        data = b"Sensitive information"
        info = b"security"

        encrypted = encrypt(key, data, info)

        # Tamper with the last byte (part of tag)
        tampered = encrypted[:-1] + bytes([encrypted[-1] ^ 0xFF])

        with pytest.raises(Exception):
            decrypt(key, tampered, info)

    def test_nonce_tampering_detected(self):
        key = b"security_test_key_32bytes_12345!"
        data = b"Sensitive information"
        info = b"security"

        encrypted = encrypt(key, data, info)

        # Tamper with the first byte (part of nonce)
        tampered = bytes([encrypted[0] ^ 0xFF]) + encrypted[1:]

        with pytest.raises(Exception):
            decrypt(key, tampered, info)

    def test_ciphertext_tampering_middle(self):
        key = b"security_test_key_32bytes_12345!"
        data = b"Sensitive information that is long enough"
        info = b"security"

        encrypted = encrypt(key, data, info)

        # Tamper with middle of ciphertext (between nonce and tag)
        mid = len(encrypted) // 2
        tampered = encrypted[:mid] + bytes([encrypted[mid] ^ 0xFF]) + encrypted[mid + 1 :]

        with pytest.raises(Exception):
            decrypt(key, tampered, info)

    def test_truncated_ciphertext(self):
        key = b"security_test_key_32bytes_12345!"
        data = b"Some data to encrypt"
        info = b"security"

        encrypted = encrypt(key, data, info)

        # Remove last few bytes
        truncated = encrypted[:-5]

        with pytest.raises(Exception):
            decrypt(key, truncated, info)

    def test_extended_ciphertext(self):
        key = b"security_test_key_32bytes_12345!"
        data = b"Some data to encrypt"
        info = b"security"

        encrypted = encrypt(key, data, info)

        # Append extra bytes
        extended = encrypted + b"\x00\x00\x00"

        with pytest.raises(Exception):
            decrypt(key, extended, info)

    def test_cross_context_isolation(self):
        key = b"isolation_test_key_32bytes_12345"
        data = b"Context-sensitive data"

        encrypted_ctx_a = encrypt(key, data, b"context_A")

        # Try to decrypt with different context
        with pytest.raises(Exception):
            decrypt(key, encrypted_ctx_a, b"context_B")

    def test_ciphertext_appears_random(self):
        key = b"randomness_test_key_32bytes_1234"
        data = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        info = b"test"

        encrypted = encrypt(key, data, info)
        ciphertext_portion = encrypted[12:-16]  # Extract ciphertext only

        # Ciphertext should not contain the repetitive pattern
        assert b"AAAA" not in ciphertext_portion


class TestDecryptAdvanced:

    def test_decrypt_boundary_length_27_bytes(self):
        key = b"boundary_test_key_32bytes_12345"
        info = b"test"

        with pytest.raises(ValueError, match="insufficient length"):
            decrypt(key, b"x" * 27, info)

    def test_decrypt_boundary_length_28_bytes(self):
        key = b"boundary_test_key_32bytes_12345"
        info = b"test"
        fake_data = b"x" * 28

        # Should fail due to invalid tag, not length
        with pytest.raises(Exception):
            decrypt(key, fake_data, info)

    def test_decrypt_swapped_nonce_and_tag(self):
        key = b"swap_test_key_32bytes_test12345!"
        data = b"Test data for swap test"
        info = b"test"

        encrypted = encrypt(key, data, info)
        nonce = encrypted[:12]
        ciphertext = encrypted[12:-16]
        tag = encrypted[-16:]

        # Swap nonce and tag positions
        swapped = tag[:12] + ciphertext + nonce + tag[12:]

        with pytest.raises(Exception):
            decrypt(key, swapped, info)

    def test_decrypt_all_zeros(self):
        key = b"zeros_test_key_32bytes_test1234"
        info = b"test"

        with pytest.raises(Exception):
            decrypt(key, b"\x00" * 50, info)

    def test_decrypt_all_ones(self):
        key = b"ones_test_key_32bytes_test12345"
        info = b"test"

        with pytest.raises(Exception):
            decrypt(key, b"\xff" * 50, info)


class TestEncryptAdvanced:

    def test_encrypt_preserves_data_length_relationship(self):
        key = b"length_test_key_32bytes_test123!"
        info = b"test"

        for length in [0, 1, 15, 16, 17, 100, 1000]:
            data = b"d" * length
            encrypted = encrypt(key, data, info)
            assert len(encrypted) == length + 12 + 16

    def test_encrypt_structure_verification(self):
        key = b"structure_test_key_32bytes_1234!"
        data = b"Test data"
        info = b"test"

        encrypted = encrypt(key, data, info)

        # Verify structure
        assert len(encrypted) >= 28
        nonce = encrypted[:12]
        ciphertext = encrypted[12:-16]
        tag = encrypted[-16:]

        assert len(nonce) == 12
        assert len(ciphertext) == len(data)
        assert len(tag) == 16


class TestRoundtripAdvanced:

    @pytest.mark.parametrize(
        "data",
        [
            b"",  # Empty
            b"\x00",  # Single null byte
            b"\xff",  # Single 0xFF byte
            b"\x00" * 100,  # Many null bytes
            b"\xff" * 100,  # Many 0xFF bytes
            bytes(range(256)),  # All byte values
            b"Hello, World!",  # ASCII text
            "Unicode: 日本語 🎉".encode("utf-8"),  # UTF-8
        ],
    )
    def test_roundtrip_parametrized_data(self, data):
        key = b"parametrized_key_32bytes_test123"
        info = b"param_test"

        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)

        assert decrypted == data

    @pytest.mark.parametrize(
        "info",
        [
            b"",
            b"a",
            b"short",
            b"x" * 1000,
            "ファイル.txt".encode("utf-8"),
            b"\x00\xff",
        ],
    )
    def test_roundtrip_parametrized_info(self, info):
        key = b"info_param_key_32bytes_test12345"
        data = b"Test data for info parameter"

        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)

        assert decrypted == data

    def test_roundtrip_random_data(self):
        key = os.urandom(32)
        data = os.urandom(1000)
        info = os.urandom(100)

        encrypted = encrypt(key, data, info)
        decrypted = decrypt(key, encrypted, info)

        assert decrypted == data

    def test_roundtrip_stress_many_iterations(self):
        key = b"stress_test_key_32bytes_test1234"
        info = b"stress"

        for i in range(100):
            data = f"Iteration {i} data".encode()
            encrypted = encrypt(key, data, info)
            decrypted = decrypt(key, encrypted, info)
            assert decrypted == data


class TestErrorMessages:

    def test_invalid_key_length_error_message_contains_expected_length(self):
        with pytest.raises(ValueError) as exc_info:
            make_gcm_cipher(b"short", b"info")

        assert "32" in str(exc_info.value)

    def test_insufficient_length_error_message_is_clear(self):
        key = b"error_msg_test_key_32bytes_12345"

        with pytest.raises(ValueError) as exc_info:
            decrypt(key, b"short", b"info")

        assert "insufficient length" in str(exc_info.value).lower()


class TestConcurrentUsage:

    def test_multiple_encryptions_no_interference(self):
        base_key = b"concurrent_test_key_32bytes_1234"

        test_data = [
            (b"data_1", b"info_1"),
            (b"data_2", b"info_2"),
            (b"data_3", b"info_3"),
        ]

        # Encrypt all
        encrypted_list = []
        for data, info in test_data:
            encrypted = encrypt(base_key, data, info)
            encrypted_list.append((encrypted, info))

        # Decrypt all and verify
        for (encrypted, info), (original_data, _) in zip(encrypted_list, test_data):
            decrypted = decrypt(base_key, encrypted, info)
            assert decrypted == original_data


def test_placeholder():
    """Placeholder test to keep file valid."""
    pass

