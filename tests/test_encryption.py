import gzip
import os
from unittest.mock import patch

import pytest
from nacl import encoding, secret

from rediskit import Encrypter


@pytest.fixture
def encrypter():
    """Fixture to create an Encrypter instance with a fixed key."""
    keys = {"__enc_v1": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
    return Encrypter(keys)


def test_encrypt_decrypt(encrypter):
    """Test that encrypting then decrypting returns the original plaintext."""
    plaintext = "This is a secret message."
    encrypted = encrypter.encrypt(plaintext)
    # Verify that the encrypted string starts with the version key prefix.
    assert encrypted.startswith("__enc_v1")

    decrypted = encrypter.decrypt(encrypted)
    assert decrypted == plaintext


def test_is_encrypted(encrypter):
    """Test the isEncrypted method for both non-encrypted and encrypted strings."""
    plaintext = "Hello World"
    # Plain text should not be recognized as encrypted.
    assert encrypter.is_encrypted(plaintext) is False

    # A properly encrypted string should be detected as encrypted.
    encrypted = encrypter.encrypt(plaintext)
    assert encrypter.is_encrypted(encrypted) is True

    # If raiseIfEncrypted is True, then an exception should be raised.
    with pytest.raises(Exception, match="The data is already encrypted"):
        encrypter.is_encrypted(encrypted, raiseIfEncrypted=True)

    with pytest.raises(Exception, match="The data is already encrypted"):
        encrypter.encrypt(encrypted, raiseIfEncrypted=True)


def test_is_encrypted_none():
    """Test isEncrypted with None input."""
    assert Encrypter.is_encrypted(None) is False


def test_is_encrypted_no_match():
    """Test isEncrypted with data that has correct prefix but isn't properly encrypted."""
    # Has prefix but no colon
    assert Encrypter.is_encrypted("__enc_v1without_colon") is False

    # Has prefix and colon but invalid base64
    with pytest.raises(Exception):
        Encrypter.is_encrypted("__enc_v1:not!valid!base64!", raiseIfEncrypted=True)


def test_decrypt_none(encrypter):
    """Test that decrypting None returns None."""
    assert encrypter.decrypt(None) is None


def test_decrypt_invalid_format(encrypter):
    """
    Test that decrypting a string that is not properly encrypted raises an error.
    When a string without a proper colon separator is passed, the method
    defaults to version '__enc_v1' and then attempts to decode it as Base64,
    which should result in an exception if the data is invalid.
    """
    invalid_encrypted = "invalid_data_without_colon"
    with pytest.raises(Exception):
        encrypter.decrypt(invalid_encrypted)


def test_generate_new_hex_key():
    """Test that generateNewHexKey produces a valid hex string of the proper length."""
    key = Encrypter.generate_new_hex_key()
    # Check that key is a string and its length is 64.
    assert isinstance(key, str)
    assert len(key) == 64
    # Verify that the key is valid hexadecimal.
    try:
        bytes.fromhex(key)
    except ValueError:
        pytest.fail("The generated key is not a valid hex string.")


def test_encode_decode_keys():
    """Test that a keys dictionary roundtrips through base64 encode/decode methods."""
    keys_dict = {"v1": "abcdef"}
    encoded = Encrypter.encode_keys_dict_to_base64(keys_dict)
    decoded = Encrypter.decode_keys_from_base64(encoded)
    assert decoded == keys_dict


def test_append_new_encrypt_key():
    """
    Test that appendNewEncryptKey properly adds a new key version and returns
    the updated keys dictionary as well as a base64-encoded JSON string.
    """
    initial_keys = {"__enc_v1": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234567"}
    encoded_keys = Encrypter.encode_keys_dict_to_base64(initial_keys)
    result = Encrypter.append_new_encrypt_key(encoded_keys)
    new_keys = result["newKeysDecoded"]
    encoded_new_keys = result["EncryptedKeys"]

    # Verify that one new key has been added.
    assert len(new_keys) == len(initial_keys) + 1
    # Since the only key initially is '__enc_v1', expect '__enc_v2' to be added.
    assert "__enc_v2" in new_keys

    # Confirm that the returned EncryptedKeys base64 string decodes to the same dictionary.
    decoded_from_encrypted = Encrypter.decode_keys_from_base64(encoded_new_keys)
    assert decoded_from_encrypted == new_keys


def test_get_secret_box_invalid_key():
    """
    Test that accessing _getSecretBox with an invalid key (not properly hex-encoded)
    results in an error.
    """
    # Create an Encrypter with an invalid hex key.
    keys = {"__enc_v1": "invalid_hex_key"}
    enc = Encrypter(keys)
    with pytest.raises(Exception):
        enc._getSecretBox("__enc_v1")


def test_encrypt_with_new_version():
    """
    Test that encrypting with a keys dictionary that contains a new version (i.e., __enc_v2)
    uses the latest version key for encryption and that decryption returns the original plaintext.
    """
    keys = {
        "__enc_v1": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "__enc_v2": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
    }
    # Encrypter should treat __enc_v2 as the latest version.
    enc = Encrypter(keys)
    plaintext = "Test encryption with new version"
    encrypted = enc.encrypt(plaintext)
    # Confirm that the encrypted string uses __enc_v2 prefix.
    assert encrypted.startswith("__enc_v2"), "Encryption did not use latest version __enc_v2"
    decrypted = enc.decrypt(encrypted)
    assert decrypted == plaintext


def test_encrypt_already_encrypted(encrypter):
    """Test that encrypting already encrypted data returns the original encrypted data."""
    plaintext = "Test message"
    encrypted = encrypter.encrypt(plaintext)
    # Encrypt again should return the same encrypted value
    re_encrypted = encrypter.encrypt(encrypted, raiseIfEncrypted=False)
    assert re_encrypted == encrypted


def test_decrypt_without_version_prefix_defaults_to_v1(encrypter):
    """
    Test that when decrypting data without a version prefix, the decrypt method
    defaults to using '__enc_v1'. This is simulated by encrypting a plaintext,
    removing the version prefix from the encrypted data, and then decrypting.
    """
    plaintext = "Default version fallback test"
    encrypted = encrypter.encrypt(plaintext, useZstd=False)
    # Remove the version prefix and colon to simulate missing version.
    try:
        _, ciphertext = encrypted.split(":", 1)
    except ValueError:
        pytest.fail("Encrypted data format is incorrect.")

    # When decrypting without a version, it should use '__enc_v1' by default.
    decrypted = encrypter.decrypt(ciphertext)
    assert decrypted == plaintext


def test_decrypt_old_version_with_new_keys():
    """
    Test that a ciphertext encrypted with an older version key (e.g., __enc_v1)
    can still be decrypted even if new keys (e.g., __enc_v2) have been added.

    Steps:
      1. Use an instance with only __enc_v1 to encrypt a message.
      2. Create a new instance with both __enc_v1 and a new __enc_v2.
      3. Verify that the ciphertext created with the old key decrypts properly.
    """
    # Step 1: Encrypt using only the old key.
    keys_old = {"__enc_v1": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
    old_enc = Encrypter(keys_old)
    plaintext = "Old version encryption test"
    encrypted_old = old_enc.encrypt(plaintext, useZstd=False)
    # Confirm that the encrypted data uses __enc_v1.
    assert encrypted_old.startswith("__enc_v1")

    # Step 2: Simulate adding a new key by creating an instance with both __enc_v1 and __enc_v2.
    keys_updated = {
        "__enc_v1": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "__enc_v2": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
    }
    new_enc = Encrypter(keys_updated)

    # Step 3: Decrypt the ciphertext produced with the old key.
    decrypted = new_enc.decrypt(encrypted_old)
    assert decrypted == plaintext


@patch("gzip.compress")
@patch("nacl.secret.SecretBox.encrypt")
def test_encrypt_compression_flow(mock_encrypt, mock_compress, encrypter):
    """Test the encryption flow with compression."""
    mock_compress.return_value = b"compressed_data"
    mock_encrypt.return_value = b"encrypted_data"

    plaintext = "Test message"
    encrypter.encrypt(plaintext, useZstd=False)

    # Verify compression was called with encoded plaintext
    mock_compress.assert_called_once_with(plaintext.encode())
    # Verify encrypt was called with compressed data
    mock_encrypt.assert_called_once_with(b"compressed_data", encoder=encoding.Base64Encoder)


@patch("gzip.decompress")
@patch("nacl.secret.SecretBox.decrypt")
def test_decrypt_decompression_flow(mock_decrypt, mock_decompress, encrypter):
    """Test the decryption flow with decompression."""
    mock_decrypt.return_value = b"compressed_data"
    mock_decompress.return_value = b"Test message"

    encrypted = "__enc_v1:encrypted_base64_data"
    decrypted = encrypter.decrypt(encrypted)

    # Verify decrypt was called with base64 encoded ciphertext
    mock_decrypt.assert_called_once_with(b"encrypted_base64_data", encoder=encoding.Base64Encoder)
    # Verify decompress was called with decrypted data
    mock_decompress.assert_called_once_with(b"compressed_data")
    # Verify final result
    assert decrypted == "Test message"


def test_encryption_with_special_characters():
    """Test that encryption/decryption works correctly with special characters and unicode."""
    keys = {"__enc_v1": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
    encrypter = Encrypter(keys)

    # Test with various special characters and Unicode
    special_texts = [
        "Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?\\",
        "Unicode: 你好, こんにちは, 안녕하세요, مرحبا, привет",
        "Emojis: 😀🙌🎉🚀🔒💻🌍🔑",
        "Mixed content with newlines\nand tabs\t and spaces    and other\r\ncharacters",
        # Very long text to test compression efficiency
        "Long text " * 1000,
    ]

    for text in special_texts:
        encrypted = encrypter.encrypt(text)
        decrypted = encrypter.decrypt(encrypted)
        assert decrypted == text, f"Failed to correctly encrypt/decrypt: {text[:50]}..."


def test_key_rotation():
    """Test that key rotation works correctly by decrypting data with older keys."""
    # Create keys for three different versions
    keys = {
        "__enc_v1": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "__enc_v2": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
        "__enc_v3": "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
    }

    # Create encrypters with different subsets of keys
    encrypter_v1 = Encrypter({"__enc_v1": keys["__enc_v1"]})
    encrypter_v2 = Encrypter({"__enc_v1": keys["__enc_v1"], "__enc_v2": keys["__enc_v2"]})
    encrypter_v3 = Encrypter(keys)  # All keys

    # Test data
    plaintext = "Key rotation test data"

    # Encrypt with different versions
    encrypted_v1 = encrypter_v1.encrypt(plaintext, useZstd=False)
    encrypted_v2 = encrypter_v2.encrypt(plaintext, useZstd=False)
    encrypted_v3 = encrypter_v3.encrypt(plaintext, useZstd=False)

    # Verify each is encrypted with the expected version
    assert encrypted_v1.startswith("__enc_v1")
    assert encrypted_v2.startswith("__enc_v2")
    assert encrypted_v3.startswith("__enc_v3")

    # The latest encrypter should be able to decrypt all versions
    assert encrypter_v3.decrypt(encrypted_v1) == plaintext
    assert encrypter_v3.decrypt(encrypted_v2) == plaintext
    assert encrypter_v3.decrypt(encrypted_v3) == plaintext

    # The middle encrypter should be able to decrypt itself and v1, but not v3
    assert encrypter_v2.decrypt(encrypted_v1) == plaintext
    assert encrypter_v2.decrypt(encrypted_v2) == plaintext
    with pytest.raises(ValueError):
        encrypter_v2.decrypt(encrypted_v3)

    # The oldest encrypter should only be able to decrypt itself
    assert encrypter_v1.decrypt(encrypted_v1) == plaintext
    with pytest.raises(ValueError):
        encrypter_v1.decrypt(encrypted_v2)
    with pytest.raises(ValueError):
        encrypter_v1.decrypt(encrypted_v3)


def test_idempotent_encryption():
    """Test that encrypting already encrypted data doesn't change it or double-encrypt it."""
    keys = {"__enc_v1": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
    encrypter = Encrypter(keys)

    plaintext = "Test idempotent encryption"
    encrypted_once = encrypter.encrypt(plaintext, useZstd=False)
    encrypted_twice = encrypter.encrypt(encrypted_once, raiseIfEncrypted=False, useZstd=False)
    encrypted_thrice = encrypter.encrypt(encrypted_twice, raiseIfEncrypted=False, useZstd=False)

    # All encrypted versions should be identical
    assert encrypted_once == encrypted_twice == encrypted_thrice

    # And should decrypt back to the original
    assert encrypter.decrypt(encrypted_once) == plaintext
    assert encrypter.decrypt(encrypted_twice) == plaintext
    assert encrypter.decrypt(encrypted_thrice) == plaintext


# --------------------------------------------------------------------------- #
#                              test fixtures                                  #
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="session")
def key_hex_dict():
    """
    Provide two deterministic 32‑byte hex keys so we get reproducible test
    vectors (all‑zero for v1, all‑one for v2).
    """
    return {
        "__enc_v1": "00" * 32,  # 32‑byte key, hex‑encoded
        "__enc_v2": "11" * 32,
    }


@pytest.fixture()
def enc(key_hex_dict):
    """Concrete Encrypter instance to use in the tests."""
    return Encrypter(key_hex_dict)


# --------------------------------------------------------------------------- #
#                             happy‑path tests                                #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("use_zstd,expected_tag", [(True, "|zstd:"), (False, "|gzip:")])
def test_roundtrip_latest_version(enc, use_zstd, expected_tag):
    """New‑format data should survive a full encrypt/decrypt round‑trip."""
    plaintext = "🎉 some secret text 🎉"

    token = enc.encrypt(plaintext, useZstd=use_zstd)
    assert token.startswith("__enc_v2") and expected_tag in token

    assert enc.decrypt(token) == plaintext


def test_roundtrip_gzip_explicit(enc):
    """Explicitly disable zstd and verify the compression tag changes."""
    plaintext = "plain‑old gzip compressed"
    token = enc.encrypt(plaintext, useZstd=False)
    assert token.startswith("__enc_v2|gzip:")
    assert enc.decrypt(token) == plaintext


# --------------------------------------------------------------------------- #
#                         legacy/compatibility paths                          #
# --------------------------------------------------------------------------- #


def _build_legacy_token(version: str, key_hex: str, data: str, *, with_prefix: bool):
    """
    Helper that mimics how data looked *before* the new format existed.
    Legacy = gzip compression, NO compression tag, ':' delimiter.
    It also lets us build a 'no‑version‑prefix' token to test that path.
    """
    box = secret.SecretBox(key_hex.encode(), encoder=encoding.HexEncoder)
    compressed = gzip.compress(data.encode())
    b64 = box.encrypt(compressed, encoder=encoding.Base64Encoder).decode()
    return f"{version}:{b64}" if with_prefix else b64


def test_decrypt_legacy_v1_with_prefix(enc, key_hex_dict):
    """`__enc_v1:ciphertext` produced before the tag was introduced."""
    token = _build_legacy_token("__enc_v1", key_hex_dict["__enc_v1"], "OLD‑STYLE", with_prefix=True)
    assert enc.decrypt(token) == "OLD‑STYLE"


def test_decrypt_legacy_v1_without_prefix(enc, key_hex_dict):
    """
    Legacy payloads stored *without* any version string must fall back to
    '__enc_v1' + gzip according to the implementation.
    """
    token = _build_legacy_token("__enc_v1", key_hex_dict["__enc_v1"], "NO‑VERSION‑PREFIX", with_prefix=False)
    assert enc.decrypt(token) == "NO‑VERSION‑PREFIX"


def test_encrypt_never_produces_zstd_with_v1(enc):
    """
    Business rule: *old* keys are never written with zstd. The library fulfils
    this because it always uses the latest key when zstd is requested.
    """
    someText = "should not get zstd + v1"
    token = enc.encrypt(someText, useZstd=True)
    assert token.startswith("__enc_v2")  # NOT v1
    assert "|zstd:" in token  # tag visible
    assert enc.decrypt(token) == someText


# --------------------------------------------------------------------------- #
#                            detection / helpers                              #
# --------------------------------------------------------------------------- #


def test_is_encrypted_detection(enc):
    clear = "definitely not encrypted"
    encrypted = enc.encrypt(clear)

    assert not enc.is_encrypted(clear)
    assert enc.is_encrypted(encrypted)

    # If raiseIfEncrypted=True the helper bubbles up through encrypt()
    with pytest.raises(Exception, match="already encrypted"):
        enc.encrypt(encrypted)  # default is raiseIfEncrypted=True

    # But with raiseIfEncrypted=False it becomes a no‑op
    assert enc.encrypt(encrypted, raiseIfEncrypted=False) == encrypted


def test_get_version_number_valid_and_invalid():
    assert Encrypter.get_encryption_key_version_number("__enc_v42") == 42
    with pytest.raises(ValueError):
        Encrypter.get_encryption_key_version_number("enc_v2")  # bad prefix


# --------------------------------------------------------------------------- #
#                         negative / defensive paths                          #
# --------------------------------------------------------------------------- #


def test_decrypt_unknown_compression_tag(enc, key_hex_dict):
    """
    Any compression tag other than gzip|zstd should raise a ValueError.
    """
    # Build a perfectly valid ciphertext but label it with a bogus tag.
    box = secret.SecretBox(key_hex_dict["__enc_v2"].encode(), encoder=encoding.HexEncoder)
    bogus_ct = box.encrypt(gzip.compress(b"bogus"), encoder=encoding.Base64Encoder).decode()
    token = "__enc_v2|lzma:" + bogus_ct

    with pytest.raises(ValueError, match="Unknown compression"):
        enc.decrypt(token)


def test_decrypt_unknown_version(enc, key_hex_dict):
    """
    Decrypting a version for which we have no key must fail fast.
    """
    # Craft a `__enc_v99` token. We deliberately *don't* add a matching key.
    box = secret.SecretBox(key_hex_dict["__enc_v2"].encode(), encoder=encoding.HexEncoder)
    ct = box.encrypt(gzip.compress(b"badVersion"), encoder=encoding.Base64Encoder).decode()
    token = "__enc_v99:" + ct

    with pytest.raises(ValueError, match="Encryption key for version __enc_v99 not found"):
        enc.decrypt(token)


# --------------------------------------------------------------------------- #
#                         1. round‑trip with bytes                            #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("use_zstd, tag", [(True, b"|zstd:"), (False, b"|gzip:")])
def test_encrypt_decrypt_roundtrip_bytes(enc, use_zstd, tag):
    payload = os.urandom(256)  # arbitrary binary blob

    token = enc.encrypt(payload, useZstd=use_zstd)
    assert isinstance(token, bytes)
    assert token.startswith(b"__enc_v2") and tag in token

    plain = enc.decrypt(token)
    assert plain == payload  # integrity check
    assert isinstance(plain, bytes)


# --------------------------------------------------------------------------- #
#                          2. isEncrypted for bytes                           #
# --------------------------------------------------------------------------- #


def test_is_encrypted_bytes(enc):
    raw = b"plain binary data"
    assert enc.is_encrypted(raw) is False

    token = enc.encrypt(raw)
    assert enc.is_encrypted(token) is True

    # raiseIfEncrypted=True must raise
    with pytest.raises(Exception):
        enc.is_encrypted(token, raiseIfEncrypted=True)

    # suppressing the guard returns the identical bytes object
    same = enc.encrypt(token, raiseIfEncrypted=False)
    assert same is token


# --------------------------------------------------------------------------- #
#               3. legacy gzip bytes without version prefix                   #
# --------------------------------------------------------------------------- #


def _legacy_token(version: str, key_hex: str, data: bytes, with_prefix: bool):
    box = secret.SecretBox(key_hex.encode(), encoder=encoding.HexEncoder)
    compressed = gzip.compress(data)
    b64 = box.encrypt(compressed, encoder=encoding.Base64Encoder)
    return (version.encode() + b":" + b64) if with_prefix else b64


def test_decrypt_legacy_bytes_no_prefix(enc, key_hex_dict):
    legacy = _legacy_token("__enc_v1", key_hex_dict["__enc_v1"], b"OLDBIN", with_prefix=False)
    assert enc.decrypt(legacy) == b"OLDBIN"


def test_decrypt_legacy_bytes_with_prefix(enc, key_hex_dict):
    legacy = _legacy_token("__enc_v1", key_hex_dict["__enc_v1"], b"OLDBIN", with_prefix=True)
    assert enc.decrypt(legacy) == b"OLDBIN"


# --------------------------------------------------------------------------- #
#                    4. idempotent encryption for bytes                       #
# --------------------------------------------------------------------------- #


def test_encrypt_already_encrypted_bytes(enc):
    data = os.urandom(64)
    first = enc.encrypt(data)
    second = enc.encrypt(first, raiseIfEncrypted=False)
    third = enc.encrypt(second, raiseIfEncrypted=False)

    assert first == second == third
    assert enc.decrypt(first) == data
