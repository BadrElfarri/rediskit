import base64
import gzip
import json
import re

import zstd
from nacl import encoding, secret
from nacl.utils import random

from rediskit import config


class Encrypter:
    VERSION_PREFIX = "__enc_v"

    # EncryptedCompressedBase64Box
    def __init__(self, keyHexDict: dict[str, str] = config.REDISKIT_ENCRYPTION_SECRET) -> None:
        """
        keysBase64 shall have the following format {"__enc_v1": "32-byte key"...,"__enc_vn": ...}
        """
        self.encryptionKeys = keyHexDict
        self.latestVersion = list(self.encryptionKeys.keys())[-1]

    def _getSecretBox(self, version: str) -> secret.SecretBox:
        hexKey = self.encryptionKeys.get(version)
        if not hexKey:
            raise ValueError(f"Encryption key for version {version} not found")
        # Ensure the key is in the proper format (32 bytes after decoding from hex)
        return secret.SecretBox(hexKey.encode(), encoder=encoding.HexEncoder)

    def encrypt[T: str | bytes | None](self, data: T, raiseIfEncrypted: bool = True, useZstd: bool = True) -> T:
        if data is None:
            return None  # type: ignore # not able to check this properly
        elif isinstance(data, str):
            dataToEncrypt: bytes = data.encode()
            isText = True
        elif isinstance(data, bytes):
            dataToEncrypt = data
            isText = False
        else:
            raise ValueError("data expected to be bytes or str")

        if self.isEncrypted(dataToEncrypt, raiseIfEncrypted):
            return data

        compressedData: bytes = zstd.compress(dataToEncrypt) if useZstd else gzip.compress(dataToEncrypt)
        tagBytes = b"zstd" if useZstd else b"gzip"

        cipherText = self._getSecretBox(self.latestVersion).encrypt(compressedData, encoder=encoding.Base64Encoder)
        token = self.latestVersion.encode() + b"|" + tagBytes + b":" + cipherText

        return token.decode() if isText else token  # type: ignore # not able to check this properly

    def decrypt[T: str | bytes | None](self, data: T) -> T:
        if data is None:
            return None  # type: ignore # not able to check this properly
        elif isinstance(data, str):
            dataToDecrypt: bytes = data.encode()
            isText = True
        elif isinstance(data, bytes):
            dataToDecrypt = data
            isText = False
        else:
            raise ValueError("data expected to be bytes or str")

        try:
            if dataToDecrypt.startswith(self.VERSION_PREFIX.encode()):
                if b"|" in dataToDecrypt:
                    # Format: __enc_vX|compression:ciphertext
                    pre, rest = dataToDecrypt.split(b"|", 1)
                    version = pre
                    compressionTag, ciphertext = rest.split(b":", 1)
                elif b":" in dataToDecrypt:
                    # Format: __enc_vX:ciphertext
                    version, ciphertext = dataToDecrypt.split(b":", 1)
                    compressionTag = b"gzip"
                else:
                    raise ValueError("Invalid encrypted data format")
            else:
                # No version: legacy, fallback to v1 + gzip
                version = f"{self.VERSION_PREFIX}1".encode()
                compressionTag = b"gzip"
                ciphertext = dataToDecrypt
        except ValueError:
            raise ValueError("Invalid encrypted data format; expected version prefix.")

        secret_box = self._getSecretBox(version.decode())
        compressed_data = secret_box.decrypt(ciphertext, encoder=encoding.Base64Encoder)

        if compressionTag == b"zstd":
            deCompressed = zstd.decompress(compressed_data)
        elif compressionTag == b"gzip":
            deCompressed = gzip.decompress(compressed_data)
        else:
            raise ValueError(f"Unknown compression '{compressionTag.decode()}' in encrypted data.")

        return deCompressed.decode() if isText else deCompressed  # type: ignore # not able to check this properly

    @staticmethod
    def getEncryptionKeyVersionNumber(versionKey: str) -> int:
        match = re.match(r"^__enc_v(\d+)$", versionKey)
        if not match:
            raise ValueError(f"Invalid version string: {versionKey}")
        return int(match.group(1))

    @staticmethod
    def isEncrypted(data: str | bytes | None, raiseIfEncrypted: bool = False) -> bool:
        # NB! can be false positive if a secret starts with "__enc_v"..., this risk is just accepted for now
        if data is None:
            return False
        dataBytes = data.encode() if isinstance(data, str) else data
        if not dataBytes.startswith(Encrypter.VERSION_PREFIX.encode()) or b":" not in dataBytes:
            return False
        if raiseIfEncrypted:
            raise Exception("The data is already encrypted")
        return True

    @staticmethod
    def generateNewHexKey() -> str:
        """
        Generate a new 32-byte key and return it as a hex-encoded string.

        >>> key = Encrypter.generateNewHexKey()
        >>> isinstance(key, str)
        True
        >>> len(key) == 64
        True
        """
        newKey = random(secret.SecretBox.KEY_SIZE)  # Generates 32 random bytes.
        hexKey = encoding.HexEncoder.encode(newKey).decode("utf-8")
        return hexKey

    @staticmethod
    def encodeKeysDictToBase64(keys: dict) -> str:
        """
        Convert the keys dictionary to a JSON string and encode it in base64.

        >>> Encrypter.encodeKeysDictToBase64({"v1": "abcdef"})
        'eyJ2MSI6ICJhYmNkZWYifQ=='
        """
        json_str = json.dumps(keys)
        base64_str = base64.b64encode(json_str.encode("utf-8")).decode("utf-8")
        return base64_str

    @staticmethod
    def decodeKeysFromBase64(encodedKeys: str) -> dict:
        """
        Decode a base64-encoded JSON string into a dictionary of keys.

        >>> Encrypter.decodeKeysFromBase64("eyJ2MSI6ICJhYmNkZWYifQ==")
        {'v1': 'abcdef'}
        """
        try:
            # Decode the base64 string into a JSON string.
            json_str = base64.b64decode(encodedKeys).decode("utf-8")
            # Convert the JSON string into a dictionary.
            keys_dict = json.loads(json_str)
            return keys_dict
        except Exception as e:
            raise ValueError("Failed to decode keys from base64 string.") from e

    @staticmethod
    def appendNewEncryptKey(currentKeys: str):
        decodedkeys = Encrypter.decodeKeysFromBase64(currentKeys)
        latestKey = list(decodedkeys.keys())[-1]
        newVersion = f"{Encrypter.VERSION_PREFIX}{Encrypter.getEncryptionKeyVersionNumber(latestKey) + 1}"
        decodedkeys.update({newVersion: Encrypter.generateNewHexKey()})
        return {"newKeysDecoded": decodedkeys, "EncryptedKeys": Encrypter.encodeKeysDictToBase64(decodedkeys)}
