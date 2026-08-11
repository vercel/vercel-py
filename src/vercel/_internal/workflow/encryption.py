"""The `encr` payload format, as `@workflow/world-vercel` writes it.

Decryption only, for now: writing `encr` is not implemented, so the payloads
this SDK produces are plain `devl`.

The envelope is two nested layers::

    encr payload:  [ 'e' 'n' 'c' 'r' ][ nonce (12) ][ ciphertext + tag (16) ]
    plaintext:     [ 'd' 'e' 'v' 'l' ][ devalue.stringify output, UTF-8 ]

`encp`, the X25519 sealed-box format, is a different construction and is not
read here.

AES-GCM comes from `cryptography`, which is a large native wheel and so ships
in the ``encryption`` extra rather than in the default install.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
from collections.abc import Callable

KEY_LENGTH = 32
"""AES-256."""

NONCE_LENGTH = 12
TAG_LENGTH = 16


class DecryptionError(Exception):
    """An encrypted payload could not be opened."""


def _load_aes_gcm_decrypt() -> Callable[[bytes, bytes, bytes], bytes | None] | None:
    """Bind `cryptography`'s AES-GCM, or ``None`` without the extra installed.

    At import time, not per call: a run input is hydrated inside the workflow
    sandbox. Binding here means the sandbox only ever calls the host function.
    """
    try:
        from cryptography.exceptions import InvalidTag
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError:
        return None

    def decrypt(key: bytes, nonce: bytes, sealed: bytes) -> bytes | None:
        """Open *sealed* — ciphertext then its 16-byte tag. ``None`` if it fails."""
        try:
            return AESGCM(key).decrypt(nonce, sealed, None)
        except InvalidTag:
            return None

    return decrypt


_AES_GCM_DECRYPT = _load_aes_gcm_decrypt()


def hkdf_sha256(*, ikm: bytes, salt: bytes, info: bytes, length: int) -> bytes:
    """HKDF-SHA256 (RFC 5869): extract *ikm* under *salt*, expand over *info*."""
    prk = hmac.new(salt, ikm, hashlib.sha256).digest()
    okm = bytearray()
    block = b""
    counter = 1
    while len(okm) < length:
        block = hmac.new(prk, block + info + bytes([counter]), hashlib.sha256).digest()
        okm += block
        counter += 1
    return bytes(okm[:length])


def derive_run_key(deployment_key: bytes, *, project_id: str, run_id: str) -> bytes:
    """The AES-256 key for one run, from the deployment's 32-byte secret.

    ``info`` is ``f"{project_id}|{run_id}"``: the project first, a literal
    ``|``, no spaces. Getting any of it wrong yields a key that decrypts
    nothing and says only that the tag failed to authenticate.
    """
    if len(deployment_key) != KEY_LENGTH:
        raise ValueError(f"A deployment key is {KEY_LENGTH} bytes, got {len(deployment_key)}")
    if not project_id:
        raise ValueError("Cannot derive a run key without a project id")
    return hkdf_sha256(
        ikm=deployment_key,
        # 32 zero bytes, spelled the way `world-vercel` spells it. HMAC pads a
        # key shorter than its 64-byte block, so this extracts identically to
        # `b""` and to the absent salt of RFC 5869 §3.1 -- the length carries no
        # meaning of its own, and matching the source is all it is for.
        salt=bytes(32),
        info=f"{project_id}|{run_id}".encode(),
        length=KEY_LENGTH,
    )


def decode_key(encoded: str, *, what: str) -> bytes:
    """Decode a base64 32-byte key."""
    normalized = encoded.strip().replace("-", "+").replace("_", "/")
    try:
        key = base64.b64decode(normalized, validate=True)
    except (binascii.Error, ValueError) as error:
        raise ValueError(f"{what} is not valid base64: {error}") from error
    if len(key) != KEY_LENGTH:
        raise ValueError(f"{what} decodes to {len(key)} bytes, expected {KEY_LENGTH}")
    return key


def open_envelope(key: bytes, payload: bytes) -> bytes:
    """Open the nonce-prefixed AES-GCM envelope an `encr` payload carries.

    *payload* is everything after the 4-byte format prefix. A wrong key and a
    modified payload are the same failure — a tag that does not authenticate —
    so :class:`DecryptionError` names both.
    """
    if _AES_GCM_DECRYPT is None:
        raise DecryptionError(
            "Reading encrypted workflow payloads requires the 'encryption' extra: "
            'pip install "vercel[encryption]"'
        )
    if len(key) != KEY_LENGTH:
        raise ValueError(f"A run key is {KEY_LENGTH} bytes, got {len(key)}")
    if len(payload) < NONCE_LENGTH + TAG_LENGTH:
        raise DecryptionError(
            f"An encrypted payload is at least {NONCE_LENGTH + TAG_LENGTH} bytes, "
            f"got {len(payload)}"
        )

    nonce, sealed = payload[:NONCE_LENGTH], payload[NONCE_LENGTH:]
    plaintext = _AES_GCM_DECRYPT(key, nonce, sealed)
    if plaintext is None:
        raise DecryptionError(
            "authentication failed: the payload was modified, or the run key is wrong"
        )
    return plaintext
