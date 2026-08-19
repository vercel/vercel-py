"""The `encr` and `encp` payload formats, as `@workflow/world-vercel` writes them.

Decryption only, for now: writing either is not implemented, so the payloads
this SDK produces are plain `devl`.

Both envelopes are two nested layers, and both descend from the same per-run
key material :func:`derive_run_key` produces::

    encr payload:  [ 'e' 'n' 'c' 'r' ][ nonce (12) ][ ciphertext + tag (16) ]
    encp payload:  [ 'e' 'n' 'c' 'p' ][ ephemeral public key (32) ][ nonce (12) ]
                   [ ciphertext + tag (16) ]
    plaintext:     [ 'd' 'e' 'v' 'l' ][ devalue.stringify output, UTF-8 ]

`encr` is what a run writes for itself, with the symmetric key it already
holds. `encp` is what somebody *outside* the run writes to it — an external
hook resumer, or a child run writing into a forwarded stream — who holds only
the run's published X25519 public key and therefore cannot read anything back.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
from typing import Any, NamedTuple

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.serialization import (
    load_der_private_key,
    load_der_public_key,
)

KEY_LENGTH = 32
"""AES-256, and an X25519 scalar or public key."""

NONCE_LENGTH = 12
TAG_LENGTH = 16

SCALAR_INFO = b"workflow/encp/x25519/v1"
"""HKDF ``info`` deriving a run's X25519 scalar from its key material."""

CONTENT_KEY_INFO = b"workflow/encp/aes256gcm/v1"
"""HKDF ``info`` prefix for a sealed payload's content key; two public keys follow."""


class DecryptionError(Exception):
    """An encrypted payload could not be opened."""


def _aes_gcm_decrypt(key: bytes, nonce: bytes, sealed: bytes) -> bytes | None:
    """Open *sealed* — ciphertext then its 16-byte tag. ``None`` if it fails."""
    try:
        return AESGCM(key).decrypt(nonce, sealed, None)
    except InvalidTag:
        return None


# Make `cryptography` do its internal imports now.
#
# It's not enough to just import `cryptography` at the beginning of this file.
# The *first* cipher call imports two more things, even when both are already
# imported:
#
# - the native extension, on cryptography below 45 or on Python 3.10;
# - `cryptography.exceptions`, on every version, when the call fails and has to
#   raise `InvalidTag`. Importing that module loads the extension as well.
#
# The cipher call may happen inside a sandbox, where imports resolve against a
# private module table. Either import then loads the native extension a second
# time and dies there: `SystemError` on Python 3.10, an aborted process on 3.14.
#
# Enforcing a silently-failing (to go down the `cryptography.exceptions` path)
# cipher call here in the host fixes all issues above.
_aes_gcm_decrypt(bytes(KEY_LENGTH), bytes(NONCE_LENGTH), bytes(TAG_LENGTH))


class RunKeyPair(NamedTuple):
    """A run's X25519 keypair, both halves derived from its key material."""

    scalar: bytes
    """The private scalar. Secret."""

    public_key: bytes
    """The public key a sealing writer needs. Published on the run entity."""


# Why these two exist at all: `cryptography` will take a raw X25519 key via
# `X25519PrivateKey.from_private_bytes` / `X25519PublicKey.from_public_bytes`,
# needing no DER, and we cannot use either. Both import the OpenSSL backend on
# every call to probe for X25519 support, and a sealed payload is opened inside
# the workflow sandbox, where that import fails. The DER loaders are used
# instead -- aliases of the already-imported binding, so calling them imports
# nothing -- which leaves us wrapping raw keys ourselves.
#
# Hence one prefix per direction, DER needing a different header per key type:
# the scalar we hold goes in as `PrivateKeyInfo`, a peer's public key off the
# wire as `SubjectPublicKeyInfo`. Both are fully determined for a 32-byte key,
# so wrapping is a concatenation rather than an encoder.

PKCS8_X25519_PREFIX = bytes.fromhex("302e020100300506032b656e04220420")
"""RFC 8410 ``PrivateKeyInfo`` around a raw 32-byte X25519 scalar::

    SEQUENCE (46)                              30 2e
      INTEGER 0                                02 01 00
      SEQUENCE (5)                             30 05
        OBJECT IDENTIFIER 1.3.101.110          06 03 2b 65 6e
      OCTET STRING (34)                        04 22
        OCTET STRING (32)                      04 20
          <scalar>

`@workflow/core`'s `sealed-box.ts` carries the same 16 bytes for the same
reason: WebCrypto cannot import a raw X25519 private key either.
"""

SPKI_X25519_PREFIX = bytes.fromhex("302a300506032b656e032100")
"""RFC 8410 ``SubjectPublicKeyInfo`` around a raw 32-byte X25519 public key::

    SEQUENCE (42)                              30 2a
      SEQUENCE (5)                             30 05
        OBJECT IDENTIFIER 1.3.101.110          06 03 2b 65 6e
      BIT STRING (33), 0 unused bits           03 21 00
        <public key>

No counterpart in `sealed-box.ts`, unlike the private prefix: WebCrypto
imports a raw X25519 *public* key happily, so TS only ever needs to wrap the
private half.
"""


def _x25519_private_key(scalar: bytes) -> Any:
    return load_der_private_key(PKCS8_X25519_PREFIX + scalar, password=None)


def _x25519_public_key(scalar: bytes) -> bytes:
    """The public half of a private scalar."""
    return _x25519_private_key(scalar).public_key().public_bytes_raw()


def _x25519_exchange(scalar: bytes, peer_public_key: bytes) -> bytes | None:
    """The secret *scalar* shares with *peer_public_key*; ``None`` if it cannot."""
    try:
        peer = load_der_public_key(SPKI_X25519_PREFIX + peer_public_key)
        return _x25519_private_key(scalar).exchange(peer)
    except ValueError:
        return None


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


def derive_run_key_pair(run_key: bytes) -> RunKeyPair:
    """The X25519 keypair a run opens its sealed payloads with.

    Derived from the same 32 bytes :func:`derive_run_key` produces, so a run
    that can decrypt its own `encr` payloads can open a sealed one with no
    further key material — the deployment re-derives the scalar on demand
    instead of anything storing it.

    Any 32 bytes are a valid X25519 scalar, the low and high bits being clamped
    during multiplication, so the HKDF output is used as it comes.
    """
    if len(run_key) != KEY_LENGTH:
        raise ValueError(f"A run key is {KEY_LENGTH} bytes, got {len(run_key)}")
    scalar = hkdf_sha256(ikm=run_key, salt=bytes(32), info=SCALAR_INFO, length=KEY_LENGTH)
    return RunKeyPair(scalar, _x25519_public_key(scalar))


def open_sealed_envelope(run_key: bytes, payload: bytes) -> bytes:
    """Open the X25519 sealed box an `encp` payload carries.

    *payload* is everything after the 4-byte format prefix: the sender's
    ephemeral public key, then the AES-GCM envelope :func:`open_envelope`
    reads. The content key comes from the ECDH shared secret, bound to both
    public keys so that a payload sealed to one run cannot be replayed at
    another.

    No AAD: `@workflow/core` seals with ``sealTo(publicKey)`` and passes none.
    The construction binds the payload to its recipient through the content
    key's ``info`` regardless.
    """
    minimum = KEY_LENGTH + NONCE_LENGTH + TAG_LENGTH
    if len(payload) < minimum:
        raise DecryptionError(f"A sealed payload is at least {minimum} bytes, got {len(payload)}")

    ephemeral_public_key, envelope = payload[:KEY_LENGTH], payload[KEY_LENGTH:]
    scalar, public_key = derive_run_key_pair(run_key)
    shared = _x25519_exchange(scalar, ephemeral_public_key)
    if shared is None:
        raise DecryptionError(
            "the sealed payload's ephemeral public key is not a valid curve point"
        )
    content_key = hkdf_sha256(
        ikm=shared,
        salt=bytes(32),
        info=CONTENT_KEY_INFO + ephemeral_public_key + public_key,
        length=KEY_LENGTH,
    )
    return open_envelope(content_key, envelope)


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
    if len(key) != KEY_LENGTH:
        raise ValueError(f"A run key is {KEY_LENGTH} bytes, got {len(key)}")
    if len(payload) < NONCE_LENGTH + TAG_LENGTH:
        raise DecryptionError(
            f"An encrypted payload is at least {NONCE_LENGTH + TAG_LENGTH} bytes, "
            f"got {len(payload)}"
        )

    nonce, sealed = payload[:NONCE_LENGTH], payload[NONCE_LENGTH:]
    plaintext = _aes_gcm_decrypt(key, nonce, sealed)
    if plaintext is None:
        raise DecryptionError(
            "authentication failed: the payload was modified, or the run key is wrong"
        )
    return plaintext
