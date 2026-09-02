from __future__ import annotations

import gzip

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from vercel.workflow._internal import serialization as ser

# Produced by Node 24's node:zlib over b'devl["charged 42"]'.
TS_GZIP = bytes.fromhex(
    "1f8b08000000000000134b492dcb89564ace482c4a4f4d513031528a0500fc837fd212000000"
)
# The zstd fixture omits the optional decompressed-size field. This verifies
# that readers do not rely on the frame declaring its output size.
TS_ZSTD_WITHOUT_CONTENT_SIZE = bytes.fromhex(
    "28b52ffd00009100006465766c5b2263686172676564203432225d"
)


@pytest.mark.parametrize(
    ("prefix", "payload"),
    [(ser.GZIP, TS_GZIP), (ser.ZSTD, TS_ZSTD_WITHOUT_CONTENT_SIZE)],
)
def test_reads_a_typescript_compression_envelope(prefix: bytes, payload: bytes) -> None:
    assert ser.hydrate(prefix + payload, what="a payload") == "charged 42"


def test_nested_compression_is_not_an_extra_protocol() -> None:
    zstd = ser.ZSTD + TS_ZSTD_WITHOUT_CONTENT_SIZE
    payload = ser.GZIP + gzip.compress(zstd, mtime=0)

    with pytest.raises(ser.SerializationError, match="unknown serialization format"):
        ser.hydrate(payload, what="a payload")


def test_compression_under_encryption_composes() -> None:
    key = bytes(range(32))
    nonce = bytes(range(12))
    compressed = ser.ZSTD + TS_ZSTD_WITHOUT_CONTENT_SIZE
    payload = ser.ENCRYPTED + nonce + AESGCM(key).encrypt(nonce, compressed, None)

    assert ser.hydrate(payload, what="a payload", key=key) == "charged 42"


@pytest.mark.parametrize("prefix", [ser.GZIP, ser.ZSTD])
def test_corrupt_compressed_payload_names_the_field(prefix: bytes) -> None:
    with pytest.raises(ser.SerializationError, match="Cannot decompress the input of run wrun_1"):
        ser.hydrate(prefix + b"not compressed", what="the input of run wrun_1")


@pytest.mark.parametrize(
    "payload",
    [
        ser.GZIP + TS_GZIP[:-1],
        ser.ZSTD + TS_ZSTD_WITHOUT_CONTENT_SIZE[:-1],
    ],
)
def test_truncated_compressed_payload_is_rejected(payload: bytes) -> None:
    with pytest.raises(ser.SerializationError, match="Cannot decompress a payload"):
        ser.hydrate(payload, what="a payload")
