from __future__ import annotations

import gzip

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from vercel.workflow._internal import compression, py_sandbox, serialization as ser, world as w

# Produced by Node 24's node:zlib over b'devl["charged 42"]'.
TS_GZIP = bytes.fromhex(
    "1f8b08000000000000134b492dcb89564ace482c4a4f4d513031528a0500fc837fd212000000"
)
# The zstd fixture omits the optional decompressed-size field. This verifies
# that readers do not rely on the frame declaring its output size.
TS_ZSTD_WITHOUT_CONTENT_SIZE = bytes.fromhex(
    "28b52ffd00009100006465766c5b2263686172676564203432225d"
)
COMPRESSING_ENCODER = ser.PayloadEncoder(compression=True)


@pytest.mark.parametrize(
    ("prefix", "payload"),
    [(ser.GZIP, TS_GZIP), (ser.ZSTD, TS_ZSTD_WITHOUT_CONTENT_SIZE)],
)
def test_reads_a_typescript_compression_envelope(prefix: bytes, payload: bytes) -> None:
    assert ser.hydrate(prefix + payload, what="a payload") == "charged 42"


def test_writes_zstd_by_default() -> None:
    value = "charged " * 256

    payload = COMPRESSING_ENCODER.encode(value)

    assert payload.startswith(ser.ZSTD)
    assert ser.hydrate(payload, what="a payload") == value


def test_gzip_can_be_selected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKFLOW_COMPRESSION_CODEC", "gzip")
    value = "charged " * 256

    payload = COMPRESSING_ENCODER.encode(value)

    assert payload.startswith(ser.GZIP)
    assert ser.hydrate(payload, what="a payload") == value


def test_compression_can_be_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKFLOW_DISABLE_COMPRESSION", "1")

    payload = COMPRESSING_ENCODER.encode("charged " * 256)

    assert payload.startswith(ser.DEVALUE_V1)


def test_small_payloads_are_not_compressed() -> None:
    assert compression.maybe_compress(b"x" * 1023, enabled=True) == b"x" * 1023


def test_disabled_compression_leaves_large_payloads_alone() -> None:
    data = b"x" * 2000

    assert compression.maybe_compress(data, enabled=False) == data


@pytest.mark.parametrize(
    ("compressed_size", "prefix"),
    [(1896, b"x"), (1895, ser.ZSTD)],
)
def test_compression_requires_five_percent_savings(
    monkeypatch: pytest.MonkeyPatch, compressed_size: int, prefix: bytes
) -> None:
    monkeypatch.setattr(compression, "compress_zstd", lambda data: b"x" * compressed_size)

    assert compression.maybe_compress(b"x" * 2000, enabled=True).startswith(prefix)


def test_new_runs_advertise_compression_support() -> None:
    assert w.SPEC_VERSION_CURRENT == w.SPEC_VERSION_SUPPORTS_COMPRESSION


def test_compression_works_inside_the_workflow_sandbox() -> None:
    value = "charged " * 256

    with py_sandbox.Sandbox().enter():
        payload = COMPRESSING_ENCODER.encode(value)

    assert ser.hydrate(payload, what="a payload") == value


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
