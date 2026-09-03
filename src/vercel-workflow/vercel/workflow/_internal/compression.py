"""Readers and writers for workflow payload compression envelopes."""

from __future__ import annotations

import gzip
import os
import sys
import zlib

if sys.version_info >= (3, 14):
    from compression import zstd as _zstd
else:
    import zstandard as _zstd

COMPRESSION_MIN_BYTES = 1024
COMPRESSION_MIN_SAVINGS_RATIO = 0.05
ZSTD_LEVEL = 3

GZIP = b"gzip"
ZSTD = b"zstd"


class DecompressionError(Exception):
    """A compressed payload is invalid."""


def maybe_compress(data: bytes, *, enabled: bool) -> bytes:
    """Compress *data* when the target run supports it and doing so pays off."""
    if (
        not enabled
        or len(data) < COMPRESSION_MIN_BYTES
        or os.getenv("WORKFLOW_DISABLE_COMPRESSION") == "1"
    ):
        return data

    if os.getenv("WORKFLOW_COMPRESSION_CODEC") == "gzip":
        prefix = GZIP
        compressed = gzip.compress(data, compresslevel=6, mtime=0)
    else:
        prefix = ZSTD
        compressed = compress_zstd(data)

    wrapped = prefix + compressed
    if len(wrapped) >= len(data) * (1 - COMPRESSION_MIN_SAVINGS_RATIO):
        return data
    return wrapped


def compress_zstd(data: bytes) -> bytes:
    if sys.version_info >= (3, 14):
        return _zstd.compress(data, level=ZSTD_LEVEL)
    return _zstd.ZstdCompressor(level=ZSTD_LEVEL).compress(data)


def decompress_gzip(data: bytes) -> bytes:
    try:
        return gzip.decompress(data)
    except (gzip.BadGzipFile, EOFError, zlib.error) as error:
        raise DecompressionError(f"invalid gzip data: {error}") from error


def decompress_zstd(data: bytes) -> bytes:
    if sys.version_info >= (3, 14):
        try:
            return _zstd.decompress(data)
        except _zstd.ZstdError as error:
            raise DecompressionError(f"invalid zstd data: {error}") from error

    # python-zstandard's one-shot API needs the frame to declare its content
    # size. Node does not promise that, so use its incremental decoder instead.
    try:
        decompressor = _zstd.ZstdDecompressor().decompressobj()
        result = decompressor.decompress(data) + decompressor.flush()
    except _zstd.ZstdError as error:
        raise DecompressionError(f"invalid zstd data: {error}") from error
    if not decompressor.eof:
        raise DecompressionError("invalid zstd data: incomplete frame")
    if decompressor.unused_data:
        raise DecompressionError("invalid zstd data: trailing bytes")
    return result
