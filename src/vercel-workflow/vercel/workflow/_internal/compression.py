"""Readers for workflow payload compression envelopes."""

from __future__ import annotations

import gzip
import sys
import zlib

if sys.version_info >= (3, 14):
    from compression import zstd as _zstd
else:
    import zstandard as _zstd


class DecompressionError(Exception):
    """A compressed payload is invalid."""


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
