"""Transitional aliases for byte streams now owned by internal core."""

from vercel.internal.core.byte_stream import (
    AsyncByteReader,
    AsyncByteSource,
    AsyncByteStreamRuntime,
    BytesLike,
    RawByteSource,
    ReadableByteStream,
    StagingByteFile,
    StagingFileRuntime,
    SyncByteReader,
    SyncByteSource,
    SyncByteStreamRuntime,
)

__all__ = [
    "AsyncByteReader",
    "AsyncByteSource",
    "AsyncByteStreamRuntime",
    "BytesLike",
    "RawByteSource",
    "ReadableByteStream",
    "StagingByteFile",
    "StagingFileRuntime",
    "SyncByteReader",
    "SyncByteSource",
    "SyncByteStreamRuntime",
]
