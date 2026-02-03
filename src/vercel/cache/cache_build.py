"""Build cache implementation using shared _core module."""

from __future__ import annotations

from ._core import (
    HEADERS_VERCEL_CACHE_ITEM_NAME,
    HEADERS_VERCEL_CACHE_STATE,
    HEADERS_VERCEL_CACHE_TAGS,
    HEADERS_VERCEL_REVALIDATE,
    AsyncBuildCache,
    SyncBuildCache,
)

# Re-export with backwards-compatible names
BuildCache = SyncBuildCache

__all__ = [
    "BuildCache",
    "AsyncBuildCache",
    "HEADERS_VERCEL_CACHE_STATE",
    "HEADERS_VERCEL_REVALIDATE",
    "HEADERS_VERCEL_CACHE_TAGS",
    "HEADERS_VERCEL_CACHE_ITEM_NAME",
]
