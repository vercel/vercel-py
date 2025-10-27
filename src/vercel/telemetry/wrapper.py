"""Wrapper functions to add telemetry tracking to SDK operations."""

import os
from typing import Any

# Try to import telemetry, but don't fail if not available
_telemetry_client = None
try:
    from .client import TelemetryClient
    _telemetry_client = TelemetryClient()
except Exception:
    pass


def _get_size_bytes(body: Any) -> int | None:
    """Get size in bytes of a body object."""
    try:
        if hasattr(body, 'seek') and hasattr(body, 'tell'):
            pos = body.tell()
            body.seek(0, 2)  # Seek to end
            size = body.tell()
            body.seek(pos)  # Reset to original position
            return size
        if isinstance(body, (bytes, bytearray)):
            return len(body)
        if isinstance(body, str):
            return len(body.encode())
    except Exception:
        pass
    return None


def track_blob_put(
    access: str = "public",
    content_type: str | None = None,
    size_bytes: int | None = None,
    multipart: bool = False,
    **kwargs: Any,
) -> None:
    """Track a blob PUT operation."""
    if _telemetry_client is None:
        return
    try:
        if size_bytes is None and kwargs.get('body') is not None:
            size_bytes = _get_size_bytes(kwargs['body'])
        
        _telemetry_client.track_blob_put(
            access=access,
            content_type=content_type,
            size_bytes=size_bytes,
            multipart=multipart,
        )
    except Exception:
        # Silently fail - don't impact user's operation
        pass


def track_blob_delete(count: int = 1) -> None:
    """Track a blob DELETE operation."""
    if _telemetry_client is None:
        return
    try:
        _telemetry_client.track_blob_delete(count=count)
    except Exception:
        # Silently fail - don't impact user's operation
        pass


def track_cache_set(ttl_seconds: int | None = None, has_tags: bool = False, **kwargs: Any) -> None:
    """Track a cache SET operation."""
    if _telemetry_client is None:
        return
    try:
        _telemetry_client.track_cache_set(ttl_seconds=ttl_seconds, has_tags=has_tags)
    except Exception:
        # Silently fail - don't impact user's operation
        pass


def track_cache_get(hit: bool, **kwargs: Any) -> None:
    """Track a cache GET operation."""
    if _telemetry_client is None:
        return
    try:
        _telemetry_client.track_cache_get(hit=hit)
    except Exception:
        # Silently fail - don't impact user's operation
        pass


def flush_telemetry() -> None:
    """Flush any pending telemetry events (fire and forget)."""
    if _telemetry_client is None:
        return
    try:
        # This should be done in a background task, but for now just call it
        import asyncio
        asyncio.run(_telemetry_client.flush())
    except Exception:
        # Silently fail
        pass

