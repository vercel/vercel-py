"""Telemetry tracking helpers for SDK operations."""

import os
from typing import Any


# Singleton telemetry client instance
_telemetry_client = None


def _get_telemetry_client():
    """Get or create the telemetry client singleton."""
    global _telemetry_client
    if _telemetry_client is None:
        try:
            from .client import TelemetryClient
            _telemetry_client = TelemetryClient()
        except Exception:
            pass
    return _telemetry_client


def _track_safely(func_name: str, **kwargs: Any) -> None:
    """Safely call a tracking function with try/catch."""
    try:
        client = _get_telemetry_client()
        if client is None:
            return
        func = getattr(client, func_name, None)
        if func:
            func(**kwargs)
    except Exception:
        # Silently fail - don't impact user's operation
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
    body: Any = None,
    **kwargs: Any,
) -> None:
    """Track a blob PUT operation."""
    try:
        client = _get_telemetry_client()
        if client is None:
            return

        if size_bytes is None and body is not None:
            size_bytes = _get_size_bytes(body)

        client.track_blob_put(
            access=access,
            content_type=content_type,
            size_bytes=size_bytes,
            multipart=multipart,
        )
    except Exception:
        # Silently fail - don't impact user's operation
        pass


def track_blob_delete(count: int = 1, **kwargs: Any) -> None:
    """Track a blob DELETE operation."""
    try:
        client = _get_telemetry_client()
        if client is None:
            return
        client.track_blob_delete(count=count)
    except Exception:
        # Silently fail - don't impact user's operation
        pass


def track_cache_set(ttl_seconds: int | None = None, has_tags: bool = False, **kwargs: Any) -> None:
    """Track a cache SET operation."""
    try:
        client = _get_telemetry_client()
        if client is None:
            return
        client.track_cache_set(ttl_seconds=ttl_seconds, has_tags=has_tags)
    except Exception:
        # Silently fail - don't impact user's operation
        pass


def track_cache_get(hit: bool, **kwargs: Any) -> None:
    """Track a cache GET operation."""
    _track_safely("track_cache_get", hit=hit)


def track_project_create(**kwargs: Any) -> None:
    """Track a project CREATE operation."""
    _track_safely("track_project_create")


def track_project_update(**kwargs: Any) -> None:
    """Track a project UPDATE operation."""
    _track_safely("track_project_update")


def track_project_delete(**kwargs: Any) -> None:
    """Track a project DELETE operation."""
    _track_safely("track_project_delete")


def track_deployment_create(**kwargs: Any) -> None:
    """Track a deployment CREATE operation."""
    _track_safely("track_deployment_create")

