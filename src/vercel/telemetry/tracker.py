"""Telemetry tracking helpers for SDK operations."""

import functools
import os
from typing import Any, Callable, Optional, TypeVar

# Singleton telemetry client instance
_telemetry_client = None

T = TypeVar("T", bound=Callable[..., Any])


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


def track(
    action: str,
    *,
    user_id: Optional[str] = None,
    team_id: Optional[str] = None,
    project_id: Optional[str] = None,
    token: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> None:
    """
    Track a telemetry event using the generic track method.
    
    This is the main entry point for tracking telemetry events.
    It automatically extracts credentials from environment/tokens if not provided.
    
    Args:
        action: The action being tracked (e.g., 'blob_put', 'cache_get')
        user_id: Optional user ID
        team_id: Optional team ID  
        project_id: Optional project ID
        token: Optional token to extract credentials from
        metadata: Optional metadata dictionary
    """
    try:
        client = _get_telemetry_client()
        if client is None:
            return
        # Expand metadata keys as direct fields if provided (and safe)
        client.track(
            action,
            user_id=user_id,
            team_id=team_id,
            project_id=project_id,
            token=token,
            **(metadata or {}),
        )
    except Exception:
        # Silently fail - don't impact user's operation
        pass


def with_telemetry(
    action: str,
    extract_metadata: Optional[Callable[[Any, tuple, dict], dict[str, Any]]] = None,
    extract_token: Optional[Callable[[Any, tuple, dict], Optional[str]]] = None,
    extract_team_id: Optional[Callable[[Any, tuple, dict], Optional[str]]] = None,
    extract_project_id: Optional[Callable[[Any, tuple, dict], Optional[str]]] = None,
) -> Callable[[T], T]:
    """
    Create a decorator that automatically tracks telemetry for a function.
    
    Usage:
        @with_telemetry(action="blob_put", extract_metadata=lambda self, args, kwargs: {"size": kwargs.get("size")})
        def put(self, path, size=None):
            ...
    
    Args:
        action: The action name to track
        extract_metadata: Optional function to extract metadata from function call
        extract_token: Optional function to extract token from function call
        extract_team_id: Optional function to extract team_id from function call
        extract_project_id: Optional function to extract project_id from function call
    
    Returns:
        Decorator function
    """
    def decorator(func: T) -> T:
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            # Execute the original function
            result = func(*args, **kwargs)
            
            # Extract metadata and credentials
            metadata = None
            token = None
            team_id = None
            project_id = None
            
            if extract_metadata:
                try:
                    metadata = extract_metadata(*args, **kwargs)
                except Exception:
                    pass
            
            if extract_token:
                try:
                    token = extract_token(*args, **kwargs)
                except Exception:
                    pass
            
            if extract_team_id:
                try:
                    team_id = extract_team_id(*args, **kwargs)
                except Exception:
                    pass
            
            if extract_project_id:
                try:
                    project_id = extract_project_id(*args, **kwargs)
                except Exception:
                    pass
            
            # Track the event
            track(
                action,
                token=token,
                team_id=team_id,
                project_id=project_id,
                metadata=metadata,
            )
            
            return result
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Execute the original function
            result = await func(*args, **kwargs)
            
            # Extract metadata and credentials (same as sync)
            metadata = None
            token = None
            team_id = None
            project_id = None
            
            if extract_metadata:
                try:
                    metadata = extract_metadata(*args, **kwargs)
                except Exception:
                    pass
            
            if extract_token:
                try:
                    token = extract_token(*args, **kwargs)
                except Exception:
                    pass
            
            if extract_team_id:
                try:
                    team_id = extract_team_id(*args, **kwargs)
                except Exception:
                    pass
            
            if extract_project_id:
                try:
                    project_id = extract_project_id(*args, **kwargs)
                except Exception:
                    pass
            
            # Track the event
            track(
                action,
                token=token,
                team_id=team_id,
                project_id=project_id,
                metadata=metadata,
            )
            
            return result
        
        # Return appropriate wrapper based on whether function is async
        import inspect
        if inspect.iscoroutinefunction(func):
            return async_wrapper  # type: ignore
        else:
            return sync_wrapper  # type: ignore
    
    return decorator


# Backwards-compatible specific wrappers used by SDK call sites

def track_blob_put(*, access: str = "public", content_type: Optional[str] = None, size_bytes: Optional[int] = None, multipart: bool = False, user_id: Optional[str] = None, team_id: Optional[str] = None, project_id: Optional[str] = None, token: Optional[str] = None) -> None:
    track(
        "blob_put",
        user_id=user_id,
        team_id=team_id,
        project_id=project_id,
        token=token,
        metadata={"access": access, "content_type": content_type, "size_bytes": size_bytes, "multipart": multipart},
    )


def track_blob_delete(*, count: int, user_id: Optional[str] = None, team_id: Optional[str] = None, project_id: Optional[str] = None, token: Optional[str] = None) -> None:
    track(
        "blob_delete",
        user_id=user_id,
        team_id=team_id,
        project_id=project_id,
        token=token,
        metadata={"count": count},
    )


def track_cache_set(*, ttl_seconds: Optional[int] = None, has_tags: bool = False, user_id: Optional[str] = None, team_id: Optional[str] = None, project_id: Optional[str] = None, token: Optional[str] = None) -> None:
    track(
        "cache_set",
        user_id=user_id,
        team_id=team_id,
        project_id=project_id,
        token=token,
        metadata={"ttl_seconds": ttl_seconds, "has_tags": has_tags},
    )


def track_cache_get(*, hit: bool, user_id: Optional[str] = None, team_id: Optional[str] = None, project_id: Optional[str] = None, token: Optional[str] = None) -> None:
    track(
        "cache_get",
        user_id=user_id,
        team_id=team_id,
        project_id=project_id,
        token=token,
        metadata={"hit": hit},
    )


def track_project_create(*, user_id: Optional[str] = None, team_id: Optional[str] = None, project_id: Optional[str] = None, token: Optional[str] = None) -> None:
    track("project_create", user_id=user_id, team_id=team_id, project_id=project_id, token=token)


def track_project_update(*, user_id: Optional[str] = None, team_id: Optional[str] = None, project_id: Optional[str] = None, token: Optional[str] = None) -> None:
    track("project_update", user_id=user_id, team_id=team_id, project_id=project_id, token=token)


def track_project_delete(*, user_id: Optional[str] = None, team_id: Optional[str] = None, project_id: Optional[str] = None, token: Optional[str] = None) -> None:
    track("project_delete", user_id=user_id, team_id=team_id, project_id=project_id, token=token)


def track_deployment_create(*, target: Optional[str] = None, force_new: bool = False, user_id: Optional[str] = None, team_id: Optional[str] = None, project_id: Optional[str] = None, token: Optional[str] = None) -> None:
    track(
        "deployment_create",
        user_id=user_id,
        team_id=team_id,
        project_id=project_id,
        token=token,
        metadata={"target": target, "force_new": force_new},
    )
