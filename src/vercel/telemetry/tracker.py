"""Telemetry tracking helpers for SDK operations."""

import functools
import os
import threading
from typing import Any, Callable, Optional, TypeVar

# Singleton telemetry client instance with thread-safe initialization
_telemetry_client = None
_telemetry_client_lock = threading.Lock()

T = TypeVar("T", bound=Callable[..., Any])


def _get_telemetry_client():
    """Get or create the telemetry client singleton (thread-safe)."""
    global _telemetry_client
    # Fast path without lock
    client = _telemetry_client
    if client is not None:
        return client
    # Slow path with double-checked locking
    try:
        from .client import TelemetryClient
    except Exception:
        return None
    with _telemetry_client_lock:
        client = _telemetry_client
        if client is None:
            try:
                _telemetry_client = TelemetryClient()
            except Exception:
                _telemetry_client = None
        return _telemetry_client


def track(
    action: str,
    *,
    user_id: Optional[str] = None,
    team_id: Optional[str] = None,
    project_id: Optional[str] = None,
    token: Optional[str] = None,
    **fields: Any,
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
        **fields: Additional event fields (whitelisted by schema)
    """
    try:
        client = _get_telemetry_client()
        if client is None:
            return
        client.track(
            action,
            user_id=user_id,
            team_id=team_id,
            project_id=project_id,
            token=token,
            **fields,
        )
    except Exception:
        # Silently fail - don't impact user's operation
        pass


def with_telemetry(
    action: str,
    extract_metadata: Optional[Callable[..., dict[str, Any]]] = None,
    extract_token: Optional[Callable[..., Optional[str]]] = None,
    extract_team_id: Optional[Callable[..., Optional[str]]] = None,
    extract_project_id: Optional[Callable[..., Optional[str]]] = None,
) -> Callable[[T], T]:
    """
    Create a decorator that automatically tracks telemetry for a function.
    
    Usage:
        @with_telemetry(
            action="blob_put",
            extract_metadata=lambda self, path, size=None: {"size": size}
        )
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
                **(metadata or {}),
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
                **(metadata or {}),
            )
            
            return result
        
        # Return appropriate wrapper based on whether function is async
        import inspect
        if inspect.iscoroutinefunction(func):
            return async_wrapper  # type: ignore
        else:
            return sync_wrapper  # type: ignore
    
    return decorator


# Specific wrapper functions are intentionally removed;
# use generic `track(action, **fields)` instead.
