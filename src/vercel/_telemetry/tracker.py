"""Telemetry tracking helpers for SDK operations."""

from __future__ import annotations

import functools
import inspect
import os
import threading
from typing import TYPE_CHECKING, Any, Callable, Literal, Mapping, Optional, Sequence, TypeVar

if TYPE_CHECKING:
    from .client import TelemetryClient

# Singleton telemetry client instance with thread-safe initialization
_telemetry_client = None
_telemetry_client_lock = threading.Lock()

T = TypeVar("T", bound=Callable[..., Any])


def get_client() -> Optional[TelemetryClient]:
    """Get or create the telemetry client singleton (thread-safe).
    
    Returns:
        TelemetryClient instance, or None if initialization fails.
    """
    global _telemetry_client
    # Fast path without lock
    client = _telemetry_client
    if client is not None:
        return client
    # Slow path with double-checked locking
    with _telemetry_client_lock:
        client = _telemetry_client
        if client is None:
            try:
                from .client import TelemetryClient
                _telemetry_client = TelemetryClient()
            except Exception:
                _telemetry_client = None
        return _telemetry_client


def track(event: str, **attrs: Any) -> None:
    """Track a telemetry event.
    
    This is the main entry point for tracking telemetry events.
    All attributes are passed through to the client's track method,
    which handles credential extraction and field whitelisting.
    
    Args:
        event: The event/action being tracked (e.g., 'blob_put', 'cache_get')
        **attrs: Additional event attributes (e.g., user_id, team_id, token, etc.)
    """
    client = get_client()
    if client is None:
        return
    try:
        client.track(event, **attrs)
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


def telemetry(
    event: str,
    capture: Sequence[str] | None = None,
    derive: Mapping[str, Callable[[tuple, dict, Any], Any]] | None = None,
    when: Literal["before", "after"] = "after",
) -> Callable[[T], T]:
    """Decorator to emit telemetry around a function call.

    Args:
        event: The event name to track
        capture: List of parameter names to capture from args/kwargs.
            Names are resolved against the function signature so positional calls
            are handled correctly.
        derive: Mapping of output field -> lambda(args, kwargs, result).
            The callable receives (args, kwargs, result) and should return
            the value for that field.
        when: Emit "before" the call, or "after" the call (default: "after").

    Returns:
        Decorator function

    Example:
        @telemetry(
            event="blob_delete",
            capture=["token"],
            derive={"count": lambda args, kwargs, rv: len(kwargs.get("urls", []))},
            when="after",
        )
        def delete(urls: list[str], *, token: str | None = None) -> None:
            ...
    """
    def decorator(func: T) -> T:
        is_coro = inspect.iscoroutinefunction(func)
        sig = inspect.signature(func)

        def _emit(ev: str, args: tuple, kwargs: dict, result: Any) -> None:
            try:
                attrs: dict[str, Any] = {}
                # Bind parameters for positional resolution
                try:
                    bound = sig.bind_partial(*args, **kwargs)
                    params: dict[str, Any] = dict(bound.arguments)  # name -> value
                except Exception:
                    params = {}

                # Capture selected params by name
                if capture:
                    for name in capture:
                        if name in kwargs:
                            attrs[name] = kwargs[name]
                        elif name in params:
                            attrs[name] = params[name]
                        # else: silently skip if not provided

                # Derived attributes
                if derive:
                    for field, getter in derive.items():
                        try:
                            attrs[field] = getter(args, kwargs, result)
                        except Exception:
                            # ignore individual derivation errors
                            pass

                track(ev, **attrs)
            except Exception:
                # Silently fail - don't impact user's operation
                pass

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            if when == "before":
                _emit(event, args, kwargs, None)
            result = await func(*args, **kwargs)
            if when == "after":
                _emit(event, args, kwargs, result)
            return result

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            if when == "before":
                _emit(event, args, kwargs, None)
            result = func(*args, **kwargs)
            if when == "after":
                _emit(event, args, kwargs, result)
            return result

        return async_wrapper if is_coro else sync_wrapper  # type: ignore

    return decorator


# Specific wrapper functions are intentionally removed;
# use generic `track(event, **attrs)` or the `telemetry` decorator instead.
