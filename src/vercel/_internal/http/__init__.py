"""Shared HTTP infrastructure for Vercel API clients."""

from vercel._internal.http.config import DEFAULT_API_BASE_URL, DEFAULT_TIMEOUT
from vercel._internal.http.httpx import (
    create_base_async_client,
    create_base_client,
)
from vercel._internal.http.retry import (
    RetryPolicy,
    SleepFn,
)
from vercel._internal.http.transport import (
    AsyncTransport,
    BaseTransport,
    BytesBody,
    JSONBody,
    RawBody,
    ReadResponsePolicy,
    RequestBody,
    SyncTransport,
    TransportOptions,
    extract_structured_error,
)

__all__ = [
    "DEFAULT_API_BASE_URL",
    "DEFAULT_TIMEOUT",
    "BaseTransport",
    "SyncTransport",
    "AsyncTransport",
    "TransportOptions",
    "JSONBody",
    "BytesBody",
    "RawBody",
    "ReadResponsePolicy",
    "RequestBody",
    "RetryPolicy",
    "SleepFn",
    "create_base_client",
    "create_base_async_client",
    "extract_structured_error",
]
