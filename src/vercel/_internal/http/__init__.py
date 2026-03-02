"""Shared HTTP infrastructure for Vercel API clients."""

from vercel._internal.http.clients import (
    create_async_request_client,
    create_base_async_client,
    create_base_client,
    create_request_client,
)
from vercel._internal.http.config import DEFAULT_API_BASE_URL, DEFAULT_TIMEOUT
from vercel._internal.http.request_client import (
    RequestClient,
    RetryPolicy,
    SleepFn,
    sync_sleep,
)
from vercel._internal.http.transport import (
    AsyncTransport,
    BaseTransport,
    BytesBody,
    JSONBody,
    RawBody,
    RequestBody,
    SyncTransport,
)

__all__ = [
    "DEFAULT_API_BASE_URL",
    "DEFAULT_TIMEOUT",
    "BaseTransport",
    "SyncTransport",
    "AsyncTransport",
    "JSONBody",
    "BytesBody",
    "RawBody",
    "RequestBody",
    "RequestClient",
    "RetryPolicy",
    "SleepFn",
    "sync_sleep",
    "create_base_client",
    "create_base_async_client",
    "create_request_client",
    "create_async_request_client",
]
