"""Shared HTTP infrastructure for Vercel API clients."""

from vercel.internal.core.http.config import DEFAULT_API_BASE_URL, DEFAULT_TIMEOUT
from vercel.internal.core.http.httpx import (
    create_base_async_client,
    create_base_client,
)
from vercel.internal.core.http.retry import (
    RetryPolicy,
    SleepFn,
)
from vercel.internal.core.http.transport import (
    AsyncTransport,
    BaseTransport,
    BytesBody,
    JSONBody,
    RawBody,
    ReadResponsePolicy,
    RequestBody,
    StreamingRequest,
    StreamingResponse,
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
    "StreamingRequest",
    "StreamingResponse",
    "RetryPolicy",
    "SleepFn",
    "create_base_client",
    "create_base_async_client",
    "extract_structured_error",
]
