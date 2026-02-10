"""Shared HTTP infrastructure for Vercel API clients."""

from .clients import (
    create_base_async_client,
    create_base_client,
    create_headers_async_client,
    create_headers_client,
    create_vercel_async_client,
    create_vercel_client,
)
from .config import DEFAULT_API_BASE_URL, DEFAULT_TIMEOUT
from .transport import (
    AsyncTransport,
    BaseTransport,
    BlockingTransport,
    BytesBody,
    JSONBody,
    RawBody,
    RequestBody,
)

__all__ = [
    "DEFAULT_API_BASE_URL",
    "DEFAULT_TIMEOUT",
    "BaseTransport",
    "BlockingTransport",
    "AsyncTransport",
    "JSONBody",
    "BytesBody",
    "RawBody",
    "RequestBody",
    "create_vercel_client",
    "create_vercel_async_client",
    "create_headers_client",
    "create_headers_async_client",
    "create_base_client",
    "create_base_async_client",
]
