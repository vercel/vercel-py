"""Shared HTTP infrastructure for Vercel API clients."""

from .config import DEFAULT_API_BASE_URL, DEFAULT_TIMEOUT, HTTPConfig, require_token
from .iter_coroutine import iter_coroutine
from .transport import (
    AsyncTransport,
    BaseTransport,
    BlockingTransport,
    BytesBody,
    JSONBody,
    RequestBody,
)

__all__ = [
    "HTTPConfig",
    "DEFAULT_API_BASE_URL",
    "DEFAULT_TIMEOUT",
    "require_token",
    "iter_coroutine",
    "BaseTransport",
    "BlockingTransport",
    "AsyncTransport",
    "JSONBody",
    "BytesBody",
    "RequestBody",
]
