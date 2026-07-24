"""Transitional aliases for HTTPX factories now owned by internal core."""

from vercel.internal.core.http.httpx import create_base_async_client, create_base_client

__all__ = ["create_base_async_client", "create_base_client"]
