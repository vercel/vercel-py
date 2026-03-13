"""Top-level exports for the clean-room stable Vercel client surface."""

from vercel.stable.client import AsyncVercel, SyncVercel, create_async_client, create_sync_client

__all__ = ["create_sync_client", "create_async_client", "SyncVercel", "AsyncVercel"]
