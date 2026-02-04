"""Core business logic for Vercel Build Cache."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from .._http import (
    AsyncTransport,
    BaseTransport,
    BlockingTransport,
    JSONBody,
    create_headers_async_client,
    create_headers_client,
)
from .._telemetry.tracker import track
from .types import AsyncCache, Cache

HEADERS_VERCEL_CACHE_STATE = "x-vercel-cache-state"
HEADERS_VERCEL_REVALIDATE = "x-vercel-revalidate"
HEADERS_VERCEL_CACHE_TAGS = "x-vercel-cache-tags"
HEADERS_VERCEL_CACHE_ITEM_NAME = "x-vercel-cache-item-name"

DEFAULT_TIMEOUT = 30.0


class _BaseBuildCache:
    """Base class for Build Cache with shared async implementation."""

    _transport: BaseTransport
    _endpoint: str
    _on_error: Callable[[Exception], None] | None

    async def _get(self, key: str) -> Any:
        try:
            resp = await self._transport.send("GET", key)
            if resp.status_code == 404:
                track("cache_get", hit=False)
                return None
            if resp.status_code == 200:
                cache_state = resp.headers.get(HEADERS_VERCEL_CACHE_STATE)
                if cache_state and cache_state.lower() != "fresh":
                    track("cache_get", hit=False)
                    return None
                track("cache_get", hit=True)
                return resp.json()
            raise RuntimeError(f"Failed to get cache: {resp.status_code} {resp.reason_phrase}")
        except Exception as e:
            if self._on_error:
                self._on_error(e)
            return None

    async def _set(
        self,
        key: str,
        value: object,
        options: dict | None = None,
    ) -> None:
        try:
            optional_headers: dict[str, str] = {}
            if options and (ttl := options.get("ttl")):
                optional_headers[HEADERS_VERCEL_REVALIDATE] = str(ttl)
            if options and (tags := options.get("tags")):
                if tags:
                    optional_headers[HEADERS_VERCEL_CACHE_TAGS] = ",".join(tags)
            if options and (name := options.get("name")):
                optional_headers[HEADERS_VERCEL_CACHE_ITEM_NAME] = name

            resp = await self._transport.send(
                "POST",
                key,
                headers=optional_headers if optional_headers else None,
                body=JSONBody(value),
            )
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to set cache: {resp.status_code} {resp.reason_phrase}")
            track(
                "cache_set",
                ttl_seconds=options.get("ttl") if options else None,
                has_tags=bool(options and options.get("tags")),
            )
        except Exception as e:
            if self._on_error:
                self._on_error(e)

    async def _delete(self, key: str) -> None:
        try:
            resp = await self._transport.send("DELETE", key)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Failed to delete cache: {resp.status_code} {resp.reason_phrase}"
                )
        except Exception as e:
            if self._on_error:
                self._on_error(e)

    async def _expire_tag(self, tag: str | Sequence[str]) -> None:
        try:
            tags = ",".join(tag) if isinstance(tag, (list, tuple, set)) else tag
            resp = await self._transport.send("POST", "revalidate", params={"tags": tags})
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Failed to revalidate tag: {resp.status_code} {resp.reason_phrase}"
                )
        except Exception as e:
            if self._on_error:
                self._on_error(e)

    async def _contains(self, key: str) -> bool:
        try:
            resp = await self._transport.send("GET", key)
            if resp.status_code == 404:
                return False
            if resp.status_code == 200:
                cache_state = resp.headers.get(HEADERS_VERCEL_CACHE_STATE)
                if cache_state and cache_state.lower() != "fresh":
                    return False
                return True
            return False
        except Exception as e:
            if self._on_error:
                self._on_error(e)
            return False


class SyncBuildCache(_BaseBuildCache, Cache):
    def __init__(
        self,
        *,
        endpoint: str,
        headers: Mapping[str, str],
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self._endpoint = endpoint
        self._on_error = on_error
        client = create_headers_client(headers, timeout=DEFAULT_TIMEOUT, base_url=endpoint)
        self._transport = BlockingTransport(client)

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> SyncBuildCache:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def get(self, key: str) -> Any:
        from .._http import iter_coroutine

        return iter_coroutine(self._get(key))

    def set(
        self,
        key: str,
        value: object,
        options: dict | None = None,
    ) -> None:
        from .._http import iter_coroutine

        iter_coroutine(self._set(key, value, options))

    def delete(self, key: str) -> None:
        from .._http import iter_coroutine

        iter_coroutine(self._delete(key))

    def expire_tag(self, tag: str | Sequence[str]) -> None:
        from .._http import iter_coroutine

        iter_coroutine(self._expire_tag(tag))

    def __contains__(self, key: str) -> bool:
        from .._http import iter_coroutine

        return iter_coroutine(self._contains(key))

    def __getitem__(self, key: str) -> Any:
        if key in self:
            return self.get(key)
        raise KeyError(key)


class AsyncBuildCache(_BaseBuildCache, AsyncCache):
    def __init__(
        self,
        *,
        endpoint: str,
        headers: Mapping[str, str],
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self._endpoint = endpoint
        self._on_error = on_error
        client = create_headers_async_client(headers, timeout=DEFAULT_TIMEOUT, base_url=endpoint)
        self._transport = AsyncTransport(client)

    async def aclose(self) -> None:
        await self._transport.aclose()

    async def __aenter__(self) -> AsyncBuildCache:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def get(self, key: str) -> Any:
        return await self._get(key)

    async def set(
        self,
        key: str,
        value: object,
        options: dict | None = None,
    ) -> None:
        await self._set(key, value, options)

    async def delete(self, key: str) -> None:
        await self._delete(key)

    async def expire_tag(self, tag: str | Sequence[str]) -> None:
        await self._expire_tag(tag)

    async def contains(self, key: str) -> bool:
        return await self._contains(key)


__all__ = [
    "SyncBuildCache",
    "AsyncBuildCache",
    "HEADERS_VERCEL_CACHE_STATE",
    "HEADERS_VERCEL_REVALIDATE",
    "HEADERS_VERCEL_CACHE_TAGS",
    "HEADERS_VERCEL_CACHE_ITEM_NAME",
    "DEFAULT_TIMEOUT",
]
