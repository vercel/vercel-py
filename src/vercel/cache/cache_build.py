from __future__ import annotations

from typing import Sequence, Mapping, Callable
import httpx
import json
from .types import Cache, AsyncCache


HEADERS_VERCEL_CACHE_STATE = "x-vercel-cache-state"
HEADERS_VERCEL_REVALIDATE = "x-vercel-revalidate"
HEADERS_VERCEL_CACHE_TAGS = "x-vercel-cache-tags"
HEADERS_VERCEL_CACHE_ITEM_NAME = "x-vercel-cache-item-name"

# Use no keep-alive for async clients to avoid lingering background tasks
ASYNC_CLIENT_LIMITS = httpx.Limits(max_keepalive_connections=0)
DEFAULT_TIMEOUT = 30.0


class BuildCache(Cache):
    def __init__(
        self,
        *,
        endpoint: str,
        headers: Mapping[str, str],
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self._endpoint = endpoint.rstrip("/") + "/"
        self._headers = dict(headers)
        self._on_error = on_error
        self._client = httpx.Client(timeout=httpx.Timeout(30.0))

    def get(self, key: str):
        try:
            r = self._client.get(self._endpoint + key, headers=self._headers)
            if r.status_code == 404:
                return None
            if r.status_code == 200:
                cache_state = r.headers.get(HEADERS_VERCEL_CACHE_STATE)
                if cache_state and cache_state.lower() != "fresh":
                    r.close()
                    return None
                return r.json()
            raise RuntimeError(f"Failed to get cache: {r.status_code} {r.reason_phrase}")
        except Exception as e:
            if self._on_error:
                self._on_error(e)
            return None

    def set(
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

            r = self._client.post(
                self._endpoint + key,
                headers={**self._headers, **optional_headers},
                content=json.dumps(value),
            )
            if r.status_code != 200:
                raise RuntimeError(f"Failed to set cache: {r.status_code} {r.reason_phrase}")
        except Exception as e:
            if self._on_error:
                self._on_error(e)

    def delete(self, key: str) -> None:
        try:
            r = self._client.delete(self._endpoint + key, headers=self._headers)
            if r.status_code != 200:
                raise RuntimeError(f"Failed to delete cache: {r.status_code} {r.reason_phrase}")
        except Exception as e:
            if self._on_error:
                self._on_error(e)

    def expire_tag(self, tag: str | Sequence[str]) -> None:
        try:
            tags = ",".join(tag) if isinstance(tag, (list, tuple, set)) else tag
            r = self._client.post(
                f"{self._endpoint}revalidate",
                params={"tags": tags},
                headers=self._headers,
            )
            if r.status_code != 200:
                raise RuntimeError(f"Failed to revalidate tag: {r.status_code} {r.reason_phrase}")
        except Exception as e:
            if self._on_error:
                self._on_error(e)

    def __contains__(self, key: str) -> bool:
        try:
            r = self._client.get(self._endpoint + key, headers=self._headers)
            try:
                if r.status_code == 404:
                    return False
                if r.status_code == 200:
                    cache_state = r.headers.get(HEADERS_VERCEL_CACHE_STATE)
                    # Consider present only when fresh
                    if cache_state and cache_state.lower() != "fresh":
                        return False
                    return True
                return False
            finally:
                # Ensure the response is closed regardless of outcome
                r.close()
        except Exception as e:
            if self._on_error:
                self._on_error(e)
            return False

    def __getitem__(self, key: str):
        if key in self:
            return self.get(key)
        raise KeyError(key)


class AsyncBuildCache(AsyncCache):
    def __init__(
        self,
        *,
        endpoint: str,
        headers: Mapping[str, str],
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self._endpoint = endpoint.rstrip("/") + "/"
        self._headers = dict(headers)
        self._on_error = on_error

    async def get(self, key: str):
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(DEFAULT_TIMEOUT), limits=ASYNC_CLIENT_LIMITS
            ) as client:
                r = await client.get(self._endpoint + key, headers=self._headers)
                if r.status_code == 404:
                    await r.aclose()
                    return None
                if r.status_code == 200:
                    cache_state = r.headers.get(HEADERS_VERCEL_CACHE_STATE)
                    if cache_state and cache_state.lower() != "fresh":
                        await r.aclose()
                        return None
                    data = r.json()
                    await r.aclose()
                    return data
                await r.aclose()
                raise RuntimeError(f"Failed to get cache: {r.status_code} {r.reason_phrase}")
        except Exception as e:
            if self._on_error:
                self._on_error(e)
            return None

    async def set(
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

            async with httpx.AsyncClient(
                timeout=httpx.Timeout(DEFAULT_TIMEOUT), limits=ASYNC_CLIENT_LIMITS
            ) as client:
                r = await client.post(
                    self._endpoint + key,
                    headers={**self._headers, **optional_headers},
                    content=json.dumps(value),
                )
                if r.status_code != 200:
                    await r.aclose()
                    raise RuntimeError(f"Failed to set cache: {r.status_code} {r.reason_phrase}")
                await r.aclose()
        except Exception as e:
            if self._on_error:
                self._on_error(e)

    async def delete(self, key: str) -> None:
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(DEFAULT_TIMEOUT), limits=ASYNC_CLIENT_LIMITS
            ) as client:
                r = await client.delete(self._endpoint + key, headers=self._headers)
                if r.status_code != 200:
                    await r.aclose()
                    raise RuntimeError(f"Failed to delete cache: {r.status_code} {r.reason_phrase}")
                await r.aclose()
        except Exception as e:
            if self._on_error:
                self._on_error(e)

    async def expire_tag(self, tag: str | Sequence[str]) -> None:
        try:
            tags = ",".join(tag) if isinstance(tag, (list, tuple, set)) else tag
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(DEFAULT_TIMEOUT), limits=ASYNC_CLIENT_LIMITS
            ) as client:
                r = await client.post(
                    f"{self._endpoint}revalidate",
                    params={"tags": tags},
                    headers=self._headers,
                )
                if r.status_code != 200:
                    await r.aclose()
                    raise RuntimeError(
                        f"Failed to revalidate tag: {r.status_code} {r.reason_phrase}"
                    )
                await r.aclose()
        except Exception as e:
            if self._on_error:
                self._on_error(e)

    async def contains(self, key: str) -> bool:
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(DEFAULT_TIMEOUT), limits=ASYNC_CLIENT_LIMITS
            ) as client:
                r = await client.get(self._endpoint + key, headers=self._headers)
                if r.status_code == 404:
                    await r.aclose()
                    return False
                if r.status_code == 200:
                    cache_state = r.headers.get(HEADERS_VERCEL_CACHE_STATE)
                    if cache_state and cache_state.lower() != "fresh":
                        await r.aclose()
                        return False
                    await r.aclose()
                    return True
                await r.aclose()
                return False
        except Exception as e:
            if self._on_error:
                self._on_error(e)
            return False
