from __future__ import annotations

import json
import httpx
from typing import Callable, Mapping, Sequence


HEADERS_VERCEL_CACHE_STATE = "x-vercel-cache-state"
HEADERS_VERCEL_REVALIDATE = "x-vercel-revalidate"
HEADERS_VERCEL_CACHE_TAGS = "x-vercel-cache-tags"
HEADERS_VERCEL_CACHE_ITEM_NAME = "x-vercel-cache-item-name"


class BuildCache:
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
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(30.0))

    async def get(self, key: str):
        try:
            r = await self._client.get(self._endpoint + key, headers=self._headers)
            if r.status_code == 404:
                return None
            if r.status_code == 200:
                cache_state = r.headers.get(HEADERS_VERCEL_CACHE_STATE)
                if cache_state and cache_state.lower() != "fresh":
                    await r.aclose()
                    return None
                return r.json()
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

            r = await self._client.post(
                self._endpoint + key,
                headers={**self._headers, **optional_headers},
                content=json.dumps(value),
            )
            if r.status_code != 200:
                raise RuntimeError(
                    f"Failed to set cache: {r.status_code} {r.reason_phrase}"
                )
        except Exception as e:
            if self._on_error:
                self._on_error(e)

    async def delete(self, key: str) -> None:
        try:
            r = await self._client.delete(self._endpoint + key, headers=self._headers)
            if r.status_code != 200:
                raise RuntimeError(
                    f"Failed to delete cache: {r.status_code} {r.reason_phrase}"
                )
        except Exception as e:
            if self._on_error:
                self._on_error(e)

    async def expire_tag(self, tag: str | Sequence[str]) -> None:
        try:
            tags = ",".join(tag) if isinstance(tag, (list, tuple, set)) else tag
            r = await self._client.post(
                f"{self._endpoint}revalidate",
                params={"tags": tags},
                headers=self._headers,
            )
            if r.status_code != 200:
                raise RuntimeError(
                    f"Failed to revalidate tag: {r.status_code} {r.reason_phrase}"
                )
        except Exception as e:
            if self._on_error:
                self._on_error(e)
