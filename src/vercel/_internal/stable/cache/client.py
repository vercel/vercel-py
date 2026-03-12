"""Clean-room cache helpers for the stable client surface."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import httpx

from vercel._internal.http import JSONBody, RequestClient, sync_sleep
from vercel._internal.http.request_client import SleepFn
from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.stable.runtime import AsyncRuntime, SyncRuntime
from vercel.stable.options import CacheOptions, CacheSetOptions

HEADERS_VERCEL_CACHE_STATE = "x-vercel-cache-state"
HEADERS_VERCEL_REVALIDATE = "x-vercel-revalidate"
HEADERS_VERCEL_CACHE_TAGS = "x-vercel-cache-tags"
HEADERS_VERCEL_CACHE_ITEM_NAME = "x-vercel-cache-item-name"
_DEFAULT_NAMESPACE_SEPARATOR = "$"


def default_key_hash_function(key: str) -> str:
    h = 5381
    for ch in key:
        h = ((h * 33) ^ ord(ch)) & 0xFFFFFFFF
    return format(h, "x")


def create_key_transformer(
    key_fn: Callable[[str], str] | None,
    namespace: str | None,
    separator: str | None,
) -> Callable[[str], str]:
    resolved_key_fn = key_fn or default_key_hash_function
    resolved_separator = separator or _DEFAULT_NAMESPACE_SEPARATOR

    def make_key(key: str) -> str:
        hashed = resolved_key_fn(key)
        if not namespace:
            return hashed
        return f"{namespace}{resolved_separator}{hashed}"

    return make_key


@dataclass(slots=True)
class _CacheEntry:
    value: object
    expires_at: float | None
    tags: frozenset[str]


@dataclass(slots=True)
class _SharedCacheStore:
    entries: dict[str, _CacheEntry]
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def get(self, key: str) -> object | None:
        with self._lock:
            entry = self.entries.get(key)
            if entry is None:
                return None
            if entry.expires_at is not None and entry.expires_at <= time.monotonic():
                self.entries.pop(key, None)
                return None
            return entry.value

    def set(
        self,
        key: str,
        value: object,
        options: Mapping[str, Any] | CacheSetOptions | None,
    ) -> None:
        normalized = _coerce_cache_options(options)
        ttl_value = normalized.get("ttl")
        expires_at = (
            time.monotonic() + float(ttl_value) if isinstance(ttl_value, (int, float)) else None
        )
        tags_value = normalized.get("tags")
        tags = (
            frozenset(tag for tag in tags_value if isinstance(tag, str))
            if tags_value
            else frozenset()
        )
        with self._lock:
            self.entries[key] = _CacheEntry(value=value, expires_at=expires_at, tags=tags)

    def delete(self, key: str) -> None:
        with self._lock:
            self.entries.pop(key, None)

    def expire_tag(self, tags: Sequence[str]) -> None:
        active_tags = frozenset(tags)
        with self._lock:
            for key, entry in list(self.entries.items()):
                if entry.tags & active_tags:
                    self.entries.pop(key, None)


_SHARED_STORE = _SharedCacheStore(entries={})


@dataclass(slots=True)
class CacheClientLineage:
    runtime: SyncRuntime | AsyncRuntime
    root_timeout: float | None
    env: Mapping[str, str]
    store: _SharedCacheStore = field(default_factory=lambda: _SHARED_STORE)
    request_state: CacheRequestState = field(default_factory=lambda: CacheRequestState())


@dataclass(slots=True)
class CacheRequestState:
    request_client: RequestClient | None = None


def _load_env_headers(env: Mapping[str, str]) -> Mapping[str, str]:
    raw = env.get("RUNTIME_CACHE_HEADERS")
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except ValueError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(key): str(value) for key, value in payload.items()}


def resolve_cache_endpoint(options: CacheOptions, env: Mapping[str, str]) -> str | None:
    return options.endpoint or env.get("RUNTIME_CACHE_ENDPOINT")


def resolve_cache_headers(options: CacheOptions, env: Mapping[str, str]) -> dict[str, str]:
    headers = dict(_load_env_headers(env))
    headers.update({str(key): str(value) for key, value in options.headers.items()})
    return headers


def uses_remote_cache(options: CacheOptions, env: Mapping[str, str]) -> bool:
    return resolve_cache_endpoint(options, env) is not None and bool(
        resolve_cache_headers(options, env)
    )


@dataclass(slots=True)
class StableCacheRequestClient:
    _lineage: CacheClientLineage
    _options: CacheOptions
    _sleep_fn: SleepFn

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: JSONBody | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        request_client = await self._get_request_client()
        return await request_client.send(
            method,
            self._build_url(path),
            params=params,
            body=body,
            headers=headers,
            timeout=self._lineage.root_timeout if timeout is None else timeout,
        )

    def uses_remote_cache(self) -> bool:
        return uses_remote_cache(self._options, self._lineage.env)

    async def _get_request_client(self) -> RequestClient:
        request_client = self._lineage.request_state.request_client
        if request_client is None:
            transport = await self._lineage.runtime.get_transport(
                timeout=self._lineage.root_timeout,
            )
            request_client = RequestClient(
                transport=transport,
                base_headers=self._base_headers(),
                sleep_fn=self._sleep_fn,
            )
            self._lineage.request_state.request_client = request_client
        return request_client

    def _build_url(self, path: str) -> str:
        return _join_cache_url(resolve_cache_endpoint(self._options, self._lineage.env), path)

    def _base_headers(self) -> dict[str, str]:
        return resolve_cache_headers(self._options, self._lineage.env)


@dataclass(slots=True)
class StableCacheBackend:
    _lineage: CacheClientLineage
    _options: CacheOptions
    _request_client: StableCacheRequestClient

    def transform_key(self, key: str) -> str:
        return create_key_transformer(
            self._options.key_hash_function,
            self._options.namespace,
            self._options.namespace_separator,
        )(key)

    async def get(self, key: str) -> object | None:
        transformed = self.transform_key(key)
        if not self._request_client.uses_remote_cache():
            return self._lineage.store.get(transformed)

        response = await self._request_client.request("GET", transformed)
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            return None
        cache_state = response.headers.get(HEADERS_VERCEL_CACHE_STATE)
        if cache_state is not None and cache_state.lower() != "fresh":
            return None
        return response.json()

    async def set(
        self,
        key: str,
        value: object,
        options: Mapping[str, Any] | CacheSetOptions | None = None,
    ) -> None:
        transformed = self.transform_key(key)
        if not self._request_client.uses_remote_cache():
            self._lineage.store.set(transformed, value, options)
            return

        headers = _cache_headers(options)
        await self._request_client.request(
            "POST",
            transformed,
            body=JSONBody(value),
            headers=headers,
        )

    async def delete(self, key: str) -> None:
        transformed = self.transform_key(key)
        if not self._request_client.uses_remote_cache():
            self._lineage.store.delete(transformed)
            return

        await self._request_client.request("DELETE", transformed)

    async def expire_tag(self, tag: str | Sequence[str]) -> None:
        tags = [tag] if isinstance(tag, str) else [value for value in tag if isinstance(value, str)]
        if not self._request_client.uses_remote_cache():
            self._lineage.store.expire_tag(tags)
            return

        await self._request_client.request("POST", "revalidate", params={"tags": ",".join(tags)})

    async def contains(self, key: str) -> bool:
        return await self.get(key) is not None


def create_sync_request_client(
    *,
    lineage: CacheClientLineage,
    options: CacheOptions,
) -> StableCacheRequestClient:
    return StableCacheRequestClient(
        _lineage=lineage,
        _options=options,
        _sleep_fn=sync_sleep,
    )


def create_async_request_client(
    *,
    lineage: CacheClientLineage,
    options: CacheOptions,
) -> StableCacheRequestClient:
    return StableCacheRequestClient(
        _lineage=lineage,
        _options=options,
        _sleep_fn=asyncio.sleep,
    )


def sync_get(
    *,
    backend: StableCacheBackend,
    key: str,
) -> object | None:
    return iter_coroutine(backend.get(key))


def sync_set(
    *,
    backend: StableCacheBackend,
    key: str,
    value: object,
    options: Mapping[str, Any] | CacheSetOptions | None = None,
) -> None:
    iter_coroutine(backend.set(key, value, options))


def sync_delete(*, backend: StableCacheBackend, key: str) -> None:
    iter_coroutine(backend.delete(key))


def sync_expire_tag(*, backend: StableCacheBackend, tag: str | Sequence[str]) -> None:
    iter_coroutine(backend.expire_tag(tag))


def sync_contains(*, backend: StableCacheBackend, key: str) -> bool:
    return iter_coroutine(backend.contains(key))


def _join_cache_url(endpoint: str | None, path: str) -> str:
    if endpoint is None:
        raise RuntimeError("cache endpoint is required for remote cache operations")
    return endpoint.rstrip("/") + "/" + path.lstrip("/")


def _coerce_cache_options(
    options: Mapping[str, Any] | CacheSetOptions | None,
) -> dict[str, Any]:
    if options is None:
        return {}
    if isinstance(options, CacheSetOptions):
        payload: dict[str, Any] = {}
        if options.ttl is not None:
            payload["ttl"] = options.ttl
        if options.tags:
            payload["tags"] = list(options.tags)
        if options.name is not None:
            payload["name"] = options.name
        return payload
    return dict(options)


def _cache_headers(options: Mapping[str, Any] | CacheSetOptions | None) -> dict[str, str]:
    headers: dict[str, str] = {}
    normalized = _coerce_cache_options(options)
    ttl = normalized.get("ttl")
    if isinstance(ttl, (int, float)):
        headers[HEADERS_VERCEL_REVALIDATE] = str(ttl)
    tags = normalized.get("tags")
    if isinstance(tags, Sequence) and not isinstance(tags, str):
        values = [tag for tag in tags if isinstance(tag, str)]
        if values:
            headers[HEADERS_VERCEL_CACHE_TAGS] = ",".join(values)
    name = normalized.get("name")
    if isinstance(name, str) and name:
        headers[HEADERS_VERCEL_CACHE_ITEM_NAME] = name
    return headers


__all__ = [
    "CacheClientLineage",
    "CacheRequestState",
    "StableCacheBackend",
    "StableCacheRequestClient",
    "create_key_transformer",
    "create_async_request_client",
    "create_sync_request_client",
    "default_key_hash_function",
    "resolve_cache_endpoint",
    "resolve_cache_headers",
    "uses_remote_cache",
    "sync_contains",
    "sync_delete",
    "sync_expire_tag",
    "sync_get",
    "sync_set",
]
