from __future__ import annotations

import json
import time

import pytest
import respx
from httpx import Response

import vercel
from vercel.stable.errors import TransportClosedError
from vercel.stable.options import CacheSetOptions


def _clear_cache_store(*caches: object) -> None:
    seen: set[int] = set()
    for cache in caches:
        lineage = getattr(cache, "_lineage", None)
        store = getattr(lineage, "store", None)
        if store is None or id(store) in seen:
            continue
        store.entries.clear()
        seen.add(id(store))


def test_cache_in_memory_namespace_ttl_and_tags() -> None:
    vc = vercel.create_sync_client()
    cache_a = vc.get_cache(namespace="stable-a")
    cache_b = vc.get_cache(namespace="stable-b")

    try:
        cache_a.set("user", {"name": "Ada"}, CacheSetOptions(ttl=1, tags=("user", "feed")))
        cache_b.set("user", {"name": "Bob"})

        assert cache_a.get("user") == {"name": "Ada"}
        assert cache_b.get("user") == {"name": "Bob"}
        assert "user" in cache_a

        cache_a.expire_tag("feed")
        assert cache_a.get("user") is None
        assert cache_b.get("user") == {"name": "Bob"}

        cache_a.set("ttl-key", "value", CacheSetOptions(ttl=1))
        time.sleep(1.1)
        assert cache_a.get("ttl-key") is None
    finally:
        vc.close()


@respx.mock
def test_cache_remote_uses_bare_key_urls_and_revalidate() -> None:
    endpoint = "https://cache.example.com/v1"
    get_route = respx.get("https://cache.example.com/v1/ns$abc123").mock(
        return_value=Response(200, json={"name": "Ada"}, headers={"x-vercel-cache-state": "fresh"})
    )
    set_route = respx.post("https://cache.example.com/v1/ns$abc123").mock(
        return_value=Response(200, json={"status": "OK"})
    )
    delete_route = respx.delete("https://cache.example.com/v1/ns$abc123").mock(
        return_value=Response(200, json={"status": "OK"})
    )
    revalidate_route = respx.post("https://cache.example.com/v1/revalidate").mock(
        return_value=Response(200, json={"status": "OK"})
    )

    vc = vercel.create_sync_client(timeout=5.0)
    cache = vc.get_cache(
        endpoint=endpoint,
        headers={"x-cache-token": "cache-token"},
        namespace="ns",
        key_hash_function=lambda key: "abc123",
    )

    try:
        cache.set(
            "user",
            {"name": "Ada"},
            CacheSetOptions(ttl=30, tags=("user",), name="profile"),
        )
        assert cache.get("user") == {"name": "Ada"}
        cache.expire_tag(["user", "feed"])
        cache.delete("user")
    finally:
        vc.close()

    assert set_route.called
    set_request = set_route.calls[0].request
    assert json.loads(set_request.content) == {"name": "Ada"}
    assert set_request.headers["x-cache-token"] == "cache-token"
    assert set_request.headers["x-vercel-revalidate"] == "30"
    assert set_request.headers["x-vercel-cache-tags"] == "user"
    assert set_request.headers["x-vercel-cache-item-name"] == "profile"

    assert get_route.called
    assert delete_route.called
    assert revalidate_route.called
    assert revalidate_route.calls[0].request.url.params["tags"] == "user,feed"


@respx.mock
def test_cache_remote_can_resolve_endpoint_and_headers_from_root_env() -> None:
    endpoint = "https://cache.example.com/v1"
    set_route = respx.post("https://cache.example.com/v1/env$abc123").mock(
        return_value=Response(200, json={"status": "OK"})
    )
    get_route = respx.get("https://cache.example.com/v1/env$abc123").mock(
        return_value=Response(200, json={"name": "Ada"}, headers={"x-vercel-cache-state": "fresh"})
    )

    vc = vercel.create_sync_client(
        timeout=5.0,
        env={
            "RUNTIME_CACHE_ENDPOINT": endpoint,
            "RUNTIME_CACHE_HEADERS": '{"x-cache-env":"env-token"}',
        },
    )
    cache = vc.get_cache(
        headers={"x-cache-overlay": "overlay-token"},
        namespace="env",
        key_hash_function=lambda key: "abc123",
    )

    try:
        cache.set("user", {"name": "Ada"})
        assert cache.get("user") == {"name": "Ada"}
    finally:
        vc.close()

    assert set_route.called
    set_request = set_route.calls[0].request
    assert set_request.headers["x-cache-env"] == "env-token"
    assert set_request.headers["x-cache-overlay"] == "overlay-token"

    assert get_route.called
    get_request = get_route.calls[0].request
    assert get_request.headers["x-cache-env"] == "env-token"
    assert get_request.headers["x-cache-overlay"] == "overlay-token"


def test_sync_cache_ensure_connected_reuses_root_lineage() -> None:
    vc = vercel.create_sync_client()
    cache_a = vc.get_cache(namespace="stable-a")
    cache_b = vc.get_cache(namespace="stable-b")

    try:
        _clear_cache_store(cache_a, cache_b)
        assert cache_a.ensure_connected() is cache_a
        assert cache_b.ensure_connected() is cache_b
        cache_a.set("user", {"name": "Ada"})
        assert cache_b.get("user") is None
    finally:
        _clear_cache_store(cache_a, cache_b)
        vc.close()


def test_sync_cache_with_options_keeps_shared_store_but_derives_request_overlays() -> None:
    vc = vercel.create_sync_client(timeout=5.0)
    base = vc.get_cache(namespace="stable-a")
    child = base.with_options(namespace="stable-b", headers={"x-cache-token": "overlay"})

    try:
        _clear_cache_store(base, child)
        base.set("user", {"name": "Ada"})

        assert base._lineage is not child._lineage
        assert base._lineage.runtime is child._lineage.runtime
        assert base._lineage.store is child._lineage.store
        assert base._lineage.request_state is not child._lineage.request_state
        assert child.get("user") is None
        assert child._options.headers["x-cache-token"] == "overlay"
        assert child._options.namespace == "stable-b"
        assert base._options.namespace == "stable-a"
    finally:
        _clear_cache_store(base, child)
        vc.close()


@respx.mock
def test_sync_cache_with_options_inherits_remote_endpoint_and_hash_function() -> None:
    endpoint = "https://cache.example.com/v1"

    def key_hash(key: str) -> str:
        return f"hashed-{key}"

    base_route = respx.post("https://cache.example.com/v1/base:hashed-user").mock(
        return_value=Response(200, json={"status": "OK"})
    )
    child_route = respx.post("https://cache.example.com/v1/child:hashed-user").mock(
        return_value=Response(200, json={"status": "OK"})
    )

    vc = vercel.create_sync_client(timeout=5.0)
    base = vc.get_cache(
        endpoint=endpoint,
        headers={"x-cache-token": "base-token"},
        namespace="base",
        namespace_separator=":",
        key_hash_function=key_hash,
    )
    child = base.with_options(namespace="child", headers={"x-cache-token": "child-token"})

    try:
        base.set("user", {"name": "Ada"})
        child.set("user", {"name": "Bob"})
    finally:
        vc.close()

    assert base_route.called
    assert child_route.called
    assert base_route.calls[0].request.headers["x-cache-token"] == "base-token"
    assert child_route.calls[0].request.headers["x-cache-token"] == "child-token"


@respx.mock
def test_sync_cache_reuses_request_client_per_cache_lineage() -> None:
    endpoint = "https://cache.example.com/v1"
    route = respx.get("https://cache.example.com/v1/ns$abc123").mock(
        return_value=Response(200, json={"name": "Ada"}, headers={"x-vercel-cache-state": "fresh"})
    )

    vc = vercel.create_sync_client(timeout=5.0)
    cache = vc.get_cache(
        endpoint=endpoint,
        headers={"x-cache-token": "cache-token"},
        namespace="ns",
        key_hash_function=lambda key: "abc123",
    )

    try:
        assert cache._lineage.request_state.request_client is None
        assert cache.get("user") == {"name": "Ada"}
        request_client = cache._lineage.request_state.request_client
        assert request_client is not None
        assert cache.get("user") == {"name": "Ada"}
        assert cache._lineage.request_state.request_client is request_client
    finally:
        vc.close()

    assert route.called


@pytest.mark.asyncio
async def test_async_cache_ensure_connected_reuses_root_lineage() -> None:
    vc = vercel.create_async_client()
    cache_a = vc.get_cache(namespace="stable-a")
    cache_b = vc.get_cache(namespace="stable-b")

    try:
        _clear_cache_store(cache_a, cache_b)
        assert await cache_a.ensure_connected() is cache_a
        assert await cache_b.ensure_connected() is cache_b
        await cache_a.set("user", {"name": "Ada"})
        assert await cache_b.get("user") is None
    finally:
        _clear_cache_store(cache_a, cache_b)
        await vc.aclose()


def test_sync_cache_ensure_connected_rejects_closed_lineage() -> None:
    vc = vercel.create_sync_client()
    cache = vc.get_cache(namespace="stable-a")
    vc.close()

    with pytest.raises(TransportClosedError):
        cache.ensure_connected()


@pytest.mark.asyncio
async def test_async_cache_ensure_connected_rejects_closed_lineage() -> None:
    vc = vercel.create_async_client()
    cache = vc.get_cache(namespace="stable-a")
    await vc.aclose()

    with pytest.raises(TransportClosedError):
        await cache.ensure_connected()
