from __future__ import annotations

from collections.abc import Generator

import pytest

from vercel.cache import context as ctx
from vercel.headers import set_headers as set_shared_headers


@pytest.fixture
def isolated_context() -> Generator[None, None, None]:
    ctx._cv_wait_until.set(None)
    ctx._cv_cache.set(None)
    ctx._cv_async_cache.set(None)
    ctx._cv_purge.set(None)
    set_shared_headers(None)
    yield
    ctx._cv_wait_until.set(None)
    ctx._cv_cache.set(None)
    ctx._cv_async_cache.set(None)
    ctx._cv_purge.set(None)
    set_shared_headers(None)


class FakeCacheObject: ...


class TestSetContextSetsValues:
    def test_cache_is_assigned(self, isolated_context: None) -> None:
        instance = FakeCacheObject()
        ctx.set_context(cache=instance)
        assert ctx.get_context().cache is instance

    def test_async_cache_is_assigned(self, isolated_context: None) -> None:
        instance = FakeCacheObject()
        ctx.set_context(async_cache=instance)
        assert ctx.get_context().async_cache is instance

    def test_only_specified_slots_are_touched(self, isolated_context: None) -> None:
        existing = FakeCacheObject()
        ctx.set_context(async_cache=existing)
        assert ctx.get_context().cache is None

        new_sync = FakeCacheObject()
        ctx.set_context(cache=new_sync)

        snapshot = ctx.get_context()
        assert snapshot.cache is new_sync
        assert snapshot.async_cache is existing


class TestSetContextClearsWithNone:
    def test_cache_none_clears_existing_value(self, isolated_context: None) -> None:
        ctx.set_context(cache=FakeCacheObject())
        assert ctx.get_context().cache is not None

        ctx.set_context(cache=None)
        assert ctx.get_context().cache is None

    def test_async_cache_none_clears_existing_value(self, isolated_context: None) -> None:
        ctx.set_context(async_cache=FakeCacheObject())
        assert ctx.get_context().async_cache is not None

        ctx.set_context(async_cache=None)
        assert ctx.get_context().async_cache is None

    def test_headers_none_clears_existing_value(self, isolated_context: None) -> None:
        ctx.set_context(headers={"a": "b"})
        assert ctx.get_context().headers == {"a": "b"}

        ctx.set_context(headers=None)
        assert ctx.get_context().headers is None

    def test_mixed_set_and_clear_in_single_call(self, isolated_context: None) -> None:
        ctx.set_context(cache=FakeCacheObject())
        new_async = FakeCacheObject()

        ctx.set_context(cache=None, async_cache=new_async)

        snapshot = ctx.get_context()
        assert snapshot.cache is None
        assert snapshot.async_cache is new_async


class TestSharedHeaderContext:
    def test_cache_set_headers_updates_shared_headers(self, isolated_context: None) -> None:
        from vercel.headers import get_headers

        ctx.set_headers({"x-test": "from-cache"})

        assert get_headers() == {"x-test": "from-cache"}

    def test_shared_set_headers_updates_cache_context(self, isolated_context: None) -> None:
        from vercel.headers import set_headers

        set_headers({"x-test": "from-shared"})

        assert ctx.get_headers() == {"x-test": "from-shared"}
        assert ctx.get_context().headers == {"x-test": "from-shared"}
