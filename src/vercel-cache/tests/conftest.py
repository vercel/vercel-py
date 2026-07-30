from __future__ import annotations

from collections.abc import Generator

import pytest


@pytest.fixture
def mock_env_clear(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    env_vars_to_clear = [
        "VERCEL_TOKEN",
        "VERCEL_TEAM_ID",
        "VERCEL_PROJECT_ID",
        "BLOB_READ_WRITE_TOKEN",
        "BLOB_STORE_ID",
        "VERCEL_OIDC_TOKEN",
        "VERCEL_OIDC_TOKEN_HEADER",
        "VERCEL_CACHE_API_TOKEN",
        "VERCEL_CACHE_API_URL",
        "VERCEL_URL",
        "VERCEL_ENV",
        "VERCEL_REGION",
    ]
    for var in env_vars_to_clear:
        monkeypatch.delenv(var, raising=False)

    import vercel.cache.runtime_cache as runtime_cache
    from vercel.headers import set_headers

    set_headers(None)
    monkeypatch.setattr(runtime_cache, "_cached_cache_instance", None)
    monkeypatch.setattr(runtime_cache, "_cached_async_cache_instance", None)
    yield
    set_headers(None)
