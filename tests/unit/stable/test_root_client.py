from __future__ import annotations

import threading
from typing import Any

import httpx
import pytest
from hypothesis import given, strategies as st

from vercel._internal.stable.options import merge_mapping
from vercel.stable.client import create_sync_client


class _TrackingEnv(dict):
    """A dict subclass that records every .get() call."""

    def __init__(self) -> None:
        super().__init__()
        self.get_calls: list[str] = []

    def get(self, key: str, default: Any = None) -> Any:  # noqa: ANN401
        self.get_calls.append(key)
        return super().get(key, default)


def test_client_creation_defers_env_access() -> None:
    env = _TrackingEnv()
    client = create_sync_client(timeout=12.0, env=env)
    _ = client.get_sdk(token="token")
    _ = client.get_blob(token="blob-token")
    _ = client.get_sandbox(token="sandbox-token")

    assert env.get_calls == [], "create_sync_client should not access env at creation time"


def test_sync_runtime_initializes_once_under_threaded_access() -> None:
    transport_calls: list[float | None] = []
    created_clients: list[httpx.Client] = []
    start = threading.Event()

    def track_create_base_client(*, timeout: float | None = None) -> httpx.Client:
        start.wait(timeout=1)
        transport_calls.append(timeout)
        client = httpx.Client()
        created_clients.append(client)
        return client

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "vercel._internal.stable.runtime.create_base_client",
            track_create_base_client,
        )

        client = create_sync_client(timeout=16.0, env={})
        threads = [threading.Thread(target=client.ensure_connected) for _ in range(4)]
        for thread in threads:
            thread.start()
        start.set()
        for thread in threads:
            thread.join()

        client.close()

    assert transport_calls == [16.0]
    for http_client in created_clients:
        assert http_client.is_closed


# ---------------------------------------------------------------------------
# Property-based tests
# ---------------------------------------------------------------------------

_key_st = st.text(min_size=1, max_size=50, alphabet=st.characters(codec="ascii"))
_val_st = st.text(min_size=0, max_size=50, alphabet=st.characters(codec="ascii"))
_mapping_st = st.dictionaries(_key_st, _val_st, max_size=10)


@given(base=_mapping_st, override=_mapping_st)
def test_prop_merge_mapping_last_writer_wins(
    base: dict[str, str],
    override: dict[str, str],
) -> None:
    result = merge_mapping(base, override)
    for k in override:
        assert result[k] == override[k]
    assert set(result.keys()) == set(base.keys()) | set(override.keys())
