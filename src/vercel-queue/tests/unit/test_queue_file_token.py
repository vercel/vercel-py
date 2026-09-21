from __future__ import annotations

import base64
import json
import time
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace

import pytest

from vercel.oidc import token as oidc
from vercel.queue import ALL_DEPLOYMENTS
from vercel.queue.devserver import EmbeddedQueueDevServer


@pytest.fixture
def rotate_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Callable[[], str]:
    monkeypatch.delenv("VERCEL_QUEUE_TOKEN", raising=False)
    monkeypatch.delenv("VERCEL_OIDC_TOKEN", raising=False)
    path = tmp_path / "token"
    now = time.time()
    monkeypatch.setattr(oidc, "_OIDC_TOKEN_PATH", path)
    monkeypatch.setattr(oidc, "time", SimpleNamespace(time=lambda: now, monotonic=lambda: now))

    def rotate() -> str:
        nonlocal now
        now += 60
        payload = base64.urlsafe_b64encode(json.dumps({"exp": now + 120}).encode()).decode()
        token = f"header.{payload}.signature"
        replacement = tmp_path / "replacement"
        replacement.write_text(token, encoding="utf-8")
        replacement.replace(path)
        return token

    return rotate


def test_sync_client_refreshes_file_token_for_send_poll_and_lease(
    eqs: EmbeddedQueueDevServer, rotate_token: Callable[[], str]
) -> None:
    client = eqs.get_sync_client(token=None, deployment=ALL_DEPLOYMENTS)
    first = rotate_token()
    client.send("emails", {"ok": True})
    assert eqs.state.requests[-1].headers["Authorization"] == f"Bearer {first}"

    second = rotate_token()
    [delivery] = client.poll("emails", "test-group", limit=1)
    assert eqs.state.requests[-1].headers["Authorization"] == f"Bearer {second}"

    third = rotate_token()
    client.extend_lease(delivery.message, 30)
    assert eqs.state.requests[-1].headers["Authorization"] == f"Bearer {third}"
    client.acknowledge(delivery.message)
    assert eqs.state.requests[-1].headers["Authorization"] == f"Bearer {third}"
    client.send("emails", {"ok": False})
    assert eqs.state.requests[-1].headers["Authorization"] == f"Bearer {third}"


@pytest.mark.anyio
async def test_async_client_refreshes_file_token_for_send_poll_and_lease(
    eqs: EmbeddedQueueDevServer, rotate_token: Callable[[], str]
) -> None:
    client = eqs.get_async_client(token=None, deployment=ALL_DEPLOYMENTS)
    first = rotate_token()
    await client.send("emails", {"ok": True})
    assert eqs.state.requests[-1].headers["Authorization"] == f"Bearer {first}"

    second = rotate_token()
    deliveries = [delivery async for delivery in client.poll("emails", "test-group", limit=1)]
    assert eqs.state.requests[-1].headers["Authorization"] == f"Bearer {second}"

    third = rotate_token()
    await client.extend_lease(deliveries[0].message, 30)
    assert eqs.state.requests[-1].headers["Authorization"] == f"Bearer {third}"
    await client.acknowledge(deliveries[0].message)
    assert eqs.state.requests[-1].headers["Authorization"] == f"Bearer {third}"
    await client.send("emails", {"ok": False})
    assert eqs.state.requests[-1].headers["Authorization"] == f"Bearer {third}"


@pytest.mark.parametrize("explicit", [False, True])
def test_queue_overrides_take_precedence_over_file(
    eqs: EmbeddedQueueDevServer,
    rotate_token: Callable[[], str],
    monkeypatch: pytest.MonkeyPatch,
    *,
    explicit: bool,
) -> None:
    rotate_token()
    monkeypatch.setenv("VERCEL_QUEUE_TOKEN", "env-token")
    client = eqs.get_sync_client(
        token="explicit-token" if explicit else None, deployment=ALL_DEPLOYMENTS
    )
    client.send("emails", {"ok": True})
    expected = "explicit-token" if explicit else "env-token"
    assert eqs.state.requests[-1].headers["Authorization"] == f"Bearer {expected}"
