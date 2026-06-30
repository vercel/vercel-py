"""Backend hook-event 404 -> HookNotFoundError mapping (VercelWorld).

The Vercel backend returns 404 on hook_disposed / hook_received when the hook is
already disposed or never existed. VercelWorld.events_create translates that into
a typed HookNotFoundError so the runtime can treat a duplicate dispose as a benign
skip. Other 404s stay a generic WorkflowWorldError carrying the status.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from vercel._internal.workflow import world as w
from vercel._internal.workflow.worlds.vercel import VercelWorld

RUN_ID = "wrun_test"


def _world() -> VercelWorld:
    # Pass a token so events_create doesn't try to fetch an OIDC token.
    return VercelWorld(token="test-token")


@respx.mock
async def test_hook_disposed_404_maps_to_hook_not_found() -> None:
    world = _world()
    respx.post(f"{world._base_url}/v3/runs/{RUN_ID}/events").mock(
        return_value=httpx.Response(404, json={"message": "hook not found", "code": "not_found"})
    )

    with pytest.raises(w.HookNotFoundError):
        await world.events_create(RUN_ID, w.HookDisposedEvent(correlationId="hook_1"))


@respx.mock
async def test_non_hook_404_is_not_translated() -> None:
    world = _world()
    respx.post(f"{world._base_url}/v3/runs/{RUN_ID}/events").mock(
        return_value=httpx.Response(404, json={"message": "run not found"})
    )

    with pytest.raises(w.WorkflowWorldError) as exc_info:
        await world.events_create(RUN_ID, w.RunStartedEvent())
    assert exc_info.value.status == 404


@respx.mock
async def test_410_maps_to_run_expired() -> None:
    world = _world()
    respx.post(f"{world._base_url}/v3/runs/{RUN_ID}/events").mock(
        return_value=httpx.Response(410, json={"message": "run expired"})
    )

    with pytest.raises(w.RunExpiredError) as exc_info:
        await world.events_create(RUN_ID, w.RunStartedEvent())
    assert exc_info.value.status == 410


@respx.mock
async def test_429_maps_to_throttle_with_retry_after() -> None:
    world = _world()
    respx.post(f"{world._base_url}/v3/runs/{RUN_ID}/events").mock(
        return_value=httpx.Response(
            429, headers={"retry-after": "12"}, json={"message": "slow down"}
        )
    )

    with pytest.raises(w.ThrottleError) as exc_info:
        await world.events_create(RUN_ID, w.RunStartedEvent())
    assert exc_info.value.status == 429
    assert exc_info.value.retry_after == 12
