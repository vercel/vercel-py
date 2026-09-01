"""Vercel request IDs attached to workflow event writes."""

from __future__ import annotations

from collections.abc import Mapping

import cbor2
import httpx
import pytest
import respx

from vercel.headers import HeadersContext
from vercel.workflow._internal import world as w
from vercel.workflow._internal.worlds.vercel import VercelWorld

RUN_ID = "wrun_test"


def _event_route(world: VercelWorld):
    return respx.post(f"{world._base_url}/v3/runs/{RUN_ID}/events").mock(
        return_value=httpx.Response(
            200,
            content=cbor2.dumps({}),
            headers={"content-type": "application/cbor"},
        ),
    )


def _sent_body(route) -> dict:
    return cbor2.loads(route.calls.last.request.content)


@respx.mock
async def test_event_write_carries_the_ambient_vercel_request_id() -> None:
    world = VercelWorld(token="test-token")
    route = _event_route(world)

    with HeadersContext({"x-vercel-id": " iad1::abc-123 "}).use():
        await world.events_create(RUN_ID, w.RunStartedEvent())

    assert _sent_body(route)["vercelId"] == "iad1::abc-123"


@pytest.mark.parametrize(
    "headers",
    [None, {}, {"x-other": "value"}, {"x-vercel-id": "  "}],
)
@respx.mock
async def test_event_write_omits_an_unavailable_vercel_request_id(
    headers: Mapping[str, str] | None,
) -> None:
    world = VercelWorld(token="test-token")
    route = _event_route(world)

    with HeadersContext(headers).use():
        await world.events_create(RUN_ID, w.RunStartedEvent())

    assert "vercelId" not in _sent_body(route)
