"""The flow route handler, driven the way a web framework drives it.

`workflow_entrypoint()` returns one handler for one route, and everything it
needs about the request arrives through `HTTPRequest` -- which promises a plain
mapping of headers, not an httpx one. HTTP headers are case-insensitive, plain
mappings are not, and what case they arrive in is decided by the framework in
front of us: ASGI lowercases, `BaseHTTPRequestHandler` and WSGI do not. So the
handler has to be indifferent to it, and that is what this pins.

A queue delivery, not the health probe, because the delivery is the path that
reads headers at all -- and a probe payload is the one delivery that needs no
run to exist first, so a real `LocalWorld` can answer it.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping

import pytest

from vercel.workflow._internal import core, runtime, world as w
from vercel.workflow._internal.worlds import local as local_mod

CID = "01K1CASEINSENSITIVE00000"
PROBE = json.dumps({"__healthCheck": True, "correlationId": CID}).encode()
HEALTH_QUEUE = "__wkf_workflow_health_check"


class Delivery(w.HTTPRequest):
    """A queue delivery whose headers are spelled however *headers* spells them."""

    def __init__(self, headers: Mapping[str, str], body: bytes = PROBE) -> None:
        self._headers = headers
        self._body = body

    @property
    def method(self) -> str:
        return "POST"

    @property
    def url(self) -> str:
        return runtime.FLOW_ROUTE

    @property
    def headers(self) -> Mapping[str, str]:
        return self._headers

    async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        yield self._body


@pytest.fixture(autouse=True)
def _reset_world(isolated_subscriptions):
    """`isolated_subscriptions` because these tests subscribe for real.

    Registrations are process-global and refuse to overlap, so a subscription
    another test left behind makes `workflow_entrypoint` here raise
    `DuplicateSubscriptionError` -- intermittently, depending on what shared the
    worker. Clearing on the way out is not enough; the fixture also clears on the
    way in, and puts back what it found.
    """
    yield
    w.set_world(None)


@pytest.mark.parametrize(
    "spelling",
    [
        {"x-vqs-queue-name": HEALTH_QUEUE, "content-type": "application/json"},
        {"X-VQS-Queue-Name": HEALTH_QUEUE, "Content-Type": "application/json"},
    ],
    ids=["lowercase", "mixed-case"],
)
async def test_a_delivery_is_dispatched_whatever_case_its_headers_use(
    spelling, tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = local_mod.LocalWorld()
    w.set_world(world)
    handler = runtime.workflow_entrypoint(core.Workflows(as_vercel_job=False))

    try:
        response = await handler(Delivery(spelling))
    finally:
        await world.aclose()

    assert response.status == 200
    # Accepted *and* dispatched: the answer on disk is what the subscribed
    # handler wrote, so a 200 alone could not have produced it.
    assert json.loads(response.body) == {"ok": True}
    assert await world.streams_list(f"wrun_hc_{CID}") == [f"__health_check__{CID}"]
