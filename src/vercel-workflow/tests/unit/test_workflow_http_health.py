"""The HTTP health probe, which shares the flow route with queue deliveries.

`?__health` is answered with plain JSON -- no queue, no stream, no correlation
id. Both of its callers are local-development paths: the CLI's reachability
precheck before it runs the queue probe (which POSTs) and dev-server port
discovery (which sends HEAD, and reads only the status).

Same URL as a delivery, so what matters is the dispatch, in both directions: a
probe must not reach the queue handler, and -- the expensive one -- a delivery
must not be answered as a probe, which drops the message and strands its run.
``FlowWorld`` hands back a recording queue handler so both are visible.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from vercel.workflow._internal import core, runtime, world as w

from ..world_stubs import NoStreams

DELIVERED = w.HTTPResponse(202, b"delivered", {})


class Request(w.HTTPRequest):
    """The smallest thing a web framework adapter has to supply."""

    def __init__(self, target: str = runtime.FLOW_ROUTE, method: str = "POST") -> None:
        self._target = target
        self._method = method

    @property
    def method(self) -> str:
        return self._method

    @property
    def url(self) -> str:
        return self._target

    @property
    def headers(self) -> httpx.Headers:
        return httpx.Headers()

    async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        return
        yield b""  # pragma: no cover -- makes this a generator


class FlowWorld(NoStreams, w.World):
    """A world whose queue handler records what was passed through to it."""

    def __init__(self) -> None:
        self.delivered: list[w.HTTPRequest] = []

    def create_queue_handler(
        self, queue_name_prefix: w.QueuePrefix, handler: w.QueueHandler
    ) -> w.HTTPHandler:
        async def http_handler(request: w.HTTPRequest) -> w.HTTPResponse:
            self.delivered.append(request)
            return DELIVERED

        return http_handler

    # -- unused World surface -------------------------------------------
    async def get_deployment_id(self) -> str:
        return ""

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
        raise NotImplementedError

    async def runs_get(self, run_id: str) -> w.WorkflowRun:
        raise NotImplementedError

    async def steps_get(self, run_id: str, step_id: str) -> w.WorkflowStep:
        raise NotImplementedError

    async def hooks_get_by_token(self, token: str) -> w.Hook:
        raise NotImplementedError

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        raise NotImplementedError

    async def events_list(self, run_id: str, *, pagination: Any = None) -> Any:
        raise NotImplementedError


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)


@pytest.fixture
def flow() -> tuple[w.HTTPHandler, FlowWorld]:
    """The flow route handler, over a world that records deliveries."""
    world = FlowWorld()
    w.set_world(world)
    return runtime.workflow_entrypoint(core.Workflows(as_vercel_job=False)), world


async def test_answers_a_probe_with_json(flow) -> None:
    handler, world = flow

    response = await handler(Request(f"{runtime.FLOW_ROUTE}?__health"))

    assert response.status == 200
    assert response.headers["content-type"] == "application/json"
    assert json.loads(response.body) == {
        "healthy": True,
        "endpoint": runtime.FLOW_ROUTE,
        "specVersion": w.SPEC_VERSION_CURRENT,
    }
    # A probe is not a message: nothing was handed to the queue handler.
    assert world.delivered == []


async def test_a_delivery_is_passed_through_untouched(flow) -> None:
    """The direction that costs a run if it regresses."""
    handler, world = flow
    request = Request()

    response = await handler(request)

    assert response is DELIVERED
    assert world.delivered == [request]


async def test_the_reported_path_is_the_one_that_was_probed(flow) -> None:
    """Taken from the request, as `withHealthCheck` takes `url.pathname`, so an
    app serving the route elsewhere does not claim the well-known path."""
    handler, _ = flow

    response = await handler(Request("/api/workflow?__health=1"))

    assert json.loads(response.body)["endpoint"] == "/api/workflow"


async def test_the_answer_claims_no_capability_it_does_not_have(flow) -> None:
    """Omitted for the same reason as on the queue transport: it names a
    JavaScript package's version, and the reader feeds it to that package's
    capability tables."""
    handler, _ = flow

    response = await handler(Request(f"{runtime.FLOW_ROUTE}?__health"))

    assert "workflowCoreVersion" not in json.loads(response.body)


async def test_a_preflight_gets_an_empty_204(flow) -> None:
    handler, world = flow

    response = await handler(Request(f"{runtime.FLOW_ROUTE}?__health", "OPTIONS"))

    assert response.status == 204
    assert response.body == b""
    assert world.delivered == []


async def test_every_answer_carries_the_cors_headers(flow) -> None:
    """The observability UI checks endpoints from another origin."""
    handler, _ = flow

    for method in ("POST", "HEAD", "OPTIONS"):
        response = await handler(Request(f"{runtime.FLOW_ROUTE}?__health", method))

        assert response.headers["access-control-allow-origin"] == "*"
        assert response.headers["access-control-allow-methods"] == "POST, OPTIONS, GET, HEAD"
        assert response.headers["access-control-allow-headers"] == "Content-Type"
