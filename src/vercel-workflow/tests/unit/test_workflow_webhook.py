from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Mapping
from dataclasses import dataclass

import pydantic
import pytest

from tests.payloads import PLAIN_ENCODER
from vercel.workflow import (
    BaseHook,
    HTTPResponse,
    WebhookEvent,
    WebhookRequest,
    Workflows,
    get_hook_by_token,
)
from vercel.workflow._internal import runtime, serialization as ser, webhook, world as w
from vercel.workflow._internal.worlds import local as local_mod


class Event(BaseHook, pydantic.BaseModel):
    sequence: int
    kind: str


@dataclass
class DataclassEvent(BaseHook):
    sequence: int


registry = Workflows(as_vercel_job=False, base_url="https://example.com/api/")


@registry.workflow
async def receive_two() -> dict[str, object]:
    hook = Event.wait_webhook(
        metadata={"provider": "test"},
        respond_with=HTTPResponse(201, b"stored", {"x-result": "fixed"}),
    )
    url = await hook.get_url()
    first = await hook
    second = await hook
    hook.dispose()
    return {
        "url": url,
        "first": first.body.sequence,
        "second": second.body.sequence,
        "method": first.method,
        "request_url": first.url,
        "signature": first.headers["x-signature"],
        "raw_body": first.raw_body.decode(),
    }


@registry.workflow
async def receive_one() -> int:
    request = await Event.wait_webhook()
    return request.body.sequence


class RecordingWorld(local_mod.LocalWorld):
    def __init__(self) -> None:
        super().__init__()
        self.queued: list[tuple[str, w.QueuePayload]] = []

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs) -> str:
        self.queued.append((queue_name, message))
        return "msg_test"


class Request:
    def __init__(
        self,
        method: str,
        url: str,
        body: bytes = b"",
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.method = method
        self.url = url
        self.headers = headers or {}
        self._body = body

    async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        yield self._body


class RepeatedHeaders(Mapping[str, str]):
    def __init__(self, pairs: list[tuple[str, str]]) -> None:
        self._pairs = pairs

    def __getitem__(self, key: str) -> str:
        return next(value for name, value in self._pairs if name.lower() == key.lower())

    def __iter__(self) -> Iterator[str]:
        return iter(dict(self._pairs))

    def __len__(self) -> int:
        return len(dict(self._pairs))

    def multi_items(self) -> list[tuple[str, str]]:
        return self._pairs


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)


async def _start(world: RecordingWorld, workflow_name: str) -> str:
    created = await world.events_create(
        None,
        w.RunCreatedEventData(
            deployment_id="",
            workflow_name=workflow_name,
            input=PLAIN_ENCODER.encode([]),
        ).into_event(),
    )
    assert created.run is not None
    await _invoke(created.run.run_id, workflow_name)
    return created.run.run_id


async def _invoke(run_id: str, workflow_name: str) -> None:
    await runtime.workflow_handler(
        w.WorkflowInvokePayload(run_id=run_id).model_dump(by_alias=True),
        attempt=1,
        queue_name=w.get_queue_name(workflow_name),
        message_id="msg_invoke",
        registry=registry,
    )


async def _only_hook(world: RecordingWorld, run_id: str) -> w.Hook:
    events = (await world.events_list(run_id)).data
    created = next(event for event in events if isinstance(event, w.HookCreatedEvent))
    return await world.hooks_get_by_token(created.event_data.token)


def test_public_types_and_base_url_resolution(monkeypatch) -> None:
    monkeypatch.setenv("VERCEL_URL", "preview.example")
    assert Workflows(as_vercel_job=False)._webhook_url("tok") == (
        "https://preview.example/.well-known/workflow/v1/webhook/tok"
    )
    monkeypatch.delenv("VERCEL_URL")
    monkeypatch.setenv("PORT", "4321")
    assert Workflows(as_vercel_job=False)._webhook_url("tok") == (
        "http://localhost:4321/.well-known/workflow/v1/webhook/tok"
    )
    with pytest.raises(ValueError, match="absolute HTTP"):
        Workflows(as_vercel_job=False, base_url="/relative")

    ctx = runtime.WorkflowOrchestratorContext(
        [], run_id="wrun_test", seed="wrun_test", started_at=0, registry=registry
    )
    event = ctx.create_webhook(Event)
    assert isinstance(event, WebhookEvent)
    assert event.token
    assert event._url.startswith("https://example.com/api/.well-known/workflow/v1/webhook/")


async def test_webhook_round_trip_preserves_the_request_and_fixed_response(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = RecordingWorld()
    w.set_world(world)
    run_id = await _start(world, receive_two.workflow_id)
    hook = await _only_hook(world, run_id)
    assert hook.is_webhook is True
    assert (await get_hook_by_token(hook.token)).metadata == {"provider": "test"}

    target = f"/api/.well-known/workflow/v1/webhook/{hook.token}?source=unit"
    headers = RepeatedHeaders([("X-Signature", "one"), ("X-Repeated", "a"), ("X-Repeated", "b")])
    first_body = b'{"sequence":1,"kind":"created"}'
    response = await registry.webhook_handler(Request("POST", target, first_body, headers))
    assert response == HTTPResponse(201, b"stored", {"x-result": "fixed"})
    response = await registry.webhook_handler(
        Request("PATCH", target, b'{"sequence":2,"kind":"finished"}')
    )
    assert response.status == 201

    await _invoke(run_id, receive_two.workflow_id)
    assert await runtime.Run(run_id).return_value() == {
        "url": f"https://example.com/api/.well-known/workflow/v1/webhook/{hook.token}",
        "first": 1,
        "second": 2,
        "method": "POST",
        "request_url": f"https://example.com{target}",
        "signature": "one",
        "raw_body": first_body.decode(),
    }
    assert (await registry.webhook_handler(Request("POST", target))).status == 404


@pytest.mark.parametrize("method", ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])
async def test_all_supported_methods_are_recorded(method, tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = RecordingWorld()
    w.set_world(world)
    run_id = await _start(world, receive_one.workflow_id)
    hook = await _only_hook(world, run_id)
    target = f"/api/.well-known/workflow/v1/webhook/{hook.token}"

    response = await registry.webhook_handler(
        Request(method, target, b'{"sequence":7,"kind":"method"}')
    )

    assert response == HTTPResponse(202, b"", {})
    received = next(
        event
        for event in (await world.events_list(run_id)).data
        if isinstance(event, w.HookReceivedEvent)
    )
    value = ser.hydrate(received.event_data.payload, what="the webhook request")
    request = webhook.decode_request(value, Event)
    assert isinstance(request, WebhookRequest)
    assert request.method == method
    assert request.body.sequence == 7


def test_dataclass_bodies_and_invalid_json_are_decoded_when_consumed() -> None:
    encoded = webhook.encode_request(
        method="POST",
        url="https://example.com/hook",
        headers=[("X-One", "1"), ("X-One", "2")],
        raw_body=b'{"sequence":9}',
    )
    request = webhook.decode_request(encoded, DataclassEvent)
    assert request.body == DataclassEvent(sequence=9)
    assert request.headers.get_list("x-one") == ["1", "2"]

    encoded["rawBody"] = b"not json"
    with pytest.raises(ValueError):
        webhook.decode_request(encoded, DataclassEvent)


async def test_missing_and_regular_hook_tokens_are_both_not_found(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = RecordingWorld()
    w.set_world(world)
    created = await world.events_create(
        None,
        w.RunCreatedEventData(
            deployment_id="", workflow_name="wf", input=PLAIN_ENCODER.encode([])
        ).into_event(),
    )
    assert created.run is not None
    await world.events_create(
        created.run.run_id,
        w.HookCreatedEventData(token="regular").into_event("hook_regular"),
    )

    for token in ("regular", "missing"):
        response = await registry.webhook_handler(
            Request("POST", f"/api/.well-known/workflow/v1/webhook/{token}", b"{}")
        )
        assert response == HTTPResponse(404, b"", {})


async def test_a_failed_durable_write_returns_500_without_waking_the_run(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = RecordingWorld()
    w.set_world(world)
    run_id = await _start(world, receive_one.workflow_id)
    hook = await _only_hook(world, run_id)
    queued_before = len(world.queued)

    async def fail_write(*args, **kwargs):
        raise RuntimeError("storage unavailable")

    monkeypatch.setattr(world, "events_create", fail_write)
    response = await registry.webhook_handler(
        Request(
            "POST",
            f"/api/.well-known/workflow/v1/webhook/{hook.token}",
            b'{"sequence":1,"kind":"failed"}',
        )
    )

    assert response == HTTPResponse(500, b"", {})
    assert len(world.queued) == queued_before
