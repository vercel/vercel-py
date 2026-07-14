from __future__ import annotations

from typing import Any, cast

import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timedelta

import anyio
import httpx
import pytest
from pydantic import BaseModel

from vercel.queue import (
    ALL_DEPLOYMENTS,
    BadRequestError,
    CommunicationError,
    ConsumerDiscoveryError,
    ConsumerRegistryNotConfiguredError,
    Delivery,
    DuplicateIdempotencyKeyError,
    MessageNotFoundError,
    QueueClient,
    Topic,
)
from vercel.queue._internal.client import USER_AGENT
from vercel.queue._internal.constants import (
    HEADER_USER_AGENT,
    VQS_HEADER_CLIENT_TS,
)
from vercel.queue.devserver import EmbeddedQueueDevServer
from vercel.queue.sync import QueueClient as SyncQueueClient
from vercel.queue.testing import reset_default_async_queue_clients

from .helpers import make_leased_metadata


def _queue_debug_events(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    return [
        json.loads(record.message) for record in caplog.records if record.name == "vercel.queue"
    ]


def _sync_client(**kwargs: Any) -> SyncQueueClient:
    return SyncQueueClient(**kwargs)


def _async_client(**kwargs: Any) -> QueueClient:
    return QueueClient(**kwargs)


def test_send_uses_v3_endpoint_and_filters_protected_headers(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = _sync_client(
        token="token",
        base_url=eqs.base_url,
        deployment=ALL_DEPLOYMENTS,
        headers={"Authorization": "Bearer bad", "x-user": "ok"},
    )
    result = client.send(
        Topic[dict[str, str]]("emails"),
        {"subject": "hi"},
        idempotency_key="idem_1",
        delay=2,
        deployment="dpl_1",
        headers={"Content-Type": "text/plain", "Vqs-Delay-Seconds": "999"},
    )

    assert result == "msg_1"
    request = eqs.state.requests[-1]
    assert request.method == "POST"
    assert request.path == "/api/v3/topic/emails"
    assert request.headers["Authorization"] == "Bearer token"
    assert request.headers["Content-Type"] == "application/json"
    assert request.headers["Vqs-Delay-Seconds"] == "2"
    assert request.headers["Vqs-Deployment-Id"] == "dpl_1"
    assert request.headers["Vqs-Idempotency-Key"] == "idem_1"
    assert request.headers[HEADER_USER_AGENT] == USER_AGENT
    assert datetime.fromisoformat(request.headers[VQS_HEADER_CLIENT_TS]).tzinfo is not None
    assert request.headers["x-user"] == "ok"
    assert json.loads(request.body) == {"subject": "hi"}


def test_send_with_dev_token_still_pins_explicit_deployment(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = _sync_client(
        token="vc-dev-token",
        base_url=eqs.base_url,
        deployment="dpl_1",
    )

    message_id = client.send("emails", {"subject": "hi"})

    assert message_id == "msg_1"
    request = eqs.state.requests[-1]
    assert request.headers["Authorization"] == "Bearer vc-dev-token"
    assert request.headers["Vqs-Deployment-Id"] == "dpl_1"
    assert eqs.state.by_id[message_id].deployment == "dpl_1"


def test_send_infers_pydantic_model_json_transport(
    eqs: EmbeddedQueueDevServer,
) -> None:
    class Payload(BaseModel):
        count: int

    client = _sync_client(token="token", base_url=eqs.base_url, deployment=ALL_DEPLOYMENTS)

    client.send("models", Payload(count=3))

    request = eqs.state.requests[-1]
    assert request.headers["Content-Type"] == "application/json"
    assert request.body == b'{"count":3}'


@pytest.mark.parametrize("payload", [b"raw", bytearray(b"raw"), memoryview(b"raw")])
def test_send_infers_byte_buffer_transport(
    eqs: EmbeddedQueueDevServer,
    payload: bytes | bytearray | memoryview,
) -> None:
    client = _sync_client(token="token", base_url=eqs.base_url, deployment=ALL_DEPLOYMENTS)

    client.send("bytes", payload)

    request = eqs.state.requests[-1]
    assert request.headers["Content-Type"] == "application/octet-stream"
    assert request.body == b"raw"


def test_send_infers_text_buffer_transport(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = _sync_client(token="token", base_url=eqs.base_url, deployment=ALL_DEPLOYMENTS)

    client.send("text", "caf\u00e9")

    request = eqs.state.requests[-1]
    assert request.headers["Content-Type"] == "text/plain; charset=utf-8"
    assert request.body == "caf\u00e9".encode()


def test_send_explicit_transport_overrides_inference(
    eqs: EmbeddedQueueDevServer,
) -> None:
    class CustomTransport:
        content_type = "application/x-custom"

        def serialize(self, value: str) -> bytes:
            return f"custom:{value}".encode()

        async def deserialize(
            self,
            payload: AsyncIterator[bytes],
            *,
            content_type: str,
        ) -> str:
            del payload, content_type
            return ""

    client = _sync_client(token="token", base_url=eqs.base_url, deployment=ALL_DEPLOYMENTS)

    client.send(Topic[str]("custom", transport=CustomTransport()), "raw")

    request = eqs.state.requests[-1]
    assert request.headers["Content-Type"] == "application/x-custom"
    assert request.body == b"custom:raw"


def test_base_url_path_prefix_is_preserved_for_queue_api(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = _sync_client(
        token="token",
        base_url=f"{eqs.base_url}/queues/iad1/",
        deployment=ALL_DEPLOYMENTS,
    )

    client.send("emails", {"ok": True})

    assert eqs.state.requests[-1].path == "/queues/iad1/api/v3/topic/emails"


def test_string_base_url_template_resolves_region() -> None:
    client = _sync_client(
        token="token",
        region="sfo1",
        base_url="https://proxy.example/queues/{region}/",
        deployment=ALL_DEPLOYMENTS,
    )
    assert client.base_url == "https://proxy.example/queues/sfo1"


def test_callable_base_url_resolves_region() -> None:
    client = _sync_client(
        token="token",
        region="fra1",
        base_url=lambda region: f"https://{region}.queue.test/root/",
        deployment=ALL_DEPLOYMENTS,
    )
    assert client.base_url == "https://fra1.queue.test/root"


@pytest.mark.anyio
async def test_runtime_http_resolves_root_relative_paths_against_base_url_prefix(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = _async_client(
        token="token",
        base_url=f"{eqs.base_url}/raw-prefix",
        deployment=ALL_DEPLOYMENTS,
    )
    response = await client.http.post(
        "/api/v3/topic/emails",
        headers={"Authorization": "Bearer token"},
        json={"ok": True},
    )

    assert response.status_code == 201
    assert eqs.state.requests[-1].path == "/raw-prefix/api/v3/topic/emails"


@pytest.mark.anyio
async def test_runtime_http_preserves_absolute_urls(
    eqs: EmbeddedQueueDevServer,
) -> None:
    client = _async_client(
        token="token",
        base_url="http://vqs.test/raw-prefix",
        deployment=ALL_DEPLOYMENTS,
    )
    response = await client.http.post(
        f"{eqs.base_url}/api/v3/topic/emails",
        headers={"Authorization": "Bearer token"},
        json={"ok": True},
    )

    assert response.status_code == 201
    assert eqs.state.requests[-1].path == "/api/v3/topic/emails"


@pytest.mark.anyio
async def test_runtime_http_rejects_bare_relative_urls() -> None:
    client = _async_client(token="token", region="iad1", deployment=ALL_DEPLOYMENTS)
    with pytest.raises(ValueError, match="absolute or root-relative"):
        await client.http.get("emails", headers={})


@pytest.mark.parametrize("debug_value", [None, "false"])
def test_queue_debug_logs_are_disabled_by_default(
    eqs: EmbeddedQueueDevServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    debug_value: str | None,
) -> None:
    if debug_value is None:
        monkeypatch.delenv("VERCEL_QUEUE_DEBUG", raising=False)
    else:
        monkeypatch.setenv("VERCEL_QUEUE_DEBUG", debug_value)
    caplog.set_level(logging.INFO, logger="vercel.queue")

    eqs.get_sync_client(token="token", deployment=ALL_DEPLOYMENTS).send(
        "emails",
        {"secret": "payload-body"},
    )

    assert _queue_debug_events(caplog) == []


@pytest.mark.parametrize("debug_value", ["1", "true"])
def test_queue_debug_logs_sync_http_request_response_and_redacts_values(
    eqs: EmbeddedQueueDevServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    debug_value: str,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", debug_value)
    caplog.set_level(logging.INFO, logger="vercel.queue")

    client = eqs.get_sync_client(
        token="secret-token",
        deployment=ALL_DEPLOYMENTS,
        headers={"x-custom": "custom-secret"},
    )
    client.send("emails", {"secret": "payload-body"})
    deliveries: list[Delivery[Any]] = list(client.poll("emails", "test-group"))
    [delivery] = deliveries
    message = delivery.message
    client.extend_lease(message, 30)
    client.acknowledge(message)

    text = caplog.text
    assert "secret-token" not in text
    assert "Bearer secret-token" not in text
    assert "custom-secret" not in text
    assert "payload-body" not in text
    assert message.metadata.receipt_handle is not None

    events = _queue_debug_events(caplog)
    assert all(
        record.levelno == logging.INFO for record in caplog.records if record.name == "vercel.queue"
    )
    assert {event["event"] for event in events} >= {"http.request", "http.response"}
    request_events = [event for event in events if event["event"] == "http.request"]
    assert any(event["kind"] == "request" for event in request_events)
    assert any(event["kind"] == "stream" for event in request_events)
    assert any("x-custom" in cast("list[str]", event["headers"]) for event in request_events)
    assert all(
        "Authorization" not in cast("list[str]", event["headers"]) for event in request_events
    )
    assert all(message.metadata.receipt_handle not in str(event) for event in request_events)
    assert all(message.metadata.receipt_handle not in str(event) for event in events)


def test_queue_client_suppresses_httpx_request_logs_with_unredacted_urls(
    eqs: EmbeddedQueueDevServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level(logging.INFO)
    client = eqs.get_sync_client(
        token="token",
        deployment=ALL_DEPLOYMENTS,
    )

    client.send("emails", {"ok": True})
    deliveries: list[Delivery[Any]] = list(client.poll("emails", "test-group"))
    [delivery] = deliveries
    message = delivery.message
    client.acknowledge(message)

    assert not any(record.name == "httpx" for record in caplog.records)
    assert message.metadata.receipt_handle is not None
    events = _queue_debug_events(caplog)
    assert all(message.metadata.receipt_handle not in str(event) for event in events)
    assert "rh_1_REDACTED" in caplog.text


@pytest.mark.anyio
async def test_queue_debug_logs_async_http_request_response(
    eqs: EmbeddedQueueDevServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level(logging.INFO, logger="vercel.queue")

    client = eqs.get_async_client(
        token="secret-token",
        base_url=eqs.base_url,
    )
    await client.send("emails", {"ok": True})
    stream: AsyncIterator[Delivery[Any]] = client.poll("emails", "test-group")
    deliveries: list[Delivery[Any]] = [item async for item in stream]
    [delivery] = deliveries
    message = delivery.message
    await client.extend_lease(message, 30)
    await client.acknowledge(message)

    events = _queue_debug_events(caplog)
    assert any(event["event"] == "http.request" and event["method"] == "POST" for event in events)
    assert any(event["event"] == "http.request" and event["method"] == "PATCH" for event in events)
    assert any(event["event"] == "http.request" and event["method"] == "DELETE" for event in events)
    assert any(
        event["event"] == "http.response" and event["status_code"] == 204 for event in events
    )


def test_send_maps_duplicate_error(
    eqs: EmbeddedQueueDevServer,
) -> None:
    eqs.app._server.respond_once(
        method="POST",
        action="topic",
        status_code=409,
        body=b"duplicate",
    )
    with pytest.raises(DuplicateIdempotencyKeyError):
        eqs.get_sync_client(token="token", deployment=ALL_DEPLOYMENTS).send(
            "emails",
            {"ok": True},
        )


def test_debug_logs_deferred_send(
    eqs: EmbeddedQueueDevServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    caplog.set_level(logging.INFO, logger="vercel.queue")
    eqs.app._server.respond_once(
        method="POST",
        action="topic",
        status_code=202,
    )

    with pytest.warns(UserWarning, match="202 Accepted"):
        result = eqs.get_sync_client(
            token="token",
            deployment=ALL_DEPLOYMENTS,
        ).send("emails", {"ok": True})

    assert result is None
    assert any(
        event["event"] == "send.deferred"
        and event["topic"] == "emails"
        and event["status_code"] == 202
        for event in _queue_debug_events(caplog)
    )


@pytest.mark.parametrize(
    ("status", "error_type"),
    [
        (502, ConsumerDiscoveryError),
        (503, ConsumerRegistryNotConfiguredError),
    ],
)
def test_send_maps_protocol_server_errors(
    eqs: EmbeddedQueueDevServer,
    status: int,
    error_type: type[Exception],
) -> None:
    eqs.app._server.respond_once(
        method="POST",
        action="topic",
        status_code=status,
    )

    with pytest.raises(error_type):
        eqs.get_sync_client(token="token", deployment=ALL_DEPLOYMENTS).send(
            "emails",
            {"ok": True},
        )


def test_queue_client_reuses_http_client(
    eqs: EmbeddedQueueDevServer,
) -> None:
    created = 0

    def client_factory(**kwargs: Any) -> httpx.Client:
        nonlocal created
        created += 1
        return httpx.Client(**kwargs)

    client = _sync_client(
        token="token",
        base_url=eqs.base_url,
        deployment=ALL_DEPLOYMENTS,
        http_client_factory=client_factory,
    )
    client.send("emails", {"ok": True})
    client.send("emails", {"ok": False})

    assert created == 1
    assert len(eqs.state.messages) == 2


def test_queue_client_accepts_http_client_factory(
    eqs: EmbeddedQueueDevServer,
) -> None:
    factory_kwargs: list[dict[str, Any]] = []
    http_client = httpx.Client()

    def client_factory(**kwargs: Any) -> httpx.Client:
        factory_kwargs.append(kwargs)
        return http_client

    client = _sync_client(
        token="token",
        base_url=eqs.base_url,
        deployment=ALL_DEPLOYMENTS,
        timeout=2.5,
        http_client_factory=client_factory,
    )
    result = client.send("emails", {"ok": True})

    assert result == "msg_1"
    assert factory_kwargs == [{}]
    assert eqs.state.by_id["msg_1"].topic == "emails"
    assert not http_client.is_closed


def test_module_send_reuses_process_default_client(
    embedded_queue_module_env: EmbeddedQueueDevServer,
) -> None:
    created = 0

    def client_factory(**kwargs: Any) -> httpx.Client:
        nonlocal created
        created += 1
        return httpx.Client(**kwargs)

    client = SyncQueueClient(http_client_factory=client_factory)
    client.send("emails", {"ok": True}, deployment=ALL_DEPLOYMENTS)
    client.send("emails", {"ok": False}, deployment=ALL_DEPLOYMENTS)

    assert created == 1
    assert len(embedded_queue_module_env.state.messages) == 2


def test_module_send_reuses_process_default_client_across_contexts(
    embedded_queue_module_env: EmbeddedQueueDevServer,
) -> None:
    created = 0

    def client_factory(**kwargs: Any) -> httpx.Client:
        nonlocal created
        created += 1
        return httpx.Client(**kwargs)

    client = SyncQueueClient(http_client_factory=client_factory)
    client.send("emails", {"ok": True}, deployment=ALL_DEPLOYMENTS)
    client.send("emails", {"ok": False}, deployment=ALL_DEPLOYMENTS)

    assert created == 1
    assert len(embedded_queue_module_env.state.messages) == 2


def test_module_poll_acknowledge_extend_lease_reuse_process_default_client(
    embedded_queue_module_env: EmbeddedQueueDevServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    embedded_queue_module_env.get_sync_client(token="token", deployment="dpl_1").send(
        "emails",
        {"ok": True},
    )
    created = 0
    monkeypatch.setenv("VERCEL_DEPLOYMENT_ID", "dpl_1")

    def client_factory(**kwargs: Any) -> httpx.Client:
        nonlocal created
        created += 1
        return httpx.Client(**kwargs)

    client = SyncQueueClient(http_client_factory=client_factory)
    deliveries: list[Delivery[Any]] = list(client.poll("emails", "test-group", limit=1))
    client.extend_lease(deliveries[0].message, timedelta(seconds=30))
    client.acknowledge(deliveries[0].message)

    assert created == 1
    assert embedded_queue_module_env.state.by_id["msg_1"].acknowledged


def _mock_transport_sync_client(
    handler: Any,
    **kwargs: Any,
) -> SyncQueueClient:
    def client_factory(**client_kwargs: Any) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(handler), **client_kwargs)

    return _sync_client(
        token="token",
        region="iad1",
        base_url="http://queue.test",
        deployment=ALL_DEPLOYMENTS,
        http_client_factory=client_factory,
        **kwargs,
    )


def test_sync_acknowledge_retries_transport_errors() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if len(calls) < 3:
            raise httpx.ConnectError("connection reset", request=request)
        return httpx.Response(204)

    client = _mock_transport_sync_client(handler)
    client.acknowledge(make_leased_metadata("emails"))

    assert calls == ["DELETE", "DELETE", "DELETE"]


def test_sync_acknowledge_gives_up_after_transport_error_retries() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        raise httpx.ConnectError("connection reset", request=request)

    client = _mock_transport_sync_client(handler)
    with pytest.raises(CommunicationError) as exc_info:
        client.acknowledge(make_leased_metadata("emails"))

    assert calls == ["DELETE", "DELETE", "DELETE"]
    # The wrapped error keeps the underlying httpx failure as its cause but
    # does not require callers to depend on httpx.
    assert isinstance(exc_info.value.__cause__, httpx.ConnectError)
    assert isinstance(exc_info.value, ConnectionError)


def test_sync_acknowledge_tolerates_missing_lease() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        return httpx.Response(404)

    client = _mock_transport_sync_client(handler)
    client.acknowledge(make_leased_metadata("emails"))

    assert calls == ["DELETE"]


def test_sync_extend_lease_retries_transport_errors() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if len(calls) < 2:
            raise httpx.ConnectError("connection reset", request=request)
        return httpx.Response(200)

    client = _mock_transport_sync_client(handler)
    client.extend_lease(make_leased_metadata("emails"), 30)

    assert calls == ["PATCH", "PATCH"]


def test_sync_extend_lease_still_raises_for_missing_lease() -> None:
    client = _mock_transport_sync_client(lambda request: httpx.Response(404))
    with pytest.raises(MessageNotFoundError):
        client.extend_lease(make_leased_metadata("emails"), 30)


def test_sync_retry_after_tolerates_missing_lease_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _mock_transport_sync_client(lambda request: httpx.Response(404))
    with caplog.at_level(logging.WARNING, logger="vercel.queue._internal.lease"):
        client.retry_after(make_leased_metadata("emails"), 5)

    assert any(
        record.levelno == logging.WARNING and "no longer exists" in record.message
        for record in caplog.records
    )


def test_sync_retry_after_raises_other_client_errors() -> None:
    client = _mock_transport_sync_client(lambda request: httpx.Response(400, text="bad"))
    with pytest.raises(BadRequestError):
        client.retry_after(make_leased_metadata("emails"), 5)


@pytest.mark.anyio
async def test_async_acknowledge_retries_transport_errors() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if len(calls) < 3:
            raise httpx.ConnectError("connection reset", request=request)
        return httpx.Response(204)

    def client_factory(**client_kwargs: Any) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(handler), **client_kwargs)

    client = _async_client(
        token="token",
        region="iad1",
        base_url="http://queue.test",
        deployment=ALL_DEPLOYMENTS,
        http_client_factory=client_factory,
    )
    await client.acknowledge(make_leased_metadata("emails"))

    assert calls == ["DELETE", "DELETE", "DELETE"]


@pytest.mark.anyio
async def test_async_retry_after_tolerates_missing_lease_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def client_factory(**client_kwargs: Any) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(404)),
            **client_kwargs,
        )

    client = _async_client(
        token="token",
        region="iad1",
        base_url="http://queue.test",
        deployment=ALL_DEPLOYMENTS,
        http_client_factory=client_factory,
    )
    with caplog.at_level(logging.WARNING, logger="vercel.queue._internal.lease"):
        await client.retry_after(make_leased_metadata("emails"), 5)

    assert any(
        record.levelno == logging.WARNING and "no longer exists" in record.message
        for record in caplog.records
    )


@pytest.mark.anyio
async def test_async_send_maps_duplicate_error(
    eqs: EmbeddedQueueDevServer,
) -> None:
    eqs.app._server.respond_once(
        method="POST",
        action="topic",
        status_code=409,
        body=b"duplicate",
    )
    with pytest.raises(DuplicateIdempotencyKeyError):
        await eqs.get_async_client(token="token", deployment=ALL_DEPLOYMENTS).send(
            "emails",
            {"ok": True},
        )


@pytest.mark.anyio
async def test_async_queue_client_reuses_http_client(
    eqs: EmbeddedQueueDevServer,
) -> None:
    created = 0

    def client_factory(**kwargs: Any) -> httpx.AsyncClient:
        nonlocal created
        created += 1
        return httpx.AsyncClient(**kwargs)

    client = _async_client(
        token="token",
        base_url=eqs.base_url,
        deployment=ALL_DEPLOYMENTS,
        http_client_factory=client_factory,
    )
    await client.send("emails", {"ok": True})
    await client.send("emails", {"ok": False})

    assert created == 1
    assert len(eqs.state.messages) == 2


@pytest.mark.anyio
async def test_async_queue_client_accepts_http_client_factory(
    eqs: EmbeddedQueueDevServer,
) -> None:
    factory_kwargs: list[dict[str, Any]] = []
    http_client = httpx.AsyncClient()

    def client_factory(**kwargs: Any) -> httpx.AsyncClient:
        factory_kwargs.append(kwargs)
        return http_client

    client = _async_client(
        token="token",
        base_url=eqs.base_url,
        deployment=ALL_DEPLOYMENTS,
        timeout=2.5,
        http_client_factory=client_factory,
    )
    result = await client.send("emails", {"ok": True})

    assert result == "msg_1"
    assert factory_kwargs == [{}]
    assert eqs.state.by_id["msg_1"].topic == "emails"
    assert not http_client.is_closed


def test_async_queue_client_defers_http_client_factory_until_request() -> None:
    created = 0

    def client_factory(**kwargs: Any) -> httpx.AsyncClient:
        del kwargs
        nonlocal created
        created += 1
        return httpx.AsyncClient()

    _async_client(
        token="token",
        deployment=ALL_DEPLOYMENTS,
        http_client_factory=client_factory,
    )
    assert created == 0


@pytest.mark.anyio
async def test_async_clients_share_loop_http_client_pool(
    embedded_queue_module_env: EmbeddedQueueDevServer,
) -> None:
    created = 0

    def client_factory(**kwargs: Any) -> httpx.AsyncClient:
        nonlocal created
        created += 1
        return httpx.AsyncClient(**kwargs)

    try:
        client = QueueClient(http_client_factory=client_factory)
        await client.send("emails", {"ok": True}, deployment=ALL_DEPLOYMENTS)
        await client.send("emails", {"ok": False}, deployment=ALL_DEPLOYMENTS)
    finally:
        await reset_default_async_queue_clients()

    assert created == 1
    assert len(embedded_queue_module_env.state.messages) == 2


@pytest.mark.anyio
async def test_async_clients_share_loop_http_client_pool_across_tasks(
    embedded_queue_module_env: EmbeddedQueueDevServer,
) -> None:
    created = 0

    def client_factory(**kwargs: Any) -> httpx.AsyncClient:
        nonlocal created
        created += 1
        return httpx.AsyncClient(**kwargs)

    async def send_one(payload: dict[str, int]) -> None:
        client = QueueClient(http_client_factory=client_factory)
        await client.send("emails", payload, deployment=ALL_DEPLOYMENTS)

    try:
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(send_one, {"index": 1})
            task_group.start_soon(send_one, {"index": 2})
    finally:
        await reset_default_async_queue_clients()

    assert created == 1
    assert len(embedded_queue_module_env.state.messages) == 2


def test_async_clients_use_separate_http_client_pool_per_anyio_run(
    embedded_queue_module_env: EmbeddedQueueDevServer,
    anyio_backend: str,
) -> None:
    created = 0

    def client_factory(**kwargs: Any) -> httpx.AsyncClient:
        nonlocal created
        created += 1
        return httpx.AsyncClient(**kwargs)

    async def send_one(payload: dict[str, int]) -> None:
        try:
            client = QueueClient(http_client_factory=client_factory)
            await client.send("emails", payload, deployment=ALL_DEPLOYMENTS)
        finally:
            await reset_default_async_queue_clients()

    anyio.run(send_one, {"index": 1}, backend=anyio_backend)
    anyio.run(send_one, {"index": 2}, backend=anyio_backend)

    assert created == 2
    assert len(embedded_queue_module_env.state.messages) == 2


def test_queue_clients_do_not_expose_lifecycle_apis() -> None:
    sync_client = SyncQueueClient(token="token", deployment=ALL_DEPLOYMENTS)
    async_client = QueueClient(token="token", deployment=ALL_DEPLOYMENTS)

    assert not hasattr(sync_client, "close")
    assert not hasattr(sync_client, "closed")
    assert not hasattr(sync_client, "__enter__")
    assert not hasattr(async_client, "close")
    assert not hasattr(async_client, "closed")
    assert not hasattr(async_client, "__aenter__")
