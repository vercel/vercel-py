from __future__ import annotations

from typing import cast

import contextlib
import logging
from collections.abc import AsyncIterable, Iterator
from dataclasses import dataclass

import httpx
import pytest

import vercel.queue._internal.devserver as queue_devserver_internal
import vercel.queue.devserver as queue_devserver
from vercel.headers import get_headers, set_headers
from vercel.queue import ALL_DEPLOYMENTS, QueueClient, QueueClientAsgiApp, asgi_app, subscribe
from vercel.queue._internal.asgi import AsgiMessage
from vercel.queue._internal.constants import (
    CLOUD_EVENT_HEADER_VQS_MESSAGE_ID,
    CLOUD_EVENT_HEADER_VQS_TOPIC,
)
from vercel.queue._internal.errors import ProtocolError
from vercel.queue.devserver import EmbeddedQueueDevServer, queue_client_asgi_dev_server

from .helpers import callback_headers


def test_devserver_exports_expected_symbols() -> None:
    assert queue_devserver.EmbeddedQueueDevServer is EmbeddedQueueDevServer
    assert queue_devserver.queue_client_asgi_dev_server is queue_client_asgi_dev_server
    assert callable(queue_devserver.embedded_queue_dev_server)


def test_devserver_main_defaults_to_random_port(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[dict[str, object]] = []

    @dataclass(frozen=True)
    class _Server:
        base_url: str = "http://127.0.0.1:54321"

        def is_running(self) -> bool:
            return False

    @contextlib.contextmanager
    def _server(**kwargs: object) -> Iterator[_Server]:
        calls.append(kwargs)
        yield _Server()

    monkeypatch.setattr(queue_devserver_internal, "embedded_queue_dev_server", _server)

    with pytest.raises(RuntimeError, match="stopped unexpectedly"):
        queue_devserver.main(["--host", "127.0.0.1"])

    assert calls == [
        {
            "host": "127.0.0.1",
            "port": 0,
            "manual_clock": False,
        }
    ]
    assert capsys.readouterr().out == '{"baseUrl": "http://127.0.0.1:54321"}\n'


class _FakeClient:
    def __init__(self, *, exc: BaseException | None = None) -> None:
        self.exc = exc
        self.calls: list[tuple[AsyncIterable[bytes], dict[str, str]]] = []

    async def accept_and_handle(
        self,
        raw_body: AsyncIterable[bytes],
        headers: dict[str, str],
    ) -> None:
        self.calls.append((raw_body, headers))
        if self.exc is not None:
            raise self.exc
        async for _chunk in raw_body:
            pass


@pytest.fixture(autouse=True)
def restore_queue_logger_level() -> Iterator[None]:
    logger = logging.getLogger("vercel.queue")
    original_level = logger.level
    try:
        yield
    finally:
        logger.setLevel(original_level)


async def _call_app(
    app: QueueClientAsgiApp,
    *,
    method: str = "POST",
    body_chunks: list[bytes] | None = None,
    headers: dict[str, str] | None = None,
) -> list[AsgiMessage]:
    chunks = body_chunks if body_chunks is not None else [b'{"ok": true}']
    messages = [
        {
            "type": "http.request",
            "body": chunk,
            "more_body": index < len(chunks) - 1,
        }
        for index, chunk in enumerate(chunks)
    ]
    sent: list[AsgiMessage] = []

    async def receive() -> AsgiMessage:
        return messages.pop(0)

    async def send(message: AsgiMessage) -> None:
        sent.append(message)

    await app(
        {
            "type": "http",
            "method": method,
            "headers": [
                (name.encode("latin-1"), value.encode("latin-1"))
                for name, value in (headers or callback_headers()).items()
            ],
        },
        receive,
        send,
    )
    return sent


async def _run_lifespan(app: QueueClientAsgiApp) -> list[AsgiMessage]:
    messages = [
        {"type": "lifespan.startup"},
        {"type": "lifespan.shutdown"},
    ]
    sent: list[AsgiMessage] = []

    async def receive() -> AsgiMessage:
        return messages.pop(0)

    async def send(message: AsgiMessage) -> None:
        sent.append(message)

    await app({"type": "lifespan"}, receive, send)
    return sent


@pytest.mark.anyio
async def test_asgi_app_calls_accept_and_handle_and_returns_no_content() -> None:
    client = _FakeClient()
    app = QueueClientAsgiApp(cast("QueueClient", client))

    sent = await _call_app(app)

    assert sent[0]["status"] == 204
    assert len(client.calls) == 1
    assert client.calls[0][1][CLOUD_EVENT_HEADER_VQS_TOPIC] == "emails"


@pytest.mark.anyio
async def test_asgi_app_rejects_non_post() -> None:
    client = _FakeClient()
    app = QueueClientAsgiApp(cast("QueueClient", client))

    sent = await _call_app(app, method="GET")

    assert sent[0]["status"] == 405
    assert sent[0]["headers"] == [(b"allow", b"POST")]
    assert client.calls == []


@pytest.mark.anyio
async def test_asgi_app_returns_bad_request_for_invalid_push_metadata() -> None:
    client = _FakeClient(exc=ProtocolError("bad push metadata"))
    app = QueueClientAsgiApp(cast("QueueClient", client))

    sent = await _call_app(app)

    assert sent[0]["status"] == 400


@pytest.mark.anyio
async def test_asgi_app_logs_bad_request_for_invalid_push_metadata(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _FakeClient(exc=ProtocolError("bad push metadata"))
    app = QueueClientAsgiApp(cast("QueueClient", client))

    with caplog.at_level("WARNING", logger="vercel.queue"):
        sent = await _call_app(app)

    assert sent[0]["status"] == 400
    assert "Vercel Queue push callback rejected: bad push metadata" in caplog.text


@pytest.mark.anyio
async def test_asgi_app_returns_server_error_for_handler_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _FakeClient(exc=RuntimeError("handler failed"))
    app = QueueClientAsgiApp(cast("QueueClient", client))

    with caplog.at_level("ERROR", logger="vercel.queue"):
        sent = await _call_app(app)

    assert sent[0]["status"] == 500
    assert "Vercel Queue push callback failed" in caplog.text
    assert "RuntimeError: handler failed" in caplog.text


@pytest.mark.anyio
async def test_asgi_app_passes_request_body_as_async_stream() -> None:
    seen_chunks: list[bytes] = []

    class Client(_FakeClient):
        async def accept_and_handle(
            self,
            raw_body: AsyncIterable[bytes],
            headers: dict[str, str],
        ) -> None:
            self.calls.append((raw_body, headers))
            seen_chunks.extend([chunk async for chunk in raw_body])

    client = Client()
    app = QueueClientAsgiApp(cast("QueueClient", client))

    sent = await _call_app(app, body_chunks=[b'{"ok"', b": true}"])

    assert sent[0]["status"] == 204
    assert seen_chunks == [b'{"ok"', b": true}"]


@pytest.mark.anyio
async def test_asgi_app_installs_request_headers_for_delivery_context() -> None:
    seen_headers: list[dict[str, str]] = []
    set_headers({"x-existing": "outer"})

    class Client(_FakeClient):
        async def accept_and_handle(
            self,
            raw_body: AsyncIterable[bytes],
            headers: dict[str, str],
        ) -> None:
            self.calls.append((raw_body, headers))
            seen_headers.append(dict(get_headers() or {}))
            async for _chunk in raw_body:
                pass

    client = Client()
    app = QueueClientAsgiApp(cast("QueueClient", client))

    sent = await _call_app(
        app,
        headers={
            **callback_headers(),
            "x-vercel-oidc-token": "push-token",
        },
    )

    assert sent[0]["status"] == 204
    assert seen_headers[0]["x-vercel-oidc-token"] == "push-token"
    assert get_headers() == {"x-existing": "outer"}


@pytest.mark.anyio
async def test_asgi_lifespan_shutdown_completes_without_closing_client() -> None:
    client = _FakeClient()

    events = await _run_lifespan(QueueClientAsgiApp(cast("QueueClient", client)))

    assert [event["type"] for event in events] == [
        "lifespan.startup.complete",
        "lifespan.shutdown.complete",
    ]


@pytest.mark.anyio
async def test_queue_client_asgi_dev_server_handles_push_callback(
    eqs: EmbeddedQueueDevServer,
    isolated_subscriptions: None,
) -> None:
    handled: list[dict[str, bool]] = []

    @subscribe(topic="emails", consumer_group="tests")
    async def handle(payload: dict[str, bool]) -> None:
        handled.append(payload)

    client = QueueClient(token="token", base_url=eqs.base_url, deployment=ALL_DEPLOYMENTS)
    with queue_client_asgi_dev_server(client=client) as server:
        assert server.is_running()
        eqs.get_sync_client().send("emails", {"ok": True})
        delivery = next(eqs.iter_push_deliveries("emails", "tests"))

        async with httpx.AsyncClient(timeout=5) as http_client:
            response = await http_client.post(
                server.base_url,
                content=delivery.body,
                headers=delivery.headers,
            )

        assert response.status_code == 204
        assert handled == [{"ok": True}]
        assert eqs.state.by_id[delivery.headers[CLOUD_EVENT_HEADER_VQS_MESSAGE_ID]].acknowledged


def test_asgi_app_helper_creates_owned_client() -> None:
    app = asgi_app(token="token")

    assert isinstance(app, QueueClientAsgiApp)


def test_asgi_app_configures_queue_logger_warning_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = logging.getLogger("vercel.queue")
    original_level = logger.level
    monkeypatch.delenv("VERCEL_QUEUE_DEBUG", raising=False)
    try:
        app = asgi_app(token="token")

        assert isinstance(app, QueueClientAsgiApp)
        assert logger.level == logging.WARNING
    finally:
        logger.setLevel(original_level)


def test_asgi_app_configures_queue_logger_info_when_debug_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = logging.getLogger("vercel.queue")
    original_level = logger.level
    monkeypatch.setenv("VERCEL_QUEUE_DEBUG", "1")
    try:
        app = asgi_app(token="token")

        assert isinstance(app, QueueClientAsgiApp)
        assert logger.level == logging.INFO
    finally:
        logger.setLevel(original_level)
