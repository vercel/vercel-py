"""The `schedule_handler` ASGI adapter."""

import json
import logging
import threading
from collections.abc import Mapping
from typing import Any

import pytest
from conftest import DISPATCH_HEADERS
from pydantic import BaseModel

from vercel.headers import get_headers
from vercel.schedules import ScheduleAsgiApp, ScheduleEvent, schedule_handler


class Response:
    def __init__(self) -> None:
        self.status: int | None = None
        self.headers: dict[str, str] = {}
        self.body = b""

    def json(self) -> Any:
        return json.loads(self.body)


async def call(
    app: ScheduleAsgiApp[Any],
    *,
    method: str = "POST",
    headers: Mapping[str, str] = DISPATCH_HEADERS,
    body: bytes = b"",
    chunks: int = 1,
) -> Response:
    scope = {
        "type": "http",
        "method": method,
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
    }
    size = max(1, -(-len(body) // chunks))
    parts = [body[i : i + size] for i in range(0, len(body), size)] if body else [b""]
    messages = [
        {"type": "http.request", "body": part, "more_body": i < len(parts) - 1}
        for i, part in enumerate(parts)
    ]
    response = Response()

    async def receive() -> dict[str, Any]:
        return messages.pop(0)

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            response.status = message["status"]
            response.headers = {k.decode(): v.decode() for k, v in message["headers"]}
        else:
            response.body += message.get("body", b"")

    await app(scope, receive, send)
    return response


JSON = {**DISPATCH_HEADERS, "content-type": "application/json"}


async def test_async_handler_receives_event_and_answers_200() -> None:
    seen: list[ScheduleEvent[Any]] = []

    @schedule_handler
    async def app(event: ScheduleEvent[Any]) -> None:
        seen.append(event)

    response = await call(app, headers=JSON, body=b'{"a": 1}', chunks=3)

    assert response.status == 200
    assert response.body == b""
    assert seen[0].name == "every-ten"
    assert seen[0].payload == {"a": 1}
    assert getattr(app, "__name__") == "app"  # noqa: B009


async def test_sync_handler_runs_in_a_worker_thread() -> None:
    threads: list[str] = []

    @schedule_handler
    def app(event: ScheduleEvent[Any]) -> None:
        threads.append(threading.current_thread().name)

    response = await call(app)

    assert response.status == 200
    assert threads and threads[0] != threading.main_thread().name


async def test_payload_type_is_applied() -> None:
    class Cleanup(BaseModel):
        max_age_days: int

    seen: list[Cleanup | None] = []

    @schedule_handler(payload_type=Cleanup)
    async def app(event: ScheduleEvent[Cleanup]) -> None:
        seen.append(event.payload)

    ok = await call(app, headers=JSON, body=b'{"max_age_days": 3}')
    bad = await call(app, headers=JSON, body=b'{"max_age_days": "x"}')

    assert ok.status == 200
    assert seen == [Cleanup(max_age_days=3)]
    assert bad.status == 400
    assert "Cleanup" in bad.json()["error"]


async def test_non_dispatch_request_is_400_with_reason() -> None:
    @schedule_handler
    async def app(event: ScheduleEvent[Any]) -> None:
        raise AssertionError("must not run")

    response = await call(app, headers={})

    assert response.status == 400
    assert response.headers["content-type"] == "application/json"
    assert "does not look like a schedule dispatch" in response.json()["error"] or (
        "spec version" in response.json()["error"]
    )


async def test_non_post_is_405() -> None:
    @schedule_handler
    async def app(event: ScheduleEvent[Any]) -> None:
        raise AssertionError("must not run")

    response = await call(app, method="GET")

    assert response.status == 405
    assert response.headers["allow"] == "POST"


async def test_handler_failure_is_500_and_logged(caplog: pytest.LogCaptureFixture) -> None:
    @schedule_handler
    async def app(event: ScheduleEvent[Any]) -> None:
        raise RuntimeError("boom")

    with caplog.at_level(logging.ERROR, logger="vercel.schedules"):
        response = await call(app)

    assert response.status == 500
    assert response.json() == {"error": "Schedule handler failed"}
    assert "boom" in caplog.text


async def test_request_headers_are_visible_through_vercel_headers() -> None:
    seen: list[str | None] = []

    @schedule_handler
    async def app(event: ScheduleEvent[Any]) -> None:
        headers = get_headers()
        seen.append(None if headers is None else headers.get("ce-vssschedulename"))

    await call(app)

    assert seen == ["every-ten"]


async def test_lifespan_is_acknowledged() -> None:
    @schedule_handler
    async def app(event: ScheduleEvent[Any]) -> None:
        pass

    messages = [{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]
    sent: list[str] = []

    async def receive() -> dict[str, Any]:
        return messages.pop(0)

    async def send(message: dict[str, Any]) -> None:
        sent.append(message["type"])

    await app({"type": "lifespan"}, receive, send)

    assert sent == ["lifespan.startup.complete", "lifespan.shutdown.complete"]
