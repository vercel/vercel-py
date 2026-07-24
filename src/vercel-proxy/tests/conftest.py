from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from starlette.types import ASGIApp, Message


def make_scope(
    path: str = "/",
    *,
    method: str = "GET",
    headers: Mapping[str, str] | Iterable[tuple[str, str]] = (),
    query_string: str = "",
) -> dict[str, Any]:
    header_items = headers.items() if isinstance(headers, Mapping) else headers
    raw_headers = [(name.lower().encode(), value.encode()) for name, value in header_items]
    if not any(name == b"host" for name, _ in raw_headers):
        raw_headers.append((b"host", b"example.com"))
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method,
        "scheme": "https",
        "path": path,
        "raw_path": path.encode(),
        "query_string": query_string.encode(),
        "root_path": "",
        "headers": raw_headers,
        "client": ("127.0.0.1", 1234),
        "server": ("example.com", 443),
    }


async def invoke(
    app: ASGIApp,
    scope: dict[str, Any] | None = None,
    *,
    body: bytes = b"",
) -> list[Message]:
    messages: list[Message] = []
    request_sent = False

    async def receive() -> Message:
        nonlocal request_sent
        if request_sent:
            return {"type": "http.disconnect"}
        request_sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: Message) -> None:
        messages.append(message)

    await app(scope or make_scope(), receive, send)
    return messages


def response_headers(messages: list[Message]) -> dict[str, str]:
    start = next(message for message in messages if message["type"] == "http.response.start")
    return {
        name.decode("latin-1"): value.decode("latin-1") for name, value in start.get("headers", [])
    }
