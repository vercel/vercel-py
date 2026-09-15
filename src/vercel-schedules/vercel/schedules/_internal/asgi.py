"""ASGI adapter for schedule dispatches."""

from __future__ import annotations

import functools
import inspect
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Generic, TypeAlias, overload

import anyio.to_thread

from vercel.headers import HeadersContext, headers_from_asgi_scope
from vercel.schedules._internal.errors import ScheduleEventParseError
from vercel.schedules._internal.events import parse_schedule_event
from vercel.schedules._internal.models import PayloadT as T, ScheduleEvent

logger = logging.getLogger("vercel.schedules")

ScheduleHandler: TypeAlias = Callable[[ScheduleEvent[T]], Awaitable[None] | None]
"""A function invoked once per schedule firing.

Either `async def` or a plain `def`; plain functions run in a worker thread so
they do not block the event loop.
"""

AsgiScope: TypeAlias = dict[str, Any]
AsgiMessage: TypeAlias = dict[str, Any]
AsgiReceive: TypeAlias = Callable[[], Awaitable[AsgiMessage]]
AsgiSend: TypeAlias = Callable[[AsgiMessage], Awaitable[None]]


class ScheduleAsgiApp(Generic[T]):
    """An ASGI application that runs a handler for each schedule dispatch.

    Responds `200` when the handler completes, `400` when the request is not a
    schedule dispatch, `405` for methods other than `POST`, and `500` when the
    handler raises. Handler exceptions are logged under `vercel.schedules`.
    """

    def __init__(self, handler: ScheduleHandler[T], *, payload_type: type[T] | None) -> None:
        self.handler = handler
        self.payload_type = payload_type
        functools.update_wrapper(self, handler)

    async def __call__(self, scope: AsgiScope, receive: AsgiReceive, send: AsgiSend) -> None:
        scope_type = scope.get("type")
        if scope_type == "http":
            await self._handle_http(scope, receive, send)
        elif scope_type == "lifespan":
            await self._handle_lifespan(receive, send)
        else:
            raise RuntimeError(f"Unsupported ASGI scope type: {scope_type!r}")

    async def _handle_http(self, scope: AsgiScope, receive: AsgiReceive, send: AsgiSend) -> None:
        method = str(scope.get("method", ""))
        if method != "POST":
            await _respond(send, 405, headers=[(b"allow", b"POST")])
            return

        headers = headers_from_asgi_scope(scope)
        try:
            body = await _read_body(receive)
            event = parse_schedule_event(
                headers, body, method=method, payload_type=self.payload_type
            )
        except ScheduleEventParseError as exc:
            logger.warning("Schedule dispatch rejected: %s", exc)
            await _respond(send, 400, body={"error": str(exc)})
            return
        except Exception:
            logger.exception("Failed to parse schedule dispatch")
            await _respond(send, 500, body={"error": "Failed to parse schedule dispatch"})
            return

        try:
            with HeadersContext(headers).use():
                await self._run(event)
        except Exception:
            logger.exception("Schedule handler failed for %s/%s", event.namespace, event.name)
            await _respond(send, 500, body={"error": "Schedule handler failed"})
            return

        await _respond(send, 200)

    async def _run(self, event: ScheduleEvent[T]) -> None:
        if inspect.iscoroutinefunction(self.handler):
            await self.handler(event)
            return
        result = await anyio.to_thread.run_sync(self.handler, event)
        if inspect.isawaitable(result):
            await result

    async def _handle_lifespan(self, receive: AsgiReceive, send: AsgiSend) -> None:
        while True:
            message = await receive()
            message_type = message.get("type")
            if message_type == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message_type == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return


async def _read_body(receive: AsgiReceive) -> bytes | None:
    chunks: list[bytes] = []
    while True:
        message = await receive()
        message_type = message.get("type")
        if message_type == "http.disconnect":
            raise ScheduleEventParseError("request body disconnected before completion")
        if message_type != "http.request":
            raise ScheduleEventParseError(f"unexpected ASGI message: {message_type!r}")
        chunks.append(message.get("body", b""))
        if not message.get("more_body", False):
            break
    body = b"".join(chunks)
    return body or None


async def _respond(
    send: AsgiSend,
    status: int,
    *,
    body: dict[str, Any] | None = None,
    headers: list[tuple[bytes, bytes]] | None = None,
) -> None:
    response_headers = list(headers or [])
    payload = b""
    if body is not None:
        payload = json.dumps(body).encode()
        response_headers.append((b"content-type", b"application/json"))
    response_headers.append((b"content-length", str(len(payload)).encode()))
    await send({"type": "http.response.start", "status": status, "headers": response_headers})
    await send({"type": "http.response.body", "body": payload})


@overload
def schedule_handler(handler: ScheduleHandler[Any], /) -> ScheduleAsgiApp[Any]: ...


@overload
def schedule_handler(
    *, payload_type: type[T]
) -> Callable[[ScheduleHandler[T]], ScheduleAsgiApp[T]]: ...


@overload
def schedule_handler(
    *, payload_type: None = None
) -> Callable[[ScheduleHandler[Any]], ScheduleAsgiApp[Any]]: ...


def schedule_handler(
    handler: ScheduleHandler[Any] | None = None,
    /,
    *,
    payload_type: type[Any] | None = None,
) -> Any:
    """Turn a handler into an ASGI app that serves schedule dispatches.

    Use it bare or with a payload type:

    ```python
    @schedule_handler
    async def app(event: ScheduleEvent) -> None: ...

    @schedule_handler(payload_type=CleanupPayload)
    async def app(event: ScheduleEvent[CleanupPayload]) -> None: ...
    ```

    The result is a plain ASGI callable: export it as your function's `app`, or
    mount it under a route in any ASGI framework.

    Args:
        handler: The function to run per firing, `async def` or plain `def`.
        payload_type: Validate the payload against this type before the handler
            runs. A mismatch answers `400` without invoking the handler.

    Returns:
        The ASGI app, or a decorator producing one when called with keywords only.
    """

    def wrap(fn: ScheduleHandler[Any]) -> ScheduleAsgiApp[Any]:
        return ScheduleAsgiApp(fn, payload_type=payload_type)

    if handler is None:
        return wrap
    return wrap(handler)


__all__ = ["ScheduleAsgiApp", "ScheduleHandler", "schedule_handler"]
