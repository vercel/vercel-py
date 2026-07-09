from __future__ import annotations

from typing import Any, TypeAlias, TypedDict

import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping

from vercel.headers import HeadersContext, headers_from_asgi_scope

from .client import QueueClient
from .config import CURRENT_DEPLOYMENT, BaseUrl, DeploymentOption
from .errors import ProtocolError
from .http import AsyncHttpClientFactory
from .log import configure_asgi_logger, debug_log
from .types import Duration

logger = logging.getLogger("vercel.queue")


class AsgiScope(TypedDict, total=False):
    type: str
    method: str
    headers: list[tuple[bytes, bytes]]


AsgiMessage: TypeAlias = dict[str, Any]
AsgiReceive: TypeAlias = Callable[[], Awaitable[AsgiMessage]]
AsgiSend: TypeAlias = Callable[[AsgiMessage], Awaitable[None]]


class QueueClientAsgiApp:
    """ASGI push-callback app backed by an async queue client."""

    def __init__(self, client: QueueClient) -> None:
        configure_asgi_logger()
        self.client = client

    async def __call__(
        self,
        scope: AsgiScope,
        receive: AsgiReceive,
        send: AsgiSend,
    ) -> None:
        scope_type = scope.get("type")
        if scope_type == "http":
            await self._handle_http(scope, receive, send)
            return
        if scope_type == "lifespan":
            await self._handle_lifespan(receive, send)
            return
        raise RuntimeError(f"Unsupported ASGI scope type: {scope_type!r}")

    async def _handle_http(
        self,
        scope: AsgiScope,
        receive: AsgiReceive,
        send: AsgiSend,
    ) -> None:
        if scope.get("method") != "POST":
            await _send_status(send, 405)
            return

        headers = headers_from_asgi_scope(scope)
        try:
            with HeadersContext(headers).use():
                await self.client.accept_and_handle(
                    _body_chunks(receive),
                    headers,
                )
        except (ProtocolError, TypeError, ValueError) as exc:
            debug_log("asgi.bad_request", error=repr(exc))
            logger.warning("Vercel Queue push callback rejected: %s", exc)
            await _send_status(send, 400)
        except Exception as exc:
            debug_log("asgi.delivery_failure", error=repr(exc))
            logger.exception("Vercel Queue push callback failed")
            await _send_status(send, 500)
        else:
            await _send_status(send, 204)

    async def _handle_lifespan(self, receive: AsgiReceive, send: AsgiSend) -> None:
        while True:
            message = await receive()
            message_type = message.get("type")
            if message_type == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message_type == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return


def asgi_app(
    *,
    client: QueueClient | None = None,
    token: str | None = None,
    region: str | None = None,
    base_url: BaseUrl | None = None,
    deployment: DeploymentOption = CURRENT_DEPLOYMENT,
    headers: Mapping[str, str] | None = None,
    timeout: Duration | None = 10.0,
    http_client_factory: AsyncHttpClientFactory | None = None,
) -> QueueClientAsgiApp:
    """Create an ASGI queue push-callback app."""
    configure_asgi_logger()
    resolved_client = client or QueueClient(
        token=token,
        region=region,
        base_url=base_url,
        deployment=deployment,
        headers=headers,
        timeout=timeout,
        http_client_factory=http_client_factory,
    )
    return QueueClientAsgiApp(resolved_client)


async def _body_chunks(receive: AsgiReceive) -> AsyncIterator[bytes]:
    while True:
        message = await receive()
        message_type = message.get("type")
        if message_type == "http.disconnect":
            raise ValueError("request body disconnected before completion")
        if message_type != "http.request":
            raise ValueError(f"unexpected ASGI message: {message_type!r}")
        body = message.get("body", b"")
        if body:
            yield body
        if not message.get("more_body", False):
            return


async def _send_status(send: AsgiSend, status: int) -> None:
    headers: list[tuple[bytes, bytes]] = []
    if status == 405:
        headers.append((b"allow", b"POST"))
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": headers,
    })
    await send({"type": "http.response.body", "body": b""})


# Only add public symbols to __all__; internal helpers must stay unexported.
__all__ = ("QueueClientAsgiApp", "asgi_app")
