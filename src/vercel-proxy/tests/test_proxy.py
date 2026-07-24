from __future__ import annotations

import asyncio
import inspect
from typing import Any

import pytest
from starlette.types import Message

from vercel.proxy import Proxy, Request, Route, RoutingResponse, rewrite

from .conftest import invoke, make_scope, response_headers


async def test_middleware_composes_last_registered_as_outermost() -> None:
    events: list[str] = []
    proxy = Proxy(routes=[Route.rewrite("/", "/destination")])

    @proxy.middleware("http")
    async def inner(request: Request, call_next):
        events.append("inner:request")
        response = await call_next(request)
        events.append("inner:response")
        response.headers["x-inner"] = "true"
        return response

    @proxy.middleware("http")
    async def outer(request: Request, call_next):
        events.append("outer:request")
        response = await call_next(request)
        events.append("outer:response")
        response.headers["x-outer"] = "true"
        return response

    headers = response_headers(await invoke(proxy))

    assert events == [
        "outer:request",
        "inner:request",
        "inner:response",
        "outer:response",
    ]
    assert headers["x-middleware-rewrite"] == "/destination"
    assert headers["x-inner"] == "true"
    assert headers["x-outer"] == "true"


async def test_call_next_returns_a_synthetic_routing_response() -> None:
    proxy = Proxy()

    @proxy.middleware("http")
    async def authenticate(request: Request, call_next):
        response = await call_next(request)
        assert isinstance(response, RoutingResponse)
        response.headers["x-authenticated"] = "true"
        response.request_headers = dict(request.headers)
        assert response.request_headers is not None
        response.request_headers["x-user-id"] = "user_123"
        return response

    headers = response_headers(
        await invoke(proxy, make_scope(headers={"authorization": "Bearer token"}))
    )

    assert headers["x-middleware-next"] == "1"
    assert headers["x-authenticated"] == "true"
    assert headers["x-middleware-override-headers"] == "authorization,host,x-user-id"
    assert headers["x-middleware-request-x-user-id"] == "user_123"


async def test_middleware_can_short_circuit_or_return_none() -> None:
    called = False
    short_circuit = Proxy(routes=[Route.rewrite("/", "/route")])

    @short_circuit.middleware("http")
    async def stop(request: Request, call_next):
        return rewrite("/middleware")

    headers = response_headers(await invoke(short_circuit))
    assert headers["x-middleware-rewrite"] == "/middleware"

    continue_proxy = Proxy(routes=[Route("/", lambda request: _mark_called())])

    def _mark_called():
        nonlocal called
        called = True
        return rewrite("/route")

    @continue_proxy.middleware("http")
    async def return_none(request: Request, call_next):
        return None

    headers = response_headers(await invoke(continue_proxy))
    assert headers["x-middleware-next"] == "1"
    assert called is False


async def test_request_body_is_cached_across_middleware_and_route() -> None:
    seen: list[bytes] = []

    async def endpoint(request: Request):
        seen.append(await request.body())
        return None

    proxy = Proxy(routes=[Route("/", endpoint)])

    @proxy.middleware("http")
    async def read_body(request: Request, call_next):
        seen.append(await request.body())
        return await call_next(request)

    await invoke(proxy, body=b"payload")

    assert seen == [b"payload", b"payload"]


async def test_concurrent_requests_do_not_share_path_parameters() -> None:
    async def endpoint(request: Request):
        value = request.path_params["value"]
        await asyncio.sleep(0)
        return rewrite(f"/seen/{value}")

    proxy = Proxy(routes=[Route("/{value}", endpoint)])

    first, second = await asyncio.gather(
        invoke(proxy, make_scope("/one")),
        invoke(proxy, make_scope("/two")),
    )

    assert response_headers(first)["x-middleware-rewrite"] == "/seen/one"
    assert response_headers(second)["x-middleware-rewrite"] == "/seen/two"


def test_proxy_has_an_unambiguous_raw_asgi_signature() -> None:
    proxy = Proxy()
    parameters = list(inspect.signature(proxy).parameters.values())

    assert [parameter.name for parameter in parameters] == ["scope", "receive", "send"]
    assert all(parameter.default is inspect.Parameter.empty for parameter in parameters)
    assert not hasattr(proxy, "user_middleware")
    assert proxy.__vercel_proxy__ is True


def test_only_http_middleware_is_supported() -> None:
    proxy = Proxy()

    with pytest.raises(ValueError, match="only supports HTTP"):
        proxy.middleware("websocket")


async def test_invalid_middleware_return_is_rejected() -> None:
    proxy = Proxy()

    @proxy.middleware("http")
    async def invalid(request: Request, call_next):
        return False

    with pytest.raises(TypeError, match="proxy middleware"):
        await invoke(proxy)


async def test_middleware_registration_freezes_after_first_request() -> None:
    proxy = Proxy()
    await invoke(proxy)

    with pytest.raises(RuntimeError, match="cannot be registered"):

        @proxy.middleware("http")
        async def too_late(request: Request, call_next):
            return await call_next(request)


async def test_lifespan_completes_startup_and_shutdown() -> None:
    proxy = Proxy()
    incoming: asyncio.Queue[Message] = asyncio.Queue()
    await incoming.put({"type": "lifespan.startup"})
    await incoming.put({"type": "lifespan.shutdown"})
    outgoing: list[Message] = []

    async def receive() -> Message:
        return await incoming.get()

    async def send(message: Message) -> None:
        outgoing.append(message)

    await proxy({"type": "lifespan", "asgi": {"version": "3.0"}}, receive, send)

    assert outgoing == [
        {"type": "lifespan.startup.complete"},
        {"type": "lifespan.shutdown.complete"},
    ]


async def test_websocket_is_explicitly_rejected_in_v1() -> None:
    proxy = Proxy()
    outgoing: list[Message] = []

    async def receive() -> Message:
        return {"type": "websocket.connect"}

    async def send(message: Message) -> None:
        outgoing.append(message)

    scope: dict[str, Any] = {
        "type": "websocket",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "scheme": "wss",
        "path": "/socket",
        "raw_path": b"/socket",
        "query_string": b"",
        "root_path": "",
        "headers": [],
        "client": ("127.0.0.1", 1234),
        "server": ("example.com", 443),
        "subprotocols": [],
    }
    await proxy(scope, receive, send)

    assert outgoing == [{"type": "websocket.close", "code": 1008}]
