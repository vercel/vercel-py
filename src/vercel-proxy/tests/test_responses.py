from __future__ import annotations

import pytest
from starlette.responses import RedirectResponse

from vercel.proxy import RoutingResponse, continue_routing, redirect, rewrite

from .conftest import invoke, response_headers


async def test_continue_routing_emits_only_the_control_protocol_at_send_time() -> None:
    response = continue_routing(headers={"x-visible": "yes"})

    assert isinstance(response, RoutingResponse)
    assert response.action == "continue"
    assert response.destination is None
    assert dict(response.headers) == {"x-visible": "yes"}

    messages = await invoke(response)

    assert response_headers(messages) == {
        "x-visible": "yes",
        "x-middleware-next": "1",
    }
    assert dict(response.headers) == {"x-visible": "yes"}
    assert messages[-1] == {"type": "http.response.body", "body": b""}


async def test_rewrite_supports_relative_and_absolute_destinations() -> None:
    relative = rewrite("/new/path?source=proxy")
    absolute = rewrite("https://other.example/path")

    assert relative.action == "rewrite"
    assert relative.destination == "/new/path?source=proxy"
    assert response_headers(await invoke(relative))["x-middleware-rewrite"] == (
        "/new/path?source=proxy"
    )
    assert response_headers(await invoke(absolute))["x-middleware-rewrite"] == (
        "https://other.example/path"
    )


async def test_rewrite_encodes_destination_for_an_http_header() -> None:
    response = rewrite("/hello world/❤?from=two words")

    assert response.destination == "/hello%20world/%E2%9D%A4?from=two%20words"
    assert response_headers(await invoke(response))["x-middleware-rewrite"] == (
        "/hello%20world/%E2%9D%A4?from=two%20words"
    )


async def test_request_headers_are_a_complete_mutable_replacement() -> None:
    response = continue_routing(request_headers={"Authorization": "Bearer original"})
    assert response.request_headers is not None
    response.request_headers["authorization"] = "Bearer updated"
    response.request_headers["x-user-id"] = "user_123"

    headers = response_headers(await invoke(response))

    assert headers["x-middleware-override-headers"] == "authorization,x-user-id"
    assert headers["x-middleware-request-authorization"] == "Bearer updated"
    assert headers["x-middleware-request-x-user-id"] == "user_123"


async def test_empty_request_headers_emit_an_explicit_empty_replacement() -> None:
    headers = response_headers(await invoke(continue_routing(request_headers={})))

    assert "x-middleware-override-headers" in headers
    assert headers["x-middleware-override-headers"] == ""


async def test_routing_response_does_not_generate_content_length() -> None:
    assert "content-length" not in response_headers(await invoke(continue_routing()))

    explicit = continue_routing(headers={"content-length": "0"})
    assert response_headers(await invoke(explicit))["content-length"] == "0"


async def test_middleware_protocol_headers_are_reserved() -> None:
    response = continue_routing()
    response.headers["x-middleware-next"] = "user-value"

    with pytest.raises(ValueError, match="reserved"):
        await invoke(response)


def test_redirect_returns_a_native_starlette_response() -> None:
    response = redirect("/login", status_code=308, headers={"x-reason": "auth"})

    assert isinstance(response, RedirectResponse)
    assert response.status_code == 308
    assert response.headers["location"] == "/login"
    assert response.headers["x-reason"] == "auth"


def test_redirect_rejects_non_redirect_status() -> None:
    with pytest.raises(ValueError, match="between 300 and 399"):
        redirect("/login", status_code=200)
