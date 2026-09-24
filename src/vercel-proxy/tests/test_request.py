"""Tests for vercel.proxy.Request."""

import uuid
from typing import Any, cast

import pytest
import starlette.requests
from starlette.datastructures import URL, Headers, QueryParams

from vercel.proxy import Request


def make_scope(
    *,
    method: str = "GET",
    path: str = "/",
    query_string: bytes = b"",
    headers: list[tuple[bytes, bytes]] | None = None,
    path_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    scope: dict[str, Any] = {
        "type": "http",
        "method": method,
        "scheme": "https",
        "server": ("example.com", 443),
        "client": ("203.0.113.7", 51234),
        "path": path,
        "root_path": "",
        "query_string": query_string,
        "headers": headers or [(b"host", b"example.com")],
    }
    if path_params is not None:
        scope["path_params"] = path_params
    return scope


# ---------------------------------------------------------------------------
# Type
# ---------------------------------------------------------------------------


def test_is_starlette_request() -> None:
    assert isinstance(Request(make_scope()), starlette.requests.Request)


def test_rejects_receive_argument() -> None:
    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"secret"}

    with pytest.raises(TypeError):
        cast(Any, Request)(make_scope(), receive)


def test_rejects_non_http_scope() -> None:
    with pytest.raises(AssertionError):
        Request({**make_scope(), "type": "websocket"})


# ---------------------------------------------------------------------------
# Non-body API
# ---------------------------------------------------------------------------


def test_method() -> None:
    assert Request(make_scope(method="POST")).method == "POST"


def test_url() -> None:
    req = Request(make_scope(path="/users/42", query_string=b"a=1"))
    assert isinstance(req.url, URL)
    assert str(req.url) == "https://example.com/users/42?a=1"
    assert req.url.path == "/users/42"


def test_headers_case_insensitive() -> None:
    req = Request(make_scope(headers=[(b"host", b"example.com"), (b"x-tenant", b"acme")]))
    assert isinstance(req.headers, Headers)
    assert req.headers["X-Tenant"] == "acme"


def test_headers_repeated() -> None:
    req = Request(make_scope(headers=[(b"x-a", b"1"), (b"x-a", b"2")]))
    assert req.headers.getlist("x-a") == ["1", "2"]


def test_query_params() -> None:
    req = Request(make_scope(query_string=b"tag=a&tag=b&q=hi"))
    assert isinstance(req.query_params, QueryParams)
    assert req.query_params["q"] == "hi"
    assert req.query_params.getlist("tag") == ["a", "b"]


def test_cookies() -> None:
    req = Request(make_scope(headers=[(b"cookie", b'session=abc; token="x y"')]))
    assert req.cookies == {"session": "abc", "token": "x y"}


def test_cookies_empty() -> None:
    assert Request(make_scope()).cookies == {}


def test_path_params_default_empty() -> None:
    assert Request(make_scope()).path_params == {}


def test_path_params_from_scope() -> None:
    uid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    req = Request(make_scope(path_params={"id": 42, "uid": uid}))
    assert req.path_params == {"id": 42, "uid": uid}


def test_client() -> None:
    client = Request(make_scope()).client
    assert client is not None
    assert (client.host, client.port) == ("203.0.113.7", 51234)


# ---------------------------------------------------------------------------
# Body access blocked
# ---------------------------------------------------------------------------

BODY_UNAVAILABLE = "request body is not available in proxy handlers"


def test_receive_raises() -> None:
    with pytest.raises(RuntimeError, match=BODY_UNAVAILABLE):
        _ = Request(make_scope()).receive


def test_stream_raises() -> None:
    with pytest.raises(RuntimeError, match=BODY_UNAVAILABLE):
        Request(make_scope()).stream()


async def test_body_raises() -> None:
    with pytest.raises(RuntimeError, match=BODY_UNAVAILABLE):
        await Request(make_scope()).body()


async def test_json_raises() -> None:
    with pytest.raises(RuntimeError, match=BODY_UNAVAILABLE):
        await Request(make_scope()).json()


def test_form_raises() -> None:
    with pytest.raises(RuntimeError, match=BODY_UNAVAILABLE):
        Request(make_scope()).form()


def test_form_raises_with_limits() -> None:
    with pytest.raises(RuntimeError, match=BODY_UNAVAILABLE):
        Request(make_scope()).form(max_files=1, max_fields=1, max_part_size=1)


async def test_is_disconnected_raises() -> None:
    with pytest.raises(RuntimeError, match=BODY_UNAVAILABLE):
        await Request(make_scope()).is_disconnected()


async def test_internal_receive_channel_is_empty() -> None:
    # Even Starlette's private channel is the empty placeholder, never a real
    # receive callable.
    with pytest.raises(RuntimeError, match="Receive channel has not been made available"):
        await Request(make_scope())._receive()
