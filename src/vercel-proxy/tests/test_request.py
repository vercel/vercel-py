"""Tests for vercel.proxy.Request."""

import uuid
from typing import Any

import pytest
import starlette.requests
from starlette.datastructures import URL, Headers, QueryParams
from starlette.types import Message, Receive

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


def test_rejects_non_http_scope() -> None:
    with pytest.raises(AssertionError):
        Request({**make_scope(), "type": "websocket"})


# ---------------------------------------------------------------------------
# Request data
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
# Body
# ---------------------------------------------------------------------------


def receive_body(body: bytes) -> Receive:
    async def receive() -> Message:
        return {"type": "http.request", "body": body, "more_body": False}

    return receive


async def test_body() -> None:
    assert await Request(make_scope(method="POST"), receive_body(b"hello")).body() == b"hello"


async def test_json() -> None:
    request = Request(make_scope(method="POST"), receive_body(b'{"a": 1}'))
    assert await request.json() == {"a": 1}


async def test_form() -> None:
    headers = [(b"content-type", b"application/x-www-form-urlencoded")]
    request = Request(make_scope(method="POST", headers=headers), receive_body(b"a=1&b=2"))
    assert dict(await request.form()) == {"a": "1", "b": "2"}
