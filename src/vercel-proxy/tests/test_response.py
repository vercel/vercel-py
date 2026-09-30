"""Tests for the vercel.proxy response classes."""

from collections.abc import Callable, Mapping

import pytest
from starlette import responses
from starlette.datastructures import Headers, MutableHeaders
from starlette.types import Message

from vercel.proxy import (
    ContinueResponse,
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
    RewriteResponse,
)

# Each factory builds a response with the given headers.
Factory = Callable[[Mapping[str, str]], Response]

_FACTORIES: dict[str, Factory] = {
    "response": lambda headers: Response(headers=headers),
    "html": lambda headers: HTMLResponse("", headers=headers),
    "plain_text": lambda headers: PlainTextResponse("", headers=headers),
    "json": lambda headers: JSONResponse({}, headers=headers),
    "redirect": lambda headers: RedirectResponse("/x", headers=headers),
    "continue": lambda headers: ContinueResponse(headers=headers),
    "rewrite": lambda headers: RewriteResponse("/x", headers=headers),
}

# Each change adds a header with the given name.
_HEADER_CHANGES: dict[str, Callable[[MutableHeaders, str], object]] = {
    "setitem": lambda headers, name: headers.__setitem__(name, "1"),
    "setdefault": lambda headers, name: headers.setdefault(name, "1"),
    "append": lambda headers, name: headers.append(name, "1"),
    "update": lambda headers, name: headers.update({name: "1"}),
    "ior": lambda headers, name: headers.__ior__({name: "1"}),
}


async def send_response(response: Response) -> list[Message]:
    messages: list[Message] = []

    async def receive() -> Message:
        raise AssertionError("response must not read the request")

    async def send(message: Message) -> None:
        messages.append(message)

    await response({"type": "http"}, receive, send)
    return messages


# ---------------------------------------------------------------------------
# Starlette classes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("cls", "base"),
    [
        (Response, responses.Response),
        (HTMLResponse, responses.HTMLResponse),
        (PlainTextResponse, responses.PlainTextResponse),
        (JSONResponse, responses.JSONResponse),
        (RedirectResponse, responses.RedirectResponse),
    ],
)
def test_subclasses_starlette(cls: type[Response], base: type[responses.Response]) -> None:
    assert issubclass(cls, Response)
    assert issubclass(cls, base)


def test_continue_response_is_response() -> None:
    assert isinstance(ContinueResponse(), Response)


def test_rewrite_response_is_continue_response() -> None:
    assert isinstance(RewriteResponse("/x"), ContinueResponse)


def test_response_body_and_status() -> None:
    res = Response(b"tea", status_code=418)
    assert (res.status_code, res.body) == (418, b"tea")


def test_json_response() -> None:
    res = JSONResponse({"ok": True, "name": "caf\u00e9"}, status_code=403)
    assert res.status_code == 403
    assert res.body == '{"ok":true,"name":"caf\u00e9"}'.encode()
    assert res.headers["content-type"] == "application/json"


def test_redirect_response() -> None:
    res = RedirectResponse("/caf\u00e9 menu")
    assert res.status_code == 307
    assert res.headers["location"] == "/caf%C3%A9%20menu"


def test_text_responses_content_type() -> None:
    assert PlainTextResponse("hi").headers["content-type"] == "text/plain; charset=utf-8"
    assert HTMLResponse("<p>").headers["content-type"] == "text/html; charset=utf-8"


# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("factory", _FACTORIES.values(), ids=list(_FACTORIES))
def test_headers_accept_str_dict(factory: Factory) -> None:
    headers: dict[str, str] = {"x-a": "1"}
    assert factory(headers).headers["x-a"] == "1"


@pytest.mark.parametrize("factory", _FACTORIES.values(), ids=list(_FACTORIES))
def test_headers_accept_starlette_headers(factory: Factory) -> None:
    assert factory(Headers({"x-a": "1"})).headers["x-a"] == "1"


@pytest.mark.parametrize("factory", _FACTORIES.values(), ids=list(_FACTORIES))
def test_headers_can_be_changed(factory: Factory) -> None:
    res = factory({"x-a": "1", "x-b": "2"})
    res.headers["x-a"] = "3"
    res.headers.append("x-c", "4")
    res.headers.setdefault("x-d", "5")
    del res.headers["x-b"]
    assert res.headers["x-a"] == "3"
    assert res.headers["x-c"] == "4"
    assert res.headers["x-d"] == "5"
    assert "x-b" not in res.headers


def test_headers_case_insensitive() -> None:
    res = Response(headers={"X-A": "1"})
    res.headers["x-a"] = "2"
    assert res.headers.getlist("X-A") == ["2"]


def test_headers_not_shared_with_caller() -> None:
    headers = {"x-a": "1"}
    res = Response(headers=headers)
    headers["x-a"] = "2"
    assert res.headers["x-a"] == "1"


@pytest.mark.parametrize("factory", _FACTORIES.values(), ids=list(_FACTORIES))
@pytest.mark.parametrize("name", ["x-middleware-next", "X-Middleware-Rewrite"])
def test_headers_reject_reserved(factory: Factory, name: str) -> None:
    with pytest.raises(ValueError, match=f'invalid header "{name}": .* reserved'):
        factory({name: "1"})


@pytest.mark.parametrize("change", _HEADER_CHANGES.values(), ids=list(_HEADER_CHANGES))
@pytest.mark.parametrize("factory", _FACTORIES.values(), ids=list(_FACTORIES))
def test_header_changes_reject_reserved(
    change: Callable[[MutableHeaders, str], object], factory: Factory
) -> None:
    res = factory({})
    before = list(res.raw_headers)
    with pytest.raises(ValueError, match='invalid header "x-middleware-a": .* reserved'):
        change(res.headers, "x-middleware-a")
    assert res.raw_headers == before


def test_headers_reject_non_latin1() -> None:
    with pytest.raises(UnicodeEncodeError):
        Response(headers={"x-a": "\u20ac"})
    res = Response()
    with pytest.raises(UnicodeEncodeError):
        res.headers["x-a"] = "\u20ac"


# ---------------------------------------------------------------------------
# Cookies
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("factory", _FACTORIES.values(), ids=list(_FACTORIES))
def test_set_cookie(factory: Factory) -> None:
    res = factory({})
    res.set_cookie("a", "1")
    res.set_cookie("b", "2")
    assert res.headers.getlist("set-cookie") == [
        "a=1; Path=/; SameSite=lax",
        "b=2; Path=/; SameSite=lax",
    ]


def test_delete_cookie() -> None:
    res = ContinueResponse()
    res.delete_cookie("a", path="/app")
    cookie = res.headers["set-cookie"]
    assert cookie.startswith('a=""; expires=')
    assert cookie.endswith("; Max-Age=0; Path=/app; SameSite=lax")


# ---------------------------------------------------------------------------
# ContinueResponse and RewriteResponse
# ---------------------------------------------------------------------------


def test_continue_defaults() -> None:
    res = ContinueResponse()
    assert res.request_headers == {}
    assert (res.status_code, res.body) == (200, b"")


def test_rewrite_request_headers() -> None:
    res = RewriteResponse("/x", request_headers={"x-a": "1"})
    assert res.request_headers == {"x-a": "1"}


def test_rewrite_destination() -> None:
    assert RewriteResponse("https://internal.example.com/api").destination == (
        "https://internal.example.com/api"
    )


def test_continue_request_headers() -> None:
    res = ContinueResponse(request_headers={"x-tenant": "acme", "authorization": None})
    assert res.request_headers == {"x-tenant": "acme", "authorization": None}


def test_continue_request_headers_not_shared_with_caller() -> None:
    request_headers: dict[str, str | None] = {"x-a": "1"}
    res = ContinueResponse(request_headers=request_headers)
    request_headers["x-a"] = "2"
    assert res.request_headers == {"x-a": "1"}


def test_continue_headers_and_request_headers_are_separate() -> None:
    res = ContinueResponse(headers={"x-a": "1"}, request_headers={"x-b": "2"})
    assert res.headers.get("x-b") is None
    assert res.request_headers == {"x-b": "2"}


@pytest.mark.parametrize("name", ["x-middleware-next", "X-Middleware-Rewrite"])
def test_continue_request_headers_reject_reserved(name: str) -> None:
    with pytest.raises(ValueError, match=f'invalid header "{name}": .* reserved'):
        ContinueResponse(request_headers={name: "1"})


async def test_continue_request_headers_changed_to_reserved_rejected_on_send() -> None:
    res = ContinueResponse()
    res.request_headers["x-middleware-a"] = "1"
    with pytest.raises(ValueError, match='invalid header "x-middleware-a": .* reserved'):
        await send_response(res)


def test_continue_status_code_is_hidden() -> None:
    res = ContinueResponse()
    res.status_code = 200
    with pytest.raises(RuntimeError, match="continue responses always have status 200"):
        res.status_code = 404
    assert res.status_code == 200


def test_continue_body_is_hidden() -> None:
    res = ContinueResponse()
    res.body = b""
    with pytest.raises(RuntimeError, match="continue responses cannot have a body"):
        res.body = b"x"
    assert res.body == b""


async def test_continue_sends_changed_request_headers() -> None:
    res = RewriteResponse("/v2")
    res.request_headers["x-a"] = "1"
    res.request_headers["cookie"] = None
    messages = await send_response(res)
    assert messages == [
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"x-middleware-rewrite", b"/v2"),
                (b"x-middleware-override-headers-diff", b"x-a,cookie"),
                (b"x-middleware-request-x-a", b"1"),
                (b"content-length", b"0"),
            ],
        },
        {"type": "http.response.body", "body": b""},
    ]
