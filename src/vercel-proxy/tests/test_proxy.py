"""Tests for vercel.proxy.Proxy, driven through its ASGI interface."""

import functools
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, cast

import pytest
import starlette.responses
from starlette.types import Message

from vercel.proxy import (
    ContinueResponse,
    Handler,
    JSONResponse,
    PlainTextResponse,
    Proxy,
    RedirectResponse,
    Request,
    Response,
    RewriteResponse,
)


@dataclass
class Sent:
    status: int
    headers: list[tuple[bytes, bytes]]
    body: bytes
    header_dict: dict[str, str] = field(init=False)

    def __post_init__(self) -> None:
        self.header_dict = {k.decode(): v.decode() for k, v in self.headers}


def make_scope(
    path: str = "/",
    *,
    method: str = "GET",
    host: str | None = "example.com",
    query_string: bytes = b"",
) -> dict[str, Any]:
    headers = [(b"host", host.encode())] if host is not None else []
    return {
        "type": "http",
        "method": method,
        "scheme": "https",
        "server": ("server.internal", 443),
        "path": path,
        "root_path": "",
        "query_string": query_string,
        "headers": headers,
    }


async def never_receive() -> Message:
    raise AssertionError("proxy must not read the request body")


async def call(proxy: Proxy, scope: dict[str, Any]) -> Sent:
    messages: list[Message] = []

    async def send(message: Message) -> None:
        messages.append(message)

    await proxy(scope, never_receive, send)
    assert [m["type"] for m in messages] == ["http.response.start", "http.response.body"]
    return Sent(messages[0]["status"], messages[0]["headers"], messages[1]["body"])


async def not_found(req: Request) -> Response:
    return Response(status_code=404)


def capture(seen: list[Any], value: Callable[[Request], Any]) -> Handler:
    """Handler that records *value(request)* in *seen* and passes the request on."""

    def handler(req: Request) -> Response:
        seen.append(value(req))
        return ContinueResponse()

    return handler


# ---------------------------------------------------------------------------
# Emitted responses
# ---------------------------------------------------------------------------


async def test_next_emits_middleware_next() -> None:
    proxy = Proxy()
    proxy.route("/")(lambda req: ContinueResponse())
    sent = await call(proxy, make_scope())
    assert (sent.status, sent.body) == (200, b"")
    assert sent.headers == [(b"x-middleware-next", b"1"), (b"content-length", b"0")]


async def test_next_with_request_headers_emits_override() -> None:
    proxy = Proxy()
    proxy.route("/")(
        lambda req: ContinueResponse(request_headers={"X-Tenant": "acme", "Cookie": None})
    )
    sent = await call(proxy, make_scope())
    assert sent.headers == [
        (b"x-middleware-next", b"1"),
        (b"x-middleware-override-headers-diff", b"x-tenant,cookie"),
        (b"x-middleware-request-x-tenant", b"acme"),
        (b"content-length", b"0"),
    ]


async def test_next_with_headers_emits_response_headers() -> None:
    proxy = Proxy()
    proxy.route("/")(
        lambda req: ContinueResponse(headers={"X-A": "1"}, request_headers={"x-b": "2"})
    )
    sent = await call(proxy, make_scope())
    assert sent.headers == [
        (b"x-middleware-next", b"1"),
        (b"x-middleware-override-headers-diff", b"x-b"),
        (b"x-middleware-request-x-b", b"2"),
        (b"x-a", b"1"),
        (b"content-length", b"0"),
    ]


async def test_changed_headers_and_cookies_emitted() -> None:
    def handler(req: Request) -> Response:
        res = ContinueResponse(headers={"x-a": "1"})
        res.headers["x-b"] = "2"
        res.set_cookie("a", "1")
        res.set_cookie("b", "2")
        return res

    proxy = Proxy()
    proxy.route("/")(handler)
    sent = await call(proxy, make_scope())
    assert sent.headers == [
        (b"x-middleware-next", b"1"),
        (
            b"x-middleware-set-cookie",
            b"a=1; Path=/; SameSite=lax,b=2; Path=/; SameSite=lax",
        ),
        (b"x-a", b"1"),
        (b"content-length", b"0"),
        (b"x-b", b"2"),
        (b"set-cookie", b"a=1; Path=/; SameSite=lax"),
        (b"set-cookie", b"b=2; Path=/; SameSite=lax"),
    ]


async def test_rewrite_emits_middleware_set_cookie() -> None:
    def handler(req: Request) -> Response:
        res = RewriteResponse("/app")
        res.set_cookie("a", "1")
        return res

    proxy = Proxy()
    proxy.route("/")(handler)
    sent = await call(proxy, make_scope())
    assert sent.headers == [
        (b"x-middleware-rewrite", b"/app"),
        (b"x-middleware-set-cookie", b"a=1; Path=/; SameSite=lax"),
        (b"content-length", b"0"),
        (b"set-cookie", b"a=1; Path=/; SameSite=lax"),
    ]


async def test_answered_response_omits_middleware_set_cookie() -> None:
    def handler(req: Request) -> Response:
        res = PlainTextResponse("ok")
        res.set_cookie("a", "1")
        return res

    proxy = Proxy()
    proxy.route("/")(handler)
    sent = await call(proxy, make_scope())
    names = [name for name, _ in sent.headers]
    assert b"set-cookie" in names
    assert b"x-middleware-set-cookie" not in names


async def test_rewrite_emits_middleware_rewrite() -> None:
    proxy = Proxy()
    proxy.route("/")(lambda req: RewriteResponse("https://internal.example.com/api?a=1"))
    sent = await call(proxy, make_scope())
    assert (sent.status, sent.body) == (200, b"")
    assert sent.headers == [
        (b"x-middleware-rewrite", b"https://internal.example.com/api?a=1"),
        (b"content-length", b"0"),
    ]


async def test_caller_content_length_not_overridden() -> None:
    proxy = Proxy()
    proxy.route("/")(lambda req: Response(b"abc", status_code=200, headers={"Content-Length": "3"}))
    sent = await call(proxy, make_scope())
    assert sent.headers == [(b"content-length", b"3"), (b"x-middleware-refresh", b"1")]


async def test_rewrite_encodes_destination() -> None:
    proxy = Proxy()
    proxy.route("/")(lambda req: RewriteResponse("/caf\u00e9 menu"))
    sent = await call(proxy, make_scope())
    assert sent.header_dict["x-middleware-rewrite"] == "/caf%C3%A9%20menu"


async def test_rewrite_does_not_double_encode() -> None:
    proxy = Proxy()
    proxy.route("/")(lambda req: RewriteResponse("/a%20b"))
    sent = await call(proxy, make_scope())
    assert sent.header_dict["x-middleware-rewrite"] == "/a%20b"


async def test_rewrite_with_request_headers_emits_override() -> None:
    proxy = Proxy()
    proxy.route("/")(lambda req: RewriteResponse("/v2", request_headers={"x-a": "1"}))
    sent = await call(proxy, make_scope())
    assert sent.headers == [
        (b"x-middleware-rewrite", b"/v2"),
        (b"x-middleware-override-headers-diff", b"x-a"),
        (b"x-middleware-request-x-a", b"1"),
        (b"content-length", b"0"),
    ]


async def test_rewrite_with_headers_emits_response_headers() -> None:
    proxy = Proxy()
    proxy.route("/")(lambda req: RewriteResponse("/v2", headers={"x-a": "1"}))
    sent = await call(proxy, make_scope())
    assert sent.headers == [
        (b"x-middleware-rewrite", b"/v2"),
        (b"x-a", b"1"),
        (b"content-length", b"0"),
    ]


async def test_redirect_emits_status_and_location() -> None:
    proxy = Proxy()
    proxy.route("/")(lambda req: RedirectResponse("/login", status_code=302, headers={"x-a": "1"}))
    sent = await call(proxy, make_scope())
    assert (sent.status, sent.body) == (302, b"")
    assert sent.headers == [
        (b"x-a", b"1"),
        (b"content-length", b"0"),
        (b"location", b"/login"),
    ]


async def test_json_emits_body_and_headers() -> None:
    proxy = Proxy()
    proxy.route("/")(lambda req: JSONResponse({"ok": True}, status_code=403))
    sent = await call(proxy, make_scope())
    assert (sent.status, sent.body) == (403, b'{"ok":true}')
    assert sent.headers == [
        (b"content-length", b"11"),
        (b"content-type", b"application/json"),
        (b"x-middleware-refresh", b"1"),
    ]


async def test_respond_emits_raw_response() -> None:
    proxy = Proxy()
    proxy.route("/")(lambda req: Response(b"tea", status_code=418, headers={"X-A": "1"}))
    sent = await call(proxy, make_scope())
    assert (sent.status, sent.body) == (418, b"tea")
    assert sent.headers == [
        (b"x-a", b"1"),
        (b"content-length", b"3"),
        (b"x-middleware-refresh", b"1"),
    ]


async def test_response_with_location_marked_final() -> None:
    proxy = Proxy()
    proxy.route("/")(
        lambda req: JSONResponse({"id": 1}, status_code=201, headers={"location": "/items/1"})
    )
    sent = await call(proxy, make_scope())
    assert (sent.status, sent.body) == (201, b'{"id":1}')
    assert sent.header_dict["location"] == "/items/1"
    assert sent.header_dict["x-middleware-refresh"] == "1"


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
async def test_redirect_not_marked_final(status: int) -> None:
    proxy = Proxy()
    proxy.route("/")(lambda req: RedirectResponse("/login", status_code=status))
    sent = await call(proxy, make_scope())
    assert "x-middleware-refresh" not in sent.header_dict


async def test_hand_built_redirect_marked_final() -> None:
    proxy = Proxy()
    proxy.route("/")(lambda req: Response(status_code=302, headers={"location": "/login"}))
    sent = await call(proxy, make_scope())
    assert sent.header_dict["x-middleware-refresh"] == "1"


# ---------------------------------------------------------------------------
# Path matching
# ---------------------------------------------------------------------------


async def test_request_app_is_proxy() -> None:
    seen: list[Any] = []
    proxy = Proxy(fallback=capture(seen, lambda req: req.app))
    proxy.route("/")(capture(seen, lambda req: req.app))
    await call(proxy, make_scope("/"))
    await call(proxy, make_scope("/other"))
    assert seen == [proxy, proxy]


async def test_path_matches_whole_path() -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route("/about")(lambda req: Response(status_code=200))
    assert (await call(proxy, make_scope("/about/team"))).status == 404


# ---------------------------------------------------------------------------
# Trailing slash
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("pattern", "path", "expected"),
    [
        ("/about", "/about", {}),
        ("/about", "/about/", {}),
        ("/about/", "/about", {}),
        ("/about/", "/about/", {}),
        ("/users/{id}", "/users/1/", {"id": "1"}),
        ("/dashboard/{path:path}", "/dashboard", {"path": ""}),
        ("/dashboard/{path:path}", "/dashboard/a/", {"path": "a/"}),
    ],
    ids=[
        "exact",
        "add-slash",
        "strip-slash",
        "exact-slash",
        "param-add-slash",
        "path-param-empty",
        "path-param-keeps-slash",
    ],
)
async def test_trailing_slash_optional(pattern: str, path: str, expected: dict[str, Any]) -> None:
    seen: list[dict[str, Any]] = []
    proxy = Proxy(fallback=not_found)
    proxy.route(pattern)(capture(seen, lambda req: req.path_params))
    await call(proxy, make_scope(path))
    assert seen == [expected]


async def test_trailing_slash_request_path_unchanged() -> None:
    seen: list[str] = []
    proxy = Proxy()
    proxy.route("/about")(capture(seen, lambda req: req.url.path))
    await call(proxy, make_scope("/about/"))
    assert seen == ["/about/"]


async def test_trailing_slash_exact_route_wins() -> None:
    proxy = Proxy()
    proxy.route("/about")(lambda req: Response(status_code=201))
    proxy.route("/about/")(lambda req: Response(status_code=202))
    assert (await call(proxy, make_scope("/about"))).status == 201
    assert (await call(proxy, make_scope("/about/"))).status == 202


async def test_trailing_slash_exact_match_beats_earlier_route() -> None:
    proxy = Proxy()
    proxy.route("/users/{id}")(lambda req: Response(status_code=201))
    proxy.route("/{path:path}")(lambda req: Response(status_code=202))
    assert (await call(proxy, make_scope("/users/1/"))).status == 202


async def test_trailing_slash_other_form_keeps_route_order() -> None:
    proxy = Proxy()
    proxy.route("/users/{id}/")(lambda req: Response(status_code=201))
    proxy.route("/users/{name}/")(lambda req: Response(status_code=202))
    assert (await call(proxy, make_scope("/users/1"))).status == 201


async def test_strict_keeps_route_order() -> None:
    proxy = Proxy(strict=True)
    proxy.route("/users/{id}")(lambda req: Response(status_code=201))
    proxy.route("/{path:path}")(lambda req: Response(status_code=202))
    assert (await call(proxy, make_scope("/users/1/"))).status == 202


async def test_trailing_slash_only_one_optional() -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route("/about")(lambda req: Response(status_code=200))
    assert (await call(proxy, make_scope("/about//"))).status == 404


async def test_trailing_slash_root_route() -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route("/")(lambda req: Response(status_code=200))
    assert (await call(proxy, make_scope("/"))).status == 200
    assert (await call(proxy, make_scope("//"))).status == 200


async def test_trailing_slash_with_root_path() -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route("/users")(lambda req: Response(status_code=200))
    scope = make_scope("/app/users/")
    scope["root_path"] = "/app"
    assert (await call(proxy, scope)).status == 200


async def test_trailing_slash_respects_host() -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route("/about", host="a.example.com")(lambda req: Response(status_code=200))
    assert (await call(proxy, make_scope("/about/", host="a.example.com"))).status == 200
    assert (await call(proxy, make_scope("/about/", host="b.example.com"))).status == 404


async def test_trailing_slash_respects_methods() -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route("/about", methods=["POST"])(lambda req: Response(status_code=200))
    assert (await call(proxy, make_scope("/about/", method="POST"))).status == 200
    assert (await call(proxy, make_scope("/about/", method="GET"))).status == 404


@pytest.mark.parametrize(
    ("pattern", "exact", "other"),
    [
        ("/about", "/about", "/about/"),
        ("/about/", "/about/", "/about"),
        ("/users/{id}", "/users/1", "/users/1/"),
        ("/dashboard/{path:path}", "/dashboard/", "/dashboard"),
    ],
    ids=["no-slash", "slash", "param", "path-param"],
)
async def test_strict_requires_exact_match(pattern: str, exact: str, other: str) -> None:
    proxy = Proxy(fallback=not_found, strict=True)
    proxy.route(pattern)(lambda req: Response(status_code=200))
    assert (await call(proxy, make_scope(exact))).status == 200
    assert (await call(proxy, make_scope(other))).status == 404


@pytest.mark.parametrize(
    ("pattern", "path", "expected"),
    [
        ("/users/{name}", "/users/alice", {"name": "alice"}),
        ("/users/{name:str}", "/users/alice", {"name": "alice"}),
        ("/users/{id:int}", "/users/42", {"id": 42}),
        ("/price/{amount:float}", "/price/9.5", {"amount": 9.5}),
        (
            "/items/{uid:uuid}",
            "/items/12345678-1234-5678-1234-567812345678",
            {"uid": uuid.UUID("12345678-1234-5678-1234-567812345678")},
        ),
        ("/files/{rest:path}", "/files/a/b/c.txt", {"rest": "a/b/c.txt"}),
    ],
    ids=["default", "str", "int", "float", "uuid", "path"],
)
async def test_path_params_converted(pattern: str, path: str, expected: dict[str, Any]) -> None:
    seen: list[dict[str, Any]] = []
    proxy = Proxy()
    proxy.route(pattern)(capture(seen, lambda req: req.path_params))
    await call(proxy, make_scope(path))
    assert seen == [expected]
    assert type(seen[0][next(iter(expected))]) is type(next(iter(expected.values())))


@pytest.mark.parametrize(
    ("pattern", "path"),
    [
        ("/users/{name}", "/users/a/b"),
        ("/users/{id:int}", "/users/abc"),
        ("/users/{id:int}", "/users/-1"),
        ("/items/{uid:uuid}", "/items/not-a-uuid"),
    ],
    ids=["str-with-slash", "int-not-digits", "int-negative", "uuid-invalid"],
)
async def test_path_param_mismatch_falls_through(pattern: str, path: str) -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route(pattern)(lambda req: ContinueResponse())
    assert (await call(proxy, make_scope(path))).status == 404


async def test_first_matching_route_wins() -> None:
    proxy = Proxy()
    proxy.route("/users/{id:int}")(lambda req: Response(status_code=201))
    proxy.route("/users/{name}")(lambda req: Response(status_code=202))
    assert (await call(proxy, make_scope("/users/7"))).status == 201
    assert (await call(proxy, make_scope("/users/bob"))).status == 202


# ---------------------------------------------------------------------------
# Method matching
# ---------------------------------------------------------------------------


async def test_methods_filter() -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route("/", methods=["post", "PUT"])(lambda req: Response(status_code=200))
    assert (await call(proxy, make_scope(method="POST"))).status == 200
    assert (await call(proxy, make_scope(method="PUT"))).status == 200
    assert (await call(proxy, make_scope(method="GET"))).status == 404


async def test_methods_omitted_matches_all() -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route("/")(lambda req: Response(status_code=200))
    for method in ["GET", "HEAD", "POST", "PROPFIND"]:
        assert (await call(proxy, make_scope(method=method))).status == 200


async def test_get_route_also_matches_head() -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route("/", methods=["GET"])(lambda req: Response(status_code=200))
    assert (await call(proxy, make_scope(method="HEAD"))).status == 200
    assert (await call(proxy, make_scope(method="POST"))).status == 404


# ---------------------------------------------------------------------------
# Host matching
# ---------------------------------------------------------------------------


async def test_host_literal() -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route("/", host="api.example.com")(lambda req: Response(status_code=200))
    assert (await call(proxy, make_scope(host="api.example.com"))).status == 200
    assert (await call(proxy, make_scope(host="www.example.com"))).status == 404


async def test_host_ignores_port() -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route("/", host="api.example.com")(lambda req: Response(status_code=200))
    assert (await call(proxy, make_scope(host="api.example.com:8443"))).status == 200


async def test_host_is_case_sensitive() -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route("/", host="api.example.com")(lambda req: Response(status_code=200))
    assert (await call(proxy, make_scope(host="API.example.com"))).status == 404


async def test_host_ipv6_literal() -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route("/", host="[::1]")(lambda req: Response(status_code=200))
    assert (await call(proxy, make_scope(host="[::1]:8080"))).status == 200


async def test_host_found_among_other_headers() -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route("/", host="api.example.com")(lambda req: Response(status_code=200))
    scope = make_scope(host=None)
    scope["headers"] = [(b"accept", b"*/*"), (b"host", b"api.example.com")]
    assert (await call(proxy, scope)).status == 200


async def test_host_route_needs_host_header() -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route("/", host="server.internal")(lambda req: Response(status_code=200))
    assert (await call(proxy, make_scope(host=None))).status == 404


async def test_path_matched_after_root_path() -> None:
    proxy = Proxy(fallback=not_found)
    proxy.route("/users")(lambda req: Response(status_code=200))
    scope = make_scope("/app/users")
    scope["root_path"] = "/app"
    assert (await call(proxy, scope)).status == 200


async def test_host_params_merged_with_path_params() -> None:
    seen: list[dict[str, Any]] = []
    proxy = Proxy()
    proxy.route("/users/{id:int}", host="{tenant}.example.com")(
        capture(seen, lambda req: req.path_params)
    )
    await call(proxy, make_scope("/users/3", host="acme.example.com"))
    assert seen == [{"tenant": "acme", "id": 3}]


async def test_host_capture_matches_across_dots() -> None:
    seen: list[dict[str, Any]] = []
    proxy = Proxy()
    proxy.route("/", host="{tenant}.example.com")(capture(seen, lambda req: req.path_params))
    await call(proxy, make_scope(host="a.b.example.com"))
    assert seen == [{"tenant": "a.b"}]


# ---------------------------------------------------------------------------
# Route registration errors
# ---------------------------------------------------------------------------


def test_path_must_start_with_slash() -> None:
    with pytest.raises(ValueError, match='invalid route path "users": must start with "/"'):
        Proxy().route("users")


def test_host_must_not_be_path() -> None:
    with pytest.raises(ValueError, match='invalid route host "/x"'):
        Proxy().route("/", host="/x")


def test_param_in_host_and_path_rejected() -> None:
    with pytest.raises(ValueError, match='"id" captured in both host and path'):
        Proxy().route("/{id}", host="{id}.example.com")


def test_duplicate_path_param_rejected() -> None:
    with pytest.raises(ValueError, match="Duplicated param name id"):
        Proxy().route("/{id}/{id}")


def test_unknown_convertor_rejected() -> None:
    with pytest.raises(AssertionError, match="Unknown path convertor 'bogus'"):
        Proxy().route("/{id:bogus}")


def test_methods_accepts_any_collection() -> None:
    proxy = Proxy()
    proxy.route("/a", methods=("GET",))(lambda req: ContinueResponse())
    proxy.route("/b", methods={"POST"})(lambda req: ContinueResponse())


def test_methods_string_rejected() -> None:
    with pytest.raises(TypeError, match='invalid route methods "GET"'):
        Proxy().route("/", methods="GET")


def test_methods_empty_rejected() -> None:
    with pytest.raises(ValueError, match="invalid route methods: must not be empty"):
        Proxy().route("/", methods=[])


def test_route_returns_handler_unchanged() -> None:
    def handler(req: Request) -> Response:
        return ContinueResponse()

    assert Proxy().route("/")(handler) is handler


# ---------------------------------------------------------------------------
# Fallback
# ---------------------------------------------------------------------------


async def test_default_fallback_is_next() -> None:
    sent = await call(Proxy(), make_scope("/anything"))
    assert sent.headers == [(b"x-middleware-next", b"1"), (b"content-length", b"0")]


async def test_fallback_response() -> None:
    sent = await call(Proxy(fallback=Response(status_code=404)), make_scope())
    assert sent.status == 404


async def test_fallback_handler_in_constructor() -> None:
    seen: list[dict[str, Any]] = []
    proxy = Proxy(fallback=capture(seen, lambda req: req.path_params))
    sent = await call(proxy, make_scope("/nope"))
    assert sent.headers == [(b"x-middleware-next", b"1"), (b"content-length", b"0")]
    assert seen == [{}]


async def test_sync_fallback_handler() -> None:
    proxy = Proxy(fallback=lambda req: Response(status_code=410))
    assert (await call(proxy, make_scope())).status == 410


async def test_fallback_decorator_overrides_constructor() -> None:
    proxy = Proxy(fallback=Response(status_code=500))

    @proxy.fallback
    async def fallback(req: Request) -> Response:
        return Response(status_code=404)

    assert (await call(proxy, make_scope())).status == 404


# ---------------------------------------------------------------------------
# Handler invocation
# ---------------------------------------------------------------------------


async def test_handler_receives_vercel_request() -> None:
    seen: list[Request] = []
    proxy = Proxy()
    proxy.route("/q")(capture(seen, lambda req: req))
    await call(proxy, make_scope("/q", query_string=b"a=1"))
    assert type(seen[0]) is Request
    assert seen[0].query_params["a"] == "1"


async def test_async_handler_runs_on_event_loop_thread() -> None:
    threads: list[int] = []
    proxy = Proxy()

    @proxy.route("/")
    async def handler(req: Request) -> Response:
        threads.append(threading.get_ident())
        return ContinueResponse()

    await call(proxy, make_scope())
    assert threads == [threading.get_ident()]


async def test_sync_handler_runs_in_worker_thread() -> None:
    threads: list[int] = []
    proxy = Proxy()

    @proxy.route("/")
    def handler(req: Request) -> Response:
        threads.append(threading.get_ident())
        return ContinueResponse()

    await call(proxy, make_scope())
    assert len(threads) == 1
    assert threads[0] != threading.get_ident()


async def test_async_partial_handler() -> None:
    async def handler(req: Request, status: int) -> Response:
        return Response(status_code=status)

    proxy = Proxy()
    proxy.route("/")(functools.partial(handler, status=204))
    assert (await call(proxy, make_scope())).status == 204


async def test_async_callable_object_handler() -> None:
    class Handler:
        async def __call__(self, req: Request) -> Response:
            return Response(status_code=205)

    proxy = Proxy()
    proxy.route("/")(Handler())
    assert (await call(proxy, make_scope())).status == 205


async def test_handler_returning_non_response_raises() -> None:
    proxy = Proxy()

    def handler(req: Request) -> Any:
        return {"not": "a response"}

    proxy.route("/")(handler)
    with pytest.raises(TypeError, match="expected vercel.proxy.Response, got builtins.dict"):
        await call(proxy, make_scope())


async def test_handler_returning_starlette_response_raises() -> None:
    proxy = Proxy()
    proxy.route("/")(lambda req: cast(Any, starlette.responses.JSONResponse({})))
    with pytest.raises(
        TypeError,
        match="expected vercel.proxy.Response, got starlette.responses.JSONResponse",
    ):
        await call(proxy, make_scope())


async def test_handler_exception_propagates() -> None:
    proxy = Proxy()

    @proxy.route("/")
    def handler(req: Request) -> Response:
        raise KeyError("boom")

    with pytest.raises(KeyError, match="boom"):
        await call(proxy, make_scope())


async def test_handler_reads_body() -> None:
    async def receive() -> Message:
        return {"type": "http.request", "body": b"secret", "more_body": False}

    async def handler(req: Request) -> Response:
        return PlainTextResponse(await req.body())

    proxy = Proxy()
    proxy.route("/", methods=["POST"])(handler)
    messages: list[Message] = []

    async def send(message: Message) -> None:
        messages.append(message)

    await proxy(make_scope(method="POST"), receive, send)
    assert messages[1]["body"] == b"secret"


# ---------------------------------------------------------------------------
# ASGI scopes
# ---------------------------------------------------------------------------


async def test_lifespan() -> None:
    incoming: list[Message] = [{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]
    sent: list[Message] = []

    async def receive() -> Message:
        return incoming.pop(0)

    async def send(message: Message) -> None:
        sent.append(message)

    await Proxy()({"type": "lifespan"}, receive, send)
    assert sent == [
        {"type": "lifespan.startup.complete"},
        {"type": "lifespan.shutdown.complete"},
    ]


def make_websocket_scope(
    path: str = "/",
    *,
    scheme: str | None = "wss",
    host: str = "example.com",
) -> dict[str, Any]:
    scope = make_scope(path, host=host)
    del scope["method"]
    if scheme is None:
        del scope["scheme"]
    else:
        scope["scheme"] = scheme
    return {
        **scope,
        "type": "websocket",
        "headers": [*scope["headers"], (b"upgrade", b"websocket")],
        "subprotocols": [],
        "extensions": {"websocket.http.response": {}},
    }


async def call_websocket(proxy: Proxy, scope: dict[str, Any]) -> Sent:
    messages: list[Message] = []

    async def send(message: Message) -> None:
        messages.append(message)

    await proxy(scope, never_receive, send)
    assert [m["type"] for m in messages] == [
        "websocket.http.response.start",
        "websocket.http.response.body",
    ]
    return Sent(messages[0]["status"], messages[0]["headers"], messages[1]["body"])


async def test_websocket_body_is_empty() -> None:
    async def handler(req: Request) -> Response:
        return PlainTextResponse(f"[{(await req.body()).decode()}]")

    proxy = Proxy()
    proxy.route("/chat")(handler)
    sent = await call_websocket(proxy, make_websocket_scope("/chat"))
    assert sent.body == b"[]"


async def test_websocket_continue() -> None:
    proxy = Proxy()
    proxy.route("/chat")(lambda req: ContinueResponse(request_headers={"x-user": "1"}))
    sent = await call_websocket(proxy, make_websocket_scope("/chat"))
    assert (sent.status, sent.body) == (200, b"")
    assert sent.headers == [
        (b"x-middleware-next", b"1"),
        (b"x-middleware-override-headers-diff", b"x-user"),
        (b"x-middleware-request-x-user", b"1"),
        (b"content-length", b"0"),
    ]


async def test_websocket_rewrite() -> None:
    proxy = Proxy()
    proxy.route("/chat")(lambda req: RewriteResponse("/rooms/1"))
    sent = await call_websocket(proxy, make_websocket_scope("/chat"))
    assert sent.header_dict["x-middleware-rewrite"] == "/rooms/1"


async def test_websocket_answered() -> None:
    proxy = Proxy()
    proxy.route("/chat")(lambda req: PlainTextResponse("forbidden", status_code=403))
    sent = await call_websocket(proxy, make_websocket_scope("/chat"))
    assert (sent.status, sent.body) == (403, b"forbidden")
    assert sent.header_dict["x-middleware-refresh"] == "1"


async def test_websocket_redirect() -> None:
    proxy = Proxy()
    proxy.route("/chat")(lambda req: RedirectResponse("/login"))
    sent = await call_websocket(proxy, make_websocket_scope("/chat"))
    assert sent.status == 307
    assert sent.header_dict["location"] == "/login"


async def test_websocket_fallback() -> None:
    sent = await call_websocket(Proxy(), make_websocket_scope("/chat"))
    assert sent.header_dict["x-middleware-next"] == "1"


async def test_websocket_routed_as_get() -> None:
    proxy = Proxy()
    proxy.route("/chat", methods=["POST"])(lambda req: Response(status_code=405))
    proxy.route("/chat")(lambda req: PlainTextResponse(f"{req.method} {req.headers['upgrade']}"))
    sent = await call_websocket(proxy, make_websocket_scope("/chat"))
    assert sent.body == b"GET websocket"


@pytest.mark.parametrize(
    ("scheme", "expected"),
    [("wss", "https"), ("ws", "http"), (None, "http")],
    ids=["wss", "ws", "missing"],
)
async def test_websocket_scheme_mapped(scheme: str | None, expected: str) -> None:
    seen: list[Any] = []
    proxy = Proxy()
    proxy.route("/chat")(capture(seen, lambda req: str(req.url)))
    await call_websocket(proxy, make_websocket_scope("/chat", scheme=scheme))
    assert seen == [f"{expected}://example.com/chat"]


async def test_websocket_host_route() -> None:
    proxy = Proxy()
    proxy.route("/chat", host="other.example.com")(lambda req: Response(status_code=404))
    proxy.route("/chat", host="{tenant}.example.com")(
        lambda req: PlainTextResponse(req.path_params["tenant"])
    )
    sent = await call_websocket(proxy, make_websocket_scope("/chat", host="acme.example.com"))
    assert sent.body == b"acme"


@pytest.mark.parametrize("extensions", ["missing", None, {}], ids=["missing", "none", "empty"])
async def test_websocket_requires_denial_extension(extensions: Any) -> None:
    async def send(message: Message) -> None:
        raise AssertionError("nothing should be sent")

    scope = make_websocket_scope()
    if extensions == "missing":
        del scope["extensions"]
    else:
        scope["extensions"] = extensions
    with pytest.raises(RuntimeError, match="websocket denial response extension"):
        await Proxy()(scope, never_receive, send)


async def test_unknown_scope_rejected() -> None:
    async def send(message: Message) -> None:
        raise AssertionError("nothing should be sent")

    with pytest.raises(RuntimeError, match='unsupported ASGI scope type "custom"'):
        await Proxy()({"type": "custom"}, never_receive, send)
