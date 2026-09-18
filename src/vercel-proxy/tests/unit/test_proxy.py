import asyncio
import functools
import json
from typing import Any

import pytest

from vercel.proxy import Proxy, Request, Response

# ---------------------------------------------------------------------------
# ASGI test helpers
# ---------------------------------------------------------------------------

_NEXT = [
    {"type": "http.response.start", "status": 200, "headers": [(b"x-middleware-next", b"1")]},
    {"type": "http.response.body", "body": b""},
]


def _scope(
    *,
    method: str = "GET",
    path: str = "/",
    headers: list[tuple[bytes, bytes]] | None = None,
) -> dict[str, Any]:
    return {
        "type": "http",
        "method": method,
        "path": path,
        "query_string": b"",
        "scheme": "https",
        "headers": headers or [],
        "server": ("localhost", 443),
    }


def _drive(
    app: Proxy,
    path: str = "/",
    *,
    method: str = "GET",
    headers: list[tuple[bytes, bytes]] | None = None,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []

    async def _run() -> None:
        async def receive() -> dict[str, Any]:  # noqa: RUF029
            return {"type": "http.disconnect"}

        async def send(message: dict[str, Any]) -> None:  # noqa: RUF029
            events.append(message)

        await app(_scope(method=method, path=path, headers=headers), receive, send)

    asyncio.run(_run())
    return events


# ---------------------------------------------------------------------------
# Basic routing
# ---------------------------------------------------------------------------


def test_route_matched() -> None:
    app = Proxy()

    @app.route("/hello")
    def handler(request: Request) -> Response:
        return Response.next()

    assert _drive(app, "/hello") == _NEXT


def test_route_no_match_falls_to_default_fallback() -> None:
    app = Proxy()

    @app.route("/hello")
    def handler(request: Request) -> Response:
        return Response.respond(status=403)

    assert _drive(app, "/other") == _NEXT


def test_first_match_wins() -> None:
    app = Proxy()

    @app.route("/path")
    def first(request: Request) -> Response:
        return Response.respond(status=200, body=b"first")

    @app.route("/path")
    def second(request: Request) -> Response:
        return Response.respond(status=200, body=b"second")

    assert _drive(app, "/path") == [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"first"},
    ]


# ---------------------------------------------------------------------------
# path_params populated
# ---------------------------------------------------------------------------


def test_path_params_populated() -> None:
    captured: dict[str, str] = {}

    app = Proxy()

    @app.route("/users/{id}")
    def handler(request: Request) -> Response:
        captured.update(request.path_params)
        return Response.next()

    assert _drive(app, "/users/42") == _NEXT
    assert captured == {"id": "42"}


def test_path_params_empty_for_exact_match() -> None:
    captured: dict[str, str] = {}

    app = Proxy()

    @app.route("/health")
    def handler(request: Request) -> Response:
        captured.update(request.path_params)
        return Response.next()

    assert _drive(app, "/health") == _NEXT
    assert captured == {}


# ---------------------------------------------------------------------------
# Method filtering
# ---------------------------------------------------------------------------


def test_method_match() -> None:
    app = Proxy()

    @app.route("/resource", methods=["GET"])
    def handler(request: Request) -> Response:
        return Response.respond(status=200, body=b"ok")

    assert _drive(app, "/resource", method="GET") == [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"ok"},
    ]


def test_method_no_match_falls_through() -> None:
    app = Proxy(fallback=Response.respond(status=418))

    @app.route("/resource", methods=["GET"])
    def handler(request: Request) -> Response:
        return Response.respond(status=200, body=b"ok")

    assert _drive(app, "/resource", method="POST") == [
        {"type": "http.response.start", "status": 418, "headers": []},
        {"type": "http.response.body", "body": b""},
    ]


def test_method_case_insensitive() -> None:
    app = Proxy()

    @app.route("/resource", methods=["GET"])
    def handler(request: Request) -> Response:
        return Response.respond(status=200, body=b"ok")

    assert _drive(app, "/resource", method="get") == [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"ok"},
    ]


def test_no_methods_matches_all() -> None:
    app = Proxy()

    @app.route("/resource")
    def handler(request: Request) -> Response:
        return Response.respond(status=200, body=b"ok")

    expected = [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"ok"},
    ]
    for method in ("GET", "POST", "PUT", "DELETE"):
        assert _drive(app, "/resource", method=method) == expected


# ---------------------------------------------------------------------------
# Fallback
# ---------------------------------------------------------------------------


def test_constructor_fallback_static() -> None:
    app = Proxy(fallback=Response.respond(status=418))
    assert _drive(app, "/unmatched") == [
        {"type": "http.response.start", "status": 418, "headers": []},
        {"type": "http.response.body", "body": b""},
    ]


def test_constructor_fallback_callable() -> None:
    app = Proxy(fallback=lambda req: Response.respond(status=418))
    assert _drive(app, "/unmatched") == [
        {"type": "http.response.start", "status": 418, "headers": []},
        {"type": "http.response.body", "body": b""},
    ]


def test_decorator_fallback_overrides_constructor() -> None:
    app = Proxy(fallback=Response.respond(status=418))

    @app.fallback
    def handle_rest(request: Request) -> Response:
        return Response.respond(status=200, body=b"fallback")

    assert _drive(app, "/unmatched") == [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"fallback"},
    ]


def test_decorator_fallback_async() -> None:
    app = Proxy()

    @app.fallback
    async def handle_rest(request: Request) -> Response:
        return Response.respond(status=202)

    assert _drive(app, "/unmatched") == [
        {"type": "http.response.start", "status": 202, "headers": []},
        {"type": "http.response.body", "body": b""},
    ]


# ---------------------------------------------------------------------------
# Sync and async handlers
# ---------------------------------------------------------------------------


def test_sync_handler() -> None:
    app = Proxy()

    @app.route("/sync")
    def handler(request: Request) -> Response:
        return Response.respond(status=200, body=b"sync")

    assert _drive(app, "/sync") == [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"sync"},
    ]


def test_async_handler() -> None:
    app = Proxy()

    @app.route("/async")
    async def handler(request: Request) -> Response:
        return Response.respond(status=200, body=b"async")

    assert _drive(app, "/async") == [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"async"},
    ]


# ---------------------------------------------------------------------------
# Trailing slash
# ---------------------------------------------------------------------------


def test_trailing_slash_transparent() -> None:
    app = Proxy()

    @app.route("/users/{id}")
    def handler(request: Request) -> Response:
        return Response.respond(status=200, body=b"matched")

    expected = [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"matched"},
    ]
    assert _drive(app, "/users/42") == expected
    assert _drive(app, "/users/42/") == expected


def test_strict_rejects_trailing_slash() -> None:
    app = Proxy(strict=True, fallback=Response.respond(status=418))

    @app.route("/users/{id}")
    def handler(request: Request) -> Response:
        return Response.respond(status=200, body=b"matched")

    assert _drive(app, "/users/42") == [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"matched"},
    ]
    assert _drive(app, "/users/42/") == [
        {"type": "http.response.start", "status": 418, "headers": []},
        {"type": "http.response.body", "body": b""},
    ]


# ---------------------------------------------------------------------------
# ASGI protocol emission — continuing
# ---------------------------------------------------------------------------


def test_next_emits_middleware_next() -> None:
    app = Proxy()

    @app.route("/")
    def handler(request: Request) -> Response:
        return Response.next()

    assert _drive(app, "/") == _NEXT


def test_rewrite_emits_middleware_rewrite() -> None:
    app = Proxy()

    @app.route("/")
    def handler(request: Request) -> Response:
        return Response.rewrite("/target")

    assert _drive(app, "/") == [
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"x-middleware-rewrite", b"/target")],
        },
        {"type": "http.response.body", "body": b""},
    ]


def test_rewrite_non_ascii_destination_percent_encoded() -> None:
    app = Proxy()

    @app.route("/")
    def handler(request: Request) -> Response:
        return Response.rewrite("/café")

    assert _drive(app, "/") == [
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"x-middleware-rewrite", b"/caf%C3%A9")],
        },
        {"type": "http.response.body", "body": b""},
    ]


def test_rewrite_already_encoded_destination_not_double_encoded() -> None:
    app = Proxy()

    @app.route("/")
    def handler(request: Request) -> Response:
        return Response.rewrite("/caf%C3%A9")

    assert _drive(app, "/") == [
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"x-middleware-rewrite", b"/caf%C3%A9")],
        },
        {"type": "http.response.body", "body": b""},
    ]


def test_next_with_headers_emits_diff_header() -> None:
    app = Proxy()

    @app.route("/")
    def handler(request: Request) -> Response:
        return Response.next(headers={"x-injected": "yes"})

    assert _drive(app, "/") == [
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"x-middleware-next", b"1"),
                (b"x-middleware-override-headers-diff", b"x-injected"),
                (b"x-middleware-request-x-injected", b"yes"),
            ],
        },
        {"type": "http.response.body", "body": b""},
    ]


def test_next_with_none_header_dropped() -> None:
    app = Proxy()

    @app.route("/")
    def handler(request: Request) -> Response:
        return Response.next(headers={"x-to-drop": None})

    assert _drive(app, "/") == [
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"x-middleware-next", b"1"),
                (b"x-middleware-override-headers-diff", b"x-to-drop"),
            ],
        },
        {"type": "http.response.body", "body": b""},
    ]


def test_next_with_mixed_set_and_drop() -> None:
    app = Proxy()

    @app.route("/")
    def handler(request: Request) -> Response:
        return Response.next(headers={"x-set": "value", "x-drop": None})

    assert _drive(app, "/") == [
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"x-middleware-next", b"1"),
                (b"x-middleware-override-headers-diff", b"x-set,x-drop"),
                (b"x-middleware-request-x-set", b"value"),
            ],
        },
        {"type": "http.response.body", "body": b""},
    ]


def test_next_with_headers_diff_excludes_original_headers() -> None:
    app = Proxy()

    @app.route("/")
    def handler(request: Request) -> Response:
        return Response.next(headers={"x-a": "1", "x-b": "2"})

    assert _drive(app, "/", headers=[(b"x-existing", b"value")]) == [
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"x-middleware-next", b"1"),
                (b"x-middleware-override-headers-diff", b"x-a,x-b"),
                (b"x-middleware-request-x-a", b"1"),
                (b"x-middleware-request-x-b", b"2"),
            ],
        },
        {"type": "http.response.body", "body": b""},
    ]


# ---------------------------------------------------------------------------
# ASGI protocol emission — terminating
# ---------------------------------------------------------------------------


def test_redirect_emits_middleware_redirect() -> None:
    app = Proxy()

    @app.route("/")
    def handler(request: Request) -> Response:
        return Response.redirect("/login")

    assert _drive(app, "/") == [
        {
            "type": "http.response.start",
            "status": 307,
            "headers": [(b"x-middleware-redirect", b"/login")],
        },
        {"type": "http.response.body", "body": b""},
    ]


def test_respond_emits_body_and_status() -> None:
    app = Proxy()

    @app.route("/")
    def handler(request: Request) -> Response:
        return Response.respond(status=403, body=b"forbidden")

    assert _drive(app, "/") == [
        {"type": "http.response.start", "status": 403, "headers": []},
        {"type": "http.response.body", "body": b"forbidden"},
    ]


def test_json_emits_content_type_and_body() -> None:
    app = Proxy()

    @app.route("/")
    def handler(request: Request) -> Response:
        return Response.json({"ok": True})

    assert _drive(app, "/") == [
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json")],
        },
        {"type": "http.response.body", "body": json.dumps({"ok": True}).encode()},
    ]


# ---------------------------------------------------------------------------
# Host matching
# ---------------------------------------------------------------------------


def test_host_exact_match() -> None:
    app = Proxy()

    @app.route("/", host="myapp.com")
    def handler(request: Request) -> Response:
        return Response.respond(status=200, body=b"matched")

    assert _drive(app, "/", headers=[(b"host", b"myapp.com")]) == [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"matched"},
    ]


def test_host_no_match_falls_through() -> None:
    app = Proxy(fallback=Response.respond(status=418))

    @app.route("/", host="myapp.com")
    def handler(request: Request) -> Response:
        return Response.respond(status=200, body=b"matched")

    assert _drive(app, "/", headers=[(b"host", b"other.com")]) == [
        {"type": "http.response.start", "status": 418, "headers": []},
        {"type": "http.response.body", "body": b""},
    ]


def test_host_param_captured_in_path_params() -> None:
    captured: dict[str, str] = {}

    app = Proxy()

    @app.route("/{path:path}", host="{tenant}.myapp.com")
    def handler(request: Request) -> Response:
        captured.update(request.path_params)
        return Response.respond(status=200, body=b"matched")

    assert _drive(app, "/dashboard", headers=[(b"host", b"acme.myapp.com")]) == [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"matched"},
    ]
    assert captured == {"tenant": "acme", "path": "dashboard"}


def test_host_ipv6_does_not_mangle() -> None:
    app = Proxy(fallback=Response.respond(status=418))

    @app.route("/", host="myapp.com")
    def handler(request: Request) -> Response:
        return Response.respond(status=200, body=b"matched")

    # IPv6 host should fall through cleanly, not raise or match wrongly
    assert _drive(app, "/", headers=[(b"host", b"[::1]:8080")]) == [
        {"type": "http.response.start", "status": 418, "headers": []},
        {"type": "http.response.body", "body": b""},
    ]


def test_host_port_stripped_before_matching() -> None:
    app = Proxy()

    @app.route("/", host="myapp.com")
    def handler(request: Request) -> Response:
        return Response.respond(status=200, body=b"matched")

    assert _drive(app, "/", headers=[(b"host", b"myapp.com:3000")]) == [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"matched"},
    ]


def test_host_path_overlap_raises_at_registration() -> None:
    app = Proxy()
    with pytest.raises(ValueError, match="appear in both host and path"):

        @app.route("/{tenant}", host="{tenant}.myapp.com")
        def handler(request: Request) -> Response:
            return Response.next()


# ---------------------------------------------------------------------------
# Callable objects and functools.partial handlers
# ---------------------------------------------------------------------------


def test_sync_callable_object() -> None:
    class Handler:
        def __call__(self, request: Request) -> Response:
            return Response.respond(status=200, body=b"callable")

    app = Proxy()
    app.route("/")(Handler())

    assert _drive(app, "/") == [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"callable"},
    ]


def test_async_callable_object() -> None:
    class Handler:
        async def __call__(self, request: Request) -> Response:
            return Response.respond(status=202, body=b"async callable")

    app = Proxy()
    app.route("/")(Handler())

    assert _drive(app, "/") == [
        {"type": "http.response.start", "status": 202, "headers": []},
        {"type": "http.response.body", "body": b"async callable"},
    ]


def test_sync_partial_handler() -> None:
    def handler(prefix: bytes, request: Request) -> Response:
        return Response.respond(status=200, body=prefix + b" partial")

    app = Proxy()
    app.route("/")(functools.partial(handler, b"sync"))

    assert _drive(app, "/") == [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"sync partial"},
    ]


def test_async_partial_handler() -> None:
    async def handler(prefix: bytes, request: Request) -> Response:
        return Response.respond(status=200, body=prefix + b" partial")

    app = Proxy()
    app.route("/")(functools.partial(handler, b"async"))

    assert _drive(app, "/") == [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"async partial"},
    ]


# ---------------------------------------------------------------------------
# Non-http scopes
# ---------------------------------------------------------------------------


def test_lifespan_scope() -> None:
    messages = [
        {"type": "lifespan.startup"},
        {"type": "lifespan.shutdown"},
    ]
    sent: list[dict[str, Any]] = []

    async def _run() -> None:
        it = iter(messages)

        async def receive() -> dict[str, Any]:  # noqa: RUF029
            return next(it)

        async def send(message: dict[str, Any]) -> None:  # noqa: RUF029
            sent.append(message)

        await Proxy()({"type": "lifespan"}, receive, send)

    asyncio.run(_run())
    assert sent == [
        {"type": "lifespan.startup.complete"},
        {"type": "lifespan.shutdown.complete"},
    ]


@pytest.mark.parametrize(
    ("scope_type", "match"),
    [
        ("websocket", "unexpected ASGI scope type 'websocket'"),
        ("mqtt", "unexpected ASGI scope type 'mqtt'"),
    ],
)
def test_unexpected_scope_raises(scope_type: str, match: str) -> None:
    async def _run() -> None:
        async def receive() -> dict[str, Any]:  # noqa: RUF029
            return {}

        async def send(message: dict[str, Any]) -> None:  # noqa: RUF029
            pass

        await Proxy()({"type": scope_type}, receive, send)

    with pytest.raises(RuntimeError, match=match):
        asyncio.run(_run())
