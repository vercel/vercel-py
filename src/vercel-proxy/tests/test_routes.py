from __future__ import annotations

import threading

import pytest

from vercel.proxy import PlainTextResponse, Proxy, Request, Route, rewrite
from vercel.proxy.matchers import cookie, header, host, query

from .conftest import invoke, make_scope, response_headers


async def test_unmatched_route_continues_routing() -> None:
    proxy = Proxy(routes=[Route("/dashboard", lambda request: None)])

    headers = response_headers(await invoke(proxy, make_scope("/other")))

    assert headers["x-middleware-next"] == "1"


async def test_matched_none_continues_routing() -> None:
    async def dashboard(request: Request) -> None:
        assert request.path_params["path"] == "settings"
        return None

    proxy = Proxy(routes=[Route("/dashboard/{path:path}", dashboard)])

    headers = response_headers(await invoke(proxy, make_scope("/dashboard/settings")))

    assert headers["x-middleware-next"] == "1"


async def test_dynamic_route_can_rewrite() -> None:
    async def legacy(request: Request):
        return rewrite(f"/new/{request.path_params['path']}")

    proxy = Proxy(routes=[Route("/legacy/{path:path}", legacy)])

    headers = response_headers(await invoke(proxy, make_scope("/legacy/docs/start")))

    assert headers["x-middleware-rewrite"] == "/new/docs/start"


async def test_route_can_end_routing_with_a_normal_response() -> None:
    proxy = Proxy(
        routes=[Route("/health", lambda request: PlainTextResponse("healthy", status_code=202))]
    )

    messages = await invoke(proxy, make_scope("/health"))

    assert messages[0]["status"] == 202
    assert "x-middleware-next" not in response_headers(messages)
    assert messages[-1]["body"] == b"healthy"


async def test_declarative_rewrite_and_redirect_interpolate_route_parameters() -> None:
    rewrite_proxy = Proxy(routes=[Route.rewrite("/legacy/{path:path}", "/new/{path}")])
    redirect_proxy = Proxy(routes=[Route.redirect("/old/{slug}", "/docs/{slug}", status_code=308)])

    rewrite_headers = response_headers(
        await invoke(rewrite_proxy, make_scope("/legacy/guides/start"))
    )
    redirect_messages = await invoke(redirect_proxy, make_scope("/old/python"))
    redirect_headers = response_headers(redirect_messages)

    assert rewrite_headers["x-middleware-rewrite"] == "/new/guides/start"
    assert redirect_messages[0]["status"] == 308
    assert redirect_headers["location"] == "/docs/python"


def test_declarative_destination_rejects_unknown_parameter() -> None:
    with pytest.raises(ValueError, match="unknown route parameter: missing"):
        Route.rewrite("/legacy/{path:path}", "/new/{missing}")


async def test_methods_default_to_all_and_explicit_get_includes_head() -> None:
    all_methods = Proxy(routes=[Route.rewrite("/resource", "/matched")])
    get_only = Proxy(routes=[Route.rewrite("/resource", "/matched", methods=["GET"])])

    assert (
        response_headers(await invoke(all_methods, make_scope("/resource", method="POST")))[
            "x-middleware-rewrite"
        ]
        == "/matched"
    )
    assert (
        response_headers(await invoke(get_only, make_scope("/resource", method="HEAD")))[
            "x-middleware-rewrite"
        ]
        == "/matched"
    )
    assert (
        response_headers(await invoke(get_only, make_scope("/resource", method="POST")))[
            "x-middleware-next"
        ]
        == "1"
    )


async def test_conditions_match_headers_cookies_queries_and_hostname() -> None:
    async def tenant(request: Request):
        return rewrite(f"/tenants/{request.path_params['subdomain']}/{request.path_params['path']}")

    proxy = Proxy(
        routes=[
            Route(
                "/dashboard/{path:path}",
                tenant,
                has=[
                    host("{subdomain}.example.com"),
                    header("x-plan", "pro"),
                    cookie("session"),
                    query("preview", "1"),
                ],
                missing=[header("x-blocked")],
            )
        ]
    )
    scope = make_scope(
        "/dashboard/settings",
        headers={
            "host": "ACME.EXAMPLE.COM:443",
            "x-plan": "pro",
            "cookie": "session=signed",
        },
        query_string="preview=1",
    )

    headers = response_headers(await invoke(proxy, scope))

    assert headers["x-middleware-rewrite"] == "/tenants/acme/settings"


async def test_duplicate_header_values_match_when_any_value_matches() -> None:
    proxy = Proxy(routes=[Route.rewrite("/", "/pro", has=[header("x-plan", "pro")])])
    scope = make_scope("/", headers=[("x-plan", "free"), ("x-plan", "pro")])

    assert response_headers(await invoke(proxy, scope))["x-middleware-rewrite"] == "/pro"


async def test_failed_conditions_fall_through_to_the_next_route() -> None:
    proxy = Proxy(
        routes=[
            Route.rewrite("/dashboard", "/pro", has=[header("x-plan", "pro")]),
            Route.rewrite("/dashboard", "/free"),
        ]
    )

    headers = response_headers(await invoke(proxy, make_scope("/dashboard")))

    assert headers["x-middleware-rewrite"] == "/free"


def test_host_and_path_parameters_cannot_overlap() -> None:
    with pytest.raises(ValueError, match="overlap: tenant"):
        Route("/{tenant}", lambda request: None, has=[host("{tenant}.example.com")])


async def test_sync_endpoints_run_outside_the_event_loop_thread() -> None:
    event_loop_thread = threading.get_ident()
    endpoint_thread: int | None = None

    def endpoint(request: Request):
        nonlocal endpoint_thread
        endpoint_thread = threading.get_ident()
        return rewrite("/sync")

    proxy = Proxy(routes=[Route("/", endpoint)])
    headers = response_headers(await invoke(proxy))

    assert headers["x-middleware-rewrite"] == "/sync"
    assert endpoint_thread is not None
    assert endpoint_thread != event_loop_thread


async def test_route_must_return_response_or_none() -> None:
    proxy = Proxy(routes=[Route("/", lambda request: False)])  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="proxy route"):
        await invoke(proxy)
