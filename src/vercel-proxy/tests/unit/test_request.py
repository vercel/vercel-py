import dataclasses

import pytest

from vercel.proxy import Cookies, Headers, Params, Request


def _scope(
    *,
    method: str = "GET",
    path: str = "/",
    query_string: bytes = b"",
    scheme: str = "https",
    headers: list[tuple[bytes, bytes]] | None = None,
    server: tuple[str, int] | None = ("localhost", 443),
) -> dict:
    return {
        "type": "http",
        "method": method,
        "path": path,
        "query_string": query_string,
        "scheme": scheme,
        "headers": headers or [],
        "server": server,
    }


# ---------------------------------------------------------------------------
# Construction via _from_asgi_scope
# ---------------------------------------------------------------------------


def test_basic_construction() -> None:
    r = Request._from_asgi_scope(_scope(method="GET", path="/hello"))  # noqa: SLF001
    assert r.method == "GET"
    assert r.path == "/hello"


def test_method_normalised_to_uppercase() -> None:
    r = Request._from_asgi_scope(_scope(method="post"))  # noqa: SLF001
    assert r.method == "POST"


def test_method_already_uppercase() -> None:
    r = Request._from_asgi_scope(_scope(method="DELETE"))  # noqa: SLF001
    assert r.method == "DELETE"


def test_nonstandard_method_preserved() -> None:
    r = Request._from_asgi_scope(_scope(method="PROPFIND"))  # noqa: SLF001
    assert r.method == "PROPFIND"


def test_url_no_query_string() -> None:
    r = Request._from_asgi_scope(  # noqa: SLF001
        _scope(scheme="https", path="/api", headers=[(b"host", b"example.com")])
    )
    assert r.url == "https://example.com/api"


def test_url_with_query_string() -> None:
    r = Request._from_asgi_scope(  # noqa: SLF001
        _scope(path="/search", query_string=b"q=hello&page=2", headers=[(b"host", b"example.com")])
    )
    assert r.url == "https://example.com/search?q=hello&page=2"


def test_url_host_from_header_preferred_over_server() -> None:
    r = Request._from_asgi_scope(  # noqa: SLF001
        _scope(
            scheme="https",
            path="/",
            headers=[(b"host", b"example.com")],
            server=("127.0.0.1", 8000),
        )
    )
    assert r.url == "https://example.com/"


def test_url_host_from_server_when_no_header() -> None:
    r = Request._from_asgi_scope(  # noqa: SLF001
        _scope(scheme="http", path="/", headers=[], server=("myhost", 8080))
    )
    assert r.url == "http://myhost:8080/"


def test_url_host_omits_port_443() -> None:
    r = Request._from_asgi_scope(  # noqa: SLF001
        _scope(scheme="https", path="/", headers=[], server=("myhost", 443))
    )
    assert "443" not in r.url


def test_url_host_omits_port_80() -> None:
    r = Request._from_asgi_scope(  # noqa: SLF001
        _scope(scheme="http", path="/", headers=[], server=("myhost", 80))
    )
    assert "80" not in r.url


# ---------------------------------------------------------------------------
# Query params
# ---------------------------------------------------------------------------


def test_query_params_parsed() -> None:
    r = Request._from_asgi_scope(_scope(query_string=b"foo=bar&baz=qux"))  # noqa: SLF001
    assert r.query_params["foo"] == "bar"
    assert r.query_params["baz"] == "qux"


def test_query_params_first_value_wins_for_getitem() -> None:
    r = Request._from_asgi_scope(_scope(query_string=b"x=1&x=2"))  # noqa: SLF001
    assert r.query_params["x"] == "1"


def test_query_params_get_all_returns_all_values() -> None:
    r = Request._from_asgi_scope(_scope(query_string=b"tag=python&tag=async&tag=web"))  # noqa: SLF001
    assert r.query_params.get_all("tag") == ["python", "async", "web"]


def test_query_params_get_all_missing_key_returns_empty() -> None:
    r = Request._from_asgi_scope(_scope(query_string=b"x=1"))  # noqa: SLF001
    assert r.query_params.get_all("missing") == []


def test_query_params_empty_when_no_query_string() -> None:
    r = Request._from_asgi_scope(_scope(query_string=b""))  # noqa: SLF001
    assert len(r.query_params) == 0


def test_query_params_blank_values_kept() -> None:
    r = Request._from_asgi_scope(_scope(query_string=b"empty="))  # noqa: SLF001
    assert r.query_params["empty"] == ""


# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------


def test_headers_populated_from_scope() -> None:
    r = Request._from_asgi_scope(  # noqa: SLF001
        _scope(headers=[(b"content-type", b"application/json")])
    )
    assert isinstance(r.headers, Headers)
    assert r.headers["content-type"] == "application/json"


# ---------------------------------------------------------------------------
# path_params
# ---------------------------------------------------------------------------


def test_path_params_default_empty() -> None:
    r = Request._from_asgi_scope(_scope())  # noqa: SLF001
    assert len(r.path_params) == 0


def test_path_params_populated() -> None:
    r = Request._from_asgi_scope(_scope(), path_params={"id": "42"})  # noqa: SLF001
    assert r.path_params["id"] == "42"


def test_path_params_is_params() -> None:
    r = Request._from_asgi_scope(_scope(), path_params={"id": "42"})  # noqa: SLF001
    assert isinstance(r.path_params, Params)


# ---------------------------------------------------------------------------
# Immutability (frozen dataclass)
# ---------------------------------------------------------------------------


def test_frozen_raises_on_assignment() -> None:
    r = Request._from_asgi_scope(_scope())  # noqa: SLF001
    with pytest.raises(dataclasses.FrozenInstanceError):
        r.method = "POST"  # type: ignore[misc]


def test_frozen_raises_on_delete() -> None:
    r = Request._from_asgi_scope(_scope())  # noqa: SLF001
    with pytest.raises(dataclasses.FrozenInstanceError):
        del r.method  # type: ignore[misc]


def test_query_params_immutable() -> None:
    r = Request._from_asgi_scope(_scope(query_string=b"x=1"))  # noqa: SLF001
    with pytest.raises(TypeError):
        r.query_params["x"] = "2"  # type: ignore[index]


def test_path_params_immutable() -> None:
    r = Request._from_asgi_scope(_scope(), path_params={"id": "1"})  # noqa: SLF001
    with pytest.raises(TypeError):
        r.path_params["id"] = "2"  # type: ignore[index]


# ---------------------------------------------------------------------------
# cookies
# ---------------------------------------------------------------------------


def test_cookies_empty_when_no_header() -> None:
    r = Request._from_asgi_scope(_scope())  # noqa: SLF001
    assert isinstance(r.cookies, Cookies)
    assert dict(r.cookies) == {}


def test_cookies_single() -> None:
    r = Request._from_asgi_scope(  # noqa: SLF001
        _scope(headers=[(b"cookie", b"session=abc123")])
    )
    assert r.cookies["session"] == "abc123"


def test_cookies_multiple() -> None:
    r = Request._from_asgi_scope(  # noqa: SLF001
        _scope(headers=[(b"cookie", b"a=1; b=2; c=3")])
    )
    assert r.cookies["a"] == "1"
    assert r.cookies["b"] == "2"
    assert r.cookies["c"] == "3"


def test_cookies_value_with_equals() -> None:
    r = Request._from_asgi_scope(  # noqa: SLF001
        _scope(headers=[(b"cookie", b"token=abc=def==")])
    )
    assert r.cookies["token"] == "abc=def=="


def test_cookies_whitespace_around_separator() -> None:
    r = Request._from_asgi_scope(  # noqa: SLF001
        _scope(headers=[(b"cookie", b"a=1;  b=2")])
    )
    assert r.cookies["a"] == "1"
    assert r.cookies["b"] == "2"


def test_cookies_multiple_cookie_headers_joined() -> None:
    r = Request._from_asgi_scope(  # noqa: SLF001
        _scope(headers=[(b"cookie", b"a=1"), (b"cookie", b"b=2")])
    )
    assert r.cookies["a"] == "1"
    assert r.cookies["b"] == "2"


def test_cookies_quoted_value_verbatim() -> None:
    # RFC 6265: no unquoting — value returned as-is
    r = Request._from_asgi_scope(  # noqa: SLF001
        _scope(headers=[(b"cookie", b'token="hello world"')])
    )
    assert r.cookies["token"] == '"hello world"'


def test_cookies_no_value() -> None:
    r = Request._from_asgi_scope(  # noqa: SLF001
        _scope(headers=[(b"cookie", b"flag=")])
    )
    assert r.cookies["flag"] == ""


def test_cookies_duplicate_name_first_wins() -> None:
    r = Request._from_asgi_scope(  # noqa: SLF001
        _scope(headers=[(b"cookie", b"x=first; x=second")])
    )
    assert r.cookies["x"] == "first"


def test_cookies_duplicate_across_headers_first_wins() -> None:
    r = Request._from_asgi_scope(  # noqa: SLF001
        _scope(headers=[(b"cookie", b"a=first"), (b"cookie", b"a=second")])
    )
    assert r.cookies["a"] == "first"
