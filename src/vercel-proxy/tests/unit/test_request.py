import dataclasses

import pytest

from vercel.proxy import Headers, Params, Request


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


def test_query_params_first_value_wins_for_duplicates() -> None:
    r = Request._from_asgi_scope(_scope(query_string=b"x=1&x=2"))  # noqa: SLF001
    assert r.query_params["x"] == "1"


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
