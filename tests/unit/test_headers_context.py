from __future__ import annotations

from collections.abc import Generator, Mapping

import pytest

from vercel.headers import (
    HeadersContext,
    context_aware_thread,
    get_headers,
    get_headers_context,
    headers_from_asgi_scope,
    headers_from_wsgi_environ,
    set_headers,
)


@pytest.fixture(autouse=True)
def isolated_headers_context() -> Generator[None, None, None]:
    set_headers(None)
    try:
        yield
    finally:
        set_headers(None)


def test_headers_context_run_installs_and_restores_headers() -> None:
    set_headers({"x-current": "outer"})
    context = get_headers_context()
    set_headers({"x-current": "inner"})

    def read_headers() -> Mapping[str, str] | None:
        return get_headers()

    assert context.run(read_headers) == {"x-current": "outer"}
    assert get_headers() == {"x-current": "inner"}


def test_headers_context_use_installs_and_restores_headers() -> None:
    set_headers({"x-current": "outer"})
    context = get_headers_context()
    set_headers({"x-current": "inner"})

    with context.use():
        assert get_headers() == {"x-current": "outer"}

    assert get_headers() == {"x-current": "inner"}


def test_headers_context_use_accepts_explicit_headers() -> None:
    set_headers({"x-current": "outer"})

    with HeadersContext({"x-current": "explicit"}).use():
        assert get_headers() == {"x-current": "explicit"}

    assert get_headers() == {"x-current": "outer"}


def test_headers_context_restores_after_exception() -> None:
    set_headers({"x-current": "outer"})
    context = get_headers_context()
    set_headers({"x-current": "inner"})

    def fail() -> None:
        assert get_headers() == {"x-current": "outer"}
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        context.run(fail)

    assert get_headers() == {"x-current": "inner"}


def test_headers_context_use_restores_after_exception() -> None:
    set_headers({"x-current": "outer"})
    context = get_headers_context()
    set_headers({"x-current": "inner"})

    with pytest.raises(RuntimeError, match="boom"):
        with context.use():
            assert get_headers() == {"x-current": "outer"}
            raise RuntimeError("boom")

    assert get_headers() == {"x-current": "inner"}


def test_headers_context_is_snapshot_isolated() -> None:
    headers = {"x-current": "initial"}
    set_headers(headers)
    context = get_headers_context()

    headers["x-current"] = "mutated"
    set_headers({"x-current": "latest"})

    assert context.run(get_headers) == {"x-current": "initial"}
    with pytest.raises(TypeError):
        context.headers["x-current"] = "changed"  # type: ignore[index]


def test_context_aware_thread_runs_with_creation_headers() -> None:
    seen: list[Mapping[str, str] | None] = []
    set_headers({"x-current": "thread"})
    thread = context_aware_thread(target=lambda: seen.append(get_headers()))
    set_headers({"x-current": "main"})

    thread.start()
    thread.join(timeout=5)

    assert seen == [{"x-current": "thread"}]
    assert get_headers() == {"x-current": "main"}


def test_headers_from_asgi_scope_decodes_latin1_headers() -> None:
    scope = {
        "headers": [
            (b"x-test", b"ok"),
            (b"x-city", "S\xe3o Paulo".encode("latin-1")),
        ]
    }

    assert headers_from_asgi_scope(scope) == {
        "x-test": "ok",
        "x-city": "S\xe3o Paulo",
    }


def test_headers_from_asgi_scope_uses_last_duplicate_header() -> None:
    assert headers_from_asgi_scope({"headers": [(b"x-test", b"first"), (b"x-test", b"last")]}) == {
        "x-test": "last"
    }


def test_headers_from_wsgi_environ_extracts_http_headers() -> None:
    assert headers_from_wsgi_environ(
        {
            "CONTENT_TYPE": "application/json",
            "CONTENT_LENGTH": "12",
            "HTTP_X_VERCEL_OIDC_TOKEN": "token",
            "HTTP_CE_VQSMESSAGEID": "msg_1",
            "REQUEST_METHOD": "POST",
        }
    ) == {
        "Content-Type": "application/json",
        "Content-Length": "12",
        "X-Vercel-Oidc-Token": "token",
        "Ce-Vqsmessageid": "msg_1",
    }


def test_headers_from_wsgi_environ_stringifies_values() -> None:
    assert headers_from_wsgi_environ({"HTTP_X_COUNT": 3}) == {"X-Count": "3"}
