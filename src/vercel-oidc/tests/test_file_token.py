from __future__ import annotations

import base64
import json
import threading
import time
from collections.abc import Awaitable, Callable, Iterator
from pathlib import Path
from unittest.mock import Mock

import pytest

from vercel.headers import set_headers
from vercel.oidc import token as oidc


def _token(*, lifetime: float = 3600, subject: str = "file") -> str:
    payload = {"exp": time.time() + lifetime, "sub": subject}
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    return f"header.{encoded}.signature"


@pytest.fixture
def token_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    monkeypatch.delenv("VERCEL_OIDC_TOKEN", raising=False)
    set_headers(None)
    oidc._clear_cached_oidc_token()
    path = tmp_path / "token"
    monkeypatch.setenv("VERCEL_OIDC_TOKEN_FILE", str(path))
    monkeypatch.setattr(oidc, "find_project_info", Mock(side_effect=RuntimeError("no project")))
    yield path
    set_headers(None)
    oidc._clear_cached_oidc_token()


@pytest.fixture(params=["context", "sync", "async"])
def lookup(request: pytest.FixtureRequest) -> Callable[[], Awaitable[str]]:
    async def get() -> str:
        if request.param == "context":
            return oidc.get_vercel_oidc_token_sync()
        if request.param == "sync":
            return oidc.get_vercel_oidc_token()
        return await oidc.get_vercel_oidc_token_async()

    return get


async def test_rereads_replaced_file_on_each_call(
    token_file: Path, lookup: Callable[[], Awaitable[str]]
) -> None:
    first = _token()
    token_file.write_text(f" \n{first}\n ", encoding="utf-8")
    assert await lookup() == first
    second = _token(lifetime=1200, subject="rotated")
    replacement = token_file.with_suffix(".replacement")
    replacement.write_text(second, encoding="utf-8")
    replacement.replace(token_file)
    assert await lookup() == second


async def test_file_precedes_environment_token(
    token_file: Path, lookup: Callable[[], Awaitable[str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", _token(subject="environment"))
    current = _token()
    token_file.write_text(current, encoding="utf-8")
    assert await lookup() == current
    oidc.find_project_info.assert_not_called()  # type: ignore[attr-defined]


async def test_request_header_precedes_missing_file(
    token_file: Path, lookup: Callable[[], Awaitable[str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    header = _token(subject="request")
    set_headers({"x-vercel-oidc-token": header})
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", _token(subject="environment"))
    read = Mock(side_effect=AssertionError("must not read file"))
    monkeypatch.setattr(Path, "read_text", read)
    assert await lookup() == header
    read.assert_not_called()


async def test_file_precedes_remembered_header_without_live_request(
    token_file: Path, lookup: Callable[[], Awaitable[str]]
) -> None:
    header = _token(subject="request")
    set_headers({"x-vercel-oidc-token": header})
    assert await lookup() == header
    set_headers(None)
    current = _token()
    token_file.write_text(current, encoding="utf-8")
    assert await lookup() == current


async def test_missing_file_never_falls_back_and_is_retried_next_call(
    token_file: Path, lookup: Callable[[], Awaitable[str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", _token(subject="environment"))
    with pytest.raises(FileNotFoundError) as error:
        await lookup()
    assert error.value.filename == str(token_file)
    oidc.find_project_info.assert_not_called()  # type: ignore[attr-defined]
    fresh = _token()
    token_file.write_text(fresh, encoding="utf-8")
    assert await lookup() == fresh
    token_file.unlink()
    with pytest.raises(FileNotFoundError):
        await lookup()


async def test_unreadable_file_never_falls_back(
    token_file: Path, lookup: Callable[[], Awaitable[str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", _token(subject="environment"))
    monkeypatch.setattr(Path, "read_text", Mock(side_effect=PermissionError("denied")))
    with pytest.raises(PermissionError, match="denied"):
        await lookup()
    oidc.find_project_info.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.parametrize("content", ["", " \n\t"])
async def test_empty_file_includes_path_and_never_falls_back(
    token_file: Path,
    lookup: Callable[[], Awaitable[str]],
    monkeypatch: pytest.MonkeyPatch,
    content: str,
) -> None:
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", _token(subject="environment"))
    token_file.write_text(content, encoding="utf-8")
    with pytest.raises(oidc.VercelOidcTokenError) as error:
        await lookup()
    assert str(error.value) == f"The Vercel OIDC token file is empty: {token_file}"
    oidc.find_project_info.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.parametrize("configured", [None, ""])
async def test_unconfigured_file_uses_environment_without_reading(
    token_file: Path,
    lookup: Callable[[], Awaitable[str]],
    monkeypatch: pytest.MonkeyPatch,
    configured: str | None,
) -> None:
    if configured is None:
        monkeypatch.delenv("VERCEL_OIDC_TOKEN_FILE")
    else:
        monkeypatch.setenv("VERCEL_OIDC_TOKEN_FILE", configured)
    env = _token(subject="environment")
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", env)
    read = Mock()
    monkeypatch.setattr(Path, "read_text", read)
    assert await lookup() == env
    read.assert_not_called()


@pytest.mark.parametrize("async_lookup", [False, True])
@pytest.mark.parametrize("lifetime", [-1, 60])
async def test_expired_or_near_expiry_file_token_allows_local_cli_refresh(
    token_file: Path, monkeypatch: pytest.MonkeyPatch, async_lookup: bool, lifetime: float
) -> None:
    token_file.write_text(_token(lifetime=lifetime), encoding="utf-8")
    fresh = _token(subject="refreshed")
    monkeypatch.setattr(oidc, "find_project_info", lambda: {"projectId": "prj_test"})
    refresh = Mock(side_effect=lambda: monkeypatch.setenv("VERCEL_OIDC_TOKEN", fresh))

    async def refresh_async() -> None:
        refresh()

    monkeypatch.setattr(oidc, "refresh_token", refresh)
    monkeypatch.setattr(oidc, "refresh_token_async", refresh_async)
    actual = (
        await oidc.get_vercel_oidc_token_async() if async_lookup else oidc.get_vercel_oidc_token()
    )
    assert actual == fresh
    refresh.assert_called_once_with()


async def test_async_reads_off_event_loop(
    token_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = _token()
    token_file.write_text(token, encoding="utf-8")
    loop_thread = threading.get_ident()
    read = oidc._read_file_oidc_token

    def read_in_worker() -> str | None:
        assert threading.get_ident() != loop_thread
        return read()

    monkeypatch.setattr(oidc, "_read_file_oidc_token", read_in_worker)
    assert await oidc.get_vercel_oidc_token_async() == token
