from __future__ import annotations

import base64
import json
import threading
import time
from collections.abc import Awaitable, Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from vercel.headers import set_headers
from vercel.oidc import token as oidc


def _token(payload: object) -> str:
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    return f"header.{encoded}.signature"


@dataclass
class TokenFile:
    path: Path
    now: float
    read: Mock
    monotonic: float = 0.0

    def advance(self, seconds: float) -> None:
        self.now += seconds
        self.monotonic += seconds

    def replace(self, lifetime: float = 3600) -> str:
        token = _token({"exp": self.now + lifetime})
        replacement = self.path.with_suffix(".replacement")
        replacement.write_text(f"{token}\n", encoding="utf-8")
        replacement.replace(self.path)
        return token


@pytest.fixture
def token_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TokenFile]:
    monkeypatch.delenv("VERCEL_OIDC_TOKEN", raising=False)
    set_headers(None)
    oidc._clear_cached_oidc_token()
    state = TokenFile(tmp_path / "token", time.time(), Mock(wraps=oidc._read_file_oidc_token))
    monkeypatch.setattr(oidc, "_OIDC_TOKEN_PATH", state.path)
    monkeypatch.setattr(
        oidc, "time", SimpleNamespace(time=lambda: state.now, monotonic=lambda: state.monotonic)
    )
    monkeypatch.setattr(oidc, "_read_file_oidc_token", state.read)
    monkeypatch.setattr(oidc, "find_project_info", Mock(side_effect=RuntimeError("no project")))
    yield state
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


async def test_caches_until_expiration_buffer_then_reopens_replaced_file(
    token_file: TokenFile, lookup: Callable[[], Awaitable[str]]
) -> None:
    first = token_file.replace()
    assert await lookup() == first
    token_file.advance(1800)
    second = token_file.replace()
    assert await lookup() == first
    token_file.advance(1739)
    assert await lookup() == first
    assert token_file.read.call_count == 1

    token_file.advance(1)
    assert await lookup() == second
    assert token_file.read.call_count == 2


async def test_rotation_accepts_a_replacement_with_shorter_expiration(
    token_file: TokenFile, lookup: Callable[[], Awaitable[str]]
) -> None:
    first = token_file.replace(lifetime=120)
    assert await lookup() == first

    token_file.advance(60)
    second = token_file.replace(lifetime=50)
    assert await lookup() == second
    assert token_file.read.call_count == 2

    token_file.advance(50)
    with pytest.raises(oidc.VercelOidcTokenError):
        await lookup()


async def test_refresh_buffer_is_not_delayed_by_retry_interval(
    token_file: TokenFile, lookup: Callable[[], Awaitable[str]]
) -> None:
    first = token_file.replace(lifetime=80)
    assert await lookup() == first
    second = token_file.replace()

    token_file.advance(19)
    assert await lookup() == first
    assert token_file.read.call_count == 1
    token_file.advance(1)
    assert await lookup() == second
    assert token_file.read.call_count == 2


@pytest.mark.parametrize("lifetime", [20, 60])
async def test_token_discovered_inside_refresh_buffer_is_rechecked_on_next_lookup(
    token_file: TokenFile, lookup: Callable[[], Awaitable[str]], lifetime: float
) -> None:
    first = token_file.replace(lifetime=lifetime)
    assert await lookup() == first
    second = token_file.replace()

    assert await lookup() == second
    assert token_file.read.call_count == 2
    assert await lookup() == second
    assert token_file.read.call_count == 2


async def test_unchanged_token_inside_refresh_buffer_retries_no_later_than_expiration(
    token_file: TokenFile, lookup: Callable[[], Awaitable[str]]
) -> None:
    first = token_file.replace(lifetime=20)
    assert await lookup() == first
    assert await lookup() == first
    assert token_file.read.call_count == 2

    token_file.advance(19)
    assert await lookup() == first
    assert token_file.read.call_count == 2
    token_file.advance(1)
    second = token_file.replace()
    assert await lookup() == second
    assert token_file.read.call_count == 3


async def test_short_lived_file_token_does_not_trigger_cli_refresh(
    token_file: TokenFile, lookup: Callable[[], Awaitable[str]]
) -> None:
    first = token_file.replace(lifetime=120)
    assert await lookup() == first
    token_file.advance(60)
    assert await lookup() == first
    second = token_file.replace()
    token_file.advance(29)
    assert await lookup() == first
    assert token_file.read.call_count == 2
    token_file.advance(1)
    assert await lookup() == second


@pytest.mark.parametrize("failure", [PermissionError("denied"), FileNotFoundError(), None])
async def test_failed_refresh_keeps_valid_cache_but_never_returns_expired_token(
    token_file: TokenFile,
    lookup: Callable[[], Awaitable[str]],
    failure: OSError | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = token_file.replace(lifetime=80)
    assert await lookup() == first
    # Simulate an unreadable, temporarily missing, or empty replacement file.
    if failure is None:
        token_file.path.write_text("", encoding="utf-8")
    else:
        monkeypatch.setattr(Path, "read_text", Mock(side_effect=failure))
    token_file.advance(30)
    assert await lookup() == first
    token_file.advance(30)
    assert await lookup() == first
    token_file.advance(20)
    with pytest.raises(oidc.VercelOidcTokenError):
        await lookup()
    # Expiry forces another read, even before the 30-second retry interval.
    assert token_file.read.call_count == 4


async def test_missing_file_is_retried_after_startup(
    token_file: TokenFile, lookup: Callable[[], Awaitable[str]]
) -> None:
    with pytest.raises(oidc.VercelOidcTokenError):
        await lookup()
    token = token_file.replace()
    token_file.advance(29)
    with pytest.raises(oidc.VercelOidcTokenError):
        await lookup()
    assert token_file.read.call_count == 1
    token_file.advance(1)
    assert await lookup() == token


@pytest.mark.parametrize("clock_adjustment", [-3600, 3600])
async def test_missing_file_retry_ignores_wall_clock_adjustments(
    token_file: TokenFile,
    lookup: Callable[[], Awaitable[str]],
    clock_adjustment: float,
) -> None:
    with pytest.raises(oidc.VercelOidcTokenError):
        await lookup()
    token_file.now += clock_adjustment
    fresh = token_file.replace()

    with pytest.raises(oidc.VercelOidcTokenError):
        await lookup()
    token_file.advance(29)
    with pytest.raises(oidc.VercelOidcTokenError):
        await lookup()
    assert token_file.read.call_count == 1
    token_file.advance(1)
    assert await lookup() == fresh
    assert token_file.read.call_count == 2


@pytest.mark.parametrize("clock_adjustment", [-10, 10])
async def test_cached_file_retry_ignores_wall_clock_adjustments(
    token_file: TokenFile,
    lookup: Callable[[], Awaitable[str]],
    clock_adjustment: float,
) -> None:
    first = token_file.replace(lifetime=120)
    assert await lookup() == first
    token_file.advance(80)
    assert await lookup() == first
    token_file.now += clock_adjustment
    second = token_file.replace()

    assert await lookup() == first
    token_file.advance(29)
    assert await lookup() == first
    assert token_file.read.call_count == 2
    token_file.advance(1)
    assert await lookup() == second
    assert token_file.read.call_count == 3


async def test_wall_clock_expiration_bypasses_retry_backoff(
    token_file: TokenFile, lookup: Callable[[], Awaitable[str]]
) -> None:
    first = token_file.replace(lifetime=20)
    assert await lookup() == first
    assert await lookup() == first
    assert token_file.read.call_count == 2

    token_file.now += 20
    second = token_file.replace()
    assert await lookup() == second
    assert token_file.read.call_count == 3


async def test_refresh_window_uses_wall_clock_time(
    token_file: TokenFile, lookup: Callable[[], Awaitable[str]]
) -> None:
    first = token_file.replace(lifetime=120)
    assert await lookup() == first
    second = token_file.replace()

    token_file.monotonic += 60
    assert await lookup() == first
    assert token_file.read.call_count == 1
    token_file.now += 60
    assert await lookup() == second
    assert token_file.read.call_count == 2


@pytest.mark.parametrize(
    "content",
    [
        b"",
        b"not-a-jwt",
        b"\xff",
        _token([]).encode(),
        _token({}).encode(),
        *(
            _token({"exp": exp}).encode()
            for exp in [0, True, "later", None, float("nan"), float("inf")]
        ),
    ],
)
async def test_invalid_file_falls_back_and_recovers(
    token_file: TokenFile, lookup: Callable[[], Awaitable[str]], content: bytes
) -> None:
    token_file.path.write_bytes(content)
    with pytest.raises(oidc.VercelOidcTokenError):
        await lookup()
    fresh = token_file.replace()
    token_file.advance(30)
    assert await lookup() == fresh


async def test_existing_sources_take_precedence_even_after_file_is_cached(
    token_file: TokenFile,
    lookup: Callable[[], Awaitable[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token_file.replace()
    await lookup()
    env = _token({"exp": token_file.now + 7200, "sub": "env"})
    header = _token({"exp": token_file.now + 7200, "sub": "header"})
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", env)
    assert await lookup() == env
    set_headers({"x-vercel-oidc-token": header})
    assert await lookup() == header
    set_headers(None)
    assert await lookup() == header
    assert token_file.read.call_count == 1


@pytest.mark.parametrize("async_lookup", [False, True])
async def test_expired_environment_token_falls_back_to_file(
    token_file: TokenFile, monkeypatch: pytest.MonkeyPatch, async_lookup: bool
) -> None:
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", _token({"exp": 1}))
    fresh = token_file.replace(lifetime=120)
    actual = (
        await oidc.get_vercel_oidc_token_async() if async_lookup else oidc.get_vercel_oidc_token()
    )
    assert actual == fresh


@pytest.mark.parametrize("async_lookup", [False, True])
async def test_unusable_file_allows_local_cli_refresh(
    token_file: TokenFile, monkeypatch: pytest.MonkeyPatch, async_lookup: bool
) -> None:
    token_file.replace(lifetime=-1)
    fresh = _token({"exp": token_file.now + 3600})
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


async def test_sync_and_async_share_cache_and_async_reads_off_event_loop(
    token_file: TokenFile, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = token_file.replace(lifetime=120)
    assert oidc.get_vercel_oidc_token() == first
    assert await oidc.get_vercel_oidc_token_async() == first
    assert token_file.read.call_count == 1

    loop_thread = threading.get_ident()
    read = token_file.read

    def read_in_worker() -> tuple[str, float] | None:
        assert threading.get_ident() != loop_thread
        return read()

    monkeypatch.setattr(oidc, "_read_file_oidc_token", read_in_worker)
    token_file.advance(60)
    second = token_file.replace()
    assert await oidc.get_vercel_oidc_token_async() == second
    assert oidc.get_vercel_oidc_token() == second
    assert read.call_count == 2


def test_concurrent_lookups_share_one_file_read_per_refresh(token_file: TokenFile) -> None:
    first = token_file.replace(lifetime=120)
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert list(pool.map(lambda _: oidc.get_vercel_oidc_token(), range(16))) == [first] * 16
        token_file.advance(60)
        second = token_file.replace()
        assert list(pool.map(lambda _: oidc.get_vercel_oidc_token(), range(16))) == [second] * 16
    assert token_file.read.call_count == 2
