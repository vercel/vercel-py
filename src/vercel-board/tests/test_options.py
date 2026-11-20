"""Behavioral tests for BoardCredentials and BoardServiceOptions."""

from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import ValidationError

from vercel.board import (
    BoardCredentials,
    BoardServiceOptions,
    BoardValidationError,
    static_credentials_factory,
    sync as board_sync,
)


def test_board_credentials_is_frozen_and_redacts_token() -> None:
    creds = BoardCredentials(token="super-secret-token-value")

    assert creds.token == "super-secret-token-value"
    assert "super-secret-token-value" not in repr(creds)
    assert "super-secret-token-value" not in str(creds)

    with pytest.raises(AttributeError):
        creds.token = "other"  # type: ignore[misc]


def test_board_credentials_rejects_empty_token() -> None:
    with pytest.raises(BoardValidationError):
        BoardCredentials(token="")


def test_options_defaults_and_board_url_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BOARD_URL", raising=False)

    opts = BoardServiceOptions()
    assert opts.url is None
    assert opts.api_base_url == "https://api.vercel.com"
    assert opts.team is None
    assert opts.credentials_factory is None
    assert opts.timeout == timedelta(seconds=30)

    monkeypatch.setenv("BOARD_URL", "https://board.example.com/api/board/")
    env_opts = BoardServiceOptions()
    assert env_opts.url == "https://board.example.com/api/board"

    explicit_none = BoardServiceOptions(url=None)
    assert explicit_none.url is None


def test_options_coerces_timeout_and_rejects_invalid_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("BOARD_URL", raising=False)

    assert BoardServiceOptions(timeout=15).timeout == timedelta(seconds=15)
    assert BoardServiceOptions(timeout=2.5).timeout == timedelta(seconds=2.5)
    assert BoardServiceOptions(timeout=timedelta(minutes=1)).timeout == timedelta(minutes=1)

    for bad_timeout in (0, -5, True, False, float("nan"), float("inf"), "30s"):
        with pytest.raises((ValidationError, BoardValidationError, TypeError)):
            BoardServiceOptions(timeout=bad_timeout)  # type: ignore[arg-type]


def test_options_rejects_non_callable_credentials_factory_and_invalid_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("BOARD_URL", raising=False)

    with pytest.raises((ValidationError, BoardValidationError, TypeError)):
        BoardServiceOptions(credentials_factory="bare-token")  # type: ignore[arg-type]

    with pytest.raises((ValidationError, BoardValidationError)):
        BoardServiceOptions(team="bad/team")

    with pytest.raises((ValidationError, BoardValidationError)):
        BoardServiceOptions(team="bad team")

    with pytest.raises((ValidationError, BoardValidationError)):
        BoardServiceOptions(api_base_url="http://example.com")


async def test_static_credentials_factories_return_redacted_credentials() -> None:
    async_factory = static_credentials_factory("async-secret")
    async_creds = await async_factory()
    assert isinstance(async_creds, BoardCredentials)
    assert async_creds.token == "async-secret"

    sync_factory = board_sync.static_credentials_factory("sync-secret")
    sync_creds = sync_factory()
    assert isinstance(sync_creds, BoardCredentials)
    assert sync_creds.token == "sync-secret"
