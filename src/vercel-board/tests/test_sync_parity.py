"""Sync and async surface parity tests for vercel.board and vercel.board.sync."""

from __future__ import annotations

import inspect

import pytest

from vercel import board as board_async
from vercel.board import sync as board_sync

MODE_SPECIFIC = {
    "Board",
    "BoardClient",
    "BoardCredentialsFactory",
    "BoardServiceOptions",
    "board",
    "static_credentials_factory",
}


def test_public_exports_match() -> None:
    async_names = {name for name in board_async.__all__ if name != "sync"}
    sync_names = set(board_sync.__all__)
    assert async_names == sync_names


@pytest.mark.parametrize(
    "name",
    [n for n in board_async.__all__ if n not in MODE_SPECIFIC and n != "sync"],
)
def test_shared_symbols_are_identical(name: str) -> None:
    assert getattr(board_async, name) is getattr(board_sync, name)


@pytest.mark.parametrize("name", sorted(MODE_SPECIFIC))
def test_mode_specific_symbols_differ(name: str) -> None:
    assert getattr(board_async, name) is not getattr(board_sync, name)


def _assert_parameter_parity(async_fn: object, sync_fn: object) -> None:
    async_sig = inspect.signature(async_fn)  # type: ignore[arg-type]
    sync_sig = inspect.signature(sync_fn)  # type: ignore[arg-type]
    assert list(async_sig.parameters) == list(sync_sig.parameters)
    for param_name in async_sig.parameters:
        a_param = async_sig.parameters[param_name]
        s_param = sync_sig.parameters[param_name]
        assert a_param.kind == s_param.kind
        assert a_param.default == s_param.default


def _public_methods(cls: type) -> set[str]:
    return {
        name
        for name, value in inspect.getmembers(cls)
        if not name.startswith("_") and callable(value)
    }


def test_board_factory_and_handle_method_parity() -> None:
    _assert_parameter_parity(board_async.board, board_sync.board)
    assert not inspect.iscoroutinefunction(board_async.board)
    assert not inspect.iscoroutinefunction(board_sync.board)

    async_methods = _public_methods(board_async.Board)
    sync_methods = _public_methods(board_sync.Board)
    assert async_methods == sync_methods
    assert {
        "as_profile",
        "me",
        "discovery",
        "post",
        "reply",
        "edit",
        "delete",
        "react",
        "label",
        "get_posted",
    } <= async_methods

    for method in sorted(async_methods):
        async_method = getattr(board_async.Board, method)
        sync_method = getattr(board_sync.Board, method)
        _assert_parameter_parity(async_method, sync_method)
        if method == "as_profile":
            assert not inspect.iscoroutinefunction(async_method)
            assert not inspect.iscoroutinefunction(sync_method)
        else:
            assert inspect.iscoroutinefunction(async_method)
            assert not inspect.iscoroutinefunction(sync_method)


def test_board_client_method_parity() -> None:
    for method in ("create", "board"):
        _assert_parameter_parity(
            getattr(board_async.BoardClient, method),
            getattr(board_sync.BoardClient, method),
        )
    assert inspect.iscoroutinefunction(board_async.BoardClient.aclose)
    assert not inspect.iscoroutinefunction(board_sync.BoardClient.close)
