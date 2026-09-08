from __future__ import annotations

import asyncio

import pytest

from vercel._internal.core import outcome


def test_value_unwraps() -> None:
    assert outcome.Value(42).unwrap() == 42


def test_an_exception_can_be_a_successful_value() -> None:
    error = RuntimeError("as data")

    assert outcome.Value(error).unwrap() is error


def test_error_raises_the_captured_exception() -> None:
    error = RuntimeError("failed")

    with pytest.raises(RuntimeError) as caught:
        outcome.Error(error).unwrap()

    assert caught.value is error


async def test_value_sets_a_future_result() -> None:
    future = asyncio.Future[int]()

    outcome.Value(42).set_future(future)

    assert await future == 42


async def test_error_sets_a_future_exception() -> None:
    error = RuntimeError("failed")
    future = asyncio.Future[int]()

    outcome.Error(error).set_future(future)

    with pytest.raises(RuntimeError) as caught:
        await future
    assert caught.value is error
