"""Sync and async surfaces must stay identical in name, signature, and docs."""

import inspect

import pytest

from vercel import schedules
from vercel.schedules import sync as schedules_sync

ASYNC_OPERATIONS = [
    "create_schedule",
    "get_schedule",
    "delete_schedule",
    "enable_schedule",
    "disable_schedule",
]
ITERATOR_OPERATIONS = ["list_schedules"]
OPERATIONS = ASYNC_OPERATIONS + ITERATOR_OPERATIONS


def test_public_names_match() -> None:
    async_names = {name for name in schedules.__all__ if name != "sync"}

    assert async_names == set(schedules_sync.__all__)


@pytest.mark.parametrize("name", OPERATIONS)
def test_signatures_match(name: str) -> None:
    async_signature = inspect.signature(getattr(schedules, name))
    sync_signature = inspect.signature(getattr(schedules_sync, name))

    assert list(async_signature.parameters) == list(sync_signature.parameters)
    for parameter in async_signature.parameters:
        async_parameter = async_signature.parameters[parameter]
        sync_parameter = sync_signature.parameters[parameter]
        assert async_parameter.kind == sync_parameter.kind
        assert async_parameter.default == sync_parameter.default
        assert async_parameter.annotation == sync_parameter.annotation


@pytest.mark.parametrize("name", OPERATIONS)
def test_docstrings_match(name: str) -> None:
    assert getattr(schedules, name).__doc__ == getattr(schedules_sync, name).__doc__


@pytest.mark.parametrize("name", OPERATIONS)
def test_async_operations_are_coroutines_and_sync_ones_are_not(name: str) -> None:
    async_operation = getattr(schedules, name)

    if name in ITERATOR_OPERATIONS:
        assert not inspect.iscoroutinefunction(async_operation)
    else:
        assert inspect.iscoroutinefunction(async_operation)
    assert not inspect.iscoroutinefunction(getattr(schedules_sync, name))


@pytest.mark.parametrize(
    "name",
    [n for n in schedules.__all__ if n not in OPERATIONS and n != "sync"],
)
def test_shared_symbols_are_the_same_object_in_both_surfaces(name: str) -> None:
    assert getattr(schedules, name) is getattr(schedules_sync, name)
