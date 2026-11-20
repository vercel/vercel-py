"""Sync and async surfaces must stay identical in name, signature, and docs."""

import inspect

import pytest

from vercel import drive
from vercel.drive import sync as drive_sync

ASYNC_OPERATIONS = ["get_drive", "get_or_create_drive", "fork_drive", "delete_drive"]
ITERATOR_OPERATIONS = ["query_drives"]
OPERATIONS = ASYNC_OPERATIONS + ITERATOR_OPERATIONS
# Each surface has its own handle, and takes a credentials factory for its mode.
MODE_SPECIFIC = {
    "Drive": "SyncDrive",
    "DriveCredentialsFactory": "SyncDriveCredentialsFactory",
    "DriveServiceOptions": "DriveServiceOptions",
}
ASYNC_ONLY = {"sync", "__version__"}


def test_public_names_match() -> None:
    async_names = {MODE_SPECIFIC.get(name, name) for name in drive.__all__} - ASYNC_ONLY

    assert async_names == set(drive_sync.__all__)


@pytest.mark.parametrize("name", OPERATIONS)
def test_operations_match_apart_from_awaiting(name: str) -> None:
    async_operation = getattr(drive, name)
    sync_operation = getattr(drive_sync, name)

    assert inspect.iscoroutinefunction(async_operation) is (name in ASYNC_OPERATIONS)
    assert not inspect.iscoroutinefunction(sync_operation)
    async_parameters = inspect.signature(async_operation).parameters
    sync_parameters = inspect.signature(sync_operation).parameters
    assert list(async_parameters) == list(sync_parameters)
    for parameter, async_parameter in async_parameters.items():
        sync_parameter = sync_parameters[parameter]
        assert async_parameter.kind == sync_parameter.kind
        assert async_parameter.default == sync_parameter.default
        assert async_parameter.annotation == sync_parameter.annotation


@pytest.mark.parametrize("method", ["fork", "delete", "snapshot"])
def test_handle_methods_match(method: str) -> None:
    async_parameters = inspect.signature(getattr(drive.Drive, method)).parameters
    sync_parameters = inspect.signature(getattr(drive_sync.SyncDrive, method)).parameters

    assert list(async_parameters) == list(sync_parameters)


@pytest.mark.parametrize(
    "name", [n for n in drive.__all__ if n not in {*OPERATIONS, *MODE_SPECIFIC, *ASYNC_ONLY}]
)
def test_shared_symbols_are_the_same_object_in_both_surfaces(name: str) -> None:
    assert getattr(drive, name) is getattr(drive_sync, name)
