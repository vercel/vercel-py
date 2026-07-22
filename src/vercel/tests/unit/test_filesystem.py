"""Behavioral tests for the internal filesystem client layer."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TypeVar

import pytest

from vercel._internal.fs import (
    AsyncFileHandle,
    AsyncFilesystemClient,
    SyncFileHandle,
    SyncFilesystemClient,
)
from vercel._internal.iter_coroutine import iter_coroutine

_T = TypeVar("_T")


class _PathLike:
    def __init__(self, path: Path) -> None:
        self._path = path

    def __fspath__(self) -> str:
        return os.fspath(self._path)


class TestSyncFilesystemClient:
    def test_sync_client_supports_iter_coroutine_temp_file_workflow(self, tmp_path: Path) -> None:
        client = SyncFilesystemClient()
        destination = tmp_path / "nested" / "dir" / "artifact.bin"
        temp_path = tmp_path / "nested" / "dir" / "artifact.bin.part"
        destination_path = iter_coroutine(client.coerce_path(_PathLike(destination)))
        temp_file_path = iter_coroutine(client.coerce_path(temp_path))

        assert destination_path == os.fspath(destination)

        iter_coroutine(client.create_parent_directories(temp_file_path))
        handle: SyncFileHandle = iter_coroutine(client.open_binary_writer(temp_file_path))
        iter_coroutine(client.write(handle, b"hello "))
        iter_coroutine(client.write(handle, b"world"))
        iter_coroutine(client.close(handle))
        iter_coroutine(client.replace(temp_file_path, destination_path))

        assert destination.parent.is_dir()
        assert not temp_path.exists()
        assert destination.read_bytes() == b"hello world"

    def test_sync_client_remove_if_exists_handles_existing_and_missing_files(
        self, tmp_path: Path
    ) -> None:
        client = SyncFilesystemClient()
        destination = tmp_path / "stale.bin"
        destination.write_bytes(b"stale")
        destination_path = iter_coroutine(client.coerce_path(_PathLike(destination)))

        iter_coroutine(client.remove_if_exists(destination_path))

        assert not destination.exists()

        iter_coroutine(client.remove_if_exists(destination_path))

        assert not destination.exists()


class TestAsyncFilesystemClient:
    @pytest.mark.asyncio
    async def test_async_client_writes_and_replaces_final_contents(self, tmp_path: Path) -> None:
        client = AsyncFilesystemClient()
        destination = tmp_path / "async" / "artifact.bin"
        temp_path = tmp_path / "async" / "artifact.bin.part"
        destination_path = await client.coerce_path(_PathLike(destination))
        temp_file_path = await client.coerce_path(temp_path)

        assert destination_path == os.fspath(destination)

        await client.create_parent_directories(temp_file_path)
        handle: AsyncFileHandle = await client.open_binary_writer(temp_file_path)
        await client.write(handle, b"async ")
        await client.write(handle, b"contents")
        await client.close(handle)
        await client.replace(temp_file_path, destination_path)

        assert destination.parent.is_dir()
        assert not temp_path.exists()
        assert destination.read_bytes() == b"async contents"

    @pytest.mark.asyncio
    async def test_async_client_remove_if_exists_handles_existing_and_missing_files(
        self, tmp_path: Path
    ) -> None:
        client = AsyncFilesystemClient()
        destination = tmp_path / "remove-me.bin"
        destination.write_bytes(b"old")
        destination_path = await client.coerce_path(_PathLike(destination))

        await client.remove_if_exists(destination_path)

        assert not destination.exists()

        await client.remove_if_exists(destination_path)

        assert not destination.exists()
