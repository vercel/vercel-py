"""Structural command and filesystem capabilities, independent of lifecycle."""

from __future__ import annotations

import subprocess
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Literal, Protocol, TextIO, overload, runtime_checkable

from vercel.sandbox._internal.models import CompletedProcess, DirectoryEntry, RemotePath

if TYPE_CHECKING:
    from vercel.sandbox._internal.async_filesystem_handle import (
        SandboxBinaryReader,
        SandboxBinaryWriter,
        SandboxTextReader,
        SandboxTextWriter,
    )
    from vercel.sandbox._internal.async_runtime import Process, SandboxFilesystemBatch
    from vercel.sandbox._internal.sync_filesystem_handle import (
        SyncSandboxBinaryReader,
        SyncSandboxBinaryWriter,
        SyncSandboxTextReader,
        SyncSandboxTextWriter,
    )
    from vercel.sandbox._internal.sync_runtime import SyncProcess, SyncSandboxFilesystemBatch


@runtime_checkable
class SandboxFilesystemOperations(Protocol):
    """Filesystem operations honoring the execution identity."""

    @overload
    def open(
        self,
        path: RemotePath,
        mode: Literal["r"] = "r",
        *,
        cwd: RemotePath | None = None,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        size: None = None,
        permissions: None = None,
    ) -> SandboxTextReader: ...

    @overload
    def open(
        self,
        path: RemotePath,
        mode: Literal["rb"],
        *,
        cwd: RemotePath | None = None,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        size: None = None,
        permissions: None = None,
    ) -> SandboxBinaryReader: ...

    @overload
    def open(
        self,
        path: RemotePath,
        mode: Literal["w"],
        *,
        cwd: RemotePath | None = None,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        size: None = None,
        permissions: int | None = None,
    ) -> SandboxTextWriter: ...

    @overload
    def open(
        self,
        path: RemotePath,
        mode: Literal["wb"],
        *,
        cwd: RemotePath | None = None,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        size: int | None = None,
        permissions: int | None = None,
    ) -> SandboxBinaryWriter: ...

    async def mkdir(
        self, path: RemotePath, *, cwd: RemotePath | None = None, recursive: bool = True
    ) -> None: ...

    async def read_bytes(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bytes: ...

    async def read_text(
        self,
        path: RemotePath,
        *,
        cwd: RemotePath | None = None,
        encoding: str = "utf-8",
        errors: str = "strict",
    ) -> str: ...

    async def write_bytes(
        self,
        path: RemotePath,
        data: bytes,
        *,
        cwd: RemotePath | None = None,
        mode: int | None = None,
    ) -> None: ...

    async def write_text(
        self,
        path: RemotePath,
        text: str,
        *,
        cwd: RemotePath | None = None,
        encoding: str = "utf-8",
        errors: str = "strict",
        mode: int | None = None,
    ) -> None: ...

    def batch(self, *, cwd: RemotePath | None = None) -> SandboxFilesystemBatch: ...

    async def exists(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bool: ...

    async def is_file(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bool: ...

    async def is_dir(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bool: ...

    async def listdir(
        self, path: RemotePath = ".", *, cwd: RemotePath | None = None
    ) -> list[DirectoryEntry]: ...

    async def remove(
        self,
        path: RemotePath,
        *,
        cwd: RemotePath | None = None,
        recursive: bool = False,
        missing_ok: bool = False,
    ) -> None: ...

    async def rename(
        self, source: RemotePath, destination: RemotePath, *, cwd: RemotePath | None = None
    ) -> None: ...


@runtime_checkable
class SandboxExecution(Protocol):
    """Commands and files on a sandbox, runtime session, or user."""

    @property
    def fs(self) -> SandboxFilesystemOperations: ...

    async def run_process(
        self,
        command: str,
        args: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
        check: bool = False,
        stdout: TextIO | int | None = None,
        stderr: TextIO | int | None = None,
        capture_output: bool = False,
    ) -> CompletedProcess: ...

    async def create_process(
        self,
        command: str,
        args: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
        stdout: int = subprocess.PIPE,
        stderr: int = subprocess.PIPE,
    ) -> Process: ...


@runtime_checkable
class SyncSandboxFilesystemOperations(Protocol):
    """Filesystem operations honoring the execution identity."""

    @overload
    def open(
        self,
        path: RemotePath,
        mode: Literal["r"] = "r",
        *,
        cwd: RemotePath | None = None,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        size: None = None,
        permissions: None = None,
    ) -> SyncSandboxTextReader: ...

    @overload
    def open(
        self,
        path: RemotePath,
        mode: Literal["rb"],
        *,
        cwd: RemotePath | None = None,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        size: None = None,
        permissions: None = None,
    ) -> SyncSandboxBinaryReader: ...

    @overload
    def open(
        self,
        path: RemotePath,
        mode: Literal["w"],
        *,
        cwd: RemotePath | None = None,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        size: None = None,
        permissions: int | None = None,
    ) -> SyncSandboxTextWriter: ...

    @overload
    def open(
        self,
        path: RemotePath,
        mode: Literal["wb"],
        *,
        cwd: RemotePath | None = None,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        size: int | None = None,
        permissions: int | None = None,
    ) -> SyncSandboxBinaryWriter: ...

    def mkdir(
        self, path: RemotePath, *, cwd: RemotePath | None = None, recursive: bool = True
    ) -> None: ...

    def read_bytes(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bytes: ...

    def read_text(
        self,
        path: RemotePath,
        *,
        cwd: RemotePath | None = None,
        encoding: str = "utf-8",
        errors: str = "strict",
    ) -> str: ...

    def write_bytes(
        self,
        path: RemotePath,
        data: bytes,
        *,
        cwd: RemotePath | None = None,
        mode: int | None = None,
    ) -> None: ...

    def write_text(
        self,
        path: RemotePath,
        text: str,
        *,
        cwd: RemotePath | None = None,
        encoding: str = "utf-8",
        errors: str = "strict",
        mode: int | None = None,
    ) -> None: ...

    def batch(self, *, cwd: RemotePath | None = None) -> SyncSandboxFilesystemBatch: ...

    def exists(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bool: ...

    def is_file(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bool: ...

    def is_dir(self, path: RemotePath, *, cwd: RemotePath | None = None) -> bool: ...

    def listdir(
        self, path: RemotePath = ".", *, cwd: RemotePath | None = None
    ) -> list[DirectoryEntry]: ...

    def remove(
        self,
        path: RemotePath,
        *,
        cwd: RemotePath | None = None,
        recursive: bool = False,
        missing_ok: bool = False,
    ) -> None: ...

    def rename(
        self, source: RemotePath, destination: RemotePath, *, cwd: RemotePath | None = None
    ) -> None: ...


@runtime_checkable
class SyncSandboxExecution(Protocol):
    """Commands and files on a sandbox, runtime session, or user."""

    @property
    def fs(self) -> SyncSandboxFilesystemOperations: ...

    def run_process(
        self,
        command: str,
        args: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
        check: bool = False,
        stdout: TextIO | int | None = None,
        stderr: TextIO | int | None = None,
        capture_output: bool = False,
    ) -> CompletedProcess: ...

    def create_process(
        self,
        command: str,
        args: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
        stdout: int = subprocess.PIPE,
        stderr: int = subprocess.PIPE,
    ) -> SyncProcess: ...
