"""Execution-mode-neutral helpers for Sandbox runtime handles."""

import copy
import posixpath
import signal as signal_module
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import timedelta
from enum import Enum, auto
from pathlib import PurePosixPath
from typing import Generic, Literal, TypeAlias, TypeVar

from vercel._internal.byte_stream import ReadableByteStream
from vercel._internal.unstable.sandbox.errors import SandboxResponseError
from vercel._internal.unstable.sandbox.models import (
    JSONObject,
    NetworkPolicy,
    ProcessStatus,
    SandboxStatus,
    _WriteFile,
)
from vercel._internal.unstable.sandbox.state import (
    ProcessState,
    SandboxRouteState,
    SandboxRuntimeSessionState,
    SandboxState,
    SnapshotRetentionState,
    SnapshotState,
)

RuntimeSessionHandleT = TypeVar("RuntimeSessionHandleT", bound="RuntimeSessionHandleBase")
RemotePath: TypeAlias = str | PurePosixPath
_SourceT = TypeVar("_SourceT")


@dataclass(frozen=True, slots=True)
class _UploadFileEntry(Generic[_SourceT]):
    path: str
    size: int
    source: _SourceT
    mode: int | None = None
    archive_path: str | None = None


_StreamUploadFileEntry: TypeAlias = _UploadFileEntry[ReadableByteStream]


class _FilesystemBatchState(Enum):
    CREATED = auto()
    ACTIVE = auto()
    CLOSED = auto()


class _SandboxFilesystemBatchBase:
    __slots__ = ("_files", "_state")

    def __init__(self) -> None:
        self._files: list[_WriteFile] = []
        self._state = _FilesystemBatchState.CREATED

    def _stage(self, file: _WriteFile) -> None:
        if self._state is not _FilesystemBatchState.ACTIVE:
            raise RuntimeError("filesystem batch staging is only allowed inside its context")
        _validate_file_mode(file.mode)
        self._files.append(file)

    def write_bytes(self, path: RemotePath, data: bytes, *, mode: int | None = None) -> None:
        """Stage a byte file for writing when the batch exits.

        Args:
            path: Absolute path or path relative to the batch working directory.
            data: File contents.
            mode: Optional POSIX permission bits for the file.

        Raises:
            RuntimeError: If called outside the batch context.
        """
        self._stage(_WriteFile(path=_coerce_remote_path(path), content=data, mode=mode))

    def write_text(
        self,
        path: RemotePath,
        text: str,
        *,
        encoding: str = "utf-8",
        errors: str = "strict",
        mode: int | None = None,
    ) -> None:
        """Encode and stage a text file for writing when the batch exits.

        Args:
            path: Absolute path or path relative to the batch working directory.
            text: Text to write.
            encoding: Text encoding used to encode ``text``.
            errors: Encoding error policy.
            mode: Optional POSIX permission bits for the file.

        Raises:
            RuntimeError: If called outside the batch context.
        """
        self._stage(
            _WriteFile(
                path=_coerce_remote_path(path),
                content=text.encode(encoding, errors=errors),
                mode=mode,
            )
        )

    def _enter(self) -> None:
        if self._state is not _FilesystemBatchState.CREATED:
            raise RuntimeError("filesystem batch contexts can only be entered once")
        self._state = _FilesystemBatchState.ACTIVE

    def _close(self) -> Sequence[_WriteFile]:
        self._state = _FilesystemBatchState.CLOSED
        return self._files


def _coerce_remote_path(path: RemotePath) -> str:
    if not isinstance(path, (str, PurePosixPath)):
        raise TypeError("path must be a string or PurePosixPath")
    return str(path)


def _resolve_write_files_cwd(cwd: RemotePath | None, *, default: str) -> str:
    if not posixpath.isabs(default):
        raise ValueError("default cwd must be an absolute path")
    if cwd is None:
        return posixpath.normpath(default)
    normalized_cwd = _coerce_remote_path(cwd)
    if posixpath.isabs(normalized_cwd):
        return posixpath.normpath(normalized_cwd)
    return posixpath.normpath(posixpath.join(default, normalized_cwd))


def _normalize_tar_path(path: str, *, cwd: str) -> str:
    if not posixpath.isabs(cwd):
        raise ValueError("cwd must be an absolute path")
    absolute_path = (
        posixpath.normpath(path)
        if posixpath.isabs(path)
        else posixpath.normpath(posixpath.join(cwd, path))
    )
    return posixpath.relpath(absolute_path, "/")


def _validate_transfer_size(size: object) -> int:
    if isinstance(size, bool) or not isinstance(size, int):
        raise TypeError("size must be an integer >= 0")
    if size < 0:
        raise ValueError("size must be >= 0")
    return size


def _validate_chunk_size(chunk_size: object) -> int:
    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int):
        raise TypeError("chunk_size must be a positive integer")
    if chunk_size < 1:
        raise ValueError("chunk_size must be positive")
    return chunk_size


def _validate_file_mode(mode: object) -> int | None:
    if mode is None:
        return None
    if isinstance(mode, bool) or not isinstance(mode, int):
        raise TypeError("file mode must be an integer or None")
    if not 0 <= mode <= 0o777:
        raise ValueError("file mode must be between 0 and 0o777")
    return mode


def _signal_number(value: int | str | signal_module.Signals | None) -> int:
    if value is None:
        return int(signal_module.Signals.SIGTERM)
    if isinstance(value, signal_module.Signals):
        return int(value)
    if isinstance(value, int):
        return value
    normalized = value.upper()
    if not normalized.startswith("SIG"):
        normalized = f"SIG{normalized}"
    try:
        return int(signal_module.Signals[normalized])
    except KeyError as exc:
        raise ValueError(f"Unknown signal: {value!r}") from exc


class _ProcessHandleState:
    __slots__ = ("_payload",)

    def __init__(self, payload: ProcessState) -> None:
        self._payload = payload

    @property
    def id(self) -> str:
        return self._payload.id

    @property
    def name(self) -> str:
        return self._payload.name

    @property
    def args(self) -> list[str]:
        return list(self._payload.args)

    @property
    def cwd(self) -> str:
        return self._payload.cwd

    @property
    def session_id(self) -> str:
        return self._payload.session_id

    @property
    def _session_id(self) -> str:
        return self._payload.session_id

    @property
    def returncode(self) -> int | None:
        return self._payload.returncode

    @property
    def started_at(self) -> int:
        return self._payload.started_at

    @property
    def status(self) -> ProcessStatus:
        return ProcessStatus.RUNNING if self.returncode is None else ProcessStatus.EXITED

    @property
    def stdin(self) -> None:
        return None

    def _apply_payload(self, payload: ProcessState) -> None:
        if payload.id != self._payload.id or payload.session_id != self._payload.session_id:
            raise SandboxResponseError(
                "Sandbox process mutation response returned a different process identity",
                data=payload,
            )
        self._payload = payload


class SnapshotHandleBase:
    __slots__ = ("_payload",)

    def __init__(self, payload: SnapshotState) -> None:
        self._payload = payload

    @property
    def id(self) -> str:
        return self._payload.id

    @property
    def source_session_id(self) -> str:
        return self._payload.source_session_id

    @property
    def region(self) -> str:
        return self._payload.region

    @property
    def status(self) -> Literal["created", "deleted", "failed"]:
        return self._payload.status

    @property
    def size_bytes(self) -> int:
        return self._payload.size_bytes

    @property
    def expires_at(self) -> int | None:
        return self._payload.expires_at

    @property
    def created_at(self) -> int:
        return self._payload.created_at

    @property
    def updated_at(self) -> int:
        return self._payload.updated_at

    @property
    def last_used_at(self) -> int | None:
        return self._payload.last_used_at

    @property
    def creation_method(self) -> str | None:
        return self._payload.creation_method

    @property
    def parent_id(self) -> str | None:
        return self._payload.parent_id

    def _apply_payload(self, payload: SnapshotState) -> None:
        if payload.id != self._payload.id:
            raise SandboxResponseError(
                "Snapshot mutation response returned a different snapshot identity",
                data=payload,
            )
        self._payload = payload


class RuntimeSessionHandleBase:
    __slots__ = ("_payload",)

    def __init__(self, payload: SandboxRuntimeSessionState) -> None:
        self._payload = payload

    @property
    def id(self) -> str:
        return self._payload.id

    @property
    def sandbox_name(self) -> str | None:
        return self._payload.sandbox_name

    @property
    def project_id(self) -> str | None:
        return self._payload.project_id

    @property
    def status(self) -> SandboxStatus | None:
        return self._payload.status

    @property
    def runtime(self) -> str | None:
        return self._payload.runtime

    @property
    def cwd(self) -> str | None:
        return self._payload.cwd

    @property
    def region(self) -> str | None:
        return self._payload.region

    @property
    def memory(self) -> int | None:
        return self._payload.memory

    @property
    def vcpus(self) -> int | None:
        return self._payload.vcpus

    @property
    def execution_time_limit(self) -> timedelta | None:
        return self._payload.execution_time_limit

    @property
    def network_policy(self) -> NetworkPolicy | None:
        return self._payload.network_policy

    @property
    def requested_at(self) -> int | None:
        return self._payload.requested_at

    @property
    def started_at(self) -> int | None:
        return self._payload.started_at

    @property
    def stopped_at(self) -> int | None:
        return self._payload.stopped_at

    def _apply_payload(self, payload: SandboxRuntimeSessionState) -> None:
        if payload.id != self._payload.id:
            raise SandboxResponseError(
                "Sandbox runtime-session mutation response returned a different session identity",
                data=payload,
            )
        self._payload = payload

    def _write_files_cwd(self, cwd: RemotePath | None) -> str:
        return _resolve_write_files_cwd(cwd, default=self.cwd or "/vercel/sandbox")


class SandboxHandleBase(Generic[RuntimeSessionHandleT]):
    __slots__ = ("_payload", "_current_session", "_session_factory")

    def __init__(
        self,
        payload: SandboxState,
        session_factory: Callable[[SandboxRuntimeSessionState], RuntimeSessionHandleT],
    ) -> None:
        self._payload = payload
        self._session_factory = session_factory
        self._current_session = (
            None if payload.current_session is None else session_factory(payload.current_session)
        )

    @property
    def current_session(self) -> RuntimeSessionHandleT | None:
        return self._current_session

    @property
    def name(self) -> str:
        return self._payload.name

    @property
    def current_session_id(self) -> str:
        return self._payload.current_session_id

    @property
    def runtime(self) -> str | None:
        return self._payload.runtime

    @property
    def status(self) -> SandboxStatus | None:
        return self._payload.status

    @property
    def persistent(self) -> bool | None:
        return self._payload.persistent

    @property
    def current_snapshot_id(self) -> str | None:
        return self._payload.current_snapshot_id

    @property
    def project_id(self) -> str | None:
        return self._payload.project_id

    @property
    def cwd(self) -> str | None:
        return self._payload.cwd

    @property
    def region(self) -> str | None:
        return self._payload.region

    @property
    def memory(self) -> int | None:
        return self._payload.memory

    @property
    def vcpus(self) -> int | None:
        return self._payload.vcpus

    @property
    def execution_time_limit(self) -> timedelta | None:
        return self._payload.execution_time_limit

    @property
    def network_policy(self) -> NetworkPolicy | None:
        return self._payload.network_policy

    @property
    def snapshot_expiration(self) -> timedelta | None:
        return self._payload.snapshot_expiration

    @property
    def snapshot_retention(self) -> SnapshotRetentionState | None:
        return self._payload.snapshot_retention

    @property
    def status_updated_at(self) -> int | None:
        return self._payload.status_updated_at

    @property
    def created_at(self) -> int | None:
        return self._payload.created_at

    @property
    def updated_at(self) -> int | None:
        return self._payload.updated_at

    @property
    def tags(self) -> dict[str, str] | None:
        return None if self._payload.tags is None else dict(self._payload.tags)

    @property
    def routes(self) -> tuple[SandboxRouteState, ...]:
        return self._payload.routes

    @property
    def raw(self) -> JSONObject | None:
        return copy.deepcopy(self._payload.raw)

    def _apply_payload(self, payload: SandboxState) -> None:
        if payload.name != self._payload.name:
            raise SandboxResponseError(
                "Sandbox mutation response returned a different sandbox identity",
                data=payload,
            )
        returned_session = payload.current_session
        if returned_session is not None and returned_session.id != payload.current_session_id:
            raise SandboxResponseError(
                "Sandbox response session does not match current session identity",
                data=payload,
            )
        if payload._current_session_attached and returned_session is not None:
            if (
                self._current_session is not None
                and self._current_session.id == returned_session.id
            ):
                self._current_session._apply_payload(returned_session)
            else:
                self._current_session = self._session_factory(returned_session)
        elif (
            payload._current_session_attached
            or payload.current_session_id != self._payload.current_session_id
        ):
            self._current_session = None
        self._payload = replace(
            payload,
            routes=payload.routes if payload._routes_attached else self._payload.routes,
            current_session=(
                None if self._current_session is None else self._current_session._payload
            ),
            _routes_attached=True,
            _current_session_attached=True,
        )

    def _apply_current_session_payload(
        self, payload: SandboxRuntimeSessionState
    ) -> RuntimeSessionHandleT:
        if payload.id != self.current_session_id:
            raise SandboxResponseError(
                "Sandbox current-session operation returned a different session identity",
                data=payload,
            )
        if self._current_session is None:
            self._current_session = self._session_factory(payload)
        else:
            self._current_session._apply_payload(payload)
        return self._current_session

    def _write_files_cwd(self, cwd: RemotePath | None) -> str:
        if self.current_session is not None and self.current_session.cwd is not None:
            default = self.current_session.cwd
        else:
            default = self.cwd or "/vercel/sandbox"
        return _resolve_write_files_cwd(cwd, default=default)
