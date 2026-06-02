"""Execution-mode-neutral helpers for Sandbox runtime handles."""

import copy
import posixpath
import signal as signal_module
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import timedelta
from typing import Generic, Literal, TypeAlias, TypeVar

from vercel._internal.unstable.sandbox.errors import SandboxResponseError
from vercel._internal.unstable.sandbox.models import (
    JSONObject,
    JSONValue,
    SandboxCommandLog,
    SandboxCommandLogStream,
    SandboxStatus,
)
from vercel._internal.unstable.sandbox.state import (
    SandboxCommandState,
    SandboxRouteState,
    SandboxRuntimeSessionState,
    SandboxState,
    SnapshotRetentionState,
    SnapshotState,
)

_LogSnapshot: TypeAlias = tuple[SandboxCommandLogStream, str]
RuntimeSessionHandleT = TypeVar("RuntimeSessionHandleT", bound="RuntimeSessionHandleBase")


def _resolve_write_files_cwd(cwd: str | None, *, default: str) -> str:
    if not posixpath.isabs(default):
        raise ValueError("default cwd must be an absolute path")
    if cwd is None:
        return posixpath.normpath(default)
    if posixpath.isabs(cwd):
        return posixpath.normpath(cwd)
    return posixpath.normpath(posixpath.join(default, cwd))


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


def _log_from_snapshot(snapshot: _LogSnapshot) -> SandboxCommandLog:
    stream, data = snapshot
    return SandboxCommandLog(stream=stream, data=data)


def _select_output(
    snapshots: Sequence[_LogSnapshot], stream: Literal["stdout", "stderr", "both"]
) -> str:
    return "".join(data for source, data in snapshots if stream == "both" or source == stream)


class _CommandHandleState:
    __slots__ = ("_payload", "_log_cache", "_log_cache_generation")

    def __init__(self, payload: SandboxCommandState) -> None:
        self._payload = payload
        self._log_cache: tuple[_LogSnapshot, ...] | None = None
        self._log_cache_generation = 0

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
    def exit_code(self) -> int | None:
        return self._payload.exit_code

    @property
    def started_at(self) -> int:
        return self._payload.started_at

    @property
    def status(self) -> Literal["running", "exited"]:
        return "running" if self._payload.exit_code is None else "exited"

    def _apply_payload(self, payload: SandboxCommandState) -> None:
        if payload.id != self._payload.id or payload.session_id != self._payload.session_id:
            raise SandboxResponseError(
                "Sandbox command mutation response returned a different command identity",
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
    def network_policy(self) -> JSONValue | None:
        return copy.deepcopy(self._payload.network_policy)

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

    def _write_files_cwd(self, cwd: str | None) -> str:
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
    def network_policy(self) -> JSONValue | None:
        return copy.deepcopy(self._payload.network_policy)

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

    def _write_files_cwd(self, cwd: str | None) -> str:
        if self.current_session is not None and self.current_session.cwd is not None:
            default = self.current_session.cwd
        else:
            default = self.cwd or "/vercel/sandbox"
        return _resolve_write_files_cwd(cwd, default=default)
