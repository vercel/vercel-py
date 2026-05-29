"""Execution-mode-neutral helpers for Sandbox runtime handles."""

import copy
import signal as signal_module
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Literal, TypeAlias

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
    SnapshotState,
)

_LogSnapshot: TypeAlias = tuple[SandboxCommandLogStream, str]


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


@dataclass(slots=True, eq=False, repr=False)
class _CommandHandleState:
    _payload: SandboxCommandState
    _log_cache: tuple[_LogSnapshot, ...] | None = field(init=False, default=None)
    _log_cache_generation: int = field(init=False, default=0)

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


@dataclass(slots=True, eq=False, repr=False)
class SnapshotHandleBase:
    _payload: SnapshotState

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


@dataclass(slots=True, eq=False, repr=False)
class RuntimeSessionHandleBase:
    _payload: SandboxRuntimeSessionState

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
    def execution_time_limit(self) -> int | None:
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
        return cwd or self.cwd or "/vercel/sandbox"


@dataclass(slots=True, eq=False, repr=False)
class SandboxHandleBase:
    _payload: SandboxState

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
    def execution_time_limit(self) -> int | None:
        return self._payload.execution_time_limit

    @property
    def network_policy(self) -> JSONValue | None:
        return copy.deepcopy(self._payload.network_policy)

    @property
    def snapshot_expiration(self) -> int | None:
        return self._payload.snapshot_expiration

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
