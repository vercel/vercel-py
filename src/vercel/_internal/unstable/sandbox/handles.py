"""Session-bound operational handles for the experimental Sandbox API."""

import copy
import signal as signal_module
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Literal, TypeAlias, cast

import httpx

from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.unstable.sandbox.api_client import (
    _CommandPayload,
    _RuntimeSessionPayload,
    _SandboxPayload,
    _SandboxRoutePayload,
    _SnapshotPayload,
)
from vercel._internal.unstable.sandbox.errors import (
    SandboxCleanupError,
    SandboxInvalidHandleError,
    SandboxResponseError,
)
from vercel._internal.unstable.sandbox.log_stream import _parse_command_log_record
from vercel._internal.unstable.sandbox.models import (
    DurationInput,
    JSONObject,
    JSONValue,
    SandboxCommandLog,
    SandboxCommandLogStream,
    SandboxResources,
    SandboxStatus,
    SnapshotRetention,
    WriteFile,
)
from vercel._internal.unstable.session import SdkSession, SyncSdkSession

if TYPE_CHECKING:
    from vercel._internal.unstable.sandbox.operations import CreateRuntimeSessionOperation
    from vercel._internal.unstable.sandbox.service import SandboxService


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


_LogSnapshot: TypeAlias = tuple[SandboxCommandLogStream, str]


def _log_from_snapshot(snapshot: _LogSnapshot) -> SandboxCommandLog:
    stream, data = snapshot
    return SandboxCommandLog(stream=stream, data=data)


def _select_output(
    snapshots: Sequence[_LogSnapshot], stream: Literal["stdout", "stderr", "both"]
) -> str:
    return "".join(data for source, data in snapshots if stream == "both" or source == stream)


def _iter_sync_logs(response_factory: Callable[[], httpx.Response]) -> Iterator[SandboxCommandLog]:
    response = response_factory()
    try:
        for line in response.iter_lines():
            if not line:
                continue
            event = _parse_command_log_record(line)
            if event is not None:
                yield event
    finally:
        response.close()


class _PayloadHandle:
    __slots__ = ("_payload", "_sdk_session")


class _BaseCommand(_PayloadHandle):
    __slots__ = ("_log_cache", "_log_cache_generation")

    def __init__(
        self,
        *,
        payload: _CommandPayload,
        sdk_session: SdkSession | SyncSdkSession,
    ) -> None:
        self._payload = payload
        self._sdk_session = sdk_session
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

    def _apply_payload(self, payload: _CommandPayload) -> None:
        if payload.id != self._payload.id or payload.session_id != self._payload.session_id:
            raise SandboxResponseError(
                "Sandbox command mutation response returned a different command identity",
                data=payload.model_dump(by_alias=True),
            )
        self._payload = payload

    def _require_sandbox_service(self) -> "SandboxService":
        if not isinstance(self._sdk_session, SdkSession):
            raise SandboxInvalidHandleError(
                "Sandbox command handle is not attached to an SDK session"
            )
        return self._sdk_session.sandbox_service()

    def _require_sync_sandbox_service(self) -> "SandboxService":
        if not isinstance(self._sdk_session, SyncSdkSession):
            raise SandboxInvalidHandleError(
                "Sandbox command handle is not attached to an SDK session"
            )
        return self._sdk_session.sandbox_service()


class SandboxCommand(_BaseCommand):
    __slots__ = ()

    async def refresh(self, *, wait: bool = False) -> "SandboxCommand":
        payload = await self._require_sandbox_service().get_command_payload(
            session_id=self.session_id, command_id=self.id, wait=wait
        )
        self._apply_payload(payload)
        return self

    async def wait(self) -> "SandboxCommand":
        return await self.refresh(wait=True)

    async def kill(
        self, signal: int | str | signal_module.Signals | None = None
    ) -> "SandboxCommand":
        payload = await self._require_sandbox_service().kill_command_payload(
            session_id=self.session_id,
            command_id=self.id,
            signal=_signal_number(signal),
        )
        self._apply_payload(payload)
        return self

    def logs(self, *, refresh: bool = False) -> AsyncIterator[SandboxCommandLog]:
        async def iter_logs() -> AsyncIterator[SandboxCommandLog]:
            if not refresh and self._log_cache is not None:
                for snapshot in self._log_cache:
                    yield _log_from_snapshot(snapshot)
                return

            if refresh:
                self._log_cache = None
                self._log_cache_generation += 1
            generation = self._log_cache_generation
            staged: list[_LogSnapshot] = []
            async for event in self._require_sandbox_service().command_logs(
                session_id=self.session_id, command_id=self.id
            ):
                staged.append((event.stream, event.data))
                yield event
            if generation == self._log_cache_generation:
                self._log_cache = tuple(staged)

        return iter_logs()

    async def output(self, stream: Literal["stdout", "stderr", "both"] = "both") -> str:
        snapshots: list[_LogSnapshot] = []
        async for line in self.logs():
            snapshots.append((line.stream, line.data))
        return _select_output(snapshots, stream)

    async def stdout(self) -> str:
        return await self.output("stdout")

    async def stderr(self) -> str:
        return await self.output("stderr")


class SyncSandboxCommand(_BaseCommand):
    __slots__ = ()

    def refresh(self, *, wait: bool = False) -> "SyncSandboxCommand":
        payload = iter_coroutine(
            self._require_sync_sandbox_service().get_command_payload(
                session_id=self.session_id, command_id=self.id, wait=wait
            )
        )
        self._apply_payload(payload)
        return self

    def wait(self) -> "SyncSandboxCommand":
        return self.refresh(wait=True)

    def kill(self, signal: int | str | signal_module.Signals | None = None) -> "SyncSandboxCommand":
        payload = iter_coroutine(
            self._require_sync_sandbox_service().kill_command_payload(
                session_id=self.session_id,
                command_id=self.id,
                signal=_signal_number(signal),
            )
        )
        self._apply_payload(payload)
        return self

    def logs(self, *, refresh: bool = False) -> Iterator[SandboxCommandLog]:
        def iter_logs() -> Iterator[SandboxCommandLog]:
            if not refresh and self._log_cache is not None:
                for snapshot in self._log_cache:
                    yield _log_from_snapshot(snapshot)
                return

            if refresh:
                self._log_cache = None
                self._log_cache_generation += 1
            generation = self._log_cache_generation
            staged: list[_LogSnapshot] = []
            for event in _iter_sync_logs(
                lambda: iter_coroutine(
                    self._require_sync_sandbox_service().command_logs_response(
                        session_id=self.session_id, command_id=self.id
                    )
                )
            ):
                staged.append((event.stream, event.data))
                yield event
            if generation == self._log_cache_generation:
                self._log_cache = tuple(staged)

        return iter_logs()

    def output(self, stream: Literal["stdout", "stderr", "both"] = "both") -> str:
        snapshots: list[_LogSnapshot] = []
        for line in self.logs():
            snapshots.append((line.stream, line.data))
        return _select_output(snapshots, stream)

    def stdout(self) -> str:
        return self.output("stdout")

    def stderr(self) -> str:
        return self.output("stderr")


class _BaseSnapshot(_PayloadHandle):
    __slots__ = ()

    def __init__(
        self,
        *,
        payload: _SnapshotPayload,
        sdk_session: SdkSession | SyncSdkSession,
    ) -> None:
        self._payload = payload
        self._sdk_session = sdk_session

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

    def _apply_payload(self, payload: _SnapshotPayload) -> None:
        if payload.id != self._payload.id:
            raise SandboxResponseError(
                "Snapshot mutation response returned a different snapshot identity",
                data=payload.model_dump(by_alias=True),
            )
        self._payload = payload

    def _require_sandbox_service(self) -> "SandboxService":
        if not isinstance(self._sdk_session, SdkSession):
            raise SandboxInvalidHandleError("Snapshot handle is not attached to an SDK session")
        return self._sdk_session.sandbox_service()

    def _require_sync_sandbox_service(self) -> "SandboxService":
        if not isinstance(self._sdk_session, SyncSdkSession):
            raise SandboxInvalidHandleError("Snapshot handle is not attached to an SDK session")
        return self._sdk_session.sandbox_service()


class Snapshot(_BaseSnapshot):
    __slots__ = ()

    async def delete(self) -> "Snapshot":
        payload = await self._require_sandbox_service().delete_snapshot_payload(snapshot_id=self.id)
        self._apply_payload(payload)
        return self


class SyncSnapshot(_BaseSnapshot):
    __slots__ = ()

    def delete(self) -> "SyncSnapshot":
        payload = iter_coroutine(
            self._require_sync_sandbox_service().delete_snapshot_payload(snapshot_id=self.id)
        )
        self._apply_payload(payload)
        return self


class _BaseRuntimeSession(_PayloadHandle):
    __slots__ = ()

    def __init__(
        self,
        *,
        payload: _RuntimeSessionPayload,
        sdk_session: SdkSession | SyncSdkSession,
    ) -> None:
        self._payload = payload
        self._sdk_session = sdk_session

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

    def _apply_payload(self, payload: _RuntimeSessionPayload) -> None:
        if payload.id != self._payload.id:
            raise SandboxResponseError(
                "Sandbox runtime-session mutation response returned a different session identity",
                data=payload.model_dump(by_alias=True),
            )
        self._payload = payload

    def _require_sandbox_service(self) -> "SandboxService":
        if not isinstance(self._sdk_session, SdkSession):
            raise SandboxInvalidHandleError(
                "Sandbox runtime-session handle is not attached to an SDK session"
            )
        return self._sdk_session.sandbox_service()

    def _require_sync_sandbox_service(self) -> "SandboxService":
        if not isinstance(self._sdk_session, SyncSdkSession):
            raise SandboxInvalidHandleError(
                "Sandbox runtime-session handle is not attached to an SDK session"
            )
        return self._sdk_session.sandbox_service()

    def _write_files_cwd(self, cwd: str | None) -> str:
        return cwd or self.cwd or "/vercel/sandbox"


class SandboxRuntimeSession(_BaseRuntimeSession):
    __slots__ = ()

    async def run_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> SandboxCommand:
        return await self._require_sandbox_service().run_command(
            session_id=self.id, command=command, args=args, cwd=cwd, env=env, sudo=sudo
        )

    async def start_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> SandboxCommand:
        return await self._require_sandbox_service().start_command(
            session_id=self.id, command=command, args=args, cwd=cwd, env=env, sudo=sudo
        )

    async def get_command(self, command_id: str, *, wait: bool = False) -> SandboxCommand:
        return await self._require_sandbox_service().get_command(
            session_id=self.id, command_id=command_id, wait=wait
        )

    async def query_commands(self) -> list[SandboxCommand]:
        return await self._require_sandbox_service().query_commands(session_id=self.id)

    async def refresh(
        self, *, include_system_routes: bool | None = None
    ) -> "SandboxRuntimeSession":
        payload = await self._require_sandbox_service().get_runtime_session_payload(
            session_id=self.id, include_system_routes=include_system_routes
        )
        self._apply_payload(payload)
        return self

    async def extend_execution_time_limit(self, duration: DurationInput) -> "SandboxRuntimeSession":
        payload = await self._require_sandbox_service().extend_runtime_session_timeout_payload(
            session_id=self.id, duration=duration
        )
        self._apply_payload(payload)
        return self

    async def update_network_policy(self, network_policy: JSONValue) -> "SandboxRuntimeSession":
        payload = (
            await self._require_sandbox_service().update_runtime_session_network_policy_payload(
                session_id=self.id, network_policy=network_policy
            )
        )
        self._apply_payload(payload)
        return self

    async def mkdir(self, path: str, *, cwd: str | None = None, recursive: bool = True) -> None:
        await self._require_sandbox_service().mkdir(
            session_id=self.id, path=path, cwd=cwd, recursive=recursive
        )

    async def read_file(self, path: str, *, cwd: str | None = None) -> bytes:
        return await self._require_sandbox_service().read_file(
            session_id=self.id, path=path, cwd=cwd
        )

    async def read_text(
        self, path: str, *, cwd: str | None = None, encoding: str = "utf-8", errors: str = "strict"
    ) -> str:
        return (await self.read_file(path, cwd=cwd)).decode(encoding, errors=errors)

    async def write_files(
        self, files: Sequence[WriteFile], *, cwd: str | None = None, encoding: str = "utf-8"
    ) -> None:
        await self._require_sandbox_service().write_files(
            session_id=self.id, files=files, cwd=self._write_files_cwd(cwd), encoding=encoding
        )

    async def snapshot(self, *, expiration: DurationInput = None) -> Snapshot:
        snapshot, payload = await self._require_sandbox_service().create_snapshot_for_session(
            session_id=self.id, expiration=expiration
        )
        self._apply_payload(payload)
        return snapshot

    def command_logs(self, command_id: str) -> AsyncIterator[SandboxCommandLog]:
        return self._require_sandbox_service().command_logs(
            session_id=self.id, command_id=command_id
        )

    async def stop(self) -> "SandboxRuntimeSession":
        payload = await self._require_sandbox_service().stop_runtime_session_payload(
            session_id=self.id
        )
        self._apply_payload(payload)
        return self


class SyncSandboxRuntimeSession(_BaseRuntimeSession):
    __slots__ = ()

    def __enter__(self) -> "SyncSandboxRuntimeSession":
        self._require_sync_sandbox_service()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            payload = iter_coroutine(
                self._require_sync_sandbox_service().stop_runtime_session_payload(
                    session_id=self.id
                )
            )
            self._apply_payload(payload)
        except Exception as cleanup_exc:
            raise SandboxCleanupError(
                f"Failed to clean up sandbox runtime session {self.id!r}",
                resource_type="sandbox_runtime_session",
                resource_id=self.id,
                cause=cleanup_exc,
            ) from cleanup_exc

    def run_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> SyncSandboxCommand:
        return cast(
            SyncSandboxCommand,
            iter_coroutine(
                self._require_sync_sandbox_service().run_command(
                    session_id=self.id, command=command, args=args, cwd=cwd, env=env, sudo=sudo
                )
            ),
        )

    def start_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> SyncSandboxCommand:
        return cast(
            SyncSandboxCommand,
            iter_coroutine(
                self._require_sync_sandbox_service().start_command(
                    session_id=self.id, command=command, args=args, cwd=cwd, env=env, sudo=sudo
                )
            ),
        )

    def get_command(self, command_id: str, *, wait: bool = False) -> SyncSandboxCommand:
        return cast(
            SyncSandboxCommand,
            iter_coroutine(
                self._require_sync_sandbox_service().get_command(
                    session_id=self.id, command_id=command_id, wait=wait
                )
            ),
        )

    def query_commands(self) -> list[SyncSandboxCommand]:
        return cast(
            list[SyncSandboxCommand],
            iter_coroutine(self._require_sync_sandbox_service().query_commands(session_id=self.id)),
        )

    def refresh(self, *, include_system_routes: bool | None = None) -> "SyncSandboxRuntimeSession":
        payload = iter_coroutine(
            self._require_sync_sandbox_service().get_runtime_session_payload(
                session_id=self.id, include_system_routes=include_system_routes
            )
        )
        self._apply_payload(payload)
        return self

    def extend_execution_time_limit(self, duration: DurationInput) -> "SyncSandboxRuntimeSession":
        payload = iter_coroutine(
            self._require_sync_sandbox_service().extend_runtime_session_timeout_payload(
                session_id=self.id, duration=duration
            )
        )
        self._apply_payload(payload)
        return self

    def update_network_policy(self, network_policy: JSONValue) -> "SyncSandboxRuntimeSession":
        payload = iter_coroutine(
            self._require_sync_sandbox_service().update_runtime_session_network_policy_payload(
                session_id=self.id, network_policy=network_policy
            )
        )
        self._apply_payload(payload)
        return self

    def mkdir(self, path: str, *, cwd: str | None = None, recursive: bool = True) -> None:
        iter_coroutine(
            self._require_sync_sandbox_service().mkdir(
                session_id=self.id, path=path, cwd=cwd, recursive=recursive
            )
        )

    def read_file(self, path: str, *, cwd: str | None = None) -> bytes:
        return iter_coroutine(
            self._require_sync_sandbox_service().read_file(session_id=self.id, path=path, cwd=cwd)
        )

    def read_text(
        self, path: str, *, cwd: str | None = None, encoding: str = "utf-8", errors: str = "strict"
    ) -> str:
        return self.read_file(path, cwd=cwd).decode(encoding, errors=errors)

    def write_files(
        self, files: Sequence[WriteFile], *, cwd: str | None = None, encoding: str = "utf-8"
    ) -> None:
        iter_coroutine(
            self._require_sync_sandbox_service().write_files(
                session_id=self.id, files=files, cwd=self._write_files_cwd(cwd), encoding=encoding
            )
        )

    def snapshot(self, *, expiration: DurationInput = None) -> SyncSnapshot:
        snapshot, payload = iter_coroutine(
            self._require_sync_sandbox_service().create_snapshot_for_session(
                session_id=self.id, expiration=expiration
            )
        )
        self._apply_payload(payload)
        return cast(SyncSnapshot, snapshot)

    def command_logs(self, command_id: str) -> Iterator[SandboxCommandLog]:
        return _iter_sync_logs(
            lambda: iter_coroutine(
                self._require_sync_sandbox_service().command_logs_response(
                    session_id=self.id, command_id=command_id
                )
            )
        )

    def stop(self) -> "SyncSandboxRuntimeSession":
        payload = iter_coroutine(
            self._require_sync_sandbox_service().stop_runtime_session_payload(session_id=self.id)
        )
        self._apply_payload(payload)
        return self


class _BaseSandbox(_PayloadHandle):
    __slots__ = ("_current_session",)

    def __init__(
        self, *, payload: _SandboxPayload, sdk_session: SdkSession | SyncSdkSession
    ) -> None:
        self._payload = payload
        self._sdk_session = sdk_session
        self._current_session: SandboxRuntimeSession | SyncSandboxRuntimeSession | None = None
        if payload.current_session is not None:
            self._current_session = self._new_runtime_session(payload.current_session)

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
    def routes(self) -> tuple[_SandboxRoutePayload, ...]:
        return self._payload.routes

    @property
    def raw(self) -> JSONObject | None:
        return copy.deepcopy(self._payload.raw)

    @property
    def current_session(self) -> SandboxRuntimeSession | SyncSandboxRuntimeSession | None:
        return self._current_session

    def _new_runtime_session(
        self, payload: _RuntimeSessionPayload
    ) -> SandboxRuntimeSession | SyncSandboxRuntimeSession:
        if isinstance(self._sdk_session, SdkSession):
            return SandboxRuntimeSession(payload=payload, sdk_session=self._sdk_session)
        return SyncSandboxRuntimeSession(payload=payload, sdk_session=self._sdk_session)

    def _apply_payload(self, payload: _SandboxPayload) -> None:
        if payload.name != self._payload.name:
            raise SandboxResponseError(
                "Sandbox mutation response returned a different sandbox identity",
                data=payload.model_dump(by_alias=True),
            )
        returned_session = payload.current_session
        if returned_session is not None and returned_session.id != payload.current_session_id:
            raise SandboxResponseError(
                "Sandbox response session does not match current session identity",
                data=payload.model_dump(by_alias=True),
            )
        if returned_session is not None:
            if (
                self._current_session is not None
                and self._current_session.id == returned_session.id
            ):
                self._current_session._apply_payload(returned_session)
            else:
                self._current_session = self._new_runtime_session(returned_session)
        elif payload.current_session_id != self._payload.current_session_id:
            self._current_session = None
        self._payload = payload

    def _apply_current_session_payload(
        self, payload: _RuntimeSessionPayload
    ) -> SandboxRuntimeSession | SyncSandboxRuntimeSession:
        if payload.id != self.current_session_id:
            raise SandboxResponseError(
                "Sandbox current-session operation returned a different session identity",
                data=payload.model_dump(by_alias=True),
            )
        if self._current_session is None:
            self._current_session = self._new_runtime_session(payload)
        else:
            self._current_session._apply_payload(payload)
        return self._current_session

    def _require_sandbox_service(self) -> "SandboxService":
        if not isinstance(self._sdk_session, SdkSession):
            raise SandboxInvalidHandleError("Sandbox handle is not attached to an SDK session")
        return self._sdk_session.sandbox_service()

    def _require_sync_sandbox_service(self) -> "SandboxService":
        if not isinstance(self._sdk_session, SyncSdkSession):
            raise SandboxInvalidHandleError("Sandbox handle is not attached to an SDK session")
        return self._sdk_session.sandbox_service()

    def _write_files_cwd(self, cwd: str | None) -> str:
        if cwd is not None:
            return cwd
        if self.current_session is not None and self.current_session.cwd is not None:
            return self.current_session.cwd
        return self.cwd or "/vercel/sandbox"


class Sandbox(_BaseSandbox):
    __slots__ = ()

    @property
    def current_session(self) -> SandboxRuntimeSession | None:
        return cast(SandboxRuntimeSession | None, self._current_session)

    def session(self) -> "CreateRuntimeSessionOperation":
        from vercel._internal.unstable.sandbox.operations import create_runtime_session_operation

        if not isinstance(self._sdk_session, SdkSession):
            raise SandboxInvalidHandleError("Sandbox handle is not attached to an SDK session")
        return create_runtime_session_operation(sandbox=self, session=self._sdk_session)

    async def run_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> SandboxCommand:
        return await self._require_sandbox_service().run_command(
            session_id=self.current_session_id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
        )

    async def start_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> SandboxCommand:
        return await self._require_sandbox_service().start_command(
            session_id=self.current_session_id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
        )

    async def get_command(self, command_id: str, *, wait: bool = False) -> SandboxCommand:
        return await self._require_sandbox_service().get_command(
            session_id=self.current_session_id, command_id=command_id, wait=wait
        )

    async def query_commands(self) -> list[SandboxCommand]:
        return await self._require_sandbox_service().query_commands(
            session_id=self.current_session_id
        )

    async def list_sessions(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> list[SandboxRuntimeSession]:
        return (
            await self._require_sandbox_service().query_sessions_page(
                project_id=self.project_id,
                name=self.name,
                page_size=page_size,
                cursor=cursor,
                sort_order=sort_order,
            )
        ).sessions

    async def list_snapshots(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> list[Snapshot]:
        return (
            await self._require_sandbox_service().query_snapshots_page(
                project_id=self.project_id,
                name=self.name,
                page_size=page_size,
                cursor=cursor,
                sort_order=sort_order,
            )
        ).snapshots

    async def extend_execution_time_limit(self, duration: DurationInput) -> SandboxRuntimeSession:
        payload = await self._require_sandbox_service().extend_runtime_session_timeout_payload(
            session_id=self.current_session_id, duration=duration
        )
        return cast(SandboxRuntimeSession, self._apply_current_session_payload(payload))

    async def update_network_policy(self, network_policy: JSONValue) -> SandboxRuntimeSession:
        payload = (
            await self._require_sandbox_service().update_runtime_session_network_policy_payload(
                session_id=self.current_session_id, network_policy=network_policy
            )
        )
        return cast(SandboxRuntimeSession, self._apply_current_session_payload(payload))

    async def mkdir(self, path: str, *, cwd: str | None = None, recursive: bool = True) -> None:
        await self._require_sandbox_service().mkdir(
            session_id=self.current_session_id, path=path, cwd=cwd, recursive=recursive
        )

    async def read_file(self, path: str, *, cwd: str | None = None) -> bytes:
        return await self._require_sandbox_service().read_file(
            session_id=self.current_session_id, path=path, cwd=cwd
        )

    async def read_text(
        self, path: str, *, cwd: str | None = None, encoding: str = "utf-8", errors: str = "strict"
    ) -> str:
        return (await self.read_file(path, cwd=cwd)).decode(encoding, errors=errors)

    async def write_files(
        self, files: Sequence[WriteFile], *, cwd: str | None = None, encoding: str = "utf-8"
    ) -> None:
        await self._require_sandbox_service().write_files(
            session_id=self.current_session_id,
            files=files,
            cwd=self._write_files_cwd(cwd),
            encoding=encoding,
        )

    async def snapshot(self, *, expiration: DurationInput = None) -> Snapshot:
        snapshot, payload = await self._require_sandbox_service().create_snapshot_for_session(
            session_id=self.current_session_id, expiration=expiration
        )
        self._apply_current_session_payload(payload)
        return snapshot

    async def destroy(self) -> "Sandbox":
        payload = await self._require_sandbox_service().destroy_sandbox_payload(
            name=self.name, project_id=self.project_id
        )
        self._apply_payload(payload)
        return self

    async def update(
        self,
        *,
        runtime: str | None = None,
        ports: list[int] | None = None,
        execution_time_limit: DurationInput = None,
        resources: SandboxResources | None = None,
        persistent: bool | None = None,
        network_policy: JSONValue | None = None,
        env: dict[str, str] | None = None,
        tags: dict[str, str] | None = None,
        snapshot_expiration: DurationInput = None,
        snapshot_retention: SnapshotRetention | None = None,
        current_snapshot_id: str | None = None,
    ) -> "Sandbox":
        payload = await self._require_sandbox_service().update_sandbox_payload(
            name=self.name,
            project_id=self.project_id,
            runtime=runtime,
            ports=ports,
            execution_time_limit=execution_time_limit,
            resources=resources,
            persistent=persistent,
            network_policy=network_policy,
            env=env,
            tags=tags,
            snapshot_expiration=snapshot_expiration,
            snapshot_retention=snapshot_retention,
            current_snapshot_id=current_snapshot_id,
        )
        self._apply_payload(payload)
        return self


class SyncSandbox(_BaseSandbox):
    __slots__ = ()

    @property
    def current_session(self) -> SyncSandboxRuntimeSession | None:
        return cast(SyncSandboxRuntimeSession | None, self._current_session)

    def __enter__(self) -> "SyncSandbox":
        self._require_sync_sandbox_service()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            payload = iter_coroutine(
                self._require_sync_sandbox_service().destroy_sandbox_payload(
                    name=self.name, project_id=self.project_id
                )
            )
            self._apply_payload(payload)
        except Exception as cleanup_exc:
            raise SandboxCleanupError(
                f"Failed to clean up sandbox {self.name!r}",
                resource_type="sandbox",
                resource_id=self.name,
                cause=cleanup_exc,
            ) from cleanup_exc

    def session(self) -> SyncSandboxRuntimeSession:
        return cast(
            SyncSandboxRuntimeSession,
            iter_coroutine(
                self._require_sync_sandbox_service().create_runtime_session(
                    name=self.name, project_id=self.project_id
                )
            ),
        )

    def run_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> SyncSandboxCommand:
        return cast(
            SyncSandboxCommand,
            iter_coroutine(
                self._require_sync_sandbox_service().run_command(
                    session_id=self.current_session_id,
                    command=command,
                    args=args,
                    cwd=cwd,
                    env=env,
                    sudo=sudo,
                )
            ),
        )

    def start_command(
        self,
        command: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> SyncSandboxCommand:
        return cast(
            SyncSandboxCommand,
            iter_coroutine(
                self._require_sync_sandbox_service().start_command(
                    session_id=self.current_session_id,
                    command=command,
                    args=args,
                    cwd=cwd,
                    env=env,
                    sudo=sudo,
                )
            ),
        )

    def get_command(self, command_id: str, *, wait: bool = False) -> SyncSandboxCommand:
        return cast(
            SyncSandboxCommand,
            iter_coroutine(
                self._require_sync_sandbox_service().get_command(
                    session_id=self.current_session_id, command_id=command_id, wait=wait
                )
            ),
        )

    def query_commands(self) -> list[SyncSandboxCommand]:
        return cast(
            list[SyncSandboxCommand],
            iter_coroutine(
                self._require_sync_sandbox_service().query_commands(
                    session_id=self.current_session_id
                )
            ),
        )

    def list_sessions(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> list[SyncSandboxRuntimeSession]:
        page = iter_coroutine(
            self._require_sync_sandbox_service().query_sessions_page(
                project_id=self.project_id,
                name=self.name,
                page_size=page_size,
                cursor=cursor,
                sort_order=sort_order,
            )
        )
        return cast(list[SyncSandboxRuntimeSession], page.sessions)

    def list_snapshots(
        self,
        *,
        page_size: int | None = None,
        cursor: str | None = None,
        sort_order: str | None = None,
    ) -> list[SyncSnapshot]:
        page = iter_coroutine(
            self._require_sync_sandbox_service().query_snapshots_page(
                project_id=self.project_id,
                name=self.name,
                page_size=page_size,
                cursor=cursor,
                sort_order=sort_order,
            )
        )
        return cast(list[SyncSnapshot], page.snapshots)

    def extend_execution_time_limit(self, duration: DurationInput) -> SyncSandboxRuntimeSession:
        payload = iter_coroutine(
            self._require_sync_sandbox_service().extend_runtime_session_timeout_payload(
                session_id=self.current_session_id, duration=duration
            )
        )
        return cast(SyncSandboxRuntimeSession, self._apply_current_session_payload(payload))

    def update_network_policy(self, network_policy: JSONValue) -> SyncSandboxRuntimeSession:
        payload = iter_coroutine(
            self._require_sync_sandbox_service().update_runtime_session_network_policy_payload(
                session_id=self.current_session_id, network_policy=network_policy
            )
        )
        return cast(SyncSandboxRuntimeSession, self._apply_current_session_payload(payload))

    def mkdir(self, path: str, *, cwd: str | None = None, recursive: bool = True) -> None:
        iter_coroutine(
            self._require_sync_sandbox_service().mkdir(
                session_id=self.current_session_id, path=path, cwd=cwd, recursive=recursive
            )
        )

    def read_file(self, path: str, *, cwd: str | None = None) -> bytes:
        return iter_coroutine(
            self._require_sync_sandbox_service().read_file(
                session_id=self.current_session_id, path=path, cwd=cwd
            )
        )

    def read_text(
        self, path: str, *, cwd: str | None = None, encoding: str = "utf-8", errors: str = "strict"
    ) -> str:
        return self.read_file(path, cwd=cwd).decode(encoding, errors=errors)

    def write_files(
        self, files: Sequence[WriteFile], *, cwd: str | None = None, encoding: str = "utf-8"
    ) -> None:
        iter_coroutine(
            self._require_sync_sandbox_service().write_files(
                session_id=self.current_session_id,
                files=files,
                cwd=self._write_files_cwd(cwd),
                encoding=encoding,
            )
        )

    def snapshot(self, *, expiration: DurationInput = None) -> SyncSnapshot:
        snapshot, payload = iter_coroutine(
            self._require_sync_sandbox_service().create_snapshot_for_session(
                session_id=self.current_session_id, expiration=expiration
            )
        )
        self._apply_current_session_payload(payload)
        return cast(SyncSnapshot, snapshot)

    def destroy(self) -> "SyncSandbox":
        payload = iter_coroutine(
            self._require_sync_sandbox_service().destroy_sandbox_payload(
                name=self.name, project_id=self.project_id
            )
        )
        self._apply_payload(payload)
        return self

    def update(
        self,
        *,
        runtime: str | None = None,
        ports: list[int] | None = None,
        execution_time_limit: DurationInput = None,
        resources: SandboxResources | None = None,
        persistent: bool | None = None,
        network_policy: JSONValue | None = None,
        env: dict[str, str] | None = None,
        tags: dict[str, str] | None = None,
        snapshot_expiration: DurationInput = None,
        snapshot_retention: SnapshotRetention | None = None,
        current_snapshot_id: str | None = None,
    ) -> "SyncSandbox":
        payload = iter_coroutine(
            self._require_sync_sandbox_service().update_sandbox_payload(
                name=self.name,
                project_id=self.project_id,
                runtime=runtime,
                ports=ports,
                execution_time_limit=execution_time_limit,
                resources=resources,
                persistent=persistent,
                network_policy=network_policy,
                env=env,
                tags=tags,
                snapshot_expiration=snapshot_expiration,
                snapshot_retention=snapshot_retention,
                current_snapshot_id=current_snapshot_id,
            )
        )
        self._apply_payload(payload)
        return self
