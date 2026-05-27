"""Awaitable/context-manager operations for Sandbox flows."""

import warnings
from collections.abc import Generator, Mapping
from dataclasses import dataclass
from types import TracebackType
from typing import Any

from vercel._internal.unstable.context import get_active_session
from vercel._internal.unstable.sandbox.errors import SandboxCleanupError
from vercel._internal.unstable.sandbox.models import (
    DurationInput,
    JSONValue,
    Sandbox,
    SandboxResources,
    SandboxRuntimeSession,
    SandboxSource,
    SnapshotRetention,
)
from vercel._internal.unstable.session import AliveToken, SdkSession


@dataclass(frozen=True, slots=True)
class _CreateSandboxParams:
    project_id: str | None = None
    name: str | None = None
    runtime: str | None = None
    source: SandboxSource | None = None
    ports: list[int] | None = None
    execution_time_limit: DurationInput = None
    resources: SandboxResources | None = None
    persistent: bool | None = None
    network_policy: JSONValue | None = None
    env: Mapping[str, str] | None = None
    tags: Mapping[str, str] | None = None
    snapshot_expiration: DurationInput = None
    snapshot_retention: SnapshotRetention | None = None


class CreateSandboxOperation:
    def __init__(self, *, session: SdkSession, params: _CreateSandboxParams) -> None:
        self._session = session
        self._params = params
        self._consumed = False
        self._handle: Sandbox | None = None
        self._resource_token: AliveToken | None = None

    def _mark_consumed(self) -> None:
        if self._consumed:
            raise RuntimeError("sandbox.create_sandbox(...) operations can only be used once")
        self._consumed = True

    async def _run_once(self) -> Sandbox:
        self._mark_consumed()
        return await self._session.sandbox_service().create_sandbox(
            project_id=self._params.project_id,
            name=self._params.name,
            runtime=self._params.runtime,
            source=self._params.source,
            ports=self._params.ports,
            execution_time_limit=self._params.execution_time_limit,
            resources=self._params.resources,
            persistent=self._params.persistent,
            network_policy=self._params.network_policy,
            env=self._params.env,
            tags=self._params.tags,
            snapshot_expiration=self._params.snapshot_expiration,
            snapshot_retention=self._params.snapshot_retention,
        )

    def __await__(self) -> Generator[Any, None, Sandbox]:
        return self._run_once().__await__()

    async def __aenter__(self) -> Sandbox:
        handle = await self._run_once()
        resource_token = AliveToken()
        handle._attach_resource_token(resource_token)
        self._handle = handle
        self._resource_token = resource_token
        return handle

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._handle is None or self._resource_token is None:
            return None

        try:
            await self._session.sandbox_service().destroy_sandbox(
                name=self._handle.name,
                project_id=self._handle.project_id,
            )
        except Exception as exc:
            raise SandboxCleanupError(
                f"Failed to clean up sandbox {self._handle.name!r}",
                resource_type="sandbox",
                resource_id=self._handle.name,
                cause=exc,
            ) from exc
        self._resource_token.invalidate()
        return None

    def __del__(self) -> None:
        if self._consumed:
            return
        warnings.warn(
            "sandbox.create_sandbox(...) operation was never awaited or entered",
            RuntimeWarning,
            stacklevel=2,
        )


class CreateRuntimeSessionOperation:
    def __init__(self, *, session: SdkSession, sandbox: Sandbox) -> None:
        self._session = session
        self._sandbox = sandbox
        self._consumed = False
        self._handle: SandboxRuntimeSession | None = None
        self._resource_token: AliveToken | None = None

    def _mark_consumed(self) -> None:
        if self._consumed:
            raise RuntimeError("sandbox runtime-session operations can only be used once")
        self._consumed = True

    async def _run_once(self) -> SandboxRuntimeSession:
        self._mark_consumed()
        self._sandbox._raise_if_invalid()
        handle = await self._session.sandbox_service().create_runtime_session(
            name=self._sandbox.name,
            project_id=self._sandbox.project_id,
        )
        self._sandbox._attach_resource_tokens_to_runtime_session(handle)
        return handle

    def __await__(self) -> Generator[Any, None, SandboxRuntimeSession]:
        return self._run_once().__await__()

    async def __aenter__(self) -> SandboxRuntimeSession:
        handle = await self._run_once()
        resource_token = AliveToken()
        handle._attach_resource_token(resource_token)
        self._handle = handle
        self._resource_token = resource_token
        return handle

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._handle is None or self._resource_token is None:
            return None

        try:
            await self._session.sandbox_service().destroy_runtime_session(
                session_id=self._handle.id,
            )
        except Exception as exc:
            raise SandboxCleanupError(
                f"Failed to clean up sandbox runtime session {self._handle.id!r}",
                resource_type="sandbox_runtime_session",
                resource_id=self._handle.id,
                cause=exc,
            ) from exc
        self._resource_token.invalidate()
        return None


def create_sandbox_operation(
    *,
    project_id: str | None = None,
    name: str | None = None,
    runtime: str | None = None,
    source: SandboxSource | None = None,
    ports: list[int] | None = None,
    execution_time_limit: DurationInput = None,
    resources: SandboxResources | None = None,
    persistent: bool | None = None,
    network_policy: JSONValue | None = None,
    env: Mapping[str, str] | None = None,
    tags: Mapping[str, str] | None = None,
    snapshot_expiration: DurationInput = None,
    snapshot_retention: SnapshotRetention | None = None,
) -> CreateSandboxOperation:
    return CreateSandboxOperation(
        session=get_active_session(),
        params=_CreateSandboxParams(
            project_id=project_id,
            name=name,
            runtime=runtime,
            source=source,
            ports=ports,
            execution_time_limit=execution_time_limit,
            resources=resources,
            persistent=persistent,
            network_policy=network_policy,
            env=env,
            tags=tags,
            snapshot_expiration=snapshot_expiration,
            snapshot_retention=snapshot_retention,
        ),
    )


def create_runtime_session_operation(
    *,
    sandbox: Sandbox,
    session: SdkSession,
) -> CreateRuntimeSessionOperation:
    return CreateRuntimeSessionOperation(sandbox=sandbox, session=session)
