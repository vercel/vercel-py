"""Sandbox service layer."""

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

import anyio

from vercel._internal.unstable.sandbox.api_client import SandboxApiClient
from vercel._internal.unstable.sandbox.errors import (
    SandboxResponseError,
    SandboxTerminalStateError,
)
from vercel._internal.unstable.sandbox.models import (
    DurationInput,
    JSONValue,
    Sandbox,
    SandboxRuntimeSession,
    SandboxStatus,
)
from vercel._internal.unstable.sandbox.options import SandboxServiceOptions
from vercel._internal.unstable.session import AliveToken

if TYPE_CHECKING:
    from vercel._internal.unstable.session import SdkSession

_READY_SANDBOX_STATUSES = frozenset({SandboxStatus.RUNNING})
_TERMINAL_SANDBOX_STATUSES = frozenset(
    {SandboxStatus.STOPPED, SandboxStatus.FAILED, SandboxStatus.ABORTED}
)
_TRANSITIONAL_SANDBOX_STATUSES = frozenset(
    {SandboxStatus.PENDING, SandboxStatus.STOPPING, SandboxStatus.SNAPSHOTTING}
)
_READY_POLL_INTERVAL_SECONDS = 0.5
AsyncSleep = Callable[[float], Awaitable[None]]


def _sandbox_status(sandbox: Sandbox) -> SandboxStatus | None:
    if sandbox.current_session is not None and sandbox.current_session.status is not None:
        return sandbox.current_session.status
    return sandbox.status


class SandboxService:
    def __init__(
        self,
        *,
        api_client: SandboxApiClient,
        alive_token: AliveToken,
        options: SandboxServiceOptions,
        sdk_session: "SdkSession | None" = None,
        sleep: AsyncSleep | None = None,
    ) -> None:
        self._api_client = api_client
        self._alive_token = alive_token
        self._options = options
        self._sdk_session = sdk_session
        self._sleep = sleep or anyio.sleep

    @property
    def api_client(self) -> SandboxApiClient:
        return self._api_client

    @property
    def alive_token(self) -> AliveToken:
        return self._alive_token

    @property
    def options(self) -> SandboxServiceOptions:
        return self._options

    def _bind_sandbox(self, sandbox: Sandbox) -> Sandbox:
        sandbox._bind_alive_tokens(
            session_token=self._alive_token,
            sdk_session=self._sdk_session,
        )
        return sandbox

    def _bind_runtime_session(self, session: SandboxRuntimeSession) -> SandboxRuntimeSession:
        session._bind_alive_tokens(session_token=self._alive_token)
        return session

    async def _wait_for_ready_sandbox(
        self,
        sandbox: Sandbox,
        *,
        project_id: str | None = None,
    ) -> Sandbox:
        while True:
            self._alive_token.raise_if_invalid()
            status = _sandbox_status(sandbox)

            if status in _READY_SANDBOX_STATUSES:
                return sandbox

            if status in _TERMINAL_SANDBOX_STATUSES:
                raise SandboxTerminalStateError(
                    f"Sandbox {sandbox.name!r} reached terminal state {status!r}",
                    status=status,
                    sandbox=sandbox,
                )

            if status not in _TRANSITIONAL_SANDBOX_STATUSES:
                raise SandboxResponseError(
                    "Sandbox API response did not include a recognized creation status",
                    data=sandbox.model_dump(by_alias=True),
                )

            await self._sleep(_READY_POLL_INTERVAL_SECONDS)
            sandbox = await self.get_sandbox(
                name=sandbox.name,
                project_id=project_id or sandbox.project_id,
                resume=False,
            )

    async def create_sandbox(
        self,
        *,
        project_id: str | None = None,
        name: str | None = None,
        runtime: str | None = None,
        source: JSONValue | None = None,
        ports: list[int] | None = None,
        timeout: DurationInput = None,
        resources: JSONValue | None = None,
        persistent: bool | None = None,
        network_policy: JSONValue | None = None,
        env: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        snapshot_expiration: DurationInput = None,
        keep_last_snapshots: JSONValue | None = None,
    ) -> Sandbox:
        self._alive_token.raise_if_invalid()
        response = await self._api_client.create_sandbox(
            project_id=project_id,
            name=name,
            runtime=runtime,
            source=source,
            ports=ports,
            timeout=timeout,
            resources=resources,
            persistent=persistent,
            network_policy=network_policy,
            env=env,
            tags=tags,
            snapshot_expiration=snapshot_expiration,
            keep_last_snapshots=keep_last_snapshots,
        )
        sandbox = self._bind_sandbox(response.to_sandbox())
        return await self._wait_for_ready_sandbox(sandbox, project_id=project_id)

    async def get_sandbox(
        self,
        *,
        name: str,
        project_id: str | None = None,
        resume: bool = True,
        include_system_routes: bool | None = None,
    ) -> Sandbox:
        self._alive_token.raise_if_invalid()
        response = await self._api_client.get_sandbox(
            name=name,
            project_id=project_id,
            resume=resume,
            include_system_routes=include_system_routes,
        )
        return self._bind_sandbox(response.to_sandbox())

    async def query_sandboxes(
        self,
        *,
        project_id: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        sort_by: str | None = None,
        sort_order: str | None = None,
        name_prefix: str | None = None,
        tags: str | list[str] | None = None,
    ) -> list[Sandbox]:
        self._alive_token.raise_if_invalid()
        response = await self._api_client.query_sandboxes(
            project_id=project_id,
            limit=limit,
            cursor=cursor,
            sort_by=sort_by,
            sort_order=sort_order,
            name_prefix=name_prefix,
            tags=tags,
        )
        return [self._bind_sandbox(sandbox) for sandbox in response.sandboxes]

    async def destroy_sandbox(self, *, name: str, project_id: str | None = None) -> Sandbox:
        self._alive_token.raise_if_invalid()
        response = await self._api_client.destroy_sandbox(name=name, project_id=project_id)
        return self._bind_sandbox(response.to_sandbox())

    async def create_runtime_session(
        self,
        *,
        name: str,
        project_id: str | None = None,
        resume: bool = True,
        include_system_routes: bool | None = None,
    ) -> SandboxRuntimeSession:
        self._alive_token.raise_if_invalid()
        response = await self._api_client.create_runtime_session(
            name=name,
            project_id=project_id,
            resume=resume,
            include_system_routes=include_system_routes,
        )
        sandbox = response.to_sandbox()
        if sandbox.current_session is None:
            raise SandboxResponseError(
                "Sandbox API response is missing object field 'session'",
                data=response.model_dump(by_alias=True),
            )
        return self._bind_runtime_session(sandbox.current_session)

    async def destroy_runtime_session(self, *, session_id: str) -> Sandbox:
        self._alive_token.raise_if_invalid()
        response = await self._api_client.destroy_runtime_session(session_id=session_id)
        return self._bind_sandbox(response.to_sandbox())

    def close(self) -> None:
        self._api_client.close()

    async def aclose(self) -> None:
        await self._api_client.aclose()
