from __future__ import annotations

import builtins
import time
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from os import PathLike
from typing import Any

from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.sandbox.core import AsyncSandboxOpsClient, SyncSandboxOpsClient
from vercel._internal.sandbox.errors import SandboxNotFoundError
from vercel._internal.sandbox.models import (
    CommandResponse,
    Sandbox as SandboxModel,
    SandboxAndRoutesResponse,
    SandboxStatus,
    Source,
    WriteFile,
)
from vercel._internal.sandbox.network_policy import (
    ApiNetworkPolicy,
    NetworkPolicy,
)
from vercel._internal.sandbox.pagination import SandboxListParams
from vercel._internal.sandbox.time import normalize_duration_ms

from ..oidc import Credentials, get_credentials
from .command import (
    AsyncCommand,
    AsyncCommandFinished,
    Command,
    CommandFinished,
)
from .page import AsyncSandboxPage, AsyncSandboxPager, SandboxPage
from .pty.shell import start_interactive_shell
from .snapshot import (
    AsyncSnapshot,
    Snapshot as SnapshotClass,
    SnapshotExpiration,
)


def _normalize_source(source: Source | None) -> dict[str, Any] | None:
    """Convert snake_case keys in source dict to camelCase for the API."""
    if source is None:
        return None

    # Map of snake_case -> camelCase for source dict keys
    key_map = {
        "snapshot_id": "snapshotId",
    }

    return {key_map.get(k, k): v for k, v in source.items()}


async def _build_async_sandbox_page(
    *,
    creds: Credentials,
    params: SandboxListParams,
) -> AsyncSandboxPage:
    client = AsyncSandboxOpsClient(team_id=creds.team_id, token=creds.token)
    try:
        response = await client.list_sandboxes(
            project_id=params.project_id,
            limit=params.limit,
            since=params.since,
            until=params.until,
        )
    finally:
        await client.aclose()

    async def fetch_next_page(page_info) -> AsyncSandboxPage:
        return await _build_async_sandbox_page(
            creds=creds,
            params=params.with_until(page_info.until),
        )

    return AsyncSandboxPage.create(
        sandboxes=response.sandboxes,
        pagination=response.pagination,
        fetch_next_page=fetch_next_page,
    )


async def _build_sync_sandbox_page(
    *,
    creds: Credentials,
    params: SandboxListParams,
) -> SandboxPage:
    client = SyncSandboxOpsClient(team_id=creds.team_id, token=creds.token)
    try:
        response = await client.list_sandboxes(
            project_id=params.project_id,
            limit=params.limit,
            since=params.since,
            until=params.until,
        )
    finally:
        client.close()

    async def fetch_next_page(page_info) -> SandboxPage:
        return await _build_sync_sandbox_page(
            creds=creds,
            params=params.with_until(page_info.until),
        )

    return SandboxPage.create(
        sandboxes=response.sandboxes,
        pagination=response.pagination,
        fetch_next_page=fetch_next_page,
    )


@dataclass
class AsyncSandbox:
    client: AsyncSandboxOpsClient
    sandbox: SandboxModel
    routes: list[dict[str, Any]]

    @property
    def sandbox_id(self) -> str:
        return self.sandbox.id

    @property
    def status(self) -> SandboxStatus:
        return self.sandbox.status

    @property
    def source_snapshot_id(self) -> str | None:
        """If the sandbox was created from a snapshot, the ID of that snapshot."""
        return self.sandbox.source_snapshot_id

    @property
    def timeout(self) -> int:
        """The timeout of the sandbox in milliseconds."""
        return self.sandbox.timeout

    @property
    def network_policy(self) -> NetworkPolicy | None:
        return self.sandbox.network_policy

    @property
    def interactive_port(self) -> int | None:
        """Port for interactive PTY connections.

        Returns None if the sandbox was not created with interactive=True.
        """
        return self.sandbox.interactive_port

    @staticmethod
    async def create(
        *,
        source: Source | None = None,
        ports: list[int] | None = None,
        timeout: int | timedelta | None = None,
        resources: dict[str, Any] | None = None,
        runtime: str | None = None,
        token: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
        interactive: bool = False,
        env: dict[str, str] | None = None,
        network_policy: NetworkPolicy | None = None,
    ) -> AsyncSandbox:
        """Create a new sandbox.

        Args:
            source: Source to initialize the sandbox from (git, tarball, or snapshot).
            ports: List of ports to expose.
            timeout: Sandbox timeout in milliseconds or as a ``timedelta``.
            resources: Resource configuration.
            runtime: Runtime to use.
            token: API token (uses OIDC if not provided).
            project_id: Project ID (uses OIDC if not provided).
            team_id: Team ID (uses OIDC if not provided).
            interactive: Enable interactive shell support. When True, the sandbox
                will have an interactive port for PTY connections.
            env: Default environment variables for the sandbox. These are inherited
                by all commands unless overridden per-command.
            network_policy: Sandbox network policy. Accepts ``"allow-all"``,
                ``"deny-all"``, or ``NetworkPolicyCustom``. Omitted when ``None``.

        Returns:
            Created AsyncSandbox instance.
        """
        creds: Credentials = get_credentials(token=token, project_id=project_id, team_id=team_id)
        client = AsyncSandboxOpsClient(team_id=creds.team_id, token=creds.token)
        resp: SandboxAndRoutesResponse = await client.create_sandbox(
            project_id=creds.project_id,
            source=_normalize_source(source),
            ports=ports,
            timeout=normalize_duration_ms(timeout),
            resources=resources,
            runtime=runtime,
            interactive=interactive,
            env=env,
            network_policy=network_policy,
        )
        return AsyncSandbox(
            client=client,
            sandbox=resp.sandbox,
            routes=[r.model_dump() for r in resp.routes],
        )

    @staticmethod
    async def get(
        *,
        sandbox_id: str,
        token: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
    ) -> AsyncSandbox:
        creds: Credentials = get_credentials(token=token, project_id=project_id, team_id=team_id)
        client = AsyncSandboxOpsClient(team_id=creds.team_id, token=creds.token)
        resp: SandboxAndRoutesResponse = await client.get_sandbox(sandbox_id=sandbox_id)
        return AsyncSandbox(
            client=client,
            sandbox=resp.sandbox,
            routes=[r.model_dump() for r in resp.routes],
        )

    @staticmethod
    def list(
        *,
        limit: int | None = None,
        since: datetime | int | None = None,
        until: datetime | int | None = None,
        token: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
    ) -> AsyncSandboxPager:
        """List sandboxes and return the first page.

        Args:
            limit: Maximum number of sandboxes to request per page.
            since: Lower timestamp bound as a timezone-aware ``datetime`` or
                integer milliseconds since the Unix epoch.
            until: Upper timestamp bound as a timezone-aware ``datetime`` or
                integer milliseconds since the Unix epoch.
            token: API token. Uses configured credentials when omitted.
            project_id: Project ID used for credential resolution and as the
                sandbox list scope. Uses configured credentials when omitted.
            team_id: Team ID scope for the sandbox API.

        Returns:
            An awaitable pager whose first awaited value is the first page of
            typed sandbox results. Continue pagination with ``iter_pages()``
            or ``iter_items()`` on the page or pager.
        """
        creds: Credentials = get_credentials(token=token, project_id=project_id, team_id=team_id)
        params = SandboxListParams(
            project_id=creds.project_id,
            limit=limit,
            since=since,
            until=until,
        )
        return AsyncSandboxPager(
            _fetch_first_page=lambda: _build_async_sandbox_page(creds=creds, params=params)
        )

    async def refresh(self) -> None:
        """Re-fetch this sandbox's state from the API, updating in place."""
        resp = await self.client.get_sandbox(sandbox_id=self.sandbox.id)
        self.sandbox = resp.sandbox
        self.routes = [r.model_dump() for r in resp.routes]

    async def update_network_policy(self, network_policy: NetworkPolicy) -> NetworkPolicy:
        response = await self.client.update_network_policy(
            sandbox_id=self.sandbox.id,
            network_policy=ApiNetworkPolicy.from_network_policy(network_policy),
        )
        self.sandbox = response.sandbox
        updated_network_policy = self.sandbox.network_policy
        if updated_network_policy is None:
            raise RuntimeError("Sandbox API response did not include network policy")
        return updated_network_policy

    async def wait_for_status(
        self,
        status: SandboxStatus | str,
        *,
        timeout: float = 30.0,
        poll_interval: float = 0.5,
    ) -> None:
        """Wait for this sandbox to reach the given status.

        Args:
            status: The target status to wait for (e.g. ``"running"``).
            timeout: Maximum time to wait in seconds.
            poll_interval: Time between status checks in seconds.

        Raises:
            TimeoutError: If the sandbox does not reach *status* within *timeout*.
        """
        import asyncio

        target_status = SandboxStatus(status)
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            if self.status == target_status:
                return
            await asyncio.sleep(poll_interval)
            await self.refresh()
        if self.status == target_status:
            return
        raise TimeoutError(
            f"Sandbox {self.sandbox_id} did not reach '{target_status}' status within {timeout}s"
        )

    def domain(self, port: int) -> str:
        for r in self.routes:
            if r.get("port") == port:
                # Prefer URL when provided by the API; fall back to subdomain
                return r.get("url") or f"https://{r['subdomain']}.vercel.run"
        raise ValueError(f"No route for port {port}")

    async def get_command(self, cmd_id: str) -> AsyncCommand:
        resp = await self.client.get_command(sandbox_id=self.sandbox.id, cmd_id=cmd_id)
        assert isinstance(resp, CommandResponse)
        return AsyncCommand(client=self.client, sandbox_id=self.sandbox.id, cmd=resp.command)

    async def run_command(
        self,
        cmd: str,
        args: builtins.list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> AsyncCommandFinished:
        command_response = await self.client.run_command(
            sandbox_id=self.sandbox.id,
            command=cmd,
            args=args or [],
            cwd=cwd,
            env=env or {},
            sudo=sudo,
        )
        command = AsyncCommand(
            client=self.client, sandbox_id=self.sandbox.id, cmd=command_response.command
        )
        # Wait for completion
        return await command.wait()

    async def run_command_detached(
        self,
        cmd: str,
        args: builtins.list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> AsyncCommand:
        command_response = await self.client.run_command(
            sandbox_id=self.sandbox.id,
            command=cmd,
            args=args or [],
            cwd=cwd,
            env=env or {},
            sudo=sudo,
        )
        return AsyncCommand(
            client=self.client, sandbox_id=self.sandbox.id, cmd=command_response.command
        )

    async def mk_dir(self, path: str, *, cwd: str | None = None) -> None:
        await self.client.mk_dir(sandbox_id=self.sandbox.id, path=path, cwd=cwd)

    async def iter_file(
        self, path: str, *, cwd: str | None = None, chunk_size: int = 65536
    ) -> AsyncIterator[bytes]:
        return await self.client.iter_file(
            sandbox_id=self.sandbox.id,
            path=path,
            cwd=cwd,
            chunk_size=chunk_size,
        )

    async def read_file(self, path: str, *, cwd: str | None = None) -> bytes | None:
        try:
            return await self.client.read_file(sandbox_id=self.sandbox.id, path=path, cwd=cwd)
        except SandboxNotFoundError:
            return None

    async def download_file(
        self,
        remote_path: str,
        local_path: str | PathLike,
        *,
        cwd: str | None = None,
        create_parents: bool = False,
        chunk_size: int = 65536,
    ) -> str:
        return await self.client.download_file(
            sandbox_id=self.sandbox.id,
            remote_path=remote_path,
            local_path=local_path,
            cwd=cwd,
            create_parents=create_parents,
            chunk_size=chunk_size,
        )

    async def write_files(self, files: builtins.list[WriteFile]) -> None:
        await self.client.write_files(
            sandbox_id=self.sandbox.id,
            cwd=self.sandbox.cwd,
            extract_dir="/",
            files=files,
        )

    async def stop(
        self,
        *,
        blocking: bool = False,
        timeout: float = 30.0,
        poll_interval: float = 0.5,
    ) -> None:
        """Stop this sandbox.

        Args:
            blocking: When ``True``, wait until the sandbox reaches
                ``"stopped"`` before returning.
            timeout: Maximum time to wait in seconds when ``blocking=True``.
            poll_interval: Time between refreshes in seconds when
                ``blocking=True``.

        Raises:
            TimeoutError: If ``blocking=True`` and the sandbox does not reach
                ``"stopped"`` within *timeout*.
        """
        response = await self.client.stop_sandbox(sandbox_id=self.sandbox.id)
        self.sandbox = response.sandbox
        if not blocking:
            return
        await self.wait_for_status("stopped", timeout=timeout, poll_interval=poll_interval)

    async def extend_timeout(self, duration: int | timedelta) -> None:
        """
        Extend the timeout of the sandbox by the specified duration.

        This allows you to extend the lifetime of a sandbox up until the maximum
        execution timeout for your plan.

        Args:
            duration: The duration in milliseconds or as a ``timedelta`` to
                extend the timeout by.
        """
        response = await self.client.extend_timeout(sandbox_id=self.sandbox.id, duration=duration)
        self.sandbox = response.sandbox

    async def snapshot(
        self, *, expiration: int | timedelta | SnapshotExpiration | None = None
    ) -> AsyncSnapshot:
        """
        Create a snapshot from this currently running sandbox.
        New sandboxes can then be created from this snapshot.

        Note: this sandbox will be stopped as part of the snapshot creation process.
        """
        normalized_expiration = None if expiration is None else SnapshotExpiration(expiration)
        response = await self.client.create_snapshot(
            sandbox_id=self.sandbox.id,
            expiration=normalized_expiration,
        )
        self.sandbox = response.sandbox
        return AsyncSnapshot(client=self.client, snapshot=response.snapshot)

    async def shell(
        self,
        command: builtins.list[str] | None = None,
        *,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        sudo: bool = False,
    ) -> None:
        """Start an interactive shell session.

        This takes over the terminal and provides a full interactive experience,
        forwarding stdin/stdout between the local terminal and the remote sandbox.

        Requires the sandbox to be created with interactive=True.

        Args:
            command: Command to execute (default: ["/bin/bash"]).
            env: Additional environment variables.
            cwd: Working directory.
            sudo: Run with elevated privileges.

        Raises:
            RuntimeError: If sandbox doesn't have interactive support enabled.

        Example:
            async with await AsyncSandbox.create(interactive=True) as sandbox:
                await sandbox.shell(["python3"])
        """
        await start_interactive_shell(self, command, env=env, cwd=cwd, sudo=sudo)

    # Async context manager to ensure cleanup
    async def __aenter__(self) -> AsyncSandbox:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            await self.stop()
        except Exception:
            # Best-effort stop; ignore errors during teardown
            pass
        await self.client.aclose()


@dataclass
class Sandbox:
    client: SyncSandboxOpsClient
    sandbox: SandboxModel
    routes: list[dict[str, Any]]

    @property
    def sandbox_id(self) -> str:
        return self.sandbox.id

    @property
    def status(self) -> SandboxStatus:
        return self.sandbox.status

    @property
    def source_snapshot_id(self) -> str | None:
        """If the sandbox was created from a snapshot, the ID of that snapshot."""
        return self.sandbox.source_snapshot_id

    @property
    def interactive_port(self) -> int | None:
        """Port for interactive PTY connections.

        Returns None if the sandbox was not created with interactive=True.

        Note: For interactive shell sessions, use AsyncSandbox instead.
        """
        return self.sandbox.interactive_port

    @property
    def timeout(self) -> int:
        """The timeout of the sandbox in milliseconds."""
        return self.sandbox.timeout

    @property
    def network_policy(self) -> NetworkPolicy | None:
        return self.sandbox.network_policy

    @staticmethod
    def create(
        *,
        source: Source | None = None,
        ports: list[int] | None = None,
        timeout: int | timedelta | None = None,
        resources: dict[str, Any] | None = None,
        runtime: str | None = None,
        token: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
        interactive: bool = False,
        env: dict[str, str] | None = None,
        network_policy: NetworkPolicy | None = None,
    ) -> Sandbox:
        """Create a new sandbox.

        Args:
            source: Source to initialize the sandbox from (git, tarball, or snapshot).
            ports: List of ports to expose.
            timeout: Sandbox timeout in milliseconds or as a ``timedelta``.
            resources: Resource configuration.
            runtime: Runtime to use.
            token: API token (uses OIDC if not provided).
            project_id: Project ID (uses OIDC if not provided).
            team_id: Team ID (uses OIDC if not provided).
            interactive: Enable interactive shell support. When True, the sandbox
                will have an interactive port for PTY connections.
                Note: For interactive shell sessions, use AsyncSandbox instead.
            env: Default environment variables for the sandbox. These are inherited
                by all commands unless overridden per-command.
            network_policy: Sandbox network policy. Accepts ``"allow-all"``,
                ``"deny-all"``, or ``NetworkPolicyCustom``. Omitted when ``None``.

        Returns:
            Created Sandbox instance.
        """
        creds: Credentials = get_credentials(token=token, project_id=project_id, team_id=team_id)
        client = SyncSandboxOpsClient(team_id=creds.team_id, token=creds.token)
        resp: SandboxAndRoutesResponse = iter_coroutine(
            client.create_sandbox(
                project_id=creds.project_id,
                source=_normalize_source(source),
                ports=ports,
                timeout=normalize_duration_ms(timeout),
                resources=resources,
                runtime=runtime,
                interactive=interactive,
                env=env,
                network_policy=network_policy,
            )
        )
        return Sandbox(
            client=client,
            sandbox=resp.sandbox,
            routes=[r.model_dump() for r in resp.routes],
        )

    @staticmethod
    def get(
        *,
        sandbox_id: str,
        token: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
    ) -> Sandbox:
        creds: Credentials = get_credentials(token=token, project_id=project_id, team_id=team_id)
        client = SyncSandboxOpsClient(team_id=creds.team_id, token=creds.token)
        resp: SandboxAndRoutesResponse = iter_coroutine(client.get_sandbox(sandbox_id=sandbox_id))
        return Sandbox(
            client=client,
            sandbox=resp.sandbox,
            routes=[r.model_dump() for r in resp.routes],
        )

    @staticmethod
    def list(
        *,
        limit: int | None = None,
        since: datetime | int | None = None,
        until: datetime | int | None = None,
        token: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
    ) -> SandboxPage:
        """List sandboxes and return the first page.

        Args:
            limit: Maximum number of sandboxes to request per page.
            since: Lower timestamp bound as a timezone-aware ``datetime`` or
                integer milliseconds since the Unix epoch.
            until: Upper timestamp bound as a timezone-aware ``datetime`` or
                integer milliseconds since the Unix epoch.
            token: API token. Uses configured credentials when omitted.
            project_id: Project ID used for credential resolution and as the
                sandbox list scope. Uses configured credentials when omitted.
            team_id: Team ID scope for the sandbox API.

        Returns:
            The first page of typed sandbox results. Continue pagination with
            ``iter_pages()`` or ``iter_items()`` on the returned page.
        """
        creds: Credentials = get_credentials(token=token, project_id=project_id, team_id=team_id)
        params = SandboxListParams(
            project_id=creds.project_id,
            limit=limit,
            since=since,
            until=until,
        )
        return iter_coroutine(_build_sync_sandbox_page(creds=creds, params=params))

    def refresh(self) -> None:
        """Re-fetch this sandbox's state from the API, updating in place."""
        resp = iter_coroutine(self.client.get_sandbox(sandbox_id=self.sandbox.id))
        self.sandbox = resp.sandbox
        self.routes = [r.model_dump() for r in resp.routes]

    def update_network_policy(self, network_policy: NetworkPolicy) -> NetworkPolicy:
        response = iter_coroutine(
            self.client.update_network_policy(
                sandbox_id=self.sandbox.id,
                network_policy=ApiNetworkPolicy.from_network_policy(network_policy),
            )
        )
        self.sandbox = response.sandbox
        updated_network_policy = self.sandbox.network_policy
        if updated_network_policy is None:
            raise RuntimeError("Sandbox API response did not include network policy")
        return updated_network_policy

    def wait_for_status(
        self,
        status: SandboxStatus | str,
        *,
        timeout: float = 30.0,
        poll_interval: float = 0.5,
    ) -> None:
        """Wait for this sandbox to reach the given status.

        Args:
            status: The target status to wait for (e.g. ``"running"``).
            timeout: Maximum time to wait in seconds.
            poll_interval: Time between status checks in seconds.

        Raises:
            TimeoutError: If the sandbox does not reach *status* within *timeout*.
        """
        target_status = SandboxStatus(status)
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            if self.status == target_status:
                return
            time.sleep(poll_interval)
            self.refresh()
        if self.status == target_status:
            return
        raise TimeoutError(
            f"Sandbox {self.sandbox_id} did not reach '{target_status}' status within {timeout}s"
        )

    def domain(self, port: int) -> str:
        for r in self.routes:
            if r.get("port") == port:
                return r.get("url") or f"https://{r['subdomain']}.vercel.run"
        raise ValueError(f"No route for port {port}")

    def get_command(self, cmd_id: str) -> Command:
        resp = iter_coroutine(self.client.get_command(sandbox_id=self.sandbox.id, cmd_id=cmd_id))
        assert isinstance(resp, CommandResponse)
        return Command(client=self.client, sandbox_id=self.sandbox.id, cmd=resp.command)

    def run_command(
        self,
        cmd: str,
        args: builtins.list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> CommandFinished:
        command_response = iter_coroutine(
            self.client.run_command(
                sandbox_id=self.sandbox.id,
                command=cmd,
                args=args or [],
                cwd=cwd,
                env=env or {},
                sudo=sudo,
            )
        )
        command = Command(
            client=self.client, sandbox_id=self.sandbox.id, cmd=command_response.command
        )
        return command.wait()

    def run_command_detached(
        self,
        cmd: str,
        args: builtins.list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> Command:
        command_response = iter_coroutine(
            self.client.run_command(
                sandbox_id=self.sandbox.id,
                command=cmd,
                args=args or [],
                cwd=cwd,
                env=env or {},
                sudo=sudo,
            )
        )
        return Command(client=self.client, sandbox_id=self.sandbox.id, cmd=command_response.command)

    def mk_dir(self, path: str, *, cwd: str | None = None) -> None:
        iter_coroutine(self.client.mk_dir(sandbox_id=self.sandbox.id, path=path, cwd=cwd))

    def iter_file(
        self, path: str, *, cwd: str | None = None, chunk_size: int = 65536
    ) -> Iterator[bytes]:
        return self.client.iter_file(
            sandbox_id=self.sandbox.id,
            path=path,
            cwd=cwd,
            chunk_size=chunk_size,
        )

    def read_file(self, path: str, *, cwd: str | None = None) -> bytes | None:
        try:
            return iter_coroutine(
                self.client.read_file(sandbox_id=self.sandbox.id, path=path, cwd=cwd)
            )
        except SandboxNotFoundError:
            return None

    def download_file(
        self,
        remote_path: str,
        local_path: str | PathLike,
        *,
        cwd: str | None = None,
        create_parents: bool = False,
        chunk_size: int = 65536,
    ) -> str:
        return iter_coroutine(
            self.client.download_file(
                sandbox_id=self.sandbox.id,
                remote_path=remote_path,
                local_path=local_path,
                cwd=cwd,
                create_parents=create_parents,
                chunk_size=chunk_size,
            )
        )

    def write_files(self, files: builtins.list[WriteFile]) -> None:
        iter_coroutine(
            self.client.write_files(
                sandbox_id=self.sandbox.id,
                cwd=self.sandbox.cwd,
                extract_dir="/",
                files=files,
            )
        )

    def stop(
        self,
        *,
        blocking: bool = False,
        timeout: float = 30.0,
        poll_interval: float = 0.5,
    ) -> None:
        """Stop this sandbox.

        Args:
            blocking: When ``True``, wait until the sandbox reaches
                ``"stopped"`` before returning.
            timeout: Maximum time to wait in seconds when ``blocking=True``.
            poll_interval: Time between refreshes in seconds when
                ``blocking=True``.

        Raises:
            TimeoutError: If ``blocking=True`` and the sandbox does not reach
                ``"stopped"`` within *timeout*.
        """
        response = iter_coroutine(self.client.stop_sandbox(sandbox_id=self.sandbox.id))
        self.sandbox = response.sandbox
        if not blocking:
            return
        self.wait_for_status("stopped", timeout=timeout, poll_interval=poll_interval)

    def extend_timeout(self, duration: int | timedelta) -> None:
        """
        Extend the timeout of the sandbox by the specified duration.

        This allows you to extend the lifetime of a sandbox up until the maximum
        execution timeout for your plan.

        Args:
            duration: The duration in milliseconds or as a ``timedelta`` to
                extend the timeout by.
        """
        response = iter_coroutine(
            self.client.extend_timeout(sandbox_id=self.sandbox.id, duration=duration)
        )
        self.sandbox = response.sandbox

    def snapshot(
        self, *, expiration: int | timedelta | SnapshotExpiration | None = None
    ) -> SnapshotClass:
        """
        Create a snapshot from this currently running sandbox.
        New sandboxes can then be created from this snapshot.

        Note: this sandbox will be stopped as part of the snapshot creation process.
        """
        normalized_expiration = None if expiration is None else SnapshotExpiration(expiration)
        response = iter_coroutine(
            self.client.create_snapshot(
                sandbox_id=self.sandbox.id,
                expiration=normalized_expiration,
            )
        )
        self.sandbox = response.sandbox
        return SnapshotClass(client=self.client, snapshot=response.snapshot)

    def __enter__(self) -> Sandbox:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            self.stop()
        except Exception:
            pass
        self.client.close()
