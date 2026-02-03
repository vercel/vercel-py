"""Vercel Sandbox API Client."""

from __future__ import annotations

import io
import posixpath
import tarfile
from collections.abc import AsyncGenerator, Generator
from typing import Any

import httpx

from .._http import iter_coroutine
from ._core import (
    USER_AGENT,
    APIError,
    AsyncAPIClient as _AsyncAPIClient,
    SyncAPIClient as _SyncAPIClient,
)
from .models import (
    CommandFinishedResponse,
    CommandResponse,
    CreateSnapshotResponse,
    LogLine,
    SandboxAndRoutesResponse,
    SandboxResponse,
    SnapshotResponse,
    WriteFile,
)

# Re-export APIError for backwards compatibility
__all__ = ["APIClient", "AsyncAPIClient", "APIError"]


class AsyncAPIClient(_AsyncAPIClient):
    """Async client for Sandbox API operations."""

    async def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Make an API request (for backwards compatibility)."""
        return await self._request(method, path, **kwargs)

    async def request_json(self, method: str, path: str, **kwargs: Any) -> Any:
        """Make an API request and return JSON (for backwards compatibility)."""
        return await self._request_json(method, path, **kwargs)

    async def create_sandbox(
        self,
        *,
        project_id: str,
        ports: list[int] | None = None,
        source: dict[str, Any] | None = None,
        timeout: int | None = None,
        resources: dict[str, Any] | None = None,
        runtime: str | None = None,
        interactive: bool = False,
    ) -> SandboxAndRoutesResponse:
        """Create a new sandbox."""
        return await self._create_sandbox(
            project_id=project_id,
            ports=ports,
            source=source,
            timeout=timeout,
            resources=resources,
            runtime=runtime,
            interactive=interactive,
        )

    async def get_sandbox(self, *, sandbox_id: str) -> SandboxAndRoutesResponse:
        """Get sandbox by ID."""
        return await self._get_sandbox(sandbox_id=sandbox_id)

    async def run_command(
        self,
        *,
        sandbox_id: str,
        command: str,
        args: list[str],
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> CommandResponse:
        """Run a command in the sandbox."""
        return await self._run_command(
            sandbox_id=sandbox_id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
        )

    async def get_command(
        self, *, sandbox_id: str, cmd_id: str, wait: bool = False
    ) -> CommandResponse | CommandFinishedResponse:
        """Get command status."""
        return await self._get_command(sandbox_id=sandbox_id, cmd_id=cmd_id, wait=wait)

    async def get_logs(self, *, sandbox_id: str, cmd_id: str) -> AsyncGenerator[LogLine, None]:
        """Stream command logs (cannot be unified - uses async generator)."""
        try:
            async with self._client.stream(
                "GET",
                f"/v1/sandboxes/{sandbox_id}/cmd/{cmd_id}/logs",
                params={"teamId": self._team_id},
                headers={
                    "user-agent": USER_AGENT,
                    "authorization": f"Bearer {self._token}",
                    "accept": "text/event-stream",
                },
            ) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line:
                        continue
                    try:
                        yield LogLine.model_validate_json(line)
                    except Exception:
                        continue
        except (
            httpx.RemoteProtocolError,
            httpx.ReadError,
            httpx.ProtocolError,
            httpx.TransportError,
        ):
            return

    async def stop_sandbox(self, *, sandbox_id: str) -> SandboxResponse:
        """Stop a sandbox."""
        return await self._stop_sandbox(sandbox_id=sandbox_id)

    async def mk_dir(self, *, sandbox_id: str, path: str, cwd: str | None = None) -> None:
        """Create a directory in the sandbox."""
        await self._mk_dir(sandbox_id=sandbox_id, path=path, cwd=cwd)

    async def read_file(
        self, *, sandbox_id: str, path: str, cwd: str | None = None
    ) -> bytes | None:
        """Read a file from the sandbox."""
        return await self._read_file(sandbox_id=sandbox_id, path=path, cwd=cwd)

    async def write_files(
        self,
        *,
        sandbox_id: str,
        files: list[WriteFile],
        extract_dir: str,
        cwd: str,
    ) -> None:
        """Write files to the sandbox (uses raw httpx for binary content)."""

        def normalize_path(file_path: str) -> str:
            base_path = (
                posixpath.normpath(file_path)
                if posixpath.isabs(file_path)
                else posixpath.normpath(posixpath.join(cwd, file_path))
            )
            return posixpath.relpath(base_path, extract_dir)

        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
            for f in files:
                data = f["content"]
                rel = normalize_path(f["path"])
                info = tarfile.TarInfo(name=rel)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))

        payload = buffer.getvalue()
        r = await self._client.request(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/fs/write",
            params={"teamId": self._team_id},
            headers={
                "content-type": "application/gzip",
                "x-cwd": extract_dir,
                "user-agent": USER_AGENT,
                "authorization": f"Bearer {self._token}",
            },
            content=payload,
        )
        r.raise_for_status()

    async def kill_command(self, *, sandbox_id: str, command_id: str, signal: int = 15) -> None:
        """Kill a running command."""
        r = await self._client.request(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/cmd/{command_id}/kill",
            params={"teamId": self._team_id},
            headers={"user-agent": USER_AGENT, "authorization": f"Bearer {self._token}"},
            json={"signal": signal},
        )
        r.raise_for_status()

    async def extend_timeout(self, *, sandbox_id: str, duration: int) -> SandboxResponse:
        """Extend sandbox timeout."""
        return await self._extend_timeout(sandbox_id=sandbox_id, duration=duration)

    async def create_snapshot(self, *, sandbox_id: str) -> CreateSnapshotResponse:
        """Create a snapshot of the sandbox."""
        return await self._create_snapshot(sandbox_id=sandbox_id)

    async def get_snapshot(self, *, snapshot_id: str) -> SnapshotResponse:
        """Get snapshot by ID."""
        return await self._get_snapshot(snapshot_id=snapshot_id)

    async def delete_snapshot(self, *, snapshot_id: str) -> SnapshotResponse:
        """Delete a snapshot."""
        return await self._delete_snapshot(snapshot_id=snapshot_id)


class APIClient(_SyncAPIClient):
    """Sync client for Sandbox API operations."""

    def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Make an API request (for backwards compatibility)."""
        return iter_coroutine(self._request(method, path, **kwargs))

    def request_json(self, method: str, path: str, **kwargs: Any) -> Any:
        """Make an API request and return JSON (for backwards compatibility)."""
        return iter_coroutine(self._request_json(method, path, **kwargs))

    def create_sandbox(
        self,
        *,
        project_id: str,
        ports: list[int] | None = None,
        source: dict[str, Any] | None = None,
        timeout: int | None = None,
        resources: dict[str, Any] | None = None,
        runtime: str | None = None,
        interactive: bool = False,
    ) -> SandboxAndRoutesResponse:
        """Create a new sandbox."""
        return iter_coroutine(
            self._create_sandbox(
                project_id=project_id,
                ports=ports,
                source=source,
                timeout=timeout,
                resources=resources,
                runtime=runtime,
                interactive=interactive,
            )
        )

    def get_sandbox(self, *, sandbox_id: str) -> SandboxAndRoutesResponse:
        """Get sandbox by ID."""
        return iter_coroutine(self._get_sandbox(sandbox_id=sandbox_id))

    def run_command(
        self,
        *,
        sandbox_id: str,
        command: str,
        args: list[str],
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> CommandResponse:
        """Run a command in the sandbox."""
        return iter_coroutine(
            self._run_command(
                sandbox_id=sandbox_id,
                command=command,
                args=args,
                cwd=cwd,
                env=env,
                sudo=sudo,
            )
        )

    def get_command(
        self, *, sandbox_id: str, cmd_id: str, wait: bool = False
    ) -> CommandResponse | CommandFinishedResponse:
        """Get command status."""
        return iter_coroutine(
            self._get_command(sandbox_id=sandbox_id, cmd_id=cmd_id, wait=wait)
        )

    def get_logs(self, *, sandbox_id: str, cmd_id: str) -> Generator[LogLine, None, None]:
        """Stream command logs (cannot be unified - uses sync generator)."""
        try:
            with self._client.stream(
                "GET",
                f"/v1/sandboxes/{sandbox_id}/cmd/{cmd_id}/logs",
                params={"teamId": self._team_id},
                headers={
                    "user-agent": USER_AGENT,
                    "authorization": f"Bearer {self._token}",
                    "accept": "text/event-stream",
                },
            ) as resp:
                resp.raise_for_status()
                for line in resp.iter_lines():
                    if not line:
                        continue
                    try:
                        yield LogLine.model_validate_json(line)
                    except Exception:
                        continue
        except (
            httpx.RemoteProtocolError,
            httpx.ReadError,
            httpx.ProtocolError,
            httpx.TransportError,
        ):
            return

    def stop_sandbox(self, *, sandbox_id: str) -> SandboxResponse:
        """Stop a sandbox."""
        return iter_coroutine(self._stop_sandbox(sandbox_id=sandbox_id))

    def mk_dir(self, *, sandbox_id: str, path: str, cwd: str | None = None) -> None:
        """Create a directory in the sandbox."""
        iter_coroutine(self._mk_dir(sandbox_id=sandbox_id, path=path, cwd=cwd))

    def read_file(self, *, sandbox_id: str, path: str, cwd: str | None = None) -> bytes | None:
        """Read a file from the sandbox."""
        return iter_coroutine(self._read_file(sandbox_id=sandbox_id, path=path, cwd=cwd))

    def write_files(
        self,
        *,
        sandbox_id: str,
        files: list[WriteFile],
        extract_dir: str,
        cwd: str,
    ) -> None:
        """Write files to the sandbox (uses raw httpx for binary content)."""

        def normalize_path(file_path: str) -> str:
            base_path = (
                posixpath.normpath(file_path)
                if posixpath.isabs(file_path)
                else posixpath.normpath(posixpath.join(cwd, file_path))
            )
            return posixpath.relpath(base_path, extract_dir)

        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
            for f in files:
                data = f["content"]
                rel = normalize_path(f["path"])
                info = tarfile.TarInfo(name=rel)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))

        payload = buffer.getvalue()
        r = self._client.request(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/fs/write",
            params={"teamId": self._team_id},
            headers={
                "content-type": "application/gzip",
                "x-cwd": extract_dir,
                "user-agent": USER_AGENT,
                "authorization": f"Bearer {self._token}",
            },
            content=payload,
        )
        r.raise_for_status()

    def kill_command(self, *, sandbox_id: str, command_id: str, signal: int = 15) -> None:
        """Kill a running command."""
        r = self._client.request(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/cmd/{command_id}/kill",
            params={"teamId": self._team_id},
            headers={"user-agent": USER_AGENT, "authorization": f"Bearer {self._token}"},
            json={"signal": signal},
        )
        r.raise_for_status()

    def extend_timeout(self, *, sandbox_id: str, duration: int) -> SandboxResponse:
        """Extend sandbox timeout."""
        return iter_coroutine(self._extend_timeout(sandbox_id=sandbox_id, duration=duration))

    def create_snapshot(self, *, sandbox_id: str) -> CreateSnapshotResponse:
        """Create a snapshot of the sandbox."""
        return iter_coroutine(self._create_snapshot(sandbox_id=sandbox_id))

    def get_snapshot(self, *, snapshot_id: str) -> SnapshotResponse:
        """Get snapshot by ID."""
        return iter_coroutine(self._get_snapshot(snapshot_id=snapshot_id))

    def delete_snapshot(self, *, snapshot_id: str) -> SnapshotResponse:
        """Delete a snapshot."""
        return iter_coroutine(self._delete_snapshot(snapshot_id=snapshot_id))
