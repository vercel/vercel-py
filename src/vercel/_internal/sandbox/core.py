"""Shared sandbox API logic for sync and async clients.

Uses the iter-coroutine pattern: all business logic lives in async methods on
``BaseSandboxOpsClient``.  ``SyncSandboxOpsClient`` pairs these with a
``SyncTransport`` so that ``iter_coroutine()`` can drive them without an event
loop, while ``AsyncSandboxOpsClient`` uses a real ``AsyncTransport``.
"""

from __future__ import annotations

import io
import platform
import posixpath
import sys
import tarfile
from collections.abc import AsyncGenerator, Generator
from importlib.metadata import version as _pkg_version
from typing import Any

import httpx

from vercel._internal.http import (
    BytesBody,
    JSONBody,
    RequestClient,
    create_async_request_client,
    create_request_client,
)
from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.sandbox.errors import APIError
from vercel.sandbox.models import (
    CommandFinishedResponse,
    CommandResponse,
    CreateSnapshotResponse,
    LogLine,
    SandboxAndRoutesResponse,
    SandboxResponse,
    SnapshotResponse,
    WriteFile,
)

try:
    VERSION = _pkg_version("vercel")
except Exception:
    VERSION = "development"

PLATFORM = platform.uname()
USER_AGENT = (
    f"vercel/sandbox/{VERSION} (Python/{sys.version}; {PLATFORM.system}/{PLATFORM.machine})"
)


# ---------------------------------------------------------------------------
# Request client — error handling + request_json convenience
# ---------------------------------------------------------------------------


class SandboxRequestClient:
    """Low-level request layer wrapping a :class:`RequestClient`.

    Translates non-2xx responses into :class:`APIError` and provides
    a ``request_json`` convenience method.
    """

    def __init__(self, *, request_client: RequestClient) -> None:
        self._client = request_client

    async def request(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        query: dict[str, Any] | None = None,
        body: JSONBody | BytesBody | None = None,
        stream: bool = False,
    ) -> httpx.Response:
        params: dict[str, Any] | None = None
        if query:
            params = {k: v for k, v in query.items() if v is not None}

        response = await self._client.send(
            method,
            path,
            headers=headers,
            params=params,
            body=body,
            stream=stream,
        )

        if 200 <= response.status_code < 300:
            return response

        # Parse a helpful error message
        parsed: Any | None = None
        message = f"HTTP {response.status_code}"
        try:
            parsed = response.json()
            if isinstance(parsed, dict):
                if "message" in parsed and isinstance(parsed["message"], str):
                    message = f"{message}: {parsed['message']}"
                elif "error" in parsed:
                    err = parsed["error"]
                    if isinstance(err, dict):
                        code = err.get("code")
                        msg = err.get("message") or err.get("msg")
                        if msg:
                            message = f"{message}: {msg}"
                        if code:
                            message = f"{message} (code={code})"
        except Exception:
            parsed = None

        if parsed is None:
            try:
                text = response.text
                if text:
                    snippet = text if len(text) <= 500 else text[:500] + "\u2026"
                    message = f"{message}: {snippet}"
            except Exception:
                pass

        raise APIError(response, message, data=parsed)

    async def request_json(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> Any:
        headers = kwargs.pop("headers", None) or {}
        headers.setdefault("content-type", "application/json")
        r = await self.request(method, path, headers=headers, **kwargs)
        return r.json()


# ---------------------------------------------------------------------------
# Tarball builder (pure Python, no I/O)
# ---------------------------------------------------------------------------


def _build_tarball(files: list[WriteFile], cwd: str, extract_dir: str) -> bytes:
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
    return buffer.getvalue()


# ---------------------------------------------------------------------------
# Base ops client — shared async business logic
# ---------------------------------------------------------------------------


class BaseSandboxOpsClient:
    """All sandbox API operations as ``async`` methods.

    Concrete subclasses provide a ``SandboxRequestClient`` backed by either a
    sync or async transport.
    """

    _request_client: SandboxRequestClient

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
        env: dict[str, str] | None = None,
    ) -> SandboxAndRoutesResponse:
        body: dict[str, Any] = {"projectId": project_id}
        if ports:
            body["ports"] = ports
        if source is not None:
            body["source"] = source
        if timeout is not None:
            body["timeout"] = timeout
        if resources is not None:
            body["resources"] = resources
        if runtime is not None:
            body["runtime"] = runtime
        if interactive:
            body["__interactive"] = True
        if env is not None:
            body["env"] = env

        data = await self._request_client.request_json("POST", "/v1/sandboxes", body=JSONBody(body))
        return SandboxAndRoutesResponse.model_validate(data)

    async def get_sandbox(self, *, sandbox_id: str) -> SandboxAndRoutesResponse:
        data = await self._request_client.request_json("GET", f"/v1/sandboxes/{sandbox_id}")
        return SandboxAndRoutesResponse.model_validate(data)

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
        body: dict[str, Any] = {
            "command": command,
            "args": args,
            "env": env or {},
            "sudo": sudo,
        }
        if cwd is not None:
            body["cwd"] = cwd
        data = await self._request_client.request_json(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/cmd",
            body=JSONBody(body),
        )
        return CommandResponse.model_validate(data)

    async def get_command(
        self, *, sandbox_id: str, cmd_id: str, wait: bool = False
    ) -> CommandResponse | CommandFinishedResponse:
        data = await self._request_client.request_json(
            "GET",
            f"/v1/sandboxes/{sandbox_id}/cmd/{cmd_id}",
            query={"wait": "true"} if wait else None,
        )
        if wait:
            return CommandFinishedResponse.model_validate(data)
        return CommandResponse.model_validate(data)

    async def stop_sandbox(self, *, sandbox_id: str) -> SandboxResponse:
        data = await self._request_client.request_json("POST", f"/v1/sandboxes/{sandbox_id}/stop")
        return SandboxResponse.model_validate(data)

    async def mk_dir(self, *, sandbox_id: str, path: str, cwd: str | None = None) -> None:
        body: dict[str, Any] = {"path": path}
        if cwd is not None:
            body["cwd"] = cwd
        await self._request_client.request_json(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/fs/mkdir",
            body=JSONBody(body),
        )

    async def read_file(
        self, *, sandbox_id: str, path: str, cwd: str | None = None
    ) -> bytes | None:
        body: dict[str, Any] = {"path": path}
        if cwd is not None:
            body["cwd"] = cwd
        try:
            resp = await self._request_client.request(
                "POST",
                f"/v1/sandboxes/{sandbox_id}/fs/read",
                body=JSONBody(body),
            )
        except APIError as e:
            if e.status_code == 404:
                return None
            raise
        if resp.content is None:
            return None
        return resp.content

    async def write_files(
        self,
        *,
        sandbox_id: str,
        files: list[WriteFile],
        extract_dir: str,
        cwd: str,
    ) -> None:
        payload = _build_tarball(files, cwd, extract_dir)
        await self._request_client.request(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/fs/write",
            headers={
                "x-cwd": extract_dir,
            },
            body=BytesBody(payload, "application/gzip"),
        )

    async def kill_command(self, *, sandbox_id: str, command_id: str, signal: int = 15) -> None:
        await self._request_client.request(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/cmd/{command_id}/kill",
            body=JSONBody({"signal": signal}),
        )

    async def extend_timeout(self, *, sandbox_id: str, duration: int) -> SandboxResponse:
        data = await self._request_client.request_json(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/extend-timeout",
            body=JSONBody({"duration": duration}),
        )
        return SandboxResponse.model_validate(data)

    async def create_snapshot(self, *, sandbox_id: str) -> CreateSnapshotResponse:
        data = await self._request_client.request_json(
            "POST", f"/v1/sandboxes/{sandbox_id}/snapshot"
        )
        return CreateSnapshotResponse.model_validate(data)

    async def get_snapshot(self, *, snapshot_id: str) -> SnapshotResponse:
        data = await self._request_client.request_json(
            "GET", f"/v1/sandboxes/snapshots/{snapshot_id}"
        )
        return SnapshotResponse.model_validate(data)

    async def delete_snapshot(self, *, snapshot_id: str) -> SnapshotResponse:
        data = await self._request_client.request_json(
            "DELETE", f"/v1/sandboxes/snapshots/{snapshot_id}"
        )
        return SnapshotResponse.model_validate(data)

    async def _get_log_stream(self, *, sandbox_id: str, cmd_id: str) -> httpx.Response:
        return await self._request_client.request(
            "GET",
            f"/v1/sandboxes/{sandbox_id}/cmd/{cmd_id}/logs",
            headers={"accept": "text/event-stream"},
            stream=True,
        )


# ---------------------------------------------------------------------------
# Sync variant
# ---------------------------------------------------------------------------


class SyncSandboxOpsClient(BaseSandboxOpsClient):
    def __init__(self, *, host: str = "https://api.vercel.com", team_id: str, token: str) -> None:
        rc = create_request_client(
            token=token,
            base_headers={"user-agent": USER_AGENT},
            base_params={"teamId": team_id},
            timeout=None,
            base_url=host,
        )
        self._request_client = SandboxRequestClient(request_client=rc)
        self._rc = rc

    def get_logs(self, *, sandbox_id: str, cmd_id: str) -> Generator[LogLine, None, None]:
        resp = iter_coroutine(self._get_log_stream(sandbox_id=sandbox_id, cmd_id=cmd_id))
        try:
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
        finally:
            resp.close()

    def close(self) -> None:
        self._rc.close()

    def __enter__(self) -> SyncSandboxOpsClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Async variant
# ---------------------------------------------------------------------------


class AsyncSandboxOpsClient(BaseSandboxOpsClient):
    def __init__(self, *, host: str = "https://api.vercel.com", team_id: str, token: str) -> None:
        rc = create_async_request_client(
            token=token,
            base_headers={"user-agent": USER_AGENT},
            base_params={"teamId": team_id},
            timeout=None,
            base_url=host,
        )
        self._request_client = SandboxRequestClient(request_client=rc)
        self._rc = rc

    async def get_logs(self, *, sandbox_id: str, cmd_id: str) -> AsyncGenerator[LogLine, None]:
        resp = await self._get_log_stream(sandbox_id=sandbox_id, cmd_id=cmd_id)
        try:
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
        finally:
            await resp.aclose()

    async def aclose(self) -> None:
        await self._rc.aclose()

    async def __aenter__(self) -> AsyncSandboxOpsClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()


__all__ = [
    "SyncSandboxOpsClient",
    "AsyncSandboxOpsClient",
]
