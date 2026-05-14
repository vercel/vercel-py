"""Shared sandbox API logic for sync and async clients.

Uses the iter-coroutine pattern: all business logic lives in async methods on
``BaseSandboxOpsClient``.  ``SyncSandboxOpsClient`` pairs these with a
``SyncTransport`` so that ``iter_coroutine()`` can drive them without an event
loop, while ``AsyncSandboxOpsClient`` uses a real ``AsyncTransport``.
"""

from __future__ import annotations

import io
import os
import platform
import posixpath
import sys
import tarfile
from collections.abc import AsyncGenerator, AsyncIterator, Generator
from contextlib import asynccontextmanager
from datetime import timedelta
from importlib.metadata import version as _pkg_version
from typing import Any, Protocol, TypeAlias, cast

import httpx

from vercel._internal.auth import TokenProvider
from vercel._internal.fs import (
    FileHandle,
    FilesystemClient,
    create_async_filesystem_client,
    create_filesystem_client,
)
from vercel._internal.http import (
    AsyncTransport,
    BaseTransport,
    BytesBody,
    JSONBody,
    SyncTransport,
    TransportOptions,
    create_base_async_client,
    create_base_client,
    extract_structured_error,
)
from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.sandbox.errors import (
    APIError,
    SandboxAuthError,
    SandboxNotFoundError,
    SandboxPermissionError,
    SandboxRateLimitError,
    SandboxServerError,
)
from vercel._internal.sandbox.models import (
    ApiNetworkPolicy,
    CommandFinishedResponse,
    CommandResponse,
    CreateSandboxRequest,
    CreateSnapshotResponse,
    LogLine,
    NetworkPolicy,
    Resources,
    SandboxAndRoutesResponse,
    SandboxesResponse,
    SandboxResponse,
    SnapshotResponse,
    SnapshotsResponse,
    Source,
    WriteFile,
)
from vercel._internal.sandbox.snapshot import SnapshotExpiration
from vercel._internal.time import to_ms_int

try:
    VERSION = _pkg_version("vercel")
except Exception:
    VERSION = "development"

PLATFORM = platform.uname()
USER_AGENT = (
    f"vercel/sandbox/{VERSION} (Python/{sys.version}; {PLATFORM.system}/{PLATFORM.machine})"
)

JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | dict[str, "JSONValue"] | list["JSONValue"]
RequestQuery: TypeAlias = dict[str, str | int | float | bool | None]


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


class ProjectIdProvider(Protocol):
    async def __call__(self) -> str: ...


def _make_sandbox_token_provider() -> TokenProvider:
    from vercel.oidc import get_credentials

    async def _provider() -> str:
        return get_credentials().token

    return _provider


def _make_sandbox_project_id_provider() -> ProjectIdProvider:
    from vercel.oidc import get_credentials

    async def _provider() -> str:
        return get_credentials().project_id

    return _provider


# ---------------------------------------------------------------------------
# Request client — error handling + request_json convenience
# ---------------------------------------------------------------------------


class SandboxRequestClient:
    """Low-level request layer wrapping a :class:`BaseTransport`.

    Translates non-2xx responses into sandbox-specific :class:`APIError`
    subclasses and provides a ``request_json`` convenience method.
    """

    def __init__(
        self,
        *,
        transport: BaseTransport,
        token_provider: TokenProvider,
    ) -> None:
        self._transport = transport
        self._token_provider = token_provider

    async def resolve_token(self, token: str | None = None) -> str:
        if token is not None:
            return token
        return await self._token_provider()

    async def request(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        headers: dict[str, str] | None = None,
        params: RequestQuery | None = None,
        body: JSONBody | BytesBody | None = None,
        stream: bool = False,
    ) -> httpx.Response:
        resolved_token = await self.resolve_token(token)
        response = await self._transport.send(
            method,
            path,
            token=resolved_token,
            headers=headers,
            params=params,
            body=body,
            stream=stream,
        )

        if not response.is_success:
            try:
                response.read()
            except RuntimeError:
                await response.aread()
            raise _build_sandbox_error(response)

        return response

    async def request_json(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        headers: dict[str, str] | None = None,
        params: RequestQuery | None = None,
        body: JSONBody | BytesBody | None = None,
        stream: bool = False,
    ) -> JSONValue:
        headers = dict(headers or {})
        headers.setdefault("content-type", "application/json")

        r = await self.request(
            method,
            path,
            token=token,
            headers=headers,
            params=params,
            body=body,
            stream=stream,
        )
        return cast(JSONValue, r.json())

    def close(self) -> None:
        if isinstance(self._transport, SyncTransport):
            self._transport.close()

    async def aclose(self) -> None:
        if isinstance(self._transport, AsyncTransport):
            await self._transport.aclose()


def _build_sandbox_error(response: httpx.Response) -> APIError:
    message, data = extract_structured_error(response)
    status_code = response.status_code

    if status_code == 404:
        return SandboxNotFoundError(response, message, data=data)
    if status_code == 401:
        return SandboxAuthError(response, message, data=data)
    if status_code == 403:
        return SandboxPermissionError(response, message, data=data)
    if status_code == 429:
        return SandboxRateLimitError(
            response,
            message,
            data=data,
            retry_after=response.headers.get("retry-after"),
        )
    if 500 <= status_code < 600:
        return SandboxServerError(response, message, data=data)
    return APIError(response, message, data=data)


# ---------------------------------------------------------------------------
# Tarball builder (pure Python, no I/O)
# ---------------------------------------------------------------------------


def _normalize_mode(mode: object) -> int | None:
    match mode:
        case None:
            return None
        case bool():
            raise TypeError("mode must be an integer between 0 and 0o777")
        case int() if 0 <= mode <= 0o777:
            return mode
        case int():
            raise ValueError("mode must be an integer between 0 and 0o777")
        case _:
            raise TypeError("mode must be an integer between 0 and 0o777")


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
            mode = _normalize_mode(f.get("mode"))
            if mode is not None:
                info.mode = mode
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
    _filesystem_client: FilesystemClient[Any]

    def __init__(
        self,
        *,
        request_client: SandboxRequestClient,
        filesystem_client: FilesystemClient[Any],
        project_id_provider: ProjectIdProvider | None = None,
    ) -> None:
        self._request_client = request_client
        self._filesystem_client = filesystem_client
        self._project_id_provider = project_id_provider or _make_sandbox_project_id_provider()

    async def resolve_project_id(self) -> str:
        return await self._project_id_provider()

    async def _get(
        self,
        path: str,
        *,
        token: str | None = None,
        params: RequestQuery | None = None,
    ) -> JSONValue:
        return await self._request_client.request_json(
            "GET",
            path,
            token=token,
            params=params,
        )

    async def _post(
        self,
        path: str,
        *,
        token: str | None = None,
        body: JSONBody | BytesBody | None = None,
        stream: bool = False,
    ) -> JSONValue:
        return await self._request_client.request_json(
            "POST",
            path,
            token=token,
            body=body,
            stream=stream,
        )

    async def create_sandbox(
        self,
        *,
        project_id: str,
        token: str | None = None,
        ports: list[int] | None = None,
        source: Source | None = None,
        timeout: int | timedelta | None = None,
        resources: Resources | None = None,
        runtime: str | None = None,
        network_policy: NetworkPolicy | None = None,
        interactive: bool = False,
        env: dict[str, str] | None = None,
    ) -> SandboxAndRoutesResponse:
        body = CreateSandboxRequest(
            project_id=project_id,
            ports=ports if ports else None,
            source=source,
            timeout=timeout,
            resources=resources,
            runtime=runtime,
            network_policy=network_policy,
            interactive=True if interactive else None,
            env=env,
        ).model_dump(by_alias=True, exclude_none=True)
        data = await self._post(
            "/v1/sandboxes",
            token=token,
            body=JSONBody(body),
        )
        return SandboxAndRoutesResponse.model_validate(data)

    async def get_sandbox(
        self,
        *,
        sandbox_id: str,
        token: str | None = None,
    ) -> SandboxAndRoutesResponse:
        data = await self._get(
            f"/v1/sandboxes/{sandbox_id}",
            token=token,
        )
        return SandboxAndRoutesResponse.model_validate(data)

    async def list_sandboxes(
        self,
        *,
        project_id: str | None = None,
        token: str | None = None,
        limit: int | None = None,
        since: int | None = None,
        until: int | None = None,
    ) -> SandboxesResponse:
        params: RequestQuery = {
            "project": project_id,
            "limit": limit,
            "since": since,
            "until": until,
        }
        data = await self._get(
            "/v1/sandboxes",
            token=token,
            params={k: v for k, v in params.items() if v is not None},
        )
        return SandboxesResponse.model_validate(data)

    async def update_network_policy(
        self,
        *,
        sandbox_id: str,
        token: str | None = None,
        network_policy: ApiNetworkPolicy,
    ) -> SandboxResponse:
        data = await self._post(
            f"/v1/sandboxes/{sandbox_id}/network-policy",
            token=token,
            body=JSONBody(network_policy.model_dump(by_alias=True, exclude_none=True)),
        )
        return SandboxResponse.model_validate(data)

    async def run_command(
        self,
        *,
        sandbox_id: str,
        token: str | None = None,
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
        data = await self._post(
            f"/v1/sandboxes/{sandbox_id}/cmd",
            token=token,
            body=JSONBody(body),
        )
        return CommandResponse.model_validate(data)

    async def get_command(
        self,
        *,
        sandbox_id: str,
        cmd_id: str,
        wait: bool = False,
        token: str | None = None,
    ) -> CommandResponse | CommandFinishedResponse:
        data = await self._get(
            f"/v1/sandboxes/{sandbox_id}/cmd/{cmd_id}",
            token=token,
            params={"wait": "true"} if wait else None,
        )
        if wait:
            return CommandFinishedResponse.model_validate(data)
        return CommandResponse.model_validate(data)

    async def stop_sandbox(
        self,
        *,
        sandbox_id: str,
        token: str | None = None,
    ) -> SandboxResponse:
        data = await self._post(
            f"/v1/sandboxes/{sandbox_id}/stop",
            token=token,
        )
        return SandboxResponse.model_validate(data)

    async def mk_dir(
        self,
        *,
        sandbox_id: str,
        path: str,
        cwd: str | None = None,
        token: str | None = None,
    ) -> None:
        body: dict[str, Any] = {"path": path}
        if cwd is not None:
            body["cwd"] = cwd

        await self._post(
            f"/v1/sandboxes/{sandbox_id}/fs/mkdir",
            token=token,
            body=JSONBody(body),
        )

    async def read_file(
        self,
        *,
        sandbox_id: str,
        path: str,
        cwd: str | None = None,
        token: str | None = None,
    ) -> bytes:
        body: dict[str, Any] = {"path": path}
        if cwd is not None:
            body["cwd"] = cwd
        resp = await self._request_client.request(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/fs/read",
            token=token,
            body=JSONBody(body),
        )
        return resp.content

    async def _open_file_stream(
        self,
        *,
        sandbox_id: str,
        path: str,
        cwd: str | None = None,
        token: str | None = None,
    ) -> httpx.Response:
        body: dict[str, Any] = {"path": path}
        if cwd is not None:
            body["cwd"] = cwd
        return await self._request_client.request(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/fs/read",
            token=token,
            body=JSONBody(body),
            stream=True,
        )

    def _stream_file_chunks(
        self, response: httpx.Response, *, chunk_size: int
    ) -> AsyncIterator[bytes]:
        raise NotImplementedError

    async def _close_file_response(self, response: httpx.Response) -> None:
        raise NotImplementedError

    @asynccontextmanager
    async def file_chunk_stream(
        self,
        *,
        sandbox_id: str,
        path: str,
        cwd: str | None = None,
        chunk_size: int = 65536,
        token: str | None = None,
    ) -> AsyncIterator[AsyncIterator[bytes]]:
        response = await self._open_file_stream(
            sandbox_id=sandbox_id,
            path=path,
            cwd=cwd,
            token=token,
        )

        try:
            yield self._stream_file_chunks(response, chunk_size=chunk_size)
        finally:
            await self._close_file_response(response)

    async def download_file(
        self,
        *,
        sandbox_id: str,
        remote_path: str,
        local_path: str | os.PathLike,
        cwd: str | None = None,
        create_parents: bool = False,
        chunk_size: int = 65536,
        token: str | None = None,
    ) -> str:
        if not remote_path:
            raise ValueError("remote_path is required")
        if not local_path:
            raise ValueError("local_path is required")

        destination = os.path.abspath(await self._filesystem_client.coerce_path(local_path))
        if create_parents:
            await self._filesystem_client.create_parent_directories(destination)
        temp_path = destination + ".part"

        async with self.file_chunk_stream(
            sandbox_id=sandbox_id,
            path=remote_path,
            cwd=cwd,
            chunk_size=chunk_size,
            token=token,
        ) as stream:
            handle: FileHandle | None = None
            try:
                handle = await self._filesystem_client.open_binary_writer(temp_path)
                try:
                    async for chunk in stream:
                        if chunk:
                            await self._filesystem_client.write(handle, chunk)
                finally:
                    if handle is not None:
                        await self._filesystem_client.close(handle)
                        handle = None
                await self._filesystem_client.replace(temp_path, destination)
            except Exception:
                await self._filesystem_client.remove_if_exists(temp_path)
                raise

        return destination

    async def write_files(
        self,
        *,
        sandbox_id: str,
        files: list[WriteFile],
        extract_dir: str,
        cwd: str,
        token: str | None = None,
    ) -> None:
        payload = _build_tarball(files, cwd, extract_dir)
        await self._request_client.request(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/fs/write",
            token=token,
            headers={
                "x-cwd": extract_dir,
            },
            body=BytesBody(payload, "application/gzip"),
        )

    async def kill_command(
        self,
        *,
        sandbox_id: str,
        command_id: str,
        signal: int = 15,
        token: str | None = None,
    ) -> None:
        await self._request_client.request(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/cmd/{command_id}/kill",
            token=token,
            body=JSONBody({"signal": signal}),
        )

    async def extend_timeout(
        self,
        *,
        sandbox_id: str,
        duration: timedelta,
        token: str | None = None,
    ) -> SandboxResponse:
        data = await self._post(
            f"/v1/sandboxes/{sandbox_id}/extend-timeout",
            token=token,
            body=JSONBody({"duration": to_ms_int(duration)}),
        )
        return SandboxResponse.model_validate(data)

    async def create_snapshot(
        self,
        *,
        sandbox_id: str,
        expiration: SnapshotExpiration | None = None,
        token: str | None = None,
    ) -> CreateSnapshotResponse:
        body = None if expiration is None else JSONBody({"expiration": int(expiration)})
        data = await self._post(
            f"/v1/sandboxes/{sandbox_id}/snapshot",
            token=token,
            body=body,
        )
        return CreateSnapshotResponse.model_validate(data)

    async def get_snapshot(
        self,
        *,
        snapshot_id: str,
        token: str | None = None,
    ) -> SnapshotResponse:
        data = await self._get(
            f"/v1/sandboxes/snapshots/{snapshot_id}",
            token=token,
        )
        return SnapshotResponse.model_validate(data)

    async def list_snapshots(
        self,
        *,
        project_id: str | None = None,
        token: str | None = None,
        limit: int | None = None,
        since: int | None = None,
        until: int | None = None,
    ) -> SnapshotsResponse:
        params: RequestQuery = {
            "project": project_id,
            "limit": limit,
            "since": since,
            "until": until,
        }
        data = await self._get(
            "/v1/sandboxes/snapshots",
            token=token,
            params={k: v for k, v in params.items() if v is not None},
        )
        return SnapshotsResponse.model_validate(data)

    async def delete_snapshot(
        self,
        *,
        snapshot_id: str,
        token: str | None = None,
    ) -> SnapshotResponse:
        data = await self._request_client.request_json(
            "DELETE",
            f"/v1/sandboxes/snapshots/{snapshot_id}",
            token=token,
        )
        return SnapshotResponse.model_validate(data)

    async def _get_log_stream(
        self,
        *,
        sandbox_id: str,
        cmd_id: str,
        token: str | None = None,
    ) -> httpx.Response:
        return await self._request_client.request(
            "GET",
            f"/v1/sandboxes/{sandbox_id}/cmd/{cmd_id}/logs",
            token=token,
            headers={"accept": "text/event-stream"},
            stream=True,
        )


# ---------------------------------------------------------------------------
# Sync variant
# ---------------------------------------------------------------------------


class SyncSandboxOpsClient(BaseSandboxOpsClient):
    def __init__(
        self,
        *,
        host: str = "https://api.vercel.com",
        filesystem_client: FilesystemClient[Any] | None = None,
    ) -> None:
        transport_options = TransportOptions(
            timeout=timedelta(seconds=180),
            base_url=host,
            max_connections=100,
            enable_http2=False,
        )
        transport = SyncTransport(create_base_client(transport_options))
        super().__init__(
            request_client=SandboxRequestClient(
                transport=transport,
                token_provider=_make_sandbox_token_provider(),
            ),
            filesystem_client=filesystem_client or create_filesystem_client(),
            project_id_provider=_make_sandbox_project_id_provider(),
        )

    def get_logs(
        self,
        *,
        sandbox_id: str,
        cmd_id: str,
        token: str | None = None,
    ) -> Generator[LogLine, None, None]:
        resp = iter_coroutine(
            self._get_log_stream(
                sandbox_id=sandbox_id,
                cmd_id=cmd_id,
                token=token,
            )
        )
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

    def iter_file(
        self,
        *,
        sandbox_id: str,
        path: str,
        cwd: str | None = None,
        chunk_size: int = 65536,
        token: str | None = None,
    ) -> Generator[bytes, None, None]:
        resp = iter_coroutine(
            self._open_file_stream(
                sandbox_id=sandbox_id,
                path=path,
                cwd=cwd,
                token=token,
            )
        )

        def _iterate() -> Generator[bytes, None, None]:
            try:
                for chunk in resp.iter_bytes(chunk_size=chunk_size):
                    if chunk:
                        yield chunk
            finally:
                resp.close()

        return _iterate()

    def _stream_file_chunks(
        self, response: httpx.Response, *, chunk_size: int
    ) -> AsyncIterator[bytes]:
        async def _iterate() -> AsyncIterator[bytes]:
            for chunk in response.iter_bytes(chunk_size=chunk_size):
                yield chunk

        return _iterate()

    async def _close_file_response(self, response: httpx.Response) -> None:
        response.close()

    def close(self) -> None:
        self._request_client.close()

    def __enter__(self) -> SyncSandboxOpsClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Async variant
# ---------------------------------------------------------------------------


class AsyncSandboxOpsClient(BaseSandboxOpsClient):
    def __init__(
        self,
        *,
        host: str = "https://api.vercel.com",
        filesystem_client: FilesystemClient[Any] | None = None,
    ) -> None:
        transport_options = TransportOptions(
            timeout=timedelta(seconds=180),
            base_url=host,
            max_connections=100,
            enable_http2=False,
        )
        transport = AsyncTransport(create_base_async_client(transport_options))
        super().__init__(
            request_client=SandboxRequestClient(
                transport=transport,
                token_provider=_make_sandbox_token_provider(),
            ),
            filesystem_client=filesystem_client or create_async_filesystem_client(),
            project_id_provider=_make_sandbox_project_id_provider(),
        )

    async def get_logs(
        self,
        *,
        sandbox_id: str,
        cmd_id: str,
        token: str | None = None,
    ) -> AsyncGenerator[LogLine, None]:
        resp = await self._get_log_stream(
            sandbox_id=sandbox_id,
            cmd_id=cmd_id,
            token=token,
        )
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

    async def iter_file(
        self,
        *,
        sandbox_id: str,
        path: str,
        cwd: str | None = None,
        chunk_size: int = 65536,
        token: str | None = None,
    ) -> AsyncGenerator[bytes, None]:
        resp = await self._open_file_stream(
            sandbox_id=sandbox_id,
            path=path,
            cwd=cwd,
            token=token,
        )

        async def _iterate() -> AsyncGenerator[bytes, None]:
            try:
                async for chunk in resp.aiter_bytes(chunk_size=chunk_size):
                    if chunk:
                        yield chunk
            finally:
                await resp.aclose()

        return _iterate()

    def _stream_file_chunks(
        self, response: httpx.Response, *, chunk_size: int
    ) -> AsyncIterator[bytes]:
        async def _iterate() -> AsyncIterator[bytes]:
            async for chunk in response.aiter_bytes(chunk_size=chunk_size):
                yield chunk

        return _iterate()

    async def _close_file_response(self, response: httpx.Response) -> None:
        await response.aclose()

    async def aclose(self) -> None:
        await self._request_client.aclose()

    async def __aenter__(self) -> AsyncSandboxOpsClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()


__all__ = [
    "SyncSandboxOpsClient",
    "AsyncSandboxOpsClient",
]
