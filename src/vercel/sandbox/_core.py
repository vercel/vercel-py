"""Core business logic for Vercel Sandbox API."""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from typing import Any

import httpx

from .._http import (
    AsyncTransport,
    BaseTransport,
    BlockingTransport,
    JSONBody,
    create_vercel_async_client,
    create_vercel_client,
)
from .models import (
    CommandFinishedResponse,
    CommandResponse,
    CreateSnapshotResponse,
    SandboxAndRoutesResponse,
    SandboxResponse,
    SnapshotResponse,
)

VERSION = "0.1.0"
USER_AGENT = (
    f"vercel/sandbox/{VERSION} (Python/{sys.version}; {os.uname().sysname}/{os.uname().machine})"
)

DEFAULT_HOST = "https://api.vercel.com"


class APIError(Exception):
    def __init__(self, response: httpx.Response, message: str, *, data: Any | None = None):
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code
        self.data = data


def _parse_error_message(response: httpx.Response) -> tuple[str, Any | None]:
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
                snippet = text if len(text) <= 500 else text[:500] + "..."
                message = f"{message}: {snippet}"
        except Exception:
            pass

    return message, parsed


class _BaseAPIClient:
    """
    Base class for Sandbox API operations.

    All methods are async and use _transport for HTTP requests.
    Subclasses provide sync or async transport implementations.
    """

    _transport: BaseTransport
    _team_id: str
    _token: str
    _host: str

    def _build_headers(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        headers = {
            "user-agent": USER_AGENT,
            "content-type": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    def _build_params(self, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {"teamId": self._team_id}
        if extra:
            params.update({k: v for k, v in extra.items() if v is not None})
        return params

    async def _request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        query: Mapping[str, Any] | None = None,
        json_body: Any | None = None,
    ) -> httpx.Response:
        req_headers = self._build_headers(headers)
        params = self._build_params(query)

        body = JSONBody(json_body) if json_body is not None else None

        resp = await self._transport.send(
            method,
            path,
            headers=req_headers,
            params=params,
            body=body,
        )

        if 200 <= resp.status_code < 300:
            return resp

        message, parsed = _parse_error_message(resp)
        raise APIError(resp, message, data=parsed)

    async def _request_json(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> Any:
        resp = await self._request(method, path, **kwargs)
        return resp.json()

    async def _create_sandbox(
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

        data = await self._request_json("POST", "/v1/sandboxes", json_body=body)
        return SandboxAndRoutesResponse.model_validate(data)

    async def _get_sandbox(self, *, sandbox_id: str) -> SandboxAndRoutesResponse:
        data = await self._request_json("GET", f"/v1/sandboxes/{sandbox_id}")
        return SandboxAndRoutesResponse.model_validate(data)

    async def _run_command(
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
        data = await self._request_json(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/cmd",
            json_body=body,
        )
        return CommandResponse.model_validate(data)

    async def _get_command(
        self, *, sandbox_id: str, cmd_id: str, wait: bool = False
    ) -> CommandResponse | CommandFinishedResponse:
        data = await self._request_json(
            "GET",
            f"/v1/sandboxes/{sandbox_id}/cmd/{cmd_id}",
            query={"wait": "true"} if wait else None,
        )
        if wait:
            return CommandFinishedResponse.model_validate(data)
        return CommandResponse.model_validate(data)

    async def _stop_sandbox(self, *, sandbox_id: str) -> SandboxResponse:
        data = await self._request_json("POST", f"/v1/sandboxes/{sandbox_id}/stop")
        return SandboxResponse.model_validate(data)

    async def _mk_dir(self, *, sandbox_id: str, path: str, cwd: str | None = None) -> None:
        body: dict[str, Any] = {"path": path}
        if cwd is not None:
            body["cwd"] = cwd
        await self._request_json(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/fs/mkdir",
            json_body=body,
        )

    async def _read_file(
        self, *, sandbox_id: str, path: str, cwd: str | None = None
    ) -> bytes | None:
        body: dict[str, Any] = {"path": path}
        if cwd is not None:
            body["cwd"] = cwd
        try:
            resp = await self._request(
                "POST",
                f"/v1/sandboxes/{sandbox_id}/fs/read",
                json_body=body,
            )
        except APIError as e:
            if e.status_code == 404:
                return None
            raise
        if resp.content is None:
            return None
        return resp.content

    async def _extend_timeout(self, *, sandbox_id: str, duration: int) -> SandboxResponse:
        data = await self._request_json(
            "POST",
            f"/v1/sandboxes/{sandbox_id}/extend-timeout",
            json_body={"duration": duration},
        )
        return SandboxResponse.model_validate(data)

    async def _create_snapshot(self, *, sandbox_id: str) -> CreateSnapshotResponse:
        data = await self._request_json("POST", f"/v1/sandboxes/{sandbox_id}/snapshot")
        return CreateSnapshotResponse.model_validate(data)

    async def _get_snapshot(self, *, snapshot_id: str) -> SnapshotResponse:
        data = await self._request_json("GET", f"/v1/sandboxes/snapshots/{snapshot_id}")
        return SnapshotResponse.model_validate(data)

    async def _delete_snapshot(self, *, snapshot_id: str) -> SnapshotResponse:
        data = await self._request_json("DELETE", f"/v1/sandboxes/snapshots/{snapshot_id}")
        return SnapshotResponse.model_validate(data)


class SyncAPIClient(_BaseAPIClient):
    def __init__(self, *, host: str = DEFAULT_HOST, team_id: str, token: str):
        self._host = host.rstrip("/")
        self._team_id = team_id
        self._token = token
        client = create_vercel_client(token=token, timeout=None, base_url=self._host)
        self._transport = BlockingTransport(client)
        # Raw httpx client for streaming/raw access
        self._client = httpx.Client(base_url=self._host, timeout=httpx.Timeout(None))

    def close(self) -> None:
        self._transport.close()
        self._client.close()


class AsyncAPIClient(_BaseAPIClient):
    def __init__(self, *, host: str = DEFAULT_HOST, team_id: str, token: str):
        self._host = host.rstrip("/")
        self._team_id = team_id
        self._token = token
        client = create_vercel_async_client(token=token, timeout=None, base_url=self._host)
        self._transport = AsyncTransport(client)
        # Raw httpx client for streaming/raw access
        self._client = httpx.AsyncClient(base_url=self._host, timeout=httpx.Timeout(None))

    async def aclose(self) -> None:
        await self._transport.aclose()
        await self._client.aclose()


__all__ = [
    "SyncAPIClient",
    "AsyncAPIClient",
    "APIError",
    "USER_AGENT",
    "DEFAULT_HOST",
]
