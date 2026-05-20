import inspect
import platform
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from importlib.metadata import version as _pkg_version
from typing import Any, cast

import httpx

from vercel._internal.http import JSONBody, RetryPolicy, SleepFn, extract_structured_error
from vercel._internal.http.transport import BaseTransport
from vercel._internal.unstable.sandbox.auth import (
    SandboxCredentialProvider,
    SandboxCredentials,
    SyncSandboxCredentialProvider,
    resolve_sandbox_credentials,
    resolve_sync_sandbox_credentials,
)
from vercel._internal.unstable.sandbox.errors import SandboxAPIError
from vercel._internal.unstable.sandbox.models import Sandbox
from vercel._internal.unstable.sandbox.options import SandboxOptions
from vercel._internal.unstable.sandbox.params import SandboxCreateParams
from vercel._internal.unstable.sandbox.v2 import (
    build_v2_create_sandbox_body,
    parse_v2_sandbox_response,
)

try:
    _VERSION = _pkg_version("vercel")
except Exception:
    _VERSION = "development"

_PLATFORM = platform.uname()
_USER_AGENT = (
    f"vercel/unstable-sandbox/{_VERSION} "
    f"(Python/{sys.version}; {_PLATFORM.system}/{_PLATFORM.machine})"
)

DEFAULT_SANDBOX_API_URL = "https://api.vercel.com"

SandboxCredentialsResolver = Callable[[], Awaitable[SandboxCredentials]]


@dataclass(frozen=True, slots=True)
class _CredentialSnapshot:
    token: str
    project_id: str
    team_id: str


class SandboxApiClient:
    """Async-shaped Sandbox HTTP API client shared by sync and async callers."""

    def __init__(
        self,
        *,
        transport: BaseTransport,
        credentials_resolver: SandboxCredentialsResolver,
        sleep_fn: SleepFn,
        api_url: str | None = None,
        request_timeout: timedelta | None = None,
        retry_attempts: int | None = None,
    ) -> None:
        self._transport = transport
        self._credentials_resolver = credentials_resolver
        self._sleep_fn = sleep_fn
        self._api_url = api_url or DEFAULT_SANDBOX_API_URL
        self._request_timeout = request_timeout
        self._retry_attempts = retry_attempts

    async def create(self, params: SandboxCreateParams) -> Sandbox:
        credentials = await self._resolve_credentials()
        data = await self._request_json(
            "POST",
            "/v2/sandboxes",
            token=credentials.token,
            body=JSONBody(build_v2_create_sandbox_body(params, project_id=credentials.project_id)),
        )
        return parse_v2_sandbox_response(data)

    async def get_sandbox(self, name: str) -> Sandbox:
        credentials = await self._resolve_credentials()
        data = await self._request_json(
            "GET",
            f"/v2/sandboxes/{name}",
            token=credentials.token,
            query={"teamId": credentials.team_id},
        )
        return parse_v2_sandbox_response(data)

    async def _resolve_credentials(self) -> _CredentialSnapshot:
        credentials = await self._credentials_resolver()
        return _CredentialSnapshot(
            token=credentials.token,
            project_id=credentials.project_id,
            team_id=credentials.team_id,
        )

    def _resolve_path(self, path: str) -> str:
        if path.startswith("/"):
            return f"{self._api_url.rstrip('/')}{path}"
        return path

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        token: str,
        query: dict[str, Any] | None = None,
        body: JSONBody | None = None,
    ) -> object:
        params = (
            {key: value for key, value in query.items() if value is not None} if query else None
        )
        headers = {"user-agent": _USER_AGENT}
        if body is not None:
            headers["content-type"] = "application/json"
        response = await self._send_with_retry(
            method,
            self._resolve_path(path),
            token=token,
            headers=headers,
            params=params,
            body=body,
        )
        if 200 <= response.status_code < 300:
            data: object = response.json()
            return data
        raise _build_api_error(response)

    async def _send_with_retry(
        self,
        method: str,
        path: str,
        *,
        token: str,
        headers: dict[str, str],
        params: dict[str, Any] | None,
        body: JSONBody | None = None,
    ) -> httpx.Response:
        retry = _retry_policy(self._retry_attempts)
        attempts = retry.retries if retry is not None else 0

        for attempt in range(attempts + 1):
            try:
                response = await self._transport.send(
                    method,
                    path,
                    token=token,
                    headers=headers,
                    params=params,
                    body=body,
                    timeout=self._request_timeout,
                )
            except httpx.TransportError:
                if retry is not None and retry.retry_on_network_error and attempt < attempts:
                    await self._backoff(retry, attempt)
                    continue
                raise

            if (
                retry is not None
                and retry.retry_on_response is not None
                and retry.retry_on_response(response)
                and attempt < attempts
            ):
                await self._backoff(retry, attempt)
                continue

            return response

        raise RuntimeError("unreachable retry state")

    async def _backoff(self, retry: RetryPolicy, attempt: int) -> None:
        delay = min(retry.backoff_base * (2**attempt), retry.backoff_max)
        result = self._sleep_fn(delay)
        if inspect.isawaitable(result):
            await result


def create_sandbox_credentials_resolver(
    options: SandboxOptions | None,
) -> SandboxCredentialsResolver:
    sandbox_options = options or SandboxOptions()

    async def resolve() -> SandboxCredentials:
        return await resolve_sandbox_credentials(
            credential_provider=cast(
                SandboxCredentialProvider | None,
                sandbox_options.credential_provider,
            ),
            project_id=sandbox_options.project_id,
            team_id=sandbox_options.team_id,
        )

    return resolve


def create_sync_sandbox_credentials_resolver(
    options: SandboxOptions | None,
) -> SandboxCredentialsResolver:
    sandbox_options = options or SandboxOptions()

    async def resolve() -> SandboxCredentials:
        return resolve_sync_sandbox_credentials(
            credential_provider=cast(
                SyncSandboxCredentialProvider | None,
                sandbox_options.credential_provider,
            ),
            project_id=sandbox_options.project_id,
            team_id=sandbox_options.team_id,
        )

    return resolve


def _retry_policy(retry_attempts: int | None) -> RetryPolicy | None:
    if retry_attempts is None:
        return None
    return RetryPolicy(retries=retry_attempts)


def _build_api_error(response: httpx.Response) -> SandboxAPIError:
    message, data = extract_structured_error(response)
    return SandboxAPIError(
        message,
        response=response,
        status_code=response.status_code,
        data=data,
        retry_after=response.headers.get("retry-after"),
    )


__all__ = [
    "DEFAULT_SANDBOX_API_URL",
    "SandboxApiClient",
    "SandboxCredentialsResolver",
    "create_sandbox_credentials_resolver",
    "create_sync_sandbox_credentials_resolver",
]
