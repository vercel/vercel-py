"""Internal Drives API client.

Owns the request and response wire models, camelCase aliasing, the user agent,
retries, and the mapping of non-2xx responses onto `DriveApiError`. Callers pass
Python values such as a `DriveQuery`; only the models here know their wire form.

Drives are addressed by name or ID under `/v1/drives`, scoped by the team and
project of the request's credentials.
"""

import os
import platform
import sys
from collections.abc import Mapping
from datetime import timedelta
from importlib.metadata import PackageNotFoundError, version as _pkg_version
from typing import Any, Literal, TypeVar, cast

import anyio
from httpx2 import Response
from httpx2._types import QueryParamTypes
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from vercel._internal.core.detect_agent import detect_agent_name
from vercel._internal.core.http import (
    NO_READ_TIMEOUT,
    BaseTransport,
    JSONBody,
    ReadResponsePolicy,
    RequestBody,
    RequestTimeout,
    extract_structured_error,
)
from vercel._internal.core.region import ResolvedRegion
from vercel._internal.core.url import format_url_path
from vercel.drives._internal.errors import DriveApiError, DriveResponseError
from vercel.drives._internal.models import (
    DriveQuery,
    DriveQueryByCreatedAt,
    DriveQueryByName,
    DriveQueryByUpdatedAt,
    DrivesPageState,
    DriveState,
)
from vercel.drives._internal.options import DriveCredentials, DriveCredentialsFactory
from vercel.drives._internal.retry import AsyncSleep, with_request_retries

try:
    VERSION = _pkg_version("vercel-drives")
except PackageNotFoundError:  # pragma: no cover - bundled distribution
    try:
        VERSION = _pkg_version("vercel-drives-bundle")
    except PackageNotFoundError:
        VERSION = "development"

PLATFORM = platform.uname()
_TELEMETRY_DISABLED_ENV = (
    "VERCEL_TELEMETRY_DISABLED",
    "VERCEL_SANDBOX_TELEMETRY_DISABLED",
    "VERCEL_DRIVES_TELEMETRY_DISABLED",
)

ResponseModelT = TypeVar("ResponseModelT", bound=BaseModel)


def _user_agent() -> str:
    agent_name = None
    if not any(os.environ.get(name) for name in _TELEMETRY_DISABLED_ENV):
        agent_name = detect_agent_name()
    agent_info = f" agent/{agent_name}" if agent_name else ""
    return (
        f"vercel-drives/{VERSION}{agent_info} "
        f"(Python/{sys.version}; {PLATFORM.system}/{PLATFORM.machine})"
    )


class _ApiModel(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)


class _ApiRequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    def to_api_dict(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True)


class _ProjectScopeRequest(_ApiRequestModel):
    project_id: str = Field(serialization_alias="projectId")


class _GetOrCreateDriveRequest(_ProjectScopeRequest):
    max_size_bytes: int | None = Field(default=None, serialization_alias="maxSizeBytes")
    region: str | None = None


class _ForkDriveRequest(_ApiRequestModel):
    name: str


class _QueryDrivesRequest(_ProjectScopeRequest):
    limit: int | None = None
    cursor: str | None = None
    sort_by: Literal["createdAt", "updatedAt", "name"] | None = Field(
        default=None, serialization_alias="sortBy"
    )
    sort_order: Literal["asc", "desc"] | None = Field(default=None, serialization_alias="sortOrder")
    name_prefix: str | None = Field(default=None, serialization_alias="namePrefix")

    @classmethod
    def from_query(
        cls,
        query: DriveQuery | None,
        *,
        project_id: str,
        limit: int | None,
        cursor: str | None,
    ) -> "_QueryDrivesRequest":
        if query is None:
            return cls(project_id=project_id, limit=limit, cursor=cursor)
        sort_by: Literal["createdAt", "updatedAt", "name"]
        name_prefix = None
        if isinstance(query, DriveQueryByCreatedAt):
            sort_by = "createdAt"
        elif isinstance(query, DriveQueryByUpdatedAt):
            sort_by = "updatedAt"
        elif isinstance(query, DriveQueryByName):
            sort_by = "name"
            name_prefix = query.name_prefix
        else:
            raise TypeError(f"Unsupported drive query type: {type(query)!r}")
        return cls(
            project_id=project_id,
            limit=limit,
            cursor=cursor,
            sort_by=sort_by,
            sort_order=query.sort_order,
            name_prefix=name_prefix,
        )


class _DrivePayload(_ApiModel):
    id: str
    name: str
    project_id: str = Field(alias="projectId")
    region: str
    max_size_bytes: int = Field(alias="maxSizeBytes")
    parent_drive_id: str | None = Field(default=None, alias="parentDriveId")
    root_drive_id: str | None = Field(default=None, alias="rootDriveId")
    current_session_id: str | None = Field(default=None, alias="currentSessionId")
    current_sandbox_name: str | None = Field(default=None, alias="currentSandboxName")
    created_at: int = Field(alias="createdAt")
    updated_at: int = Field(alias="updatedAt")

    def to_drive(self) -> DriveState:
        return DriveState(
            id=self.id,
            name=self.name,
            project_id=self.project_id,
            region=self.region,
            max_size_bytes=self.max_size_bytes,
            parent_drive_id=self.parent_drive_id,
            root_drive_id=self.root_drive_id,
            current_session_id=self.current_session_id,
            current_sandbox_name=self.current_sandbox_name,
            created_at=self.created_at,
            updated_at=self.updated_at,
        )


class _DriveResponse(_ApiModel):
    drive: _DrivePayload


class _Pagination(_ApiModel):
    next: str | None = None


class _DrivesResponse(_ApiModel):
    drives: list[_DrivePayload]
    pagination: _Pagination | None = None


class DriveApiClient:
    """Wire-level access to the Vercel Drives API."""

    def __init__(
        self,
        *,
        base_url: str,
        credentials_factory: DriveCredentialsFactory,
        transport: BaseTransport,
        request_timeout: timedelta | None = None,
        sleep: AsyncSleep = anyio.sleep,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._credentials_factory = credentials_factory
        self._transport = transport
        self._request_timeout: RequestTimeout = (
            NO_READ_TIMEOUT if request_timeout is None else request_timeout
        )
        self._sleep = sleep

    async def get_drive(self, *, name_or_id: str) -> DriveState:
        """GET /v1/drives/:nameOrId."""
        credentials = await self._credentials_factory()
        response = await self._request(
            "GET",
            format_url_path("/v1/drives/{name_or_id}", name_or_id=name_or_id),
            credentials=credentials,
            params=_project_scope(credentials),
        )
        return self._parse_drive(response)

    async def get_or_create_drive(
        self,
        *,
        name: str,
        max_size_bytes: int | None = None,
        region: ResolvedRegion | None = None,
    ) -> tuple[DriveState, bool]:
        """POST /v1/drives/:name. The API answers 201 only when it created the Drive."""
        credentials = await self._credentials_factory()
        request = _GetOrCreateDriveRequest(
            project_id=credentials.project_id,
            max_size_bytes=max_size_bytes,
            region=region,
        )
        response = await self._request(
            "POST",
            format_url_path("/v1/drives/{name}", name=name),
            credentials=credentials,
            body=JSONBody(request.to_api_dict()),
        )
        return self._parse_drive(response), response.status_code == 201

    async def fork_drive(self, *, name: str, fork_name: str) -> DriveState:
        """POST /v1/drives/:name/fork."""
        credentials = await self._credentials_factory()
        response = await self._request(
            "POST",
            format_url_path("/v1/drives/{name}/fork", name=name),
            credentials=credentials,
            params=_project_scope(credentials),
            body=JSONBody(_ForkDriveRequest(name=fork_name).to_api_dict()),
        )
        return self._parse_drive(response)

    async def delete_drive(self, *, name: str) -> DriveState:
        """DELETE /v1/drives/:nameOrId."""
        credentials = await self._credentials_factory()
        response = await self._request(
            "DELETE",
            format_url_path("/v1/drives/{name}", name=name),
            credentials=credentials,
            params=_project_scope(credentials),
        )
        return self._parse_drive(response)

    async def query_drives(
        self,
        *,
        query: DriveQuery | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> DrivesPageState:
        """GET /v1/drives."""
        credentials = await self._credentials_factory()
        request = _QueryDrivesRequest.from_query(
            query, project_id=credentials.project_id, limit=limit, cursor=cursor
        )
        response = await self._request(
            "GET", "/v1/drives", credentials=credentials, params=request.to_api_dict()
        )
        page = self._parse(response, _DrivesResponse)
        return DrivesPageState(
            drives=tuple(drive.to_drive() for drive in page.drives),
            next_cursor=None if page.pagination is None else page.pagination.next,
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        credentials: DriveCredentials,
        params: Mapping[str, Any] | None = None,
        body: RequestBody = None,
    ) -> Response:
        query = cast(QueryParamTypes, {"teamId": credentials.team_id, **dict(params or {})})
        headers = {"user-agent": _user_agent()}
        if body is not None:
            headers["content-type"] = "application/json"

        async def send() -> Response:
            response = await self._transport.send(
                method,
                f"{self._base_url}{path}",
                token=credentials.token,
                params=query,
                body=body,
                headers=headers,
                timeout=self._request_timeout,
                read_response=ReadResponsePolicy.ALWAYS,
            )
            if not response.is_success:
                message, data = extract_structured_error(response)
                raise DriveApiError(response, message, data=data)
            return response

        return await with_request_retries(send, sleep=self._sleep)

    def _parse(self, response: Response, model: type[ResponseModelT]) -> ResponseModelT:
        try:
            data = response.json()
        except ValueError as exc:
            raise DriveResponseError(
                "Drives API response body could not be decoded as JSON"
            ) from exc
        try:
            return model.model_validate(data)
        except ValidationError as exc:
            raise DriveResponseError(
                "Drives API response did not match the expected shape", data=data
            ) from exc

    def _parse_drive(self, response: Response) -> DriveState:
        return self._parse(response, _DriveResponse).drive.to_drive()


def _project_scope(credentials: DriveCredentials) -> dict[str, Any]:
    return _ProjectScopeRequest(project_id=credentials.project_id).to_api_dict()


__all__ = ["DriveApiClient"]
