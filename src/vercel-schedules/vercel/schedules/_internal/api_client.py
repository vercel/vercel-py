"""Internal Schedules API client.

Owns request/response wire models, camelCase aliasing, the user agent, and the
mapping of non-2xx responses onto the `SchedulesApiError` taxonomy.

Schedules are addressed by name, scoped by an optional namespace query
parameter, matching `@vercel/schedules`.
"""

import platform
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from importlib.metadata import PackageNotFoundError, version as _pkg_version
from typing import Any, Literal, TypeVar
from urllib.parse import quote

from httpx2 import Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from vercel._internal.core.http import (
    BaseTransport,
    JSONBody,
    ReadResponsePolicy,
    RequestBody,
    extract_structured_error,
)
from vercel._internal.core.time import from_epoch_ms
from vercel.schedules._internal.errors import (
    ScheduleNotFoundError,
    SchedulesApiError,
    SchedulesResponseError,
)
from vercel.schedules._internal.models import (
    JITTER_UNIT,
    CronExpression,
    FunctionTarget,
    OneOffExpression,
    QueueTarget,
    Schedule,
    ScheduleExpression,
    ScheduleSource,
    ScheduleState,
)
from vercel.schedules._internal.options import SchedulesCredentialsFactory

try:
    VERSION = _pkg_version("vercel-schedules")
except PackageNotFoundError:  # pragma: no cover - bundled distribution
    try:
        VERSION = _pkg_version("vercel-schedules-bundle")
    except PackageNotFoundError:
        VERSION = "development"

PLATFORM = platform.uname()
USER_AGENT = (
    f"vercel-schedules/{VERSION} (Python/{sys.version}; {PLATFORM.system}/{PLATFORM.machine})"
)

ResponseModelT = TypeVar("ResponseModelT", bound=BaseModel)


class _ApiModel(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)


class _CronExpressionModel(_ApiModel):
    type: Literal["cron"]
    cron: str


class _SingleExpressionModel(_ApiModel):
    type: Literal["single"]
    at: datetime


class _QueueTargetModel(_ApiModel):
    type: Literal["queue"]
    topic: str


class _FunctionTargetModel(_ApiModel):
    type: Literal["function"]
    function: str


class _ScheduleModel(_ApiModel):
    schedule_id: str = Field(alias="scheduleId")
    owner_id: str = Field(alias="ownerId")
    project_id: str = Field(alias="projectId")
    track_id: str = Field(alias="trackId")
    name: str
    namespace: str
    expression: _CronExpressionModel | _SingleExpressionModel = Field(discriminator="type")
    timezone: str
    jitter: int | None = None
    target: _QueueTargetModel | _FunctionTargetModel = Field(discriminator="type")
    state: ScheduleState
    source: ScheduleSource
    created_at: int = Field(alias="createdAt")
    updated_at: int = Field(alias="updatedAt")

    def to_schedule(self) -> Schedule:
        expression: ScheduleExpression
        if isinstance(self.expression, _CronExpressionModel):
            expression = CronExpression(cron=self.expression.cron)
        else:
            expression = OneOffExpression(at=self.expression.at)
        return Schedule(
            schedule_id=self.schedule_id,
            owner_id=self.owner_id,
            project_id=self.project_id,
            track_id=self.track_id,
            name=self.name,
            namespace=self.namespace,
            expression=expression,
            timezone=self.timezone,
            jitter=None if self.jitter is None else self.jitter * JITTER_UNIT,
            target=(
                QueueTarget(topic=self.target.topic)
                if isinstance(self.target, _QueueTargetModel)
                else FunctionTarget(function=self.target.function)
            ),
            state=self.state,
            source=self.source,
            created_at=from_epoch_ms(self.created_at),
            updated_at=from_epoch_ms(self.updated_at),
        )


class _ListSchedulesResponseModel(_ApiModel):
    data: list[_ScheduleModel]
    cursor: str | None = None


@dataclass(frozen=True, slots=True)
class SchedulesPage:
    """One page of a schedule listing."""

    schedules: list[Schedule]
    next_cursor: str | None


def _parse(response: Response, model: type[ResponseModelT]) -> ResponseModelT:
    try:
        payload = response.json()
    except Exception as exc:
        raise SchedulesResponseError("Schedules API returned a non-JSON success response") from exc
    try:
        return model.model_validate(payload)
    except ValidationError as exc:
        raise SchedulesResponseError(
            "Schedules API returned a malformed success response", data=payload
        ) from exc


def _raise_api_error(response: Response) -> None:
    fallback, data = extract_structured_error(response)
    if isinstance(data, Mapping):
        fallback = response.reason_phrase or fallback
    else:
        fallback = fallback.removeprefix(f"HTTP {response.status_code}: ")
    message = _error_message(data) or response.text.strip() or fallback
    error_class = ScheduleNotFoundError if response.status_code == 404 else SchedulesApiError
    raise error_class(response, message, data=data)


def _error_message(data: object) -> str | None:
    if not isinstance(data, Mapping):
        return None
    for key in ("message", "error"):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
        if isinstance(value, Mapping) and isinstance(value.get("message"), str):
            return value["message"]
    return None


class SchedulesApiClient:
    """Wire-level access to the Vercel Schedules service."""

    def __init__(
        self,
        *,
        base_url: str,
        credentials_factory: SchedulesCredentialsFactory,
        transport: BaseTransport,
        timeout: timedelta,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._credentials_factory = credentials_factory
        self._transport = transport
        self._timeout = timeout

    async def create_schedule(self, body: Mapping[str, Any]) -> Schedule:
        """POST /v1/schedules."""
        response = await self._request("POST", "/v1/schedules", body=JSONBody(dict(body)))
        return _parse(response, _ScheduleModel).to_schedule()

    async def list_schedules(
        self,
        *,
        namespace: str | None,
        cursor: str | None,
        limit: int | None,
    ) -> SchedulesPage:
        """GET /v1/schedules."""
        params: dict[str, str] = {}
        if namespace is not None:
            params["namespace"] = namespace
        if cursor is not None:
            params["cursor"] = cursor
        if limit is not None:
            params["limit"] = str(limit)
        response = await self._request("GET", "/v1/schedules", params=params)
        page = _parse(response, _ListSchedulesResponseModel)
        return SchedulesPage(
            schedules=[item.to_schedule() for item in page.data],
            next_cursor=page.cursor,
        )

    async def get_schedule(self, name: str, *, namespace: str | None) -> Schedule:
        """GET /v1/schedules/:name."""
        response = await self._request(
            "GET", _schedule_path(name), params=_namespace_params(namespace)
        )
        return _parse(response, _ScheduleModel).to_schedule()

    async def update_schedule(
        self, name: str, *, namespace: str | None, body: Mapping[str, Any]
    ) -> Schedule:
        """PATCH /v1/schedules/:name."""
        response = await self._request(
            "PATCH",
            _schedule_path(name),
            params=_namespace_params(namespace),
            body=JSONBody(dict(body)),
        )
        return _parse(response, _ScheduleModel).to_schedule()

    async def delete_schedule(self, name: str, *, namespace: str | None) -> None:
        """DELETE /v1/schedules/:name."""
        # The service answers with `{scheduleId}` or 204; neither carries news.
        await self._request("DELETE", _schedule_path(name), params=_namespace_params(namespace))

    async def enable_schedule(self, name: str, *, namespace: str | None) -> Schedule:
        """POST /v1/schedules/:name/enable."""
        response = await self._request(
            "POST", _schedule_path(name, "/enable"), params=_namespace_params(namespace)
        )
        return _parse(response, _ScheduleModel).to_schedule()

    async def disable_schedule(self, name: str, *, namespace: str | None) -> Schedule:
        """POST /v1/schedules/:name/disable."""
        response = await self._request(
            "POST", _schedule_path(name, "/disable"), params=_namespace_params(namespace)
        )
        return _parse(response, _ScheduleModel).to_schedule()

    async def invoke_schedule(self, name: str, *, namespace: str | None) -> None:
        """POST /v1/schedules/:name/invoke."""
        await self._request(
            "POST", _schedule_path(name, "/invoke"), params=_namespace_params(namespace)
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        body: RequestBody = None,
    ) -> Response:
        token = await self._credentials_factory()
        response = await self._transport.send(
            method,
            f"{self._base_url}{path}",
            token=token,
            params=params or None,
            body=body,
            headers={"user-agent": USER_AGENT},
            timeout=self._timeout,
            read_response=ReadResponsePolicy.ALWAYS,
        )
        if not response.is_success:
            _raise_api_error(response)
        return response


def _schedule_path(name: str, suffix: str = "") -> str:
    return f"/v1/schedules/{quote(name, safe='')}{suffix}"


def _namespace_params(namespace: str | None) -> dict[str, str]:
    return {} if namespace is None else {"namespace": namespace}


__all__ = [
    "USER_AGENT",
    "SchedulesApiClient",
    "SchedulesPage",
]
