"""Internal Schedules API client.

Owns request/response wire models, camelCase aliasing, the user agent, and the
mapping of non-2xx responses onto the `SchedulesApiError` taxonomy.
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
    CronExpression,
    OneOffExpression,
    QueueTarget,
    Schedule,
    ScheduleExpression,
    ScheduleSource,
    ScheduleState,
    StateOverride,
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

# Jitter is an undocumented `number` on the wire. Seconds is the assumption;
# this is the one place to change if that turns out to be wrong.
_JITTER_UNIT = timedelta(seconds=1)

ResponseModelT = TypeVar("ResponseModelT", bound=BaseModel)


class _ApiModel(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)


class _CronExpressionModel(_ApiModel):
    type: Literal["cron"]
    cron: str


class _SingleExpressionModel(_ApiModel):
    type: Literal["single"]
    at: datetime


class _TargetModel(_ApiModel):
    type: str
    topic: str


class _StateOverrideModel(_ApiModel):
    state: ScheduleState
    until: int | float


class _ScheduleModel(_ApiModel):
    schedule_id: str = Field(alias="scheduleId")
    owner_id: str = Field(alias="ownerId")
    project_id: str = Field(alias="projectId")
    track_id: str = Field(alias="trackId")
    name: str
    namespace: str
    expression: _CronExpressionModel | _SingleExpressionModel = Field(discriminator="type")
    jitter: int | float | None = None
    target: _TargetModel
    state: ScheduleState
    state_override: _StateOverrideModel | None = Field(default=None, alias="stateOverride")
    source: ScheduleSource
    created_at: int | float = Field(alias="createdAt")
    updated_at: int | float = Field(alias="updatedAt")

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
            jitter=None if self.jitter is None else self.jitter * _JITTER_UNIT,
            target=QueueTarget(topic=self.target.topic),
            state=self.state,
            state_override=(
                None
                if self.state_override is None
                else StateOverride(
                    state=self.state_override.state,
                    until=from_epoch_ms(self.state_override.until),
                )
            ),
            source=self.source,
            created_at=from_epoch_ms(self.created_at),
            updated_at=from_epoch_ms(self.updated_at),
        )


class _CreateScheduleResponseModel(_ApiModel):
    schedule_id: str = Field(alias="scheduleId")


class _ListSchedulesResponseModel(_ApiModel):
    data: list[_ScheduleModel]
    cursor: str | None = None


@dataclass(frozen=True, slots=True)
class SchedulesPage:
    """One page of a schedule listing."""

    schedules: list[Schedule]
    next_cursor: str | None


def jitter_to_wire(jitter: timedelta) -> int:
    """Render a jitter duration in the unit the service expects."""
    return int(jitter / _JITTER_UNIT)


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

    async def create_schedule(self, body: Mapping[str, Any]) -> str:
        """POST /v1/schedules."""
        response = await self._request("POST", "/v1/schedules", body=JSONBody(dict(body)))
        return self._parse(response, _CreateScheduleResponseModel).schedule_id

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
        page = self._parse(response, _ListSchedulesResponseModel)
        return SchedulesPage(
            schedules=[item.to_schedule() for item in page.data],
            next_cursor=page.cursor,
        )

    async def get_schedule(self, schedule_id: str) -> Schedule:
        """GET /v1/schedules/:id."""
        response = await self._request("GET", f"/v1/schedules/{_encode(schedule_id)}")
        return self._parse(response, _ScheduleModel).to_schedule()

    async def delete_schedule(self, schedule_id: str) -> None:
        """DELETE /v1/schedules/:id."""
        # The service answers with `{scheduleId}` or 204; neither carries news.
        await self._request("DELETE", f"/v1/schedules/{_encode(schedule_id)}")

    async def enable_schedule(self, schedule_id: str) -> Schedule:
        """POST /v1/schedules/:id/enable."""
        response = await self._request("POST", f"/v1/schedules/{_encode(schedule_id)}/enable")
        return self._parse(response, _ScheduleModel).to_schedule()

    async def disable_schedule(self, schedule_id: str) -> Schedule:
        """POST /v1/schedules/:id/disable."""
        response = await self._request("POST", f"/v1/schedules/{_encode(schedule_id)}/disable")
        return self._parse(response, _ScheduleModel).to_schedule()

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

    def _parse(self, response: Response, model: type[ResponseModelT]) -> ResponseModelT:
        try:
            payload = response.json()
        except Exception as exc:
            raise SchedulesResponseError(
                "Schedules API returned a non-JSON success response"
            ) from exc
        try:
            return model.model_validate(payload)
        except ValidationError as exc:
            raise SchedulesResponseError(
                "Schedules API returned a malformed success response", data=payload
            ) from exc


def _encode(schedule_id: str) -> str:
    return quote(schedule_id, safe="")


__all__ = ["USER_AGENT", "SchedulesApiClient", "SchedulesPage", "jitter_to_wire"]
