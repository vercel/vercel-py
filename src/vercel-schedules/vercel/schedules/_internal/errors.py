"""Schedules errors for the SDK surface."""

import httpx2 as httpx

from vercel._internal.core.errors import VercelError


class SchedulesError(VercelError):
    """Base error for Vercel Schedules operations."""


class SchedulesApiError(SchedulesError):
    """Raised when the Schedules API returns an error response.

    Attributes:
        response: The raw httpx2 response.
        status_code: HTTP status code.
        status_text: HTTP reason phrase.
        code: Machine-readable error code from the API, such as `"not_found"`
            or `"schedule_already_exists"`. The API may add codes, so handle
            unrecognized values.
        retry_after: Seconds to wait before retrying, from the `Retry-After`
            header, when the API sends one.
        request_id: The `x-vercel-id` response header, when present.
        data: The parsed error body, when it was JSON.
    """

    def __init__(
        self,
        response: httpx.Response,
        message: str,
        *,
        code: str | None = None,
        retry_after: int | None = None,
        data: object | None = None,
    ) -> None:
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code
        self.status_text = response.reason_phrase
        self.code = code
        self.retry_after = retry_after
        self.request_id = response.headers.get("x-vercel-id")
        self.data = data

    def __str__(self) -> str:
        details = [f"status={self.status_code}"]
        if self.code:
            details.append(f"code={self.code}")
        if self.request_id:
            details.append(f"request_id={self.request_id}")
        message = super().__str__()
        rendered = f"({', '.join(details)})"
        return f"{message} {rendered}" if message else rendered


class ScheduleNotFoundError(SchedulesApiError, LookupError):
    """Raised when the schedule named does not exist.

    Also a `LookupError`, so it reads the same as a missing key to callers that
    treat "not there" as an ordinary outcome.
    """


class SchedulesResponseError(SchedulesError):
    """Raised when a successful Schedules API response is malformed."""

    def __init__(self, message: str, *, data: object | None = None) -> None:
        super().__init__(message)
        self.data = data


class SchedulesCredentialsError(SchedulesError):
    """Raised when no token for the Schedules API can be resolved."""


class SchedulesValidationError(SchedulesError, ValueError):
    """Raised when caller-supplied arguments are rejected before any request.

    Also a `ValueError`, so both `except SchedulesError` and the `except
    ValueError` a caller would reach for on an invalid argument catch it.
    """


class ScheduleEventParseError(SchedulesError):
    """Raised when the environment does not hold a valid schedule firing."""


class NotAScheduleInvocationError(ScheduleEventParseError):
    """Raised when the process was not started by a schedule firing.

    For example, when a schedule command is run by hand.
    """


__all__ = [
    "NotAScheduleInvocationError",
    "ScheduleEventParseError",
    "ScheduleNotFoundError",
    "SchedulesApiError",
    "SchedulesCredentialsError",
    "SchedulesError",
    "SchedulesResponseError",
    "SchedulesValidationError",
]
