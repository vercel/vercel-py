from __future__ import annotations

from datetime import datetime
from typing import Any

from .duration import DurationParam, parse_duration_to_date

DEFAULT_RETRY_AFTER_SECONDS = 1


class FatalError(Exception):
    """A step failure that will not be retried."""

    # TypeScript's FatalError.is() also accepts this marker.
    fatal = True


class HookConflictError(Exception):
    """A hook token already owned by another active workflow run."""

    token: str
    conflicting_run_id: str | None

    def __init__(self, token: str, conflicting_run_id: str | None = None) -> None:
        owner = f' (run "{conflicting_run_id}")' if conflicting_run_id else ""
        super().__init__(f'Hook token "{token}" is already in use by another workflow{owner}')
        self.token = token
        self.conflicting_run_id = conflicting_run_id


class RemoteError(Exception):
    """An error whose class is unavailable in Python."""

    def __init__(
        self,
        message: str,
        *,
        name: str,
    ) -> None:
        super().__init__(message)
        self.name = name
        self.message = message
        # Keep enough data to preserve TypeScript-only error tags and fields
        # when the value crosses Python and is serialized again.
        self._wire_tag: str | None = None
        self._wire_payload: dict[str, Any] | None = None

    def __str__(self) -> str:
        return f"{self.name}: {self.message}" if self.message else self.name


class RetryableError(Exception):
    """A step failure that sets how long to wait before the next attempt.

    Raise it from a step body::

        raise RetryableError("rate limited", retry_after="10s")

    ``retry_after`` accepts the same values ``sleep()`` accepts: a duration
    string, a number of seconds, a ``timedelta``, or an absolute
    timezone-aware ``datetime``. It defaults to one second from now, as ``RetryableError``
    in ``@workflow/errors`` does.

    It changes only *when* the next attempt runs, not how many attempts there
    are: a step that has used up its ``max_retries`` fails whether or not it
    raised this.
    """

    retry_at: datetime
    """When the next attempt may start, resolved at construction time."""

    def __init__(
        self,
        message: str,
        *,
        retry_after: DurationParam | None = None,
    ) -> None:
        super().__init__(message)
        if retry_after is None:
            retry_after = DEFAULT_RETRY_AFTER_SECONDS
        self.retry_at = parse_duration_to_date(retry_after)


class WorkflowRunFailedError(Exception):
    def __init__(self, run_id: str, error: Any, *, error_code: str | None = None) -> None:
        if error is None:
            message = "unknown error"
        else:
            try:
                message = str(error)
            except Exception:
                message = type(error).__name__
        super().__init__(f'Workflow run "{run_id}" failed: {message}')
        self.run_id = run_id
        self.error = error
        self.error_code = error_code
        if isinstance(error, Exception):
            self.__cause__ = error
