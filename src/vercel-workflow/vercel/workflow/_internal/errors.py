from __future__ import annotations

from datetime import datetime

from .duration import parse_duration_to_date


class FatalError(Exception):
    """A step failure that will not be retried."""


class RetryableError(Exception):
    """A step failure that names when the next attempt should run.

    Raise it from a step body to steer the wait before the retry::

        raise RetryableError("rate limited", retry_after="10s")

    ``retry_after`` takes what ``sleep()`` takes -- a duration string, a number
    of milliseconds, or an absolute timezone-aware ``datetime`` -- and defaults
    to one second from now, matching ``RetryableError`` in ``@workflow/errors``.

    It only moves the next attempt; it does not buy extra ones. A step that has
    used up ``max_retries`` fails whether or not it raised this.
    """

    retry_after: datetime

    def __init__(
        self,
        message: str,
        *,
        retry_after: int | float | str | datetime | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after = parse_duration_to_date(1_000 if retry_after is None else retry_after)
