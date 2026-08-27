from __future__ import annotations

from datetime import datetime

from .duration import DurationParam, parse_duration_to_date

DEFAULT_RETRY_AFTER_MS = 1_000


class FatalError(Exception):
    """A step failure that will not be retried."""


class RetryableError(Exception):
    """A step failure that sets how long to wait before the next attempt.

    Raise it from a step body::

        raise RetryableError("rate limited", retry_after="10s")

    ``retry_after`` accepts the same values ``sleep()`` accepts: a duration
    string, a number of milliseconds, a ``timedelta``, or an absolute
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
            retry_after = DEFAULT_RETRY_AFTER_MS
        self.retry_at = parse_duration_to_date(retry_after)
