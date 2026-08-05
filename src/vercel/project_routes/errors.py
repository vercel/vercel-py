from __future__ import annotations

from vercel.errors import VercelError


class ProjectRoutesError(VercelError):
    """A project routing-rules API request failed.

    ``code`` carries the machine-readable API error code when the response
    included one (for example ``routes_limit_exceeded``).
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        code: str | None = None,
        response_body: object | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.response_body = response_body


__all__ = ["ProjectRoutesError"]
