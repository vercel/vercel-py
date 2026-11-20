"""Board errors for the SDK surface."""

from __future__ import annotations

from datetime import timedelta
from enum import StrEnum
from typing import Self

import httpx2 as httpx

from vercel._internal.core.errors import VercelError


class ErrorOrigin(StrEnum):
    """Where an error originated: the local SDK handle or the Board server."""

    HANDLE = "handle"
    BOARD = "board"


class BoardError(VercelError):
    """Base error for Vercel Board operations.

    Attributes:
        code: Machine-readable error code, such as ``"not_found"`` or
            ``"revision_conflict"``.
        status_code: HTTP status code when the error came from an HTTP response.
        hint: Actionable guidance describing how to fix the error, when present.
        origin: Whether the error came from the local SDK handle or the Board
            server.
        retry_after: How long to wait before retrying, when the server sent a
            ``Retry-After`` header.
        request_id: The ``x-vercel-id`` response header, when present.
        response: The raw ``httpx2.Response``, when available.
    """

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        status_code: int | None = None,
        hint: str | None = None,
        origin: ErrorOrigin = ErrorOrigin.HANDLE,
        retry_after: timedelta | None = None,
        request_id: str | None = None,
        response: httpx.Response | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = (
            status_code
            if status_code is not None
            else (response.status_code if response is not None else None)
        )
        self.hint = hint
        self.origin = origin
        self.retry_after = retry_after
        self.request_id = (
            request_id
            if request_id is not None
            else (response.headers.get("x-vercel-id") if response is not None else None)
        )
        self.response = response

    def __str__(self) -> str:
        details: list[str] = []
        if self.status_code is not None:
            details.append(f"status={self.status_code}")
        if self.code:
            details.append(f"code={self.code}")
        if self.request_id:
            details.append(f"request_id={self.request_id}")
        message = super().__str__()
        if not details:
            return message
        rendered = f"({', '.join(details)})"
        return f"{message} {rendered}" if message else rendered


class BoardValidationError(BoardError, ValueError):
    """Raised when caller-supplied arguments are rejected before any request.

    Also a ``ValueError``, so both ``except BoardError`` and ``except ValueError``
    catch it.
    """


class BoardCredentialsError(BoardError):
    """Raised when no credentials for the Board API can be resolved."""


class BoardApiError(BoardError):
    """Raised when the Board API returns an error response."""

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        status_code: int | None = None,
        hint: str | None = None,
        origin: ErrorOrigin = ErrorOrigin.BOARD,
        retry_after: timedelta | None = None,
        request_id: str | None = None,
        response: httpx.Response | None = None,
    ) -> None:
        super().__init__(
            message,
            code=code,
            status_code=status_code,
            hint=hint,
            origin=origin,
            retry_after=retry_after,
            request_id=request_id,
            response=response,
        )


class BoardNotFoundError(BoardApiError, LookupError):
    """Raised when a board, channel, message, or resource does not exist.

    Also a ``LookupError``.
    """


class BoardPermissionError(BoardApiError, PermissionError):
    """Raised when the caller lacks permission for a Board operation.

    Also a ``PermissionError``.
    """

    def __new__(cls, message: str, *args: object, **kwargs: object) -> Self:
        # OSError.__new__ rejects arbitrary keyword arguments; forward only the
        # message so BoardApiError.__init__ handles all metadata.
        return PermissionError.__new__(cls, message)


class BoardConflictError(BoardApiError):
    """Raised when a Board write conflicts with concurrent or existing state."""


class BoardRevisionConflictError(BoardConflictError):
    """Raised when an edit or delete fails a ``revision`` precondition check."""


class BoardLabelConflictError(BoardConflictError):
    """Raised when a label mutation fails an ``if_labels`` precondition check."""


class BoardNewerMessageError(BoardConflictError):
    """Raised when a reply fails an ``after`` precondition check."""


class BoardRateLimitError(BoardApiError):
    """Raised when the Board API rate-limits a request (HTTP 429)."""


class BoardResponseError(BoardError):
    """Raised when a Board API response is malformed or cannot be decoded."""


__all__ = [
    "BoardApiError",
    "BoardConflictError",
    "BoardCredentialsError",
    "BoardError",
    "BoardLabelConflictError",
    "BoardNewerMessageError",
    "BoardNotFoundError",
    "BoardPermissionError",
    "BoardRateLimitError",
    "BoardResponseError",
    "BoardRevisionConflictError",
    "BoardValidationError",
    "ErrorOrigin",
]
