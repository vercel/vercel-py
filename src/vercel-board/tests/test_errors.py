from __future__ import annotations

from datetime import timedelta

import httpx2 as httpx
import pytest

from vercel._internal.core.errors import VercelError
from vercel.board import (
    BoardApiError,
    BoardConflictError,
    BoardCredentialsError,
    BoardError,
    BoardLabelConflictError,
    BoardNewerMessageError,
    BoardNotFoundError,
    BoardPermissionError,
    BoardRateLimitError,
    BoardResponseError,
    BoardRevisionConflictError,
    BoardValidationError,
    ErrorOrigin,
)


def test_error_inheritance_hierarchy() -> None:
    assert issubclass(BoardError, VercelError)
    assert issubclass(BoardValidationError, BoardError)
    assert issubclass(BoardValidationError, ValueError)
    assert issubclass(BoardCredentialsError, BoardError)
    assert issubclass(BoardApiError, BoardError)
    assert issubclass(BoardNotFoundError, BoardApiError)
    assert issubclass(BoardNotFoundError, LookupError)
    assert issubclass(BoardPermissionError, BoardApiError)
    assert issubclass(BoardPermissionError, PermissionError)
    assert issubclass(BoardConflictError, BoardApiError)
    assert issubclass(BoardRevisionConflictError, BoardConflictError)
    assert issubclass(BoardLabelConflictError, BoardConflictError)
    assert issubclass(BoardNewerMessageError, BoardConflictError)
    assert issubclass(BoardRateLimitError, BoardApiError)
    assert issubclass(BoardResponseError, BoardError)


def test_board_error_attributes_and_defaults() -> None:
    err = BoardError("something failed")
    assert str(err) == "something failed"
    assert err.code is None
    assert err.status_code is None
    assert err.hint is None
    assert err.origin is ErrorOrigin.HANDLE
    assert err.retry_after is None
    assert err.request_id is None
    assert err.response is None


def test_board_api_error_extracts_response_metadata() -> None:
    response = httpx.Response(
        429,
        headers={"x-vercel-id": "iad1::req-123"},
        request=httpx.Request("POST", "https://api.vercel.com/v1/messageboard"),
    )
    err = BoardRateLimitError(
        "slow down",
        code="rate_limited",
        hint="wait a moment",
        retry_after=timedelta(seconds=5),
        response=response,
    )
    assert err.code == "rate_limited"
    assert err.status_code == 429
    assert err.hint == "wait a moment"
    assert err.origin is ErrorOrigin.BOARD
    assert err.retry_after == timedelta(seconds=5)
    assert err.request_id == "iad1::req-123"
    assert err.response is response
    rendered = str(err)
    assert "slow down" in rendered
    assert "status=429" in rendered
    assert "code=rate_limited" in rendered
    assert "request_id=iad1::req-123" in rendered


@pytest.mark.parametrize(
    ("exc_type", "builtin_type"),
    [
        (BoardValidationError, ValueError),
        (BoardNotFoundError, LookupError),
        (BoardPermissionError, PermissionError),
    ],
)
def test_builtin_error_subclasses_construct_with_keyword_arguments(
    exc_type: type[BoardError],
    builtin_type: type[Exception],
) -> None:
    err = exc_type(
        "rejected",
        code="denied",
        status_code=403,
        hint="check access",
        origin=ErrorOrigin.BOARD,
        retry_after=timedelta(seconds=1),
        request_id="req-1",
    )
    assert isinstance(err, exc_type)
    assert isinstance(err, builtin_type)
    assert isinstance(err, BoardError)
    assert isinstance(err, VercelError)
    assert err.code == "denied"
    assert err.status_code == 403
    assert err.hint == "check access"
    assert err.origin is ErrorOrigin.BOARD
    assert err.retry_after == timedelta(seconds=1)
    assert err.request_id == "req-1"
    assert "rejected" in str(err)
