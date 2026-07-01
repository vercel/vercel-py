from __future__ import annotations

import json
from dataclasses import dataclass

from .errors import (
    BadRequestError,
    ConsumerDiscoveryError,
    ConsumerRegistryNotConfiguredError,
    DuplicateIdempotencyKeyError,
    ForbiddenError,
    InternalServerError,
    MessageAlreadyProcessedError,
    MessageLeaseExpiredError,
    MessageLockedError,
    MessageNotFoundError,
    MessageNotInFlightError,
    MessageUnavailableError,
    ReceiptHandleMismatchError,
    ServiceError,
    ThrottledError,
    UnauthorizedError,
)
from .http import AsyncHttpResponse, parse_retry_after, response_text
from .types import MessageID

_QUEUE_STATUS_ERRORS = {
    401: UnauthorizedError,
    403: ForbiddenError,
}


async def queue_response_error(
    response: AsyncHttpResponse,
    *,
    message_id: MessageID | None = None,
) -> Exception | None:
    if response.status_code == 400:
        return BadRequestError(await response_text(response) or "Invalid parameters")
    if error_type := _QUEUE_STATUS_ERRORS.get(response.status_code):
        return error_type()
    if response.status_code == 404 and message_id:
        return MessageNotFoundError(message_id)
    if response.status_code == 409 and message_id:
        return await _conflict_error(response, message_id)
    if response.status_code == 410 and message_id:
        return MessageAlreadyProcessedError(message_id)
    if response.status_code == 429:
        return ThrottledError(parse_retry_after(response))
    if response.status_code >= 500:
        return InternalServerError(
            await response_text(response) or f"Server error: {response.status_code}"
        )
    if response.status_code >= 300:
        return ServiceError(
            response.status_code,
            await response_text(response) or f"Unexpected queue response: {response.status_code}",
        )
    return None


async def send_response_error(response: AsyncHttpResponse) -> Exception | None:
    if response.status_code == 409:
        return DuplicateIdempotencyKeyError("Duplicate idempotency key detected")
    if response.status_code == 502:
        return ConsumerDiscoveryError(await response_text(response) or None)
    if response.status_code == 503:
        return ConsumerRegistryNotConfiguredError(await response_text(response) or None)
    return await queue_response_error(response)


async def _conflict_error(response: AsyncHttpResponse, message_id: MessageID) -> Exception:
    body = await _response_body(response)
    retry_after = parse_retry_after(response)
    if body.original_message_id:
        return MessageUnavailableError(
            message_id,
            f"originalMessageId={body.original_message_id}",
            original_message_id=body.original_message_id,
        )
    if body.error == "Message is not currently in-flight":
        return MessageNotInFlightError(message_id, retry_after)
    if body.error == "Message lease has expired":
        return MessageLeaseExpiredError(message_id, retry_after)
    if body.error == "Receipt handle does not match current lease holder":
        return ReceiptHandleMismatchError(message_id, retry_after)
    return MessageLockedError(message_id, retry_after, body.error)


@dataclass(frozen=True)
class _ResponseBody:
    error: str | None = None
    original_message_id: MessageID | None = None


async def _response_body(response: AsyncHttpResponse) -> _ResponseBody:
    text = await response_text(response)
    if not text:
        return _ResponseBody()
    try:
        data = json.loads(text)
    except ValueError:
        return _ResponseBody(error=text)
    if not isinstance(data, dict):
        return _ResponseBody(error=text)
    original_message_id = data.get("originalMessageId")
    error = data.get("error")
    return _ResponseBody(
        error=str(error) if error else None,
        original_message_id=MessageID(str(original_message_id)) if original_message_id else None,
    )
