from __future__ import annotations

from typing import Final

from .types import MessageID


class QueueError(Exception):
    """Base exception for Vercel Queue errors."""

    status_code: int | None = None


class BadRequestError(QueueError, ValueError):
    status_code = 400
    default_message: Final[str] = "Invalid parameters"

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.default_message)


class UnauthorizedError(QueueError, PermissionError):
    status_code = 401
    default_message: Final[str] = "Missing or invalid authentication token"

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.default_message)


class ForbiddenError(QueueError, PermissionError):
    status_code = 403
    default_message: Final[str] = "Queue environment does not match token environment"

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.default_message)


class ProtocolError(QueueError, RuntimeError):
    """The queue service returned malformed or incomplete protocol metadata."""


class PayloadValidationError(QueueError, ValueError):
    """Raised when queue payload validation rejects a message."""


class SubscriptionError(QueueError, TypeError):
    """Raised when a queue subscriber cannot be registered."""


class DuplicateSubscriptionError(SubscriptionError):
    """Raised when a queue subscriber overlaps an existing local registration."""


class ServiceError(QueueError, RuntimeError):
    default_message = "Unexpected queue response"

    def __init__(self, status_code: int, message: str | None = None) -> None:
        self.status_code = int(status_code)
        super().__init__(message or f"{self.default_message}: {self.status_code}")


class InternalServerError(ServiceError):
    status_code = 500
    default_message = "Unexpected server error"

    def __init__(self, message: str | None = None, status_code: int = 500) -> None:
        super().__init__(status_code, message or self.default_message)


class TokenResolutionError(QueueError, RuntimeError):
    status_code = 500


class DeploymentResolutionError(QueueError, RuntimeError):
    status_code = 500


class DuplicateIdempotencyKeyError(QueueError, RuntimeError):
    status_code = 409


class ConsumerDiscoveryError(QueueError, RuntimeError):
    status_code = 502

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "Failed to discover queue consumer")


class ConsumerRegistryNotConfiguredError(QueueError, RuntimeError):
    status_code = 503

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "Queue consumer registry is not configured")


class InvalidLimitError(QueueError, ValueError):
    status_code = 400

    def __init__(self, limit: int, min_value: int = 1, max_value: int = 10) -> None:
        self.limit = int(limit)
        self.min = int(min_value)
        self.max = int(max_value)
        super().__init__(
            f"Invalid limit: {self.limit}. Limit must be between {self.min} and {self.max}."
        )


class MessageNotFoundError(QueueError, LookupError):
    status_code = 404

    def __init__(self, message_id: MessageID) -> None:
        self.message_id = message_id
        super().__init__(f"Message {message_id} not found")


class MessageAlreadyProcessedError(QueueError, RuntimeError):
    status_code = 410

    def __init__(self, message_id: MessageID) -> None:
        self.message_id = message_id
        super().__init__(f"Message {message_id} has already been processed")


class MessageUnavailableError(QueueError, RuntimeError):
    status_code = 409

    def __init__(
        self,
        message_id: MessageID,
        reason: str | None = None,
        *,
        original_message_id: MessageID | None = None,
    ) -> None:
        self.message_id = message_id
        self.reason = reason
        self.original_message_id = original_message_id
        suffix = f": {reason}" if reason else ""
        super().__init__(f"Message {message_id} not available for processing{suffix}")


class MessageCorruptedError(QueueError, RuntimeError):
    status_code = 500

    def __init__(self, message_id: MessageID, reason: str) -> None:
        self.message_id = message_id
        self.reason = reason
        super().__init__(f"Message {message_id} is corrupted: {reason}")


class MessageLockedError(QueueError, RuntimeError):
    status_code = 409

    def __init__(
        self,
        message_id: MessageID,
        retry_after: int | None = None,
        reason: str | None = None,
    ) -> None:
        self.message_id = message_id
        self.retry_after = retry_after
        self.reason = reason
        reason_msg = f" {reason}." if reason else ""
        retry_msg = (
            f" Retry after {retry_after} seconds."
            if retry_after is not None
            else " Try again later."
        )
        super().__init__(f"Message {message_id} is temporarily locked.{reason_msg}{retry_msg}")


class MessageNotInFlightError(MessageLockedError):
    def __init__(self, message_id: MessageID, retry_after: int | None = None) -> None:
        super().__init__(message_id, retry_after, "Message is not currently in-flight")


class MessageLeaseExpiredError(MessageLockedError):
    def __init__(self, message_id: MessageID, retry_after: int | None = None) -> None:
        super().__init__(message_id, retry_after, "Message lease has expired")


class ReceiptHandleMismatchError(MessageLockedError):
    def __init__(self, message_id: MessageID, retry_after: int | None = None) -> None:
        super().__init__(
            message_id,
            retry_after,
            "Receipt handle does not match current lease holder",
        )


class UnhandledMessageError(QueueError, RuntimeError):
    status_code = 500

    def __init__(self, topic: str | None) -> None:
        self.topic = topic
        super().__init__(f"No queue subscribers found for topic {topic!r}.")


class ThrottledError(QueueError, RuntimeError):
    status_code = 429

    def __init__(self, retry_after: int | None = None) -> None:
        self.retry_after = retry_after
        suffix = f", Retry-After={retry_after}" if retry_after is not None else ""
        super().__init__(f"Throttled by queue service{suffix}")


# Only add public symbols to __all__; internal helpers must stay unexported.
__all__: tuple[str, ...] = (
    "BadRequestError",
    "ConsumerDiscoveryError",
    "ConsumerRegistryNotConfiguredError",
    "DeploymentResolutionError",
    "DuplicateIdempotencyKeyError",
    "DuplicateSubscriptionError",
    "ForbiddenError",
    "InternalServerError",
    "InvalidLimitError",
    "MessageAlreadyProcessedError",
    "MessageCorruptedError",
    "MessageLeaseExpiredError",
    "MessageLockedError",
    "MessageNotFoundError",
    "MessageNotInFlightError",
    "MessageUnavailableError",
    "PayloadValidationError",
    "ProtocolError",
    "QueueError",
    "ReceiptHandleMismatchError",
    "ServiceError",
    "SubscriptionError",
    "ThrottledError",
    "TokenResolutionError",
    "UnauthorizedError",
    "UnhandledMessageError",
)
