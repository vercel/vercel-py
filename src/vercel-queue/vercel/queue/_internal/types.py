from __future__ import annotations

from typing import Any, ClassVar, Generic, Protocol, TypeAlias, TypeGuard, TypeVar, cast

from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Iterable,
    Iterator,
    Mapping,
)
from dataclasses import dataclass
from datetime import datetime, timedelta

from .constants import DEFAULT_RETRY_AFTER_SECONDS
from .names import SanitizedName, validate_topic_name

T = TypeVar("T")
_TYPE_VAR_TYPE = type(T)

Duration: TypeAlias = int | float | timedelta
MessageID: TypeAlias = str
ReceiptHandle: TypeAlias = str
RequestContent: TypeAlias = bytes | Iterable[bytes] | AsyncIterable[bytes]
Headers: TypeAlias = Mapping[str, str]
RawHeaders: TypeAlias = Mapping[str, str]


class StrContainer(Protocol):
    """Container of strings that excludes bare strings structurally."""

    # Bare str has __contains__(str), while normal containers accept object.
    # Requiring the wider signature lets type checkers reject str here.

    def __iter__(self) -> Iterator[str]: ...

    def __contains__(self, item: object, /) -> bool: ...


def _is_duration(value: object) -> TypeGuard[Duration]:
    return isinstance(value, (int, float, timedelta)) and not isinstance(value, bool)


def duration_to_seconds(duration: Duration) -> int:
    if isinstance(duration, timedelta):
        return int(duration.total_seconds())
    if isinstance(duration, int) and not isinstance(duration, bool):
        return duration
    if isinstance(duration, float):
        return int(duration)
    raise TypeError("duration must be an int or float number of seconds or datetime.timedelta")


def duration_to_float_seconds(duration: Duration) -> float:
    if isinstance(duration, timedelta):
        return duration.total_seconds()
    if isinstance(duration, int) and not isinstance(duration, bool):
        return float(duration)
    if isinstance(duration, float):
        return duration
    raise TypeError("duration must be an int or float number of seconds or datetime.timedelta")


@dataclass(frozen=True, kw_only=True, eq=False)
class Topic(Generic[T]):
    """A named Vercel Queues topic.

    Topics identify the stream that messages are sent to and received from.
    """

    name: SanitizedName
    """Topic name to send to or receive from."""

    transport: Transport[Any] | None = None
    """Optional transport used when sending to or polling this topic."""

    __topic_origin__: ClassVar[type[Topic[Any]] | None] = None
    __topic_payload_type__: ClassVar[Any] = None
    _specializations: ClassVar[dict[Any, type[Topic[Any]]]] = {}

    def __class_getitem__(cls, params: Any) -> type[Topic[Any]]:
        if isinstance(params, tuple):
            if len(params) != 1:
                raise TypeError("Topic expects exactly one type argument")
            params = params[0]
        if isinstance(params, _TYPE_VAR_TYPE):
            return cast("type[Topic[Any]]", cls)

        try:
            return cls._specializations[params]
        except KeyError:
            pass

        payload_repr = _topic_payload_type_repr(params)
        specialization = type(
            f"Topic[{payload_repr}]",
            (cls,),
            {
                "__module__": cls.__module__,
                "__topic_origin__": Topic,
                "__topic_payload_type__": params,
            },
        )
        cls._specializations[params] = specialization
        return cast("type[Topic[Any]]", specialization)

    def __init__(
        self,
        name: str | SanitizedName,
        *,
        transport: Transport[Any] | None = None,
    ) -> None:
        object.__setattr__(self, "name", SanitizedName(validate_topic_name(name)))
        object.__setattr__(self, "transport", transport)

    def __repr__(self) -> str:
        return f"Topic(name={self.name!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Topic):
            return NotImplemented
        return self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)


def _topic_payload_type_repr(payload_type: object) -> str:
    name = getattr(payload_type, "__qualname__", None)
    if isinstance(name, str):
        return name
    name = getattr(payload_type, "__name__", None)
    if isinstance(name, str):
        return name
    return repr(payload_type)


@dataclass(frozen=True, kw_only=True)
class MessageMetadata:
    """Metadata attached to a queue message delivery.

    Identifiers and delivery tokens are opaque service values and should be
    passed back unchanged when acknowledging or extending a message.
    """

    message_id: MessageID
    """Opaque message ID assigned by the service."""

    delivery_count: int
    """Number of delivery attempts observed for the message."""

    created_at: datetime
    """Message creation timestamp."""

    topic: str
    """Topic name the message belongs to."""

    consumer_group: str
    """Consumer group that owns this delivery."""

    receipt_handle: ReceiptHandle | None = None
    """Opaque delivery token used for follow-up operations."""

    content_type: str | None = None
    """Stored message content type."""

    region: str | None = None
    """Queue region for follow-up operations."""

    expires_at: datetime | None = None
    """Message expiration timestamp, when supplied by the service."""

    visibility_deadline: datetime | None = None
    """Current processing deadline, when supplied by the service."""


@dataclass(frozen=True, kw_only=True)
class Message(Generic[T]):
    """A deserialized payload plus metadata for this delivery."""

    payload: T
    """Message payload returned by the configured transport."""

    metadata: MessageMetadata
    """Metadata for this delivery."""

    @property
    def message_id(self) -> MessageID:
        return self.metadata.message_id


class QueueDirective(Exception):  # noqa: N818
    """Directive raised by a queue subscriber.

    Queue delivery is at least once. Subscriber directives let a handler choose
    how the SDK resolves the delivery after user code runs.
    """

    reason: object | None

    def __init__(self, reason: object | None = None) -> None:
        self.reason = reason
        super().__init__(str(reason) if reason is not None else "")


class Handoff(QueueDirective):
    """Directive that tells the SDK a delivery was handed off.

    ``accept_and_handle`` normally owns the full push-delivery lifecycle: it
    accepts the delivery, keeps the processing lease alive while subscribers
    run, and acknowledges the message when all matching subscribers return
    successfully. ``Handoff`` tells the SDK that subscriber code successfully
    handed the delivery to another system with its own processing lifecycle.

    Raising this directive stops subscriber dispatch and suppresses the SDK's
    automatic follow-up action. The SDK also stops its lease renewal and leaves
    the current queue lease open. External code must take over from that point:
    it is responsible for any further lease renewal needed while processing
    continues, and for eventually acknowledging the message or changing its
    visibility using the original message metadata. If external code does none
    of those things, the message becomes eligible for redelivery when the lease
    expires.

    This is distinct from ``RetryAfter``, which asks the SDK to update
    visibility immediately, and from raising a normal exception, which reports
    handler failure. For example, the Celery broker uses ``Handoff`` after a
    Vercel Queue push delivery has been accepted into Kombu, because Celery
    must remain responsible for the eventual ACK or reject by delivery tag.
    """


class RetryAfter(QueueDirective):
    """Directive that retries a message after a delay.

    Raising ``RetryAfter`` stops subscriber dispatch and asks the SDK to make
    the message visible again after ``delay``. The SDK keeps lifecycle ownership
    through that follow-up action: it stops lease renewal, updates the delivery
    visibility using the current receipt handle, and reports success to the
    push-delivery caller once that update succeeds. Use this when the subscriber
    cannot process the message now, but wants the SDK to schedule the next
    delivery attempt instead of treating the handler as failed.

    A delay of zero makes the message eligible for immediate redelivery.

    Args:
        delay: Retry delay in seconds or as a ``datetime.timedelta``. Defaults
            to 60 seconds.
        reason: Optional diagnostic value stored on the directive.

    """

    timeout_seconds: int

    def __init__(
        self,
        delay: Duration = DEFAULT_RETRY_AFTER_SECONDS,
        reason: object | None = None,
    ) -> None:
        self.timeout_seconds = duration_to_seconds(delay)
        if self.timeout_seconds < 0:
            raise ValueError("delay must be non-negative")
        super().__init__(reason)

    def __repr__(self) -> str:
        return f"RetryAfter(timeout_seconds={self.timeout_seconds!r})"


class Transport(Protocol[T]):
    content_type: str

    def serialize(self, value: T) -> RequestContent: ...

    async def deserialize(
        self,
        payload: AsyncIterator[bytes],
        *,
        content_type: str,
    ) -> T: ...


# Only add public symbols to __all__; internal helpers must stay unexported.
__all__: tuple[str, ...] = (
    "Duration",
    "Handoff",
    "Message",
    "MessageID",
    "MessageMetadata",
    "QueueDirective",
    "ReceiptHandle",
    "RetryAfter",
    "StrContainer",
    "Topic",
)
