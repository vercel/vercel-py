from __future__ import annotations

from typing import Any, Literal, ParamSpec, Protocol, TypeAlias, TypeVar, cast, overload

import inspect
import logging
import threading
import weakref
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from importlib import import_module
from itertools import count
from types import MappingProxyType

from .errors import (
    DuplicateSubscriptionError,
    PayloadValidationError,
    SubscriptionError,
    UnhandledMessageError,
)
from .log import debug_enabled, debug_log_for_msg
from .names import (
    SanitizedName,
    normalize_name,
    sanitize_name,
    validate_subscription_pattern,
    validate_topic_name,
)
from .transports import (
    TransportKind,
    is_untyped_payload_annotation,
    payload_transport_kind,
    reject_invalid_payload_annotation,
    transport_for_kind,
)
from .types import (
    Duration,
    Message,
    MessageMetadata,
    QueueDirective,
    RetryAfter,
    StrContainer,
    Topic,
    Transport,
    duration_to_seconds,
)
from .typeutils import (
    ResolvedAnnotation,
    TypeAnnotationResolutionError,
    args,
    origin_is,
    resolve_annotation_with_namespace_from_call_stack,
    strip_annotated,
)

_Subscriber: TypeAlias = Callable[..., Any | Awaitable[Any]]
_SubscriberRef: TypeAlias = weakref.ReferenceType[_Subscriber]
P = ParamSpec("P")
R = TypeVar("R")
R_co = TypeVar("R_co", covariant=True)
T = TypeVar("T")


class PayloadAdapter(Protocol):
    def validate_python(self, value: Any, /) -> Any: ...


class EmbeddedDispatcher(Protocol):
    def register_subscription(
        self,
        *,
        topic: str,
        consumer_group: str,
        retry_after_seconds: int | None,
        initial_delay_seconds: int | None,
        max_concurrency: int | None,
        max_attempts: int | None,
    ) -> None: ...


class QueueSubscriber(Protocol[P, R_co]):
    __module__: str
    __qualname__: str
    __vercel_queue_subscriber__: Literal[True]

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R_co: ...


class _TypedTopicSubscriberDecorator(Protocol[T]):
    @overload
    def __call__(self, func: Callable[[T], R], /) -> QueueSubscriber[[T], R]: ...

    @overload
    def __call__(
        self,
        func: Callable[[Message[T]], R],
        /,
    ) -> QueueSubscriber[[Message[T]], R]: ...


InvocationMode: TypeAlias = Literal["payload", "message"]


@dataclass(frozen=True, kw_only=True)
class InvocationPlan:
    payload_adapter: PayloadAdapter | None
    mode: InvocationMode
    transport_kind: TransportKind

    def prepare_payload(self, payload: Any) -> Any:
        if self.payload_adapter is None:
            return payload
        try:
            return self.payload_adapter.validate_python(payload)
        except Exception as exc:
            # Keep pydantic optional and private to this implementation.
            if exc.__class__.__name__ == "ValidationError":
                raise PayloadValidationError(str(exc)) from exc
            raise


@dataclass(frozen=True, kw_only=True)
class _Subscription:
    func_ref: _SubscriberRef
    order: int
    consumer_group: SanitizedName
    invocation: InvocationPlan
    topic: str
    retry_after_seconds: int | None = None
    initial_delay_seconds: int | None = None
    max_concurrency: int | None = None
    max_attempts: int | None = None

    def func(self) -> _Subscriber | None:
        return self.func_ref()


@dataclass(frozen=True, kw_only=True)
class _MatchedSubscription:
    subscription: _Subscription
    func: _Subscriber


@dataclass(frozen=True, kw_only=True)
class _RegistrySnapshot:
    subscriptions: tuple[_Subscription, ...]
    wildcard_by_consumer: Mapping[str, tuple[_Subscription, ...]]
    exact_by_consumer_topic: Mapping[tuple[str, str], tuple[_Subscription, ...]]
    prefix_by_consumer: Mapping[str, tuple[_Subscription, ...]]
    dispatchers: tuple[weakref.ReferenceType[EmbeddedDispatcher], ...]


_EMPTY_SNAPSHOT = _RegistrySnapshot(
    subscriptions=(),
    wildcard_by_consumer={},
    exact_by_consumer_topic={},
    prefix_by_consumer={},
    dispatchers=(),
)

_registry_snapshot = _EMPTY_SNAPSHOT
_subscriptions_lock = threading.Lock()
_subscription_order = count()
_LOGGER = logging.getLogger("vercel.queue")


def _build_registry_snapshot(
    subscriptions: Iterable[_Subscription],
    dispatchers: Iterable[weakref.ReferenceType[EmbeddedDispatcher]],
) -> _RegistrySnapshot:
    live_subscriptions = tuple(sub for sub in subscriptions if sub.func() is not None)
    live_dispatchers = tuple(ref for ref in dispatchers if ref() is not None)

    wildcard: dict[str, list[_Subscription]] = {}
    exact: dict[tuple[str, str], list[_Subscription]] = {}
    prefix: dict[str, list[_Subscription]] = {}
    for sub in live_subscriptions:
        consumer_group = str(sub.consumer_group)
        if sub.topic == "*":
            wildcard.setdefault(consumer_group, []).append(sub)
        elif sub.topic.endswith("*"):
            prefix.setdefault(consumer_group, []).append(sub)
        else:
            exact.setdefault((consumer_group, sub.topic), []).append(sub)

    return _RegistrySnapshot(
        subscriptions=live_subscriptions,
        wildcard_by_consumer=MappingProxyType({
            key: tuple(value) for key, value in wildcard.items()
        }),
        exact_by_consumer_topic=MappingProxyType({
            key: tuple(value) for key, value in exact.items()
        }),
        prefix_by_consumer=MappingProxyType({key: tuple(value) for key, value in prefix.items()}),
        dispatchers=live_dispatchers,
    )


def _publish_registry_snapshot(snapshot: _RegistrySnapshot) -> None:
    global _registry_snapshot  # noqa: PLW0603
    _registry_snapshot = snapshot


def _prune_registry_snapshot_locked() -> _RegistrySnapshot:
    snapshot = _build_registry_snapshot(
        _registry_snapshot.subscriptions,
        _registry_snapshot.dispatchers,
    )
    _publish_registry_snapshot(snapshot)
    return snapshot


def register_embedded_dispatcher(dispatcher: EmbeddedDispatcher) -> None:
    with _subscriptions_lock:
        current = _prune_registry_snapshot_locked()
        if any(ref() is dispatcher for ref in current.dispatchers):
            return
        updated = _build_registry_snapshot(
            current.subscriptions,
            (*current.dispatchers, weakref.ref(dispatcher)),
        )
        _publish_registry_snapshot(updated)
        subscriptions = updated.subscriptions
    for subscription in subscriptions:
        dispatcher.register_subscription(
            topic=subscription.topic,
            consumer_group=str(subscription.consumer_group),
            retry_after_seconds=subscription.retry_after_seconds,
            initial_delay_seconds=subscription.initial_delay_seconds,
            max_concurrency=subscription.max_concurrency,
            max_attempts=subscription.max_attempts,
        )


def unregister_embedded_dispatcher(dispatcher: EmbeddedDispatcher) -> None:
    with _subscriptions_lock:
        live: list[weakref.ReferenceType[EmbeddedDispatcher]] = []
        for ref in _registry_snapshot.dispatchers:
            live_dispatcher = ref()
            if live_dispatcher is None or live_dispatcher is dispatcher:
                continue
            live.append(ref)
        _publish_registry_snapshot(_build_registry_snapshot(_registry_snapshot.subscriptions, live))


def _clear_embedded_dispatchers() -> None:
    with _subscriptions_lock:
        _publish_registry_snapshot(_build_registry_snapshot(_registry_snapshot.subscriptions, ()))


def clear_subscriptions_for_tests() -> None:
    with _subscriptions_lock:
        _publish_registry_snapshot(_EMPTY_SNAPSHOT)


def _notify_embedded_dispatchers(subscription: _Subscription) -> None:
    dispatchers: list[EmbeddedDispatcher] = []
    with _subscriptions_lock:
        live: list[weakref.ReferenceType[EmbeddedDispatcher]] = []
        for ref in _registry_snapshot.dispatchers:
            dispatcher = ref()
            if dispatcher is None:
                continue
            live.append(ref)
            dispatchers.append(dispatcher)
        if len(live) != len(_registry_snapshot.dispatchers):
            _publish_registry_snapshot(
                _build_registry_snapshot(_registry_snapshot.subscriptions, live)
            )
    for dispatcher in dispatchers:
        dispatcher.register_subscription(
            topic=subscription.topic,
            consumer_group=str(subscription.consumer_group),
            retry_after_seconds=subscription.retry_after_seconds,
            initial_delay_seconds=subscription.initial_delay_seconds,
            max_concurrency=subscription.max_concurrency,
            max_attempts=subscription.max_attempts,
        )


@dataclass(frozen=True, kw_only=True)
class Subscription:
    """Deployment trigger metadata for a registered queue subscriber."""

    func: Callable[..., Any]
    topic: str
    consumer_group: str
    retry_after_seconds: int | None = None
    initial_delay_seconds: int | None = None
    max_concurrency: int | None = None
    max_attempts: int | None = None


def _subscriber_ref(func: _Subscriber) -> _SubscriberRef:
    try:
        return weakref.WeakMethod(func)  # type: ignore[arg-type]
    except TypeError:
        return weakref.ref(func)


def _is_message_annotation(annotation: Any) -> bool:
    annotation = strip_annotated(annotation)
    return annotation is Message or origin_is(annotation, Message)


def _message_payload_annotation(annotation: Any) -> Any:
    annotation = strip_annotated(annotation)
    if annotation is Message:
        return inspect.Signature.empty
    message_args = args(annotation)
    if not message_args:
        return inspect.Signature.empty
    return message_args[0]


def _topic_payload_annotation(topic: str | SanitizedName | Topic[Any]) -> Any:
    if not isinstance(topic, Topic):
        return inspect.Signature.empty
    if getattr(type(topic), "__topic_origin__", None) is not Topic:
        return inspect.Signature.empty
    return type(topic).__topic_payload_type__


def _normalize_subscription_topic(topic: str | SanitizedName | Topic[Any]) -> str:
    if isinstance(topic, SanitizedName):
        return str(topic)
    if isinstance(topic, str):
        validate_subscription_pattern(topic)
        return topic
    if isinstance(topic, Topic):
        return validate_topic_name(topic)
    raise TypeError("topic must be a string or Topic")


def _resolve_invocation_payload_annotation(
    handler_annotation: Any,
    topic_annotation: Any,
) -> Any:
    handler_annotation = strip_annotated(handler_annotation)
    topic_annotation = strip_annotated(topic_annotation)
    if is_untyped_payload_annotation(topic_annotation):
        return handler_annotation
    if is_untyped_payload_annotation(handler_annotation):
        return topic_annotation
    if handler_annotation != topic_annotation:
        raise SubscriptionError(
            "queue subscriber payload annotation "
            f"{handler_annotation!r} is incompatible with topic payload "
            f"annotation {topic_annotation!r}"
        )
    return handler_annotation


def _payload_adapter(
    annotation: Any,
    *,
    localns: dict[str, Any] | None = None,
) -> PayloadAdapter | None:
    annotation = strip_annotated(annotation)
    if is_untyped_payload_annotation(annotation):
        return None
    reject_invalid_payload_annotation(annotation)
    if _transport_kind(annotation) != "json":
        return None
    try:
        type_adapter = import_module("pydantic").TypeAdapter
    except ImportError as exc:
        raise RuntimeError(
            "Typed queue subscribers require pydantic. Install `vercel-queue[typed]` "
            "or remove the payload type annotation."
        ) from exc
    adapter = type_adapter(annotation)
    if localns and not adapter.pydantic_complete:
        adapter.rebuild(_types_namespace=localns, raise_errors=False)
    if not adapter.pydantic_complete:
        raise SubscriptionError(
            f"could not resolve queue subscriber payload model annotations for {annotation!r}"
        )
    return adapter


def _transport_kind(annotation: Any) -> TransportKind:
    return payload_transport_kind(annotation)


def _resolve_payload_annotation(
    func: _Subscriber,
    payload_param: inspect.Parameter,
) -> ResolvedAnnotation:
    annotation = payload_param.annotation
    if annotation in {inspect.Signature.empty, Any}:
        return ResolvedAnnotation(annotation)
    try:
        return resolve_annotation_with_namespace_from_call_stack(
            annotation,
            globalns=getattr(func, "__globals__", {}),
        )
    except TypeAnnotationResolutionError as exc:
        raise SubscriptionError(
            f"could not resolve queue subscriber type annotations for "
            f"{getattr(func, '__qualname__', func)!r}"
        ) from exc.__cause__


def _build_invocation_plan(
    func: _Subscriber,
    *,
    topic_payload_annotation: Any = inspect.Signature.empty,
) -> InvocationPlan:
    signature = inspect.signature(func)
    input_params: list[inspect.Parameter] = []
    for param in signature.parameters.values():
        if param.kind in {
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        }:
            raise SubscriptionError("queue subscriber must not accept *args or **kwargs")
        if param.kind is inspect.Parameter.KEYWORD_ONLY:
            if param.default is inspect.Parameter.empty:
                raise SubscriptionError(
                    "queue subscriber keyword-only parameters must have defaults"
                )
            continue
        if param.default is inspect.Parameter.empty:
            input_params.append(param)

    if len(input_params) != 1:
        raise SubscriptionError(
            "queue subscriber must accept exactly one required payload parameter"
        )

    payload_param = input_params[0]
    resolved_annotation = _resolve_payload_annotation(func, payload_param)
    annotation = resolved_annotation.annotation
    mode: InvocationMode = "message" if _is_message_annotation(annotation) else "payload"
    payload_annotation = (
        _message_payload_annotation(annotation) if mode == "message" else annotation
    )
    payload_annotation = _resolve_invocation_payload_annotation(
        payload_annotation,
        topic_payload_annotation,
    )
    reject_invalid_payload_annotation(payload_annotation)
    return InvocationPlan(
        payload_adapter=_payload_adapter(
            payload_annotation,
            localns=resolved_annotation.localns,
        ),
        mode=mode,
        transport_kind=_transport_kind(payload_annotation),
    )


def _call_subscription(
    matched: _MatchedSubscription,
    message: Any,
    metadata: MessageMetadata,
) -> Any:
    invocation = matched.subscription.invocation

    payload = invocation.prepare_payload(message)
    if debug_enabled():
        debug_log_for_msg(
            "message.handler_start",
            metadata,
            handler=_handler_name(matched.func),
        )
    if invocation.mode == "message":
        return matched.func(Message(payload=payload, metadata=metadata))
    return matched.func(payload)


def _handler_name(func: _Subscriber) -> str:
    module = getattr(func, "__module__", type(func).__module__)
    qualname = getattr(func, "__qualname__", type(func).__qualname__)
    return f"{module}.{qualname}"


def _log_handler_exception(exc: BaseException, metadata: MessageMetadata) -> None:
    _LOGGER.error(
        "queue subscriber failed while polling topic %r for consumer group %r",
        metadata.topic,
        metadata.consumer_group,
        exc_info=(type(exc), exc, exc.__traceback__),
    )


def _subscription_for_func(func: _Subscriber) -> _MatchedSubscription:
    with _subscriptions_lock:
        snapshot = _prune_registry_snapshot_locked()
    matches: list[_MatchedSubscription] = []
    for sub in snapshot.subscriptions:
        candidate = sub.func()
        if candidate is func:
            matches.append(_MatchedSubscription(subscription=sub, func=candidate))
    if not matches:
        raise SubscriptionError("queue subscriber is not registered")
    if len(matches) > 1:
        raise SubscriptionError("queue subscriber has multiple registrations")
    return matches[0]


def _subscription_matches_topic(subscription: _Subscription, topic: str) -> bool:
    pattern = subscription.topic
    if pattern == "*":
        return True
    if pattern.endswith("*"):
        return topic.startswith(pattern[:-1])
    return topic == pattern


async def call_subscriber(
    subscriber: _Subscriber,
    message: Message[Any],
) -> None:
    matched = _subscription_for_func(subscriber)
    if not _subscription_matches_topic(matched.subscription, message.metadata.topic):
        raise UnhandledMessageError(message.metadata.topic)
    try:
        await _maybe_await_result(_call_subscription(matched, message.payload, message.metadata))
    except QueueDirective:
        raise
    except Exception as exc:  # noqa: BLE001
        _log_handler_exception(exc, message.metadata)
        raise RetryAfter from None


def call_subscriber_sync(
    subscriber: _Subscriber,
    message: Message[Any],
) -> None:
    matched = _subscription_for_func(subscriber)
    if not _subscription_matches_topic(matched.subscription, message.metadata.topic):
        raise UnhandledMessageError(message.metadata.topic)
    try:
        _call_subscription(matched, message.payload, message.metadata)
    except QueueDirective:
        raise
    except Exception as exc:  # noqa: BLE001
        _log_handler_exception(exc, message.metadata)
        raise RetryAfter from None


def reject_async_subscriber_for_sync(subscriber: _Subscriber) -> None:
    matched = _subscription_for_func(subscriber)
    if inspect.iscoroutinefunction(matched.func):
        raise RuntimeError("async subscribers must be polled with an async polling loop")


def poll_targets_for_subscriber(
    subscriber: _Subscriber,
    topics: StrContainer | None,
) -> tuple[tuple[str, SanitizedName], ...]:
    matched = _subscription_for_func(subscriber)
    subscription = matched.subscription
    if topics is None:
        if subscription.topic == "*" or subscription.topic.endswith("*"):
            raise SubscriptionError(
                "queue subscriber uses a wildcard topic pattern; pass concrete topics"
            )
        return ((subscription.topic, subscription.consumer_group),)

    if isinstance(topics, str):
        raise TypeError("topics must be an iterable of topic strings, not a string")

    targets: list[tuple[str, SanitizedName]] = []
    seen: set[str] = set()
    for topic in topics:
        if not isinstance(topic, str):
            raise TypeError("topics must contain only strings")
        resolved_topic = validate_topic_name(topic)
        if resolved_topic in seen:
            continue
        seen.add(resolved_topic)
        if not _subscription_matches_topic(subscription, resolved_topic):
            raise SubscriptionError(
                f"topic {resolved_topic!r} does not match subscriber topic pattern "
                f"{subscription.topic!r}"
            )
        targets.append((resolved_topic, subscription.consumer_group))
    if not targets:
        raise SubscriptionError("topics must contain at least one topic")
    return tuple(targets)


def _transport_for_kind(kind: TransportKind) -> Transport[Any]:
    return transport_for_kind(kind)


def infer_subscriber_transport(metadata: MessageMetadata) -> Transport[Any]:
    matching = _matching_subscriptions(metadata)
    if not matching:
        raise _no_matching_subscriptions_error(metadata.topic)

    kinds = {matched.subscription.invocation.transport_kind for matched in matching}
    if len(kinds) != 1:
        raise SubscriptionError(
            "matching queue subscribers require incompatible payload transports: "
            + ", ".join(sorted(kinds))
        )
    return _transport_for_kind(kinds.pop())


async def _maybe_await_result(result: Any) -> Any:
    if inspect.isawaitable(result):
        return await result
    return result


def _default_consumer_group(func: _Subscriber) -> SanitizedName:
    module = getattr(func, "__module__", None)
    qualname = getattr(func, "__qualname__", getattr(func, "__name__", "subscriber"))
    if module:
        return sanitize_name(f"{module}.{qualname}")
    return sanitize_name(str(qualname))


def _fully_qualified_handler_name(func: _Subscriber) -> str:
    module = getattr(func, "__module__", None)
    qualname = getattr(func, "__qualname__", getattr(func, "__name__", repr(func)))
    if module:
        return f"{module}.{qualname}"
    return str(qualname)


def _optional_non_negative_int(name: str, value: int | None) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _optional_non_negative_duration(name: str, value: Duration | None) -> int | None:
    if value is None:
        return None
    seconds = duration_to_seconds(value)
    if seconds < 0:
        raise ValueError(f"{name} must be non-negative")
    return seconds


def _optional_bounded_duration(
    name: str,
    value: Duration | None,
    *,
    minimum: int,
    maximum: int,
) -> int | None:
    seconds = _optional_non_negative_duration(name, value)
    if seconds is None:
        return None
    if seconds < minimum or seconds > maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum} seconds")
    return seconds


def _topic_prefix(pattern: str) -> str | None:
    if pattern == "*":
        return ""
    if pattern.endswith("*"):
        return pattern[:-1]
    return None


def _subscription_patterns_overlap(first: str, second: str) -> bool:
    first_prefix = _topic_prefix(first)
    second_prefix = _topic_prefix(second)
    if first_prefix is None and second_prefix is None:
        return first == second
    if first_prefix is None:
        return first.startswith(cast("str", second_prefix))
    if second_prefix is None:
        return second.startswith(first_prefix)
    return first_prefix.startswith(second_prefix) or second_prefix.startswith(first_prefix)


def _validate_subscription_is_unambiguous(
    subscription: _Subscription,
    existing_subscriptions: Iterable[_Subscription],
) -> None:
    for existing in existing_subscriptions:
        existing_func = existing.func()
        if existing_func is None:
            continue
        if existing.consumer_group != subscription.consumer_group:
            continue
        if not _subscription_patterns_overlap(subscription.topic, existing.topic):
            continue
        raise DuplicateSubscriptionError(
            "queue subscriber topic pattern "
            f"{subscription.topic!r} overlaps existing topic pattern "
            f"{existing.topic!r} for consumer group {subscription.consumer_group!r}; "
            f"conflicting handler: {_fully_qualified_handler_name(existing_func)}"
        )


def _register_subscription(
    func: Callable[P, R],
    *,
    consumer_group: str | SanitizedName | None = None,
    topic: str | SanitizedName | Topic[Any],
    retry_after: Duration | None = None,
    initial_delay: Duration | None = None,
    max_concurrency: int | None = None,
    max_attempts: int | None = None,
) -> QueueSubscriber[P, R]:
    topic_name = _normalize_subscription_topic(topic)
    topic_payload_annotation = _topic_payload_annotation(topic)

    resolved_consumer_group = (
        _default_consumer_group(func)
        if consumer_group is None
        else normalize_name(
            consumer_group,
            field="consumer_group",
        )
    )
    subscription = _Subscription(
        func_ref=_subscriber_ref(cast("_Subscriber", func)),
        order=next(_subscription_order),
        consumer_group=resolved_consumer_group,
        invocation=_build_invocation_plan(
            cast("_Subscriber", func),
            topic_payload_annotation=topic_payload_annotation,
        ),
        topic=topic_name,
        retry_after_seconds=_optional_bounded_duration(
            "retry_after",
            retry_after,
            minimum=1,
            maximum=86400,
        ),
        initial_delay_seconds=_optional_bounded_duration(
            "initial_delay",
            initial_delay,
            minimum=0,
            maximum=86400,
        ),
        max_concurrency=_optional_non_negative_int("max_concurrency", max_concurrency),
        max_attempts=_optional_non_negative_int("max_attempts", max_attempts),
    )
    with _subscriptions_lock:
        current = _prune_registry_snapshot_locked()
        _validate_subscription_is_unambiguous(subscription, current.subscriptions)
        updated = _build_registry_snapshot(
            (*current.subscriptions, subscription),
            current.dispatchers,
        )
        _publish_registry_snapshot(updated)
    _notify_embedded_dispatchers(subscription)
    return cast("QueueSubscriber[P, R]", func)


@overload
def subscribe(func: Callable[P, R], /, *, topic: str) -> QueueSubscriber[P, R]: ...


@overload
def subscribe(
    func: Callable[P, R],
    /,
    *,
    topic: SanitizedName,
) -> QueueSubscriber[P, R]: ...


@overload
def subscribe(func: Callable[[T], R], /, *, topic: Topic[T]) -> QueueSubscriber[[T], R]: ...


@overload
def subscribe(
    func: Callable[[Message[T]], R],
    /,
    *,
    topic: Topic[T],
) -> QueueSubscriber[[Message[T]], R]: ...


@overload
def subscribe(
    *,
    topic: str,
    consumer_group: str | SanitizedName | None = None,
    retry_after: Duration | None = None,
    initial_delay: Duration | None = None,
    max_concurrency: int | None = None,
    max_attempts: int | None = None,
) -> Callable[[Callable[P, R]], QueueSubscriber[P, R]]: ...


@overload
def subscribe(
    *,
    topic: SanitizedName,
    consumer_group: str | SanitizedName | None = None,
    retry_after: Duration | None = None,
    initial_delay: Duration | None = None,
    max_concurrency: int | None = None,
    max_attempts: int | None = None,
) -> Callable[[Callable[P, R]], QueueSubscriber[P, R]]: ...


@overload
def subscribe(
    *,
    topic: Topic[T],
    consumer_group: str | SanitizedName | None = None,
    retry_after: Duration | None = None,
    initial_delay: Duration | None = None,
    max_concurrency: int | None = None,
    max_attempts: int | None = None,
) -> _TypedTopicSubscriberDecorator[T]: ...


@overload
def subscribe(
    func: None,
    /,
    *,
    topic: str,
    consumer_group: str | SanitizedName | None = None,
    retry_after: Duration | None = None,
    initial_delay: Duration | None = None,
    max_concurrency: int | None = None,
    max_attempts: int | None = None,
) -> Callable[[Callable[P, R]], QueueSubscriber[P, R]]: ...


@overload
def subscribe(
    func: None,
    /,
    *,
    topic: SanitizedName,
    consumer_group: str | SanitizedName | None = None,
    retry_after: Duration | None = None,
    initial_delay: Duration | None = None,
    max_concurrency: int | None = None,
    max_attempts: int | None = None,
) -> Callable[[Callable[P, R]], QueueSubscriber[P, R]]: ...


@overload
def subscribe(
    func: None,
    /,
    *,
    topic: Topic[T],
    consumer_group: str | SanitizedName | None = None,
    retry_after: Duration | None = None,
    initial_delay: Duration | None = None,
    max_concurrency: int | None = None,
    max_attempts: int | None = None,
) -> _TypedTopicSubscriberDecorator[T]: ...


def subscribe(
    func: _Subscriber | None = None,
    /,
    *,
    topic: str | SanitizedName | Topic[Any],
    consumer_group: str | SanitizedName | None = None,
    retry_after: Duration | None = None,
    initial_delay: Duration | None = None,
    max_concurrency: int | None = None,
    max_attempts: int | None = None,
) -> _Subscriber | Callable[[_Subscriber], _Subscriber]:
    """Register a function as a queue subscriber.

    Args:
        func: Function being decorated when ``@subscribe(topic=...)`` is used.
        topic: Required topic filter for this subscriber. A string matches exactly,
            a string ending in ``*`` matches by prefix, and ``"*"`` matches every
            topic.
        consumer_group: Local/in-process consumer group override. When omitted,
            the SDK derives one from the function's fully-qualified Python name.
        retry_after: Optional base retry delay for generated queue trigger
            configuration.
        initial_delay: Optional deploy-time delay before generated queue
            consumers start processing.
        max_concurrency: Optional push dispatcher concurrency cap.
        max_attempts: Optional push dispatcher delivery attempt cap.

    Returns:
        The original function, or a decorator when called with arguments.

    Raises:
        RuntimeError: If the subscriber has typed payload validation but
            pydantic is not installed.
        TypeError: If the subscriber signature cannot accept a payload.
        ValueError: If ``consumer_group`` is an empty string.

    """

    def decorator(f: Callable[P, R]) -> Callable[P, R]:
        return _register_subscription(
            f,
            consumer_group=consumer_group,
            topic=topic,
            retry_after=retry_after,
            initial_delay=initial_delay,
            max_concurrency=max_concurrency,
            max_attempts=max_attempts,
        )

    return decorator(func) if func is not None else decorator


def get_subscriptions() -> tuple[Subscription, ...]:
    """Return deployment trigger metadata for registered subscribers."""
    subscriptions: list[Subscription] = []
    with _subscriptions_lock:
        snapshot = _prune_registry_snapshot_locked()
    for sub in snapshot.subscriptions:
        func = sub.func()
        if func is not None:
            subscriptions.append(
                Subscription(
                    func=func,
                    topic=sub.topic,
                    consumer_group=str(sub.consumer_group),
                    retry_after_seconds=sub.retry_after_seconds,
                    initial_delay_seconds=sub.initial_delay_seconds,
                    max_concurrency=sub.max_concurrency,
                    max_attempts=sub.max_attempts,
                )
            )
    return tuple(subscriptions)


def _no_matching_subscriptions_error(
    topic: str | None,
    consumer_group: str | None = None,
) -> UnhandledMessageError:
    return UnhandledMessageError(topic, consumer_group)


def _matching_prefix_subscriptions(
    snapshot: _RegistrySnapshot,
    *,
    topic: str,
    consumer_group: str,
) -> tuple[_Subscription, ...]:
    return tuple(
        sub
        for sub in snapshot.prefix_by_consumer.get(consumer_group, ())
        if topic.startswith(sub.topic[:-1])
    )


def _merge_candidates_in_order(
    first: tuple[_Subscription, ...],
    second: tuple[_Subscription, ...],
    third: tuple[_Subscription, ...],
) -> tuple[_Subscription, ...]:
    if not second and not third:
        return first
    if not first and not third:
        return second
    if not first and not second:
        return third
    return tuple(sorted((*first, *second, *third), key=lambda sub: sub.order))


def _live_candidates(candidates: tuple[_Subscription, ...]) -> tuple[_MatchedSubscription, ...]:
    live: list[_MatchedSubscription] = []
    for sub in candidates:
        func = sub.func()
        if func is not None:
            live.append(_MatchedSubscription(subscription=sub, func=func))
    return tuple(live)


def _matching_subscriptions(
    metadata: MessageMetadata,
) -> tuple[_MatchedSubscription, ...]:
    snapshot = _registry_snapshot
    consumer_group = str(metadata.consumer_group)
    topic = metadata.topic
    matching = _live_candidates(
        _merge_candidates_in_order(
            snapshot.wildcard_by_consumer.get(consumer_group, ()),
            snapshot.exact_by_consumer_topic.get((consumer_group, topic), ()),
            _matching_prefix_subscriptions(
                snapshot,
                topic=topic,
                consumer_group=consumer_group,
            ),
        )
    )
    if not matching:
        raise _no_matching_subscriptions_error(metadata.topic, str(metadata.consumer_group))
    return matching


async def call_subscribers(
    message: Message[Any],
) -> None:
    sub = _matching_subscriptions(message.metadata)[0]
    await _maybe_await_result(_call_subscription(sub, message.payload, message.metadata))


def call_subscribers_sync(
    message: Message[Any],
) -> None:
    sub = _matching_subscriptions(message.metadata)[0]
    result = _call_subscription(sub, message.payload, message.metadata)
    if inspect.isawaitable(result):
        close = getattr(result, "close", None)
        if callable(close):
            close()
        raise RuntimeError("async subscribers must be polled with an async polling loop")


# Only add public symbols to __all__; internal helpers must stay unexported.
__all__ = (
    "QueueSubscriber",
    "Subscription",
    "get_subscriptions",
    "subscribe",
)
