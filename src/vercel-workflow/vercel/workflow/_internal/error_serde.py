"""devalue reducers and revivers for workflow errors."""

from __future__ import annotations

import builtins
import traceback
from collections.abc import Callable
from datetime import datetime
from typing import Any, TypedDict, cast

from vercel._internal.core.polyfills import UTC

from .errors import FatalError, HookConflictError, RemoteError, RetryableError


class _OptionalErrorFields(TypedDict, total=False):
    stack: str
    cause: Any


class ErrorPayload(_OptionalErrorFields):
    message: str


class NamedErrorPayload(ErrorPayload):
    name: str


class RetryableErrorPayload(ErrorPayload):
    retryAfter: int


class _OptionalHookConflictFields(TypedDict, total=False):
    conflictingRunId: str


class HookConflictErrorPayload(ErrorPayload, _OptionalHookConflictFields):
    token: str


_ERROR_CLASS_BY_TAG: dict[str, type[Exception]] = {
    "FatalError": FatalError,
    "TypeError": TypeError,
    "SyntaxError": SyntaxError,
    "ReferenceError": NameError,
    "RangeError": ValueError,
}

ERROR_TAG = "Error"
HOOK_CONFLICT_ERROR_TAG = "HookConflictError"
RETRYABLE_ERROR_TAG = "RetryableError"

# Error tags without Python counterparts. RemoteError retains their original
# payload so fields such as AggregateError.errors survive a Python round trip.
_FOREIGN_ERROR_TAGS = (
    "EvalError",
    "URIError",
    "DOMException",
    "AggregateError",
    "RuntimeDecryptionError",
)


def _error_stack(value: Exception) -> str | None:
    stored = getattr(value, "__dict__", {}).get("stack")
    if isinstance(stored, str):
        return stored
    if value.__traceback__ is None:
        return None
    # Causes are serialized structurally instead of being repeated in stack.
    formatted = traceback.format_exception(type(value), value, value.__traceback__, chain=False)
    return "".join(formatted).rstrip("\n")


def _error_message(value: Exception) -> str:
    if isinstance(value, RemoteError):
        return value.message
    return str(value)


def _error_payload(value: Exception) -> ErrorPayload:
    payload: ErrorPayload = {"message": _error_message(value)}
    stack = _error_stack(value)
    if stack is not None:
        payload["stack"] = stack
    # Python's implicit __context__ has no JavaScript counterpart.
    if value.__cause__ is not None:
        attributes = getattr(value, "__dict__", {})
        if value.__cause__ is attributes.get("_workflow_wrapped_cause"):
            payload["cause"] = attributes["_workflow_wire_cause"]
        else:
            payload["cause"] = value.__cause__
    return payload


def _reduce_error_class(cls: type[Exception], *, subclasses: bool) -> Callable[[Any], Any]:
    def reduce(value: Any) -> Any:
        if not isinstance(value, cls):
            return False
        if not subclasses and type(value) is not cls:
            return False
        return _error_payload(value)

    return reduce


def reduce_error(value: Any) -> NamedErrorPayload | bool:
    if not isinstance(value, Exception):
        return False
    name = value.name if isinstance(value, RemoteError) else type(value).__name__
    return {"name": name, **_error_payload(value)}


def as_exception(value: Any) -> Exception:
    if isinstance(value, Exception):
        return value
    return RuntimeError(value)


def _build_error(cls: type[Exception], message: str) -> Exception:
    try:
        return cls(message)
    except Exception:
        return RemoteError(message, name=cls.__name__)


def _require_error_payload(value: Any) -> ErrorPayload:
    if not isinstance(value, dict) or not isinstance(value.get("message"), str):
        raise ValueError(f"malformed error payload: {value!r}")
    return cast(ErrorPayload, value)


def _apply_error_payload(exc: Exception, payload: ErrorPayload) -> Exception:
    stack = payload.get("stack")
    if isinstance(stack, str):
        exc.stack = stack  # type: ignore[attr-defined]
    if "cause" in payload:
        cause = as_exception(payload["cause"])
        exc.__cause__ = cause
        exc._workflow_wire_cause = payload["cause"]  # type: ignore[attr-defined]
        exc._workflow_wrapped_cause = cause  # type: ignore[attr-defined]
    return exc


def _revive_error_class(cls: type[Exception]) -> Callable[[Any], Any]:
    def revive(value: Any) -> Any:
        payload = _require_error_payload(value)
        return _apply_error_payload(_build_error(cls, payload["message"]), payload)

    return revive


def _reduce_hook_conflict_error(value: Any) -> HookConflictErrorPayload | bool:
    if not isinstance(value, HookConflictError):
        return False
    payload: HookConflictErrorPayload = {
        **_error_payload(value),
        "token": value.token,
    }
    if value.conflicting_run_id is not None:
        payload["conflictingRunId"] = value.conflicting_run_id
    return payload


def _revive_hook_conflict_error(value: Any) -> HookConflictError:
    payload = _require_error_payload(value)
    token = value.get("token")
    if not isinstance(token, str):
        raise ValueError(f"invalid HookConflictError token: {token!r}")
    conflicting_run_id = value.get("conflictingRunId")
    if conflicting_run_id is not None and not isinstance(conflicting_run_id, str):
        raise ValueError(f"invalid HookConflictError conflictingRunId: {conflicting_run_id!r}")
    exc = HookConflictError(token, conflicting_run_id)
    _apply_error_payload(exc, payload)
    return exc


def _reduce_retryable_error(value: Any) -> RetryableErrorPayload | bool:
    if not isinstance(value, RetryableError):
        return False
    return {
        **_error_payload(value),
        "retryAfter": int(value.retry_at.timestamp() * 1000),
    }


def _revive_retryable_error(value: Any) -> RetryableError:
    payload = _require_error_payload(value)
    retry_after = value.get("retryAfter")
    at = None
    if isinstance(retry_after, int | float) and not isinstance(retry_after, bool):
        try:
            at = datetime.fromtimestamp(retry_after / 1000, UTC)
        except (OSError, OverflowError, ValueError) as error:
            raise ValueError(f"invalid RetryableError retryAfter: {retry_after!r}") from error
    elif retry_after is not None:
        raise ValueError(f"invalid RetryableError retryAfter: {retry_after!r}")
    exc = RetryableError(payload["message"], retry_after=at)
    _apply_error_payload(exc, payload)
    return exc


def _revive_foreign_error(tag: str) -> Callable[[Any], Any]:
    def revive(value: Any) -> Any:
        payload = _require_error_payload(value)
        name = value.get("name")
        exc = RemoteError(payload["message"], name=name if isinstance(name, str) else tag)
        exc._wire_tag = tag
        exc._wire_payload = dict(value)
        return _apply_error_payload(exc, payload)

    return revive


def _reduce_foreign_error(tag: str) -> Callable[[Any], Any]:
    def reduce(value: Any) -> Any:
        if not isinstance(value, RemoteError) or value._wire_tag != tag:
            return False
        payload = dict(value._wire_payload or {})
        payload.update(_error_payload(value))
        if value.__cause__ is None:
            payload.pop("cause", None)
        if tag == "DOMException":
            payload["name"] = value.name
        return payload

    return reduce


def revive_error(value: Any) -> Exception:
    payload = _require_error_payload(value)
    name = value.get("name")
    if not isinstance(name, str):
        raise ValueError(f"malformed error payload: {value!r}")
    cls = getattr(builtins, name, None)
    if isinstance(cls, type) and issubclass(cls, Exception):
        exc = _build_error(cls, payload["message"])
    else:
        exc = RemoteError(payload["message"], name=name)
    return _apply_error_payload(exc, payload)


# Callers must place Instance before these reducers so registered exception
# classes retain their custom serialization.
REDUCERS: dict[str, Callable[[Any], Any]] = {
    **{
        tag: _reduce_error_class(cls, subclasses=cls is FatalError)
        for tag, cls in _ERROR_CLASS_BY_TAG.items()
    },
    HOOK_CONFLICT_ERROR_TAG: _reduce_hook_conflict_error,
    RETRYABLE_ERROR_TAG: _reduce_retryable_error,
    **{tag: _reduce_foreign_error(tag) for tag in _FOREIGN_ERROR_TAGS},
    ERROR_TAG: reduce_error,
}

REVIVERS: dict[str, Callable[[Any], Any]] = {
    **{tag: _revive_error_class(cls) for tag, cls in _ERROR_CLASS_BY_TAG.items()},
    **{tag: _revive_foreign_error(tag) for tag in _FOREIGN_ERROR_TAGS},
    HOOK_CONFLICT_ERROR_TAG: _revive_hook_conflict_error,
    RETRYABLE_ERROR_TAG: _revive_retryable_error,
    ERROR_TAG: revive_error,
}
