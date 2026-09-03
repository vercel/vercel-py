"""Compatibility tests for TypeScript's serialized error tags."""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.payloads import PLAIN_ENCODER
from vercel._internal.core.polyfills import UTC
from vercel.workflow import (
    FatalError,
    HookConflictError,
    RemoteError,
    RetryableError,
    WorkflowRunFailedError,
    serializable,
)
from vercel.workflow._internal import runtime, serialization as ser, world as w


def _wire(error: Exception) -> str:
    """The devalue text an error payload carries, minus the format prefix."""
    return PLAIN_ENCODER.encode_error(error)[len(ser.DEVALUE_V1) :].decode()


def _round_trip(error: Exception) -> Exception:
    return ser.hydrate_error(PLAIN_ENCODER.encode_error(error), what="a payload")


def _raised(error: Exception) -> Exception:
    """*error* with a traceback on it, as one that was actually raised."""
    try:
        raise error
    except Exception as raised:
        return raised


# Wire format


def test_fatal_error_uses_the_typescript_tag() -> None:
    assert _wire(FatalError("card declined")) == '[["FatalError",1],{"message":2},"card declined"]'


def test_hook_conflict_uses_the_typescript_tag() -> None:
    error = HookConflictError("shared-token", "wrun_owner")

    assert _wire(error) == (
        '[["HookConflictError",1],{"message":2,"token":3,"conflictingRunId":4},'
        '"Hook token \\"shared-token\\" is already in use by another workflow '
        '(run \\"wrun_owner\\")","shared-token","wrun_owner"]'
    )


def test_cause_uses_its_own_error_tag() -> None:
    error = _raised_with_cause()

    assert '["FatalError",1]' in _wire(error)
    assert '"cause":' in _wire(error)
    assert '["TypeError",' in _wire(error)


def test_unmapped_python_error_uses_the_generic_tag_with_its_name() -> None:
    assert _wire(KeyError("k")) == '[["Error",1],{"name":2,"message":3},"KeyError","\'k\'"]'


def test_stack_is_omitted_until_the_error_is_raised() -> None:
    assert "stack" not in _wire(FatalError("never raised"))

    wire = _wire(_raised(FatalError("raised")))
    assert '"stack"' in wire
    assert "Traceback (most recent call last)" in wire


def test_python_builtins_map_to_typescript_error_tags() -> None:
    assert _wire(TypeError("t")).startswith('[["TypeError"')
    assert _wire(SyntaxError("s")).startswith('[["SyntaxError"')
    # Python has no ReferenceError or RangeError.
    assert _wire(NameError("n")).startswith('[["ReferenceError"')
    assert _wire(ValueError("v")).startswith('[["RangeError"')


def test_specific_error_tags_do_not_claim_subclasses() -> None:
    class Narrower(ValueError):
        pass

    assert _wire(Narrower("v")) == '[["Error",1],{"name":2,"message":3},"Narrower","v"]'


def test_fatal_subclasses_keep_the_fatal_tag() -> None:
    class ContextViolation(FatalError):
        pass

    assert _wire(ContextViolation("nope")).startswith('[["FatalError"')


# TypeScript payloads

# Literal output from `@workflow/core`'s devalue serializer.
TS_FATAL_WITH_CAUSE = ser.DEVALUE_V1 + (
    b'[["FatalError",1],{"message":2,"stack":3,"cause":4},"fatal with cause",'
    b'"FatalError: fatal with cause\\n    at throwFatalErrorWithCause (99_e2e.ts:1365:19)",'
    b'["TypeError",5],{"message":6,"stack":7},"underlying type error",'
    b'"TypeError: underlying type error\\n    at throwFatalErrorWithCause (99_e2e.ts:1364:16)"]'
)

TS_HOOK_CONFLICT = ser.DEVALUE_V1 + (
    b'[["HookConflictError",1],{"message":2,"token":3,"conflictingRunId":4},'
    b'"Hook token \\"shared-token\\" is already in use by another workflow '
    b'(run \\"wrun_owner\\")","shared-token","wrun_owner"]'
)


def test_typescript_fatal_error_hydrates_to_python_fatal_error() -> None:
    error = ser.hydrate_error(TS_FATAL_WITH_CAUSE, what="the error of step step_0")

    assert isinstance(error, FatalError)
    assert str(error) == "fatal with cause"
    assert isinstance(error.__cause__, TypeError)
    assert str(error.__cause__) == "underlying type error"


def test_typescript_hook_conflict_hydrates_to_python_hook_conflict() -> None:
    error = ser.hydrate_error(TS_HOOK_CONFLICT, what="an error")

    assert isinstance(error, HookConflictError)
    assert error.token == "shared-token"
    assert error.conflicting_run_id == "wrun_owner"
    assert str(error) == (
        'Hook token "shared-token" is already in use by another workflow (run "wrun_owner")'
    )


def test_error_payload_requires_a_message() -> None:
    payload = ser.DEVALUE_V1 + b'[["FatalError",1],{}]'

    with pytest.raises(ser.SerializationError, match="malformed error payload"):
        ser.hydrate_error(payload, what="an error")


def test_typescript_stack_is_preserved() -> None:
    error = ser.hydrate_error(TS_FATAL_WITH_CAUSE, what="an error")

    stack: str = error.stack  # type: ignore[attr-defined]
    assert stack.startswith("FatalError: fatal with cause\n    at ")


def test_remote_stack_survives_another_serialization() -> None:
    error = ser.hydrate_error(TS_FATAL_WITH_CAUSE, what="an error")

    wire = _wire(error)
    assert "FatalError: fatal with cause\\n    at " in wire
    assert "Traceback (most recent call last)" not in wire


def test_unknown_typescript_error_keeps_its_name_and_tag() -> None:
    payload = ser.DEVALUE_V1 + b'[["URIError",1],{"message":2},"bad escape"]'

    error = ser.hydrate_error(payload, what="an error")

    assert isinstance(error, RemoteError)
    assert error.name == "URIError"
    assert str(error) == "URIError: bad escape"
    assert _wire(error) == '[["URIError",1],{"message":2},"bad escape"]'


def test_non_exception_cause_survives_a_python_round_trip() -> None:
    wire = '[["Error",1],{"name":2,"message":3,"cause":4},"OddError","bad",{"code":5},"E"]'
    error = ser.hydrate_error(ser.DEVALUE_V1 + wire.encode(), what="an error")

    assert isinstance(error.__cause__, RuntimeError)
    assert error.__cause__.args == ({"code": "E"},)
    assert _wire(error) == wire


def test_dom_exception_keeps_its_payload_name() -> None:
    payload = ser.DEVALUE_V1 + b'[["DOMException",1],{"message":2,"name":3},"aborted","AbortError"]'

    error = ser.hydrate_error(payload, what="an error")

    assert isinstance(error, RemoteError)
    assert error.name == "AbortError"


def test_typescript_only_error_tag_remains_readable() -> None:
    payload = ser.DEVALUE_V1 + b'[["AggregateError",1],{"message":2,"errors":3},"all failed",[]]'

    error = ser.hydrate_error(payload, what="an error")

    assert isinstance(error, RemoteError)
    assert error.name == "AggregateError"
    assert _wire(error) == '[["AggregateError",1],{"message":2,"errors":3},"all failed",[]]'


def test_retryable_error_carries_its_deadline_both_ways() -> None:
    deadline = datetime(2026, 2, 2, 2, 40, tzinfo=UTC)
    wire = '[["RetryableError",1],{"message":2,"retryAfter":3},"later",1770000000000]'

    assert _wire(RetryableError("later", retry_after=deadline)) == wire

    error = ser.hydrate_error(ser.DEVALUE_V1 + wire.encode(), what="an error")
    assert isinstance(error, RetryableError)
    assert str(error) == "later"
    assert error.retry_at == deadline


def test_retryable_subclass_keeps_retry_semantics() -> None:
    class BackoffError(RetryableError):
        pass

    assert _wire(BackoffError("later")).startswith('[["RetryableError"')


def test_legacy_python_errors_remain_readable() -> None:
    from_string = ser.hydrate_error("old step failed", what="an error")
    from_object = ser.hydrate_error(
        {"message": "old run failed", "stack": "legacy stack", "code": "ValueError"},
        what="an error",
    )

    assert isinstance(from_string, RuntimeError)
    assert str(from_string) == "old step failed"
    assert isinstance(from_object, ValueError)
    assert from_object.stack == "legacy stack"  # type: ignore[attr-defined]


def test_non_exception_throw_is_wrapped_as_runtime_error() -> None:
    # JavaScript throws whatever it likes; Python has to raise an exception.
    payload = PLAIN_ENCODER.encode({"kind": "business-rule-violation", "code": "INVOICE_LOCKED"})

    error = ser.hydrate_error(payload, what="an error")

    assert isinstance(error, RuntimeError)
    assert error.args[0] == {"kind": "business-rule-violation", "code": "INVOICE_LOCKED"}


# Python round trips


def _raised_with_cause() -> Exception:
    try:
        try:
            raise TypeError("underlying type error")
        except TypeError as root:
            raise FatalError("fatal with cause") from root
    except FatalError as error:
        return error


def test_fatal_error_and_cause_survive_a_round_trip() -> None:
    error = _round_trip(_raised_with_cause())

    assert isinstance(error, FatalError)
    assert str(error) == "fatal with cause"
    assert isinstance(error.__cause__, TypeError)
    assert str(error.__cause__) == "underlying type error"


def test_hook_conflict_survives_a_round_trip_without_an_owner() -> None:
    error = _round_trip(HookConflictError("shared-token"))

    assert isinstance(error, HookConflictError)
    assert error.token == "shared-token"
    assert error.conflicting_run_id is None


@pytest.mark.parametrize(
    "error",
    [TypeError("t"), SyntaxError("s"), NameError("n"), ValueError("v"), ZeroDivisionError("z")],
    ids=lambda error: type(error).__name__,
)
def test_builtin_error_round_trips_as_its_own_class(error: Exception) -> None:
    assert type(_round_trip(error)) is type(error)


def test_error_with_required_constructor_arguments_uses_remote_error() -> None:
    error = _round_trip(UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"))

    assert isinstance(error, RemoteError)
    assert error.name == "UnicodeDecodeError"


def test_registered_exception_class_outranks_error_tags() -> None:
    @serializable
    class PaymentDeclined(Exception):
        def __init__(self, code: str) -> None:
            super().__init__(f"declined: {code}")
            self.code = code

        def _workflow_serialize(self) -> dict[str, str]:
            return {"code": self.code}

        @classmethod
        def _workflow_deserialize(cls, data: dict[str, str]) -> PaymentDeclined:
            return cls(data["code"])

    assert '["Instance",' in _wire(PaymentDeclined("card_expired"))

    error = _round_trip(PaymentDeclined("card_expired"))
    assert isinstance(error, PaymentDeclined)
    assert error.code == "card_expired"


def test_unserializable_error_uses_fallback_payload() -> None:
    class Unstringable(Exception):
        def __str__(self) -> str:
            raise ValueError("nope")

    error = _round_trip(Unstringable())

    assert isinstance(error, RemoteError)
    assert error.name == "Unstringable"
    assert "could not be serialized" in str(error)


def test_run_failure_wrapper_tolerates_an_unstringable_error() -> None:
    class Unstringable(Exception):
        def __str__(self) -> str:
            raise ValueError("nope")

    wrapped = WorkflowRunFailedError("wrun_123", Unstringable())

    assert str(wrapped) == 'Workflow run "wrun_123" failed: Unstringable'
    assert isinstance(wrapped.__cause__, Unstringable)


# Plaintext error code


def test_user_errors_get_user_error_code() -> None:
    assert runtime.classify_run_error(FatalError("card declined")) == "USER_ERROR"
    assert runtime.classify_run_error(RuntimeError("boom")) == "USER_ERROR"


def test_replay_and_world_failures_get_specific_error_codes() -> None:
    assert runtime.classify_run_error(runtime.NondeterminismError("diverged")) == (
        "REPLAY_DIVERGENCE"
    )
    throttled = w.ThrottleError("slow down", status=429, retry_after=5)
    assert runtime.classify_run_error(throttled) == "WORLD_CONTRACT_ERROR"
    assert (
        runtime.classify_run_error(w.WorkflowWorldError("server error", status=500))
        == "WORLD_CONTRACT_ERROR"
    )


def test_non_retryable_world_errors_get_user_error_code() -> None:
    assert runtime.classify_run_error(w.RunExpiredError("expired", status=410)) == "USER_ERROR"
    assert (
        runtime.classify_run_error(w.WorkflowWorldError("bad request", status=400)) == "USER_ERROR"
    )
