"""Thrown values on the wire, on `@workflow/core`'s error tags.

`run_failed`, `step_failed` and `step_retrying` carry the thrown value through
the serialization pipeline, so an error is a payload like any other -- and which
tag it rides is a contract with the TypeScript side, not an internal detail. A
`FatalError` written here has to be a `FatalError` to `FatalError.is()` over
there, and the payload TypeScript writes for one has to come back as this SDK's
class. Both directions are asserted literally.
"""

from __future__ import annotations

import pytest

from vercel.workflow import FatalError, RemoteError, serializable
from vercel.workflow._internal import runtime, serialization as ser, world as w


def _wire(error: BaseException) -> str:
    """The devalue text an error payload carries, minus the format prefix."""
    return ser.dehydrate_error(error)[len(ser.DEVALUE_V1) :].decode()


def _round_trip(error: BaseException) -> BaseException:
    return ser.hydrate_error(ser.dehydrate_error(error), what="a payload")


def _raised(error: BaseException) -> BaseException:
    """*error* with a traceback on it, as one that was actually raised."""
    try:
        raise error
    except BaseException as raised:  # noqa: BLE001
        return raised


# ═══════════════════════════════════════════════════════════════════════════
# the wire form
# ═══════════════════════════════════════════════════════════════════════════


def test_a_fatal_error_rides_the_tag_typescript_reads() -> None:
    # `["FatalError", {message}]` -- flattened, so the parts are slots. A
    # constructed error has no traceback, so there is no `stack` beside it.
    assert _wire(FatalError("card declined")) == '[["FatalError",1],{"message":2},"card declined"]'


def test_a_cause_nests_under_its_own_tag() -> None:
    error = _raised_with_cause()

    # The cause is a slot of its own, tagged `TypeError` rather than folded into
    # the outer message -- which is what lets a JavaScript reader get a real
    # `TypeError` back out of `err.cause`.
    assert '["FatalError",1]' in _wire(error)
    assert '"cause":' in _wire(error)
    assert '["TypeError",' in _wire(error)


def test_an_error_with_no_pair_carries_its_name() -> None:
    # The catch-all tag. `name` is what keeps the identity legible on a side that
    # cannot rebuild the class.
    assert _wire(KeyError("k")) == '[["Error",1],{"name":2,"message":3},"KeyError","\'k\'"]'


def test_a_stack_is_only_there_when_there_was_one() -> None:
    assert "stack" not in _wire(FatalError("never raised"))

    wire = _wire(_raised(FatalError("raised")))
    assert '"stack"' in wire
    assert "Traceback (most recent call last)" in wire


def test_the_python_pairs_are_the_tags_javascript_would_use() -> None:
    assert _wire(TypeError("t")).startswith('[["TypeError"')
    assert _wire(SyntaxError("s")).startswith('[["SyntaxError"')
    # No Python `ReferenceError` / `RangeError`; these are the built-ins that
    # mean what JavaScript means by them.
    assert _wire(NameError("n")).startswith('[["ReferenceError"')
    assert _wire(ValueError("v")).startswith('[["RangeError"')


def test_a_tag_is_claimed_by_the_class_it_names_not_its_subclasses() -> None:
    class Narrower(ValueError):
        pass

    # Not `RangeError`: JavaScript has no class for this one, so it travels on
    # the catch-all under its own name rather than pretending to be a `ValueError`.
    assert _wire(Narrower("v")) == '[["Error",1],{"name":2,"message":3},"Narrower","v"]'


def test_a_fatal_subclass_is_still_written_as_fatal() -> None:
    class ContextViolation(FatalError):
        pass

    # The exception to the rule above, and the reason for it: fatality is what
    # the far side acts on. Upstream lands in the same place -- it matches on
    # `value.name`, which its constructor pins to `FatalError` for subclasses too.
    assert _wire(ContextViolation("nope")).startswith('[["FatalError"')


# ═══════════════════════════════════════════════════════════════════════════
# reading what TypeScript wrote
# ═══════════════════════════════════════════════════════════════════════════

# What `devalue.stringify` produces in `@workflow/core` for a `FatalError`
# carrying a `TypeError` cause -- the payload `errorStepThrowFatalRoundTrip`
# puts in a `step_failed` event. Written out rather than generated, so this
# stays an assertion about the other implementation.
TS_FATAL_WITH_CAUSE = ser.DEVALUE_V1 + (
    b'[["FatalError",1],{"message":2,"stack":3,"cause":4},"fatal with cause",'
    b'"FatalError: fatal with cause\\n    at throwFatalErrorWithCause (99_e2e.ts:1365:19)",'
    b'["TypeError",5],{"message":6,"stack":7},"underlying type error",'
    b'"TypeError: underlying type error\\n    at throwFatalErrorWithCause (99_e2e.ts:1364:16)"]'
)


def test_a_typescript_fatal_error_arrives_as_this_sdks_class() -> None:
    error = ser.hydrate_error(TS_FATAL_WITH_CAUSE, what="the error of step step_0")

    assert isinstance(error, FatalError)
    assert str(error) == "fatal with cause"
    # The cause chain, with the JavaScript `TypeError` as a Python one.
    assert isinstance(error.__cause__, TypeError)
    assert str(error.__cause__) == "underlying type error"


def test_the_writers_stack_is_kept_where_typescript_keeps_it() -> None:
    error = ser.hydrate_error(TS_FATAL_WITH_CAUSE, what="an error")

    # An attribute rather than a field, because the class is `FatalError` and
    # nothing here declares one -- the same `.stack` a JavaScript reader reads,
    # and it does not compete with the Python traceback this exception gets when
    # it is raised next.
    stack: str = error.stack  # type: ignore[attr-defined]
    assert stack.startswith("FatalError: fatal with cause\n    at ")


def test_a_class_javascript_has_and_python_does_not_keeps_its_name() -> None:
    payload = ser.DEVALUE_V1 + b'[["URIError",1],{"message":2},"bad escape"]'

    error = ser.hydrate_error(payload, what="an error")

    assert isinstance(error, RemoteError)
    assert error.name == "URIError"
    assert str(error) == "URIError: bad escape"
    # And it goes back out as what it was, so passing through a Python run does
    # not cost the next JavaScript reader anything.
    assert _wire(error) == '[["Error",1],{"name":2,"message":3},"URIError","bad escape"]'


def test_a_dom_exception_keeps_the_name_inside_its_payload() -> None:
    payload = ser.DEVALUE_V1 + b'[["DOMException",1],{"message":2,"name":3},"aborted","AbortError"]'

    error = ser.hydrate_error(payload, what="an error")

    assert isinstance(error, RemoteError)
    assert error.name == "AbortError"


def test_a_tag_from_a_feature_this_sdk_lacks_still_reads() -> None:
    # `RetryableError` is a class this SDK does not export yet. Declining the tag
    # would make `devalue.parse` throw and cost the whole payload, so it arrives
    # as what it is -- an error named `RetryableError` -- minus the `retryAfter`
    # there is nothing here to do with.
    payload = (
        ser.DEVALUE_V1 + b'[["RetryableError",1],{"message":2,"retryAfter":1770000000000},"later"]'
    )

    error = ser.hydrate_error(payload, what="an error")

    assert isinstance(error, RemoteError)
    assert error.name == "RetryableError"


def test_a_thrown_value_that_is_not_an_error_at_all_is_still_raisable() -> None:
    # JavaScript throws whatever it likes; Python has to raise an exception.
    payload = ser.dehydrate({"kind": "business-rule-violation", "code": "INVOICE_LOCKED"})

    error = ser.hydrate_error(payload, what="an error")

    assert isinstance(error, RuntimeError)
    assert error.args[0] == {"kind": "business-rule-violation", "code": "INVOICE_LOCKED"}


# ═══════════════════════════════════════════════════════════════════════════
# round trips
# ═══════════════════════════════════════════════════════════════════════════


def _raised_with_cause() -> BaseException:
    try:
        try:
            raise TypeError("underlying type error")
        except TypeError as root:
            raise FatalError("fatal with cause") from root
    except FatalError as error:
        return error


def test_a_fatal_error_and_its_cause_survive_a_round_trip() -> None:
    error = _round_trip(_raised_with_cause())

    assert isinstance(error, FatalError)
    assert str(error) == "fatal with cause"
    assert isinstance(error.__cause__, TypeError)
    assert str(error.__cause__) == "underlying type error"


@pytest.mark.parametrize(
    "error",
    [TypeError("t"), SyntaxError("s"), NameError("n"), ValueError("v"), ZeroDivisionError("z")],
    ids=lambda error: type(error).__name__,
)
def test_a_built_in_comes_back_as_itself(error: Exception) -> None:
    # The paired tags for the first four; for the rest, the catch-all's `name`
    # resolved against the built-ins -- which is what makes a Python-to-Python
    # step failure catchable by the class the step raised.
    assert type(_round_trip(error)) is type(error)


def test_an_exception_that_will_not_take_a_message_degrades_rather_than_raises() -> None:
    # `UnicodeDecodeError` needs five constructor arguments, and a payload
    # carrying one string cannot supply them. Guessing at the rest would produce
    # an exception that lies about where it came from.
    error = _round_trip(UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"))

    assert isinstance(error, RemoteError)
    assert error.name == "UnicodeDecodeError"


def test_a_registered_exception_class_outranks_the_error_tags() -> None:
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

    # `Instance` is offered before the error tags, so registering a class is how
    # an app keeps fields the error tags would flatten -- upstream orders its
    # `WORKFLOW_SERIALIZE` classes ahead of its error reducers for the same reason.
    assert '["Instance",' in _wire(PaymentDeclined("card_expired"))

    error = _round_trip(PaymentDeclined("card_expired"))
    assert isinstance(error, PaymentDeclined)
    assert error.code == "card_expired"


def test_an_error_that_cannot_be_encoded_does_not_take_the_failure_with_it() -> None:
    class Unstringable(Exception):
        def __str__(self) -> str:
            raise ValueError("nope")

    # This is the payload of the event that records a failure. If encoding it
    # raised there would be no failure left to report and the delivery would
    # loop, so the class name goes instead.
    error = _round_trip(Unstringable())

    assert isinstance(error, RemoteError)
    assert error.name == "Unstringable"
    assert "could not be serialized" in str(error)


# ═══════════════════════════════════════════════════════════════════════════
# the plaintext category
# ═══════════════════════════════════════════════════════════════════════════


def test_a_thrown_error_is_the_users() -> None:
    # What `errorRetryFatal` asserts on the run: `USER_ERROR`, not the name of
    # whichever class happened to reach the handler.
    assert runtime.classify_run_error(FatalError("card declined")) == "USER_ERROR"
    assert runtime.classify_run_error(RuntimeError("boom")) == "USER_ERROR"


def test_a_diverged_replay_and_a_broken_world_are_not() -> None:
    assert runtime.classify_run_error(runtime.NondeterminismError("diverged")) == (
        "REPLAY_DIVERGENCE"
    )
    # Including a throttle: the queue would normally have redelivered it, and it
    # only reaches a terminal classification if the run gave up.
    throttled = w.ThrottleError("slow down", status=429, retry_after=5)
    assert runtime.classify_run_error(throttled) == "WORLD_CONTRACT_ERROR"
    assert (
        runtime.classify_run_error(w.WorkflowWorldError("server error", status=500))
        == "WORLD_CONTRACT_ERROR"
    )
