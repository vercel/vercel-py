"""Custom value types on `@workflow/core`'s ``Instance`` rail.

The tag and its payload shape are a contract with the TypeScript side, not an
internal detail: `workflow inspect` renders ``["Instance", {classId, data}]``
as ``Decimal@decimal '1.50'`` through `observabilityRevivers`, and a JavaScript
peer can revive the real object by registering the same ``classId``. So the
wire form is asserted here literally, not just round-tripped.
"""

from __future__ import annotations

import datetime
import decimal
import enum
import pathlib
import uuid

import pytest

from vercel._internal import devalue
from vercel._internal.workflow import serde, serialization as ser


def _wire(value):
    """The devalue text a payload carries, minus the format prefix."""
    return ser.dehydrate(value)[len(ser.DEVALUE_V1) :].decode()


def _round_trip(value):
    return ser.hydrate(ser.dehydrate(value), what="a payload")


# ═══════════════════════════════════════════════════════════════════════════
# the wire form
# ═══════════════════════════════════════════════════════════════════════════


def test_the_tag_is_the_one_typescript_reads() -> None:
    # `["Instance", {classId, data}]` -- flattened, so the parts are slots.
    assert _wire(decimal.Decimal("1.50")) == (
        '[["Instance",1],{"classId":2,"data":3},"class//decimal//Decimal","1.50"]'
    )


def test_an_unregistered_class_is_left_to_the_other_reducers() -> None:
    # The reducer has to decline, not claim, or every value would become an
    # Instance -- devalue treats any truthy return as a match.
    assert _wire({"tier": "pro"}) == '[{"tier":1},"pro"]'
    assert _wire([1, 2]) == "[[1,2],1,2]"


def test_a_value_beside_a_custom_one_is_unaffected() -> None:
    # The property that makes this rail worth using: one exotic value does not
    # cost the payload around it, on either side.
    restored = _round_trip({"total": decimal.Decimal("1.50"), "currency": "usd"})
    assert restored == {"total": decimal.Decimal("1.50"), "currency": "usd"}


# ═══════════════════════════════════════════════════════════════════════════
# built-in registrations
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "value",
    [
        decimal.Decimal("1.50"),
        decimal.Decimal("-0.000001"),
        uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
        datetime.date(2026, 7, 30),
        datetime.time(17, 6, 33, 500000),
        datetime.timedelta(days=2, seconds=3, microseconds=4),
        pathlib.Path("/tmp/report.csv"),
    ],
)
def test_stdlib_types_round_trip(value) -> None:
    restored = _round_trip(value)
    assert restored == value
    assert type(restored) is type(value)


def test_timedelta_keeps_microseconds() -> None:
    # `total_seconds()` is a float and would round; the exact triple does not.
    value = datetime.timedelta(days=999_999, microseconds=1)
    assert _round_trip(value) == value


def test_datetime_stays_a_native_date_not_an_instance() -> None:
    """`datetime` is a `date` subclass, so the `date` registration must not
    capture it -- devalue carries it as a JS `Date`, which JavaScript can read
    without knowing anything about this SDK."""
    value = datetime.datetime(2026, 7, 30, 17, 6, 33, tzinfo=datetime.timezone.utc)

    assert _wire(value) == '[["Date","2026-07-30T17:06:33.000Z"]]'
    assert _round_trip(value) == value


@pytest.mark.parametrize("value", [(1, 2), frozenset({1}), {1, 2}, b"\\x00", "pro", 42])
def test_types_devalue_already_carries_are_left_alone(value) -> None:
    # Wrapping these would take a plain array/Set away from a JavaScript
    # reader for the sake of a Python distinction.
    assert "Instance" not in _wire(value)


# ═══════════════════════════════════════════════════════════════════════════
# registering your own
# ═══════════════════════════════════════════════════════════════════════════


class Point:
    def __init__(self, x: int, y: int) -> None:
        self.x, self.y = x, y

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Point) and (self.x, self.y) == (other.x, other.y)

    def __workflow_serialize__(self) -> dict[str, int]:
        return {"x": self.x, "y": self.y}

    @classmethod
    def __workflow_deserialize__(cls, data: dict[str, int]) -> Point:
        return cls(**data)


class Tier(enum.Enum):
    PRO = "pro"


class _Ledger:
    """A class nothing registers, for the paths that must refuse one."""


@pytest.fixture(autouse=True)
def _clean_registry():
    """Undo whatever a test registers, leaving the built-ins in place."""
    class_ids = dict(serde._by_class_id)
    classes = dict(serde._by_class)
    yield
    serde._by_class_id.clear()
    serde._by_class_id.update(class_ids)
    serde._by_class.clear()
    serde._by_class.update(classes)
    serde._resolved.clear()


def test_a_user_class_round_trips_through_the_dunder_protocol() -> None:
    serde.register_serializable(Point)

    assert _round_trip(Point(1, 2)) == Point(1, 2)
    assert f'"{serde.default_class_id(Point)}"' in _wire(Point(1, 2))


def test_the_decorator_registers_the_class_it_wraps() -> None:
    @serde.serializable
    class Tagged:
        def __workflow_serialize__(self) -> int:
            return 7

        @classmethod
        def __workflow_deserialize__(cls, data: int) -> Tagged:
            return cls()

    assert isinstance(_round_trip(Tagged()), Tagged)


def test_an_enum_needs_no_methods() -> None:
    serde.register_serializable(Tier)

    assert _round_trip(Tier.PRO) is Tier.PRO
    # The value, not the member name -- what a JavaScript reader wants to see.
    assert '"pro"' in _wire(Tier.PRO)


def test_an_unregistered_enum_says_how_to_send_it() -> None:
    with pytest.raises(ser.SerializationError, match=r"Register Tier with @serializable"):
        ser.dehydrate(Tier.PRO)
    with pytest.raises(ser.SerializationError, match=r"enum.StrEnum"):
        ser.dehydrate(Tier.PRO)


def test_an_unregistered_class_says_how_to_send_it() -> None:
    # The qualname is what you would pass to @serializable, so it is the name
    # worth printing even when it is a mouthful.
    with pytest.raises(ser.SerializationError, match=r"Register .*Ledger with @serializable"):
        ser.dehydrate(_Ledger())


def test_the_failing_field_is_named() -> None:
    with pytest.raises(ser.SerializationError, match=r"at \.book"):
        ser.dehydrate({"book": _Ledger()})


def test_a_class_id_can_be_pinned_for_a_typescript_peer() -> None:
    serde.register_serializable(Point, class_id="class//./src/geometry//Point")

    assert '"class//./src/geometry//Point"' in _wire(Point(1, 2))
    assert _round_trip(Point(1, 2)) == Point(1, 2)


def test_a_registration_covers_subclasses_by_mro() -> None:
    # How one `PurePath` registration serves `PosixPath` and `WindowsPath`.
    class Origin(Point): ...

    serde.register_serializable(Point)

    assert '"class//' in _wire(Origin(0, 0))
    # ...and reading gives the registered class back, since that is the only
    # thing the classId names.
    assert type(_round_trip(Origin(0, 0))) is Point


def _define_and_register() -> type:
    """A fresh class object under a classId that is already taken.

    What the sandbox does: it re-imports the workflow's module, so every
    decorated class in it is registered again, with a new class object each
    time and the same classId.
    """

    @serde.serializable(class_id="class//tests//Reloaded")
    class Reloaded:
        def __workflow_serialize__(self) -> int:
            return 7

        @classmethod
        def __workflow_deserialize__(cls, data: int) -> Reloaded:
            return cls()

    return Reloaded


def test_re_registering_replaces_rather_than_conflicts() -> None:
    first = _define_and_register()
    second = _define_and_register()
    assert first is not second

    # The latest definition is the live one, and the previous class object no
    # longer resolves -- there is one classId, and it names one class.
    assert type(_round_trip(second())) is second
    assert type(_round_trip(first())) is second


def test_a_class_without_the_protocol_is_refused_at_registration() -> None:
    class Bare: ...

    with pytest.raises(TypeError, match="__workflow_serialize__"):
        serde.register_serializable(Bare)

    with pytest.raises(TypeError, match="__workflow_deserialize__"):
        serde.register_serializable(Bare, serialize=str)


# ═══════════════════════════════════════════════════════════════════════════
# reading what the other side wrote
# ═══════════════════════════════════════════════════════════════════════════


def _foreign_instance(data) -> bytes:
    """A payload carrying an ``Instance`` this side never registered."""
    # Claim only the marker: a reducer that claims everything would also claim
    # the payload dict it just returned.
    reduce = {"Instance": lambda value: data if isinstance(value, _Ledger) else False}
    return ser.DEVALUE_V1 + devalue.stringify(_Ledger(), reduce).encode()


def test_an_unknown_class_id_is_refused_rather_than_guessed() -> None:
    # A JavaScript class this side has never heard of. Handing the workflow a
    # stand-in would be handing it something it was not sent.
    payload = _foreign_instance({"classId": "class//./src/models//Money", "data": "1.50"})

    with pytest.raises(ser.SerializationError, match=r"unknown class .*Money"):
        ser.hydrate(payload, what="the input of run wrun_1")


def test_corrupt_data_for_a_known_class_names_the_class() -> None:
    # `Decimal("garbage")` raises `InvalidOperation`, which is an
    # `ArithmeticError` -- not something the codec would recognize, so without
    # attribution it escapes `hydrate` raw, from several frames inside `parse`.
    payload = _foreign_instance({"classId": "class//decimal//Decimal", "data": "garbage"})

    with pytest.raises(ser.SerializationError, match=r"class//decimal//Decimal could not be read"):
        ser.hydrate(payload, what="the input of run wrun_1")


def test_a_failing_serializer_names_the_class() -> None:
    class Broken:
        def __workflow_serialize__(self) -> dict:
            raise RuntimeError("boom")

        @classmethod
        def __workflow_deserialize__(cls, data: dict) -> Broken:
            return cls()

    serde.register_serializable(Broken)

    with pytest.raises(ser.SerializationError, match=r"Broken could not be written: boom"):
        ser.dehydrate(Broken())


def test_a_malformed_instance_payload_is_refused() -> None:
    payload = _foreign_instance(["not an object"])

    with pytest.raises(ser.SerializationError, match="malformed Instance payload"):
        ser.hydrate(payload, what="a payload")
