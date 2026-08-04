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
import gc
import pathlib
import uuid

import pytest

from vercel._internal import devalue
from vercel._internal.workflow import py_sandbox, serde, serialization as ser


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

    def _workflow_serialize(self) -> dict[str, int]:
        return {"x": self.x, "y": self.y}

    @classmethod
    def _workflow_deserialize(cls, data: dict[str, int]) -> Point:
        return cls(**data)


class Tier(enum.Enum):
    PRO = "pro"


class _Ledger:
    """A class nothing registers, for the paths that must refuse one."""


@pytest.fixture(autouse=True)
def _clean_registry():
    """Undo whatever a test registers in the host registry."""
    host = serde._HOST
    class_ids, classes = dict(host.by_class_id), dict(host.by_class)
    yield
    host.by_class_id.clear()
    host.by_class_id.update(class_ids)
    host.by_class.clear()
    host.by_class.update(classes)
    host.resolved.clear()


def test_a_user_class_round_trips_through_the_dunder_protocol() -> None:
    serde.register_serializable(Point)

    assert _round_trip(Point(1, 2)) == Point(1, 2)
    assert f'"{serde.default_class_id(Point)}"' in _wire(Point(1, 2))


def test_the_decorator_registers_the_class_it_wraps() -> None:
    @serde.serializable
    class Tagged:
        def _workflow_serialize(self) -> int:
            return 7

        @classmethod
        def _workflow_deserialize(cls, data: int) -> Tagged:
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
        def _workflow_serialize(self) -> int:
            return 7

        @classmethod
        def _workflow_deserialize(cls, data: int) -> Reloaded:
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


def test_replays_do_not_accumulate_in_the_host_registry() -> None:
    """A replay re-imports the workflow's module, registering its classes again.

    Each import produces a new class object, so anything those registrations
    reach would pin one dead class -- and its module globals -- per replay.
    They go into the sandbox's own registry, which is dropped whole when the
    sandbox exits, so the host's does not grow at all.
    """
    classes, class_ids = len(serde._HOST.by_class), len(serde._HOST.by_class_id)

    for _ in range(50):
        with serde.sandboxed_registrations():
            _define_and_register()
    gc.collect()

    assert len(serde._HOST.by_class) == classes
    assert len(serde._HOST.by_class_id) == class_ids


def test_a_sandbox_registration_does_not_outlive_the_sandbox() -> None:
    """A replay must not leave the host deserializing into the sandbox's class.

    The sandbox re-imports the workflow's module, so the same classId is
    registered again against a different class object. Without a registry of
    its own the last one wins for the rest of the process, and an `Enum` is
    where that shows: the host would hand its caller a member that is neither
    `is` nor `==` the one the caller has.
    """
    host = _define_and_register()

    with serde.sandboxed_registrations():
        sandboxed = _define_and_register()
        assert type(_round_trip(sandboxed())) is sandboxed, "its own class applies inside"
        # The classId is the same on both sides, which is what lets a payload
        # cross: a step input is dehydrated here and revived by the host.
        crossing = ser.dehydrate(sandboxed())

    assert type(_round_trip(host())) is host
    assert type(ser.hydrate(crossing, what="a payload")) is host, "each side revives its own"


def test_a_sandbox_registration_does_not_reach_the_host() -> None:
    # Nothing carries a sandbox-registered class out: a run's payloads are all
    # serialized inside the sandbox, its return value included.
    with serde.sandboxed_registrations():
        serde.register_serializable(Point, class_id="class//sandbox-only//Point")

    with pytest.raises(ser.SerializationError, match="Register Point with @serializable"):
        ser.dehydrate(Point(1, 2))


def test_a_host_registration_is_not_visible_inside_a_sandbox() -> None:
    """The isolation this buys.

    A class the sandbox has not imported itself must not be reachable through
    the registry, because reviving one hands workflow code a host class object
    -- and through its `__globals__`, the host's module graph, which is what
    the sandbox exists to keep out of reach.
    """
    serde.register_serializable(Point)
    payload = ser.dehydrate(Point(1, 2))

    with serde.sandboxed_registrations():
        with pytest.raises(ser.SerializationError, match="unknown class"):
            ser.hydrate(payload, what="a payload")


def test_the_sandbox_registers_the_stdlib_classes_it_imported_itself() -> None:
    """A sandbox re-imports most of the stdlib types this module registers.

    Its `uuid.UUID` is a different class object, so a registry holding only the
    host's would not cover a `UUID` built inside a workflow.
    """
    with py_sandbox.workflow_sandbox():
        import uuid as sandboxed  # noqa: PLC0415

        value = sandboxed.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        assert type(value) is not uuid.UUID, "premise: the sandbox re-imported uuid"
        assert _round_trip(value) == value


def test_a_restricted_stdlib_class_is_not_swapped_for_the_hosts() -> None:
    """Reviving must build the sandbox's class, not the host's.

    `datetime.date` inside a sandbox is a restricted subclass, there to keep
    `date.today()` out of a workflow body. Handing back the host's class would
    undo that.
    """
    payload = ser.dehydrate(datetime.date(2026, 8, 4))

    with py_sandbox.workflow_sandbox():
        import datetime as sandboxed  # noqa: PLC0415

        revived = ser.hydrate(payload, what="a payload")
        assert type(revived) is sandboxed.date
        with pytest.raises(py_sandbox.SandboxRestrictionError):
            type(revived).today()


def test_the_wire_id_does_not_follow_the_sandbox_class_name() -> None:
    # Derived from the class it would read `class//...//_RestrictedDate`, which
    # the other side has never heard of.
    with py_sandbox.workflow_sandbox():
        import datetime as sandboxed  # noqa: PLC0415

        wire = _wire(sandboxed.date(2026, 8, 4))
        assert f'"{serde.CLASS_ID_PREFIX}datetime//date"' in wire
        assert "_Restricted" not in wire


def test_datetime_stays_native_inside_a_sandbox() -> None:
    # `registry.native` has to hold the sandbox's `datetime`, not the host's,
    # or the `date` registration would capture it through the MRO.
    with py_sandbox.workflow_sandbox():
        import datetime as sandboxed  # noqa: PLC0415

        value = sandboxed.datetime(2026, 7, 30, tzinfo=datetime.timezone.utc)
        assert "Instance" not in _wire(value)


def test_built_ins_are_visible_inside_a_sandbox() -> None:
    # They are registered when this module is imported, and `serde` is reached
    # through to the host rather than re-imported, so a registry that started
    # empty would not have them.
    with serde.sandboxed_registrations():
        assert _round_trip(decimal.Decimal("1.50")) == decimal.Decimal("1.50")
        assert _round_trip(pathlib.Path("/tmp/x")) == pathlib.Path("/tmp/x")


def test_a_class_without_the_protocol_is_refused_at_registration() -> None:
    class Bare: ...

    with pytest.raises(TypeError, match="_workflow_serialize"):
        serde.register_serializable(Bare)

    with pytest.raises(TypeError, match="_workflow_deserialize"):
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
        def _workflow_serialize(self) -> dict:
            raise RuntimeError("boom")

        @classmethod
        def _workflow_deserialize(cls, data: dict) -> Broken:
            return cls()

    serde.register_serializable(Broken)

    with pytest.raises(ser.SerializationError, match=r"Broken could not be written: boom"):
        ser.dehydrate(Broken())


def test_a_malformed_instance_payload_is_refused() -> None:
    payload = _foreign_instance(["not an object"])

    with pytest.raises(ser.SerializationError, match="malformed Instance payload"):
        ser.hydrate(payload, what="a payload")
