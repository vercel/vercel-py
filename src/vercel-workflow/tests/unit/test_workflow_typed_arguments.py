"""Type signatured-based encoding

`signature_codec.SignatureCodec` runs each value through a `pydantic.TypeAdapter` built
from its parameter's annotation, dumping on the way out and validating on the
way in, *inside* the devalue layer. So a model reaches the wire as the plain
dict devalue already writes, and comes back a model on the other side.

Everything here is opt-in by annotation. What is unannotated, `Any`, or beyond
pydantic's reach passes through exactly as it did before -- the tests at the
bottom are what pin that, and the byte-stability ones are what say the wire did
not move for anything already on it.
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import decimal
import enum
import pathlib
import typing
import uuid
from typing import TYPE_CHECKING, Any

import pydantic
import pytest

from tests.payloads import PLAIN_ENCODER
from vercel._internal.core.polyfills import UTC
from vercel.workflow import FatalError, TypeValidationError, register_serializable
from vercel.workflow._internal import (
    core,
    runtime,
    serde,
    serialization as ser,
    world as w,
)
from vercel.workflow._internal.worlds.local import LocalWorld

if TYPE_CHECKING:
    from tests.unit.does_not_exist import Absent  # type: ignore[import-not-found]


def _registry() -> core.Workflows:
    return core.Workflows(as_vercel_job=False)


@pytest.fixture(autouse=True)
def _clean_serde_registry():
    """Undo whatever a test registers in the host registry."""
    host = serde._HOST
    class_ids, classes = dict(host.by_class_id), dict(host.by_class)
    yield
    host.by_class_id.clear()
    host.by_class_id.update(class_ids)
    host.by_class.clear()
    host.by_class.update(classes)
    host.resolved.clear()


class Order(pydantic.BaseModel):
    sku: str
    quantity: int
    total: decimal.Decimal


@dataclasses.dataclass
class Address:
    street: str
    zip_code: str


class Tier(str, enum.Enum):
    BASIC = "basic"
    PRO = "pro"


class Opaque:
    """A class pydantic can build no schema for."""

    def __init__(self, value: int) -> None:
        self.value = value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Opaque) and other.value == self.value


def _dump_arguments(
    step: core.Step[Any, Any], args: tuple[Any, ...], kwargs: dict[str, Any]
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    return step.codec.dump_arguments(*step.bind_arguments(args, kwargs))


def _round_trip(step: core.Step[Any, Any], *args: Any, **kwargs: Any) -> Any:
    """A step call, all the way to bytes and back into the callee's arguments.

    The whole path `run_step` and `step_handler` split between them.
    """
    data = PLAIN_ENCODER.encode(ser.step_arguments(*_dump_arguments(step, args, kwargs)))
    decoded = ser.step_call_arguments(ser.hydrate(data, what="the input"), what="the input")
    return step.codec.validate_arguments(*decoded)


def _wire(step: core.Step[Any, Any], *args: Any, **kwargs: Any) -> Any:
    """What a step call records, hydrated but not yet validated."""
    data = PLAIN_ENCODER.encode(ser.step_arguments(*_dump_arguments(step, args, kwargs)))
    return ser.hydrate(data, what="the input")["args"]


# ═══════════════════════════════════════════════════════════════════════════
# what an annotation buys
# ═══════════════════════════════════════════════════════════════════════════


def test_a_model_crosses_as_a_plain_dict_and_comes_back_a_model() -> None:
    registry = _registry()

    @registry.step
    async def fulfil(order: Order) -> None: ...

    order = Order(sku="abc", quantity=2, total=decimal.Decimal("19.99"))

    # A plain object on the wire, so a JavaScript reader sees fields rather than
    # an opaque Instance -- and `Decimal` still rides the serde rail beneath.
    # Keyed by parameter name, as `_bind_arguments` records anything nameable.
    assert _wire(fulfil, order) == [
        {"order": {"sku": "abc", "quantity": 2, "total": decimal.Decimal("19.99")}}
    ]

    args, kwargs = _round_trip(fulfil, order)
    assert args == []
    assert kwargs == {"order": order}
    assert isinstance(kwargs["order"], Order)


def test_a_dataclass_crosses_the_same_way() -> None:
    registry = _registry()

    @registry.step
    async def ship(to: Address) -> None: ...

    address = Address(street="1 Main St", zip_code="12345")

    assert _wire(ship, address) == [{"to": {"street": "1 Main St", "zip_code": "12345"}}]
    _, kwargs = _round_trip(ship, address)
    assert kwargs == {"to": address}
    assert isinstance(kwargs["to"], Address)


def test_the_annotation_is_read_through_containers_and_unions() -> None:
    """The reason this is pydantic and not an `isinstance` check on the value.

    A model nested in a `list[...] | None` is exactly what a hand-rolled
    top-level rule would miss.
    """
    registry = _registry()

    @registry.step
    async def fulfil_many(orders: list[Order] | None) -> None: ...

    order = Order(sku="abc", quantity=1, total=decimal.Decimal("1.00"))

    assert _wire(fulfil_many, [order]) == [
        {"orders": [{"sku": "abc", "quantity": 1, "total": decimal.Decimal("1.00")}]}
    ]
    _, kwargs = _round_trip(fulfil_many, [order])
    assert kwargs == {"orders": [order]}
    assert isinstance(kwargs["orders"][0], Order)

    assert _round_trip(fulfil_many, None) == ([], {"orders": None})


def test_a_str_enum_comes_back_as_the_member_rather_than_a_bare_str() -> None:
    """A `StrEnum` already went out as a string; the annotation reads it back.

    A bare `enum.Enum` is unaffected -- python-mode dump keeps the member, which
    devalue still cannot write, so it still wants `@serializable`.
    """
    registry = _registry()

    @registry.step
    async def price(tier: Tier) -> None: ...

    assert _wire(price, Tier.PRO) == [{"tier": "pro"}]
    _, kwargs = _round_trip(price, Tier.PRO)
    assert kwargs == {"tier": Tier.PRO}
    assert isinstance(kwargs["tier"], Tier)


def test_a_return_annotation_works_the_same_way() -> None:
    registry = _registry()

    @registry.step
    async def quote(sku: str) -> Order:
        return Order(sku=sku, quantity=1, total=decimal.Decimal("1.00"))

    order = Order(sku="abc", quantity=1, total=decimal.Decimal("1.00"))
    output = PLAIN_ENCODER.encode(quote.codec.dump_return(order))

    assert ser.hydrate(output, what="the result") == {
        "sku": "abc",
        "quantity": 1,
        "total": decimal.Decimal("1.00"),
    }
    assert quote.codec.validate_return(ser.hydrate(output, what="the result")) == order


# ═══════════════════════════════════════════════════════════════════════════
# every parameter kind, in both directions
# ═══════════════════════════════════════════════════════════════════════════


def test_every_parameter_kind_dumps() -> None:
    registry = _registry()

    @registry.step
    async def each(a: Order, /, *rest: Order, k: Order, **extra: Order) -> None: ...

    def order(n: int) -> Order:
        return Order(sku=f"s{n}", quantity=n, total=decimal.Decimal(n))

    def as_dict(n: int) -> dict[str, Any]:
        return {"sku": f"s{n}", "quantity": n, "total": decimal.Decimal(n)}

    assert _wire(each, order(1), order(2), k=order(3), other=order(4)) == [
        as_dict(1),
        as_dict(2),
        {"k": as_dict(3), "other": as_dict(4)},
    ]


def test_a_purely_positional_array_maps_onto_the_right_parameters() -> None:
    registry = _registry()

    @registry.step
    async def each(a: Order, b: Order, *rest: Order, **extra: Order) -> None: ...

    def as_dict(n: int) -> dict[str, Any]:
        return {"sku": f"s{n}", "quantity": n, "total": decimal.Decimal(n)}

    args, kwargs = each.codec.validate_arguments(
        [as_dict(1), as_dict(2), as_dict(3)], {"other": as_dict(4)}
    )

    assert all(isinstance(each_arg, Order) for each_arg in args)
    assert [a.quantity for a in args] == [1, 2, 3]
    # `b` by position, `*rest` by overflow, an unknown key through `**extra`.
    assert isinstance(kwargs["other"], Order)


@pytest.mark.parametrize(
    ("args", "kwargs"),
    [
        ([], {}),
        ([{"not": "an order"}, 99], {}),
        ([], {"a": {"not": "an order"}, "z": 5}),
        ([{"not": "an order"}], {"a": {"not": "an order"}}),
    ],
)
def test_invalid_argument_shape_is_rejected_before_validation(
    args: list[Any], kwargs: dict[str, Any]
) -> None:
    registry = _registry()

    @registry.step
    async def one(a: Order) -> None: ...

    with pytest.raises(TypeError):
        one.codec.validate_arguments(args, kwargs)


# ═══════════════════════════════════════════════════════════════════════════
# the wire did not move
# ═══════════════════════════════════════════════════════════════════════════


IDENTITY_CASES: list[tuple[Any, Any]] = [
    (int, 21),
    (float, 1.5),
    (bool, True),
    (str, "usd"),
    (bytes, b"raw"),
    (list[int], [1, 2]),
    (set[int], {1, 2}),
    (tuple[int, int], (1, 2)),
    (dict[str, int], {"a": 1}),
    (decimal.Decimal, decimal.Decimal("1.50")),
    (uuid.UUID, uuid.UUID("00000000-0000-4000-8000-000000000000")),
    (datetime.date, datetime.date(2026, 7, 30)),
    (datetime.datetime, datetime.datetime(2026, 7, 30, tzinfo=UTC)),
    (datetime.timedelta, datetime.timedelta(days=1)),
    (pathlib.Path, pathlib.Path("/tmp/x")),
    (Any, {"a": 1}),
]


@pytest.mark.parametrize("annotation,value", IDENTITY_CASES, ids=lambda p: str(p)[:40])
def test_an_annotation_that_devalue_already_handles_records_the_same_bytes(
    annotation: Any, value: Any
) -> None:
    """The compatibility guarantee, one type at a time.

    Python-mode `dump_python` is a value-identity no-op for everything that
    crossed the wire before this layer existed, so recorded bytes -- and the
    determinism check that compares them -- do not move for a run in flight.
    """
    registry = _registry()

    async def body(value: annotation) -> None: ...  # type: ignore[valid-type]

    step = registry.step(body)

    assert PLAIN_ENCODER.encode(
        ser.step_arguments(*_dump_arguments(step, (value,), {}))
    ) == PLAIN_ENCODER.encode({"args": [{"value": value}]})


def test_an_object_argument_still_gets_the_sentinel_after_a_dump() -> None:
    """`argument_array` appends `{}` when the last positional value is an object.

    A dump is what can turn a value into one, so `argument_array` must see the
    dumped result.
    """
    registry = _registry()

    @registry.step
    async def ship(order: Order, /) -> None: ...

    order = Order(sku="abc", quantity=1, total=decimal.Decimal("1.00"))

    assert _wire(ship, order) == [
        {"sku": "abc", "quantity": 1, "total": decimal.Decimal("1.00")},
        {},
    ]
    _, kwargs = ser.step_call_arguments(
        ser.hydrate(
            PLAIN_ENCODER.encode(ser.step_arguments(*_dump_arguments(ship, (order,), {}))),
            what="the input",
        ),
        what="the input",
    )
    assert kwargs == {}


# ═══════════════════════════════════════════════════════════════════════════
# degrading to pass-through
# ═══════════════════════════════════════════════════════════════════════════


def test_an_unannotated_parameter_is_untouched() -> None:
    registry = _registry()

    @registry.step
    async def loose(payload) -> None: ...  # type: ignore[no-untyped-def]

    assert _round_trip(loose, {"a": 1}) == ([], {"payload": {"a": 1}})


def test_a_class_pydantic_cannot_schematize_is_untouched() -> None:
    """`arbitrary_types_allowed` makes it an is-instance schema, not a failure.

    Which is what leaves it to `@serializable` below, and keeps a `list[Opaque]`
    working elementwise rather than taking the whole annotation down.
    """
    register_serializable(
        Opaque,
        class_id="class//tests//Opaque",
        serialize=lambda o: o.value,
        deserialize=Opaque,
    )
    registry = _registry()

    @registry.step
    async def handle(thing: Opaque) -> None: ...

    assert _round_trip(handle, Opaque(3)) == ([], {"thing": Opaque(3)})


def test_annotations_that_do_not_resolve_degrade_the_whole_signature() -> None:
    """A `TYPE_CHECKING`-only name leaves the codec no worse than unannotated."""
    registry = _registry()

    @registry.step
    async def broken(absent: Absent, order: Order) -> None: ...

    # Not even `order`, which would resolve on its own: `get_type_hints` is
    # all-or-nothing, and this is the honest report of that.
    assert broken.codec._resolved_hints() == {}
    order = Order(sku="abc", quantity=1, total=decimal.Decimal("1.00"))
    assert broken.codec.dump("order", order) is order


def test_a_name_that_never_resolves_passes_through_rather_than_erroring_late() -> None:
    """The failure mode pydantic defers to the first `validate_python`.

    `TypeAdapter` accepts an annotation naming something undefined and only
    complains when used, with a `PydanticUserError` -- not a `ValidationError`,
    so nothing here would have caught it. Declining up front is what keeps an
    unreadable annotation to the pass-through this module promises everywhere
    else.

    `list["Order"]` is the same shape on Python 3.10, where `get_type_hints`
    leaves the inner string alone; 3.11 resolves it and the adapter is real.
    Either way no `PydanticUserError` reaches the caller.
    """
    registry = _registry()

    @registry.step
    async def dangling(item: Dangling) -> None: ...

    @registry.step
    async def nested(items: list[Order]) -> None: ...

    assert dangling.codec._codec("item")._adapter is None
    assert dangling.codec.validate_arguments([], {"item": {"whatever": 1}}) == (
        [],
        {"item": {"whatever": 1}},
    )

    args, kwargs = nested.codec.validate_arguments([], {"items": [_ORDER_DICT]})
    assert args == []
    if nested.codec._codec("items")._adapter is None:
        assert kwargs == {"items": [_ORDER_DICT]}  # 3.10: unresolved, passed through
    else:
        assert kwargs == {"items": [Order.model_validate(_ORDER_DICT)]}


class Dangling(pydantic.BaseModel):
    # Names a class that does not exist, so the adapter never completes.
    nope: NeverDefined  # type: ignore[name-defined]  # noqa: F821


_ORDER_DICT = {"sku": "abc", "quantity": 1, "total": decimal.Decimal("1.00")}


def test_a_type_var_is_left_to_pydantic() -> None:
    """An unbound one reads as `Any`, which is the pass-through either way.

    A bound one is why this is not worth special-casing: pydantic enforces the
    bound, and declining the annotation would quietly drop that.
    """
    registry = _registry()

    @registry.step
    async def unbound(item: _T) -> None: ...

    @registry.step
    async def bounded(item: _Bounded) -> None: ...

    assert unbound.codec.validate_arguments([], {"item": "anything"}) == ([], {"item": "anything"})

    assert bounded.codec.validate_arguments([], {"item": 5}) == ([], {"item": 5})
    with pytest.raises(TypeValidationError):
        bounded.codec.validate_arguments([], {"item": "not an int"})


_T = typing.TypeVar("_T")
_Bounded = typing.TypeVar("_Bounded", bound=int)


def test_the_codec_resolves_late_enough_for_a_forward_reference() -> None:
    """`@registry.step` runs mid-import, so hints cannot be resolved there.

    This is the shape that breaks the moment anyone makes the codec eager.
    """
    registry = _registry()

    @registry.step
    async def later(item: Later) -> None: ...

    assert later.codec.dump("item", Later(n=7)) == {"n": 7}


class Later(pydantic.BaseModel):
    n: int


def test_a_mismatched_value_goes_out_unchanged_and_unremarked(recwarn) -> None:  # type: ignore[no-untyped-def]
    """Reporting a mismatch is the receiving side's job.

    pydantic would warn here; the per-call `warnings=False` is what keeps that
    off the user's console, and off the global warnings filter.
    """
    registry = _registry()

    @registry.step
    async def fulfil(order: Order) -> None: ...

    assert fulfil.codec.dump("order", "not an order") == "not an order"
    assert [str(each.message) for each in recwarn.list] == []


def test_a_dump_failure_propagates(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    registry = _registry()

    @registry.step
    async def fulfil(order: Order) -> None: ...

    adapter = fulfil.codec._codec("order")._adapter
    assert adapter is not None

    def fail(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("cannot serialize")

    monkeypatch.setattr(adapter, "dump_python", fail)

    with pytest.raises(RuntimeError, match="cannot serialize"):
        fulfil.codec.dump("order", Order(sku="abc", quantity=1, total=decimal.Decimal(1)))


# ═══════════════════════════════════════════════════════════════════════════
# a mismatch on the way in
# ═══════════════════════════════════════════════════════════════════════════


def test_a_value_that_does_not_match_names_the_callee_and_the_parameter() -> None:
    registry = _registry()

    @registry.step
    async def fulfil(order: Order) -> None: ...

    with pytest.raises(TypeValidationError) as ei:
        fulfil.codec.validate_arguments([], {"order": {"sku": "abc"}})

    assert "fulfil" in str(ei.value)
    assert "'order'" in str(ei.value)
    assert isinstance(ei.value.__cause__, pydantic.ValidationError)


def test_a_mismatch_is_fatal_rather_than_retryable() -> None:
    """The bytes a retry would replay are the ones that did not match.

    Distinct from a `pydantic.ValidationError` a step body raises itself -- that
    one describes a peer that might behave next time, and keeps its retries.
    """
    assert issubclass(TypeValidationError, FatalError)
    assert not issubclass(TypeValidationError, pydantic.ValidationError)


# ═══════════════════════════════════════════════════════════════════════════
# through `start()`
# ═══════════════════════════════════════════════════════════════════════════


async def test_start_records_a_model_argument_as_an_object(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    registry = _registry()

    @registry.workflow
    async def checkout(*, order: Order) -> Order:
        return order

    class _World(LocalWorld):
        async def queue(self, queue_name: str, message: w.QueuePayload, **kw: Any) -> str:
            return "msg_1"

    world = _World()
    w.set_world(world)
    try:
        order = Order(sku="abc", quantity=2, total=decimal.Decimal("19.99"))
        run = await runtime.start(checkout, order=order)
        stored = await world.runs_get(run.run_id)
    finally:
        w.set_world(None)

    assert stored.input == PLAIN_ENCODER.encode(
        [{"order": {"sku": "abc", "quantity": 2, "total": decimal.Decimal("19.99")}}]
    )


async def test_return_value_validates_against_the_workflow_return(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    registry = _registry()

    @registry.workflow
    async def checkout() -> Order:
        return Order(sku="abc", quantity=1, total=decimal.Decimal("1.00"))

    class _World(LocalWorld):
        async def queue(self, queue_name: str, message: w.QueuePayload, **kw: Any) -> str:
            return "msg_1"

    world = _World()
    w.set_world(world)
    try:
        run = await runtime.start(checkout)
        await world.events_create(
            run.run_id,
            w.RunCompletedEventData(
                output=PLAIN_ENCODER.encode(
                    {"sku": "abc", "quantity": 1, "total": decimal.Decimal("1.00")}
                )
            ).into_event(),
        )
        result = await run.return_value()
    finally:
        w.set_world(None)

    assert result == Order(sku="abc", quantity=1, total=decimal.Decimal("1.00"))


async def test_a_run_built_without_a_workflow_reads_the_raw_output(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """`Run(run_id)` has no signature to validate against, and says so by not.

    A run id picked up from a webhook is the case; `start()` is the one that
    knows.
    """
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    registry = _registry()

    @registry.workflow
    async def checkout() -> Order:
        return Order(sku="abc", quantity=1, total=decimal.Decimal("1.00"))

    class _World(LocalWorld):
        async def queue(self, queue_name: str, message: w.QueuePayload, **kw: Any) -> str:
            return "msg_1"

    world = _World()
    w.set_world(world)
    try:
        started = await runtime.start(checkout)
        await world.events_create(
            started.run_id,
            w.RunCompletedEventData(
                output=PLAIN_ENCODER.encode(
                    {"sku": "abc", "quantity": 1, "total": decimal.Decimal("1.00")}
                )
            ).into_event(),
        )
        result = await runtime.Run[Any](started.run_id).return_value()
    finally:
        w.set_world(None)

    assert result == {"sku": "abc", "quantity": 1, "total": decimal.Decimal("1.00")}


# ═══════════════════════════════════════════════════════════════════════════
# the whole run, sandbox included
# ═══════════════════════════════════════════════════════════════════════════

# Module level, because replay re-imports the workflow's defining module inside
# the sandbox -- which is also what makes this the test that proves the codec
# works there: the `Order` the body sees is the sandbox's re-imported class, and
# the adapters are built against it.
e2e = core.Workflows(as_vercel_job=False)


@e2e.step
async def price(order: Order) -> Order:
    return order.model_copy(update={"total": order.total * order.quantity})


@e2e.workflow
async def checkout_e2e(order: Order) -> Order:
    return await price(order)


async def test_a_model_survives_a_real_run_through_the_sandbox(  # type: ignore[no-untyped-def]
    tmp_path, monkeypatch, isolated_subscriptions: None
) -> None:
    from vercel.workflow._internal.worlds import local as local_mod

    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = local_mod.LocalWorld()
    w.set_world(world)
    runtime.workflow_entrypoint(e2e)

    try:
        run = await runtime.start(
            checkout_e2e, Order(sku="abc", quantity=3, total=decimal.Decimal("1.50"))
        )
        result = await asyncio.wait_for(run.return_value(), 30)
        stored = await world.runs_get(run.run_id)
    finally:
        # Closed here rather than in a teardown: the embedded service's cancel
        # scope has to be exited from the task that entered it.
        await world.aclose()
        w.set_world(None)

    assert result == Order(sku="abc", quantity=3, total=decimal.Decimal("4.50"))
    assert isinstance(result, Order)
    # Plain objects on the wire the whole way, with `Decimal` on the serde rail.
    assert ser.hydrate(stored.input, what="the input") == [
        {"order": {"sku": "abc", "quantity": 3, "total": decimal.Decimal("1.50")}}
    ]
    assert ser.hydrate(stored.output, what="the output") == {
        "sku": "abc",
        "quantity": 3,
        "total": decimal.Decimal("4.50"),
    }


def test_validating_a_concrete_container_does_not_preserve_shared_references() -> None:
    """devalue carries aliasing; `validate_python` rebuilds and so drops it.

    Annotating a parameter with a concrete container or model type opts out of
    the sharing. `Any` and an unannotated parameter keep it.
    """
    registry = _registry()

    @registry.step
    async def concrete(rows: list[dict[str, int]]) -> None: ...

    @registry.step
    async def loose(rows: Any) -> None: ...

    shared = {"a": 1}

    _, concrete_kwargs = _round_trip(concrete, [shared, shared])
    assert concrete_kwargs["rows"][0] is not concrete_kwargs["rows"][1]

    _, loose_kwargs = _round_trip(loose, [shared, shared])
    assert loose_kwargs["rows"][0] is loose_kwargs["rows"][1]


def test_adapters_are_built_once_per_instance_and_not_shared() -> None:
    """Per instance, because the sandbox's classes must die with the run."""
    registry = _registry()

    @registry.step
    async def fulfil(order: Order) -> None: ...

    first = fulfil.codec._codec("order")
    assert fulfil.codec._codec("order") is first

    other = core.Step(fulfil.func)
    assert other.codec._codec("order") is not first


def test_binding_without_a_codec_only_canonicalizes_the_call() -> None:
    signature = core.inspect.signature(lambda a: a)
    assert core._bind_arguments(signature, "f", (1,), {}) == ((), {"a": 1})
