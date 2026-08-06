"""How a workflow or step call reaches the wire.

The array `@workflow/core` records has one slot per positional argument and
nowhere to name anything, so keyword arguments ride in a trailing object -- see
`serialization.argument_array`. What decides the split is the *callee's*
signature, not the caller's spelling: a parameter with a usable name is recorded
by name, and only one that cannot be named -- positional-only or `*args` -- is
recorded by position.

So `*,` is no longer load-bearing at all: ordinary and keyword-only parameters
encode alike, name-keyed, which is reorder-safe and rename-loud. `/` is what
asks for the order-keyed array a TypeScript peer writes.
"""

from __future__ import annotations

from typing import Any

import pytest

from vercel._internal.workflow import core, runtime, serialization as ser, world as w
from vercel._internal.workflow.worlds.local import LocalWorld


def _registry() -> core.Workflows:
    return core.Workflows(as_vercel_job=False)


# ═══════════════════════════════════════════════════════════════════════════
# registration takes any signature
# ═══════════════════════════════════════════════════════════════════════════


def test_every_parameter_kind_registers() -> None:
    """Nothing is rejected at registration: every kind is expressible now."""
    registry = _registry()

    @registry.step
    async def positional(amount: int, currency: str = "usd") -> int:
        return amount

    @registry.step
    async def positional_only(amount: int, /) -> int:
        return amount

    @registry.step
    async def variadic(*args: int, **fields: str) -> int:
        return len(args) + len(fields)

    @registry.step
    async def keyword_only(*, amount: int) -> int:
        return amount

    @registry.workflow
    async def nothing() -> None: ...

    assert positional.name.endswith("positional")
    assert positional_only.name.endswith("positional_only")
    assert variadic.name.endswith("variadic")
    assert keyword_only.name.endswith("keyword_only")


# ═══════════════════════════════════════════════════════════════════════════
# the signature decides the split
# ═══════════════════════════════════════════════════════════════════════════


async def _charge(amount: int, currency: str = "usd", *, tier: str = "basic") -> int:
    return amount


# Each group is every legal spelling of one call -- the same parameters bound to
# the same values -- followed by the array all of them must record.
SPELLINGS: list[tuple[list[tuple[tuple[Any, ...], dict[str, Any]]], list[Any]]] = [
    ([((21,), {}), ((), {"amount": 21})], [{"amount": 21}]),
    (
        [((21, "eur"), {}), ((21,), {"currency": "eur"}), ((), {"amount": 21, "currency": "eur"})],
        [{"amount": 21, "currency": "eur"}],
    ),
    (
        [((21,), {"tier": "pro"}), ((), {"amount": 21, "tier": "pro"})],
        [{"amount": 21, "tier": "pro"}],
    ),
]


@pytest.mark.parametrize(("spellings", "array"), SPELLINGS)
def test_the_wire_does_not_depend_on_how_the_call_was_written(spellings, array) -> None:
    """The invariant: positional-or-keyword at the *call site* never reaches the wire.

    `bind` keys `BoundArguments.arguments` by parameter name, and the split is
    recomputed from the signature -- so the array is a pure function of
    (signature, which parameters got values, those values).
    """
    step = core.Step(_charge)

    for args, kwargs in spellings:
        bound_args, bound_kwargs = step.bind_arguments(args, kwargs)
        assert ser.argument_array(bound_args, bound_kwargs) == array, f"{args=} {kwargs=}"
        assert ser.call_arguments(array, what="a call") == (list(bound_args), bound_kwargs)


def test_a_named_parameter_is_recorded_by_name_whatever_its_kind() -> None:
    """Ordinary and keyword-only parameters encode alike: reorder-safe, rename-loud."""
    step = core.Step(_charge)

    assert step.bind_arguments((21, "eur"), {"tier": "pro"}) == (
        (),
        {"amount": 21, "currency": "eur", "tier": "pro"},
    )


async def _notify(total: int, /, *rest: str, urgent: bool = False) -> int:
    return total


def test_positional_only_and_var_positional_stay_positional() -> None:
    """How a signature asks for the array a TypeScript peer writes."""
    step = core.Step(_notify)

    assert step.bind_arguments((42,), {}) == ((42,), {})
    assert ser.argument_array(*step.bind_arguments((42,), {})) == [42]
    # `*rest` extends the same array, ahead of the keyword object.
    assert step.bind_arguments((42, "now"), {"urgent": True}) == ((42, "now"), {"urgent": True})
    assert ser.argument_array(*step.bind_arguments((42, "now"), {"urgent": True})) == [
        42,
        "now",
        {"urgent": True},
    ]


def test_defaults_are_not_applied() -> None:
    """An omitted default stays off the wire, so adding one is replay-safe."""
    step = core.Step(_charge)

    assert step.bind_arguments((21,), {}) == ((), {"amount": 21})
    assert ser.step_arguments(*step.bind_arguments((21,), {})) == {"args": [{"amount": 21}]}


def test_an_arity_mistake_is_a_typeerror_naming_the_callee() -> None:
    step = core.Step(_charge)

    with pytest.raises(TypeError, match=r"_charge\(\) missing a required argument: 'amount'"):
        step.bind_arguments((), {})
    with pytest.raises(TypeError, match=r"_charge\(\) got an unexpected keyword argument 'nope'"):
        step.bind_arguments((21,), {"nope": 1})
    with pytest.raises(TypeError, match=r"_charge\(\) too many positional arguments"):
        step.bind_arguments((21, "eur", "pro"), {})


# ═══════════════════════════════════════════════════════════════════════════
# what `start()` writes
# ═══════════════════════════════════════════════════════════════════════════


async def _start(wf: core.Workflow[Any, Any], *args: Any, **kwargs: Any) -> Any:
    """Run `start()` against a real local world and return the stored run."""

    class _World(LocalWorld):
        async def queue(self, queue_name: str, message: w.QueuePayload, **kw: Any) -> str:
            return "msg_1"

    world = _World()
    w.set_world(world)
    try:
        run = await runtime.start(wf, *args, **kwargs)
        return await world.runs_get(run.run_id)
    finally:
        w.set_world(None)


async def test_start_records_the_positional_array_ts_writes(tmp_path, monkeypatch) -> None:
    """`start(wf, [123])` in the TypeScript e2e suite writes exactly this.

    A positional-only parameter is what asks for it.
    """
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    registry = _registry()

    @registry.workflow
    async def add_ten(value: int, /) -> int:
        return value + 10

    stored = await _start(add_ten, 123)

    assert stored.input == ser.dehydrate([123])
    assert ser.hydrate(stored.input, what="the input") == [123]


async def test_start_records_a_named_call_as_an_object(tmp_path, monkeypatch) -> None:
    """The other half: `start(wf, [{amount: 21}])` on the TypeScript side."""
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    registry = _registry()

    @registry.workflow
    async def checkout(*, amount: int) -> int:
        return amount

    stored = await _start(checkout, amount=21)

    assert stored.input == ser.dehydrate([{"amount": 21}])


async def test_start_normalizes_a_positional_call_on_a_named_parameter(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    registry = _registry()

    @registry.workflow
    async def add_ten(value: int) -> int:
        return value + 10

    positional = await _start(add_ten, 123)
    by_keyword = await _start(add_ten, value=123)

    assert positional.input == by_keyword.input == ser.dehydrate([{"value": 123}])


async def test_start_refuses_a_call_that_does_not_fit_the_signature(tmp_path, monkeypatch) -> None:
    """And refuses it before a run row exists, rather than after."""
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    registry = _registry()

    @registry.workflow
    async def checkout(*, amount: int) -> int:
        return amount

    with pytest.raises(TypeError, match=r"checkout\(\) too many positional arguments"):
        await runtime.start(checkout, 21)  # type: ignore[misc]


# ═══════════════════════════════════════════════════════════════════════════
# the trailing-object rule, end to end
# ═══════════════════════════════════════════════════════════════════════════


async def test_an_object_passed_positionally_survives_the_round_trip(tmp_path, monkeypatch) -> None:
    """The one call the trailing-object rule would misread.

    Only reachable through a positional-only parameter now -- a named one puts
    the dict *inside* the keyword object, where nothing is ambiguous. The encoder
    appends an empty object so the rule holds, which a JavaScript callee reads as
    one stray parameter and ignores.
    """
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    registry = _registry()

    @registry.workflow
    async def ingest(payload: dict[str, int], /) -> int:
        return len(payload)

    @registry.workflow
    async def ingest_named(payload: dict[str, int]) -> int:
        return len(payload)

    stored = await _start(ingest, {"a": 1})
    hydrated = ser.hydrate(stored.input, what="the input")

    assert hydrated == [{"a": 1}, {}]
    assert ser.call_arguments(hydrated, what="the input") == ([{"a": 1}], {})

    named = await _start(ingest_named, {"a": 1})
    assert ser.hydrate(named.input, what="the input") == [{"payload": {"a": 1}}]
