"""The rules a run's attributes are held to, and how `LocalWorld` applies them.

The rules are `@workflow/world`'s, enforced twice on purpose: once by
`set_attributes()` so a bad call fails in the body that made it and writes
nothing, and once by the World so a write that reached it anyway cannot put a
run over a limit. The end-to-end behaviour of the authoring API lives in
`tests/integration/test_workflow_attributes.py`.
"""

from __future__ import annotations

import pytest

from vercel.workflow import FatalError, set_attributes
from vercel.workflow._internal import attributes as attrs, serialization as ser, world as w
from vercel.workflow._internal.worlds import local as local_mod

WORKFLOW_NAME = "workflow//tests.wf"


# ── the rules ──────────────────────────────────────────────────────────────


def test_a_plain_key_and_value_pass() -> None:
    attrs.validate_attribute_changes([("phase", "init"), ("source", None)])


@pytest.mark.parametrize(
    "changes,message",
    [
        ([("", "v")], "must not be empty"),
        ([("k" * 257, "v")], "key length 257 exceeds limit 256"),
        ([("note", "v" * 257)], "byte length 257 exceeds limit 256"),
        # The value cap is bytes, not characters: 200 two-byte characters.
        ([("note", "é" * 200)], "byte length 400 exceeds limit 256"),
        ([("$system", "v")], "reserved prefix"),
        ([("phase", "a"), ("phase", "b")], "appears more than once"),
        ([(f"k{i}", "v") for i in range(65)], "exceed limit 64"),
    ],
)
def test_a_broken_rule_names_itself_and_its_limit(
    changes: list[tuple[str, str | None]], message: str
) -> None:
    with pytest.raises(attrs.AttributeValidationError, match=message):
        attrs.validate_attribute_changes(changes)


def test_a_reserved_key_passes_when_the_caller_asks_for_it() -> None:
    attrs.validate_attribute_changes([("$agent.kind", "durable")], allow_reserved=True)


def test_the_cap_counts_the_post_merge_total() -> None:
    """Updating a key the run already has adds nothing, so a run at the cap can
    still be written to -- but only if the World says what it already holds."""
    at_cap = {f"k{i}": "v" for i in range(64)}

    attrs.validate_attribute_changes([("k0", "new")], existing_keys=at_cap)
    with pytest.raises(attrs.AttributeValidationError, match="exceed limit 64"):
        attrs.validate_attribute_changes([("k64", "new")], existing_keys=at_cap)
    # ...and an unset makes room for one.
    attrs.validate_attribute_changes([("k0", None), ("k64", "new")], existing_keys=at_cap)


def test_applying_changes_upserts_and_removes() -> None:
    merged = attrs.apply_attribute_changes(
        {"phase": "init", "source": "body"}, [("phase", "done"), ("source", None)]
    )

    assert merged == {"phase": "done"}


def test_applying_changes_leaves_the_input_alone() -> None:
    existing = {"phase": "init"}

    attrs.apply_attribute_changes(existing, [("phase", "done")])

    assert existing == {"phase": "init"}


# ── the authoring API's half of the rules ──────────────────────────────────


def test_an_invalid_call_raises_a_catchable_fatal_error() -> None:
    """`FatalError`, not `AttributeValidationError`: this is a mistake in the
    body, and the body is where it should be catchable -- and it is raised by
    the call rather than by the await, so nothing is written either way."""
    with pytest.raises(FatalError, match="reserved prefix"):
        set_attributes({"$system": "nope"})


def test_a_non_mapping_is_rejected_by_name() -> None:
    with pytest.raises(FatalError, match="requires a mapping, got str"):
        set_attributes("phase=init")  # type: ignore[arg-type]


def test_calling_outside_a_workflow_or_step_is_fatal() -> None:
    with pytest.raises(FatalError, match="inside a workflow or a step"):
        set_attributes({"phase": "init"})


# ── the local world ────────────────────────────────────────────────────────


def _world(tmp_path, monkeypatch) -> local_mod.LocalWorld:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    return local_mod.LocalWorld()


async def _running_run(world: local_mod.LocalWorld) -> str:
    created = await world.events_create(
        None,
        w.RunCreatedEventData(
            deploymentId="dpl_1",
            workflowName=WORKFLOW_NAME,
            input=ser.dehydrate(ser.argument_array((), {})),
        ).into_event(),
    )
    assert created.run is not None
    await world.events_create(created.run.run_id, w.RunStartedEvent())
    return created.run.run_id


def _attr_set(*changes: tuple[str, str | None], correlation_id: str | None = "attr_1") -> w.Event:
    return w.AttrSetEventData(
        changes=[w.AttributeChange(key=k, value=v) for k, v in changes],
        writer=w.WorkflowAttributeWriter(),
    ).into_event(correlation_id)


async def test_the_world_materializes_the_changes_on_the_run(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    run_id = await _running_run(world)

    await world.events_create(run_id, _attr_set(("phase", "init"), ("source", "body")))
    await world.events_create(run_id, _attr_set(("phase", "done"), correlation_id="attr_2"))
    await world.events_create(run_id, _attr_set(("source", None), correlation_id="attr_3"))

    assert (await world.runs_get(run_id)).attributes == {"phase": "done"}


async def test_a_step_write_needs_no_correlation_id(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    run_id = await _running_run(world)

    await world.events_create(
        run_id,
        w.AttrSetEventData(
            changes=[w.AttributeChange(key="phase", value="step-done")],
            writer=w.StepAttributeWriter(stepId="step_1", attempt=1),
        ).into_event(),
    )

    assert (await world.runs_get(run_id)).attributes == {"phase": "step-done"}


async def test_the_same_write_twice_is_a_conflict(tmp_path, monkeypatch) -> None:
    """One `set_attributes()` call is one event. Two replays racing to write it
    must collapse to one, or the second application would re-apply changes the
    body issued once."""
    world = _world(tmp_path, monkeypatch)
    run_id = await _running_run(world)

    await world.events_create(run_id, _attr_set(("phase", "init")))
    with pytest.raises(w.EntityConflictError, match="already exists"):
        await world.events_create(run_id, _attr_set(("phase", "init")))


async def test_a_rejected_write_leaves_its_correlation_id_free(tmp_path, monkeypatch) -> None:
    """Validation runs before the claim. Otherwise a retry of an event that was
    never written comes back as "already exists", and the body waits forever for
    an event nobody is going to write."""
    world = _world(tmp_path, monkeypatch)
    run_id = await _running_run(world)

    with pytest.raises(attrs.AttributeValidationError):
        await world.events_create(run_id, _attr_set(("$system", "nope")))
    await world.events_create(run_id, _attr_set(("phase", "init")))

    assert (await world.runs_get(run_id)).attributes == {"phase": "init"}


async def test_the_world_enforces_the_rules_too(tmp_path, monkeypatch) -> None:
    """The cap is the World's to enforce: only it knows what the run holds."""
    world = _world(tmp_path, monkeypatch)
    run_id = await _running_run(world)

    await world.events_create(
        run_id, _attr_set(*((f"k{i}", "v") for i in range(64)), correlation_id="attr_fill")
    )
    with pytest.raises(attrs.AttributeValidationError, match="exceed limit 64"):
        await world.events_create(run_id, _attr_set(("one-too-many", "v")))


async def test_a_terminal_run_takes_no_more_attributes(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)
    run_id = await _running_run(world)
    await world.events_create(
        run_id, w.RunCompletedEventData(output=ser.dehydrate(None)).into_event()
    )

    with pytest.raises(w.EntityConflictError, match="terminal state"):
        await world.events_create(run_id, _attr_set(("phase", "late")))


async def test_a_missing_run_is_not_found(tmp_path, monkeypatch) -> None:
    world = _world(tmp_path, monkeypatch)

    with pytest.raises(RuntimeError, match="not found"):
        await world.events_create("wrun_nope", _attr_set(("phase", "init")))
