"""Public Run handle contracts, including durable accessors across replay."""

from __future__ import annotations

import asyncio
import decimal
import hashlib
import inspect
from dataclasses import FrozenInstanceError, dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, get_type_hints
from unittest.mock import AsyncMock, Mock

import pydantic
import pytest

from tests.payloads import PLAIN_ENCODER
from vercel.workflow import (
    Configuration,
    ConfiguredWorkflow,
    Run,
    WorkflowRunFailedError,
    WorkflowRunStatus,
    get_run,
    serializable,
    start,
)
from vercel.workflow._internal import (
    core,
    py_sandbox,
    run as run_mod,
    runtime,
    serialization as ser,
    world as w,
)
from vercel.workflow._internal.worlds.local import LocalWorld


def _registry() -> core.Workflows:
    return core.Workflows(as_vercel_job=False)


class Order(pydantic.BaseModel):
    sku: str
    quantity: int
    total: decimal.Decimal


@dataclass
class Result:
    value: int


@pytest.mark.parametrize(
    ("annotation", "value", "expected"),
    [
        (Result, {"value": 42}, Result(42)),
        (list[Result], [{"value": 42}], [Result(42)]),
        (Result | None, None, None),
        (type(None), None, None),
        (Any, {"value": 42}, {"value": 42}),
    ],
)
async def test_get_run_validates_return_value(
    monkeypatch: pytest.MonkeyPatch, annotation: Any, value: Any, expected: Any
) -> None:
    read = AsyncMock(return_value=run_mod.ReturnValueResult("completed", value))
    monkeypatch.setattr(run_mod, "_get_return_value", read)

    run = get_run("run_123", type=annotation)
    assert run.run_id == "run_123"
    read.assert_not_called()
    assert await run.return_value() == expected


@pytest.mark.parametrize("status", ["failed", "cancelled"])
async def test_get_run_preserves_run_errors(monkeypatch: pytest.MonkeyPatch, status: Any) -> None:
    read = AsyncMock(return_value=run_mod.ReturnValueResult(status, "failure"))
    monkeypatch.setattr(run_mod, "_get_return_value", read)

    error = RuntimeError if status == "cancelled" else WorkflowRunFailedError
    message = "workflow cancelled" if status == "cancelled" else "failure"
    with pytest.raises(error, match=message):
        await get_run("run_123", type=Result).return_value()


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
        run = await start(checkout)
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
        started = await start(checkout)
        await world.events_create(
            started.run_id,
            w.RunCompletedEventData(
                output=PLAIN_ENCODER.encode(
                    {"sku": "abc", "quantity": 1, "total": decimal.Decimal("1.00")}
                )
            ).into_event(),
        )
        result = await Run[Any](started.run_id).return_value()
    finally:
        w.set_world(None)

    assert result == {"sku": "abc", "quantity": 1, "total": decimal.Decimal("1.00")}


@pytest.mark.parametrize("reason", [None, "", "superseded by newer run", "x" * 512, "😀" * 256])
async def test_terminate_writes_run_cancelled_event(monkeypatch, reason) -> None:
    get_world = Mock()
    monkeypatch.setattr(w, "get_world", get_world)
    run = Run[None]("wrun_test")
    get_world.assert_not_called()
    world = Mock(spec=w.World)
    get_world.return_value = world

    await run.terminate(reason=reason)

    get_world.assert_called_once_with()
    world.events_create.assert_awaited_once()
    run_id, event = world.events_create.call_args.args
    expected = {"eventType": "run_cancelled", "specVersion": w.SPEC_VERSION_CURRENT}
    if reason is not None:
        expected["eventData"] = {"cancelReason": reason}
    assert run_id == "wrun_test"
    assert event.model_dump() == expected
    assert w.EventAdaptor.from_wire(expected).model_dump() == expected
    assert event.payloads() == ()  # The reason is plaintext metadata.
    assert len(world.mock_calls) == 1


@pytest.mark.parametrize("reason", ["x" * 513, "😀" * 257, "😀" * 256 + "x"])
async def test_terminate_rejects_oversized_reason(monkeypatch, reason) -> None:
    world = Mock(spec=w.World)
    monkeypatch.setattr(w, "get_world", lambda: world)
    terminate_step = AsyncMock()
    monkeypatch.setattr(run_mod, "_terminate", terminate_step)
    with pytest.raises(ValueError, match="512 UTF-16 code units"):
        await Run("wrun_test").terminate(reason=reason)
    terminate_step.assert_not_called()
    world.events_create.assert_not_called()


@pytest.mark.parametrize("reason", ["x" * 513, "😀" * 257, "😀" * 256 + "x"])
def test_cancelled_event_rejects_oversized_stored_reason(reason) -> None:
    # The shared event schema applies the same UTF-16 limit on reads and writes.
    with pytest.raises(pydantic.ValidationError, match="512 UTF-16 code units"):
        w.EventAdaptor.from_wire(
            {"eventType": "run_cancelled", "eventData": {"cancelReason": reason}}
        )


async def test_terminate_propagates_world_error(monkeypatch) -> None:
    world = Mock(spec=w.World)
    error = w.EntityConflictError("Run already completed")
    world.events_create = AsyncMock(side_effect=error)
    monkeypatch.setattr(w, "get_world", lambda: world)
    with pytest.raises(w.EntityConflictError) as caught:
        await Run("wrun_test").terminate()
    assert caught.value is error


@pytest.mark.parametrize("running", [False, True])
async def test_terminate_persists_status_reason_and_removes_hooks(tmp_path, monkeypatch, running):
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = LocalWorld()
    monkeypatch.setattr(w, "get_world", lambda: world)
    created = await world.events_create(
        None,
        w.RunCreatedEventData(
            deployment_id="dpl_test",
            workflow_name="workflow//tests.terminate",
            input=PLAIN_ENCODER.encode([]),
        ).into_event(),
    )
    assert created.run is not None
    run = Run[None](created.run.run_id)
    if running:
        await world.events_create(run.run_id, w.RunStartedEvent())
        await world.events_create(
            run.run_id, w.HookCreatedEventData(token="cancel-hook").into_event("hook_test")
        )

    await run.terminate(reason="operator terminated")

    assert await run.status() == "cancelled"
    stored = await world.runs_get(run.run_id)
    assert stored.completed_at is not None
    events = await world.events_list(run.run_id)
    cancelled = [event for event in events.data if isinstance(event, w.RunCancelledEvent)]
    assert len(cancelled) == 1
    assert cancelled[0].event_data is not None
    assert cancelled[0].event_data.cancel_reason == "operator terminated"
    if running:
        with pytest.raises(w.HookNotFoundError):
            await world.hooks_get_by_token("cancel-hook")
    with pytest.raises(RuntimeError, match="workflow cancelled"):
        await run.return_value()

    await run.terminate()
    assert await run.status() == "cancelled"


registry = core.Workflows(as_vercel_job=False, namespace="accessors")


def test_workflow_run_status_is_shared_and_public() -> None:
    assert WorkflowRunStatus is w.WorkflowRunStatus
    assert get_type_hints(Run.status)["return"] == WorkflowRunStatus
    assert get_type_hints(run_mod._get_status.func)["return"] == WorkflowRunStatus


class AccessorOrder(pydantic.BaseModel):
    quantity: int


@serializable
class Receipt:
    def __init__(self, quantity: int) -> None:
        self.quantity = quantity

    def _workflow_serialize(self) -> dict[str, int]:
        return {"quantity": self.quantity}

    @classmethod
    def _workflow_deserialize(cls, value: dict[str, int]) -> Receipt:
        return cls(**value)


class Claim(core.BaseHook, pydantic.BaseModel):
    pass


@registry.workflow
async def target() -> AccessorOrder:
    return AccessorOrder(quantity=7)


@registry.step
async def checkpoint() -> None:
    pass


@registry.workflow
async def observe(run_id: str, operation: str) -> list[Any]:
    run = Run[Any](run_id)
    first = await getattr(run, operation)()
    await checkpoint()
    second = await getattr(run, operation)()
    return [first, second]


@registry.workflow
async def terminate_conflict() -> str:
    owner = await Claim.wait(token="shared-token").get_conflict()
    assert owner is not None
    await owner.terminate(reason="replaced")
    await checkpoint()
    return owner.run_id


@registry.workflow
async def read_result(run_id: str, *, typed: bool = False) -> Any:
    validator = target.codec.validate_return if typed else None
    run = Run[Any](run_id, output_validator=validator)
    try:
        result = await run.return_value()
    except WorkflowRunFailedError as error:
        result = {
            "run_id": error.run_id,
            "error_code": error.error_code,
            "error_type": type(error.error).__name__,
            "message": str(error.error),
            "cause_is_error": error.__cause__ is error.error,
        }
    except RuntimeError as error:
        result = str(error)
    if typed:
        # This is the sandbox's class, not the host's AccessorOrder class.
        assert isinstance(result, AccessorOrder)
        result = result.quantity
    await checkpoint()
    return result


@registry.workflow
async def inspect_payload(run_id: str, kind: str) -> str:
    value = await Run[Any](run_id).return_value()
    if kind == "custom":
        assert isinstance(value, Receipt)
        assert value.quantity == 7
    elif kind == "cycle":
        assert value[0] is value
    elif kind == "shared":
        assert value[0] is value[1]
    else:
        assert value is None
    await checkpoint()
    return "preserved"


@registry.step
async def access_from_step(run_id: str) -> list[Any]:
    run = Run[Any](run_id)
    status = await run.status()
    attributes = await run.attributes()
    value = await run.return_value()
    # A terminal run rejects termination; the direct call still exposes the
    # World's error to the user step, without a nested step retry boundary.
    try:
        await run.terminate(reason="already finished")
    except w.EntityConflictError:
        pass
    return [status, attributes, value]


@registry.workflow
async def call_user_step(run_id: str) -> list[Any]:
    return await access_from_step(run_id)


class RecordingWorld(LocalWorld):
    def __init__(self, *, encrypted: bool = False) -> None:
        super().__init__()
        self.encrypted = encrypted
        self.target_id: str | None = None
        self.target_reads = 0
        self.terminations = 0
        self.queued: list[w.WorkflowInvokePayload] = []

    async def run_key(self, run_id: str, *, deployment_id: str | None = None) -> bytes | None:
        return hashlib.sha256(run_id.encode()).digest() if self.encrypted else None

    async def runs_get(self, run_id: str) -> w.WorkflowRun:
        if run_id == self.target_id:
            assert not py_sandbox.in_sandbox(), "World read escaped the step boundary"
            self.target_reads += 1
        return await super().runs_get(run_id)

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        if run_id == self.target_id and isinstance(data, w.RunCancelledEvent):
            assert not py_sandbox.in_sandbox()
            self.terminations += 1
        return await super().events_create(run_id, data)

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs: Any) -> str:
        assert isinstance(message, w.WorkflowInvokePayload)
        self.queued.append(message)
        return "msg_test"


@pytest.fixture
def world(tmp_path, monkeypatch):
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    instance = RecordingWorld()
    w.set_world(instance)
    try:
        yield instance
    finally:
        w.set_world(None)


async def _target(world: RecordingWorld) -> Run[AccessorOrder]:
    run = await start(target)
    world.target_id = run.run_id
    world.queued.clear()
    return run


async def _complete(world: RecordingWorld, run_id: str, value: Any) -> None:
    encoder = ser.PayloadEncoder(encryption_key=await world.run_key(run_id))
    await world.events_create(
        run_id, w.RunCompletedEventData(output=encoder.encode(value)).into_event()
    )


async def _invoke(
    run: Run[Any],
    workflow: core.Workflow[Any, Any],
    *,
    payload: w.WorkflowInvokePayload | None = None,
    worker: core.Workflows = registry,
) -> None:
    await runtime.workflow_handler(
        (payload or w.WorkflowInvokePayload(run_id=run.run_id)).model_dump(by_alias=True),
        attempt=1,
        queue_name=w.get_queue_name(workflow.workflow_id, registry.namespace),
        message_id="msg_test",
        registry=worker,
        namespace=registry.namespace,
    )


def _step(world: RecordingWorld, name: str) -> w.WorkflowInvokePayload:
    matches = [payload for payload in world.queued if payload.step_name == name]
    assert matches, f"step {name} was not queued"
    world.queued.clear()
    return matches[-1]


async def _finish_checkpoint(world: RecordingWorld, run: Run[Any], workflow: core.Workflow) -> None:
    await _invoke(run, workflow, payload=_step(world, checkpoint.name))
    await _invoke(run, workflow)


async def _result(run: Run[Any]) -> Any:
    assert await run.status() == "completed"
    return await run.return_value()


@pytest.mark.parametrize("operation", ["status", "attributes"])
async def test_observations_replay_but_new_calls_read_again(world, operation) -> None:
    owner = await _target(world)
    reader = await start(observe, owner.run_id, operation)
    await _invoke(reader, observe)
    assert world.target_reads == 0

    # A fresh worker registry knows the SDK step without user registration.
    worker = core.Workflows(as_vercel_job=False, namespace="accessors")
    await _invoke(
        reader,
        observe,
        payload=_step(world, getattr(run_mod, f"_get_{operation}").name),
        worker=worker,
    )
    assert world.target_reads == 1
    await world.events_create(
        owner.run_id,
        w.AttrSetEventData(
            changes=[w.AttributeChange(key="phase", value="done")],
            writer=w.WorkflowAttributeWriter(),
        ).into_event(),
    )
    await _complete(world, owner.run_id, {"quantity": 7})

    await _invoke(reader, observe)
    await _invoke(reader, observe)
    assert world.target_reads == 1
    await _finish_checkpoint(world, reader, observe)
    await _invoke(
        reader,
        observe,
        payload=_step(world, getattr(run_mod, f"_get_{operation}").name),
    )
    await _invoke(reader, observe)
    assert world.target_reads == 2
    expected = ["pending", "completed"] if operation == "status" else [{}, {"phase": "done"}]
    assert await _result(reader) == expected


async def test_conflict_handle_termination_is_not_repeated_on_replay(world) -> None:
    owner = await _target(world)
    await world.events_create(
        owner.run_id,
        w.HookCreatedEventData(token="shared-token").into_event("hook_owner"),
    )
    reader = await start(terminate_conflict)
    await _invoke(reader, terminate_conflict)
    assert world.terminations == 0
    payload = _step(world, run_mod._terminate.name)
    await _invoke(reader, terminate_conflict, payload=payload)
    # A duplicate queue delivery is also handled by ordinary step deduplication.
    await _invoke(reader, terminate_conflict, payload=payload)
    await _invoke(reader, terminate_conflict)
    await _invoke(reader, terminate_conflict)
    await _finish_checkpoint(world, reader, terminate_conflict)
    assert await _result(reader) == owner.run_id
    assert world.terminations == 1
    assert await owner.status() == "cancelled"
    events = (await world.events_list(owner.run_id)).data
    cancelled = [event for event in events if isinstance(event, w.RunCancelledEvent)]
    assert cancelled[0].event_data is not None
    assert cancelled[0].event_data.cancel_reason == "replaced"


@pytest.mark.parametrize("encrypted", [False, True])
@pytest.mark.parametrize("outcome", ["raw", "typed", "failed", "cancelled"])
async def test_return_value_preserves_outcomes_through_replay(world, encrypted, outcome) -> None:
    world.encrypted = encrypted
    owner = await _target(world)
    encoder = ser.PayloadEncoder(encryption_key=await world.run_key(owner.run_id))
    expected: Any
    if outcome == "failed":
        await world.events_create(
            owner.run_id,
            w.RunFailedEventData(
                error=encoder.encode_error(ValueError("bad order")), error_code="USER_ERROR"
            ).into_event(),
        )
        expected = {
            "run_id": owner.run_id,
            "error_code": "USER_ERROR",
            "error_type": "ValueError",
            "message": "bad order",
            "cause_is_error": True,
        }
    elif outcome == "cancelled":
        await owner.terminate()
        expected = "workflow cancelled"
    else:
        await _complete(world, owner.run_id, {"quantity": 7})
        expected = 7 if outcome == "typed" else {"quantity": 7}

    reader = await start(read_result, owner.run_id, typed=outcome == "typed")
    await _invoke(reader, read_result)
    await _invoke(reader, read_result, payload=_step(world, run_mod._get_return_value.name))
    assert world.target_reads == 1
    await _invoke(reader, read_result)
    await _invoke(reader, read_result)
    await _finish_checkpoint(world, reader, read_result)
    assert world.target_reads == 1
    assert await _result(reader) == expected
    events = (await world.events_list(reader.run_id)).data
    assert not any(isinstance(event, w.StepFailedEvent | w.StepRetryingEvent) for event in events)
    result_event = next(event for event in events if isinstance(event, w.StepCompletedEvent))
    assert ser.is_encrypted(result_event.event_data.result) == encrypted


async def test_accessors_inside_a_user_step_do_not_schedule_nested_steps(world) -> None:
    owner = await _target(world)
    await _complete(world, owner.run_id, {"quantity": 7})
    reader = await start(call_user_step, owner.run_id)
    await _invoke(reader, call_user_step)
    await _invoke(reader, call_user_step, payload=_step(world, access_from_step.name))
    await _invoke(reader, call_user_step)
    assert await _result(reader) == ["completed", {}, {"quantity": 7}]
    assert world.terminations == 1
    events = (await world.events_list(reader.run_id)).data
    assert sum(isinstance(event, w.StepCreatedEvent) for event in events) == 1


async def test_return_value_polls_inside_one_step(world, monkeypatch) -> None:
    owner = await _target(world)
    reader = await start(read_result, owner.run_id)
    await _invoke(reader, read_result)

    async def finish_on_sleep(delay: float) -> None:
        assert delay == 1
        await _complete(world, owner.run_id, {"quantity": 7})

    sleep = AsyncMock(side_effect=finish_on_sleep)
    # Only while the SDK step runs; do not affect the workflow loop or worker.
    with monkeypatch.context() as patch:
        patch.setattr(run_mod.asyncio, "sleep", sleep)
        await _invoke(reader, read_result, payload=_step(world, run_mod._get_return_value.name))
    sleep.assert_awaited_once_with(1)
    assert world.target_reads == 2
    await _invoke(reader, read_result)
    await _finish_checkpoint(world, reader, read_result)
    assert await _result(reader) == {"quantity": 7}


@pytest.mark.parametrize("failed", [False, True])
async def test_polling_and_decryption_keep_one_world_but_later_calls_use_the_current_one(
    world, monkeypatch, failed
) -> None:
    world.encrypted = True
    owner = await _target(world)
    replacement = Mock(spec=w.World)
    replacement.runs_get.return_value = await world.runs_get(owner.run_id)

    async def finish_and_switch_world(delay: float) -> None:
        assert delay == 1
        if failed:
            encoder = ser.PayloadEncoder(encryption_key=await world.run_key(owner.run_id))
            await world.events_create(
                owner.run_id,
                w.RunFailedEventData(
                    error=encoder.encode_error(ValueError("bad order"))
                ).into_event(),
            )
        else:
            await _complete(world, owner.run_id, {"quantity": 7})
        w.set_world(replacement)

    monkeypatch.setattr(run_mod.asyncio, "sleep", finish_and_switch_world)
    if failed:
        with pytest.raises(WorkflowRunFailedError) as caught:
            await owner.return_value()
        assert isinstance(caught.value.error, ValueError)
        assert str(caught.value.error) == "bad order"
    else:
        assert await owner.return_value() == AccessorOrder(quantity=7)
    assert world.target_reads == 3  # Initial snapshot and two polls.
    replacement.runs_get.assert_not_awaited()
    replacement.run_key.assert_not_awaited()

    assert await owner.status() == "pending"
    replacement.runs_get.assert_awaited_once_with(owner.run_id)


async def test_return_value_retries_a_failed_world_read(world, monkeypatch) -> None:
    owner = await _target(world)
    await _complete(world, owner.run_id, {"quantity": 7})
    reader = await start(read_result, owner.run_id)
    await _invoke(reader, read_result)
    payload = _step(world, run_mod._get_return_value.name)
    runs_get = world.runs_get
    fail_once = True

    async def flaky_read(run_id: str) -> w.WorkflowRun:
        nonlocal fail_once
        if run_id == owner.run_id and fail_once:
            fail_once = False
            raise w.WorkflowWorldError("temporarily unavailable", status=503)
        return await runs_get(run_id)

    monkeypatch.setattr(world, "runs_get", flaky_read)
    await _invoke(reader, read_result, payload=payload)
    events = (await world.events_list(reader.run_id)).data
    assert sum(isinstance(event, w.StepRetryingEvent) for event in events) == 1
    assert not any(isinstance(event, w.StepCompletedEvent) for event in events)

    await _invoke(reader, read_result, payload=payload)
    await _invoke(reader, read_result)
    await _finish_checkpoint(world, reader, read_result)
    assert await _result(reader) == {"quantity": 7}
    assert payload.step_id is not None
    assert (await world.steps_get(reader.run_id, payload.step_id)).attempt == 2


@pytest.mark.parametrize(
    "kind",
    [
        "custom",
        pytest.param(
            "cycle",
            marks=pytest.mark.xfail(reason="Pydantic dump does not preserve cyclic references"),
        ),
        pytest.param(
            "shared",
            marks=pytest.mark.xfail(reason="Pydantic dump does not preserve shared references"),
        ),
        "none",
    ],
)
async def test_return_value_preserves_opaque_payloads(world, kind) -> None:
    owner = await _target(world)
    value: Any
    if kind == "custom":
        value = Receipt(quantity=7)
    elif kind == "cycle":
        value = []
        value.append(value)
    elif kind == "shared":
        shared = {"quantity": 7}
        value = [shared, shared]
    else:
        value = None
    await _complete(world, owner.run_id, value)
    reader = await start(inspect_payload, owner.run_id, kind)
    await _invoke(reader, inspect_payload)
    await _invoke(
        reader,
        inspect_payload,
        payload=_step(world, run_mod._get_return_value.name),
    )
    await _invoke(reader, inspect_payload)
    await _invoke(reader, inspect_payload)
    await _finish_checkpoint(world, reader, inspect_payload)
    assert await _result(reader) == "preserved"
    assert world.target_reads == 1


async def test_suspended_context_does_not_fall_back_to_io(world) -> None:
    owner = await _target(world)
    ctx = runtime.WorkflowOrchestratorContext(
        [],
        run_id="reader",
        seed="reader",
        started_at=0,
        registry=registry,
    )
    ctx.suspended = True
    token = ctx._ctx.set(ctx)
    try:
        for operation in (owner.status, owner.attributes, owner.return_value, owner.terminate):
            with pytest.raises(asyncio.CancelledError):
                await operation()
    finally:
        ctx._ctx.reset(token)
    assert world.target_reads == world.terminations == 0


_upload_registry = core.Workflows(as_vercel_job=False, namespace="uploads")


@_upload_registry.workflow
async def upload(data: str, *, compress_arguments: bool) -> int:
    return len(data)


@pytest.fixture
def start_world(monkeypatch):
    instance = Mock(spec=w.World)
    instance.get_deployment_id = AsyncMock(return_value="dpl_current")
    instance.create_run_id.side_effect = partial(w.World.create_run_id, instance)
    instance.get_environment.return_value = None
    instance.run_key = AsyncMock(return_value=None)
    instance.events_create = AsyncMock()
    instance.queue = AsyncMock()
    monkeypatch.setattr(w, "get_world", lambda: instance)
    return instance


# @lat: [[vercel-workflow#Configured workflows]]
def test_configuration_is_immutable_and_does_not_modify_the_workflow():
    default = upload.get_config()
    plain = upload.without_argument_compression()
    another = plain.without_argument_compression()
    assert plain.workflow is another.workflow is upload
    assert plain.get_config().compress_arguments is False
    assert another.get_config().compress_arguments is False
    assert another is not plain
    assert upload.without_argument_compression() is not plain
    assert upload.get_config() == default == Configuration()
    assert default.compress_arguments is True
    with pytest.raises(FrozenInstanceError):
        plain.config = default  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        plain.get_config().compress_arguments = True  # type: ignore[misc]


async def test_configuration_can_be_copied_and_replaced_between_workflows(monkeypatch, start_world):
    monkeypatch.delenv("WORKFLOW_COMPRESSION_CODEC", raising=False)
    monkeypatch.delenv("WORKFLOW_DISABLE_COMPRESSION", raising=False)
    registry = core.Workflows(as_vercel_job=False, namespace="copies")

    @registry.workflow
    async def download(data: str) -> str:
        return data

    config = Configuration().with_region("fra1").without_argument_compression()
    assert config.get_config() is config
    configured = upload.configure(config)
    copied = download.configure(configured.get_config())
    default = config.configure(upload.get_config())
    reset = copied.configure(default)
    assert configured.get_config() is copied.get_config() is config
    assert config.region == "fra1"
    assert reset.workflow is copied.workflow is download
    assert reset.get_config() is default
    assert default.compress_arguments is True
    assert default.region is None
    assert config.compress_arguments is False

    data = "compressible payload " * 200
    await start(copied, data)
    await start(reset, data)
    plain, compressed = [
        call.args[1].event_data for call in start_world.events_create.call_args_list
    ]
    assert plain.workflow_name == compressed.workflow_name == download.workflow_id
    assert plain.input.startswith(ser.DEVALUE_V1)
    assert compressed.input.startswith(ser.ZSTD)
    assert (
        ser.hydrate(plain.input, what="arguments")
        == ser.hydrate(compressed.input, what="arguments")
        == [{"data": data}]
    )
    for call in start_world.queue.call_args_list:
        assert call.args[1].run_input.execution_context["queueNamespace"] == "copies"


@pytest.mark.parametrize("enabled", [None, True, False])
@pytest.mark.parametrize("disabled_by_environment", [False, True])
async def test_argument_compression_respects_configuration_and_environment(
    monkeypatch, start_world, enabled, disabled_by_environment
):
    monkeypatch.delenv("WORKFLOW_COMPRESSION_CODEC", raising=False)
    monkeypatch.delenv("WORKFLOW_DISABLE_COMPRESSION", raising=False)
    if disabled_by_environment:
        monkeypatch.setenv("WORKFLOW_DISABLE_COMPRESSION", "1")
    workflow = (
        upload
        if enabled is None
        else (ConfiguredWorkflow(upload) if enabled else upload.without_argument_compression())
    )
    data = "compressible payload " * 200
    run = await start(workflow, data, compress_arguments=False)
    event = start_world.events_create.call_args.args[1].event_data
    expected_prefix = (
        ser.ZSTD if enabled is not False and not disabled_by_environment else ser.DEVALUE_V1
    )
    assert event.input.startswith(expected_prefix)
    assert ser.hydrate(event.input, what="arguments") == [
        {"data": data, "compress_arguments": False}
    ]
    start_world.run_key.assert_awaited_once_with(run.run_id, deployment_id="dpl_current")
    queued = start_world.queue.call_args.args[1]
    assert queued.run_input.input == event.input
    assert queued.run_input.execution_context["queueNamespace"] == "uploads"
    assert event.workflow_name == upload.workflow_id


async def test_default_configuration_preserves_the_small_payload_threshold(
    monkeypatch, start_world
):
    monkeypatch.delenv("WORKFLOW_DISABLE_COMPRESSION", raising=False)
    await start(ConfiguredWorkflow(upload), "small", compress_arguments=True)
    assert start_world.events_create.call_args.args[1].event_data.input.startswith(ser.DEVALUE_V1)


async def test_configuration_is_reusable_and_start_remains_a_native_coroutine(start_world):
    configured = upload.without_argument_compression()
    assert inspect.iscoroutinefunction(start)
    request = start(configured, "first", compress_arguments=True)
    assert inspect.iscoroutine(request)
    start_world.events_create.assert_not_called()
    first = await asyncio.create_task(request)
    second = await start(configured, "second", compress_arguments=False)
    assert first.run_id != second.run_id
    assert configured.get_config().compress_arguments is False
    assert len(start_world.events_create.call_args_list) == 2
    for call in start_world.events_create.call_args_list:
        assert call.args[1].event_data.input.startswith(ser.DEVALUE_V1)


if TYPE_CHECKING:
    from typing import assert_type

    async def _check_configuration_types(
        workflow: core.Workflow[[str], int], other_workflow: core.Workflow[[int], str]
    ) -> None:
        config = Configuration().with_region("fra1").without_argument_compression()
        assert_type(config, Configuration)
        assert_type(config.get_config(), Configuration)
        assert_type(config.configure(Configuration()), Configuration)
        configured = workflow.with_region("fra1").without_argument_compression()
        assert_type(configured, ConfiguredWorkflow[[str], int])
        assert_type(workflow.get_config(), Configuration)
        assert_type(configured.get_config(), Configuration)
        assert_type(workflow.configure(config), ConfiguredWorkflow[[str], int])
        assert_type(configured.configure(config), ConfiguredWorkflow[[str], int])
        copied = other_workflow.configure(configured.get_config())
        assert_type(copied, ConfiguredWorkflow[[int], str])
        assert_type(await start(copied, 42), Run[str])
        assert_type(await start(configured, "payload"), Run[int])
        assert_type(await start(workflow, "payload"), Run[int])


def test_workflow_configuration_is_immutable_and_composable():
    registry = core.Workflows(as_vercel_job=False)

    @registry.workflow
    async def workflow(order_id: str) -> int:
        return len(order_id)

    european = workflow.without_argument_compression().with_region("fra1")
    american = european.with_region("iad1")
    reverse = workflow.with_region("fra1").without_argument_compression()
    assert european.workflow is american.workflow is reverse.workflow is workflow
    assert not european.get_config().compress_arguments
    assert not american.get_config().compress_arguments
    assert not reverse.get_config().compress_arguments
    assert european.get_config().region == reverse.get_config().region == "fra1"
    assert american.get_config().region == "iad1"
    assert workflow.with_region("fra1") is not european
    with pytest.raises(FrozenInstanceError):
        european.get_config().region = "iad1"  # type: ignore[misc]


async def test_region_configuration_is_reusable(monkeypatch):
    import asyncio
    import inspect

    from vercel.workflow import start
    from vercel.workflow._internal import core, serialization as ser, world as w

    registry = core.Workflows(as_vercel_job=False, namespace="orders")

    @registry.workflow
    async def workflow(order_id: str, *, region: str, deployment_id: str) -> int:
        return len(order_id)

    world = Mock(spec=w.World)
    world.get_deployment_id = AsyncMock(return_value="dpl_current")
    world.get_environment.return_value = "preview"
    world.create_run_id.side_effect = ["first", "second", "unconfigured"]
    world.run_key = AsyncMock(return_value=None)
    world.events_create = AsyncMock()
    world.queue = AsyncMock()
    monkeypatch.setattr(w, "get_world", lambda: world)
    configured = workflow.with_region("fra1").without_argument_compression()
    assert inspect.iscoroutinefunction(start)
    first = start(configured, "order_1", region="business", deployment_id="argument")
    assert inspect.iscoroutine(first)
    world.create_run_id.assert_not_called()
    first_run = await asyncio.create_task(first)
    second_run = await start(configured, "order_2", region="business", deployment_id="argument")
    assert first_run.run_id != second_run.run_id
    expected = "dpl_current"
    assert world.create_run_id.call_count == 2
    for call in world.create_run_id.call_args_list:
        assert call.kwargs == {"region": "fra1"}
    for call in world.run_key.call_args_list:
        assert call.kwargs == {"deployment_id": expected}
    for i, call in enumerate(world.events_create.call_args_list, 1):
        event = call.args[1].event_data
        assert event.input.startswith(ser.DEVALUE_V1)
        assert event.deployment_id == expected
        assert event.workflow_name == workflow.workflow_id
        assert ser.hydrate(event.input, what="arguments") == [
            {"order_id": f"order_{i}", "region": "business", "deployment_id": "argument"}
        ]
    for call in world.queue.call_args_list:
        assert call.kwargs == {"region": "fra1", "deployment_id": expected}
        assert call.args[1].run_input.deployment_id == expected
        assert call.args[1].run_input.execution_context["queueNamespace"] == "orders"
    await start(workflow, "order_3", region="business", deployment_id="argument")
    assert world.create_run_id.call_args.kwargs == {"region": None}
    assert world.queue.call_args.kwargs == {"deployment_id": "dpl_current", "region": None}


async def test_start_rejects_missing_arguments_before_preparing_run(start_world):
    with pytest.raises(TypeError):
        await start(upload, compress_arguments=True)  # type: ignore[call-arg]
    start_world.get_deployment_id.assert_not_awaited()
    start_world.run_key.assert_not_awaited()
    start_world.events_create.assert_not_awaited()
    start_world.queue.assert_not_awaited()


@pytest.mark.parametrize("failure_stage", ["prepare", "create", "queue"])
async def test_start_failure_preserves_durable_write_order(start_world, failure_stage):
    error = RuntimeError(f"{failure_stage} failed")
    failing_operation = {
        "prepare": start_world.run_key,
        "create": start_world.events_create,
        "queue": start_world.queue,
    }[failure_stage]
    failing_operation.side_effect = error
    with pytest.raises(RuntimeError) as caught:
        await start(upload, "payload", compress_arguments=True)
    assert caught.value is error
    assert start_world.events_create.await_count == (failure_stage != "prepare")
    assert start_world.queue.await_count == (failure_stage == "queue")
