"""``get_workflow_metadata()`` from a workflow body and from a step.

Mirrors the JS SDK's ``getWorkflowMetadata()``. The step side is sourced
without reading the run entity — the queue name supplies the workflow name,
the input's encoding the encryption flag — because the step path never reads
the run (resilient start executes steps for runs whose row has not landed
yet). The run's start time has no such source, so inside a step it is None.
"""

from __future__ import annotations

from typing import Any

import pytest

from tests.payloads import PLAIN_ENCODER
from vercel.workflow import Workflows, get_workflow_metadata
from vercel.workflow._internal import runtime, serialization as ser, world as w
from vercel.workflow._internal.worlds.local import LocalWorld

# Module level: replay re-imports the workflow's defining module by name
# inside the sandbox, so the registry and its workflows have to live
# somewhere importable.
registry = Workflows(as_vercel_job=False)


def _as_dict(info: Any) -> dict[str, Any]:
    return {
        "run_id": info.run_id,
        "workflow_name": info.workflow_name,
        "started_at": info.started_at.isoformat() if info.started_at else None,
        "url": info.url,
        "encryption": info.features.encryption,
    }


@registry.step
async def probe_step() -> dict[str, Any]:
    return _as_dict(get_workflow_metadata())


@registry.workflow
async def probe_workflow() -> dict[str, Any]:
    body = _as_dict(get_workflow_metadata())
    return {"workflow": body, "step": await probe_step()}


@registry.workflow
async def probe_workflow_only() -> dict[str, Any]:
    return _as_dict(get_workflow_metadata())


@registry.step
async def large_step() -> str:
    return "charged " * 256


@registry.workflow
async def workflow_with_large_step() -> str:
    return await large_step()


class _RecordingLocalWorld(LocalWorld):
    """Real LocalWorld for storage; only the outbound (networked) queue is stubbed."""

    def __init__(self, data_dir) -> None:
        super().__init__()
        self.data_dir = data_dir
        self.queued: list[tuple[str, w.QueuePayload]] = []

    async def queue(self, queue_name: str, message: w.QueuePayload, **kwargs) -> str:
        self.queued.append((queue_name, message))
        return "msg_test"


def test_get_workflow_metadata_outside_any_context_raises() -> None:
    with pytest.raises(RuntimeError, match="inside a workflow or a step"):
        get_workflow_metadata()


async def test_a_new_step_in_an_old_run_uses_the_run_format_policy(tmp_path, monkeypatch) -> None:
    world = _RecordingLocalWorld(tmp_path)
    monkeypatch.setattr(w, "the_world", world)
    old_version = w.SPEC_VERSION_SUPPORTS_COMPRESSION - 1
    created = await world.events_create(
        None,
        w.RunCreatedEvent(
            event_data=w.RunCreatedEventData(
                deployment_id="dpl_1",
                workflow_name=workflow_with_large_step.workflow_id,
                input=PLAIN_ENCODER.encode(ser.argument_array((), {})),
            ),
            spec_version=old_version,
        ),
    )
    assert created.run is not None
    run_id = created.run.run_id
    queue_name = w.get_queue_name(workflow_with_large_step.workflow_id)

    async def deliver(payload: w.WorkflowInvokePayload) -> None:
        await runtime.workflow_handler(
            payload.model_dump(by_alias=True),
            attempt=1,
            queue_name=queue_name,
            message_id="msg_1",
            registry=registry,
        )

    await deliver(w.WorkflowInvokePayload(run_id=run_id))
    (step_payload,) = [
        payload
        for _, payload in world.queued
        if isinstance(payload, w.WorkflowInvokePayload) and payload.step_id is not None
    ]
    await deliver(step_payload)

    assert step_payload.step_id is not None
    step = await world.steps_get(run_id, step_payload.step_id)
    assert step.spec_version == w.SPEC_VERSION_CURRENT
    assert step.output is not None
    assert step.output.startswith(ser.DEVALUE_V1)


async def test_metadata_matches_between_body_and_step(tmp_path, monkeypatch) -> None:
    world = _RecordingLocalWorld(tmp_path)
    monkeypatch.setattr(w, "the_world", world)

    run_result = await world.events_create(
        None,
        w.RunCreatedEventData(
            deployment_id="",
            workflow_name=probe_workflow.workflow_id,
            input=PLAIN_ENCODER.encode(ser.argument_array((), {})),
        ).into_event(),
    )
    assert run_result.run is not None
    run_id = run_result.run.run_id
    queue_name = w.get_queue_name(probe_workflow.workflow_id)

    async def deliver(payload: w.WorkflowInvokePayload) -> None:
        await runtime.workflow_handler(
            payload.model_dump(by_alias=True),
            attempt=1,
            queue_name=queue_name,
            message_id="msg_1",
            registry=registry,
        )

    # First delivery replays the body until it suspends on the step, queueing
    # the step's invoke.
    await deliver(w.WorkflowInvokePayload(run_id=run_id))
    [step_msg] = [
        m
        for _, m in world.queued
        if isinstance(m, w.WorkflowInvokePayload) and m.step_id is not None
    ]

    # Execute the step, then the completion replay.
    await deliver(step_msg)
    await deliver(w.WorkflowInvokePayload(run_id=run_id))

    run = await world.runs_get(run_id)
    assert run.status == "completed"
    assert run.output is not None
    output = ser.hydrate(run.output, what="the run output")

    # The body and the step saw the same metadata, except the run's start
    # time, which only the body has (the step path never reads the run).
    body, in_step = output["workflow"], output["step"]
    assert in_step == {**body, "started_at": None}
    assert body["run_id"] == run_id
    assert body["workflow_name"] == probe_workflow.workflow_id
    assert body["encryption"] is False
    assert body["url"].startswith("http")

    run_started_at = run.started_at
    assert run_started_at is not None
    assert body["started_at"] == run_started_at.isoformat()

    # The step's context is cleared once the step body returns.
    with pytest.raises(RuntimeError, match="inside a workflow or a step"):
        get_workflow_metadata()


async def test_encryption_feature_uses_the_resolved_key_not_the_public_key(
    tmp_path, monkeypatch
) -> None:
    key = bytes(range(32))

    class EncryptedWorld(_RecordingLocalWorld):
        async def run_key(self, run_id: str, *, deployment_id: str | None = None) -> bytes | None:
            return key

    world = EncryptedWorld(tmp_path)
    monkeypatch.setattr(w, "the_world", world)
    created = await world.events_create(
        None,
        w.RunCreatedEventData(
            deployment_id="dpl_1",
            workflow_name=probe_workflow_only.workflow_id,
            input=ser.PayloadEncoder(encryption_key=key).encode(ser.argument_array((), {})),
            # Legacy encrypted runs have no published X25519 public key.
            encryption_public_key=None,
        ).into_event(),
    )
    assert created.run is not None
    run_id = created.run.run_id

    await runtime.workflow_handler(
        w.WorkflowInvokePayload(run_id=run_id).model_dump(by_alias=True),
        attempt=1,
        queue_name=w.get_queue_name(probe_workflow_only.workflow_id),
        message_id="msg_1",
        registry=registry,
    )

    run = await world.runs_get(run_id)
    assert run.output is not None
    output = ser.hydrate(run.output, what="the run output", key=key)
    assert output["encryption"] is True


async def test_namespaced_queue_still_yields_the_workflow_name() -> None:
    # the step side derives the workflow name from the queue name, which a
    # namespace prefixes differently.
    assert (
        runtime._workflow_name_from_queue("__wkf_workflow_workflow//tests//wf")
        == "workflow//tests//wf"
    )
    assert (
        runtime._workflow_name_from_queue("__staging_wkf_workflow_workflow//tests//wf")
        == "workflow//tests//wf"
    )
