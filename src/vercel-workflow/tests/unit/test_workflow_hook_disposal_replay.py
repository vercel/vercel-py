import pydantic
import pytest

from tests.payloads import PLAIN_ENCODER
from vercel.workflow._internal import core, runtime, serialization as ser, world as w


class Approval(pydantic.BaseModel):
    approved: bool


@pytest.fixture
def context() -> runtime.WorkflowOrchestratorContext:
    return runtime.WorkflowOrchestratorContext(
        [],
        run_id="wrun_test",
        seed="seed",
        started_at=0,
        registry=core.Workflows(as_vercel_job=False),
    )


@pytest.mark.parametrize("dispose_before_second_payload", [False, True])
async def test_recorded_disposal_preserves_unclaimed_payloads(
    context: runtime.WorkflowOrchestratorContext, dispose_before_second_payload: bool
) -> None:
    hook_id = context.create_hook("approval", Approval)._correlation_id
    context.events.extend(
        [
            w.HookCreatedEventData(token="approval").into_event(hook_id),
            w.HookReceivedEventData(payload=PLAIN_ENCODER.encode({"approved": True})).into_event(
                hook_id
            ),
            w.HookReceivedEventData(payload=PLAIN_ENCODER.encode({"approved": False})).into_event(
                hook_id
            ),
            w.HookDisposedEvent(correlation_id=hook_id),
        ]
    )
    for _ in context.events:
        context.resume()

    assert (await context.run_hook(correlation_id=hook_id)).approved is True
    if not dispose_before_second_payload:
        assert (await context.run_hook(correlation_id=hook_id)).approved is False

    context.dispose_hook(correlation_id=hook_id)
    with pytest.raises(StopAsyncIteration):
        await context.run_hook(correlation_id=hook_id)


@pytest.mark.parametrize(
    ("payload", "error_type"),
    [
        (PLAIN_ENCODER.encode({"wrong": True}), pydantic.ValidationError),
        (b"dev", ser.SerializationError),
    ],
    ids=["model validation", "hydration"],
)
async def test_recorded_disposal_preserves_buffered_errors(
    context: runtime.WorkflowOrchestratorContext, payload: bytes, error_type: type[Exception]
) -> None:
    hook_id = context.create_hook("approval", Approval)._correlation_id
    context.events.extend(
        [
            w.HookCreatedEventData(token="approval").into_event(hook_id),
            w.HookReceivedEventData(payload=payload).into_event(hook_id),
            w.HookReceivedEventData(payload=PLAIN_ENCODER.encode({"approved": True})).into_event(
                hook_id
            ),
            w.HookDisposedEvent(correlation_id=hook_id),
        ]
    )
    for _ in context.events:
        context.resume()

    with pytest.raises(error_type):
        await context.run_hook(correlation_id=hook_id)
    assert (await context.run_hook(correlation_id=hook_id)).approved is True
