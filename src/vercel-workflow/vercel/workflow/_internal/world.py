import abc
import dataclasses
import json
import os
import pathlib
import re
import sys
from collections.abc import AsyncGenerator, AsyncIterator, Mapping, Sequence
from datetime import datetime
from typing import (
    Annotated,
    Any,
    Generic,
    Literal,
    Protocol,
    TypeAlias,
    TypeVar,
    overload,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import pydantic

from vercel._internal.core.polyfills import UTC, Self
from vercel.queue import SanitizedName

from . import ulid

T = TypeVar("T")
QueuePrefix: TypeAlias = str
# OpenTelemetry trace context for distributed tracing
TraceCarrier: TypeAlias = dict[str, str]

_QUEUE_NAMESPACE_PATTERN = re.compile(r"^[a-z][a-z0-9]*$")


def validate_queue_namespace(namespace: str | None) -> None:
    """Validate an optional workflow queue namespace."""
    if namespace is not None and not _QUEUE_NAMESPACE_PATTERN.fullmatch(namespace):
        raise ValueError(
            f'Invalid queue namespace "{namespace}": must be lowercase alphanumeric, '
            "starting with a letter"
        )


def get_queue_topic_prefix(namespace: str | None = None) -> QueuePrefix:
    """Build the workflow queue prefix for an optional namespace."""
    validate_queue_namespace(namespace)
    if namespace is not None:
        return f"__{namespace}_wkf_workflow_"
    return "__wkf_workflow_"


def get_queue_name(
    identifier: str,
    namespace: str | None = None,
) -> str:
    """Build a queue name for an optional namespace."""
    return f"{get_queue_topic_prefix(namespace)}{identifier}"


# The consumer group our subscribers register under. The Python builder
# introspects `get_subscriptions()` and copies this into the queue trigger it
# writes (vercel/vercel#17236), so the value only has to be stable rather than
# match anything; `default` keeps it aligned with the trigger
# `createWorkflowQueueTrigger` writes for the TypeScript SDK on these topics.
#
# Dispatch is keyed on `(consumer_group, topic)`, so a subscriber registered
# under a group the delivery does not name is never called at all.
QUEUE_CONSUMER_GROUP = SanitizedName("default")


def get_physical_topic(queue_name: str) -> SanitizedName:
    """Map a logical queue name onto the VQS topic it is published to.

    Mirrors `@workflow/world-vercel`: replace anything outside
    ``[A-Za-z0-9-_]`` with ``-`` and leave the rest — notably the underscores
    in ``__wkf_workflow_`` — alone. ``sanitize_name()`` must not be used here.
    Its reversible encoding is meant for arbitrary user-supplied names and
    doubles underscores; ours are already VQS-safe, and encoding them would
    put the result outside the ``__wkf_*`` prefix that both the builder's
    trigger pattern and platform-side service resolution recognize as a
    workflow topic.
    """
    return SanitizedName(
        "".join(
            char if char.isascii() and (char.isalnum() or char in "-_") else "-"
            for char in queue_name
        )
    )


# Spec versions, mirroring `@workflow/world`'s `spec-version.ts`. The number a
# row carries is a capability claim read by the *other* SDK, not a stamp of
# which SDK wrote it:
#
#   1  legacy: direct entity mutation, unprefixed JSON payloads
#   2  event sourcing, format-prefixed (`devl`) payloads
#   3  CBOR queue transport
#   4  native attributes (`attr_set`)
#   5  payloads may be zstd- or gzip-compressed
#   6  slot-numbered event ids
#
# `CURRENT` is what we stamp on rows we create; `MAX_SUPPORTED` is the highest
# version we accept when reading, mirroring the same split in TS.
SPEC_VERSION_CURRENT: Literal[2] = 2
SPEC_VERSION_MAX_SUPPORTED = 6

# Which version of "lazy hook resume" this SDK's queue consumer implements.
#
# Resuming a hook takes two writes: the `hook_received` event, and the queue
# message that wakes the run. Doing them one after the other is always safe but
# slower, so `resumeHook()` can also do both at once and put the payload on the
# message. That is only safe if whoever consumes the message knows to write the
# event itself from that payload, because otherwise a failed event write leaves
# the payload nowhere. So the consumer has to say up front that it does.
#
# It says so by number, and a run records the number in its `executionContext`
# when it is created. `resumeHook()` reads it off the run and only takes the
# fast path when it is high enough; a run without one gets the safe path.
#
# This is not a spec version. Those describe how a run's rows are encoded, which
# a reader can see in the rows. This describes what *code* will consume the
# run's queue messages, which nothing in the data reveals.
#
# Mirrors `HOOK_RESUME_INPUT_VERSION` in `@workflow/world`'s `hooks.ts`.
HOOK_RESUME_INPUT_VERSION = 1


class BaseModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(serialize_by_alias=True)


class RunInput(BaseModel):
    input: Any
    deployment_id: str = pydantic.Field(alias="deploymentId")
    workflow_name: str = pydantic.Field(alias="workflowName")
    spec_version: int = pydantic.Field(alias="specVersion")
    execution_context: dict[str, Any] | None = pydantic.Field(
        default=None, alias="executionContext", exclude_if=lambda e: e is None
    )
    attributes: dict[str, str] | None = pydantic.Field(default=None, exclude_if=lambda e: e is None)
    allow_reserved_attributes: Literal[True] | None = pydantic.Field(
        default=None, alias="allowReservedAttributes", exclude_if=lambda e: e is None
    )
    encryption_public_key: str | None = pydantic.Field(
        default=None, alias="encryptionPublicKey", exclude_if=lambda e: e is None
    )
    environment: str | None = pydantic.Field(default=None, exclude_if=lambda e: e is None)


class HookResumeInput(BaseModel):
    resume_id: str = pydantic.Field(alias="resumeId")
    hook_id: str = pydantic.Field(alias="hookId")
    token: str
    payload: bytes
    payload_digest: str = pydantic.Field(alias="payloadDigest")
    deployment_id: str | None = pydantic.Field(
        default=None, alias="deploymentId", exclude_if=lambda e: e is None
    )

    def occurred_at(self) -> datetime | None:
        """When the resume happened, taken from its id.

        A resume id is a ULID, and a ULID contains the time it was made. Using that
        time dates the event to when the payload was sent, instead of to whenever this
        delivery got around to writing it.

        Some ids are not ULIDs -- older producers, and hand-written ones in tests.
        There is no time to read out of those, so return nothing and let the world
        date the event.
        """
        try:
            return datetime.fromtimestamp(ulid.decode_time(self.resume_id) / 1000, UTC)
        except ValueError:
            return None


class WorkflowInvokePayload(BaseModel):
    """Payload for invoking a workflow.

    Step invocations ride this same payload on the workflow topic: a message
    carrying ``stepId`` asks the receiver to execute that step before the run
    is replayed, rather than to replay straight away.
    """

    run_id: str = pydantic.Field(alias="runId")
    # Run creation data for the resilient-start path, on first deliveries only.
    run_input: RunInput | None = pydantic.Field(
        default=None, alias="runInput", exclude_if=lambda e: e is None
    )
    # Hook payload for the lazy-hook-resume path, present only when the producer
    # parallelized its `hook_received` write with this message.
    hook_input: HookResumeInput | None = pydantic.Field(
        default=None, alias="hookInput", exclude_if=lambda e: e is None
    )
    # Step ID for step execution on the combined handler. When present, the
    # receiver executes that step instead of replaying the run.
    step_id: str | None = pydantic.Field(
        default=None, alias="stepId", exclude_if=lambda e: e is None
    )
    # Step name, sent alongside stepId because the merged topic no longer
    # encodes it: the queue name identifies the workflow, not the step.
    step_name: str | None = pydantic.Field(
        default=None, alias="stepName", exclude_if=lambda e: e is None
    )
    trace_carrier: TraceCarrier | None = pydantic.Field(
        default=None, alias="traceCarrier", exclude_if=lambda e: e is None
    )
    requested_at: datetime | None = pydantic.Field(
        default=None, alias="requestedAt", exclude_if=lambda e: e is None
    )

    @pydantic.field_serializer("requested_at", mode="plain")
    def ser_requested_at(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        return value


class HealthCheckPayload(BaseModel):
    """
    Health check payload - used to verify that the queue pipeline
    can deliver messages to the combined workflow endpoint.
    """

    # Required, not defaulted: it is the discriminator. With a default, a
    # message carrying only `correlationId` would validate as a probe, and the
    # handler answers a probe *instead of* doing the work the message asked for.
    health_check: Literal[True] = pydantic.Field(alias="__healthCheck")
    correlation_id: str = pydantic.Field(alias="correlationId")
    run_id: str | None = pydantic.Field(default=None, alias="runId", exclude_if=lambda e: e is None)


QueuePayload: TypeAlias = WorkflowInvokePayload | HealthCheckPayload
QueuePayloadAdaptor: pydantic.TypeAdapter[QueuePayload] = pydantic.TypeAdapter(QueuePayload)


class StructuredError(BaseModel):
    message: str
    stack: str | None = None
    code: str | None = None


WorkflowRunStatus: TypeAlias = Literal["pending", "running", "completed", "failed", "cancelled"]
StepStatus: TypeAlias = Literal["pending", "running", "completed", "failed", "cancelled"]


class _ContextWrapper(Generic[T]):
    def __init__(self, value: T):
        self.value = value

    def __getattr__(self, item):
        return getattr(self.value, item)

    def __getitem__(self, item):
        return self.value[item]


class BaseWorkflowRun(BaseModel):
    model_config = pydantic.ConfigDict(extra="ignore")

    run_id: str = pydantic.Field(alias="runId")
    status: WorkflowRunStatus
    deployment_id: str = pydantic.Field(alias="deploymentId")
    workflow_name: str = pydantic.Field(alias="workflowName")
    # Optional in database for backwards compatibility, defaults to 1 (legacy) when reading
    spec_version: int | None = pydantic.Field(default=None, alias="specVersion")
    execution_context: dict[str, Any] | None = pydantic.Field(
        default=None, alias="executionContext"
    )
    # run_created returns input as str `'[Circular]'`,
    # while run_completed returns input_ref
    input: bytes | str | None = None
    output: bytes | None = None
    error: StructuredError | None = None
    # Plaintext string-string metadata. Always materialized (as `{}` when
    # unset) so a run row carries the same field set the TS SDK writes.
    attributes: dict[str, str] = pydantic.Field(default_factory=dict)
    encryption_public_key: str | None = pydantic.Field(default=None, alias="encryptionPublicKey")
    expired_at: datetime | None = pydantic.Field(default=None, alias="expiredAt")
    started_at: datetime | None = pydantic.Field(default=None, alias="startedAt")
    completed_at: datetime | None = pydantic.Field(default=None, alias="completedAt")
    created_at: datetime = pydantic.Field(alias="createdAt")
    updated_at: datetime = pydantic.Field(alias="updatedAt")


class NonFinalWorkflowRun(BaseWorkflowRun):
    status: Literal["pending", "running"]
    output: None = None
    error: None = None
    completed_at: None = pydantic.Field(default=None, alias="completedAt")


class CancelledWorkflowRun(BaseWorkflowRun):
    status: Literal["cancelled"]
    output: None = None
    error: None = None
    completed_at: datetime = pydantic.Field(alias="completedAt")


class CompletedWorkflowRun(BaseWorkflowRun):
    status: Literal["completed"]
    output: bytes | None = None  # create run_completed event returns run without output
    error: None = None
    completed_at: datetime = pydantic.Field(alias="completedAt")


class FailedWorkflowRun(BaseWorkflowRun):
    status: Literal["failed"]
    output: None = None
    error: StructuredError | None = None
    completed_at: datetime = pydantic.Field(alias="completedAt")


WorkflowRun: TypeAlias = Annotated[
    NonFinalWorkflowRun | CancelledWorkflowRun | CompletedWorkflowRun | FailedWorkflowRun,
    pydantic.Field(discriminator="status"),
]
WorkflowRunAdaptor: pydantic.TypeAdapter[WorkflowRun] = pydantic.TypeAdapter(WorkflowRun)


class BaseWorkflowStep(BaseModel):
    model_config = pydantic.ConfigDict(extra="ignore")

    run_id: str = pydantic.Field(alias="runId")
    step_id: str = pydantic.Field(alias="stepId")
    step_name: str = pydantic.Field(alias="stepName")
    status: StepStatus
    input: bytes | None = None
    output: bytes | None = None
    """
    The error from a step_retrying or step_failed event.
    This tracks the most recent error the step encountered, which may
    be from a retry attempt (step_retrying) or the final failure (step_failed).
    """
    error: StructuredError | None = None
    attempt: int
    """
    When the step first started executing. Set by the first step_started event
    and not updated on subsequent retries.
    """
    started_at: datetime | None = pydantic.Field(default=None, alias="startedAt")
    completed_at: datetime | None = pydantic.Field(default=None, alias="completedAt")
    created_at: datetime = pydantic.Field(alias="createdAt")
    updated_at: datetime = pydantic.Field(alias="updatedAt")
    retry_after: datetime | None = pydantic.Field(default=None, alias="retryAfter")
    spec_version: int | None = pydantic.Field(default=None, alias="specVersion")


class NonFinalWorkflowStep(BaseWorkflowStep):
    status: Literal["pending", "running"]
    output: None = None
    completed_at: None = pydantic.Field(default=None, alias="completedAt")


class CancelledWorkflowStep(BaseWorkflowStep):
    status: Literal["cancelled"]
    output: None = None
    completed_at: datetime = pydantic.Field(alias="completedAt")


class CompletedWorkflowStep(BaseWorkflowStep):
    status: Literal["completed"]
    output: bytes | None = None
    completed_at: datetime = pydantic.Field(alias="completedAt")


class FailedWorkflowStep(BaseWorkflowStep):
    status: Literal["failed"]
    output: None = None
    error: StructuredError
    completed_at: datetime = pydantic.Field(alias="completedAt")


WorkflowStep: TypeAlias = Annotated[
    NonFinalWorkflowStep | CancelledWorkflowStep | CompletedWorkflowStep | FailedWorkflowStep,
    pydantic.Field(discriminator="status"),
]
WorkflowStepAdaptor: pydantic.TypeAdapter[WorkflowStep] = pydantic.TypeAdapter(WorkflowStep)


class ServerProps(BaseModel):
    run_id: str = pydantic.Field(alias="runId")
    event_id: str = pydantic.Field(alias="eventId")
    created_at: datetime = pydantic.Field(alias="createdAt")
    resume_id: str | None = pydantic.Field(
        default=None, alias="resumeId", exclude_if=lambda e: e is None
    )


class BaseEvent(BaseModel):
    event_type: str = pydantic.Field(alias="eventType")
    correlation_id: str | None = pydantic.Field(
        default=None, alias="correlationId", exclude_if=lambda e: e is None
    )
    spec_version: int = pydantic.Field(
        default=SPEC_VERSION_CURRENT,
        alias="specVersion",
        ge=1,
        le=SPEC_VERSION_MAX_SUPPORTED,
    )
    server_props: ServerProps | None = pydantic.Field(default=None, exclude=True)

    def payloads(self) -> tuple[Any, ...]:
        """The serialized payloads this event carries, if any.

        One per entry in JS's ``EVENT_DATA_PAYLOAD_FIELD_BY_EVENT_TYPE``.
        """
        return ()

    @pydantic.model_validator(mode="before")
    @classmethod
    def fold_server_props(cls, data: Any) -> Any:
        if isinstance(data, dict):
            server_props = {
                f.alias: data[f.alias] for f in ServerProps.model_fields.values() if f.alias in data
            }
            rv = {k: v for k, v in data.items() if k not in server_props}
            if server_props:
                rv["server_props"] = ServerProps.model_validate(server_props)
            return rv
        return data


class RunCreatedEventData(BaseModel):
    deployment_id: str = pydantic.Field(alias="deploymentId")
    workflow_name: str = pydantic.Field(alias="workflowName")
    input: bytes
    execution_context: dict[str, Any] | None = pydantic.Field(
        default=None, alias="executionContext", exclude_if=lambda e: e is None
    )

    def into_event(self) -> "RunCreatedEvent":
        return RunCreatedEvent(eventData=self)


class RunCreatedEvent(BaseEvent):
    """
    Event created when a workflow run is first created. The World implementation
    atomically creates both the event and the run entity with status 'pending'.
    """

    event_type: Literal["run_created"] = pydantic.Field(default="run_created", alias="eventType")
    event_data: RunCreatedEventData = pydantic.Field(alias="eventData")

    def payloads(self) -> tuple[Any, ...]:
        return (self.event_data.input,)


class RunStartedEventData(BaseModel):
    input: Any = None
    deployment_id: str | None = pydantic.Field(
        default=None, alias="deploymentId", exclude_if=lambda e: e is None
    )
    workflow_name: str | None = pydantic.Field(
        default=None, alias="workflowName", exclude_if=lambda e: e is None
    )
    execution_context: dict[str, Any] | None = pydantic.Field(
        default=None, alias="executionContext", exclude_if=lambda e: e is None
    )
    attributes: dict[str, str] | None = pydantic.Field(default=None, exclude_if=lambda e: e is None)
    allow_reserved_attributes: Literal[True] | None = pydantic.Field(
        default=None, alias="allowReservedAttributes", exclude_if=lambda e: e is None
    )
    # Mirrors `run_created.eventData.encryptionPublicKey`. This is the path that
    # recreates a run from the queued message, which is exactly when the key
    # would otherwise be lost for the rest of the run's life.
    encryption_public_key: str | None = pydantic.Field(
        default=None, alias="encryptionPublicKey", exclude_if=lambda e: e is None
    )

    @classmethod
    def from_run_input(cls, run_input: RunInput) -> "RunStartedEventData":
        return cls(
            input=run_input.input,
            deploymentId=run_input.deployment_id,
            workflowName=run_input.workflow_name,
            executionContext=run_input.execution_context,
            attributes=run_input.attributes,
            allowReservedAttributes=run_input.allow_reserved_attributes,
            encryptionPublicKey=run_input.encryption_public_key,
        )

    def into_event(self, *, spec_version: int = SPEC_VERSION_CURRENT) -> "RunStartedEvent":
        return RunStartedEvent(eventData=self, specVersion=spec_version)


class RunStartedEvent(BaseEvent):
    """
    Event created when a workflow run starts executing.
    Updates the run entity to status 'running'.
    """

    event_type: Literal["run_started"] = pydantic.Field(
        default="run_started",
        alias="eventType",
    )
    event_data: RunStartedEventData | None = pydantic.Field(
        default=None, alias="eventData", exclude_if=lambda e: e is None
    )

    def payloads(self) -> tuple[Any, ...]:
        if self.event_data is None or self.event_data.input is None:
            return ()
        return (self.event_data.input,)


class RunCompletedEventData(BaseModel):
    output: bytes

    def into_event(self) -> "RunCompletedEvent":
        return RunCompletedEvent(eventData=self)


class RunCompletedEvent(BaseEvent):
    """
    Event created when a workflow run completes successfully.
    Updates the run entity to status 'completed' with output.
    """

    event_type: Literal["run_completed"] = pydantic.Field(
        default="run_completed",
        alias="eventType",
    )
    event_data: RunCompletedEventData = pydantic.Field(alias="eventData")

    def payloads(self) -> tuple[Any, ...]:
        return (self.event_data.output,)


class RunFailedEventData(BaseModel):
    error: Any
    code: str | None = None

    def into_event(self) -> "RunFailedEvent":
        return RunFailedEvent(eventData=self)


class RunFailedEvent(BaseEvent):
    """
    Event created when a workflow run fails.
    Updates the run entity to status 'failed' with error.
    """

    event_type: Literal["run_failed"] = pydantic.Field(
        default="run_failed",
        alias="eventType",
    )
    event_data: RunFailedEventData = pydantic.Field(alias="eventData")

    def payloads(self) -> tuple[Any, ...]:
        return (self.event_data.error,)


class StepCreatedEventData(BaseModel):
    step_name: str = pydantic.Field(alias="stepName")
    input: bytes | dict[str, Any]

    def into_event(self, correlation_id: str) -> "StepCreatedEvent":
        return StepCreatedEvent(correlationId=correlation_id, eventData=self)


class StepCreatedEvent(BaseEvent):
    """
    Event created when a step is first invoked. The World implementation
    atomically creates both the event and the step entity.
    """

    event_type: Literal["step_created"] = pydantic.Field(
        default="step_created",
        alias="eventType",
    )
    correlation_id: str = pydantic.Field(alias="correlationId")
    event_data: StepCreatedEventData = pydantic.Field(alias="eventData")

    def payloads(self) -> tuple[Any, ...]:
        return (self.event_data.input,)


class StepStartedEventData(BaseModel):
    attempt: int | None = pydantic.Field(default=None, exclude_if=lambda e: e is None)

    def into_event(self, correlation_id: str) -> "StepStartedEvent":
        return StepStartedEvent(correlationId=correlation_id, eventData=self)


class StepStartedEvent(BaseEvent):
    event_type: Literal["step_started"] = pydantic.Field(
        default="step_started",
        alias="eventType",
    )
    correlation_id: str = pydantic.Field(alias="correlationId")
    event_data: StepStartedEventData | None = pydantic.Field(
        default=None, alias="eventData", exclude_if=lambda e: e is None
    )


class StepRetryingEventData(BaseModel):
    error: Any
    stack: str | None = None
    retry_after: datetime | None = pydantic.Field(
        default=None, alias="retryAfter", exclude_if=lambda e: e is None
    )

    def into_event(self, correlation_id: str) -> "StepRetryingEvent":
        return StepRetryingEvent(correlationId=correlation_id, eventData=self)


class StepRetryingEvent(BaseEvent):
    """
    Event created when a step fails and will be retried.
    Sets the step status back to 'pending' and records the error.
    The error is stored in step.error for debugging.
    """

    event_type: Literal["step_retrying"] = pydantic.Field(
        default="step_retrying",
        alias="eventType",
    )
    correlation_id: str = pydantic.Field(alias="correlationId")
    event_data: StepRetryingEventData = pydantic.Field(alias="eventData")

    def payloads(self) -> tuple[Any, ...]:
        return (self.event_data.error,)


class StepCompletedEventData(BaseModel):
    result: bytes | Any = None

    def into_event(self, correlation_id: str) -> "StepCompletedEvent":
        return StepCompletedEvent(correlationId=correlation_id, eventData=self)


class StepCompletedEvent(BaseEvent):
    event_type: Literal["step_completed"] = pydantic.Field(
        default="step_completed",
        alias="eventType",
    )
    correlation_id: str = pydantic.Field(alias="correlationId")
    event_data: StepCompletedEventData = pydantic.Field(alias="eventData")

    def payloads(self) -> tuple[Any, ...]:
        return (self.event_data.result,)


class StepFailedEventData(BaseModel):
    error: Any
    stack: str | None = None

    def into_event(self, correlation_id: str) -> "StepFailedEvent":
        return StepFailedEvent(correlationId=correlation_id, eventData=self)


class StepFailedEvent(BaseEvent):
    event_type: Literal["step_failed"] = pydantic.Field(
        default="step_failed",
        alias="eventType",
    )
    correlation_id: str = pydantic.Field(alias="correlationId")
    event_data: StepFailedEventData = pydantic.Field(alias="eventData")

    def payloads(self) -> tuple[Any, ...]:
        return (self.event_data.error,)


class Hook(BaseModel):
    run_id: str = pydantic.Field(alias="runId")
    hook_id: str = pydantic.Field(alias="hookId")
    token: str
    owner_id: str = pydantic.Field(alias="ownerId")
    project_id: str = pydantic.Field(alias="projectId")
    environment: str
    metadata: bytes | None = None
    created_at: datetime = pydantic.Field(alias="createdAt")
    spec_version: int | None = pydantic.Field(default=None, alias="specVersion")
    is_webhook: bool | None = pydantic.Field(default=None, alias="isWebhook")
    is_system: bool | None = pydantic.Field(default=None, alias="isSystem")


class HookCreatedEventData(BaseModel):
    token: str
    metadata: bytes | None = pydantic.Field(default=None, exclude_if=lambda e: e is None)

    def into_event(self, correlation_id: str) -> "HookCreatedEvent":
        return HookCreatedEvent(correlationId=correlation_id, eventData=self)


class HookCreatedEvent(BaseEvent):
    """
    Event created when a hook is first invoked. The World implementation
    atomically creates both the event and the hook entity.
    """

    event_type: Literal["hook_created"] = pydantic.Field(
        default="hook_created",
        alias="eventType",
    )
    correlation_id: str = pydantic.Field(alias="correlationId")
    event_data: HookCreatedEventData = pydantic.Field(alias="eventData")

    def payloads(self) -> tuple[Any, ...]:
        return (self.event_data.metadata,)


class HookReceivedEventData(BaseModel):
    payload: bytes
    token: str | None = pydantic.Field(default=None, exclude_if=lambda e: e is None)

    def into_event(self, correlation_id: str) -> "HookReceivedEvent":
        return HookReceivedEvent(correlationId=correlation_id, eventData=self)


class HookReceivedEvent(BaseEvent):
    event_type: Literal["hook_received"] = pydantic.Field(
        default="hook_received",
        alias="eventType",
    )
    correlation_id: str = pydantic.Field(alias="correlationId")
    event_data: HookReceivedEventData = pydantic.Field(alias="eventData")
    # Payload carried-over from the lazy-hook-resume path
    _queue_input: HookResumeInput | None = pydantic.PrivateAttr(None)

    def payloads(self) -> tuple[Any, ...]:
        return (self.event_data.payload,)


class HookDisposedEvent(BaseEvent):
    event_type: Literal["hook_disposed"] = pydantic.Field(
        default="hook_disposed",
        alias="eventType",
    )
    correlation_id: str = pydantic.Field(alias="correlationId")


class HookConflictEventData(BaseModel):
    token: str


class HookConflictEvent(BaseEvent):
    """
    Event created by World implementations when a hook_created request
    conflicts with an existing hook token. This event is NOT user-creatable -
    it is only returned by the World when a token conflict is detected.

    When the hook consumer sees this event, it should reject any awaited
    promises with a HookTokenConflictError.
    """

    event_type: Literal["hook_conflict"] = pydantic.Field(
        default="hook_conflict",
        alias="eventType",
    )
    correlation_id: str = pydantic.Field(alias="correlationId")
    event_data: HookConflictEventData = pydantic.Field(alias="eventData")


class WaitCreatedEventData(BaseModel):
    resume_at: datetime = pydantic.Field(alias="resumeAt")

    def into_event(self, correlation_id: str) -> "WaitCreatedEvent":
        return WaitCreatedEvent(correlationId=correlation_id, eventData=self)


class WaitCreatedEvent(BaseEvent):
    event_type: Literal["wait_created"] = pydantic.Field(
        default="wait_created",
        alias="eventType",
    )
    correlation_id: str = pydantic.Field(alias="correlationId")
    event_data: WaitCreatedEventData = pydantic.Field(alias="eventData")


class WaitCompletedEvent(BaseEvent):
    event_type: Literal["wait_completed"] = pydantic.Field(
        default="wait_completed",
        alias="eventType",
    )
    correlation_id: str = pydantic.Field(alias="correlationId")


CreateEventRequest: TypeAlias = (
    RunStartedEvent
    | RunCompletedEvent
    | RunFailedEvent
    | StepCreatedEvent
    | StepStartedEvent
    | StepRetryingEvent
    | StepCompletedEvent
    | StepFailedEvent
    | HookCreatedEvent
    | HookReceivedEvent
    | HookDisposedEvent
    | WaitCreatedEvent
    | WaitCompletedEvent
)
Event: TypeAlias = Annotated[
    (
        RunCreatedEvent
        | RunStartedEvent
        | RunCompletedEvent
        | RunFailedEvent
        | StepCreatedEvent
        | StepStartedEvent
        | StepRetryingEvent
        | StepCompletedEvent
        | StepFailedEvent
        | HookCreatedEvent
        | HookReceivedEvent
        | HookDisposedEvent
        | HookConflictEvent
        | WaitCreatedEvent
        | WaitCompletedEvent
    ),
    pydantic.Field(discriminator="event_type"),
]
EventAdaptor: pydantic.TypeAdapter[Event] = pydantic.TypeAdapter(Event)


# Hook events that require the target hook to still exist: the backend 404s them
# (and the local world rejects them) once the hook is disposed or never existed,
# surfacing as HookNotFoundError.
HOOK_EVENTS_REQUIRING_EXISTENCE = frozenset({"hook_disposed", "hook_received"})


class PaginationOptions(BaseModel):
    limit: int | None = pydantic.Field(default=None, exclude_if=lambda e: not e)
    cursor: str | None = pydantic.Field(default=None, exclude_if=lambda e: not e)
    sort_order: Literal["asc", "desc"] | None = pydantic.Field(
        default=None, serialization_alias="sortOrder", exclude_if=lambda e: e is None
    )


class PaginatedResult(BaseModel, Generic[T]):
    data: list[T]
    cursor: str | None
    has_more: bool = pydantic.Field(alias="hasMore")


class StreamInfo(BaseModel):
    """Lightweight metadata about a stream, without reading its payloads."""

    tail_index: int = pydantic.Field(alias="tailIndex")
    """Index of the last known chunk, 0-based, or ``-1`` when none was written.

    Anything deriving a start index from this has to handle ``-1`` rather than
    treating it as a position.
    """

    done: bool
    """Whether the stream has been closed."""


class StreamChunk(BaseModel):
    index: int
    data: bytes


class StreamChunksPage(PaginatedResult[StreamChunk]):
    """One page of already-written chunks -- a snapshot, not a live read."""

    # Both are optional here, unlike the base: a page that ends the stream
    # carries neither, and the endpoint omits them rather than sending nulls.
    cursor: str | None = None
    has_more: bool = pydantic.Field(default=False, alias="hasMore")

    done: bool = False
    """Whether the stream is closed, so no more chunks will ever arrive."""


class EventResult(BaseModel):
    event: Event | None = None
    events: list[Event] | None = None
    run: WorkflowRun | None = None
    step: WorkflowStep | None = None
    hook: Any | None = None
    wait: Any | None = None
    cursor: str | None = None
    has_more: bool | None = pydantic.Field(default=None, alias="hasMore")


class HTTPRequest(Protocol):
    @property
    @abc.abstractmethod
    def method(self) -> str: ...

    @property
    @abc.abstractmethod
    def url(self) -> str:
        """The request target as it arrived: a path, plus any query string."""

    @property
    @abc.abstractmethod
    def headers(self) -> Mapping[str, str]: ...

    @abc.abstractmethod
    def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]: ...


@dataclasses.dataclass
class HTTPResponse:
    status: int
    body: bytes
    headers: dict[str, str]

    @classmethod
    def json(cls, data: Any, *, status: int = 200) -> Self:
        """Create a JSON response with the given data and status code."""
        body = json.dumps(data).encode("utf-8")
        headers = {"content-type": "application/json"}
        return cls(status, body, headers)


class HTTPError(Exception):
    def __init__(self, response: HTTPResponse) -> None:
        self.response = response
        super().__init__(f"HTTP Error {response.status}")


class WorkflowWorldError(Exception):
    """A non-success response from the workflow backend that isn't mapped to a
    more specific error. Carries the HTTP status so callers can branch on it
    (e.g. a 404 on a hook event becomes a HookNotFoundError)."""

    def __init__(
        self, message: str, *, status: int, code: str | None = None, url: str | None = None
    ) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.url = url


class RunExpiredError(WorkflowWorldError):
    """HTTP 410 — the workflow run has expired and can no longer be advanced."""


class ThrottleError(WorkflowWorldError):
    """HTTP 429 — the backend is throttling requests. ``retry_after`` is the
    number of seconds to wait before retrying, when the server provides it."""

    def __init__(
        self,
        message: str,
        *,
        status: int,
        code: str | None = None,
        url: str | None = None,
        retry_after: int | None = None,
    ) -> None:
        super().__init__(message, status=status, code=code, url=url)
        self.retry_after = retry_after


class EntityConflictError(Exception):
    pass


class HookNotFoundError(Exception):
    """Raised when a hook lookup or hook event targets a hook that does not exist.

    A by-token lookup (``hooks_get_by_token``) that matches no active hook supplies
    ``token``; the server's HTTP 404 on ``hook_disposed`` / ``hook_received`` for an
    already-disposed (or never-created) hook supplies ``hook_id``. The runtime
    treats the event-path case the same as ``EntityConflictError`` — a benign
    duplicate to skip.
    """

    def __init__(self, *, token: str | None = None, hook_id: str | None = None) -> None:
        self.token = token
        self.hook_id = hook_id
        if hook_id is not None:
            message = f'Hook "{hook_id}" not found'
        elif token is not None:
            message = f'Hook not found for token "{token}"'
        else:
            message = "Hook not found"
        super().__init__(message)


class TooEarlyError(Exception):
    """Raised when a step is started before its ``retryAfter`` timestamp.

    Mirrors the server's HTTP 425 response. ``retry_after`` is the number of
    seconds remaining before the step may be started, used to size the
    re-delivery delay.
    """

    def __init__(self, message: str, retry_after: int | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


@dataclasses.dataclass(frozen=True)
class QueueContinuation:
    """A handler's request to re-enqueue its own message after a delay.

    ``idempotency_key`` lets the re-enqueue dedupe — e.g. a wait/sleep
    continuation keyed on the pending wait so repeated suspension passes over the
    same wait collapse into one delayed wake-up.
    """

    delay_seconds: float
    idempotency_key: str | None = None


class QueueHandler(Protocol):
    async def __call__(
        self, message: Any, *, attempt: int, queue_name: str, message_id: str
    ) -> QueueContinuation | None: ...


class HTTPHandler(Protocol):
    async def __call__(self, request: HTTPRequest) -> HTTPResponse: ...


class World(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def get_deployment_id(self) -> str: ...

    def get_environment(self) -> str | None:
        """The environment this World's writes are attributed to."""
        return None

    @abc.abstractmethod
    async def queue(
        self,
        queue_name: str,
        message: QueuePayload,
        *,
        deployment_id: str | None = None,
        idempotency_key: str | None = None,
        **kwargs,
    ) -> str:
        """
        Enqueues a message to the specified queue.

        Args:
            queue_name: The name of the queue to which the message will be sent.
            message: The content of the message to be sent to the queue.
            deployment_id: Optional deployment ID for the queue operation.
            idempotency_key: Optional idempotency key to prevent duplicate messages.

        Returns:
            The message ID.
        """
        ...

    @abc.abstractmethod
    def create_queue_handler(
        self, queue_name_prefix: QueuePrefix, handler: QueueHandler
    ) -> HTTPHandler:
        """
        Creates an HTTP queue handler for processing messages from a specific queue.

        Args:
            queue_name_prefix: The prefix of the queue name to handle.
            handler: The handler function to process queue messages.

        Returns:
            An HTTP handler that processes incoming queue requests.
        """
        ...

    async def run_key(self, run_id: str, *, deployment_id: str | None = None) -> bytes | None:
        """The encryption key that opens the payloads of *run_id*.

        ``None`` means the run's payloads are plaintext.
        """
        return None

    def write_manifest(self, manifest: dict[str, Any]) -> pathlib.Path | None:
        return None

    @abc.abstractmethod
    async def runs_get(self, run_id: str) -> WorkflowRun: ...

    @abc.abstractmethod
    async def steps_get(self, run_id: str, step_id: str) -> WorkflowStep: ...

    @abc.abstractmethod
    async def hooks_get_by_token(self, token: str) -> Hook: ...

    @overload
    async def events_create(self, run_id: None, data: RunCreatedEvent) -> EventResult:
        """
        Create a run_created event to start a new workflow run.
        The run_id parameter must be None - the server generates and returns the runId.

        Args:
            run_id: Must be None for run_created events
            data: The run_created event data
        Returns:
            The created event and run entity
        """
        ...

    @overload
    async def events_create(self, run_id: str, data: CreateEventRequest) -> EventResult:
        """
        Create an event for an existing workflow run and atomically update the entity.
        Returns both the event and the affected entity (run/step/hook).
        Args:
            run_id: The workflow run ID (required for all events except run_created)
            data: The event to create
        Returns:
            The created event and affected entity
        """
        ...

    @abc.abstractmethod
    async def events_create(self, run_id: str | None, data: Event) -> EventResult: ...

    @abc.abstractmethod
    async def events_list(
        self,
        run_id: str,
        *,
        pagination: PaginationOptions | None = None,
    ) -> PaginatedResult[Event]: ...

    # ── streams ────────────────────────────────────────────────────────────
    #
    # A stream is an append-only, indexed log of opaque chunks scoped to a run.
    # Unlike the entities above it lives outside the event log: chunks are not
    # events and are not replayed, which is what lets a workflow stream output
    # without growing its own history.

    @abc.abstractmethod
    async def streams_write(self, run_id: str, name: str, chunk: bytes) -> None:
        """Append one chunk to a stream, creating it if needed."""
        ...

    async def streams_write_multi(self, run_id: str, name: str, chunks: Sequence[bytes]) -> None:
        """Append several chunks in order, ideally in one round trip.

        Concrete because a world that cannot batch is still correct without it:
        chunk boundaries and indices come out the same either way, so the
        default below only costs requests.
        """
        for chunk in chunks:
            await self.streams_write(run_id, name, chunk)

    @abc.abstractmethod
    async def streams_close(self, run_id: str, name: str) -> None:
        """Mark a stream complete, ending its readers.

        Idempotent: closing a closed stream is not an error, which is what lets
        a retried close be safe.
        """
        ...

    @abc.abstractmethod
    def streams_get(
        self, run_id: str, name: str, start_index: int | None = None
    ) -> AsyncGenerator[bytes, None]:
        """Read a stream live, waiting for chunks that have not arrived yet.

        The iterator ends when the stream is closed, not when it runs out of
        currently-available chunks -- for a snapshot use
        :meth:`streams_get_chunks` instead.

        *start_index* skips that many chunks from the start. A negative value
        counts back from the current end (``-3`` starts three chunks before it),
        clamped at 0. Callers who abandon the iterator early should close it
        (``contextlib.aclosing``) so the underlying request or poll stops.
        """
        ...

    @abc.abstractmethod
    async def streams_list(self, run_id: str) -> list[str]:
        """The names of every stream this run has written to."""
        ...

    @abc.abstractmethod
    async def streams_get_chunks(
        self, run_id: str, name: str, *, limit: int | None = None, cursor: str | None = None
    ) -> StreamChunksPage:
        """One page of currently-available chunks, oldest first."""
        ...

    @abc.abstractmethod
    async def streams_get_info(self, run_id: str, name: str) -> StreamInfo:
        """A stream's tail index and closed flag, without reading payloads."""
        ...


the_world: World | None = None


def create_world() -> World:
    """
    const targetWorld = process.env.WORKFLOW_TARGET_WORLD || defaultWorld();

    if (targetWorld === 'vercel') {
    return createVercelWorld({
        token: process.env.WORKFLOW_VERCEL_AUTH_TOKEN,
        projectConfig: {
            environment: process.env.WORKFLOW_VERCEL_ENV,
            projectId: process.env.WORKFLOW_VERCEL_PROJECT,
            teamId: process.env.WORKFLOW_VERCEL_TEAM,
        },
    });
    }
    """
    target_world = os.getenv("WORKFLOW_TARGET_WORLD")
    if not target_world:
        if os.getenv("VERCEL_DEPLOYMENT_ID"):
            target_world = "vercel"
        else:
            target_world = "local"

    if target_world == "vercel":
        from .worlds.vercel import VercelWorld

        return VercelWorld(
            token=os.getenv("WORKFLOW_VERCEL_AUTH_TOKEN"),
            environment=os.getenv("WORKFLOW_VERCEL_ENV"),
            project_id=os.getenv("WORKFLOW_VERCEL_PROJECT"),
            team_id=os.getenv("WORKFLOW_VERCEL_TEAM"),
        )

    from .worlds.local import LocalWorld

    return LocalWorld()


def get_world() -> World:
    global the_world
    if the_world is None:
        the_world = create_world()
    return the_world


def set_world(world: World | None) -> None:
    global the_world
    the_world = world
