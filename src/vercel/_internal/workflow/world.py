import abc
import dataclasses
import json
import os
import sys
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

from vercel._internal.polyfills import Self

T = TypeVar("T")
QueuePrefix: TypeAlias = Literal["__wkf_step_", "__wkf_workflow_"]
# OpenTelemetry trace context for distributed tracing
TraceCarrier: TypeAlias = dict[str, str]


class BaseModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", serialize_by_alias=True)


class WorkflowInvokePayload(BaseModel):
    """Payload for invoking a workflow."""

    run_id: str = pydantic.Field(alias="runId")
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


class StepInvokePayload(BaseModel):
    """Payload for invoking a step within a workflow."""

    workflow_name: str = pydantic.Field(alias="workflowName")
    workflow_run_id: str = pydantic.Field(alias="workflowRunId")
    workflow_started_at: float = pydantic.Field(alias="workflowStartedAt")
    step_id: str = pydantic.Field(alias="stepId")
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
    can deliver messages to workflow/step endpoints.
    """

    health_check: Literal[True] = pydantic.Field(default=True, alias="__healthCheck")
    correlation_id: str = pydantic.Field(alias="correlationId")


QueuePayload: TypeAlias = WorkflowInvokePayload | StepInvokePayload | HealthCheckPayload


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
    input: list[bytes] | str | None = None
    output: list[bytes] | None = None
    error: StructuredError | None = None
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
    output: list[bytes] | None = None  # create run_completed event returns run without output
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
    input: list[bytes] | None = None
    output: list[bytes] | None = None
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
    output: list[bytes] | None = None
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


class BaseEvent(BaseModel):
    event_type: str = pydantic.Field(alias="eventType")
    correlation_id: str | None = pydantic.Field(
        default=None, alias="correlationId", exclude_if=lambda e: e is None
    )
    spec_version: Literal[1, 2] = pydantic.Field(
        default=2, alias="specVersion"
    )  # 1: legacy JSON, 2: devalue
    server_props: ServerProps | None = pydantic.Field(default=None, exclude=True)

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
    input: list[bytes]
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


class RunStartedEvent(BaseEvent):
    """
    Event created when a workflow run starts executing.
    Updates the run entity to status 'running'.
    """

    event_type: Literal["run_started"] = pydantic.Field(
        default="run_started",
        alias="eventType",
    )


class RunCompletedEventData(BaseModel):
    output: list[bytes]

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


class StepCreatedEventData(BaseModel):
    step_name: str = pydantic.Field(alias="stepName")
    input: list[bytes] | dict[str, Any]

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


class StepCompletedEventData(BaseModel):
    result: list[bytes] | Any = None

    def into_event(self, correlation_id: str) -> "StepCompletedEvent":
        return StepCompletedEvent(correlationId=correlation_id, eventData=self)


class StepCompletedEvent(BaseEvent):
    event_type: Literal["step_completed"] = pydantic.Field(
        default="step_completed",
        alias="eventType",
    )
    correlation_id: str = pydantic.Field(alias="correlationId")
    event_data: StepCompletedEventData = pydantic.Field(alias="eventData")


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


class Hook(BaseModel):
    run_id: str = pydantic.Field(alias="runId")
    hook_id: str = pydantic.Field(alias="hookId")
    token: str
    owner_id: str = pydantic.Field(alias="ownerId")
    project_id: str = pydantic.Field(alias="projectId")
    environment: str
    metadata: list[bytes] | None = None
    created_at: datetime = pydantic.Field(alias="createdAt")
    spec_version: int | None = pydantic.Field(default=None, alias="specVersion")
    is_webhook: bool | None = pydantic.Field(default=None, alias="isWebhook")


class HookCreatedEventData(BaseModel):
    token: str
    metadata: list[bytes] | None = pydantic.Field(default=None, exclude_if=lambda e: e is None)

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


class HookReceivedEventData(BaseModel):
    payload: list[bytes]

    def into_event(self, correlation_id: str) -> "HookReceivedEvent":
        return HookReceivedEvent(correlationId=correlation_id, eventData=self)


class HookReceivedEvent(BaseEvent):
    event_type: Literal["hook_received"] = pydantic.Field(
        default="hook_received",
        alias="eventType",
    )
    correlation_id: str = pydantic.Field(alias="correlationId")
    event_data: HookReceivedEventData = pydantic.Field(alias="eventData")


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


class EventResult(BaseModel):
    event: Event | None = None
    events: list[Event] | None = None
    run: WorkflowRun | None = None
    step: WorkflowStep | None = None
    hook: Any | None = None
    wait: Any | None = None


class HTTPRequest(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_header(self, name: str) -> str | None: ...

    @abc.abstractmethod
    async def get_body(self) -> bytes: ...


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


class QueueHandler(Protocol):
    async def __call__(
        self, message: Any, *, attempt: int, queue_name: str, message_id: str
    ) -> float | None: ...


class HTTPHandler(Protocol):
    async def __call__(self, request: HTTPRequest) -> HTTPResponse: ...


class World(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def get_deployment_id(self) -> str: ...

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
