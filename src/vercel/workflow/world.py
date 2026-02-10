import abc
import dataclasses
import json
import os
from datetime import datetime
from typing import (
    Annotated,
    Any,
    Literal,
    Protocol,
    Self,
    TypeVar,
    overload,
)

import pydantic

T = TypeVar("T")
type QueuePrefix = Literal["__wkf_step_", "__wkf_workflow_"]
# OpenTelemetry trace context for distributed tracing
type TraceCarrier = dict[str, str]


class BaseModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", serialize_by_alias=True)


class WorkflowInvokePayload(BaseModel):
    """Payload for invoking a workflow."""

    run_id: str = pydantic.Field(serialization_alias="runId")
    trace_carrier: TraceCarrier | None = pydantic.Field(
        default=None, serialization_alias="traceCarrier", exclude_if=lambda e: e is None
    )
    requested_at: datetime | None = pydantic.Field(
        default=None, serialization_alias="requestedAt", exclude_if=lambda e: e is None
    )


class StepInvokePayload(BaseModel):
    """Payload for invoking a step within a workflow."""

    workflow_name: str = pydantic.Field(serialization_alias="workflowName")
    workflow_run_id: str = pydantic.Field(serialization_alias="workflowRunId")
    workflow_started_at: float = pydantic.Field(serialization_alias="workflowStartedAt")
    step_id: str = pydantic.Field(serialization_alias="stepId")
    trace_carrier: TraceCarrier | None = pydantic.Field(
        default=None, serialization_alias="traceCarrier", exclude_if=lambda e: e is None
    )
    requested_at: datetime | None = pydantic.Field(
        default=None, serialization_alias="requestedAt", exclude_if=lambda e: e is None
    )


class HealthCheckPayload(BaseModel):
    """
    Health check payload - used to verify that the queue pipeline
    can deliver messages to workflow/step endpoints.
    """

    health_check: Literal[True] = pydantic.Field(default=True, serialization_alias="__healthCheck")
    correlation_id: str = pydantic.Field(serialization_alias="correlationId")


type QueuePayload = WorkflowInvokePayload | StepInvokePayload | HealthCheckPayload


class StructuredError(BaseModel):
    message: str
    stack: str | None = None
    code: str | None = None


type WorkflowRunStatus = Literal["pending", "running", "completed", "failed", "cancelled"]


class _ContextWrapper[T]:
    def __init__(self, value: T):
        self.value = value

    def __getattr__(self, item):
        return getattr(self.value, item)

    def __getitem__(self, item):
        return self.value[item]


class BaseWorkflowRun(BaseModel):
    run_id: str = pydantic.Field(alias="runId")
    status: WorkflowRunStatus
    deployment_id: str = pydantic.Field(alias="deploymentId")
    workflow_name: str = pydantic.Field(alias="workflowName")
    # Optional in database for backwards compatibility, defaults to 1 (legacy) when reading
    spec_version: int | None = pydantic.Field(default=None, alias="specVersion")
    execution_context: dict[str, Any] | None = pydantic.Field(
        default=None, alias="executionContext"
    )
    input: bytes
    output: bytes | None = None
    error: StructuredError | None = None
    expired_at: datetime | None = pydantic.Field(default=None, alias="expiredAt")
    started_at: datetime | None = pydantic.Field(default=None, alias="startedAt")
    completed_at: datetime | None = pydantic.Field(default=None, alias="completedAt")
    created_at: datetime = pydantic.Field(alias="createdAt")
    updated_at: datetime = pydantic.Field(alias="updatedAt")

    # @pydantic.model_validator(mode="wrap")
    # @classmethod
    # def discriminate(
    #     cls,
    #     data: Any,
    #     handler: pydantic.ModelWrapValidatorHandler[Self],
    #     info: pydantic.ValidationInfo,
    # ) -> Self:
    #     if isinstance(info.context, _ContextWrapper):
    #         return handler(data)
    #
    #     config = info.config or {}
    #     args = {"context": _ContextWrapper(info.context)}
    #     if "strict" in config:
    #         args["strict"] = config["strict"]
    #     if "extra_fields_behavior" in config:
    #         args["extra"] = config["extra_fields_behavior"]
    #     if "validate_by_alias" in config:
    #         args["by_alias"] = config["validate_by_alias"]
    #     if "validate_by_name" in config:
    #         args["by_name"] = config["validate_by_name"]
    #     if info.mode == "python":
    #         if "from_attributes" in config:
    #             args["from_attributes"] = config["from_attributes"]
    #         return WorkflowRunAdaptor.validate_python(data, **args)
    #     else:
    #         return WorkflowRunAdaptor.validate_json(data, **args)


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
    output: bytes
    error: None = None
    completed_at: datetime = pydantic.Field(alias="completedAt")


class FailedWorkflowRun(BaseWorkflowRun):
    status: Literal["failed"]
    output: None = None
    error: StructuredError
    completed_at: datetime = pydantic.Field(alias="completedAt")


type WorkflowRun = Annotated[
    NonFinalWorkflowRun | CancelledWorkflowRun | CompletedWorkflowRun | FailedWorkflowRun,
    pydantic.Field(discriminator="status"),
]
WorkflowRunAdaptor: pydantic.TypeAdapter[WorkflowRun] = pydantic.TypeAdapter(WorkflowRun)


class ServerProps(BaseModel):
    run_id: str = pydantic.Field(alias="runId")
    event_id: str = pydantic.Field(alias="eventId")
    created_at: datetime = pydantic.Field(alias="createdAt")


class BaseEvent(BaseModel):
    event_type: str = pydantic.Field(alias="eventType")
    correlation_id: str | None = pydantic.Field(
        default=None, alias="correlationId", exclude_if=lambda e: e is None
    )
    spec_version: Literal[1, 2] | None = pydantic.Field(
        default=None, alias="specVersion", exclude_if=lambda e: e is None
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


class RunStartedEvent(BaseEvent):
    """
    Event created when a workflow run starts executing.
    Updates the run entity to status 'running'.
    """

    event_type: Literal["run_started"] = pydantic.Field(
        default="run_started",
        alias="eventType",
    )


class WaitCreatedEventData(BaseModel):
    resume_at: datetime = pydantic.Field(alias="resumeAt")


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


type CreateEventRequest = RunStartedEvent | WaitCompletedEvent
type Event = Annotated[
    RunCreatedEvent | CreateEventRequest, pydantic.Field(discriminator="event_type")
]
EventAdaptor: pydantic.TypeAdapter[Event] = pydantic.TypeAdapter(Event)


class PaginationOptions(BaseModel):
    limit: int | None = pydantic.Field(default=None, exclude_if=lambda e: not e)
    cursor: str | None = pydantic.Field(default=None, exclude_if=lambda e: not e)
    sort_order: Literal["asc", "desc"] | None = pydantic.Field(
        default=None, serialization_alias="sortOrder", exclude_if=lambda e: e is None
    )


class PaginatedResult[T](BaseModel):
    data: list[T]
    cursor: str | None
    has_more: bool = pydantic.Field(alias="hasMore")


class EventResult(BaseModel):
    event: Event | None = None
    run: WorkflowRun | None = None


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
