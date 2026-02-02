import abc
import json
from datetime import datetime
from typing import Any, Literal, NotRequired, Protocol, TypeAlias, TypedDict

QueuePrefix: TypeAlias = Literal["__wkf_step_", "__wkf_workflow_"]

# OpenTelemetry trace context for distributed tracing
TraceCarrier: TypeAlias = dict[str, str]


class WorkflowInvokePayload(TypedDict):
    """Payload for invoking a workflow."""

    run_id: str
    trace_carrier: NotRequired[TraceCarrier]
    requested_at: NotRequired[datetime]


class StepInvokePayload(TypedDict):
    """Payload for invoking a step within a workflow."""

    workflow_name: str
    workflow_run_id: str
    workflow_started_at: float
    step_id: str
    trace_carrier: NotRequired[TraceCarrier]
    requested_at: NotRequired[datetime]


class HealthCheckPayload(TypedDict):
    """
    Health check payload - used to verify that the queue pipeline
    can deliver messages to workflow/step endpoints.
    """

    _health_check: Literal[True]
    correlation_id: str


QueuePayload: TypeAlias = WorkflowInvokePayload | StepInvokePayload | HealthCheckPayload


class HTTPRequest(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_header(self, name: str) -> str | None: ...

    @abc.abstractmethod
    async def get_body(self) -> bytes | None: ...


class HTTPResponse:
    status: int
    body: bytes
    headers: dict[str, str]

    @classmethod
    def json(cls, data: Any, *, status: int = 200) -> "HTTPResponse":
        """Create a JSON response with the given data and status code."""
        response = cls()
        response.status = status
        response.body = json.dumps(data).encode("utf-8")
        response.headers = {"content-type": "application/json"}
        return response


class QueueHandler(Protocol):
    async def __call__(
        self, message: Any, *, attempt: int, queue_name: str, message_id: str
    ) -> float | None: ...


class HTTPHandler(Protocol):
    async def __call__(self, request: HTTPRequest) -> HTTPResponse: ...


class World(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def queue(
        self,
        queue_name: str,
        message: QueuePayload,
        *,
        deployment_id: str | None = None,
        idempotency_key: str | None = None,
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


the_world: World | None = None


def create_world() -> World:
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
