import json
import os
import pathlib
import traceback
from datetime import UTC, datetime
from typing import Any, TypeVar

import cbor2
import pydantic
from vercel.workers import client as vqs_client

from .. import world as w
from ..ulid import monotonic_factory

MAX_DELAY_SECONDS = float(
    os.getenv("VERCEL_QUEUE_MAX_DELAY_SECONDS", "82800")
)  # 23 hours - leave 1h buffer before 24h retention limit
LOCAL_QUEUE_MAX_VISIBILITY = int(
    os.environ.get("WORKFLOW_LOCAL_QUEUE_MAX_VISIBILITY", "0")
) or float("inf")

T = TypeVar("T", bound=w.BaseModel)


def is_run_terminal(status: str) -> bool:
    return status in ["completed", "failed", "cancelled"]


def is_step_terminal(status: str) -> bool:
    return status in ["completed", "failed"]


def read_json(path: pathlib.Path, schema: type[T] | pydantic.TypeAdapter[T]) -> T | None:
    if path.exists():
        with path.open("rb") as f:
            data = cbor2.load(f)
        if isinstance(schema, pydantic.TypeAdapter):
            return schema.validate_python(data)
        else:
            return schema.model_validate(data)
    else:
        return None


def write_json(path: pathlib.Path, data: w.BaseModel | dict, *, overwrite: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise FileExistsError()
    if isinstance(data, w.BaseModel):
        data = data.model_dump()
    with path.open("wb") as f:
        cbor2.dump(data, f)


class LocalWorld(w.World):
    def __init__(self) -> None:
        self.monotonic_ulid = monotonic_factory()
        self.data_dir = pathlib.Path(os.getenv("WORKFLOW_LOCAL_DATA_DIR", ".workflow-data"))

    async def get_deployment_id(self) -> str:
        return ""

    async def queue(
        self,
        queue_name: str,
        message: w.QueuePayload,
        *,
        deployment_id: str | None = None,
        idempotency_key: str | None = None,
        **kwargs,
    ) -> str:
        payload = {
            "payload": message.model_dump(),
            "queueName": queue_name,
            "deploymentId": "<local>",
        }
        response = await vqs_client.send_async(
            "".join(char if char.isalnum() or char in "-_" else "-" for char in queue_name),
            payload,
            idempotency_key=idempotency_key,
            deployment_id="<local>",
        )
        return response["messageId"]

    def create_queue_handler(
        self, queue_name_prefix: w.QueuePrefix, handler: w.QueueHandler
    ) -> w.HTTPHandler:
        @vqs_client.subscribe(
            topic=(f"{queue_name_prefix}*", lambda t: bool(t and t.startswith(queue_name_prefix)))
        )
        async def async_handler(body: Any, meta: vqs_client.MessageMetadata) -> None:
            try:
                if not isinstance(body, dict):
                    raise ValueError("Invalid message body: expected a JSON object")
                if "payload" not in body:
                    raise ValueError("Invalid message body: missing 'payload' field")
                if "queueName" not in body:
                    raise ValueError("Invalid message body: missing 'queueName' field")
                queue_name = body["queueName"]
                payload = body["payload"]
                result = await handler(
                    payload,
                    queue_name=queue_name,
                    attempt=meta["deliveryCount"],
                    message_id=meta["messageId"],
                )
                if result is not None:
                    # Use delaySeconds approach: send new message with delay, then delete current
                    # Clamp to max delay (23h) - for longer sleeps, the workflow will chain
                    # multiple delayed messages until the full sleep duration has elapsed
                    delay_seconds = min(result, MAX_DELAY_SECONDS)

                    # Send new message with delay BEFORE acknowledging current message
                    # This ensures crash safety: if process dies after send but before ack,
                    # we may get a duplicate invocation but won't lose the scheduled wakeup
                    await self.queue(
                        queue_name,
                        payload,
                        deployment_id=body.get("deploymentId"),
                        delay_seconds=delay_seconds,
                    )
            except Exception:
                traceback.print_exc()
                raise

        async def http_handler(request: w.HTTPRequest) -> w.HTTPResponse:
            # Get request body
            body = await request.get_body()

            if not body:
                return w.HTTPResponse.json({"error": "Missing request body"}, status=400)

            # Get required headers
            queue_name = request.get_header("x-vqs-queue-name")
            message_id = request.get_header("x-vqs-message-id")
            attempt_str = request.get_header("x-vqs-message-attempt")

            if not queue_name or not message_id or not attempt_str:
                return w.HTTPResponse.json({"error": "Missing required headers"}, status=400)

            # Validate queue name prefix
            if not queue_name.startswith(queue_name_prefix):
                return w.HTTPResponse.json({"error": "Unhandled queue"}, status=400)

            # Validate attempt number
            try:
                attempt = int(attempt_str)
            except ValueError:
                return w.HTTPResponse.json(
                    {"error": "Invalid x-vqs-message-attempt header"}, status=400
                )

            # Deserialize the message body
            try:
                message = json.loads(body.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                return w.HTTPResponse.json({"error": f"Invalid JSON body: {e}"}, status=400)

            # Call the handler
            try:
                result = await handler(
                    message, attempt=attempt, queue_name=queue_name, message_id=message_id
                )

                # Handle timeout response
                timeout_seconds: float | None = None
                if result:
                    timeout_seconds = min(result, LOCAL_QUEUE_MAX_VISIBILITY)
                if timeout_seconds:
                    return w.HTTPResponse.json({"timeoutSeconds": timeout_seconds}, status=503)

                return w.HTTPResponse.json({"ok": True})
            except Exception as error:
                return w.HTTPResponse.json({"error": str(error)}, status=500)

        return http_handler

    async def runs_get(self, run_id: str) -> w.WorkflowRun:
        run_path = self.data_dir / "runs" / f"{run_id}.json"
        run = read_json(run_path, w.WorkflowRunAdaptor)
        if run is None:
            raise RuntimeError(f"Run {run_id} not found")
        return run

    async def steps_get(self, run_id: str, step_id: str) -> w.WorkflowStep:
        composite_key = f"{run_id}-{step_id}"
        step_path = self.data_dir / "steps" / f"{composite_key}.json"
        return read_json(step_path, w.WorkflowStepAdaptor)

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        event_id = f"evnt_{self.monotonic_ulid(None)}"
        now = datetime.now(UTC)

        if data.event_type == "run_created" and not run_id:
            effective_run_id = f"wrun_{self.monotonic_ulid(None)}"
        elif run_id is None:
            raise ValueError("runId is required for non-run_created events")
        else:
            effective_run_id = run_id

        current_run: w.WorkflowRun | None = None
        skip_run_validation_events = ["step_completed", "step_retrying"]
        if data.event_type != "run_created" and data.event_type not in skip_run_validation_events:
            run_path = self.data_dir / "runs" / f"{effective_run_id}.json"
            current_run = read_json(run_path, w.WorkflowRunAdaptor)

        if current_run and is_run_terminal(current_run.status):
            run_terminal_events = ["run_started", "run_completed", "run_failed"]

            if data.event_type == "run_cancelled" and current_run.status == "cancelled":
                event = w.EventAdaptor.validate_python(
                    data.model_dump()
                    | {
                        "runId": effective_run_id,
                        "eventId": event_id,
                        "createdAt": now,
                    }
                )
                composite_key = f"{effective_run_id}-{event_id}"
                event_path = self.data_dir / "events" / f"{composite_key}.json"
                write_json(event_path, event.model_dump() | event.server_props.model_dump())
                return w.EventResult(event=event, run=current_run)

            if data.event_type in run_terminal_events or data.event_data == "run_cancelled":
                raise RuntimeError(
                    f"Cannot transition run from terminal state {current_run.status}"
                )

            if data.event_type in ["step_created", "hook_created", "wait_created"]:
                raise RuntimeError(
                    f"Cannot create new entities on run in terminal state {current_run.status}"
                )

        validated_step: w.WorkflowStep | None = None
        step_events = ["step_started", "step_completed", "step_failed", "step_retrying"]
        if data.event_type in step_events and data.correlation_id:
            step_composite_key = f"{effective_run_id}-{data.correlation_id}"
            step_path = self.data_dir / "steps" / f"{step_composite_key}.json"
            validated_step = read_json(step_path, w.WorkflowStepAdaptor)

            if not validated_step:
                raise RuntimeError(f'Step "{data.correlation_id}" not found')

            if is_step_terminal(validated_step.status):
                raise RuntimeError(
                    f'Cannot modify step in terminal state "{validated_step.status}"'
                )

            if current_run and is_run_terminal(current_run.status):
                if validated_step.status != "running":
                    raise RuntimeError(
                        f'Cannot modify non-running step on run in terminal state "{current_run.status}"'
                    )

        event = w.EventAdaptor.validate_python(
            data.model_dump()
            | {
                "runId": effective_run_id,
                "eventId": event_id,
                "createdAt": now,
            }
        )
        run: w.WorkflowRun | None = None
        step: w.WorkflowStep | None = None

        if data.event_type == "run_created" and hasattr(data, "event_data"):
            run_data = data.event_data
            run = w.NonFinalWorkflowRun(
                runId=effective_run_id,
                deploymentId=run_data.deployment_id,
                status="pending",
                workflowName=run_data.workflow_name,
                specVersion=2,
                executionContext=run_data.execution_context,
                input=run_data.input,
                createdAt=now,
                updatedAt=now,
            )
            run_path = self.data_dir / "runs" / f"{effective_run_id}.json"
            write_json(run_path, run)

        elif data.event_type == "run_started":
            if current_run:
                run = w.NonFinalWorkflowRun(
                    runId=current_run.run_id,
                    deploymentId=current_run.deployment_id,
                    workflowName=current_run.workflow_name,
                    specVersion=current_run.spec_version,
                    executionContext=current_run.execution_context,
                    input=current_run.input,
                    createdAt=current_run.created_at,
                    expiredAt=current_run.expired_at,
                    status="running",
                    startedAt=current_run.started_at or now,
                    updatedAt=now,
                )
                run_path = self.data_dir / "runs" / f"{effective_run_id}.json"
                write_json(run_path, run, overwrite=True)

        elif data.event_type == "run_completed" and hasattr(data, "event_data"):
            completed_data = data.event_data
            if current_run:
                run = w.CompletedWorkflowRun(
                    runId=current_run.run_id,
                    deploymentId=current_run.deployment_id,
                    workflowName=current_run.workflow_name,
                    specVersion=current_run.spec_version,
                    executionContext=current_run.execution_context,
                    input=current_run.input,
                    createdAt=current_run.created_at,
                    expiredAt=current_run.expired_at,
                    startedAt=current_run.started_at,
                    status="completed",
                    output=completed_data.output,
                    completedAt=now,
                    updatedAt=now,
                )
                run_path = self.data_dir / "runs" / f"{effective_run_id}.json"
                write_json(run_path, run, overwrite=True)

        elif data.event_type == "run_failed" and hasattr(data, "event_data"):
            failed_data = data.event_data
            if isinstance(failed_data.error, str):
                error_msg = failed_data.error
            elif isinstance(failed_data.error, dict) and "message" in failed_data.error:
                error_msg = failed_data.error["message"]
            elif hasattr(failed_data.error, "message"):
                error_msg = failed_data.error.message
            else:
                error_msg = "Unknown error"
            if isinstance(failed_data.error, dict) and "stack" in failed_data.error:
                error_stack = failed_data.error["stack"]
            elif hasattr(failed_data.error, "stack"):
                error_stack = failed_data.error.stack
            else:
                error_stack = None
            if current_run:
                run = w.FailedWorkflowRun(
                    runId=current_run.run_id,
                    deploymentId=current_run.deployment_id,
                    workflowName=current_run.workflow_name,
                    specVersion=current_run.spec_version,
                    executionContext=current_run.execution_context,
                    input=current_run.input,
                    createdAt=current_run.created_at,
                    expiredAt=current_run.expired_at,
                    startedAt=current_run.started_at,
                    status="failed",
                    error=w.StructuredError(
                        message=error_msg,
                        stack=error_stack,
                        code=failed_data.code,
                    ),
                    completedAt=now,
                    updatedAt=now,
                )
                run_path = self.data_dir / "runs" / f"{effective_run_id}.json"
                write_json(run_path, run, overwrite=True)

        elif data.event_type == "run_cancelled":
            if current_run:
                run = w.CancelledWorkflowRun(
                    runId=current_run.run_id,
                    deploymentId=current_run.deployment_id,
                    workflowName=current_run.workflow_name,
                    specVersion=current_run.spec_version,
                    executionContext=current_run.execution_context,
                    input=current_run.input,
                    createdAt=current_run.created_at,
                    expiredAt=current_run.expired_at,
                    startedAt=current_run.started_at,
                    status="cancelled",
                    completedAt=now,
                    updatedAt=now,
                )
                run_path = self.data_dir / "runs" / f"{effective_run_id}.json"
                write_json(run_path, run, overwrite=True)

        elif data.event_type == "step_created" and hasattr(data, "event_data"):
            step_data = data.event_data
            step = w.NonFinalWorkflowStep(
                runId=effective_run_id,
                stepId=data.correlation_id,
                stepName=step_data.step_name,
                status="pending",
                input=step_data.input,
                attempt=0,
                createdAt=now,
                updatedAt=now,
                specVersion=2,
            )
            step_composite_key = f"{effective_run_id}-{data.correlation_id}"
            step_path = self.data_dir / "steps" / f"{step_composite_key}.json"
            write_json(step_path, step)

        elif data.event_type == "step_started":
            if validated_step:
                if validated_step.retry_after and validated_step.retry_after > now:
                    raise RuntimeError(
                        f'Cannot start step "{data.correlation_id}": retryAfter timestamp has not been reached yet'
                    )

                step_composite_key = f"{effective_run_id}-{data.correlation_id}"
                step_path = self.data_dir / "steps" / f"{step_composite_key}.json"
                step = w.NonFinalWorkflowStep.model_validate(
                    validated_step.model_dump()
                    | dict(
                        status="running",
                        startedAt=validated_step.started_at or now,
                        attempt=validated_step.attempt + 1,
                        retryAfter=None,
                        updatedAt=now,
                    )
                )
                write_json(step_path, step, overwrite=True)

        elif data.event_type == "step_completed" and hasattr(data, "event_data"):
            completed_data = data.event_data
            if validated_step:
                step_composite_key = f"{effective_run_id}-{data.correlation_id}"
                step_path = self.data_dir / "steps" / f"{step_composite_key}.json"
                step = w.CompletedWorkflowStep.model_validate(
                    validated_step.model_dump()
                    | dict(
                        status="completed",
                        output=completed_data.result,
                        completedAt=now,
                        updatedAt=now,
                    )
                )
                write_json(step_path, step, overwrite=True)

        elif data.event_type == "step_failed" and hasattr(data, "event_data"):
            failed_data = data.event_data
            if validated_step:
                step_composite_key = f"{effective_run_id}-{data.correlation_id}"
                step_path = self.data_dir / "steps" / f"{step_composite_key}.json"
                if isinstance(failed_data.error, str):
                    error_msg = failed_data.error
                elif isinstance(failed_data.error, dict) and "message" in failed_data.error:
                    error_msg = failed_data.error["message"]
                elif hasattr(failed_data.error, "message"):
                    error_msg = failed_data.error.message
                else:
                    error_msg = "Unknown error"
                if isinstance(failed_data.error, dict) and "stack" in failed_data.error:
                    error_stack = failed_data.error["stack"]
                elif hasattr(failed_data.error, "stack"):
                    error_stack = failed_data.error.stack
                else:
                    error_stack = None
                error = w.StructuredError(
                    message=error_msg,
                    stack=error_stack,
                )
                step = w.FailedWorkflowStep.model_validate(
                    validated_step.model_dump()
                    | dict(
                        status="failed",
                        error=error,
                        completedAt=now,
                        updatedAt=now,
                    )
                )
                write_json(step_path, step, overwrite=True)

        composite_key = f"{effective_run_id}-{event_id}"
        event_path = self.data_dir / "events" / f"{composite_key}.json"
        write_json(event_path, event.model_dump() | event.server_props.model_dump())

        return w.EventResult(
            event=event,
            run=run,
            step=step,
        )

    async def events_list(
        self,
        run_id: str,
        *,
        pagination: w.PaginationOptions | None = None,
    ) -> w.PaginatedResult[w.Event]:
        desc = False
        if pagination:
            if any([pagination.cursor, pagination.limit]):
                raise NotImplementedError()
            if pagination.sort_order == "desc":
                desc = True

        directory = self.data_dir / "events"
        items = [
            read_json(f, w.EventAdaptor)
            for f in directory.iterdir()
            if f.suffix == ".json" and f.stem.startswith(f"{run_id}-")
        ]
        items.sort(key=lambda item: (item.server_props.created_at, item.server_props.event_id), reverse=desc)
        return w.PaginatedResult(
            data=items,
            cursor=None,
            hasMore=False,
        )
