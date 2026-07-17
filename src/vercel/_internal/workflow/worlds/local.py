import contextlib
import hashlib
import json
import math
import os
import pathlib
import tempfile
import threading
import traceback
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timezone
from typing import Any, TypeVar, cast
from uuid import uuid4

import cbor2
import pydantic

import vercel.queue as vqs
import vercel.queue.embedded as vqs_embedded
from vercel._internal.polyfills import UTC

from .. import world as w
from ..ulid import monotonic_factory

MAX_DELAY_SECONDS = float(
    os.getenv("VERCEL_QUEUE_MAX_DELAY_SECONDS", "82800")
)  # 23 hours - leave 1h buffer before 24h retention limit
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


def atomic_write(path: str | os.PathLike[str], data: bytes, *, overwrite: bool = True) -> None:
    """Atomically write ``data`` to ``path``.

    Writes to a temp file in the same directory, then puts it in place
    with a single atomic syscall. If ``overwrite`` is True, an existing
    file is replaced; otherwise the write fails with ``FileExistsError``
    if ``path`` already exists.
    """
    directory = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=directory)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        if overwrite:
            os.replace(tmp, path)
        else:
            os.link(tmp, path)
            os.unlink(tmp)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def write_json(path: pathlib.Path, data: w.BaseModel | dict, *, overwrite: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Do an early check to avoid serializing stuff when we don't need to.
    # The exists check is not needed for correctness, though -- the real
    # check is in atomic_write, and so there is not a TOCTOU race.
    if path.exists() and not overwrite:
        raise w.EntityConflictError(f"File already exists: {path}")

    if isinstance(data, w.BaseModel):
        data = data.model_dump()
    try:
        atomic_write(path, cbor2.dumps(data), overwrite=overwrite)
    except FileExistsError:
        raise w.EntityConflictError(f"File already exists: {path}") from None


def write_exclusive(path: pathlib.Path, data: str) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        atomic_write(path, data.encode(), overwrite=False)
    except FileExistsError:
        return False
    else:
        return True


def _json_string(value: str) -> bytes:
    return json.dumps(value).encode("utf-8")


async def _chain_async_bytes(*chunks: bytes | AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    for chunk in chunks:
        if isinstance(chunk, bytes):
            yield chunk
        else:
            async for part in chunk:
                yield part


def _local_queue_delivery(
    request: w.HTTPRequest,
    *,
    queue_name: str,
    topic: str,
    consumer_group: str,
) -> tuple[AsyncIterator[bytes], dict[str, str]]:
    body = _chain_async_bytes(
        b'{"payload":',
        request.aiter_bytes(),
        b',"queueName":',
        _json_string(queue_name),
        b',"deploymentId":"<local>"}',
    )
    return body, {
        "ce-type": "com.vercel.queue.v2beta",
        "ce-vqsqueuename": topic,
        "ce-vqsconsumergroup": consumer_group,
        "ce-vqsmessageid": request.headers.get("x-vqs-message-id") or f"msg_{uuid4()}",
        "ce-vqsreceipthandle": "local",
        "ce-vqsdeliverycount": request.headers.get("x-vqs-message-attempt") or "1",
        "ce-vqscreatedat": datetime.now(timezone.utc).isoformat(),
        "content-type": request.headers.get("content-type") or "application/json",
    }


class LocalWorld(w.World):
    def __init__(self) -> None:
        self.monotonic_ulid = monotonic_factory()
        self.data_dir = pathlib.Path(os.getenv("WORKFLOW_LOCAL_DATA_DIR", ".workflow-data"))
        self._embedded_queue_service_cm: AbstractAsyncContextManager[Any] | None
        self._embedded_queue_service: Any | None
        self._queue_client: vqs.QueueClient | None
        if os.getenv("VERCEL_QUEUE_BASE_URL"):
            self._queue_mode = "external"
            self._embedded_queue_service_cm = None
            self._embedded_queue_service = None
            self._queue_client = vqs.QueueClient(region="iad1", deployment=vqs.ALL_DEPLOYMENTS)
        else:
            self._queue_mode = "embedded"
            self._embedded_queue_service_cm = vqs_embedded.embedded_queue_service()
            self._embedded_queue_service = None
            self._queue_client = None
        self._queue_callbacks: list[Any] = []
        # Per-run mutex serializing events_create, which does some
        # read-modify-writes in some cases.
        #
        # We certainly *could* do more fine-grained locking but I
        # don't think it would really help.
        self._run_locks: dict[str, threading.Lock] = {}

    def _run_lock(self, run_id: str) -> threading.Lock:
        # dict.setdefault is atomic, so concurrent callers for the same run_id
        # converge on one lock without a separate guard lock.
        return self._run_locks.setdefault(run_id, threading.Lock())

    def delete_all_hooks_for_run(self, run_id: str) -> None:
        hooks_dir = self.data_dir / "hooks"
        if not hooks_dir.exists():
            return
        for hook_path in hooks_dir.iterdir():
            if hook_path.suffix != ".json":
                continue
            hook = read_json(hook_path, w.Hook)
            if hook is not None and hook.run_id == run_id:
                hashed_token = hashlib.sha256(hook.token.encode()).hexdigest()
                constraint_path = hooks_dir / "tokens" / f"{hashed_token}.json"
                constraint_path.unlink(missing_ok=True)
                hook_path.unlink(missing_ok=True)

    async def get_deployment_id(self) -> str:
        return ""

    async def _get_queue_client(self) -> vqs.QueueClient:
        if self._queue_client is not None:
            return self._queue_client

        service_cm = cast(
            "AbstractAsyncContextManager[Any]",
            self._embedded_queue_service_cm,
        )
        self._embedded_queue_service = await service_cm.__aenter__()
        self._queue_client = self._embedded_queue_service.get_async_client()
        return self._queue_client

    async def aclose(self) -> None:
        if self._embedded_queue_service_cm is not None and self._embedded_queue_service is not None:
            await self._embedded_queue_service_cm.__aexit__(None, None, None)
            self._embedded_queue_service = None
            self._queue_client = None

    async def queue(
        self,
        queue_name: str,
        message: w.QueuePayload,
        *,
        deployment_id: str | None = None,
        idempotency_key: str | None = None,
        delay_seconds: float | None = None,
        **kwargs,
    ) -> str:
        payload = {
            "payload": message.model_dump(),
            "queueName": queue_name,
            "deploymentId": "<local>",
        }
        client = await self._get_queue_client()
        message_id = await client.send(
            vqs.sanitize_name(queue_name),
            payload,
            idempotency_key=idempotency_key,
            delay=max(1, math.ceil(delay_seconds)) if delay_seconds is not None else None,
        )
        return message_id or "msg_deferred"

    def create_queue_handler(
        self, queue_name_prefix: w.QueuePrefix, handler: w.QueueHandler
    ) -> w.HTTPHandler:
        consumer_group = f"local_{queue_name_prefix.rstrip('_')}"

        async def async_handler(message: vqs.Message[Any]) -> None:
            try:
                body = message.payload
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
                    attempt=message.metadata.delivery_count,
                    message_id=message.metadata.message_id,
                )
                if result is not None:
                    # Use delaySeconds approach: send new message with delay, then delete current
                    # Clamp to max delay (23h) - for longer sleeps, the workflow will chain
                    # multiple delayed messages until the full sleep duration has elapsed
                    delay_seconds = min(result.delay_seconds, MAX_DELAY_SECONDS)

                    # Send new message with delay BEFORE acknowledging current message
                    # This ensures crash safety: if process dies after send but before ack,
                    # we may get a duplicate invocation but won't lose the scheduled wakeup
                    await self.queue(
                        queue_name,
                        w.QueuePayloadAdaptor.validate_python(payload),
                        deployment_id=body.get("deploymentId"),
                        delay_seconds=delay_seconds,
                        idempotency_key=result.idempotency_key,
                    )
                if message.metadata.receipt_handle == "local":
                    # Local HTTP deliveries use a synthetic receipt handle so
                    # accept_and_handle can parse them like VQS pushes, but
                    # there is no real queue lease to acknowledge.
                    raise vqs.Handoff()
            except Exception as e:
                if not isinstance(e, vqs.QueueDirective):
                    traceback.print_exc()
                raise

        topic_prefix = vqs.sanitize_name(queue_name_prefix)
        vqs.subscribe(topic=f"{topic_prefix}*", consumer_group=consumer_group)(async_handler)
        self._queue_callbacks.append(async_handler)

        async def http_handler(request: w.HTTPRequest) -> w.HTTPResponse:
            queue_name_raw = request.headers.get("x-vqs-queue-name")

            if not queue_name_raw:
                return w.HTTPResponse.json({"error": "Missing required headers"}, status=400)

            queue_name = queue_name_raw
            topic = str(vqs.sanitize_name(queue_name))

            # Validate queue name prefix
            if not queue_name.startswith(queue_name_prefix):
                return w.HTTPResponse.json({"error": "Unhandled queue"}, status=400)

            body, headers = _local_queue_delivery(
                request,
                queue_name=queue_name,
                topic=topic,
                consumer_group=consumer_group,
            )

            try:
                client = await self._get_queue_client()
                await client.accept_and_handle(body, headers)
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
        step = read_json(step_path, w.WorkflowStepAdaptor)
        if step is None:
            raise RuntimeError(f"Step {step_id} not found in run {run_id}")
        return step

    async def hooks_get_by_token(self, token: str) -> w.Hook:
        hooks_dir = self.data_dir / "hooks"
        if hooks_dir.exists():
            for hook_path in hooks_dir.iterdir():
                if hook_path.suffix != ".json":
                    continue
                hook = read_json(hook_path, w.Hook)
                if hook is not None and hook.token == token:
                    return hook
        raise w.HookNotFoundError(token=token)

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        # run_created has no existing entity to race on — its create is guarded by
        # the atomic write in write_json. Every other event reads-checks-writes an
        # existing run/step, so serialize those per run. The body is synchronous,
        # so holding a threading.Lock across it is safe and brief.
        if run_id is None:
            return self._events_create_impl(run_id, data)
        with self._run_lock(run_id):
            return self._events_create_impl(run_id, data)

    def _events_create_impl(self, run_id: str | None, data: w.Event) -> w.EventResult:
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

            if data.event_type in run_terminal_events or data.event_type == "run_cancelled":
                raise w.EntityConflictError(
                    f"Cannot transition run from terminal state {current_run.status}"
                )

            if data.event_type in ["step_created", "hook_created", "wait_created"]:
                raise w.EntityConflictError(
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
                raise w.EntityConflictError(
                    f'Cannot modify step in terminal state "{validated_step.status}"'
                )

            if current_run and is_run_terminal(current_run.status):
                if validated_step.status != "running":
                    raise w.EntityConflictError(
                        f"Cannot modify non-running step on run in terminal state "
                        f'"{current_run.status}"'
                    )

        if data.event_type in w.HOOK_EVENTS_REQUIRING_EXISTENCE and data.correlation_id:
            hook_path = self.data_dir / "hooks" / f"{data.correlation_id}.json"
            existing_hook = read_json(hook_path, w.Hook)
            if existing_hook is None:
                # Already disposed (or never created). Mirrors the backend's 404.
                raise w.HookNotFoundError(hook_id=data.correlation_id)

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
                self.delete_all_hooks_for_run(effective_run_id)

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
                self.delete_all_hooks_for_run(effective_run_id)

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
                self.delete_all_hooks_for_run(effective_run_id)

        elif data.event_type == "step_created" and hasattr(data, "event_data"):
            step_data = data.event_data
            assert isinstance(step_data.input, list)
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
                    raise w.TooEarlyError(
                        f'Cannot start step "{data.correlation_id}": '
                        f"retryAfter timestamp has not been reached yet",
                        retry_after=math.ceil((validated_step.retry_after - now).total_seconds()),
                    )

                step_composite_key = f"{effective_run_id}-{data.correlation_id}"
                step_path = self.data_dir / "steps" / f"{step_composite_key}.json"
                step = w.NonFinalWorkflowStep.model_validate(
                    validated_step.model_dump()
                    | {
                        "status": "running",
                        "startedAt": validated_step.started_at or now,
                        "attempt": validated_step.attempt + 1,
                        "retryAfter": None,
                        "updatedAt": now,
                    }
                )
                write_json(step_path, step, overwrite=True)

        elif data.event_type == "step_completed" and hasattr(data, "event_data"):
            if validated_step:
                step_composite_key = f"{effective_run_id}-{data.correlation_id}"
                step_path = self.data_dir / "steps" / f"{step_composite_key}.json"
                step = w.CompletedWorkflowStep.model_validate(
                    validated_step.model_dump()
                    | {
                        "status": "completed",
                        "output": data.event_data.result,
                        "completedAt": now,
                        "updatedAt": now,
                    }
                )
                write_json(step_path, step, overwrite=True)

        elif data.event_type == "step_failed" and hasattr(data, "event_data"):
            step_failed_data = data.event_data
            if validated_step:
                step_composite_key = f"{effective_run_id}-{data.correlation_id}"
                step_path = self.data_dir / "steps" / f"{step_composite_key}.json"
                if isinstance(step_failed_data.error, str):
                    error_msg = step_failed_data.error
                elif (
                    isinstance(step_failed_data.error, dict) and "message" in step_failed_data.error
                ):
                    error_msg = step_failed_data.error["message"]
                elif hasattr(step_failed_data.error, "message"):
                    error_msg = step_failed_data.error.message
                else:
                    error_msg = "Unknown error"
                if isinstance(step_failed_data.error, dict) and "stack" in step_failed_data.error:
                    error_stack = step_failed_data.error["stack"]
                elif hasattr(step_failed_data.error, "stack"):
                    error_stack = step_failed_data.error.stack
                else:
                    error_stack = None
                error = w.StructuredError(
                    message=error_msg,
                    stack=error_stack,
                )
                step = w.FailedWorkflowStep.model_validate(
                    validated_step.model_dump()
                    | {
                        "status": "failed",
                        "error": error,
                        "completedAt": now,
                        "updatedAt": now,
                    }
                )
                write_json(step_path, step, overwrite=True)

        elif data.event_type == "hook_created" and hasattr(data, "event_data"):
            hook_data = data.event_data
            hashed_token = hashlib.sha256(hook_data.token.encode()).hexdigest()
            constraint_path = self.data_dir / "hooks" / "tokens" / f"{hashed_token}.json"
            token_claimed = write_exclusive(
                constraint_path,
                json.dumps(
                    {
                        "token": hook_data.token,
                        "hookId": data.correlation_id,
                        "runId": effective_run_id,
                    }
                ),
            )
            if not token_claimed:
                existing_claim = json.loads(constraint_path.read_text())
                if (
                    existing_claim["runId"] == effective_run_id
                    and existing_claim["hookId"] == data.correlation_id
                ):
                    # Same hook re-claiming its own token (replay re-issue or
                    # crash recovery). Idempotent, not a cross-workflow conflict.
                    raise w.EntityConflictError(
                        f'Hook "{data.correlation_id}" has already been created'
                    )
                conflict_event = w.HookConflictEvent(
                    correlationId=data.correlation_id,
                    eventData=w.HookConflictEventData(token=hook_data.token),
                    server_props=w.ServerProps(
                        runId=effective_run_id,
                        eventId=event_id,
                        createdAt=now,
                    ),
                )
                assert conflict_event.server_props is not None
                composite_key = f"{effective_run_id}-{event_id}"
                event_path = self.data_dir / "events" / f"{composite_key}.json"
                write_json(
                    event_path,
                    conflict_event.model_dump() | conflict_event.server_props.model_dump(),
                )
                return w.EventResult(
                    event=conflict_event,
                    run=run,
                    step=step,
                    hook=None,
                )
            hook = w.Hook(
                runId=effective_run_id,
                hookId=data.correlation_id,
                token=hook_data.token,
                metadata=hook_data.metadata,
                ownerId="local-owner",
                projectId="local-project",
                environment="local",
                createdAt=now,
                specVersion=2,
                isWebhook=False,
            )
            hook_path = self.data_dir / "hooks" / f"{data.correlation_id}.json"
            write_json(hook_path, hook)

        elif data.event_type == "wait_completed" and data.correlation_id:
            wait_lock = (
                self.data_dir
                / ".locks"
                / "waits"
                / f"{effective_run_id}-{data.correlation_id}.completed"
            )
            if not write_exclusive(wait_lock, ""):
                raise w.EntityConflictError(f'Wait "{data.correlation_id}" already completed')

        elif data.event_type == "hook_disposed":
            # The existence check above already rejects an already-disposed hook
            # with HookNotFoundError. This lock guards the narrow cross-process
            # window where two invocations both still see the hook present: the
            # loser gets EntityConflictError (swallowed by the runtime) instead
            # of double-deleting and writing a duplicate hook_disposed event. The
            # in-process run lock can't serialize separate processes.
            dispose_lock = self.data_dir / ".locks" / "hooks" / f"{data.correlation_id}.disposed"
            if not write_exclusive(dispose_lock, ""):
                raise w.EntityConflictError(f'Hook "{data.correlation_id}" already disposed')
            hook_path = self.data_dir / "hooks" / f"{data.correlation_id}.json"
            existing_hook = read_json(hook_path, w.Hook)
            if existing_hook is not None:
                hashed_token = hashlib.sha256(existing_hook.token.encode()).hexdigest()
                disposed_constraint_path = (
                    self.data_dir / "hooks" / "tokens" / f"{hashed_token}.json"
                )
                disposed_constraint_path.unlink(missing_ok=True)
            hook_path.unlink(missing_ok=True)

        composite_key = f"{effective_run_id}-{event_id}"
        event_path = self.data_dir / "events" / f"{composite_key}.json"
        if event.server_props:
            write_json(event_path, event.model_dump() | event.server_props.model_dump())
        else:
            write_json(event_path, event.model_dump())

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
        # Filter out None items and ensure all items have server_props
        valid_items = [item for item in items if item is not None and item.server_props is not None]
        valid_items.sort(
            key=lambda item: (item.server_props.created_at, item.server_props.event_id),  # type: ignore[union-attr]
            reverse=desc,
        )
        return w.PaginatedResult(
            data=valid_items,
            cursor=None,
            hasMore=False,
        )
