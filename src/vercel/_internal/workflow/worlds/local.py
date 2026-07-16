import asyncio
import contextlib
import hashlib
import json
import logging
import math
import os
import pathlib
import tempfile
import threading
import traceback
from datetime import datetime
from typing import Any, TypeVar

import cbor2
import pydantic

from vercel._internal.polyfills import UTC
import vercel.queue as vqs
from vercel.queue._internal.constants import (
    HEADER_CONTENT_TYPE,
    VQS_HEADER_DELAY_SECONDS,
    VQS_HEADER_DEPLOYMENT_ID,
    VQS_HEADER_IDEMPOTENCY_KEY,
)
from vercel.queue._internal.embedded import (
    EmbeddedQueueDispatcher,
    create_embedded_queue_app,
)
from vercel.queue._internal.names import SanitizedName

from .. import world as w
from ..ulid import monotonic_factory

logger = logging.getLogger("vercel.workflow")

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


class LocalWorld(w.World):
    def __init__(self) -> None:
        self.monotonic_ulid = monotonic_factory()
        self.data_dir = pathlib.Path(os.getenv("WORKFLOW_LOCAL_DATA_DIR", ".workflow-data"))
        # Per-run mutex serializing events_create, which does some
        # read-modify-writes in some cases.
        #
        # We certainly *could* do more fine-grained locking but I
        # don't think it would really help.
        self._run_locks: dict[str, threading.Lock] = {}

        # Embedded queue: in-process message storage and dispatch.
        # Uses the same EmbeddedQueueAsgiApp + EmbeddedQueueDispatcher pattern
        # as vercel-queue's embedded_queue_service(), with the dispatcher
        # delivering messages through QueueClient.accept_and_handle() to
        # handlers registered via @subscribe.
        self._eq_app = create_embedded_queue_app()
        self._eq_server = self._eq_app.server
        self._eq_dispatcher = EmbeddedQueueDispatcher(
            self._eq_server,
            client_factory=lambda: self._eq_app.get_async_client(),
        )
        self._dispatcher_started = False
        self._recovery_pending = False
        # Strong references to @subscribe handlers to prevent GC.
        self._subscriber_refs: list[object] = []

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

    def _ensure_dispatcher_running(self) -> None:
        """Lazily start the embedded queue dispatcher on first use.

        Must be called from an async context (there must be a running
        event loop).  Also kicks off deferred recovery if pending.
        """
        if self._dispatcher_started:
            return
        self._dispatcher_started = True
        loop = asyncio.get_running_loop()
        loop.create_task(self._eq_dispatcher.run())
        self._eq_dispatcher.wake()

        # Recovery is deferred to here because create_queue_handler()
        # runs at module import time when no event loop exists yet.
        if self._recovery_pending and not os.getenv("WORKFLOW_LOCAL_DISABLE_RECOVERY"):
            self._recovery_pending = False

            async def _recover() -> None:
                try:
                    await self._resume_pending_runs()
                except Exception as e:
                    logger.warning("Local run recovery failed: %r", e)

            loop.create_task(_recover())

    async def _resume_pending_runs(self) -> None:
        """Re-enqueue runs left mid-flight by a server restart.

        The embedded queue is in-memory, so a ``sleep``'s delayed wake-up
        message is lost on restart, stranding the run in ``running``.
        Re-invoking the run lets ``workflow_handler`` turn any elapsed
        wait into a ``wait_completed`` and continue. Replay is idempotent,
        so re-invoking a healthy run is harmless.
        """
        runs_dir = self.data_dir / "runs"
        if not runs_dir.exists():
            return

        for run_file in runs_dir.glob("*.json"):
            try:
                run = await self.runs_get(run_file.stem)
            except Exception:
                continue
            if run.status in ("pending", "running"):
                try:
                    # Recover the namespace from the run's execution context
                    # so the re-enqueue targets the correct namespaced topic.
                    exec_ctx = run.execution_context or {}
                    ns = exec_ctx.get("queueNamespace")
                    queue_name = w.get_queue_name("workflow", run.workflow_name, ns)
                    await self.queue(
                        queue_name,
                        w.WorkflowInvokePayload(runId=run.run_id),
                    )
                except Exception as e:
                    logger.warning("Failed to re-enqueue stranded run %s: %r", run.run_id, e)

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
        self._ensure_dispatcher_running()

        payload = {
            "payload": message.model_dump(),
            "queueName": queue_name,
            "deploymentId": "<local>",
        }
        payload_bytes = json.dumps(payload).encode()

        headers: dict[str, str] = {
            HEADER_CONTENT_TYPE: "application/json",
            VQS_HEADER_DEPLOYMENT_ID: "<local>",
        }
        if idempotency_key is not None:
            headers[VQS_HEADER_IDEMPOTENCY_KEY] = idempotency_key
        if delay_seconds is not None:
            headers[VQS_HEADER_DELAY_SECONDS] = str(max(1, math.ceil(delay_seconds)))

        stored = self._eq_server.put(queue_name, payload_bytes, headers)
        return stored.message_id

    def create_queue_handler(
        self, queue_name_prefix: w.QueuePrefix, handler: w.QueueHandler
    ) -> w.HTTPHandler:
        # Sanitize prefix for use as consumer group name.
        # Wrap in SanitizedName so @subscribe doesn't re-encode underscores.
        consumer_group = SanitizedName(
            "".join(
                char if char.isalnum() or char in "-_" else "-"
                for char in f"wkf-{queue_name_prefix}"
            )
        )

        # Register a @subscribe handler — the standard vercel-queue subscriber
        # mechanism. The embedded dispatcher will deliver messages through
        # QueueClient.accept_and_handle() → call_subscribers() → this handler.
        @vqs.subscribe(
            topic=f"{queue_name_prefix}*",
            consumer_group=consumer_group,
        )
        async def async_handler(message: Any) -> None:
            if isinstance(message, vqs.Message):
                body = message.payload
                meta_delivery_count = message.metadata.delivery_count
                meta_message_id = str(message.metadata.message_id)
            else:
                body = message
                meta_delivery_count = 1
                meta_message_id = ""

            try:
                if isinstance(body, (bytes, bytearray)):
                    if body:
                        body = json.loads(body)
                    else:
                        return  # empty body from delayed re-delivery; skip
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
                    attempt=meta_delivery_count,
                    message_id=meta_message_id,
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
            except Exception:
                traceback.print_exc()
                raise

        # Keep a strong reference so the subscriber isn't garbage-collected
        # (the vercel-queue registry uses weak references).
        self._subscriber_refs.append(async_handler)

        # Mark recovery as pending on first workflow-topic registration.
        # Actual recovery is deferred to _ensure_dispatcher_running() which
        # runs in an async context (the event loop may not exist at import time).
        # Topic prefixes are "__wkf_workflow_" (no namespace) or
        # "__{ns}_wkf_workflow_" (with namespace).
        if "_wkf_workflow_" in queue_name_prefix and not self._recovery_pending:
            self._recovery_pending = True

        # Return an HTTP handler for backward compatibility (e.g. vercel dev
        # delivering messages over HTTP).  In embedded mode this is unused.
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
                if result is not None:
                    timeout_seconds = min(result.delay_seconds, LOCAL_QUEUE_MAX_VISIBILITY)
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
