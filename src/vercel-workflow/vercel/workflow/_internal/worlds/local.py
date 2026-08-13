import asyncio
import base64
import contextlib
import hashlib
import json
import math
import os
import pathlib
import tempfile
import threading
import traceback
from collections.abc import AsyncGenerator, AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from typing import Any, TypeVar, cast
from uuid import uuid4

import pydantic

import vercel.queue as vqs
import vercel.queue.embedded as vqs_embedded
from vercel._internal.core.polyfills import UTC

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


# Marker the TypeScript `world-local` package uses to smuggle binary payloads
# through JSON. See its `jsonReplacer` / `jsonReviver`.
UINT8ARRAY_TYPE_TAG = "Uint8Array"

# A chunk file is its EOF flag byte followed by the payload. The flag is what
# marks the end of a stream: `close()` appends a chunk carrying it and nothing
# else, so a reader can tell "closed" from "nothing written yet" without a
# separate metadata file to keep in sync.
_EOF_MARKER = 1

# How often a live reader re-lists a stream's directory. The writer may be in
# another process -- `vercel dev` serving a run while a test tails it -- so the
# filesystem is the only channel that always works.
_POLL_INTERVAL_SECONDS = 0.1


class RunStreams(w.BaseModel):
    streams: list[str]


def _read_chunk(path: pathlib.Path) -> tuple[bool, bytes]:
    """A chunk file's EOF flag and payload.

    A missing file reads as an empty non-EOF chunk: a live reader lists the
    directory and then opens what it found, and a concurrent writer's atomic
    replace can retire a name in between.
    """
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return False, b""
    if not raw:
        return False, b""
    return raw[0] == _EOF_MARKER, raw[1:]


def _read_eof_flag(path: pathlib.Path) -> bool:
    """Just a chunk's EOF flag, without reading its payload."""
    try:
        with path.open("rb") as f:
            return f.read(1) == bytes([_EOF_MARKER])
    except FileNotFoundError:
        return False


def _scan_chunks(
    files: Sequence[pathlib.Path], start: int, limit: int | None = None
) -> tuple[list[tuple[pathlib.Path, bytes]], int, bool]:
    """Walk *files* from data-chunk *start*, at most *limit* chunks.

    Returns the chunks found, the data-chunk index just past them, and whether
    the walk reached the end of the stream. Before *start* only the one-byte
    EOF flag is read, never a payload; an EOF marker there ends the walk too,
    since a stream that closed before the caller's position has nothing left to
    give it.
    """
    found: list[tuple[pathlib.Path, bytes]] = []
    index = 0  # running count of data (non-EOF) chunks seen
    for path in files:
        if index < start:
            if _read_eof_flag(path):
                return found, index, True
            index += 1
            continue
        if limit is not None and len(found) >= limit:
            # Peek one past the limit: enough to tell "more to come" from "the
            # end", without reading another payload.
            if _read_eof_flag(path):
                return found, index, True
            return found, index + 1, False
        eof, payload = _read_chunk(path)
        if eof:
            return found, index, True
        found.append((path, payload))
        index += 1
    return found, index, False


def _encode_chunks_cursor(index: int) -> str:
    """An opaque token for the data-chunk index a page ended at.

    `cursor` is the world-level pagination type, and against `VercelWorld` it
    is a token the API issues and we forward verbatim -- so the local world
    mints something that fits the same slot rather than exposing its int index.
    The bytes are `@workflow/world-local`'s for want of a reason to differ.
    """
    return base64.b64encode(json.dumps({"i": index}).encode()).decode("ascii")


def _decode_chunks_cursor(cursor: str | None) -> int:
    """The data-chunk index a cursor points at, or 0 for anything unreadable.

    Matching the TS reader, which treats a corrupt cursor as "start over"
    rather than an error -- a cursor is an opaque token a client round-trips,
    and failing the whole read over a mangled one helps nobody.
    """
    if not cursor:
        return 0
    try:
        decoded = json.loads(base64.b64decode(cursor))
        index = decoded["i"]
    except Exception:
        return 0
    return index if isinstance(index, int) and index >= 0 else 0


def to_js_iso(value: datetime) -> str:
    """Format a datetime exactly like JS ``Date.prototype.toISOString``.

    Always UTC with a ``Z`` suffix and exactly three fractional digits;
    naive datetimes are read as UTC. Sub-millisecond precision is dropped,
    which a JS ``Date`` never had in the first place.
    """
    value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return f"{value:%Y-%m-%dT%H:%M:%S}.{value.microsecond // 1000:03d}Z"


def js_now() -> datetime:
    """The current time at JS ``Date`` resolution (whole milliseconds).

    Timestamps are stored via :func:`to_js_iso`, which truncates to
    milliseconds. Truncating up front keeps the value we hand back to the
    caller equal to the one a reader will parse back out of the file.
    """
    now = datetime.now(UTC)
    return now.replace(microsecond=now.microsecond // 1000 * 1000)


def _encode_js(value: Any) -> Any:
    """Rewrite a dumped model into what TS feeds to ``JSON.stringify``."""
    if isinstance(value, bytes | bytearray | memoryview):
        return {
            "__type": UINT8ARRAY_TYPE_TAG,
            "data": base64.b64encode(bytes(value)).decode("ascii"),
        }
    if isinstance(value, datetime):
        return to_js_iso(value)
    if isinstance(value, dict):
        return {k: _encode_js(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_encode_js(v) for v in value]
    if isinstance(value, float) and not isinstance(value, bool):
        # JS has a single number type: 1.0 stringifies as `1`, and
        # `JSON.stringify` writes non-finite numbers as null.
        if not math.isfinite(value):
            return None
        return int(value) if value.is_integer() else value
    return value


def _decode_js(value: Any) -> Any:
    """Inverse of :func:`_encode_js` for the parts TS revives on read."""
    if isinstance(value, dict):
        if value.get("__type") == UINT8ARRAY_TYPE_TAG and isinstance(value.get("data"), str):
            return base64.b64decode(value["data"])
        return {k: _decode_js(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_decode_js(v) for v in value]
    return value


def dumps_js(data: Any) -> bytes:
    """Serialize like ``JSON.stringify(data, jsonReplacer, 2)`` in TS.

    Two-space indent, no space before the comma, and no ``\\uXXXX`` escapes
    for non-ASCII — the byte-for-byte shape `world-local` writes.
    """
    text = json.dumps(
        _encode_js(data),
        indent=2,
        separators=(",", ": "),
        ensure_ascii=False,
        allow_nan=False,
    )
    return text.encode()


def read_json(path: pathlib.Path, schema: type[T] | pydantic.TypeAdapter[T]) -> T | None:
    if path.exists():
        data = _decode_js(json.loads(path.read_text(encoding="utf-8")))
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
        # `exclude_none` mirrors JS: an unset field is `undefined`, which
        # `JSON.stringify` drops. Writing an explicit null instead would be
        # rejected by the TS reader, whose schemas type these as `undefined`
        # (and would silently coerce a null date to the epoch).
        data = data.model_dump(exclude_none=True)
    try:
        atomic_write(path, dumps_js(data), overwrite=overwrite)
    except FileExistsError:
        raise w.EntityConflictError(f"File already exists: {path}") from None


class UnsafeEntityIdError(ValueError):
    """An id that would not stay put inside the data directory."""

    def __init__(self, kind: str, value: str) -> None:
        super().__init__(f"unsafe {kind}: {value!r}")


def assert_safe_entity_id(kind: str, value: str) -> None:
    """Reject an id that cannot be used as a path segment.

    Mirrors the TS ``assertSafeEntityId``. Stream names in particular arrive
    from a namespace the caller chose and become a *directory* name, so this is
    the check that keeps ``../`` out of the data directory.

    Callers that compose a name themselves have already made it safe --
    ``workflow_run_stream_id`` base64url-encodes the namespace, so nothing from
    ``get_writable()`` can reach here unencoded. The guard is for the world API,
    which takes a bare name from tests, the CLI, or anything else holding one.
    ``@workflow/world-local`` double-guards the same way.
    """
    if not value or value.startswith(".") or set(value) & {"/", "\\", "\0", "."}:
        raise UnsafeEntityIdError(kind, value)


def write_exclusive(path: pathlib.Path, data: str) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        atomic_write(path, data.encode(), overwrite=False)
    except FileExistsError:
        return False
    else:
        return True


# Receipt handle marking a delivery that did not come off a real queue lease,
# so the subscriber knows there is nothing to acknowledge afterwards.
_LOCAL_RECEIPT_HANDLE = "local"


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
    """Wrap a `vercel dev` HTTP delivery as if it came off the queue.

    The request body is the bare payload, while subscribers expect the
    `{payload, queueName, deploymentId}` envelope `queue()` sends, so the
    envelope is streamed back around it along with synthetic push headers.
    """
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
        "ce-vqsreceipthandle": _LOCAL_RECEIPT_HANDLE,
        "ce-vqsdeliverycount": request.headers.get("x-vqs-message-attempt") or "1",
        "ce-vqscreatedat": datetime.now(UTC).isoformat(),
        "content-type": request.headers.get("content-type") or "application/json",
    }


def event_record(event: w.Event) -> dict[str, Any]:
    """The on-disk event row: the event plus the world-assigned server props."""
    record = event.model_dump(exclude_none=True)
    if event.server_props is not None:
        record |= event.server_props.model_dump()
    return record


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
        # `${run_id}:${stream_name}` already written to the run's stream
        # registry. See `_register_stream`.
        self._registered_streams: set[str] = set()

    def _run_lock(self, run_id: str) -> threading.Lock:
        # dict.setdefault is atomic, so concurrent callers for the same run_id
        # converge on one lock without a separate guard lock.
        return self._run_locks.setdefault(run_id, threading.Lock())

    def _new_id(self, prefix: str) -> str:
        """A monotonic, lexicographically ordered id.

        Every caller mints one synchronously, *before* its first await, so a
        listing sorted by name is in the order the calls were made rather than
        the order their writes happened to finish. Chunk order in particular is
        file-name order, which no integer counter could give us: one data
        directory is shared by the dev server, the CLI and tests, so a counter
        would need an allocator, and it would have to be zero-padded to a fixed
        width to sort at all.
        """
        return f"{prefix}_{self.monotonic_ulid(None)}"

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
            w.get_physical_topic(queue_name),
            payload,
            idempotency_key=idempotency_key,
            delay=max(1, math.ceil(delay_seconds)) if delay_seconds is not None else None,
        )
        return message_id or "msg_deferred"

    def create_queue_handler(
        self, queue_name_prefix: w.QueuePrefix, handler: w.QueueHandler
    ) -> w.HTTPHandler:
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
                if message.metadata.receipt_handle == _LOCAL_RECEIPT_HANDLE:
                    # Local HTTP deliveries use a synthetic receipt handle so
                    # accept_and_handle can parse them like VQS pushes, but
                    # there is no real queue lease to acknowledge.
                    raise vqs.Handoff()
            except Exception as e:
                if not isinstance(e, vqs.QueueDirective):
                    traceback.print_exc()
                raise

        topic_prefix = w.get_physical_topic(queue_name_prefix)
        vqs.subscribe(
            topic=f"{topic_prefix}*",
            consumer_group=w.QUEUE_CONSUMER_GROUP,
        )(async_handler)
        self._queue_callbacks.append(async_handler)

        async def http_handler(request: w.HTTPRequest) -> w.HTTPResponse:
            queue_name = request.headers.get("x-vqs-queue-name")

            if not queue_name:
                return w.HTTPResponse.json({"error": "Missing required headers"}, status=400)

            # Validate queue name prefix
            if not queue_name.startswith(queue_name_prefix):
                return w.HTTPResponse.json({"error": "Unhandled queue"}, status=400)

            body, headers = _local_queue_delivery(
                request,
                queue_name=queue_name,
                topic=str(w.get_physical_topic(queue_name)),
                consumer_group=str(w.QUEUE_CONSUMER_GROUP),
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
        event_id = self._new_id("evnt")
        now = js_now()

        if data.event_type == "run_created" and not run_id:
            effective_run_id = self._new_id("wrun")
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
                write_json(event_path, event_record(event))
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
                # The event carries the version, and the row it opens inherits
                # it — `@workflow/world-local` propagates it the same way
                # (`storage/events-storage.ts` `effectiveSpecVersion`), so a run
                # this world creates is labelled by whoever wrote the event
                # rather than by which SDK happens to be storing it.
                specVersion=data.spec_version,
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
                    attributes=current_run.attributes,
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
                    attributes=current_run.attributes,
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
                    attributes=current_run.attributes,
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
                    attributes=current_run.attributes,
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
            assert isinstance(step_data.input, bytes)
            step = w.NonFinalWorkflowStep(
                runId=effective_run_id,
                stepId=data.correlation_id,
                stepName=step_data.step_name,
                status="pending",
                input=step_data.input,
                attempt=0,
                createdAt=now,
                updatedAt=now,
                specVersion=data.spec_version,
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
                # Compact, key-for-key what TS writes here — including the
                # `eventId` its cross-process claim recovery reads back.
                json.dumps(
                    {
                        "token": hook_data.token,
                        "hookId": data.correlation_id,
                        "runId": effective_run_id,
                        "eventId": event_id,
                    },
                    separators=(",", ":"),
                    ensure_ascii=False,
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
                write_json(event_path, event_record(conflict_event))
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
                specVersion=data.spec_version,
                isWebhook=False,
                isSystem=False,
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
        write_json(event_path, event_record(event))

        return w.EventResult(
            event=event,
            run=run,
            step=step,
        )

    # ── streams ────────────────────────────────────────────────────────────
    #
    # One file per chunk, under a directory per stream. The directory matters:
    # a live reader re-lists it every 100ms, and a single flat directory would
    # make each of those listings proportional to every chunk in the whole data
    # directory rather than to this one stream (vercel/workflow#2797).
    #
    # Chunk order is file name order -- see `_new_id` for why the names are
    # ULIDs.

    def _chunk_dir(self, name: str) -> pathlib.Path:
        assert_safe_entity_id("streamName", name)
        return self.data_dir / "streams" / "chunks" / name

    def _run_streams_path(self, run_id: str) -> pathlib.Path:
        assert_safe_entity_id("runId", run_id)
        return self.data_dir / "streams" / "runs" / f"{run_id}.json"

    def _chunk_files(self, name: str) -> list[pathlib.Path]:
        """Every chunk file of a stream, in chunk order."""
        try:
            entries = [p for p in self._chunk_dir(name).iterdir() if p.suffix == ".bin"]
        except FileNotFoundError:
            return []
        return sorted(entries, key=lambda p: p.name)

    def _register_stream(self, run_id: str, name: str) -> None:
        """Record that *run_id* owns *name*, so ``streams_list`` finds it.

        Remembered per process, because this runs on every chunk: without the
        cache a stream of ten thousand chunks would read and re-parse the
        registry ten thousand times. Another process registering the same
        stream is unaffected -- each registers its own on first write, and the
        file write is idempotent in content.
        """
        assert_safe_entity_id("streamName", name)
        cache_key = f"{run_id}:{name}"
        if cache_key in self._registered_streams:
            return
        path = self._run_streams_path(run_id)
        with self._run_lock(f"streams:{run_id}"):
            existing = read_json(path, RunStreams)
            names = existing.streams if existing else []
            if name not in names:
                names.append(name)
                write_json(path, RunStreams(streams=names), overwrite=True)
        self._registered_streams.add(cache_key)

    def _append_chunk(
        self, run_id: str, name: str, chunk_id: str, payload: bytes, *, eof: bool
    ) -> None:
        self._register_stream(run_id, name)
        path = self._chunk_dir(name) / f"{chunk_id}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write(path, bytes([_EOF_MARKER if eof else 0]) + payload)

    async def streams_write(self, run_id: str, name: str, chunk: bytes) -> None:
        chunk_id = self._new_id("chnk")
        self._append_chunk(run_id, name, chunk_id, chunk, eof=False)

    async def streams_write_multi(self, run_id: str, name: str, chunks: Sequence[bytes]) -> None:
        if not chunks:
            return
        chunk_ids = [self._new_id("chnk") for _ in chunks]
        for chunk_id, chunk in zip(chunk_ids, chunks, strict=True):
            self._append_chunk(run_id, name, chunk_id, chunk, eof=False)

    async def streams_close(self, run_id: str, name: str) -> None:
        chunk_id = self._new_id("chnk")
        self._append_chunk(run_id, name, chunk_id, b"", eof=True)

    async def streams_list(self, run_id: str) -> list[str]:
        data = read_json(self._run_streams_path(run_id), RunStreams)
        return data.streams if data else []

    def streams_get(
        self, run_id: str, name: str, start_index: int | None = None
    ) -> AsyncGenerator[bytes, None]:
        return self._iter_stream(name, start_index)

    async def _iter_stream(self, name: str, start_index: int | None) -> AsyncGenerator[bytes, None]:
        files = self._chunk_files(name)
        start = self._resolve_start_index(files, start_index)
        found, _, done = _scan_chunks(files, start)

        # Everything the caller asked to skip counts as delivered, so the poll
        # below does not hand it back. An EOF marker in that skipped region
        # ends the read instead: the stream closed before the start index, so
        # there is nothing left to wait for, and waiting would be waiting for
        # an end already passed. (`@workflow/world-local` waits and hangs; a
        # reader that overshoots a closed stream should come back empty, not
        # never.)
        delivered = {path.name for path in files[:start]}
        for path, payload in found:
            delivered.add(path.name)
            if payload:
                yield payload
        if done:
            return

        while True:
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)
            # By name rather than by index, because a chunk can still land
            # behind the position already reached: ids are ULIDs, and another
            # process's clock is its own. An index would re-yield the tail and
            # lose the late arrival; a name set yields it late instead.
            for path in self._chunk_files(name):
                if path.name in delivered:
                    continue
                delivered.add(path.name)
                eof, payload = _read_chunk(path)
                if eof:
                    return
                if payload:
                    yield payload

    def _resolve_start_index(self, files: Sequence[pathlib.Path], start_index: int | None) -> int:
        """Turn *start_index* into a position in *files*.

        A negative index counts back from the end, over data chunks only -- the
        trailing EOF marker is not a chunk anyone asked for.
        """
        if start_index is None:
            return 0
        if start_index >= 0:
            return start_index
        data_count = len(files)
        if files and _read_eof_flag(files[-1]):
            data_count -= 1
        return max(0, data_count + start_index)

    async def streams_get_chunks(
        self, run_id: str, name: str, *, limit: int | None = None, cursor: str | None = None
    ) -> w.StreamChunksPage:
        start = _decode_chunks_cursor(cursor)
        found, index, done = _scan_chunks(
            self._chunk_files(name), start, 100 if limit is None else limit
        )

        # The scan stopped past the page only if it peeked a chunk it did not
        # return, which is exactly when there is more to read.
        has_more = not done and index > start + len(found)
        return w.StreamChunksPage(
            data=[
                w.StreamChunk(index=start + offset, data=payload)
                for offset, (_, payload) in enumerate(found)
            ],
            cursor=_encode_chunks_cursor(start + len(found)) if has_more else None,
            hasMore=has_more,
            done=done,
        )

    async def streams_get_info(self, run_id: str, name: str) -> w.StreamInfo:
        # Starting past the last file makes the scan read every EOF flag and no
        # payload, and leaves the data-chunk count in its position -- which is
        # the whole of this answer.
        files = self._chunk_files(name)
        _, data_count, done = _scan_chunks(files, len(files))
        return w.StreamInfo(tailIndex=data_count - 1, done=done)

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
