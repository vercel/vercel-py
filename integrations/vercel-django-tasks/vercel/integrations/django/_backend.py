from __future__ import annotations

from typing import Any, TypedDict, cast

import copy
import math
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from traceback import format_exception

import vercel.queue as vqs
import vercel.queue.sync as vqs_sync
from django.conf import global_settings, settings
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS, task_backends
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import (
    DEFAULT_TASK_PRIORITY,
    Task,
    TaskContext,
    TaskError,
    TaskResult,
    TaskResultStatus,
)
from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.utils import timezone as django_timezone
from django.utils.crypto import get_random_string
from django.utils.json import normalize_json
from django.utils.module_loading import import_string
from vercel.cache import RuntimeCache

from .version import __version__

__all__ = [
    "VercelQueuesBackend",
    "__version__",
    "install_vercel_django_task_integration",
]

_CONSUMER_GROUP = "django-tasks"
_MAX_ATTEMPTS = 3
_RETRY_BACKOFF_BASE_SECONDS = 5
_RETRY_BACKOFF_FACTOR = 2.0
_MAX_RETRY_DELAY_SECONDS = 60 * 60
_DEFAULT_RESULT_TTL_SECONDS = 24 * 60 * 60
_DEFAULT_RESULT_NAMESPACE = "django-task-results"
_DEFAULT_TASK_BACKEND = {
    "BACKEND": "vercel.integrations.django.VercelQueuesBackend",
}
_ENVELOPE_VERSION = 1
_RESULT_WRAPPER_MARKER = "__vercel_django_task_result__"
_RESULT_WRAPPER_VERSION = 1


class _TaskEnvelope(TypedDict):
    version: int
    task: str
    queue: str
    args: list[Any]
    kwargs: dict[str, Any]


_StoredTaskRecord = dict[str, Any]


@dataclass(frozen=True, slots=True)
class _BackendOptions:
    result_namespace: str = _DEFAULT_RESULT_NAMESPACE
    result_ttl_seconds: int = _DEFAULT_RESULT_TTL_SECONDS

    @classmethod
    def parse(cls, options: object) -> _BackendOptions:
        if not isinstance(options, dict):
            raise TypeError("VercelQueuesBackend OPTIONS must be a dictionary")

        unknown = set(options) - {"result_namespace", "result_ttl_seconds"}
        if unknown:
            names = ", ".join(sorted(map(str, unknown)))
            raise ValueError(f"Unknown VercelQueuesBackend option(s): {names}")

        namespace = options.get("result_namespace", _DEFAULT_RESULT_NAMESPACE)
        if not isinstance(namespace, str) or not namespace:
            raise ValueError("result_namespace must be a non-empty string")

        ttl = options.get("result_ttl_seconds", _DEFAULT_RESULT_TTL_SECONDS)
        if not isinstance(ttl, int) or isinstance(ttl, bool) or ttl <= 0:
            raise ValueError("result_ttl_seconds must be a positive integer")

        return cls(result_namespace=namespace, result_ttl_seconds=ttl)


class _TaskEnvelopeTransport(vqs.RawJsonTransport[_TaskEnvelope]):
    def validate_payload(self, payload: Any) -> _TaskEnvelope:
        return _parse_envelope(payload)


class _RuntimeCacheResults:
    def __init__(self, *, namespace: str, ttl: int) -> None:
        self.ttl = ttl
        self._runtime_cache = RuntimeCache(
            namespace=str(vqs.sanitize_name(namespace)),
            strict=True,
        )

    def get(self, result_id: str) -> _StoredTaskRecord | None:
        value = self._runtime_cache.get(result_id)
        if value is None:
            return None
        return _unwrap_result_record(value)

    def set(self, result_id: str, record: _StoredTaskRecord) -> None:
        self._runtime_cache.set(
            result_id,
            _wrap_result_record(record),
            {"name": result_id, "ttl": self.ttl},
        )


@dataclass(frozen=True, slots=True)
class _PreparedEnqueue:
    envelope: _TaskEnvelope
    delay: vqs.Duration | None


_registered_subscribers: dict[tuple[str, str, str], Any] = {}
_registration_lock = threading.RLock()


def _now() -> datetime:
    return django_timezone.now()


def _json_normalize(value: Any) -> Any:
    return normalize_json(value)


def _set_result_attr(result: TaskResult, name: str, value: Any) -> None:
    object.__setattr__(result, name, value)  # noqa: PLC2801


def _exception_class_path(exc: BaseException) -> str:
    exc_type = type(exc)
    return f"{exc_type.__module__}.{exc_type.__qualname__}"


def _task_error(exc: BaseException) -> TaskError:
    return TaskError(
        exception_class_path=_exception_class_path(exc),
        traceback="".join(format_exception(exc)),
    )


def _retry_delay_seconds(attempt: int) -> int:
    delay = float(_RETRY_BACKOFF_BASE_SECONDS) * math.pow(
        _RETRY_BACKOFF_FACTOR,
        max(0, attempt - 1),
    )
    if not math.isfinite(delay):
        return _MAX_RETRY_DELAY_SECONDS
    return int(max(0, min(float(_MAX_RETRY_DELAY_SECONDS), delay)))


def _parse_iso_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise TypeError("task result timestamp must be a string or null")
    raw = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("task result timestamp is invalid") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _wrap_result_record(record: _StoredTaskRecord) -> dict[str, object]:
    return {
        _RESULT_WRAPPER_MARKER: _RESULT_WRAPPER_VERSION,
        "record": record,
    }


def _unwrap_result_record(value: object) -> _StoredTaskRecord:
    if not isinstance(value, dict):
        raise TypeError("Runtime Cache result payload is not an object")
    if value.get(_RESULT_WRAPPER_MARKER) != _RESULT_WRAPPER_VERSION:
        raise ValueError("Runtime Cache result payload has an unknown version")
    record = value.get("record")
    if not isinstance(record, dict):
        raise TypeError("Runtime Cache result record is not an object")
    return cast("_StoredTaskRecord", record)


def _parse_envelope(payload: Any) -> _TaskEnvelope:
    if not isinstance(payload, dict):
        raise TypeError("Invalid task payload: expected object")
    if payload.get("version") != _ENVELOPE_VERSION:
        raise ValueError("Invalid task payload: unknown envelope version")
    task_path = payload.get("task")
    queue = payload.get("queue")
    args = payload.get("args")
    kwargs = payload.get("kwargs")
    if not isinstance(task_path, str) or not task_path:
        raise TypeError("Invalid task payload: task must be a non-empty string")
    if not isinstance(queue, str) or not queue:
        raise TypeError("Invalid task payload: queue must be a non-empty string")
    if not isinstance(args, list) or not isinstance(kwargs, dict):
        raise TypeError("Invalid task payload: args and kwargs are required")
    return cast("_TaskEnvelope", payload)


class VercelQueuesBackend(BaseTaskBackend):
    task_class: type[Task] = Task
    supports_defer = True
    supports_async_task = True
    supports_get_result = True
    supports_priority = False

    def __init__(self, alias: str, params: dict[str, Any]) -> None:
        super().__init__(alias, params)
        self._cfg = _BackendOptions.parse(self.options)
        self._sync_queue_client: vqs_sync.QueueClient | None = None
        self._async_queue_client: vqs.QueueClient | None = None
        self._results = _RuntimeCacheResults(
            namespace=self._cfg.result_namespace,
            ttl=self._cfg.result_ttl_seconds,
        )
        self.worker_id = get_random_string(32)

    def _topic(self, queue_name: str) -> vqs.Topic[_TaskEnvelope]:
        return vqs.Topic[_TaskEnvelope](
            vqs.sanitize_name(queue_name),
            transport=_TaskEnvelopeTransport(),
        )

    def _sync_client(self) -> vqs_sync.QueueClient:
        if self._sync_queue_client is None:
            self._sync_queue_client = vqs_sync.QueueClient()
        return self._sync_queue_client

    def _async_client(self) -> vqs.QueueClient:
        if self._async_queue_client is None:
            self._async_queue_client = vqs.QueueClient()
        return self._async_queue_client

    def close(self) -> None:
        """Clear backend-owned queue clients."""
        self._sync_queue_client = None
        self._async_queue_client = None

    def _task_from_module_path(self, *, module_path: str, queue_name: str) -> Task:
        imported = import_string(module_path)
        if isinstance(imported, Task):
            func = imported.func
            takes_context = imported.takes_context
        else:
            func = imported
            takes_context = False
        if not callable(func):
            raise TypeError(f"Task function is not callable: {module_path!r}")
        return self.task_class(
            func=func,
            priority=DEFAULT_TASK_PRIORITY,
            queue_name=queue_name,
            backend=self.alias,
            takes_context=takes_context,
            run_after=None,
        )

    def _prepare_enqueue(
        self,
        task: Task,
        args: list[Any],
        kwargs: dict[str, Any],
    ) -> _PreparedEnqueue:
        self.validate_task(task)
        envelope: _TaskEnvelope = {
            "version": _ENVELOPE_VERSION,
            "task": task.module_path,
            "queue": task.queue_name,
            "args": cast("list[Any]", _json_normalize(list(args))),
            "kwargs": cast("dict[str, Any]", _json_normalize(dict(kwargs))),
        }
        delay: vqs.Duration | None = None
        if task.run_after is not None:
            seconds = (task.run_after - _now()).total_seconds()
            if seconds > 0:
                delay = int(seconds)
        return _PreparedEnqueue(envelope=envelope, delay=delay)

    def _finalize_enqueue(
        self,
        task: Task,
        prepared: _PreparedEnqueue,
        message_id: object,
    ) -> TaskResult:
        if message_id is None:
            raise RuntimeError("Vercel Queue accepted the task without returning a message id")
        result: TaskResult = TaskResult(
            task=task,
            id=str(message_id),
            status=TaskResultStatus.READY,
            enqueued_at=_now(),
            started_at=None,
            last_attempted_at=None,
            finished_at=None,
            args=prepared.envelope["args"],
            kwargs=prepared.envelope["kwargs"],
            backend=self.alias,
            errors=[],
            worker_ids=[],
        )
        self._store_result(result)
        task_enqueued.send(type(self), task_result=result)
        return copy.deepcopy(result)

    def enqueue(
        self,
        task: Task,
        args: list[Any],
        kwargs: dict[str, Any],
    ) -> TaskResult:
        prepared = self._prepare_enqueue(task, args, kwargs)
        message_id = self._sync_client().send(
            self._topic(task.queue_name),
            prepared.envelope,
            delay=prepared.delay,
        )
        return self._finalize_enqueue(task, prepared, message_id)

    async def aenqueue(
        self,
        task: Task,
        args: list[Any],
        kwargs: dict[str, Any],
    ) -> TaskResult:
        prepared = self._prepare_enqueue(task, args, kwargs)
        message_id = await self._async_client().send(
            self._topic(task.queue_name),
            prepared.envelope,
            delay=prepared.delay,
        )
        return self._finalize_enqueue(task, prepared, message_id)

    def _serialize_result(self, result: TaskResult) -> _StoredTaskRecord:
        def _datetime(value: datetime | None) -> str | None:
            return value.isoformat() if value is not None else None

        record: _StoredTaskRecord = {
            "version": 1,
            "id": result.id,
            "task": result.task.module_path,
            "queue": result.task.queue_name,
            "status": str(result.status),
            "enqueued_at": _datetime(result.enqueued_at),
            "started_at": _datetime(result.started_at),
            "finished_at": _datetime(result.finished_at),
            "last_attempted_at": _datetime(result.last_attempted_at),
            "args": _json_normalize(list(result.args)),
            "kwargs": _json_normalize(dict(result.kwargs)),
            "worker_ids": list(result.worker_ids),
            "errors": [
                {
                    "exception_class_path": error.exception_class_path,
                    "traceback": error.traceback,
                }
                for error in result.errors
            ],
        }
        if result.status == TaskResultStatus.SUCCESSFUL:
            record["return_value"] = _json_normalize(result.return_value)
        return record

    def _deserialize_result(self, record: _StoredTaskRecord) -> TaskResult:
        required = {
            "version",
            "id",
            "task",
            "queue",
            "status",
            "enqueued_at",
            "started_at",
            "finished_at",
            "last_attempted_at",
            "args",
            "kwargs",
            "worker_ids",
            "errors",
        }
        if record.get("version") != 1 or not required.issubset(record):
            raise ValueError("Runtime Cache task result record is malformed")

        result_id = record["id"]
        module_path = record["task"]
        queue_name = record["queue"]
        args = record["args"]
        kwargs = record["kwargs"]
        worker_ids = record["worker_ids"]
        errors = record["errors"]
        if not all(
            isinstance(value, str) and value for value in (result_id, module_path, queue_name)
        ):
            raise TypeError("Runtime Cache task result identity is malformed")
        if not isinstance(args, list) or not isinstance(kwargs, dict):
            raise TypeError("Runtime Cache task result arguments are malformed")
        if not isinstance(worker_ids, list) or not all(
            isinstance(item, str) for item in worker_ids
        ):
            raise TypeError("Runtime Cache task result worker IDs are malformed")
        if not isinstance(errors, list) or not all(isinstance(item, dict) for item in errors):
            raise TypeError("Runtime Cache task result errors are malformed")

        try:
            status = TaskResultStatus(record["status"])
        except (TypeError, ValueError) as exc:
            raise ValueError("Runtime Cache task result status is malformed") from exc

        task = self._task_from_module_path(
            module_path=module_path,
            queue_name=queue_name,
        )
        result: TaskResult = TaskResult(
            task=task,
            id=result_id,
            status=status,
            enqueued_at=_parse_iso_datetime(record["enqueued_at"]),
            started_at=_parse_iso_datetime(record["started_at"]),
            finished_at=_parse_iso_datetime(record["finished_at"]),
            last_attempted_at=_parse_iso_datetime(record["last_attempted_at"]),
            args=args,
            kwargs=kwargs,
            backend=self.alias,
            errors=[
                TaskError(
                    exception_class_path=str(error.get("exception_class_path") or ""),
                    traceback=str(error.get("traceback") or ""),
                )
                for error in errors
            ],
            worker_ids=worker_ids,
        )
        if "return_value" in record:
            _set_result_attr(result, "_return_value", record["return_value"])
        return result

    def _store_result(self, result: TaskResult) -> None:
        self._results.set(result.id, self._serialize_result(result))

    def get_result(self, result_id: str) -> TaskResult:
        try:
            record = self._results.get(str(result_id))
        except (ImportError, TypeError, ValueError):
            raise TaskResultDoesNotExist(result_id) from None
        if record is None:
            raise TaskResultDoesNotExist(result_id)
        try:
            return self._deserialize_result(record)
        except (ImportError, TypeError, ValueError):
            raise TaskResultDoesNotExist(result_id) from None

    async def aget_result(self, result_id: str) -> TaskResult:
        return self.get_result(result_id)

    def _load_or_initialize_result(
        self,
        *,
        message_id: str,
        envelope: _TaskEnvelope,
        task: Task,
    ) -> TaskResult:
        try:
            result = self.get_result(message_id)
        except TaskResultDoesNotExist:
            result = TaskResult(
                task=task,
                id=message_id,
                status=TaskResultStatus.READY,
                enqueued_at=None,
                started_at=None,
                last_attempted_at=None,
                finished_at=None,
                args=envelope["args"],
                kwargs=envelope["kwargs"],
                backend=self.alias,
                errors=[],
                worker_ids=[],
            )
        _set_result_attr(result, "task", task)
        _set_result_attr(result, "args", envelope["args"])
        _set_result_attr(result, "kwargs", envelope["kwargs"])
        return result

    def _start_result(self, result: TaskResult) -> None:
        now = _now()
        _set_result_attr(result, "status", TaskResultStatus.RUNNING)
        if result.started_at is None:
            _set_result_attr(result, "started_at", now)
        _set_result_attr(result, "last_attempted_at", now)
        result.worker_ids.append(self.worker_id)
        self._store_result(result)
        task_started.send(sender=type(self), task_result=result)

    def _finish_result(
        self,
        result: TaskResult,
        *,
        return_value: Any = None,
        error: BaseException | None = None,
    ) -> int | None:
        if error is None:
            _set_result_attr(result, "_return_value", _json_normalize(return_value))
            _set_result_attr(result, "status", TaskResultStatus.SUCCESSFUL)
            _set_result_attr(result, "finished_at", _now())
            self._store_result(result)
            task_finished.send(sender=type(self), task_result=result)
            return None

        result.errors.append(_task_error(error))
        attempt = len(result.worker_ids)
        if attempt < _MAX_ATTEMPTS:
            _set_result_attr(result, "status", TaskResultStatus.READY)
            _set_result_attr(result, "finished_at", None)
            self._store_result(result)
            return _retry_delay_seconds(attempt)

        _set_result_attr(result, "status", TaskResultStatus.FAILED)
        _set_result_attr(result, "finished_at", _now())
        self._store_result(result)
        task_finished.send(sender=type(self), task_result=result)
        return None

    async def _execute_message(self, message: vqs.Message[_TaskEnvelope]) -> int | None:
        envelope = _parse_envelope(message.payload)
        queue_name = envelope["queue"]
        if self.queues and queue_name not in self.queues:
            raise ValueError(f"Queue {queue_name!r} is not valid for backend {self.alias!r}")
        task = self._task_from_module_path(
            module_path=envelope["task"],
            queue_name=queue_name,
        )
        result = self._load_or_initialize_result(
            message_id=message.metadata.message_id,
            envelope=envelope,
            task=task,
        )
        self._start_result(result)
        try:
            if task.takes_context:
                return_value = await task.acall(
                    TaskContext(task_result=result),
                    *result.args,
                    **result.kwargs,
                )
            else:
                return_value = await task.acall(*result.args, **result.kwargs)
        except Exception as exc:  # noqa: BLE001
            return self._finish_result(result, error=exc)
        return self._finish_result(result, return_value=return_value)


def _resolve_backend(alias: str) -> VercelQueuesBackend:
    backend = task_backends[alias]
    if not isinstance(backend, VercelQueuesBackend):
        raise TypeError(
            f"Backend {alias!r} is {backend.__class__.__name__}, expected VercelQueuesBackend."
        )
    return backend


def _register_task_queues(backend_alias: str) -> None:
    backend = _resolve_backend(backend_alias)
    with _registration_lock:
        for queue_name in sorted(backend.queues):
            topic = backend._topic(queue_name)  # noqa: SLF001
            key = (backend.alias, str(topic.name), _CONSUMER_GROUP)
            if key in _registered_subscribers:
                continue

            async def callback(
                message: vqs.Message[_TaskEnvelope],
                *,
                _backend: VercelQueuesBackend = backend,
            ) -> None:
                retry_after = await _backend._execute_message(message)  # noqa: SLF001
                if retry_after is not None:
                    raise vqs.RetryAfter(retry_after)

            callback.__name__ = f"vercel_django_task_{backend.alias}_{topic.name}"
            subscriber = vqs.subscribe(
                topic=topic,
                consumer_group=_CONSUMER_GROUP,
                max_attempts=_MAX_ATTEMPTS,
            )(callback)
            _registered_subscribers[key] = subscriber


def install_vercel_django_task_integration(
    backend_alias: str = "default",
    *,
    register_queues: bool = True,
) -> None:
    """Install the default backend when needed and register its queue subscribers."""
    _configure_default_task_backend()
    if register_queues:
        _register_task_queues(backend_alias)


def _configure_default_task_backend() -> None:
    global_settings.TASKS[DEFAULT_TASK_BACKEND_ALIAS] = dict(_DEFAULT_TASK_BACKEND)
    if not settings.configured:
        return

    configured_backends = settings.TASKS
    if settings.is_overridden("TASKS") and configured_backends:
        return

    configured_backends[DEFAULT_TASK_BACKEND_ALIAS] = dict(_DEFAULT_TASK_BACKEND)
    task_backends.settings[DEFAULT_TASK_BACKEND_ALIAS] = dict(_DEFAULT_TASK_BACKEND)

    connections: Any = getattr(task_backends, "_connections")  # noqa: B009
    if hasattr(connections, DEFAULT_TASK_BACKEND_ALIAS):
        existing_backend = getattr(connections, DEFAULT_TASK_BACKEND_ALIAS)
        if not isinstance(existing_backend, VercelQueuesBackend):
            close = getattr(existing_backend, "close", None)
            if close is not None:
                close()
            delattr(connections, DEFAULT_TASK_BACKEND_ALIAS)
