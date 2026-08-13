from __future__ import annotations

from typing import Any

import json
import re
from dataclasses import dataclass
from hashlib import sha256
from os import environ
from pathlib import Path
from sys import modules

DEFAULT_MAX_DELAY_SECONDS = 23 * 60 * 60
DEFAULT_RETRY_AFTER_SECONDS = 30
# A wake message must outlive its own delay by enough redelivery attempts to
# ride out an incident and self-heal; one further day of retention buys that
# healing window for even a maximally delayed hop.
RETENTION_MARGIN_SECONDS = 24 * 60 * 60
# Vercel Queue service limits, enforced here so a configuration that could
# never publish fails at import instead of on the first delayed wake.
VQS_MAX_DELAY_SECONDS = 5 * 24 * 60 * 60
VQS_MAX_RETENTION_SECONDS = 7 * 24 * 60 * 60
DISCOVERY_ENV = "VERCEL_APSCHEDULER_DISCOVERY"
SUBSCRIBER_ID_ENV = "VERCEL_PYTHON_SUBSCRIBER_ID"
SUBSCRIBERS_ENV = "VERCEL_APSCHEDULER_SUBSCRIBERS"
_SCHEDULER_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")

__all__ = [
    "DEFAULT_MAX_DELAY_SECONDS",
    "DEFAULT_RETRY_AFTER_SECONDS",
    "RETENTION_MARGIN_SECONDS",
    "SUBSCRIBERS_ENV",
    "VercelAPSchedulerOptions",
    "development_deployment_id",
    "is_discovery_runtime",
    "is_queue_serving_runtime",
    "is_vercel_runtime",
    "resolve_declared_subscriber_id",
    "resolve_environment",
    "resolve_state_scope",
]


def _truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().casefold() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int) -> int:
    value = environ.get(name)
    if value is None or not value:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


def is_vercel_runtime() -> bool:
    return _truthy(environ.get("VERCEL"))


def is_discovery_runtime() -> bool:
    return _truthy(environ.get(DISCOVERY_ENV))


def is_queue_serving_runtime() -> bool:
    if environ.get(SUBSCRIBER_ID_ENV):
        return True
    if _truthy(environ.get("VERCEL_DEV_QUEUE_SERVING")):
        return True
    service_type = (environ.get("VERCEL_SERVICE_TYPE") or "").strip().casefold()
    if service_type == "worker":
        return True
    service_trigger = (environ.get("VERCEL_SERVICE_TRIGGER") or "").strip().casefold()
    return service_type == "job" and service_trigger in {"queue", "workflow"}


def resolve_environment() -> str:
    """Return the deployment's effective environment name.

    ``VERCEL_TARGET_ENV`` is what distinguishes a custom environment, which
    reports ``VERCEL_ENV=preview``. Every environment decision in this
    integration must go through this one resolution, so state scoping and
    activation can never disagree about what a deployment is.
    """
    return (environ.get("VERCEL_TARGET_ENV") or environ.get("VERCEL_ENV") or "").strip()


def resolve_declared_subscriber_id(scheduler: Any) -> str | None:
    """Reverse-look up a scheduler's builder-assigned subscriber id.

    The builder writes the ``{id, entrypoint}`` mapping into every process of
    a deployment, but only queue-serving sidecars additionally receive
    ``VERCEL_PYTHON_SUBSCRIBER_ID`` (which doubles as the queue-serving
    marker). A publishing process finds its durable identity by matching the
    declared entrypoint that names this exact scheduler object. Returns None
    while the declaring module is still importing (its variable is not bound
    yet) or when the scheduler is not declared; the loud validation of the
    mapping itself stays with automatic activation.
    """
    raw = environ.get(SUBSCRIBERS_ENV)
    if not raw:
        return None
    try:
        entries = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(entries, list):
        return None
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        subscriber_id = entry.get("id")
        entrypoint = entry.get("entrypoint")
        if not isinstance(subscriber_id, str) or not isinstance(entrypoint, str):
            continue
        module_name, separator, variable_name = entrypoint.partition(":")
        if not separator:
            continue
        module = modules.get(module_name)
        if module is not None and getattr(module, variable_name, None) is scheduler:
            return subscriber_id
    return None


# Captured at import, before user module code could chdir: every python
# process of one `vercel dev` project starts in the project directory.
_DEV_PROJECT_DIR = str(Path.cwd())


def development_deployment_id() -> str:
    """Return a stable synthetic deployment id for ``vercel dev``.

    ``vercel dev`` deliberately does not set ``VERCEL_DEPLOYMENT_ID`` — its
    mere presence makes SDKs believe they are deployed. Development state is
    deployment-scoped, and the web process and queue sidecars of one project
    must agree on the scope, so the id is derived from the project directory
    they are all spawned in. The hash also keeps two projects apart when
    development points them at one shared store.
    """
    digest = sha256(_DEV_PROJECT_DIR.encode()).hexdigest()[:12]
    return f"dpl_dev_{digest}"


def resolve_state_scope(deployment: str) -> str:
    """Return the namespace scope for a scheduler's durable state.

    Named environments (production and custom environments) share one durable
    namespace across deployments, so schedules, dynamic jobs, and the wake
    chain survive promotions. Previews and development stay deployment-scoped
    and disposable.
    """
    environment = resolve_environment()
    if not environment:
        # Falling back to a deployment scope here would silently fork a
        # production chain per deployment. Refuse rather than guess.
        raise ValueError(
            "VERCEL_ENV or VERCEL_TARGET_ENV is required to scope durable scheduler state"
        )
    if environment.casefold() in {"preview", "development"}:
        return deployment
    project = environ.get("VERCEL_PROJECT_ID", "").strip()
    if not project:
        # Without the project, two projects sharing one store would silently
        # interleave a namespace. Refuse rather than guess.
        raise ValueError(
            "VERCEL_PROJECT_ID is required to scope durable scheduler state "
            f'in the "{environment}" environment'
        )
    return f"{project}:{environment}"


@dataclass(frozen=True, slots=True)
class _SchedulerIdentity:
    """A scheduler's durable identity.

    Derived from the builder-assigned subscriber id, which is refactor-stable
    (module and variable renames never touch it). Entrypoints stay locators
    only; the ``scheduler_id`` option pins an identity explicitly.
    """

    scheduler_id: str
    wakeup_topic: str
    start_topic: str
    consumer_group: str

    @classmethod
    def from_scheduler_id(cls, scheduler_id: str) -> _SchedulerIdentity:
        if not _SCHEDULER_ID_PATTERN.fullmatch(scheduler_id):
            raise ValueError(
                "scheduler_id must contain only ASCII letters, digits, underscores, and hyphens"
            )
        return cls(
            scheduler_id=scheduler_id,
            wakeup_topic=f"__aps_{scheduler_id}_wakeup",
            start_topic=f"__aps_{scheduler_id}_start",
            consumer_group=f"apscheduler-{scheduler_id}",
        )


@dataclass(frozen=True, slots=True)
class VercelAPSchedulerOptions:
    max_delay_seconds: int = DEFAULT_MAX_DELAY_SECONDS
    retention_seconds: int | None = DEFAULT_MAX_DELAY_SECONDS + RETENTION_MARGIN_SECONDS
    retry_after_seconds: int = DEFAULT_RETRY_AFTER_SECONDS
    max_concurrency: int = 1
    # Escape hatch: pins the durable identity independently of the
    # builder-assigned subscriber id.
    scheduler_id: str | None = None

    def __post_init__(self) -> None:
        if self.max_delay_seconds <= 0:
            raise ValueError("max_delay_seconds must be a positive integer")
        if self.max_delay_seconds > VQS_MAX_DELAY_SECONDS:
            raise ValueError(
                f"max_delay_seconds cannot exceed {VQS_MAX_DELAY_SECONDS} "
                "(the queue service delay limit)"
            )
        if self.retention_seconds is not None:
            if self.retention_seconds <= 0:
                raise ValueError("retention_seconds must be a positive integer")
            if self.retention_seconds > VQS_MAX_RETENTION_SECONDS:
                raise ValueError(
                    f"retention_seconds cannot exceed {VQS_MAX_RETENTION_SECONDS} "
                    "(the queue service retention limit)"
                )
            if self.retention_seconds <= self.max_delay_seconds:
                raise ValueError(
                    "retention_seconds must exceed max_delay_seconds: a wake "
                    "must outlive its own publish delay to be deliverable"
                )

    @classmethod
    def from_env(cls) -> VercelAPSchedulerOptions:
        max_delay_seconds = _int_env(
            "VERCEL_APSCHEDULER_MAX_DELAY_SECONDS",
            DEFAULT_MAX_DELAY_SECONDS,
        )
        retention_raw = environ.get("VERCEL_APSCHEDULER_RETENTION_SECONDS")
        try:
            # Retention must outlive the longest bridged hop plus a healing
            # window, so the default follows a raised max delay.
            retention = (
                int(retention_raw)
                if retention_raw
                else max_delay_seconds + RETENTION_MARGIN_SECONDS
            )
        except ValueError as exc:
            raise ValueError(
                "VERCEL_APSCHEDULER_RETENTION_SECONDS must be a positive integer"
            ) from exc
        if retention <= 0:
            raise ValueError("VERCEL_APSCHEDULER_RETENTION_SECONDS must be a positive integer")
        return cls(
            max_delay_seconds=max_delay_seconds,
            retention_seconds=retention,
            retry_after_seconds=_int_env(
                "VERCEL_APSCHEDULER_RETRY_AFTER_SECONDS",
                DEFAULT_RETRY_AFTER_SECONDS,
            ),
            max_concurrency=_int_env("VERCEL_APSCHEDULER_MAX_CONCURRENCY", 1),
        )

    @classmethod
    def from_value(
        cls,
        value: VercelAPSchedulerOptions | dict[str, Any] | None,
    ) -> VercelAPSchedulerOptions:
        if value is None:
            return cls.from_env()
        if isinstance(value, VercelAPSchedulerOptions):
            return value
        allowed = set(cls.__dataclass_fields__)
        unknown = sorted(set(value) - allowed)
        if unknown:
            joined = ", ".join(unknown)
            raise TypeError(f"unknown APScheduler integration option(s): {joined}")
        return cls(**value)
