"""Atomic start hook negotiation and admission confirmation, matching the TS SDK."""

from __future__ import annotations

import dataclasses
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any

import anyio

from vercel._internal.core.polyfills import UTC

from . import errors, world as w
from .duration import parse_duration_to_date

if TYPE_CHECKING:
    from . import core, deployment

CONFIRM_DELAYS = (0.1, 0.3, 0.9)
ADMISSION_REJECTED = "START_HOOK_ADMISSION_REJECTED"
ADMISSION_DISABLED = "START_HOOK_ADMISSION_DISABLED"


def contract_error(message: str) -> w.WorkflowWorldError:
    return w.WorkflowWorldError(message, status=400, code="WORLD_CONTRACT_ERROR")


def rejected(message: str, reason: str) -> w.WorkflowWorldError:
    return w.WorkflowWorldError(message, status=400, code=ADMISSION_REJECTED, reason=reason)


def normalize(options: core._StartHookOptions) -> w.StartHook:
    if not isinstance(options.token, str) or not options.token:
        raise ValueError("start hook token must be a non-empty string")
    deadline = None
    retention = options.experimental_min_retention
    if retention is not None and retention != 0:
        now = datetime.now(UTC)
        deadline = parse_duration_to_date(retention, now=now)
        if deadline <= now:
            raise ValueError("start hook minimum retention must resolve to a future time")
    return w.StartHook(token=options.token, token_retention_until=deadline)


def validate_target(hook: w.StartHook, world: w.World, target: deployment.Target) -> None:
    if not world.atomic_start_hook:
        raise contract_error(
            "The configured World does not support atomic start hooks. No run was created."
        )
    if hook.token_retention_until is not None and not world.hook_retention:
        raise contract_error(
            "The configured World does not support hook retention. No run was created."
        )
    if target.spec_version < 3:
        raise contract_error(
            "Atomic start hooks require spec version 3 or later. No run was created."
        )
    if not target.cross_deployment:
        return
    if target.probe is None:
        raise w.WorkflowWorldError(
            "Could not verify the target's atomic start hook capability. "
            "No run was created; retry the start.",
            status=0,
            code="TIMEOUT",
        )
    caps = target.probe.get("capabilities") or {}
    for name in ("atomicStartHook", "hookRetention"):
        if name == "hookRetention" and hook.token_retention_until is None:
            continue
        if (
            not isinstance(caps, dict)
            or not isinstance(caps.get(name), dict)
            or caps[name].get("active") is not True
        ):
            raise contract_error(f"Target deployment does not support {name}. No run was created.")


def permanent(error: Exception) -> bool:
    return (
        isinstance(error, w.WorkflowWorldError)
        and error.code not in {"TRANSPORT", "TIMEOUT"}
        and error.status != 0
        and error.status != 429
        and error.status < 500
    )


async def submit(
    run_id: str,
    hook: w.StartHook,
    create: Callable[[], Awaitable[w.EventResult]],
    enqueue: Callable[[], Awaitable[Any]],
    on_conflict: Callable[[str], Awaitable[None]],
) -> None:
    queue_error = None
    try:
        await enqueue()
    except Exception as error:
        if permanent(error):
            raise
        queue_error = error
    result = None
    for attempt in range(len(CONFIRM_DELAYS) + 1):
        try:
            result = await create()
            break
        except errors.HookConflictError as error:
            if not error.conflicting_run_id:
                raise contract_error(
                    "World refused a start hook without naming its owner"
                ) from error
            await on_conflict(error.conflicting_run_id)
            raise errors.HookConflictError(
                error.token, error.conflicting_run_id, kind="start"
            ) from error
        except w.EntityConflictError as error:
            raise contract_error(
                "World returned EntityConflictError instead of the candidate's admission decision"
            ) from error
        except Exception as error:
            if permanent(error) and getattr(error, "code", None) != ADMISSION_DISABLED:
                raise
            if attempt == len(CONFIRM_DELAYS):
                raise errors.WorkflowStartError(
                    run_id, "queue" if queue_error else "admission", queue_error or error
                ) from error
            await anyio.sleep(CONFIRM_DELAYS[attempt])
    assert result is not None
    if result.run is None or result.run.run_id != run_id:
        raise contract_error("World admitted a different run than the requested candidate")
    if result.run.start_hook is None or result.run.start_hook.token != hook.token:
        raise contract_error(
            f'Run "{run_id}" was created but the World did not confirm its token reservation. '
            "It is queued and may execute."
        )
    if queue_error is not None:
        for attempt in range(len(CONFIRM_DELAYS) + 1):
            try:
                await enqueue()
                break
            except Exception as error:
                if permanent(error) or attempt == len(CONFIRM_DELAYS):
                    raise errors.WorkflowStartError(run_id, "queue", error) from error
                await anyio.sleep(CONFIRM_DELAYS[attempt])


@dataclasses.dataclass(frozen=True)
class RunStartHook:
    token: str
    hook_id: str
    token_retention_until: datetime | None = None


def resolve(
    run: w.WorkflowRun, events: Sequence[w.Event], run_input: w.RunInput | None
) -> RunStartHook | None:
    sources = [e.event_data.start_hook for e in events if isinstance(e, w.RunCreatedEvent)]
    sources.extend([run_input.start_hook if run_input is not None else None, run.start_hook])
    hook = None
    for source in sources:
        if source is None:
            continue
        if hook is not None and hook.token != source.token:
            raise contract_error(
                "Conflicting start hook tokens in the run, event log, and queue input"
            )
        if hook is None or hook.token_retention_until is None:
            hook = source
    return (
        None
        if hook is None
        else RunStartHook(hook.token, w.start_hook_id(run.run_id), hook.token_retention_until)
    )
