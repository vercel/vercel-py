"""Pause and resume jobs by disabling and enabling their schedules."""

from __future__ import annotations

from typing import Any

from collections.abc import Collection

from vercel.schedules import FunctionTarget, ScheduleSource, sync

from ._errors import APSchedulerConfigurationError
from ._imports import BaseScheduler, JobLookupError
from ._jobs import declared_jobs

__all__ = ["pause", "pause_job", "resume", "resume_job"]


def pause(scheduler: BaseScheduler) -> None:
    """Disable the schedules of every job `scheduler` declares."""
    _set_enabled([declared.job.id for declared in declared_jobs(scheduler)], enabled=False)


def resume(scheduler: BaseScheduler) -> None:
    """Enable the schedules of every job `scheduler` declares."""
    _set_enabled([declared.job.id for declared in declared_jobs(scheduler)], enabled=True)


def pause_job(scheduler: BaseScheduler, job_id: str) -> Any:
    """Disable the schedule of one job, and return the job."""
    job = _job(scheduler, job_id)
    _set_enabled([job_id], enabled=False)
    return job


def resume_job(scheduler: BaseScheduler, job_id: str) -> Any:
    """Enable the schedule of one job, and return the job."""
    job = _job(scheduler, job_id)
    _set_enabled([job_id], enabled=True)
    return job


def _job(scheduler: BaseScheduler, job_id: str) -> Any:
    for declared in declared_jobs(scheduler):
        if declared.job.id == job_id:
            return declared.job
    raise JobLookupError(job_id)


def _set_enabled(job_ids: Collection[str], *, enabled: bool) -> None:
    # Disabling and enabling are idempotent, so a call that fails partway
    # through can be repeated.
    for name in _deployed_names(job_ids):
        if enabled:
            sync.enable_schedule(name)
        else:
            sync.disable_schedule(name)


def _deployed_names(job_ids: Collection[str]) -> list[str]:
    """Return the deployed name of each job's schedule, `<service>.<job id>`.

    A web function doesn't know which service deploys the scheduler, so the
    name comes from the deployment's static schedules: the one whose target
    is a service function and whose name is the service's followed by the id.
    """
    found: dict[str, list[str]] = {job_id: [] for job_id in job_ids}
    if not found:
        return []
    for schedule in sync.query_schedules():
        target = schedule.target
        if schedule.source != ScheduleSource.STATIC or not isinstance(target, FunctionTarget):
            continue
        if target.service is None:
            continue
        job_id = schedule.name.removeprefix(f"{target.service}.")
        if job_id != schedule.name and job_id in found:
            found[job_id].append(schedule.name)
    missing = sorted(job_id for job_id, names in found.items() if not names)
    if missing:
        raise APSchedulerConfigurationError(
            f"no schedule of this deployment runs {_quoted(missing)}; a job runs as a "
            "schedule once a vercel.json Python schedule names its module"
        )
    shared = sorted(job_id for job_id, names in found.items() if len(names) > 1)
    if shared:
        raise APSchedulerConfigurationError(
            f"more than one schedule service runs {_quoted(shared)}, so this scheduler "
            "can't tell which to change; give the jobs ids unique in the project"
        )
    return [names[0] for names in found.values()]


def _quoted(job_ids: list[str]) -> str:
    return ", ".join(f"job {job_id!r}" for job_id in job_ids)
