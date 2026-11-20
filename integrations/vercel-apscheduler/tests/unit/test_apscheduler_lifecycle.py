from __future__ import annotations

from typing import Any

from collections.abc import Iterator

import httpx2 as httpx
import pytest
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler
from vendor import respx

from vercel.api import session
from vercel.integrations.apscheduler import APSchedulerConfigurationError
from vercel.schedules import sync

BASE_URL = "https://schedules.test"
SCHEDULES_URL = f"{BASE_URL}/v1/schedules"

pytestmark = pytest.mark.usefixtures("installed")


@pytest.fixture(autouse=True)
def schedules_session() -> Iterator[None]:
    options = sync.SchedulesServiceOptions(
        base_url=BASE_URL, credentials_factory=sync.static_credentials_factory("oidc-token")
    )
    with session(service_options=[options]):
        yield


def deployed(name: str, *, service: str | None = "jobs", source: str = "static") -> dict[str, Any]:
    target: dict[str, Any] = {"type": "function", "function": "schedules"}
    if service is not None:
        target["service"] = service
    return {
        "scheduleId": f"sch_{name}",
        "ownerId": "team_123",
        "projectId": "prj_123",
        "namespace": "default",
        "name": name,
        "timezone": "UTC",
        "expression": {"type": "cron", "cron": "0 3 * * *"},
        "target": target,
        "state": "active",
        "source": source,
        "createdAt": 1_700_000_000_000,
        "updatedAt": 1_700_000_000_000,
    }


def list_schedules(*schedules: dict[str, Any]) -> respx.Route:
    return respx.get(SCHEDULES_URL).mock(
        return_value=httpx.Response(200, json={"data": list(schedules), "cursor": None})
    )


def changes(name: str) -> dict[str, respx.Route]:
    return {
        action: respx.post(f"{SCHEDULES_URL}/{name}/{action}").mock(
            return_value=httpx.Response(200, json=deployed(name))
        )
        for action in ("enable", "disable")
    }


def nightly_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler()
    scheduler.add_job(print, "cron", hour=3, id="nightly")
    scheduler.add_job(print, "cron", hour=4, id="report")
    return scheduler


@respx.mock
def test_pause_and_resume_disable_and_enable_every_job_schedule() -> None:
    scheduler = nightly_scheduler()
    list_schedules(deployed("jobs.nightly"), deployed("jobs.report"), deployed("other.cleanup"))
    nightly, report = changes("jobs.nightly"), changes("jobs.report")

    scheduler.pause()
    assert (nightly["disable"].call_count, report["disable"].call_count) == (1, 1)

    scheduler.resume()
    assert (nightly["enable"].call_count, report["enable"].call_count) == (1, 1)


@respx.mock
def test_pause_job_and_resume_job_change_one_schedule() -> None:
    scheduler = nightly_scheduler()
    list_schedules(deployed("jobs.nightly"), deployed("jobs.report"))
    nightly, report = changes("jobs.nightly"), changes("jobs.report")

    assert scheduler.pause_job("nightly").id == "nightly"
    assert scheduler.resume_job("nightly").id == "nightly"

    assert (nightly["disable"].call_count, nightly["enable"].call_count) == (1, 1)
    assert not report["disable"].called
    assert not report["enable"].called


def test_pause_job_rejects_a_job_the_scheduler_does_not_declare() -> None:
    with pytest.raises(JobLookupError):
        nightly_scheduler().pause_job("missing")


@respx.mock
def test_only_a_static_service_schedule_named_after_the_job_counts() -> None:
    scheduler = BackgroundScheduler()
    scheduler.add_job(print, "cron", hour=3, id="nightly")
    list_schedules(
        deployed("nightly", service=None),
        deployed("jobs.nightly", source="dynamic"),
        deployed("jobsnightly"),
    )

    with pytest.raises(APSchedulerConfigurationError, match="no schedule of this deployment"):
        scheduler.pause()


@respx.mock
def test_rejects_a_job_id_two_schedule_services_deploy() -> None:
    scheduler = BackgroundScheduler()
    scheduler.add_job(print, "cron", hour=3, id="nightly")
    list_schedules(deployed("jobs.nightly"), deployed("reports.nightly", service="reports"))

    with pytest.raises(APSchedulerConfigurationError, match="more than one schedule service"):
        scheduler.pause()
