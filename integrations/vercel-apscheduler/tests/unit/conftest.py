from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest

from vercel.integrations.apscheduler import _install, _jobs, install_vercel_apscheduler_integration
from vercel.schedules import ScheduleMetadata
from vercel.schedules._internal import registry


@pytest.fixture
def installed(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(registry, "_schedules", {})
    monkeypatch.setattr(_jobs, "_declared", {})
    install_vercel_apscheduler_integration()
    try:
        yield
    finally:
        _install._uninstall()


def firing(name: str, scheduled_at: datetime) -> ScheduleMetadata:
    return ScheduleMetadata(
        schedule_id="sch_123",
        execution_id="schx_123",
        name=f"jobs.{name}",
        namespace="default",
        scheduled_at=scheduled_at.astimezone(UTC),
        source="static",
        expression="* * * * *",
    )
