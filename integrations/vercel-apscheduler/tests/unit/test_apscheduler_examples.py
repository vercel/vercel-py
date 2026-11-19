from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

from vercel.schedules import get_registered_schedules

EXAMPLE_ROOT = Path(__file__).parents[2] / "examples" / "cleanup"


def test_cleanup_example_declares_its_module_as_a_python_schedule() -> None:
    config = json.loads((EXAMPLE_ROOT / "vercel.json").read_text(encoding="utf-8"))
    assert config["schedules"] == {
        "jobs": {"root": ".", "runtime": "python", "entrypoint": "scheduler"}
    }


@pytest.mark.usefixtures("installed")
def test_cleanup_example_jobs_deploy_as_schedules() -> None:
    module_name = "_vercel_apscheduler_cleanup_example"
    spec = importlib.util.spec_from_file_location(module_name, EXAMPLE_ROOT / "scheduler.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(module_name, None)

    assert [
        (schedule.name, str(schedule.cron), schedule.timezone)
        for schedule in get_registered_schedules()
    ] == [
        ("cleanup-expired-sessions", "0 4 * * *", "UTC"),
        ("refresh-exchange-rates", "*/15 * * * *", "UTC"),
        ("month-end-report", "30 23 28-31 * *", "UTC"),
    ]
