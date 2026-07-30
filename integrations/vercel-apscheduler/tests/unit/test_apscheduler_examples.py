from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

from vercel.integrations.apscheduler import install_vercel_apscheduler_integration
from vercel.queue import get_subscriptions
from vercel.queue.testing import clear_subscriptions

EXAMPLE_ROOT = Path(__file__).parents[2] / "examples" / "cleanup"
SCHEDULER_PATH = "scheduler.py"
START_TOPIC = "__aps_scheduler_scheduler_start"
WAKEUP_TOPIC = "__aps_scheduler_scheduler_wakeup"
CONSUMER_GROUP = "apscheduler-scheduler_scheduler"


def test_cleanup_example_uses_pyproject_subscriber_contract() -> None:
    config = json.loads((EXAMPLE_ROOT / "vercel.json").read_text(encoding="utf-8"))
    assert config["regions"] == ["iad1"]
    assert "functions" not in config
    assert "crons" not in config
    assert "buildCommand" not in config
    assert (EXAMPLE_ROOT / "main.py").is_file()
    assert (EXAMPLE_ROOT / SCHEDULER_PATH).is_file()
    assert not (EXAMPLE_ROOT / "api" / "scheduler.py").exists()
    assert not (EXAMPLE_ROOT / "api" / "scheduler_watchdog.py").exists()

    pyproject = (EXAMPLE_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "[[tool.vercel.subscribers]]" in pyproject
    assert 'entrypoint = "scheduler:scheduler"' in pyproject
    assert "topics =" not in pyproject
    assert '"vercel-apscheduler>=0.1.0"' in pyproject

    scheduler_source = (EXAMPLE_ROOT / SCHEDULER_PATH).read_text(encoding="utf-8")
    assert "VercelAPSchedulerOptions" not in scheduler_source
    assert "install_vercel_apscheduler_integration" not in scheduler_source


def test_cleanup_scheduler_registers_introspectable_queue_subscriptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "_vercel_apscheduler_cleanup_example"
    module_path = EXAMPLE_ROOT / SCHEDULER_PATH
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_REGION", "iad1")
    monkeypatch.setenv("VERCEL_PYTHON_SUBSCRIBER_ID", "scheduler_scheduler")
    clear_subscriptions()
    install_vercel_apscheduler_integration()
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)

        assert [
            (
                subscription.topic,
                subscription.consumer_group,
                subscription.retry_after_seconds,
                subscription.max_concurrency,
                subscription.max_attempts,
            )
            for subscription in get_subscriptions()
        ] == [
            (START_TOPIC, CONSUMER_GROUP, 30.0, 1, None),
            (WAKEUP_TOPIC, CONSUMER_GROUP, 30.0, 1, None),
        ]
    finally:
        clear_subscriptions()
        sys.modules.pop(module_name, None)
