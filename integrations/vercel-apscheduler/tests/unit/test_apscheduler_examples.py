from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

from vercel.queue import get_subscriptions
from vercel.queue.testing import clear_subscriptions

EXAMPLE_ROOT = Path(__file__).parents[2] / "examples" / "cleanup"
FUNCTION_PATH = "api/scheduler.py"
TOPIC = "__aps_cleanup"


def test_cleanup_example_uses_explicit_function_trigger_contract() -> None:
    config = json.loads((EXAMPLE_ROOT / "vercel.json").read_text(encoding="utf-8"))
    trigger = config["functions"][FUNCTION_PATH]["experimentalTriggers"][0]

    assert trigger == {
        "type": "queue/v2beta",
        "topic": TOPIC,
        "maxConcurrency": 1,
    }
    assert "crons" not in config
    assert config["buildCommand"] == (
        "uv run python -m vercel.integrations.apscheduler --entrypoint api.scheduler:scheduler"
    )

    pyproject = (EXAMPLE_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "[[tool.vercel.subscribers]]" not in pyproject
    assert '"vercel-apscheduler>=0.1.0"' in pyproject


def test_cleanup_function_registers_matching_queue_subscription(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "_vercel_apscheduler_cleanup_example"
    module_path = EXAMPLE_ROOT / FUNCTION_PATH
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    monkeypatch.setenv("VERCEL_REGION", "iad1")
    clear_subscriptions()
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)

        assert module.OPTIONS.scheduler_id == "cleanup"
        assert module.OPTIONS.wakeup_topic == TOPIC
        assert module.OPTIONS.consumer_group == FUNCTION_PATH
        assert callable(module.app)
        assert [
            (subscription.topic, subscription.consumer_group)
            for subscription in get_subscriptions()
        ] == [(TOPIC, "api_Sscheduler_Dpy")]
    finally:
        clear_subscriptions()
        sys.modules.pop(module_name, None)
