from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from vercel.queue import get_subscriptions
from vercel.queue.testing import clear_subscriptions


def test_chunks_example_worker_registers_celery_subscription(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    example_root = Path(__file__).parents[2] / "examples" / "chunks"
    started_apps: list[object] = []
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_REGION", "iad1")
    monkeypatch.setenv("VERCEL_QUEUE_TOKEN", "token")
    monkeypatch.setattr(
        "vercel.integrations.celery._broker._start_embedded_worker",
        started_apps.append,
    )
    sys.path.insert(0, str(example_root))
    clear_subscriptions()
    try:
        importlib.import_module("api.celery_worker")
        tasks = importlib.import_module("tasks")

        assert ("celery", "api_Scelery__worker_Dpy") in [
            (sub.topic, sub.consumer_group) for sub in get_subscriptions()
        ]
        assert tasks.celery_app in started_apps
    finally:
        clear_subscriptions()
        sys.path.remove(str(example_root))
        for name in ("api", "api.celery_worker", "tasks"):
            sys.modules.pop(name, None)
