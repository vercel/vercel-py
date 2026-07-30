from __future__ import annotations

import importlib
import sys
from pathlib import Path

import dramatiq
import dramatiq.broker as dramatiq_broker
import pytest

from vercel.queue import get_subscriptions
from vercel.queue.testing import clear_subscriptions


def test_chunks_example_worker_registers_dramatiq_subscription(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    example_root = Path(__file__).parents[2] / "examples" / "chunks"
    started_brokers: list[object] = []
    old_broker = dramatiq_broker.global_broker
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_REGION", "iad1")
    monkeypatch.setenv("VERCEL_QUEUE_TOKEN", "token")
    monkeypatch.setattr(
        "vercel.integrations.dramatiq._broker._start_embedded_worker",
        started_brokers.append,
    )
    sys.path.insert(0, str(example_root))
    dramatiq_broker.global_broker = None
    clear_subscriptions()
    try:
        importlib.import_module("api.dramatiq_worker")

        broker = dramatiq.get_broker()
        assert (
            "dramatiq-vercel-dramatiq-example-chunks-default",
            "api_Sdramatiq__worker_Dpy",
        ) in [(sub.topic, sub.consumer_group) for sub in get_subscriptions()]
        assert broker in started_brokers
    finally:
        clear_subscriptions()
        sys.path.remove(str(example_root))
        dramatiq_broker.global_broker = old_broker
        for name in ("api", "api.dramatiq_worker", "tasks"):
            sys.modules.pop(name, None)
