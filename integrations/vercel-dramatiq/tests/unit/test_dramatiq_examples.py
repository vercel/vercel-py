from __future__ import annotations

import importlib
import sys
from pathlib import Path

import dramatiq.broker as dramatiq_broker
import pytest

from vercel.integrations.dramatiq import register_dramatiq_queues
from vercel.queue import get_subscriptions
from vercel.queue.testing import clear_subscriptions

EXAMPLE_ROOT = Path(__file__).parents[2] / "examples" / "chunks"


def test_chunks_example_uses_pyproject_subscriber_contract() -> None:
    assert not (EXAMPLE_ROOT / "vercel.json").exists()
    assert (EXAMPLE_ROOT / "main.py").is_file()
    assert (EXAMPLE_ROOT / "worker.py").is_file()

    pyproject = (EXAMPLE_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "[[tool.vercel.subscribers]]" in pyproject
    assert 'entrypoint = "worker:broker"' in pyproject
    assert "topics =" not in pyproject


def test_chunks_example_worker_registers_dramatiq_subscription(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started_brokers: list[object] = []
    old_broker = dramatiq_broker.global_broker
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_REGION", "iad1")
    monkeypatch.setenv("VERCEL_QUEUE_TOKEN", "token")
    monkeypatch.setattr(
        "vercel.integrations.dramatiq._broker._start_embedded_worker",
        started_brokers.append,
    )
    sys.path.insert(0, str(EXAMPLE_ROOT))
    dramatiq_broker.global_broker = None
    clear_subscriptions()
    try:
        worker = importlib.import_module("worker")

        assert (
            "dramatiq-vercel-dramatiq-example-chunks-default",
            "dramatiq",
        ) in [(sub.topic, sub.consumer_group) for sub in get_subscriptions()]

        # The Vercel build activates queue serving for the subscriber, which
        # starts the embedded worker.
        register_dramatiq_queues()
        assert worker.broker in started_brokers
    finally:
        clear_subscriptions()
        sys.path.remove(str(EXAMPLE_ROOT))
        dramatiq_broker.global_broker = old_broker
        for name in ("worker", "main", "tasks"):
            sys.modules.pop(name, None)
