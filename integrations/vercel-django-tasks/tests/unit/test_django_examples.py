from __future__ import annotations

from typing import Any

import importlib
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from django.tasks import TaskResultStatus, task_backends
from django.test import RequestFactory
from django.urls import resolve

import vercel.integrations.django._backend as vqs_django

EXAMPLE_ROOT = Path(__file__).parents[2] / "examples" / "chunks"
EXAMPLE_MODULES = (
    "chunks",
    "chunks.apps",
    "chunks.tasks",
    "chunks.urls",
    "chunks.views",
    "chunks_project",
    "chunks_project.asgi",
    "chunks_project.settings",
    "chunks_project.urls",
    "chunks_project.wsgi",
)


@dataclass
class FakeSubscription:
    topic: Any
    consumer_group: str
    callback: Any


class FinishedResult:
    def __init__(self, value: int) -> None:
        self.is_finished = False
        self.return_value = value
        self.status = TaskResultStatus.SUCCESSFUL
        self.refresh_count = 0

    def refresh(self) -> None:
        self.refresh_count += 1
        self.is_finished = True


@pytest.fixture
def chunks_example(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    monkeypatch.syspath_prepend(str(EXAMPLE_ROOT))
    for name in EXAMPLE_MODULES:
        sys.modules.pop(name, None)
    original_default_task_settings = task_backends.settings.get("default")
    try:
        yield EXAMPLE_ROOT
    finally:
        for name in EXAMPLE_MODULES:
            sys.modules.pop(name, None)
        vqs_django._registered_subscribers.clear()
        task_backends.close_all()
        clear_task_backend_connection()
        if original_default_task_settings is not None:
            task_backends.settings["default"] = original_default_task_settings
            _ = task_backends["default"]


@pytest.fixture
def fake_subscribe(monkeypatch: pytest.MonkeyPatch) -> list[FakeSubscription]:
    subscriptions: list[FakeSubscription] = []

    def subscribe(*, topic: Any = None, consumer_group: str = "default", **kwargs: Any) -> Any:
        del kwargs

        def decorator(callback: Any) -> Any:
            subscriptions.append(
                FakeSubscription(
                    topic=topic,
                    consumer_group=consumer_group,
                    callback=callback,
                )
            )
            return callback

        return decorator

    monkeypatch.setattr(vqs_django.vqs, "subscribe", subscribe)
    return subscriptions


def topic_name(topic: Any) -> str:
    return str(getattr(topic, "name", topic))


def configure_example_backend() -> None:
    task_backends.close_all()
    clear_task_backend_connection()
    task_backends.settings["default"] = {
        "BACKEND": "vercel.integrations.django.VercelQueuesBackend",
        "QUEUES": ["default"],
    }
    vqs_django._registered_subscribers.clear()


def clear_task_backend_connection(alias: str = "default") -> None:
    connections: Any = getattr(task_backends, "_connections")  # noqa: B009
    if hasattr(connections, alias):
        delattr(connections, alias)


def test_chunks_example_uses_pyproject_subscriber_contract() -> None:
    assert not (EXAMPLE_ROOT / "vercel.json").exists()

    pyproject = (EXAMPLE_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "[[tool.vercel.subscribers]]" in pyproject
    assert 'entrypoint = "chunks_project.wsgi"' in pyproject
    assert "topics =" not in pyproject


def test_chunks_example_routes_resolve(chunks_example: Path) -> None:
    del chunks_example

    send_match = resolve("/send_chunks/", urlconf="chunks_project.urls")
    assert send_match.url_name == "send_chunks"


def test_chunks_example_registers_task_queue(
    chunks_example: Path,
    fake_subscribe: list[FakeSubscription],
) -> None:
    del chunks_example
    configure_example_backend()
    importlib.import_module("chunks.tasks")

    vqs_django.install_vercel_django_task_integration()

    assert [(topic_name(sub.topic), sub.consumer_group) for sub in fake_subscribe] == [
        ("default", "django-tasks")
    ]


def test_send_chunks_view_enqueues_and_polls_results(
    chunks_example: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del chunks_example
    configure_example_backend()
    views = importlib.import_module("chunks.views")
    queued: list[tuple[int, int]] = []
    results = [FinishedResult(i + i) for i in range(100)]

    class FakeAddTask:
        def enqueue(self, left: int, right: int) -> FinishedResult:
            queued.append((left, right))
            return results[left]

    monkeypatch.setattr(views, "add", FakeAddTask())
    request = RequestFactory().get("/send_chunks/")

    response = views.send_chunks(request)

    assert response.status_code == 200
    assert queued == list(zip(range(100), range(100), strict=False))
    assert all(result.refresh_count == 1 for result in results)
    assert response.content.startswith(b"queued 100 add tasks\nresults: [0, 2, 4")
