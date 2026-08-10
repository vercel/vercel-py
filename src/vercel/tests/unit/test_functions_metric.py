from __future__ import annotations

from collections.abc import Generator, Mapping

import pytest

from vercel.cache.context import set_context
from vercel.functions import metric


@pytest.fixture(autouse=True)
def clear_metric_context() -> Generator[None, None, None]:
    set_context(metric=None)
    yield
    set_context(metric=None)


def test_metric_reports_through_runtime_context() -> None:
    reported: list[tuple[str, int | float, Mapping[str, str] | None]] = []

    def report(
        name: str,
        value: int | float,
        tags: Mapping[str, str] | None,
    ) -> None:
        reported.append((name, value, tags))

    set_context(metric=report)
    metric("tinybird.query_ms", 100, {"query": "getUser"})

    assert reported == [
        ("tinybird.query_ms", 100, {"query": "getUser"}),
    ]


def test_metric_supports_calls_without_tags() -> None:
    reported: list[tuple[str, int | float, Mapping[str, str] | None]] = []

    def report(
        name: str,
        value: int | float,
        tags: Mapping[str, str] | None,
    ) -> None:
        reported.append((name, value, tags))

    set_context(metric=report)
    metric("tinybird.query_ms", 100)

    assert reported == [("tinybird.query_ms", 100, None)]


def test_metric_is_a_noop_without_runtime_context() -> None:
    metric("tinybird.query_ms", 100)
