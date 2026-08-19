from __future__ import annotations

import sys
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from vercel.functions import get_deadline


def test_get_deadline_reads_runtime_context(monkeypatch) -> None:
    expected = datetime(2026, 8, 18, 12, 30, tzinfo=timezone.utc)
    runtime = SimpleNamespace(get_deadline=lambda: expected)
    monkeypatch.setitem(sys.modules, "vercel_runtime", runtime)

    assert get_deadline() == expected


def test_get_deadline_returns_none_outside_vercel(monkeypatch) -> None:
    monkeypatch.delitem(sys.modules, "vercel_runtime", raising=False)

    assert get_deadline() is None


def test_get_deadline_returns_none_without_runtime_support(monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "vercel_runtime", SimpleNamespace())

    assert get_deadline() is None


def test_get_deadline_propagates_runtime_bugs(monkeypatch) -> None:
    def raise_error() -> None:
        raise ValueError("invalid deadline")

    runtime = SimpleNamespace(get_deadline=raise_error)
    monkeypatch.setitem(sys.modules, "vercel_runtime", runtime)

    with pytest.raises(ValueError, match="invalid deadline"):
        get_deadline()
