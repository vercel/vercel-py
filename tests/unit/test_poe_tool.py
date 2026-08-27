from __future__ import annotations

import importlib.machinery
import importlib.util
import sys
from pathlib import Path

TOOL = Path(__file__).resolve().parents[2] / "scripts" / "poe" / "tasks" / "tool"
LOADER = importlib.machinery.SourceFileLoader("poe_tool", str(TOOL))
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
assert SPEC is not None
poe_tool = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = poe_tool
LOADER.exec_module(poe_tool)


def _command(
    monkeypatch,
    args: tuple[str, ...],
    *,
    ggt_available: bool = True,
    force_pytest: bool = False,
    lograil_progress: bool = False,
    parallel: bool = True,
) -> list[str]:
    monkeypatch.delenv("POE_EXTRA_ARGS", raising=False)
    monkeypatch.delenv("WORKSPACE_POE_TEST_RUNNER", raising=False)
    monkeypatch.delenv("WORKSPACE_POE_SCOPE_ARGS", raising=False)
    monkeypatch.setattr(
        poe_tool.shutil,
        "which",
        lambda command: f"/bin/{command}" if command == "ggt" and ggt_available else None,
    )
    if lograil_progress:
        monkeypatch.setenv("WORKSPACE_POE_LOGRAIL_PROGRESS", "1")
    else:
        monkeypatch.delenv("WORKSPACE_POE_LOGRAIL_PROGRESS", raising=False)
    if force_pytest:
        monkeypatch.setenv("FORCE_PYTEST", "1")
    else:
        monkeypatch.delenv("FORCE_PYTEST", raising=False)
    if parallel:
        monkeypatch.delenv("WORKSPACE_POE_PARALLEL", raising=False)
    else:
        monkeypatch.setenv("WORKSPACE_POE_PARALLEL", "0")
    return poe_tool.build_command("pytest", args)


def test_pytest_wrapper_prefers_ggt(monkeypatch) -> None:
    assert _command(monkeypatch, ("tests",)) == ["ggt", "tests"]


def test_pytest_wrapper_runs_ggt_sequentially_when_parallel_is_disabled(
    monkeypatch,
) -> None:
    assert _command(monkeypatch, ("tests",), parallel=False) == [
        "ggt",
        "-j",
        "1",
        "tests",
    ]


def test_pytest_wrapper_uses_structured_ggt_output_for_lograil(
    monkeypatch,
) -> None:
    assert _command(monkeypatch, ("tests",), lograil_progress=True) == [
        "ggt",
        "--output-format",
        "json",
        "tests",
    ]


def test_pytest_wrapper_falls_back_to_parallel_pytest(monkeypatch) -> None:
    assert _command(monkeypatch, ("tests",), ggt_available=False) == [
        "pytest",
        "-n",
        "auto",
        "tests",
    ]


def test_pytest_wrapper_can_force_pytest(monkeypatch) -> None:
    assert _command(monkeypatch, ("tests",), force_pytest=True) == [
        "pytest",
        "-n",
        "auto",
        "tests",
    ]
