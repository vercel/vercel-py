from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "poe" / "workspace_poe_resolve.py"
SPEC = importlib.util.spec_from_file_location("workspace_poe_resolve", SCRIPT)
assert SPEC is not None
assert SPEC.loader is not None
workspace_poe_resolve = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = workspace_poe_resolve
SPEC.loader.exec_module(workspace_poe_resolve)


def _declare_task(package_path: Path, task_name: str) -> None:
    package_path.mkdir()
    (package_path / "pyproject.toml").write_text(
        f"[tool.poe.tasks.{task_name}]\ncmd = \"python -c 'pass'\"\n",
        encoding="utf-8",
    )


def test_unscoped_opt_in_selection_includes_only_declared_tasks(tmp_path: Path) -> None:
    supported = tmp_path / "supported"
    unsupported = tmp_path / "unsupported"
    root = tmp_path / "root"
    _declare_task(supported, "test-examples")
    _declare_task(root, "test-examples-root")
    unsupported.mkdir()

    packages = [("supported", str(supported)), ("unsupported", str(unsupported))]

    assert workspace_poe_resolve.opt_in_packages("test-examples", packages, str(root)) == {
        "supported",
        "root",
    }


def test_explicit_supported_package_scope_is_accepted(tmp_path: Path) -> None:
    package = tmp_path / "sandbox"
    _declare_task(package, "test-examples")

    workspace_poe_resolve.require_task_support("vercel-sandbox", str(package), "test-examples")


def test_explicit_unsupported_package_scope_is_clear(tmp_path: Path) -> None:
    package = tmp_path / "headers"
    package.mkdir()

    with pytest.raises(SystemExit, match="test-examples.*vercel-headers"):
        workspace_poe_resolve.require_task_support("vercel-headers", str(package), "test-examples")


def test_root_example_task_uses_internal_declaration(tmp_path: Path) -> None:
    root = tmp_path / "root"
    _declare_task(root, "test-examples-root")

    assert workspace_poe_resolve.task_for_scope("root", "test-examples") == "test-examples-root"
    assert workspace_poe_resolve.task_is_supported("root", str(root), "test-examples")


def test_standard_tasks_remain_supported_without_local_declarations(tmp_path: Path) -> None:
    package = tmp_path / "package"
    package.mkdir()

    assert workspace_poe_resolve.task_is_supported("package", str(package), "test")
