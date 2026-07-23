"""Installed-distribution composition checks for the aggregate package."""

from __future__ import annotations

import subprocess
import sys
from importlib import import_module
from importlib.metadata import PackageNotFoundError, distribution

import pytest
from packaging.requirements import Requirement


def _first_party_files(distribution_name: str) -> set[str]:
    files = distribution(distribution_name).files or ()
    return {str(path) for path in files if str(path).startswith("vercel/")}


def _split_owned(path: str) -> bool:
    return path in {"vercel/__init__.py", "vercel/py.typed"} or path.startswith(
        ("vercel/internal/core/", "vercel/sandbox/")
    )


def _console_scripts(distribution_name: str) -> dict[str, str]:
    return {
        entry_point.name: entry_point.value
        for entry_point in distribution(distribution_name).entry_points
        if entry_point.group == "console_scripts"
    }


def test_installed_runtime_exposes_split_public_surface_only() -> None:
    code = (
        "import sys, vercel; "
        "assert 'vercel.sandbox' not in sys.modules; "
        "assert not hasattr(vercel, 'sandbox'); "
        "from vercel import sandbox, session; "
        "assert sandbox.__name__ == 'vercel.sandbox'; "
        "assert callable(session)"
    )
    subprocess.run([sys.executable, "-c", code], check=True)

    if _first_party_files("vercel"):
        for module_name in (
            "vercel.unstable",
            "vercel.unstable.sandbox",
            "vercel._internal.unstable",
            "vercel._internal.unstable.sandbox",
        ):
            with pytest.raises(ModuleNotFoundError):
                import_module(module_name)


def test_installed_artifacts_have_bounded_dependencies_and_distinct_ownership() -> None:
    try:
        owned = {
            name: _first_party_files(name)
            for name in ("vercel", "vercel-internal-core", "vercel-sandbox")
        }
    except PackageNotFoundError as exc:  # pragma: no cover - malformed developer environment
        pytest.fail(f"missing workspace distribution: {exc.name}")

    # Editable installs expose only .pth metadata. Wheel-installed runs exercise
    # the ownership assertions against concrete package files.
    if not any(files for files in owned.values()):
        pytest.skip("file ownership requires wheel-installed distributions")

    aggregate_requirements = {
        parsed.name: parsed
        for value in distribution("vercel").requires or ()
        for parsed in (Requirement(value),)
    }
    for name in ("vercel-internal-core", "vercel-sandbox"):
        specifiers = tuple(aggregate_requirements[name].specifier)
        assert any(spec.operator in {"<", "<="} for spec in specifiers)
        assert any(spec.operator in {">", ">="} for spec in specifiers)

    aggregate_split_files = {path for path in owned["vercel"] if _split_owned(path)}
    assert aggregate_split_files == set()

    owners: dict[str, list[str]] = {}
    for name, files in owned.items():
        for path in files:
            if _split_owned(path):
                owners.setdefault(path, []).append(name)
    assert all(len(path_owners) == 1 for path_owners in owners.values())
    assert owners["vercel/__init__.py"] == ["vercel-internal-core"]
    assert owners["vercel/py.typed"] == ["vercel-internal-core"]
    assert any(
        path.startswith("vercel/internal/core/") and path_owners == ["vercel-internal-core"]
        for path, path_owners in owners.items()
    )
    assert any(
        path.startswith("vercel/sandbox/") and path_owners == ["vercel-sandbox"]
        for path, path_owners in owners.items()
    )
    aggregate_scripts = _console_scripts("vercel")
    sandbox_scripts = _console_scripts("vercel-sandbox")

    assert aggregate_scripts["vercel"] == "vercel.__main__:main"
    assert "sandbox" not in aggregate_scripts
    assert "vercel-sandbox" not in aggregate_scripts
    assert sandbox_scripts["sandbox"] == "vercel.sandbox.__main__:main"
    assert sandbox_scripts["vercel-sandbox"] == "vercel.sandbox.__main__:main"
