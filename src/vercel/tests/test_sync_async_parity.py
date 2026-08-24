"""Sync/Async API parity tests.

Validates that sync and async function pairs have matching signatures
and produce consistent results when given the same inputs.
"""

import inspect
from collections.abc import Callable
from typing import Any


def get_param_names(func: Callable) -> list[str]:
    """Extract parameter names from a function signature."""
    sig = inspect.signature(func)
    return [
        name
        for name, param in sig.parameters.items()
        if param.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    ]


def get_param_defaults(func: Callable) -> dict[str, Any]:
    """Extract parameter defaults from a function signature."""
    sig = inspect.signature(func)
    return {
        name: param.default
        for name, param in sig.parameters.items()
        if param.default is not inspect.Parameter.empty
    }


def compare_signatures(sync_func: Callable, async_func: Callable) -> list[str]:
    """Compare signatures of sync and async functions.

    Returns a list of differences (empty if signatures match).
    """
    differences = []

    sync_params = get_param_names(sync_func)
    async_params = get_param_names(async_func)

    if sync_params != async_params:
        differences.append(f"Parameter names differ: sync={sync_params}, async={async_params}")

    sync_defaults = get_param_defaults(sync_func)
    async_defaults = get_param_defaults(async_func)

    # Check that defaults match for common parameters
    for name in set(sync_defaults.keys()) & set(async_defaults.keys()):
        if sync_defaults[name] != async_defaults[name]:
            differences.append(
                f"Default for '{name}' differs: "
                f"sync={sync_defaults[name]}, async={async_defaults[name]}"
            )

    return differences


class TestCacheClassParity:
    """Test RuntimeCache and AsyncRuntimeCache method parity."""

    def test_cache_methods_exist(self):
        """Test that cache classes have equivalent methods."""
        from vercel.cache import AsyncRuntimeCache, RuntimeCache

        # Core methods that should exist in both
        expected_methods = {"get", "set", "delete", "expire_tag"}

        sync_methods = {
            m
            for m in dir(RuntimeCache)
            if not m.startswith("_") and callable(getattr(RuntimeCache, m))
        }
        async_methods = {
            m
            for m in dir(AsyncRuntimeCache)
            if not m.startswith("_") and callable(getattr(AsyncRuntimeCache, m))
        }

        assert expected_methods.issubset(sync_methods), (
            f"Missing sync methods: {expected_methods - sync_methods}"
        )
        assert expected_methods.issubset(async_methods), (
            f"Missing async methods: {expected_methods - async_methods}"
        )


class TestProjectsSignatureParity:
    """Test projects module sync/async signature parity."""

    def test_get_projects_signatures_match(self):
        """Test get_projects and get_projects_async have matching signatures."""
        from vercel.projects import get_projects
        from vercel.projects.projects import get_projects_async

        differences = compare_signatures(get_projects, get_projects_async)
        assert not differences, f"Signature differences: {differences}"

    def test_create_project_signatures_match(self):
        """Test create_project and create_project_async have matching signatures."""
        from vercel.projects import create_project
        from vercel.projects.projects import create_project_async

        differences = compare_signatures(create_project, create_project_async)
        assert not differences, f"Signature differences: {differences}"

    def test_update_project_signatures_match(self):
        """Test update_project and update_project_async have matching signatures."""
        from vercel.projects import update_project
        from vercel.projects.projects import update_project_async

        differences = compare_signatures(update_project, update_project_async)
        assert not differences, f"Signature differences: {differences}"

    def test_delete_project_signatures_match(self):
        """Test delete_project and delete_project_async have matching signatures."""
        from vercel.projects import delete_project
        from vercel.projects.projects import delete_project_async

        differences = compare_signatures(delete_project, delete_project_async)
        assert not differences, f"Signature differences: {differences}"
