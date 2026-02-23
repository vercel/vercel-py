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


class TestBlobSignatureParity:
    """Test blob module sync/async signature parity."""

    def test_put_signatures_match(self):
        """Test put and put_async have matching signatures."""
        from vercel.blob import put, put_async

        differences = compare_signatures(put, put_async)
        assert not differences, f"Signature differences: {differences}"

    def test_delete_signatures_match(self):
        """Test delete and delete_async have matching signatures."""
        from vercel.blob import delete, delete_async

        differences = compare_signatures(delete, delete_async)
        assert not differences, f"Signature differences: {differences}"

    def test_head_signatures_match(self):
        """Test head and head_async have matching signatures."""
        from vercel.blob import head, head_async

        differences = compare_signatures(head, head_async)
        assert not differences, f"Signature differences: {differences}"

    def test_list_objects_signatures_match(self):
        """Test list_objects and list_objects_async have matching signatures."""
        from vercel.blob import list_objects, list_objects_async

        differences = compare_signatures(list_objects, list_objects_async)
        assert not differences, f"Signature differences: {differences}"

    def test_iter_objects_signatures_match(self):
        """Test iter_objects and iter_objects_async have matching signatures."""
        from vercel.blob import iter_objects, iter_objects_async

        differences = compare_signatures(iter_objects, iter_objects_async)
        assert not differences, f"Signature differences: {differences}"

    def test_copy_signatures_match(self):
        """Test copy and copy_async have matching signatures."""
        from vercel.blob import copy, copy_async

        differences = compare_signatures(copy, copy_async)
        assert not differences, f"Signature differences: {differences}"

    def test_create_folder_signatures_match(self):
        """Test create_folder and create_folder_async have matching signatures."""
        from vercel.blob import create_folder, create_folder_async

        differences = compare_signatures(create_folder, create_folder_async)
        assert not differences, f"Signature differences: {differences}"

    def test_upload_file_signatures_match(self):
        """Test upload_file and upload_file_async have matching signatures."""
        from vercel.blob import upload_file, upload_file_async

        differences = compare_signatures(upload_file, upload_file_async)
        assert not differences, f"Signature differences: {differences}"

    def test_download_file_signatures_match(self):
        """Test download_file and download_file_async have matching signatures."""
        from vercel.blob import download_file, download_file_async

        differences = compare_signatures(download_file, download_file_async)
        assert not differences, f"Signature differences: {differences}"


class TestBlobMultipartSignatureParity:
    """Test blob multipart sync/async signature parity."""

    def test_create_multipart_upload_signatures_match(self):
        """Test create_multipart_upload signatures match."""
        from vercel.blob import create_multipart_upload, create_multipart_upload_async

        differences = compare_signatures(create_multipart_upload, create_multipart_upload_async)
        assert not differences, f"Signature differences: {differences}"

    def test_upload_part_signatures_match(self):
        """Test upload_part signatures match."""
        from vercel.blob import upload_part, upload_part_async

        differences = compare_signatures(upload_part, upload_part_async)
        assert not differences, f"Signature differences: {differences}"

    def test_complete_multipart_upload_signatures_match(self):
        """Test complete_multipart_upload signatures match."""
        from vercel.blob import complete_multipart_upload, complete_multipart_upload_async

        differences = compare_signatures(complete_multipart_upload, complete_multipart_upload_async)
        assert not differences, f"Signature differences: {differences}"

    def test_create_multipart_uploader_signatures_match(self):
        """Test create_multipart_uploader signatures match."""
        from vercel.blob import create_multipart_uploader, create_multipart_uploader_async

        differences = compare_signatures(create_multipart_uploader, create_multipart_uploader_async)
        assert not differences, f"Signature differences: {differences}"


class TestBlobClientClassParity:
    """Test BlobClient and AsyncBlobClient method parity."""

    def test_client_methods_exist(self):
        """Test that both client classes have the same methods."""
        from vercel.blob import AsyncBlobClient, BlobClient

        sync_methods = {
            m for m in dir(BlobClient) if not m.startswith("_") and callable(getattr(BlobClient, m))
        }
        async_methods = {
            m
            for m in dir(AsyncBlobClient)
            if not m.startswith("_") and callable(getattr(AsyncBlobClient, m))
        }

        # Lifecycle naming intentionally differs by runtime.
        assert "close" in sync_methods
        assert "aclose" in async_methods
        sync_methods.discard("close")
        async_methods.discard("aclose")

        assert sync_methods == async_methods, (
            f"Method mismatch: sync_only={sync_methods - async_methods}, "
            f"async_only={async_methods - sync_methods}"
        )


class TestSandboxClassParity:
    """Test Sandbox and AsyncSandbox method parity."""

    def test_sandbox_methods_exist(self):
        """Test that both sandbox classes have equivalent methods."""
        from vercel.sandbox import AsyncSandbox, Sandbox

        # Get public methods (excluding dunder methods)
        sync_methods = {
            m for m in dir(Sandbox) if not m.startswith("_") and callable(getattr(Sandbox, m))
        }
        async_methods = {
            m
            for m in dir(AsyncSandbox)
            if not m.startswith("_") and callable(getattr(AsyncSandbox, m))
        }

        # AsyncSandbox has 'shell' method that Sandbox doesn't have (interactive only)
        # So we check that all sync methods exist in async
        missing_in_async = sync_methods - async_methods
        assert not missing_in_async, f"Methods missing in AsyncSandbox: {missing_in_async}"


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


class TestResultTypeParity:
    """Test that sync and async functions return the same result types."""

    def test_blob_put_returns_same_type(self):
        """Test put and put_async return the same result type."""
        from vercel.blob import put, put_async
        from vercel.blob.types import PutBlobResult

        sync_annotation = inspect.signature(put).return_annotation
        async_annotation = inspect.signature(put_async).return_annotation

        # Sync should return PutBlobResult directly
        assert sync_annotation == PutBlobResult or "PutBlobResult" in str(sync_annotation)

        # Async should return Coroutine[..., PutBlobResult] - verify inner type matches
        async_str = str(async_annotation)
        assert "PutBlobResult" in async_str, f"Async should return PutBlobResult, got {async_str}"

    def test_blob_head_returns_same_type(self):
        """Test head and head_async return the same result type."""
        from vercel.blob import head, head_async
        from vercel.blob.types import HeadBlobResult

        sync_annotation = inspect.signature(head).return_annotation
        async_annotation = inspect.signature(head_async).return_annotation

        # Sync should return HeadBlobResult directly
        assert sync_annotation == HeadBlobResult or "HeadBlobResult" in str(sync_annotation)

        # Async should return Coroutine[..., HeadBlobResult] - verify inner type matches
        async_str = str(async_annotation)
        assert "HeadBlobResult" in async_str, f"Async should return HeadBlobResult, got {async_str}"

    def test_blob_list_returns_same_type(self):
        """Test list_objects and list_objects_async return the same result type."""
        from vercel.blob import list_objects, list_objects_async
        from vercel.blob.types import ListBlobResult

        sync_annotation = inspect.signature(list_objects).return_annotation
        async_annotation = inspect.signature(list_objects_async).return_annotation

        # Sync should return ListBlobResult directly
        assert sync_annotation == ListBlobResult or "ListBlobResult" in str(sync_annotation)

        # Async should return Coroutine[..., ListBlobResult] - verify inner type matches
        async_str = str(async_annotation)
        assert "ListBlobResult" in async_str, f"Async should return ListBlobResult, got {async_str}"

    def test_blob_iter_returns_iterator_types(self):
        """Test iter_objects and iter_objects_async expose iterator return types."""
        from vercel.blob import iter_objects, iter_objects_async

        sync_annotation = inspect.signature(iter_objects).return_annotation
        async_annotation = inspect.signature(iter_objects_async).return_annotation

        assert "Iterator" in str(sync_annotation), (
            f"Sync should return Iterator, got {sync_annotation}"
        )
        assert "AsyncIterator" in str(async_annotation), (
            f"Async should return AsyncIterator, got {async_annotation}"
        )
