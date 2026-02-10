from __future__ import annotations

import contextvars
import inspect
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from os import PathLike
from typing import Any, TypeVar

import httpx

from .._http import AsyncTransport, SyncTransport, create_base_async_client, create_base_client
from .._iter_coroutine import iter_coroutine
from .._telemetry.tracker import telemetry, track
from ._core import (
    _AsyncBlobOpsClient,
    _SyncBlobOpsClient,
    get_telemetry_size_bytes,
    normalize_delete_urls,
)
from .errors import BlobError, BlobNotFoundError
from .types import (
    CreateFolderResult as CreateFolderResultType,
    HeadBlobResult as HeadBlobResultType,
    ListBlobItem,
    ListBlobResult as ListBlobResultType,
    PutBlobResult as PutBlobResultType,
)
from .utils import (
    UploadProgressEvent,
    ensure_token,
    is_url,
)

# Context variable to store the delete count for telemetry
# This allows the derive function to access the count after the iterable is consumed
_delete_count_context: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "_delete_count", default=None
)

_T = TypeVar("_T")


def _run_sync_blob_operation(
    operation: Callable[[_SyncBlobOpsClient], Awaitable[_T]],
) -> _T:
    with _SyncBlobOpsClient() as client:
        # Keep exactly one sync bridge at the wrapper boundary.
        return iter_coroutine(operation(client))


def put(
    path: str,
    body: Any,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    multipart: bool = False,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
) -> PutBlobResultType:
    token = ensure_token(token)
    result, used_multipart = _run_sync_blob_operation(
        lambda client: client._put_blob(
            path,
            body,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
            multipart=multipart,
            on_upload_progress=on_upload_progress,
        )
    )
    track(
        "blob_put",
        token=token,
        access=access,
        content_type=content_type,
        multipart=used_multipart,
        size_bytes=get_telemetry_size_bytes(body),
    )
    return result


async def put_async(
    path: str,
    body: Any,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    multipart: bool = False,
    on_upload_progress: (
        Callable[[UploadProgressEvent], None]
        | Callable[[UploadProgressEvent], Awaitable[None]]
        | None
    ) = None,
) -> PutBlobResultType:
    token = ensure_token(token)
    async with _AsyncBlobOpsClient() as client:
        result, used_multipart = await client._put_blob(
            path,
            body,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
            multipart=multipart,
            on_upload_progress=on_upload_progress,
        )
    track(
        "blob_put",
        token=token,
        access=access,
        content_type=content_type,
        multipart=used_multipart,
        size_bytes=get_telemetry_size_bytes(body),
    )
    return result


class _CountedIterable:
    """Wrapper for iterables that preserves count even after consumption.

    This is used to handle generators and other single-use iterables
    passed to delete/delete_async. The wrapper converts the iterable
    to a list once, so that the count is preserved even after the
    iterable is fully consumed by the function.
    """

    def __init__(self, iterable: Iterable[str]) -> None:
        """Convert the iterable to a list to preserve it for later counting."""
        self.items = [str(item) for item in iterable]

    def __iter__(self) -> Iterator[str]:
        """Allow iteration over the preserved items."""
        return iter(self.items)

    def __len__(self) -> int:
        """Return the count of items."""
        return len(self.items)


def _derive_delete_count(args: tuple, kwargs: dict, result: Any) -> int:
    """Derive the count of URLs being deleted."""
    # First, check if the count was stored in the context variable
    count = _delete_count_context.get()
    if count is not None:
        _delete_count_context.set(None)  # Clear it for the next call
        return count

    # Fallback: try to derive from the argument
    url_or_path = kwargs.get("url_or_path", args[0] if args else None)
    if url_or_path is None:
        return 1
    # Check if it's a _CountedIterable (which preserves count after consumption)
    if isinstance(url_or_path, _CountedIterable):
        return len(url_or_path)
    # For other iterables, try to count them (though they may be exhausted)
    if isinstance(url_or_path, Iterable) and not isinstance(url_or_path, (str, bytes)):
        try:
            return len(list(url_or_path))
        except Exception:
            return 1
    return 1


@telemetry(
    event="blob_delete",
    capture=["token"],
    derive={"count": _derive_delete_count},
    when="after",
)
def delete(
    url_or_path: str | Iterable[str],
    *,
    token: str | None = None,
) -> None:
    token = ensure_token(token)
    normalized_urls = normalize_delete_urls(url_or_path)
    _delete_count_context.set(len(normalized_urls))
    _run_sync_blob_operation(
        lambda client: client._delete_blob(
            normalized_urls,
            token=token,
        )
    )


@telemetry(
    event="blob_delete",
    capture=["token"],
    derive={"count": _derive_delete_count},
    when="after",
)
async def delete_async(
    url_or_path: str | Iterable[str],
    *,
    token: str | None = None,
) -> None:
    token = ensure_token(token)
    normalized_urls = normalize_delete_urls(url_or_path)
    _delete_count_context.set(len(normalized_urls))
    async with _AsyncBlobOpsClient() as client:
        await client._delete_blob(
            normalized_urls,
            token=token,
        )


def head(url_or_path: str, *, token: str | None = None) -> HeadBlobResultType:
    token = ensure_token(token)
    return _run_sync_blob_operation(
        lambda client: client._head_blob(
            url_or_path,
            token=token,
        )
    )


async def head_async(url_or_path: str, *, token: str | None = None) -> HeadBlobResultType:
    token = ensure_token(token)
    async with _AsyncBlobOpsClient() as client:
        return await client._head_blob(
            url_or_path,
            token=token,
        )


def get(
    url_or_path: str,
    *,
    token: str | None = None,
    timeout: float | None = None,
) -> bytes:
    token = ensure_token(token)
    target_url: str
    if is_url(url_or_path):
        target_url = url_or_path
    else:
        metadata = head(url_or_path, token=token)
        target_url = metadata.url

    effective_timeout = timeout or 30.0
    transport = SyncTransport(create_base_client(timeout=effective_timeout))
    response: httpx.Response | None = None
    try:
        response = iter_coroutine(
            transport.send(
                "GET",
                target_url,
                timeout=effective_timeout,
                follow_redirects=True,
            )
        )
        if response.status_code == 404:
            raise BlobNotFoundError()
        response.raise_for_status()
        return response.content
    except httpx.HTTPStatusError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            raise BlobNotFoundError() from exc
        raise
    except httpx.HTTPError:
        raise
    finally:
        if response is not None:
            response.close()
        transport.close()


async def get_async(
    url_or_path: str,
    *,
    token: str | None = None,
    timeout: float | None = None,
) -> bytes:
    token = ensure_token(token)
    target_url: str
    if is_url(url_or_path):
        target_url = url_or_path
    else:
        metadata = await head_async(url_or_path, token=token)
        target_url = metadata.url

    effective_timeout = timeout or 120.0
    transport = AsyncTransport(create_base_async_client(timeout=effective_timeout))
    response: httpx.Response | None = None
    try:
        response = await transport.send(
            "GET",
            target_url,
            timeout=effective_timeout,
            follow_redirects=True,
        )
        if response.status_code == 404:
            raise BlobNotFoundError()
        response.raise_for_status()
        return response.content
    except httpx.HTTPStatusError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            raise BlobNotFoundError() from exc
        raise
    except httpx.HTTPError:
        raise
    finally:
        if response is not None:
            await response.aclose()
        await transport.aclose()


def list_objects(
    *,
    limit: int | None = None,
    prefix: str | None = None,
    cursor: str | None = None,
    mode: str | None = None,
    token: str | None = None,
) -> ListBlobResultType:
    token = ensure_token(token)
    return _run_sync_blob_operation(
        lambda client: client._list_objects(
            limit=limit,
            prefix=prefix,
            cursor=cursor,
            mode=mode,
            token=token,
        )
    )


async def list_objects_async(
    *,
    limit: int | None = None,
    prefix: str | None = None,
    cursor: str | None = None,
    mode: str | None = None,
    token: str | None = None,
) -> ListBlobResultType:
    token = ensure_token(token)
    async with _AsyncBlobOpsClient() as client:
        return await client._list_objects(
            limit=limit,
            prefix=prefix,
            cursor=cursor,
            mode=mode,
            token=token,
        )


def iter_objects(
    *,
    prefix: str | None = None,
    mode: str | None = None,
    token: str | None = None,
    batch_size: int | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> Iterator[ListBlobItem]:
    token = ensure_token(token)
    with _SyncBlobOpsClient() as client:
        yield from client._iter_objects_sync(
            prefix=prefix,
            mode=mode,
            token=token,
            batch_size=batch_size,
            limit=limit,
            cursor=cursor,
        )


async def iter_objects_async(
    *,
    prefix: str | None = None,
    mode: str | None = None,
    token: str | None = None,
    batch_size: int | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> AsyncIterator[ListBlobItem]:
    token = ensure_token(token)
    async with _AsyncBlobOpsClient() as client:
        async for item in client._iter_objects(
            prefix=prefix,
            mode=mode,
            token=token,
            batch_size=batch_size,
            limit=limit,
            cursor=cursor,
        ):
            yield item


def copy(
    src_path: str,
    dst_path: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> PutBlobResultType:
    token = ensure_token(token)
    return _run_sync_blob_operation(
        lambda client: client._copy_blob(
            src_path,
            dst_path,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
        )
    )


async def copy_async(
    src_path: str,
    dst_path: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
) -> PutBlobResultType:
    token = ensure_token(token)
    async with _AsyncBlobOpsClient() as client:
        return await client._copy_blob(
            src_path,
            dst_path,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
        )


def create_folder(
    path: str,
    *,
    token: str | None = None,
    overwrite: bool = False,
) -> CreateFolderResultType:
    token = ensure_token(token)
    return _run_sync_blob_operation(
        lambda client: client._create_folder(
            path,
            token=token,
            overwrite=overwrite,
        )
    )


async def create_folder_async(
    path: str,
    *,
    token: str | None = None,
    overwrite: bool = False,
) -> CreateFolderResultType:
    token = ensure_token(token)
    async with _AsyncBlobOpsClient() as client:
        return await client._create_folder(
            path,
            token=token,
            overwrite=overwrite,
        )


def upload_file(
    local_path: str | PathLike,
    path: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    multipart: bool = False,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
) -> PutBlobResultType:
    token = ensure_token(token)
    if not local_path:
        raise BlobError("src_path is required")
    if not path:
        raise BlobError("path is required")
    if not os.path.exists(os.fspath(local_path)):
        raise BlobError("local_path does not exist")
    if not os.path.isfile(os.fspath(local_path)):
        raise BlobError("local_path is not a file")

    # Auto-enable multipart if file size exceeds 5 MiB
    size_bytes = os.path.getsize(os.fspath(local_path))
    use_multipart = multipart or (size_bytes > 5 * 1024 * 1024)

    with open(os.fspath(local_path), "rb") as f:
        return put(
            path,
            f,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
            multipart=use_multipart,
            on_upload_progress=on_upload_progress,
        )


async def upload_file_async(
    local_path: str | PathLike,
    path: str,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    multipart: bool = False,
    on_upload_progress: (
        Callable[[UploadProgressEvent], None]
        | Callable[[UploadProgressEvent], Awaitable[None]]
        | None
    ) = None,
) -> PutBlobResultType:
    token = ensure_token(token)
    if not local_path:
        raise BlobError("local_path is required")
    if not path:
        raise BlobError("path is required")
    if not os.path.exists(os.fspath(local_path)):
        raise BlobError("local_path does not exist")
    if not os.path.isfile(os.fspath(local_path)):
        raise BlobError("local_path is not a file")

    # Auto-enable multipart if file size exceeds 5 MiB
    size_bytes = os.path.getsize(os.fspath(local_path))
    use_multipart = multipart or (size_bytes > 5 * 1024 * 1024)

    with open(os.fspath(local_path), "rb") as f:
        return await put_async(
            path,
            f,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            token=token,
            multipart=use_multipart,
            on_upload_progress=on_upload_progress,
        )


def download_file(
    url_or_path: str,
    local_path: str | PathLike,
    *,
    token: str | None = None,
    timeout: float | None = None,
    overwrite: bool = True,
    create_parents: bool = True,
    progress: Callable[[int, int | None], None] | None = None,
) -> str:
    token = ensure_token(token)
    # Resolve remote URL from url_or_path
    if is_url(url_or_path):
        target_url = url_or_path
    else:
        meta = head(url_or_path, token=token)
        target_url = meta.download_url or meta.url

    # Prepare destination
    dst = os.fspath(local_path)
    if not overwrite and os.path.exists(dst):
        raise BlobError("destination exists; pass overwrite=True to replace it")
    if create_parents:
        os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)

    tmp = dst + ".part"
    bytes_read = 0
    effective_timeout = timeout or 120.0
    transport = SyncTransport(create_base_client(timeout=effective_timeout))
    response: httpx.Response | None = None

    try:
        response = iter_coroutine(
            transport.send(
                "GET",
                target_url,
                timeout=effective_timeout,
                follow_redirects=True,
                stream=True,
            )
        )
        if response.status_code == 404:
            raise BlobNotFoundError()
        response.raise_for_status()
        total = int(response.headers.get("Content-Length", "0")) or None
        with open(tmp, "wb") as f:
            for chunk in response.iter_bytes():
                if chunk:
                    f.write(chunk)
                    bytes_read += len(chunk)
                    if progress:
                        progress(bytes_read, total)

        os.replace(tmp, dst)  # atomic finalize
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    finally:
        if response is not None:
            response.close()
        transport.close()
    return dst


async def download_file_async(
    url_or_path: str,
    local_path: str | PathLike,
    *,
    token: str | None = None,
    timeout: float | None = None,
    overwrite: bool = True,
    create_parents: bool = True,
    progress: (
        Callable[[int, int | None], None] | Callable[[int, int | None], Awaitable[None]] | None
    ) = None,
) -> str:
    token = ensure_token(token)
    # Resolve remote URL from url_or_path
    if is_url(url_or_path):
        target_url = url_or_path
    else:
        meta = await head_async(url_or_path, token=token)
        target_url = meta.download_url or meta.url

    # Prepare destination
    dst = os.fspath(local_path)
    if not overwrite and os.path.exists(dst):
        raise BlobError("destination exists; pass overwrite=True to replace it")
    if create_parents:
        os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)

    tmp = dst + ".part"
    bytes_read = 0
    effective_timeout = timeout or 120.0
    transport = AsyncTransport(create_base_async_client(timeout=effective_timeout))
    response: httpx.Response | None = None

    try:
        response = await transport.send(
            "GET",
            target_url,
            timeout=effective_timeout,
            follow_redirects=True,
            stream=True,
        )
        if response.status_code == 404:
            raise BlobNotFoundError()
        response.raise_for_status()
        total = int(response.headers.get("Content-Length", "0")) or None
        with open(tmp, "wb") as f:
            async for chunk in response.aiter_bytes():
                if chunk:
                    f.write(chunk)
                    bytes_read += len(chunk)
                    if progress:
                        maybe = progress(bytes_read, total)
                        if inspect.isawaitable(maybe):
                            await maybe

        os.replace(tmp, dst)  # atomic finalize
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    finally:
        if response is not None:
            await response.aclose()
        await transport.aclose()
    return dst
