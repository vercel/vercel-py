from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import Any

from .._http import AsyncTransport, BlockingTransport, create_base_async_client, create_base_client
from .._iter_coroutine import iter_coroutine
from ._core import request_api_core
from .utils import PutHeaders, UploadProgressEvent


def _blocking_sleep(seconds: float) -> None:
    time.sleep(seconds)


def request_api(
    pathname: str,
    method: str,
    *,
    token: str | None = None,
    headers: PutHeaders | dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    body: Any = None,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
    timeout: float | None = None,
) -> Any:
    """Synchronous HTTP caller that delegates to the async blob request core."""
    effective_timeout = timeout if timeout is not None else 30.0
    transport = BlockingTransport(create_base_client(timeout=effective_timeout))
    try:
        return iter_coroutine(
            request_api_core(
                pathname,
                method,
                token=token,
                headers=headers,
                params=params,
                body=body,
                on_upload_progress=on_upload_progress,
                timeout=timeout,
                transport=transport,
                sleep_fn=_blocking_sleep,
                await_progress_callback=False,
                async_content=False,
            )
        )
    finally:
        transport.close()


async def request_api_async(
    pathname: str,
    method: str,
    *,
    token: str | None = None,
    headers: PutHeaders | dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    body: Any = None,
    on_upload_progress: (
        Callable[[UploadProgressEvent], None]
        | Callable[[UploadProgressEvent], Awaitable[None]]
        | None
    ) = None,
    timeout: float | None = None,
) -> Any:
    """Asynchronous HTTP caller backed by the shared blob request core."""
    effective_timeout = timeout if timeout is not None else 30.0
    transport = AsyncTransport(create_base_async_client(timeout=effective_timeout))
    try:
        return await request_api_core(
            pathname,
            method,
            token=token,
            headers=headers,
            params=params,
            body=body,
            on_upload_progress=on_upload_progress,
            timeout=timeout,
            transport=transport,
            async_content=True,
        )
    finally:
        await transport.aclose()
