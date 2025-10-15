from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Awaitable
import httpx

from .errors import (
    BlobAccessError,
    BlobClientTokenExpiredError,
    BlobContentTypeNotAllowedError,
    BlobError,
    BlobFileTooLargeError,
    BlobNotFoundError,
    BlobServiceNotAvailable,
    BlobServiceRateLimited,
    BlobStoreNotFoundError,
    BlobStoreSuspendedError,
    BlobUnknownError,
)
from .utils import (
    StreamingBodyWithProgress,
    UploadProgressEvent,
    compute_body_length,
    debug,
    get_api_url,
    get_api_version,
    get_proxy_through_alternative_api_header_from_env,
    get_retries,
    get_token_from_options_or_env,
    make_request_id,
    parse_rfc7231_retry_after,
    should_use_x_content_length,
    extract_store_id_from_token,
)
from .errors import BlobPathnameMismatchError


def _map_error(response: httpx.Response) -> tuple[str, BlobError]:
    try:
        data = response.json()
    except Exception:
        data = {}

    code = (data.get("error") or {}).get("code") or "unknown_error"
    message = (data.get("error") or {}).get("message") or ""

    # Heuristics mirroring TS SDK: https://github.com/vercel/storage/blob/main/packages/blob/src/api.ts
    if "contentType" in message and "is not allowed" in message:
        code = "content_type_not_allowed"
    if '"pathname"' in message and "does not match the token payload" in message:
        code = "client_token_pathname_mismatch"
    if message == "Token expired":
        code = "client_token_expired"
    if "the file length cannot be greater than" in message:
        code = "file_too_large"

    if code == "store_suspended":
        return code, BlobStoreSuspendedError()
    if code == "forbidden":
        return code, BlobAccessError()
    if code == "content_type_not_allowed":
        return code, BlobContentTypeNotAllowedError(message or "")
    if code == "client_token_pathname_mismatch":
        return code, BlobPathnameMismatchError(message or "")
    if code == "client_token_expired":
        return code, BlobClientTokenExpiredError()
    if code == "file_too_large":
        return code, BlobFileTooLargeError(message or "")
    if code == "not_found":
        return code, BlobNotFoundError()
    if code == "store_not_found":
        return code, BlobStoreNotFoundError()
    if code == "bad_request":
        return code, BlobError(message or "Bad request")
    if code == "service_unavailable":
        return code, BlobServiceNotAvailable()
    if code == "rate_limited":
        seconds = parse_rfc7231_retry_after(response.headers.get("retry-after"))
        return code, BlobServiceRateLimited(seconds)

    return code, BlobUnknownError()


def _should_retry(code: str) -> bool:
    return code in {"unknown_error", "service_unavailable", "internal_server_error"}


def _is_network_error(exc: Exception) -> bool:
    return isinstance(exc, httpx.TransportError)


def request_api(
    pathname: str,
    method: str,
    options: dict[str, Any] | None = None,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    body: Any = None,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
    timeout: float | None = None,
) -> Any:
    """Synchronous HTTP caller with retries, headers, progress and error mapping."""
    token = get_token_from_options_or_env(options or {})
    request_id = make_request_id(store_id="")
    attempt = 0
    retries = get_retries()
    api_version = get_api_version()
    extra_headers = get_proxy_through_alternative_api_header_from_env()

    send_body_length = bool(on_upload_progress) or should_use_x_content_length()
    total_length = compute_body_length(body) if send_body_length else 0

    if on_upload_progress:
        on_upload_progress(UploadProgressEvent(loaded=0, total=total_length, percentage=0.0))

    url = get_api_url(pathname)
    timeout_conf = httpx.Timeout(timeout) if timeout is not None else httpx.Timeout(30.0)

    with httpx.Client(timeout=timeout_conf) as client:
        for attempt in range(retries + 1):
            try:
                final_headers = {
                    "authorization": f"Bearer {token}",
                    "x-api-blob-request-id": request_id,
                    "x-api-blob-request-attempt": str(attempt),
                    "x-api-version": api_version,
                    **extra_headers,
                }
                if headers:
                    final_headers.update(headers)
                if send_body_length and total_length:
                    final_headers["x-content-length"] = str(total_length)

                json_body = None
                content = None

                if body is not None:
                    if isinstance(body, (bytes, bytearray, memoryview, str)) or hasattr(
                        body, "read"
                    ):
                        content = StreamingBodyWithProgress(body, on_upload_progress)
                    else:
                        json_body = body

                resp = client.request(
                    method=method,
                    url=url,
                    headers=final_headers,
                    params=params,
                    content=content,
                    json=json_body,
                )

                if 200 <= resp.status_code < 300:
                    if on_upload_progress:
                        on_upload_progress(
                            UploadProgressEvent(
                                loaded=total_length or 0,
                                total=total_length or 0,
                                percentage=100.0,
                            )
                        )
                    content_type = resp.headers.get("content-type", "")
                    if "application/json" in content_type or (resp.text or "").startswith("{"):
                        try:
                            return resp.json()
                        except Exception:
                            return resp.text
                    try:
                        return resp.json()
                    except Exception:
                        return resp.text

                code, mapped = _map_error(resp)
                if _should_retry(code) and attempt < retries:
                    debug(f"retrying API request to {pathname}", f"{code}")
                    time.sleep(min(2**attempt * 0.1, 2.0))
                    continue
                raise mapped

            except Exception as exc:
                if _is_network_error(exc) and attempt < retries:
                    debug(f"retrying API request to {pathname}", str(exc))
                    time.sleep(min(2**attempt * 0.1, 2.0))
                    continue
                if isinstance(exc, httpx.HTTPError):
                    raise BlobUnknownError() from exc
                raise

    raise BlobUnknownError()


async def request_api_async(
    pathname: str,
    method: str,
    options: dict[str, Any] | None = None,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    body: Any = None,
    on_upload_progress: Callable[[UploadProgressEvent], None]
    | Callable[[UploadProgressEvent], Awaitable[None]]
    | None = None,
    timeout: float | None = None,
) -> Any:
    """Core HTTP caller with retries, headers, progress and error mapping."""
    token = get_token_from_options_or_env(options or {})
    store_id = extract_store_id_from_token(token)
    request_id = make_request_id(store_id)
    attempt = 0
    retries = get_retries()
    api_version = get_api_version()
    extra_headers = get_proxy_through_alternative_api_header_from_env()

    send_body_length = bool(on_upload_progress) or should_use_x_content_length()
    total_length = compute_body_length(body) if send_body_length else 0

    if on_upload_progress:
        result = on_upload_progress(
            UploadProgressEvent(loaded=0, total=total_length, percentage=0.0)
        )
        # If callback is async, await it
        if asyncio.iscoroutine(result):
            await result

    url = get_api_url(pathname)
    timeout_conf = httpx.Timeout(timeout) if timeout is not None else httpx.Timeout(30.0)
    async with httpx.AsyncClient(timeout=timeout_conf) as client:
        for attempt in range(retries + 1):
            try:
                final_headers = {
                    "authorization": f"Bearer {token}",
                    "x-api-blob-request-id": request_id,
                    "x-api-blob-request-attempt": str(attempt),
                    "x-api-version": api_version,
                    **extra_headers,
                }
                if headers:
                    final_headers.update(headers)
                if send_body_length and total_length:
                    final_headers["x-content-length"] = str(total_length)

                json_body = None
                content = None

                # Wrap body for progress when possible
                if body is not None:
                    if isinstance(body, (bytes, bytearray, memoryview, str)) or hasattr(
                        body, "read"
                    ):
                        wrapped = StreamingBodyWithProgress(body, on_upload_progress)
                        # For AsyncClient, ensure async streaming content to avoid sync-body error
                        content = wrapped.__aiter__()
                    else:
                        # For objects meant to be JSON
                        json_body = body

                if content is not None:
                    resp = await client.request(
                        method=method,
                        url=url,
                        headers=final_headers,
                        params=params,
                        content=content,
                        json=json_body,
                    )
                else:
                    resp = await client.request(
                        method=method,
                        url=url,
                        headers=final_headers,
                        params=params,
                        json=json_body,
                    )

                if 200 <= resp.status_code < 300:
                    if on_upload_progress:
                        result = on_upload_progress(
                            UploadProgressEvent(
                                loaded=total_length or 0,
                                total=total_length or 0,
                                percentage=100.0,
                            )
                        )
                        # If callback is async, await it
                        if asyncio.iscoroutine(result):
                            await result
                    content_type = resp.headers.get("content-type", "")
                    if "application/json" in content_type or resp.text.startswith("{"):
                        try:
                            return resp.json()
                        except Exception:
                            return resp.text
                    try:
                        return resp.json()
                    except Exception:
                        return resp.text

                code, mapped = _map_error(resp)
                if _should_retry(code) and attempt < retries:
                    debug(f"retrying API request to {pathname}", f"{code}")
                    await asyncio.sleep(min(2**attempt * 0.1, 2.0))
                    continue
                raise mapped

            except Exception as exc:
                # If it's an httpx transport error, treat as network and retry; else raise
                if _is_network_error(exc) and attempt < retries:
                    debug(f"retrying API request to {pathname}", str(exc))
                    await asyncio.sleep(min(2**attempt * 0.1, 2.0))
                    continue
                if isinstance(exc, httpx.HTTPError):
                    raise BlobUnknownError() from exc
                raise

        raise BlobUnknownError()
