from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Any, cast

import httpx

from .._http import BaseTransport, JSONBody, RawBody
from .errors import (
    BlobAccessError,
    BlobClientTokenExpiredError,
    BlobContentTypeNotAllowedError,
    BlobError,
    BlobFileTooLargeError,
    BlobNotFoundError,
    BlobPathnameMismatchError,
    BlobServiceNotAvailable,
    BlobServiceRateLimited,
    BlobStoreNotFoundError,
    BlobStoreSuspendedError,
    BlobUnknownError,
)
from .utils import (
    PutHeaders,
    StreamingBodyWithProgress,
    UploadProgressEvent,
    compute_body_length,
    debug,
    ensure_token,
    extract_store_id_from_token,
    get_api_url,
    get_api_version,
    get_proxy_through_alternative_api_header_from_env,
    get_retries,
    make_request_id,
    parse_rfc7231_retry_after,
    should_use_x_content_length,
)

BlobProgressCallback = (
    Callable[[UploadProgressEvent], None] | Callable[[UploadProgressEvent], Awaitable[None]]
)
SleepFn = Callable[[float], Awaitable[None] | None]


def map_blob_error(response: httpx.Response) -> tuple[str, BlobError]:
    try:
        data = response.json()
    except Exception:
        data = {}

    code = (data.get("error") or {}).get("code") or "unknown_error"
    message = (data.get("error") or {}).get("message") or ""

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


def should_retry(code: str) -> bool:
    return code in {"unknown_error", "service_unavailable", "internal_server_error"}


def is_network_error(exc: Exception) -> bool:
    return isinstance(exc, httpx.TransportError)


def decode_blob_response(response: httpx.Response) -> Any:
    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type or (response.text or "").startswith("{"):
        try:
            return response.json()
        except Exception:
            return response.text
    try:
        return response.json()
    except Exception:
        return response.text


async def _maybe_await(value: Any) -> None:
    if inspect.isawaitable(value):
        await value


async def _emit_progress(
    callback: BlobProgressCallback | None,
    event: UploadProgressEvent,
    *,
    await_callback: bool,
) -> None:
    if callback is None:
        return

    result = callback(event)
    if await_callback:
        await _maybe_await(result)


async def _sleep_with_backoff(
    sleep_fn: SleepFn,
    attempt: int,
) -> None:
    delay = min(2**attempt * 0.1, 2.0)
    await _maybe_await(sleep_fn(delay))


def _build_headers(
    *,
    token: str,
    request_id: str,
    attempt: int,
    extra_headers: dict[str, str],
    request_headers: dict[str, str],
    send_body_length: bool,
    total_length: int,
    api_version: str,
) -> dict[str, str]:
    final_headers = {
        "authorization": f"Bearer {token}",
        "x-api-blob-request-id": request_id,
        "x-api-blob-request-attempt": str(attempt),
        "x-api-version": api_version,
        **extra_headers,
    }
    if request_headers:
        final_headers.update(request_headers)
    if send_body_length and total_length:
        final_headers["x-content-length"] = str(total_length)
    return final_headers


def _build_request_body(
    body: Any,
    *,
    on_upload_progress: BlobProgressCallback | None,
    async_content: bool,
) -> JSONBody | RawBody | None:
    if body is None:
        return None

    if isinstance(body, (bytes, bytearray, memoryview, str)) or hasattr(body, "read"):
        wrapped = StreamingBodyWithProgress(
            cast(bytes | bytearray | memoryview | str | Any, body),
            on_upload_progress,
        )
        content = wrapped.__aiter__() if async_content else wrapped
        return RawBody(content)

    return JSONBody(body)


async def request_api_core(
    pathname: str,
    method: str,
    *,
    token: str | None = None,
    headers: PutHeaders | dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    body: Any = None,
    on_upload_progress: BlobProgressCallback | None = None,
    timeout: float | None = None,
    transport: BaseTransport,
    sleep_fn: SleepFn = asyncio.sleep,
    await_progress_callback: bool = True,
    async_content: bool = True,
) -> Any:
    token = ensure_token(token)
    store_id = extract_store_id_from_token(token)
    request_id = make_request_id(store_id)
    retries = get_retries()
    api_version = get_api_version()
    extra_headers = get_proxy_through_alternative_api_header_from_env()
    request_headers = cast(dict[str, str], headers or {})

    send_body_length = bool(on_upload_progress) or should_use_x_content_length()
    total_length = compute_body_length(body) if send_body_length else 0

    if on_upload_progress:
        await _emit_progress(
            on_upload_progress,
            UploadProgressEvent(loaded=0, total=total_length, percentage=0.0),
            await_callback=await_progress_callback,
        )

    url = get_api_url(pathname)
    effective_timeout = timeout if timeout is not None else 30.0

    for attempt in range(retries + 1):
        try:
            final_headers = _build_headers(
                token=token,
                request_id=request_id,
                attempt=attempt,
                extra_headers=extra_headers,
                request_headers=request_headers,
                send_body_length=send_body_length,
                total_length=total_length,
                api_version=api_version,
            )

            request_body = _build_request_body(
                body,
                on_upload_progress=on_upload_progress,
                async_content=async_content,
            )

            resp = await transport.send(
                method=method,
                path=url,
                headers=final_headers,
                params=params,
                body=request_body,
                timeout=effective_timeout,
            )

            if 200 <= resp.status_code < 300:
                if on_upload_progress:
                    await _emit_progress(
                        on_upload_progress,
                        UploadProgressEvent(
                            loaded=total_length or 0,
                            total=total_length or 0,
                            percentage=100.0,
                        ),
                        await_callback=await_progress_callback,
                    )
                return decode_blob_response(resp)

            code, mapped = map_blob_error(resp)
            if should_retry(code) and attempt < retries:
                debug(f"retrying API request to {pathname}", code)
                await _sleep_with_backoff(sleep_fn, attempt)
                continue
            raise mapped

        except Exception as exc:
            if is_network_error(exc) and attempt < retries:
                debug(f"retrying API request to {pathname}", str(exc))
                await _sleep_with_backoff(sleep_fn, attempt)
                continue
            if isinstance(exc, httpx.HTTPError):
                raise BlobUnknownError() from exc
            raise

    raise BlobUnknownError()


__all__ = [
    "decode_blob_response",
    "is_network_error",
    "map_blob_error",
    "request_api_core",
    "should_retry",
]
