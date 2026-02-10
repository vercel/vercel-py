from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from typing import Any, cast

import httpx

from .._http import (
    AsyncTransport,
    BaseTransport,
    BlockingTransport,
    JSONBody,
    RawBody,
    create_base_async_client,
    create_base_client,
)
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
from .types import (
    CreateFolderResult as CreateFolderResultType,
    HeadBlobResult as HeadBlobResultType,
    ListBlobItem,
    ListBlobResult as ListBlobResultType,
    PutBlobResult as PutBlobResultType,
)
from .utils import (
    PutHeaders,
    StreamingBodyWithProgress,
    UploadProgressEvent,
    compute_body_length,
    create_put_headers,
    debug,
    ensure_token,
    extract_store_id_from_token,
    get_api_url,
    get_api_version,
    get_proxy_through_alternative_api_header_from_env,
    get_retries,
    is_url,
    make_request_id,
    parse_datetime,
    parse_rfc7231_retry_after,
    require_public_access,
    should_use_x_content_length,
    validate_path,
)

BlobProgressCallback = (
    Callable[[UploadProgressEvent], None] | Callable[[UploadProgressEvent], Awaitable[None]]
)
SleepFn = Callable[[float], Awaitable[None] | None]
PUT_BODY_OBJECT_ERROR = (
    "Body must be a string, buffer or stream. "
    "You sent a plain object, double check what you're trying to upload."
)


def _blocking_sleep(seconds: float) -> None:
    time.sleep(seconds)


async def _await_if_necessary(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await cast(Awaitable[Any], value)
    return value


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


async def _emit_progress(
    callback: BlobProgressCallback | None,
    event: UploadProgressEvent,
    *,
    await_callback: bool,
) -> None:
    if callback is None:
        return

    result = callback(event)
    if await_callback and inspect.isawaitable(result):
        await cast(Awaitable[None], result)


async def _sleep_with_backoff(
    sleep_fn: SleepFn,
    attempt: int,
) -> None:
    delay = min(2**attempt * 0.1, 2.0)
    result = sleep_fn(delay)
    if inspect.isawaitable(result):
        await cast(Awaitable[None], result)


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


def get_telemetry_size_bytes(body: Any) -> int | None:
    if isinstance(body, (bytes, bytearray)):
        return len(body)
    if isinstance(body, str):
        return len(body.encode())
    return None


def _validate_put_inputs(path: str, body: Any, access: str) -> None:
    validate_path(path)
    require_public_access(access)
    if body is None:
        raise BlobError("body is required")
    if isinstance(body, dict):
        raise BlobError(PUT_BODY_OBJECT_ERROR)


def normalize_delete_urls(url_or_path: str | Iterable[str]) -> list[str]:
    if isinstance(url_or_path, Iterable) and not isinstance(url_or_path, (str, bytes)):
        return [str(url) for url in url_or_path]
    return [str(url_or_path)]


def build_put_blob_result(raw: dict[str, Any]) -> PutBlobResultType:
    return PutBlobResultType(
        url=raw["url"],
        download_url=raw["downloadUrl"],
        pathname=raw["pathname"],
        content_type=raw["contentType"],
        content_disposition=raw["contentDisposition"],
    )


def build_head_blob_result(resp: dict[str, Any]) -> HeadBlobResultType:
    uploaded_at = (
        parse_datetime(resp["uploadedAt"])
        if isinstance(resp.get("uploadedAt"), str)
        else resp["uploadedAt"]
    )
    return HeadBlobResultType(
        size=resp["size"],
        uploaded_at=uploaded_at,
        pathname=resp["pathname"],
        content_type=resp["contentType"],
        content_disposition=resp["contentDisposition"],
        url=resp["url"],
        download_url=resp["downloadUrl"],
        cache_control=resp["cacheControl"],
    )


def build_list_blob_result(resp: dict[str, Any]) -> ListBlobResultType:
    blobs_list: list[ListBlobItem] = []
    for blob in resp.get("blobs", []):
        uploaded_at = (
            parse_datetime(blob["uploadedAt"])
            if isinstance(blob.get("uploadedAt"), str)
            else blob["uploadedAt"]
        )
        blobs_list.append(
            ListBlobItem(
                url=blob["url"],
                download_url=blob["downloadUrl"],
                pathname=blob["pathname"],
                size=blob["size"],
                uploaded_at=uploaded_at,
            )
        )
    return ListBlobResultType(
        blobs=blobs_list,
        cursor=resp.get("cursor"),
        has_more=resp.get("hasMore", False),
        folders=resp.get("folders"),
    )


def build_create_folder_result(raw: dict[str, Any]) -> CreateFolderResultType:
    return CreateFolderResultType(pathname=raw["pathname"], url=raw["url"])


def build_list_params(
    *,
    limit: int | None = None,
    prefix: str | None = None,
    cursor: str | None = None,
    mode: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if limit is not None:
        params["limit"] = int(limit)
    if prefix is not None:
        params["prefix"] = prefix
    if cursor is not None:
        params["cursor"] = cursor
    if mode is not None:
        params["mode"] = mode
    return params


class _BlobRequestClient:
    _transport: BaseTransport
    _sleep_fn: SleepFn
    _await_progress_callback: bool
    _async_content: bool

    def __init__(
        self,
        *,
        transport: BaseTransport,
        sleep_fn: SleepFn = asyncio.sleep,
        await_progress_callback: bool = True,
        async_content: bool = True,
    ) -> None:
        self._transport = transport
        self._sleep_fn = sleep_fn
        self._await_progress_callback = await_progress_callback
        self._async_content = async_content

    async def _request_api(
        self,
        pathname: str,
        method: str,
        *,
        token: str | None = None,
        headers: PutHeaders | dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        body: Any = None,
        on_upload_progress: BlobProgressCallback | None = None,
        timeout: float | None = None,
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
                await_callback=self._await_progress_callback,
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
                    async_content=self._async_content,
                )
                resp = await self._transport.send(
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
                            await_callback=self._await_progress_callback,
                        )
                    return decode_blob_response(resp)

                code, mapped = map_blob_error(resp)
                if should_retry(code) and attempt < retries:
                    debug(f"retrying API request to {pathname}", code)
                    await _sleep_with_backoff(self._sleep_fn, attempt)
                    continue
                raise mapped
            except Exception as exc:
                if is_network_error(exc) and attempt < retries:
                    debug(f"retrying API request to {pathname}", str(exc))
                    await _sleep_with_backoff(self._sleep_fn, attempt)
                    continue
                if isinstance(exc, httpx.HTTPError):
                    raise BlobUnknownError() from exc
                raise

        raise BlobUnknownError()


class _BaseBlobOpsClient(_BlobRequestClient):
    def __init__(
        self,
        *,
        transport: BaseTransport,
        sleep_fn: SleepFn = asyncio.sleep,
        await_progress_callback: bool = True,
        async_content: bool = True,
        multipart_runtime: Any,
        create_multipart_upload: Callable[..., dict[str, Any] | Awaitable[dict[str, Any]]],
        complete_multipart_upload: Callable[..., dict[str, Any] | Awaitable[dict[str, Any]]],
    ) -> None:
        super().__init__(
            transport=transport,
            sleep_fn=sleep_fn,
            await_progress_callback=await_progress_callback,
            async_content=async_content,
        )
        self._multipart_runtime = multipart_runtime
        self._create_multipart_upload = create_multipart_upload
        self._complete_multipart_upload = complete_multipart_upload

    async def _multipart_upload(
        self,
        path: str,
        body: Any,
        *,
        access: str,
        content_type: str | None = None,
        add_random_suffix: bool = False,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
        token: str | None = None,
        on_upload_progress: BlobProgressCallback | None = None,
    ) -> dict[str, Any]:
        from .multipart.uploader import (
            DEFAULT_PART_SIZE,
            _MultipartUploadSession,
            _order_uploaded_parts,
            _prepare_upload_headers,
            _shape_complete_upload_result,
            _validate_part_size,
        )

        headers = _prepare_upload_headers(
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
        )
        part_size = _validate_part_size(DEFAULT_PART_SIZE)

        create_response = cast(
            dict[str, str],
            await _await_if_necessary(self._create_multipart_upload(path, headers, token=token)),
        )
        session = _MultipartUploadSession(
            upload_id=create_response["uploadId"],
            key=create_response["key"],
            path=path,
            headers=headers,
            token=token,
        )

        total = compute_body_length(body)
        parts = cast(
            list[dict[str, Any]],
            await _await_if_necessary(
                self._multipart_runtime.upload(
                    session=session,
                    body=body,
                    part_size=part_size,
                    total=total,
                    on_upload_progress=on_upload_progress,
                )
            ),
        )
        ordered_parts = _order_uploaded_parts(parts)

        complete_response = cast(
            dict[str, Any],
            await _await_if_necessary(
                self._complete_multipart_upload(
                    upload_id=session.upload_id,
                    key=session.key,
                    path=session.path,
                    headers=session.headers,
                    token=session.token,
                    parts=ordered_parts,
                )
            ),
        )
        return _shape_complete_upload_result(complete_response)

    async def _put_blob(
        self,
        path: str,
        body: Any,
        *,
        access: str,
        content_type: str | None,
        add_random_suffix: bool,
        overwrite: bool,
        cache_control_max_age: int | None,
        token: str | None,
        multipart: bool,
        on_upload_progress: BlobProgressCallback | None,
    ) -> tuple[PutBlobResultType, bool]:
        token = ensure_token(token)
        _validate_put_inputs(path, body, access)

        headers = create_put_headers(
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            allow_overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
        )

        if multipart:
            raw = await self._multipart_upload(
                path,
                body,
                access=access,
                content_type=content_type,
                add_random_suffix=add_random_suffix,
                overwrite=overwrite,
                cache_control_max_age=cache_control_max_age,
                token=token,
                on_upload_progress=on_upload_progress,
            )
            return build_put_blob_result(raw), True

        raw = cast(
            dict[str, Any],
            await self._request_api(
                "",
                "PUT",
                token=token,
                headers=headers,
                params={"pathname": path},
                body=body,
                on_upload_progress=on_upload_progress,
            ),
        )
        return build_put_blob_result(raw), False

    async def _delete_blob(
        self,
        url_or_path: str | Iterable[str],
        *,
        token: str | None,
    ) -> int:
        token = ensure_token(token)
        urls = normalize_delete_urls(url_or_path)
        await self._request_api(
            "/delete",
            "POST",
            token=token,
            headers={"content-type": "application/json"},
            body={"urls": urls},
        )
        return len(urls)

    async def _head_blob(
        self,
        url_or_path: str,
        *,
        token: str | None,
    ) -> HeadBlobResultType:
        token = ensure_token(token)
        resp = cast(
            dict[str, Any],
            await self._request_api(
                "",
                "GET",
                token=token,
                params={"url": url_or_path},
            ),
        )
        return build_head_blob_result(resp)

    async def _list_objects(
        self,
        *,
        limit: int | None,
        prefix: str | None,
        cursor: str | None,
        mode: str | None,
        token: str | None,
    ) -> ListBlobResultType:
        token = ensure_token(token)
        resp = cast(
            dict[str, Any],
            await self._request_api(
                "",
                "GET",
                token=token,
                params=build_list_params(limit=limit, prefix=prefix, cursor=cursor, mode=mode),
            ),
        )
        return build_list_blob_result(resp)

    async def _iter_objects(
        self,
        *,
        prefix: str | None,
        mode: str | None,
        token: str | None,
        batch_size: int | None,
        limit: int | None,
        cursor: str | None,
    ) -> AsyncIterator[ListBlobItem]:
        token = ensure_token(token)
        next_cursor = cursor
        yielded_count = 0

        while True:
            effective_limit: int | None = batch_size
            if limit is not None:
                remaining = limit - yielded_count
                if remaining <= 0:
                    break
                if effective_limit is None or effective_limit > remaining:
                    effective_limit = remaining

            page = await self._list_objects(
                limit=effective_limit,
                prefix=prefix,
                cursor=next_cursor,
                mode=mode,
                token=token,
            )

            for item in page.blobs:
                yield item
                if limit is not None:
                    yielded_count += 1
                    if yielded_count >= limit:
                        return

            if not page.has_more or not page.cursor:
                break
            next_cursor = page.cursor

    async def _copy_blob(
        self,
        src_path: str,
        dst_path: str,
        *,
        access: str,
        content_type: str | None,
        add_random_suffix: bool,
        overwrite: bool,
        cache_control_max_age: int | None,
        token: str | None,
    ) -> PutBlobResultType:
        token = ensure_token(token)
        validate_path(dst_path)
        require_public_access(access)

        src_url = src_path
        if not is_url(src_url):
            src_url = (await self._head_blob(src_url, token=token)).url

        headers = create_put_headers(
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            allow_overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
        )
        raw = cast(
            dict[str, Any],
            await self._request_api(
                "",
                "PUT",
                token=token,
                headers=headers,
                params={"pathname": str(dst_path), "fromUrl": src_url},
            ),
        )
        return build_put_blob_result(raw)

    async def _create_folder(
        self,
        path: str,
        *,
        token: str | None,
        overwrite: bool,
    ) -> CreateFolderResultType:
        token = ensure_token(token)
        folder_path = path if path.endswith("/") else f"{path}/"
        headers = create_put_headers(
            add_random_suffix=False,
            allow_overwrite=overwrite,
        )
        raw = cast(
            dict[str, Any],
            await self._request_api(
                "",
                "PUT",
                token=token,
                headers=headers,
                params={"pathname": folder_path},
            ),
        )
        return build_create_folder_result(raw)


class _SyncBlobOpsClient(_BaseBlobOpsClient):
    def __init__(self, *, timeout: float = 30.0) -> None:
        from .multipart.core import (
            call_complete_multipart_upload,
            call_create_multipart_upload,
        )
        from .multipart.uploader import create_blocking_multipart_upload_runtime

        transport = BlockingTransport(create_base_client(timeout=timeout))
        super().__init__(
            transport=transport,
            sleep_fn=_blocking_sleep,
            await_progress_callback=False,
            async_content=False,
            multipart_runtime=create_blocking_multipart_upload_runtime(),
            create_multipart_upload=call_create_multipart_upload,
            complete_multipart_upload=call_complete_multipart_upload,
        )

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> _SyncBlobOpsClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class _AsyncBlobOpsClient(_BaseBlobOpsClient):
    def __init__(self, *, timeout: float = 30.0) -> None:
        from .multipart.core import (
            call_complete_multipart_upload_async,
            call_create_multipart_upload_async,
        )
        from .multipart.uploader import create_async_multipart_upload_runtime

        transport = AsyncTransport(create_base_async_client(timeout=timeout))
        super().__init__(
            transport=transport,
            multipart_runtime=create_async_multipart_upload_runtime(),
            create_multipart_upload=call_create_multipart_upload_async,
            complete_multipart_upload=call_complete_multipart_upload_async,
        )

    async def aclose(self) -> None:
        await self._transport.aclose()

    async def __aenter__(self) -> _AsyncBlobOpsClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()


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
    request_client = _BlobRequestClient(
        transport=transport,
        sleep_fn=sleep_fn,
        await_progress_callback=await_progress_callback,
        async_content=async_content,
    )
    return await request_client._request_api(
        pathname,
        method,
        token=token,
        headers=headers,
        params=params,
        body=body,
        on_upload_progress=on_upload_progress,
        timeout=timeout,
    )


__all__ = [
    "_AsyncBlobOpsClient",
    "_SyncBlobOpsClient",
    "build_create_folder_result",
    "build_head_blob_result",
    "build_list_blob_result",
    "build_list_params",
    "build_put_blob_result",
    "decode_blob_response",
    "get_telemetry_size_bytes",
    "is_network_error",
    "map_blob_error",
    "normalize_delete_urls",
    "request_api_core",
    "should_retry",
]
