from __future__ import annotations

import asyncio
import inspect
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Literal, cast
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx

from vercel._internal.blob import (
    PutHeaders,
    StreamingBodyWithProgress,
    compute_body_length,
    construct_blob_url,
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
    should_use_x_content_length,
    validate_access,
    validate_path,
)
from vercel._internal.http import (
    AsyncTransport,
    BaseTransport,
    JSONBody,
    RawBody,
    SyncTransport,
    create_base_async_client,
    create_base_client,
)
from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.telemetry.tracker import track
from vercel._internal.blob import get_download_url
from vercel._internal.blob.errors import (
    BlobAccessError,
    BlobClientTokenExpiredError,
    BlobContentTypeNotAllowedError,
    BlobError,
    BlobFileTooLargeError,
    BlobInvalidResponseJSONError,
    BlobNotFoundError,
    BlobPathnameMismatchError,
    BlobServiceNotAvailable,
    BlobServiceRateLimited,
    BlobStoreNotFoundError,
    BlobStoreSuspendedError,
    BlobUnexpectedResponseContentTypeError,
    BlobUnknownError,
)
from vercel._internal.blob.multipart import (
    DEFAULT_PART_SIZE,
    MultipartClient,
    MultipartUploadSession,
    create_async_multipart_upload_runtime,
    create_sync_multipart_upload_runtime,
    order_uploaded_parts,
    prepare_upload_headers,
    shape_complete_upload_result,
    validate_part_size,
)
from vercel._internal.blob.types import (
    Access,
    CreateFolderResult as CreateFolderResultType,
    GetBlobResult as GetBlobResultType,
    HeadBlobResult as HeadBlobResultType,
    ListBlobItem,
    ListBlobResult as ListBlobResultType,
    PutBlobResult as PutBlobResultType,
    UploadProgressEvent,
)

BlobProgressCallback = (
    Callable[[UploadProgressEvent], None] | Callable[[UploadProgressEvent], Awaitable[None]]
)
DownloadProgressCallback = (
    Callable[[int, int | None], None] | Callable[[int, int | None], Awaitable[None]]
)
SleepFn = Callable[[float], Awaitable[None] | None]
PUT_BODY_OBJECT_ERROR = (
    "Body must be a string, buffer or stream. "
    "You sent a plain object, double check what you're trying to upload."
)


def _sync_sleep(seconds: float) -> None:
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
    try:
        return response.json()
    except Exception:
        return response.text


def _is_json_content_type(content_type: str) -> bool:
    media_type = content_type.split(";", 1)[0].strip().lower()
    return media_type == "application/json" or media_type.endswith("+json")


def decode_blob_response_json(response: httpx.Response) -> Any:
    content_type = response.headers.get("content-type", "")
    if not _is_json_content_type(content_type):
        raise BlobUnexpectedResponseContentTypeError(content_type or None)

    try:
        return response.json()
    except Exception as exc:
        raise BlobInvalidResponseJSONError() from exc


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


async def _emit_download_progress(
    callback: DownloadProgressCallback | None,
    loaded: int,
    total: int | None,
    *,
    await_callback: bool,
) -> None:
    if callback is None:
        return

    result = callback(loaded, total)
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
    validate_access(access)
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


def _resolve_page_limit(
    *,
    batch_size: int | None,
    limit: int | None,
    yielded_count: int,
) -> tuple[bool, int | None]:
    page_limit = batch_size
    if limit is None:
        return False, page_limit

    remaining = limit - yielded_count
    if remaining <= 0:
        return True, None
    if page_limit is None or page_limit > remaining:
        page_limit = remaining
    return False, page_limit


def _get_next_cursor(page: ListBlobResultType) -> str | None:
    if not page.has_more or not page.cursor:
        return None
    return page.cursor


def _build_cache_bypass_url(blob_url: str) -> str:
    parsed = urlparse(blob_url)
    params = parse_qs(parsed.query)
    params["cache"] = ["0"]
    query = urlencode(params, doseq=True)
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            query,
            parsed.fragment,
        )
    )


def parse_last_modified(value: str | None) -> datetime:
    if not value:
        return datetime.now(tz=timezone.utc)
    try:
        return parsedate_to_datetime(value)
    except (ValueError, TypeError):
        pass
    try:
        return parse_datetime(value)
    except (ValueError, TypeError):
        return datetime.now(tz=timezone.utc)


class BlobRequestClient:
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

    @property
    def transport(self) -> BaseTransport:
        return self._transport

    @property
    def await_progress_callback(self) -> bool:
        return self._await_progress_callback

    def close(self) -> None:
        if isinstance(self._transport, SyncTransport):
            self._transport.close()

    async def aclose(self) -> None:
        if isinstance(self._transport, AsyncTransport):
            await self._transport.aclose()

    async def request_api(
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
        decode_mode: Literal["json", "any", "none"] = "json",
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
                    if decode_mode == "none":
                        return None
                    if decode_mode == "json":
                        return decode_blob_response_json(resp)
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


def create_sync_request_client(timeout: float = 30.0) -> BlobRequestClient:
    transport = SyncTransport(create_base_client(timeout=timeout))
    return BlobRequestClient(
        transport=transport,
        sleep_fn=_sync_sleep,
        await_progress_callback=False,
        async_content=False,
    )


def create_async_request_client(timeout: float = 30.0) -> BlobRequestClient:
    transport = AsyncTransport(create_base_async_client(timeout=timeout))
    return BlobRequestClient(
        transport=transport,
    )


class BaseBlobOpsClient:
    def __init__(
        self,
        *,
        request_client: BlobRequestClient,
        multipart_client: MultipartClient,
        multipart_runtime: Any,
    ) -> None:
        self._request_client = request_client
        self._multipart_client = multipart_client
        self._multipart_runtime = multipart_runtime

    def _stream_download_chunks(self, response: httpx.Response) -> AsyncIterator[bytes]:
        raise NotImplementedError

    async def _close_response(self, response: httpx.Response) -> None:
        raise NotImplementedError

    async def _close_download_response(self, response: httpx.Response) -> None:
        await self._close_response(response)

    def _make_upload_part_fn(self) -> Any:
        raise NotImplementedError

    async def _multipart_upload(
        self,
        path: str,
        body: Any,
        *,
        access: Access,
        content_type: str | None = None,
        add_random_suffix: bool = False,
        overwrite: bool = False,
        cache_control_max_age: int | None = None,
        token: str | None = None,
        on_upload_progress: BlobProgressCallback | None = None,
    ) -> dict[str, Any]:
        headers = prepare_upload_headers(
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
        )
        part_size = validate_part_size(DEFAULT_PART_SIZE)

        create_response = await self._multipart_client.create_multipart_upload(
            path,
            headers,
            token=token,
        )
        session = MultipartUploadSession(
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
                    upload_part_fn=self._make_upload_part_fn(),
                )
            ),
        )
        ordered_parts = order_uploaded_parts(parts)

        complete_response = await self._multipart_client.complete_multipart_upload(
            upload_id=session.upload_id,
            key=session.key,
            path=session.path,
            headers=session.headers,
            token=session.token,
            parts=ordered_parts,
        )
        return shape_complete_upload_result(complete_response)

    async def put_blob(
        self,
        path: str,
        body: Any,
        *,
        access: Access,
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
            access=access,
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
            result = build_put_blob_result(raw)
            track(
                "blob_put",
                token=token,
                access=access,
                content_type=content_type,
                multipart=True,
                size_bytes=get_telemetry_size_bytes(body),
            )
            return result, True

        raw = cast(
            dict[str, Any],
            await self._request_client.request_api(
                "",
                "PUT",
                token=token,
                headers=headers,
                params={"pathname": path},
                body=body,
                on_upload_progress=on_upload_progress,
            ),
        )
        result = build_put_blob_result(raw)
        track(
            "blob_put",
            token=token,
            access=access,
            content_type=content_type,
            multipart=False,
            size_bytes=get_telemetry_size_bytes(body),
        )
        return result, False

    async def delete_blob(
        self,
        urls: list[str],
        *,
        token: str,
    ) -> int:
        await self._request_client.request_api(
            "/delete",
            "POST",
            token=token,
            headers={"content-type": "application/json"},
            body={"urls": urls},
            decode_mode="none",
        )
        track("blob_delete", token=token, count=len(urls))
        return len(urls)

    async def head_blob(
        self,
        url_or_path: str,
        *,
        token: str | None,
    ) -> HeadBlobResultType:
        token = ensure_token(token)
        resp = cast(
            dict[str, Any],
            await self._request_client.request_api(
                "",
                "GET",
                token=token,
                params={"url": url_or_path},
            ),
        )
        return build_head_blob_result(resp)

    async def get_blob(
        self,
        url_or_path: str,
        *,
        access: Access,
        token: str | None,
        timeout: float | None,
        use_cache: bool,
        if_none_match: str | None,
        default_timeout: float,
    ) -> GetBlobResultType:
        token = ensure_token(token)
        validate_access(access)
        target_url = url_or_path
        pathname: str
        download_url: str | None = None
        if not is_url(target_url):
            pathname = target_url.lstrip("/")
            store_id = extract_store_id_from_token(token)
            if store_id:
                target_url = construct_blob_url(store_id, pathname, access)
            else:
                head_result = await self.head_blob(target_url, token=token)
                target_url = head_result.url
                pathname = head_result.pathname
                download_url = head_result.download_url
        else:
            pathname = urlparse(target_url).path.lstrip("/")
        if download_url is None:
            download_url = get_download_url(target_url)
        if not use_cache:
            target_url = _build_cache_bypass_url(target_url)

        effective_timeout = timeout or default_timeout
        headers: dict[str, str] = {}
        if access == "private":
            headers["authorization"] = f"Bearer {token}"
        if if_none_match:
            headers["if-none-match"] = if_none_match
        response: httpx.Response | None = None

        try:
            response = await self._request_client.transport.send(
                "GET",
                target_url,
                headers=headers,
                timeout=effective_timeout,
                follow_redirects=True,
            )
            if response.status_code == 404:
                raise BlobNotFoundError()
            if response.status_code == 304:
                return GetBlobResultType(
                    url=target_url,
                    download_url=download_url,
                    pathname=pathname,
                    content_type=None,
                    size=None,
                    content_disposition=response.headers.get("content-disposition", ""),
                    cache_control=response.headers.get("cache-control", ""),
                    uploaded_at=parse_last_modified(response.headers.get("last-modified")),
                    etag=response.headers.get("etag", ""),
                    content=b"",
                    status_code=304,
                )
            response.raise_for_status()
            content_length = response.headers.get("content-length")
            return GetBlobResultType(
                url=target_url,
                download_url=download_url,
                pathname=pathname,
                content_type=response.headers.get("content-type", "application/octet-stream"),
                size=int(content_length) if content_length else len(response.content),
                content_disposition=response.headers.get("content-disposition", ""),
                cache_control=response.headers.get("cache-control", ""),
                uploaded_at=parse_last_modified(response.headers.get("last-modified")),
                etag=response.headers.get("etag", ""),
                content=response.content,
                status_code=response.status_code,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                raise BlobNotFoundError() from exc
            raise
        finally:
            if response is not None:
                await self._close_response(response)

    async def copy_blob(
        self,
        src_path: str,
        dst_path: str,
        *,
        access: Access,
        content_type: str | None,
        add_random_suffix: bool,
        overwrite: bool,
        cache_control_max_age: int | None,
        token: str | None,
    ) -> PutBlobResultType:
        token = ensure_token(token)
        validate_path(dst_path)
        validate_access(access)

        src_url = src_path
        if not is_url(src_url):
            src_url = (await self.head_blob(src_url, token=token)).url

        headers = create_put_headers(
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            allow_overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
            access=access,
        )
        raw = cast(
            dict[str, Any],
            await self._request_client.request_api(
                "",
                "PUT",
                token=token,
                headers=headers,
                params={"pathname": str(dst_path), "fromUrl": src_url},
            ),
        )
        return build_put_blob_result(raw)

    async def create_folder(
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
            await self._request_client.request_api(
                "",
                "PUT",
                token=token,
                headers=headers,
                params={"pathname": folder_path},
            ),
        )
        return build_create_folder_result(raw)

    async def upload_file(
        self,
        local_path: str | os.PathLike,
        path: str,
        *,
        access: Access,
        content_type: str | None,
        add_random_suffix: bool,
        overwrite: bool,
        cache_control_max_age: int | None,
        token: str | None,
        multipart: bool,
        on_upload_progress: BlobProgressCallback | None,
        missing_local_path_error: str,
    ) -> PutBlobResultType:
        token = ensure_token(token)
        if not local_path:
            raise BlobError(missing_local_path_error)
        if not path:
            raise BlobError("path is required")

        source_path = os.fspath(local_path)
        if not os.path.exists(source_path):
            raise BlobError("local_path does not exist")
        if not os.path.isfile(source_path):
            raise BlobError("local_path is not a file")

        size_bytes = os.path.getsize(source_path)
        use_multipart = multipart or (size_bytes > 5 * 1024 * 1024)

        with open(source_path, "rb") as f:
            result, _ = await self.put_blob(
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
        return result

    async def download_file(
        self,
        url_or_path: str,
        local_path: str | os.PathLike,
        *,
        access: Access,
        token: str | None,
        timeout: float | None,
        overwrite: bool,
        create_parents: bool,
        progress: DownloadProgressCallback | None,
    ) -> str:
        token = ensure_token(token)
        validate_access(access)
        if is_url(url_or_path):
            target_url = get_download_url(url_or_path)
        elif store_id := extract_store_id_from_token(token):
            blob_url = construct_blob_url(store_id, url_or_path.lstrip("/"), access)
            target_url = get_download_url(blob_url)
        else:
            meta = await self.head_blob(url_or_path, token=token)
            target_url = meta.download_url or meta.url

        dst = os.fspath(local_path)
        if not overwrite and os.path.exists(dst):
            raise BlobError("destination exists; pass overwrite=True to replace it")
        if create_parents:
            os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)

        tmp = dst + ".part"
        bytes_read = 0
        effective_timeout = timeout or 120.0
        headers: dict[str, str] = {}
        if access == "private":
            headers["authorization"] = f"Bearer {token}"
        response: httpx.Response | None = None

        try:
            response = await self._request_client.transport.send(
                "GET",
                target_url,
                headers=headers,
                timeout=effective_timeout,
                follow_redirects=True,
                stream=True,
            )
            if response.status_code == 404:
                raise BlobNotFoundError()
            response.raise_for_status()
            total = int(response.headers.get("Content-Length", "0")) or None

            with open(tmp, "wb") as f:
                async for chunk in self._stream_download_chunks(response):
                    if not chunk:
                        continue
                    f.write(chunk)
                    bytes_read += len(chunk)
                    await _emit_download_progress(
                        progress,
                        bytes_read,
                        total,
                        await_callback=self._request_client.await_progress_callback,
                    )

            os.replace(tmp, dst)
        except Exception:
            if os.path.exists(tmp):
                os.remove(tmp)
            raise
        finally:
            if response is not None:
                await self._close_download_response(response)

        return dst


class SyncBlobOpsClient(BaseBlobOpsClient):
    def __init__(self, *, timeout: float = 30.0) -> None:
        request_client = create_sync_request_client(timeout)
        multipart_client = MultipartClient(request_client)
        super().__init__(
            request_client=request_client,
            multipart_client=multipart_client,
            multipart_runtime=create_sync_multipart_upload_runtime(),
        )

    def close(self) -> None:
        self._request_client.close()

    def _make_upload_part_fn(self) -> Any:
        return lambda **kw: iter_coroutine(self._multipart_client.upload_part(**kw))

    def list_objects(
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
            iter_coroutine(
                self._request_client.request_api(
                    "",
                    "GET",
                    token=token,
                    params=build_list_params(limit=limit, prefix=prefix, cursor=cursor, mode=mode),
                )
            ),
        )
        return build_list_blob_result(resp)

    def iter_objects(
        self,
        *,
        prefix: str | None,
        mode: str | None,
        token: str | None,
        batch_size: int | None,
        limit: int | None,
        cursor: str | None,
    ) -> Iterator[ListBlobItem]:
        next_cursor = cursor
        yielded_count = 0

        while True:
            done, effective_limit = _resolve_page_limit(
                batch_size=batch_size,
                limit=limit,
                yielded_count=yielded_count,
            )
            if done:
                break

            page = self.list_objects(
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

            next_cursor = _get_next_cursor(page)
            if next_cursor is None:
                break

    def _stream_download_chunks(self, response: httpx.Response) -> AsyncIterator[bytes]:
        async def _iterate() -> AsyncIterator[bytes]:
            for chunk in response.iter_bytes():
                yield chunk

        return _iterate()

    async def _close_download_response(self, response: httpx.Response) -> None:
        await self._close_response(response)

    async def _close_response(self, response: httpx.Response) -> None:
        response.close()

    def __enter__(self) -> SyncBlobOpsClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class AsyncBlobOpsClient(BaseBlobOpsClient):
    def __init__(self, *, timeout: float = 30.0) -> None:
        request_client = create_async_request_client(timeout)
        multipart_client = MultipartClient(request_client)
        super().__init__(
            request_client=request_client,
            multipart_client=multipart_client,
            multipart_runtime=create_async_multipart_upload_runtime(),
        )

    async def aclose(self) -> None:
        await self._request_client.aclose()

    async def __aenter__(self) -> AsyncBlobOpsClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    def _make_upload_part_fn(self) -> Any:
        return self._multipart_client.upload_part

    def _stream_download_chunks(self, response: httpx.Response) -> AsyncIterator[bytes]:
        async def _iterate() -> AsyncIterator[bytes]:
            async for chunk in response.aiter_bytes():
                yield chunk

        return _iterate()

    async def _close_download_response(self, response: httpx.Response) -> None:
        await self._close_response(response)

    async def _close_response(self, response: httpx.Response) -> None:
        await response.aclose()

    async def list_objects(
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
            await self._request_client.request_api(
                "",
                "GET",
                token=token,
                params=build_list_params(limit=limit, prefix=prefix, cursor=cursor, mode=mode),
            ),
        )
        return build_list_blob_result(resp)

    async def iter_objects(
        self,
        *,
        prefix: str | None,
        mode: str | None,
        token: str | None,
        batch_size: int | None,
        limit: int | None,
        cursor: str | None,
    ) -> AsyncIterator[ListBlobItem]:
        next_cursor = cursor
        yielded_count = 0

        while True:
            done, effective_limit = _resolve_page_limit(
                batch_size=batch_size,
                limit=limit,
                yielded_count=yielded_count,
            )
            if done:
                break

            page = await self.list_objects(
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

            next_cursor = _get_next_cursor(page)
            if next_cursor is None:
                break



__all__ = [
    "AsyncBlobOpsClient",
    "SyncBlobOpsClient",
    "BlobRequestClient",
    "create_sync_request_client",
    "create_async_request_client",
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
    "should_retry",
]
