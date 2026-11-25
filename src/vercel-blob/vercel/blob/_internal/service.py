"""Session-scoped Blob domain operations."""

from __future__ import annotations

import inspect
import time
from collections.abc import Callable, Sequence
from datetime import timedelta

from vercel._internal.core.http import StreamingResponse
from vercel._internal.core.session import SdkSession, SyncSdkSession
from vercel._internal.core.time import parse_duration_seconds, to_ms_int
from vercel.blob.errors import BlobContentLengthError, BlobCredentialsError, BlobError
from vercel.blob.models import (
    Access,
    BlobCredentials,
    BlobCredentialsFactory,
    BlobPrefix,
    BlobSignedToken,
    CopyResult,
    DownloadMetadata,
    HeadResult,
    ListBlob,
    PresignOperation,
    PutResult,
)

from .api_client import (
    BlobApiClient,
    _CopyRequest,
    _FoldedListRequest,
    _ListRequest,
    _PutRequest,
    _SignedTokenRequest,
)
from .credentials import adapt_sync_credentials_factory, normalize_credentials
from .options import BlobServiceOptions, SyncBlobServiceOptions
from .pagination import QueryPage
from .presign import (
    PresignOptions,
    presign_url,
    validate_content_types,
    validate_maximum_size,
    validate_operations,
)
from .upload import (
    AnyPutBody,
    AsyncUploadRuntime,
    SyncUploadRuntime,
    UploadRuntime,
    _BufferedUpload,
)
from .validation import (
    construct_delivery_url,
    is_url,
    parse_and_validate_delivery_url,
    validate_access,
    validate_if_match,
    validate_pathname,
    validate_url_or_pathname,
)


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _expires_at_ms(expires_in: timedelta | float | None, now_ms: int) -> int | None:
    duration = parse_duration_seconds(expires_in)
    if duration is None:
        return None
    if duration <= timedelta(0):
        raise BlobError("expires_in must be positive")
    return now_ms + to_ms_int(duration)


class BlobService:
    def __init__(
        self,
        *,
        api_client: BlobApiClient,
        credentials_factory: BlobCredentialsFactory,
        check_open: Callable[[], None],
        upload_runtime: UploadRuntime,
    ) -> None:
        self._api_client = api_client
        self.credentials_factory = credentials_factory
        self.check_open = check_open
        self._upload_runtime = upload_runtime
        self._store_id: str | None = None

    async def _get_credentials(self) -> BlobCredentials:
        res = self.credentials_factory()
        if inspect.isawaitable(res):
            creds = await res
        else:
            creds = res
        normalized = normalize_credentials(creds)
        if self._store_id is None:
            self._store_id = normalized.store_id
        elif self._store_id != normalized.store_id:
            raise BlobCredentialsError("Blob credential factory changed store ID across requests")
        return normalized

    async def put(
        self,
        pathname: str,
        body: AnyPutBody,
        *,
        access: Access,
        content_length: int | None = None,
        content_type: str | None = None,
        add_random_suffix: bool = False,
        allow_overwrite: bool = False,
        cache_control_max_age: int | None = None,
    ) -> PutResult:
        self.check_open()
        request = _PutRequest(
            pathname=pathname,
            access=access,
            add_random_suffix=add_random_suffix,
            allow_overwrite=allow_overwrite,
            content_type=content_type,
            cache_control_max_age=cache_control_max_age,
        )
        upload = self._upload_runtime.classify(body, content_length=content_length)

        if upload.content_length == 0:
            if not isinstance(upload, _BufferedUpload):
                source = self._upload_runtime.chunk_source(upload)
                first = await source.next_chunk(1)
                if first:
                    raise BlobContentLengthError("body exceeded content_length of 0 bytes")
                upload = _BufferedUpload(b"", 0)

        credentials = await self._get_credentials()
        if isinstance(upload, _BufferedUpload):
            return await self._api_client.put(request, upload.body, credentials=credentials)
        source = self._upload_runtime.chunk_source(upload)
        return await self._api_client.put_stream(
            request, source, upload.content_length, credentials=credentials
        )

    async def head(self, url_or_pathname: str) -> HeadResult:
        self.check_open()
        target = validate_url_or_pathname(url_or_pathname)
        credentials = await self._get_credentials()
        return await self._api_client.get_metadata(target, credentials=credentials)

    async def query_blobs_page(
        self,
        *,
        page_size: int | None = None,
        prefix: str | None = None,
        cursor: str | None = None,
    ) -> QueryPage[ListBlob]:
        self.check_open()
        request = _ListRequest(page_size=page_size, prefix=prefix, cursor=cursor)
        credentials = await self._get_credentials()
        return await self._api_client.query_blobs_page(request, credentials=credentials)

    async def query_blob_entries_page(
        self,
        *,
        page_size: int | None = None,
        prefix: str | None = None,
        cursor: str | None = None,
    ) -> QueryPage[ListBlob | BlobPrefix]:
        self.check_open()
        request = _FoldedListRequest(page_size=page_size, prefix=prefix, cursor=cursor)
        credentials = await self._get_credentials()
        return await self._api_client.query_blob_entries_page(request, credentials=credentials)

    async def copy(
        self,
        source: str,
        pathname: str,
        *,
        access: Access,
        content_type: str | None = None,
        add_random_suffix: bool = False,
        allow_overwrite: bool = False,
        cache_control_max_age: int | None = None,
        if_match: str | None = None,
    ) -> CopyResult:
        self.check_open()
        target = validate_url_or_pathname(source)
        request = _CopyRequest(
            pathname=pathname,
            access=access,
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            allow_overwrite=allow_overwrite,
            cache_control_max_age=cache_control_max_age,
            if_match=if_match,
        )
        credentials = await self._get_credentials()
        return await self._api_client.copy(target, request, credentials=credentials)

    async def delete(
        self, url_or_pathname: str | Sequence[str], *, if_match: str | None = None
    ) -> None:
        self.check_open()
        if isinstance(url_or_pathname, str):
            targets = [validate_url_or_pathname(url_or_pathname)]
        elif isinstance(url_or_pathname, Sequence) and not isinstance(
            url_or_pathname, (bytes, bytearray)
        ):
            targets = [validate_url_or_pathname(target) for target in url_or_pathname]
        else:
            raise BlobError("delete requires a pathname, URL, or sequence of them")
        if not 1 <= len(targets) <= 1000:
            raise BlobError("delete requires between 1 and 1000 targets")
        condition = validate_if_match(if_match)
        if condition is not None and len(targets) != 1:
            raise BlobError("if_match can only be used when deleting a single object")
        credentials = await self._get_credentials()
        await self._api_client.delete(targets, credentials=credentials, if_match=condition)

    async def issue_signed_token(
        self,
        *,
        pathname: str | None = None,
        operations: Sequence[PresignOperation] | None = None,
        expires_in: timedelta | float | None = None,
        maximum_size_in_bytes: int | None = None,
        allowed_content_types: Sequence[str] | None = None,
    ) -> BlobSignedToken:
        self.check_open()
        request = _SignedTokenRequest(
            pathname=validate_pathname(pathname) if pathname not in (None, "*") else pathname,
            operations=validate_operations(operations),
            valid_until=_expires_at_ms(expires_in, _now_ms()),
            maximum_size_in_bytes=validate_maximum_size(maximum_size_in_bytes),
            allowed_content_types=validate_content_types(allowed_content_types),
        )
        credentials = await self._get_credentials()
        return await self._api_client.issue_signed_token(request, credentials=credentials)

    def presign_url(
        self,
        signed_token: BlobSignedToken,
        pathname: str,
        *,
        operation: PresignOperation,
        access: Access | None,
        expires_in: timedelta | float | None,
        options: PresignOptions,
    ) -> str:
        self.check_open()
        now_ms = _now_ms()
        return presign_url(
            signed_token,
            pathname,
            operation=operation,
            access=access,
            expires_at_ms=_expires_at_ms(expires_in, now_ms),
            options=options,
            api_url=self._api_client.base_url,
            now_ms=now_ms,
        )

    async def open_download(
        self, url_or_pathname: str, *, access: Access
    ) -> tuple[StreamingResponse, DownloadMetadata]:
        self.check_open()
        clean_access = validate_access(access)
        credentials: BlobCredentials | None = None
        if is_url(url_or_pathname):
            target_url, url_store_id, _, _ = parse_and_validate_delivery_url(
                url_or_pathname, access=clean_access
            )
            if clean_access == "private":
                credentials = await self._get_credentials()
                if url_store_id.lower() != credentials.store_id.lower():
                    msg = f"URL store ID '{url_store_id}' does not match credential store ID"
                    raise BlobError(f"{msg} '{credentials.store_id}'")
        else:
            clean_path = validate_pathname(url_or_pathname)
            resolved = await self._get_credentials()
            target_url = construct_delivery_url(resolved.store_id, clean_path, clean_access)
            if clean_access == "private":
                credentials = resolved
        return await self._api_client.open_download(target_url, credentials=credentials)


def get_blob_service(session: SdkSession) -> BlobService:
    def factory() -> BlobService:
        options = session.get_service_option(BlobServiceOptions) or BlobServiceOptions()
        return BlobService(
            api_client=BlobApiClient(base_url=options.base_url, transport=session.get_transport()),
            credentials_factory=options.credentials_factory,
            check_open=session.check_open,
            upload_runtime=AsyncUploadRuntime(session.get_staging_file_runtime()),
        )

    return session.get_or_create_service(BlobService, factory)


def get_sync_blob_service(session: SyncSdkSession) -> BlobService:
    def factory() -> BlobService:
        options = session.get_service_option(SyncBlobServiceOptions) or SyncBlobServiceOptions()
        return BlobService(
            api_client=BlobApiClient(base_url=options.base_url, transport=session.get_transport()),
            credentials_factory=adapt_sync_credentials_factory(options.credentials_factory),
            check_open=session.check_open,
            upload_runtime=SyncUploadRuntime(session.get_staging_file_runtime()),
        )

    return session.get_or_create_service(BlobService, factory)
