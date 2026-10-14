"""Session-scoped Blob domain operations."""

from __future__ import annotations

import inspect
from collections.abc import Callable

from vercel._internal.core.http import StreamingResponse
from vercel._internal.core.session import SdkSession, SyncSdkSession
from vercel.blob.errors import BlobCredentialsError, BlobError
from vercel.blob.models import (
    Access,
    BlobCredentials,
    BlobCredentialsFactory,
    DownloadMetadata,
    HeadResult,
    PutResult,
)

from .api_client import BlobApiClient, _PutRequest
from .credentials import adapt_sync_credentials_factory, normalize_credentials
from .options import BlobServiceOptions, SyncBlobServiceOptions
from .validation import (
    construct_delivery_url,
    is_url,
    parse_and_validate_delivery_url,
    validate_access,
    validate_pathname,
    validate_url_or_pathname,
)


class BlobService:
    def __init__(
        self,
        *,
        api_client: BlobApiClient,
        credentials_factory: BlobCredentialsFactory,
        check_open: Callable[[], None],
    ) -> None:
        self._api_client = api_client
        self.credentials_factory = credentials_factory
        self.check_open = check_open
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
        body: bytes,
        *,
        access: Access,
        content_type: str | None = None,
        add_random_suffix: bool = False,
        allow_overwrite: bool = False,
        cache_control_max_age: int | None = None,
    ) -> PutResult:
        self.check_open()
        request = _PutRequest(
            pathname=pathname,
            body=body,
            access=access,
            add_random_suffix=add_random_suffix,
            allow_overwrite=allow_overwrite,
            content_type=content_type,
            cache_control_max_age=cache_control_max_age,
        )
        credentials = await self._get_credentials()
        return await self._api_client.put(request, credentials=credentials)

    async def head(self, url_or_pathname: str) -> HeadResult:
        self.check_open()
        target = validate_url_or_pathname(url_or_pathname)
        credentials = await self._get_credentials()
        return await self._api_client.get_metadata(target, credentials=credentials)

    async def delete(self, url_or_pathname: str) -> None:
        self.check_open()
        target = validate_url_or_pathname(url_or_pathname)
        credentials = await self._get_credentials()
        await self._api_client.delete([target], credentials=credentials)

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
        )

    return session.get_or_create_service(BlobService, factory)


def get_sync_blob_service(session: SyncSdkSession) -> BlobService:
    def factory() -> BlobService:
        options = session.get_service_option(SyncBlobServiceOptions) or SyncBlobServiceOptions()
        return BlobService(
            api_client=BlobApiClient(base_url=options.base_url, transport=session.get_transport()),
            credentials_factory=adapt_sync_credentials_factory(options.credentials_factory),
            check_open=session.check_open,
        )

    return session.get_or_create_service(BlobService, factory)
