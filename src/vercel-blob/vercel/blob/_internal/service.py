"""Session-scoped Blob service."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta

from vercel._internal.core.byte_stream import StagingFileRuntime
from vercel._internal.core.session import SdkSession, SyncSdkSession
from vercel.blob.errors import BlobNotFoundError, BlobStreamError

from .api_client import BlobApiClient
from .models import (
    Access,
    BlobCredentials,
    BlobCredentialsFactory,
    BlobStatResult,
    SyncBlobCredentialsFactory,
)
from .options import BlobServiceOptions, SyncBlobServiceOptions


class BlobService:
    def __init__(
        self,
        *,
        api: BlobApiClient,
        options: BlobServiceOptions,
        ensure_open: Callable[[], None],
        staging_file_runtime: StagingFileRuntime,
    ) -> None:
        self.api = api
        self.options = options
        self.ensure_open = ensure_open
        self.staging_file_runtime = staging_file_runtime

    async def stat(self, pathname: str) -> BlobStatResult:
        self.ensure_open()
        return await self.api.stat(pathname)

    async def remove(self, pathname: str, *, missing_ok: bool) -> None:
        self.ensure_open()
        try:
            await self.api.remove(pathname)
        except BlobNotFoundError:
            if missing_ok:
                return
            raise

    async def read_all(self, pathname: str, *, access: Access) -> tuple[BlobStatResult, bytes]:
        self.ensure_open()
        stat = await self.api.stat(pathname)
        if stat.size == 0:
            return stat, b""
        body = bytearray()
        start = 0
        while start < stat.size:
            end = min(stat.size - 1, start + self.options.read_buffer_size - 1)
            chunk = await self.api.read_range(stat, access=access, start=start, end=end)
            if not chunk:
                raise BlobStreamError("Blob range request ended before the advertised size")
            body.extend(chunk)
            start += len(chunk)
        return stat, bytes(body[: stat.size])

    async def publish(
        self,
        pathname: str,
        body: bytes,
        *,
        access: Access,
        content_type: str | None,
        cache_control_max_age: timedelta | None,
    ) -> None:
        self.ensure_open()
        await self.api.put(
            pathname,
            body,
            access=access,
            content_type=content_type,
            cache_control_max_age=cache_control_max_age,
        )


def _adapt_sync_credentials_factory(
    factory: SyncBlobCredentialsFactory,
) -> BlobCredentialsFactory:
    async def credentials_factory() -> BlobCredentials:
        return factory()

    return credentials_factory


def get_blob_service(session: SdkSession) -> BlobService:
    def factory() -> BlobService:
        options = session.get_service_option(BlobServiceOptions) or BlobServiceOptions()
        return BlobService(
            api=BlobApiClient(
                base_url=options.base_url,
                transport=session.get_transport(),
                credentials=options.credentials_factory,
            ),
            options=options,
            ensure_open=session.check_open,
            staging_file_runtime=session.get_staging_file_runtime(),
        )

    return session.get_or_create_service(BlobService, factory)


def get_sync_blob_service(session: SyncSdkSession) -> BlobService:
    def factory() -> BlobService:
        sync_options = (
            session.get_service_option(SyncBlobServiceOptions) or SyncBlobServiceOptions()
        )
        credentials_factory = _adapt_sync_credentials_factory(sync_options.credentials_factory)
        options = BlobServiceOptions(
            base_url=sync_options.base_url,
            credentials_factory=credentials_factory,
            default_access=sync_options.default_access,
            read_buffer_size=sync_options.read_buffer_size,
        )
        return BlobService(
            api=BlobApiClient(
                base_url=options.base_url,
                transport=session.get_transport(),
                credentials=credentials_factory,
            ),
            options=options,
            ensure_open=session.check_open,
            staging_file_runtime=session.get_staging_file_runtime(),
        )

    return session.get_or_create_service(BlobService, factory)
