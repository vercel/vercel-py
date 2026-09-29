"""Session-scoped Blob service implementation."""

from __future__ import annotations

import inspect
from collections.abc import Callable

from vercel._internal.core.http import (
    BaseTransport,
    JSONBody,
    RawBody,
    ReadResponsePolicy,
    StreamingResponse,
)
from vercel._internal.core.http._compat import http_errors
from vercel._internal.core.session import SdkSession, SyncSdkSession
from vercel.blob.errors import BlobCredentialsError, BlobError, BlobUnknownError
from vercel.blob.models import (
    Access,
    BlobCredentials,
    BlobCredentialsFactory,
    HeadResult,
    PutResult,
)

from .credentials import adapt_sync_credentials_factory, normalize_credentials
from .options import BlobServiceOptions, SyncBlobServiceOptions
from .validation import (
    construct_delivery_url,
    is_url,
    parse_and_validate_delivery_url,
    validate_access,
    validate_bool_flag,
    validate_cache_control_max_age,
    validate_content_type,
    validate_pathname,
    validate_put_body,
    validate_url_or_pathname,
)
from .wire import map_http_error, parse_head_response, parse_put_response

_CHUNK_SIZE = 64 * 1024


class BlobService:
    """Session-scoped service managing Blob operations over BaseTransport."""

    def __init__(
        self,
        *,
        base_url: str,
        transport: BaseTransport,
        credentials_factory: BlobCredentialsFactory,
        check_open: Callable[[], None],
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.transport = transport
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
        add_random_suffix: bool = True,
        allow_overwrite: bool = False,
        cache_control_max_age: int | None = None,
    ) -> PutResult:
        self.check_open()
        clean_path = validate_pathname(pathname)
        clean_body = validate_put_body(body)
        clean_access = validate_access(access)
        add_random_suffix = validate_bool_flag("add_random_suffix", add_random_suffix)
        allow_overwrite = validate_bool_flag("allow_overwrite", allow_overwrite)
        clean_content_type = validate_content_type(content_type)
        clean_cache_age = validate_cache_control_max_age(cache_control_max_age)

        credentials = await self._get_credentials()

        headers: dict[str, str] = {
            "x-api-version": "12",
            "x-vercel-blob-access": clean_access,
            "x-add-random-suffix": "1" if add_random_suffix else "0",
            "x-allow-overwrite": "1" if allow_overwrite else "0",
        }
        if clean_content_type is not None:
            headers["x-content-type"] = clean_content_type
        if clean_cache_age is not None:
            headers["x-cache-control-max-age"] = str(clean_cache_age)
        if credentials.kind == "oidc":
            headers["x-vercel-blob-store-id"] = credentials.store_id

        try:
            response = await self.transport.send(
                "PUT",
                self.base_url,
                token=credentials.token,
                params={"pathname": clean_path},
                headers=headers,
                body=RawBody(clean_body),
                follow_redirects=False,
                read_response=ReadResponsePolicy.ALWAYS,
            )
        except http_errors() as exc:
            raise BlobUnknownError() from exc

        if not response.is_success:
            raise map_http_error(response)

        return parse_put_response(response)

    async def head(
        self,
        url_or_pathname: str,
    ) -> HeadResult:
        self.check_open()
        target = validate_url_or_pathname(url_or_pathname)

        credentials = await self._get_credentials()
        headers: dict[str, str] = {"x-api-version": "12"}
        if credentials.kind == "oidc":
            headers["x-vercel-blob-store-id"] = credentials.store_id

        try:
            response = await self.transport.send(
                "GET",
                self.base_url,
                token=credentials.token,
                params={"url": target},
                headers=headers,
                follow_redirects=False,
                read_response=ReadResponsePolicy.ALWAYS,
            )
        except http_errors() as exc:
            raise BlobUnknownError() from exc

        if not response.is_success:
            raise map_http_error(response)

        return parse_head_response(response)

    async def delete(
        self,
        url_or_pathname: str,
    ) -> None:
        self.check_open()
        target = validate_url_or_pathname(url_or_pathname)

        credentials = await self._get_credentials()
        headers: dict[str, str] = {"x-api-version": "12"}
        if credentials.kind == "oidc":
            headers["x-vercel-blob-store-id"] = credentials.store_id

        try:
            response = await self.transport.send(
                "POST",
                f"{self.base_url}/delete",
                token=credentials.token,
                headers=headers,
                body=JSONBody({"urls": [target]}),
                follow_redirects=False,
                read_response=ReadResponsePolicy.ALWAYS,
            )
        except http_errors() as exc:
            raise BlobUnknownError() from exc

        if not response.is_success:
            raise map_http_error(response)

    async def open_download(
        self,
        url_or_pathname: str,
        *,
        access: Access,
    ) -> tuple[StreamingResponse, str]:
        self.check_open()
        clean_access = validate_access(access)

        target_url: str
        token: str | None = None
        store_header: str | None = None

        if is_url(url_or_pathname):
            clean_url, url_store_id, _, _ = parse_and_validate_delivery_url(
                url_or_pathname, access=clean_access
            )
            target_url = clean_url
            if clean_access == "private":
                credentials = await self._get_credentials()
                if url_store_id.lower() != credentials.store_id.lower():
                    msg = f"URL store ID '{url_store_id}' does not match credential store ID"
                    raise BlobError(f"{msg} '{credentials.store_id}'")
                token = credentials.token
                if credentials.kind == "oidc":
                    store_header = credentials.store_id
        else:
            clean_path = validate_pathname(url_or_pathname)
            credentials = await self._get_credentials()
            target_url = construct_delivery_url(credentials.store_id, clean_path, clean_access)
            if clean_access == "private":
                token = credentials.token
                if credentials.kind == "oidc":
                    store_header = credentials.store_id

        headers: dict[str, str] = {"x-api-version": "12"}
        if store_header:
            headers["x-vercel-blob-store-id"] = store_header

        try:
            stream = await self.transport.open_response_stream(
                "GET",
                target_url,
                token=token,
                headers=headers,
                follow_redirects=False,
                read_response=ReadResponsePolicy.NEVER,
                chunk_size=_CHUNK_SIZE,
            )
        except http_errors() as exc:
            raise BlobUnknownError() from exc

        return stream, target_url


def get_blob_service(session: SdkSession) -> BlobService:
    def factory() -> BlobService:
        options = session.get_service_option(BlobServiceOptions) or BlobServiceOptions()
        return BlobService(
            base_url=options.base_url,
            transport=session.get_transport(),
            credentials_factory=options.credentials_factory,
            check_open=session.check_open,
        )

    return session.get_or_create_service(BlobService, factory)


def get_sync_blob_service(session: SyncSdkSession) -> BlobService:
    def factory() -> BlobService:
        sync_options = (
            session.get_service_option(SyncBlobServiceOptions) or SyncBlobServiceOptions()
        )
        credentials_factory = adapt_sync_credentials_factory(sync_options.credentials_factory)
        return BlobService(
            base_url=sync_options.base_url,
            transport=session.get_transport(),
            credentials_factory=credentials_factory,
            check_open=session.check_open,
        )

    return session.get_or_create_service(BlobService, factory)
