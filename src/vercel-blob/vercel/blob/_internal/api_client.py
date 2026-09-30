"""Transport-agnostic client for the Vercel Blob API."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import httpx2 as httpx

from vercel._internal.core.http import (
    BaseTransport,
    JSONBody,
    RawBody,
    ReadResponsePolicy,
    RequestBody,
    StreamingResponse,
)
from vercel._internal.core.http._compat import http_errors
from vercel.blob.errors import BlobUnknownError
from vercel.blob.models import BlobCredentials, DownloadMetadata, HeadResult, PutResult

from .wire import (
    DeleteRequest,
    PutRequest,
    map_http_error,
    parse_head_response,
    parse_put_response,
    validate_and_parse_download_response,
)

_CHUNK_SIZE = 64 * 1024


class BlobApiClient:
    def __init__(self, *, base_url: str, transport: BaseTransport) -> None:
        self._base_url = base_url.rstrip("/")
        self._transport = transport

    @staticmethod
    def _headers(credentials: BlobCredentials | None) -> dict[str, str]:
        headers = {"x-api-version": "12"}
        if credentials is not None and credentials.kind == "oidc":
            headers["x-vercel-blob-store-id"] = credentials.store_id
        return headers

    async def _request(
        self,
        method: str,
        path: str,
        *,
        credentials: BlobCredentials,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        body: RequestBody = None,
    ) -> httpx.Response:
        try:
            response = await self._transport.send(
                method,
                f"{self._base_url}{path}",
                token=credentials.token,
                params=params,
                headers=self._headers(credentials) | dict(headers or {}),
                body=body,
                follow_redirects=False,
                read_response=ReadResponsePolicy.ALWAYS,
            )
        except http_errors() as exc:
            raise BlobUnknownError() from exc
        if not response.is_success:
            raise map_http_error(response)
        return response

    async def put(self, request: PutRequest, *, credentials: BlobCredentials) -> PutResult:
        response = await self._request(
            "PUT",
            "",
            credentials=credentials,
            params={"pathname": request.pathname},
            headers=request.to_headers(),
            body=RawBody(request.body),
        )
        return parse_put_response(response)

    async def get_metadata(self, url: str, *, credentials: BlobCredentials) -> HeadResult:
        response = await self._request("GET", "", credentials=credentials, params={"url": url})
        return parse_head_response(response)

    async def delete(self, urls: Sequence[str], *, credentials: BlobCredentials) -> None:
        request = DeleteRequest(urls=list(urls))
        await self._request(
            "POST", "/delete", credentials=credentials, body=JSONBody(request.model_dump())
        )

    async def open_download(
        self, url: str, *, credentials: BlobCredentials | None
    ) -> tuple[StreamingResponse, DownloadMetadata]:
        try:
            stream = await self._transport.open_response_stream(
                "GET",
                url,
                token=credentials.token if credentials is not None else None,
                headers=self._headers(credentials),
                follow_redirects=False,
                read_response=ReadResponsePolicy.NEVER,
                chunk_size=_CHUNK_SIZE,
            )
        except http_errors() as exc:
            raise BlobUnknownError() from exc
        try:
            metadata = validate_and_parse_download_response(url, stream.response)
        except BaseException:
            await stream.aclose()
            raise
        return stream, metadata
