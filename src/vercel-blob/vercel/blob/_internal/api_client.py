"""Transport-agnostic client, wire schemas, and codecs for the Vercel Blob API."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import datetime
from email.utils import parsedate_to_datetime
from functools import partial
from typing import Annotated, TypeVar

import httpx2 as httpx
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    ValidationError,
    field_serializer,
    field_validator,
    model_validator,
)

from vercel._internal.core.http import (
    BaseTransport,
    JSONBody,
    RawBody,
    ReadResponsePolicy,
    RequestBody,
    StreamingResponse,
)
from vercel._internal.core.http._compat import http_errors
from vercel.blob.errors import (
    BlobAccessError,
    BlobContentTypeNotAllowedError,
    BlobError,
    BlobFileTooLargeError,
    BlobNotFoundError,
    BlobPathnameMismatchError,
    BlobPreconditionFailedError,
    BlobServiceNotAvailable,
    BlobServiceRateLimited,
    BlobStoreNotFoundError,
    BlobStoreSuspendedError,
    BlobStreamError,
    BlobUnknownError,
)
from vercel.blob.models import Access, BlobCredentials, DownloadMetadata, HeadResult, PutResult

from .validation import (
    validate_access,
    validate_bool_flag,
    validate_cache_control_max_age,
    validate_content_type,
    validate_put_body,
    validate_upload_pathname,
)


class _ApiModel(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True, strict=True)


class _PutRequest(_ApiModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    pathname: Annotated[str, BeforeValidator(validate_upload_pathname)]
    body: Annotated[bytes, BeforeValidator(validate_put_body)]
    access: Annotated[Access, BeforeValidator(validate_access)] = Field(
        serialization_alias="x-vercel-blob-access"
    )
    add_random_suffix: Annotated[
        bool, BeforeValidator(partial(validate_bool_flag, name="add_random_suffix"))
    ] = Field(default=False, serialization_alias="x-add-random-suffix")
    allow_overwrite: Annotated[
        bool, BeforeValidator(partial(validate_bool_flag, name="allow_overwrite"))
    ] = Field(default=False, serialization_alias="x-allow-overwrite")
    content_type: Annotated[str | None, BeforeValidator(validate_content_type)] = Field(
        default=None, serialization_alias="x-content-type"
    )
    cache_control_max_age: Annotated[
        int | None, BeforeValidator(validate_cache_control_max_age)
    ] = Field(default=None, serialization_alias="x-cache-control-max-age")

    @field_serializer("add_random_suffix", "allow_overwrite")
    def _serialize_bool(self, value: bool) -> str:
        return "1" if value else "0"

    @field_serializer("cache_control_max_age")
    def _serialize_cache_age(self, value: int | None) -> str | None:
        return str(value) if value is not None else None

    def _to_headers(self) -> dict[str, str]:
        return {
            name: str(value)
            for name, value in self.model_dump(
                by_alias=True, exclude={"pathname", "body"}, exclude_none=True
            ).items()
        }


class _DeleteRequest(_ApiModel):
    urls: list[str]


class _PutResponse(_ApiModel):
    url: str
    download_url: str = Field(alias="downloadUrl")
    pathname: str
    content_type: str = Field(alias="contentType")
    content_disposition: str = Field(alias="contentDisposition")
    etag: str

    def _to_result(self) -> PutResult:
        return PutResult(
            url=self.url,
            download_url=self.download_url,
            pathname=self.pathname,
            content_type=self.content_type,
            content_disposition=self.content_disposition,
            etag=self.etag,
        )


class _HeadResponse(_ApiModel):
    url: str
    download_url: str = Field(alias="downloadUrl")
    pathname: str
    size: int = Field(ge=0)
    etag: str
    uploaded_at: datetime = Field(alias="uploadedAt")
    content_type: str | None = Field(default=None, alias="contentType")
    content_disposition: str = Field(default="", alias="contentDisposition")
    cache_control: str = Field(default="", alias="cacheControl")

    @field_validator("uploaded_at", mode="before")
    @classmethod
    def _parse_uploaded_at(cls, value: object) -> datetime:
        if not isinstance(value, str):
            raise ValueError("uploadedAt must be a string")
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def _to_result(self) -> HeadResult:
        return HeadResult(
            url=self.url,
            download_url=self.download_url,
            pathname=self.pathname,
            size=self.size,
            etag=self.etag,
            uploaded_at=self.uploaded_at,
            content_type=self.content_type,
            content_disposition=self.content_disposition,
            cache_control=self.cache_control,
        )


def _parse_http_date(value: str) -> datetime:
    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("invalid HTTP date") from exc


class _DownloadHeaders(_ApiModel):
    size: Annotated[int, BeforeValidator(int), Field(ge=0)] | None = Field(
        default=None, alias="content-length"
    )
    content_type: str | None = Field(default=None, alias="content-type")
    content_disposition: str | None = Field(default=None, alias="content-disposition")
    cache_control: str | None = Field(default=None, alias="cache-control")
    etag: str | None = None
    last_modified: Annotated[datetime, BeforeValidator(_parse_http_date)] | None = Field(
        default=None, alias="last-modified"
    )

    def _to_metadata(
        self, url: str, status_code: int, headers: Mapping[str, str]
    ) -> DownloadMetadata:
        return DownloadMetadata(
            url=url,
            status_code=status_code,
            size=self.size,
            content_type=self.content_type,
            content_disposition=self.content_disposition,
            cache_control=self.cache_control,
            etag=self.etag,
            last_modified=self.last_modified,
            headers=headers,
        )


class _ApiError(_ApiModel):
    code: str | None = None
    message: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _unwrap_error(cls, value: object) -> object:
        return value.get("error", value) if isinstance(value, dict) else value

    @field_validator("code", "message", mode="before")
    @classmethod
    def _optional_string(cls, value: object) -> str | None:
        return value if isinstance(value, str) else None


_ResponseT = TypeVar("_ResponseT", bound=_ApiModel)


def _parse_response(
    response: httpx.Response, model: type[_ResponseT], operation: str
) -> _ResponseT:
    try:
        data = response.json()
    except Exception as exc:
        raise BlobStreamError(f"Blob {operation} response is not valid JSON") from exc
    try:
        return model.model_validate(data)
    except ValidationError as exc:
        error = exc.errors()[0]
        field = error["loc"][0] if error["loc"] else None
        if field is None:
            message = f"Blob {operation} response must be a JSON object"
        elif field == "size":
            message = "Blob HEAD response has invalid size field"
        elif field == "uploadedAt":
            if error["type"] == "missing" or not isinstance(error["input"], str):
                message = "Blob HEAD response 'uploadedAt' must be a string"
            else:
                message = "Invalid uploadedAt date"
        elif field == "contentType" and operation == "HEAD":
            message = "Blob HEAD response 'contentType' must be a string or null"
        else:
            message = f"Blob {operation} response '{field}' must be a string"
        raise BlobStreamError(message) from exc


def _parse_put_response(response: httpx.Response) -> PutResult:
    return _parse_response(response, _PutResponse, "PUT")._to_result()


def _parse_head_response(response: httpx.Response) -> HeadResult:
    return _parse_response(response, _HeadResponse, "HEAD")._to_result()


def _validate_and_parse_download_response(url: str, response: httpx.Response) -> DownloadMetadata:
    if not response.is_success:
        raise _map_http_error(response)
    headers = dict(response.headers)
    try:
        parsed = _DownloadHeaders.model_validate(headers)
    except ValidationError as exc:
        field = exc.errors()[0]["loc"][0]
        raise BlobStreamError(
            f"Blob GET response has invalid {field} header: {headers[str(field)]!r}"
        ) from exc
    return parsed._to_metadata(url, response.status_code, headers)


def _parse_retry_after(value: str) -> int | None:
    try:
        return max(0, int(value))
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(value)
        now = datetime.now(retry_at.tzinfo)
        return max(0, math.ceil((retry_at - now).total_seconds()))
    except (TypeError, ValueError, OverflowError):
        return None


def _map_http_error(response: httpx.Response) -> Exception:
    code: str | None = None
    message = f"Blob request failed with HTTP {response.status_code}"
    try:
        error = _ApiError.model_validate(response.json())
        code = error.code
        if error.message is not None:
            message = error.message
    except Exception:
        pass

    if "contentType" in message and "is not allowed" in message:
        code = "content_type_not_allowed"
    if '"pathname"' in message and "does not match the token payload" in message:
        code = "client_token_pathname_mismatch"
    if "the file length cannot be greater than" in message:
        code = "file_too_large"

    if code == "store_not_found":
        return BlobStoreNotFoundError(message, status_code=response.status_code, code=code)
    if code == "store_suspended":
        return BlobStoreSuspendedError(message, status_code=response.status_code, code=code)
    if code in ("blob_not_found", "not_found"):
        return BlobNotFoundError(message, status_code=response.status_code, code=code)
    if code == "forbidden":
        return BlobAccessError(message, status_code=response.status_code, code=code)
    if code == "content_type_not_allowed":
        return BlobContentTypeNotAllowedError(message, status_code=response.status_code, code=code)
    if code == "client_token_pathname_mismatch":
        return BlobPathnameMismatchError(message, status_code=response.status_code, code=code)
    if code == "file_too_large":
        return BlobFileTooLargeError(message, status_code=response.status_code, code=code)
    if code == "precondition_failed":
        return BlobPreconditionFailedError(message, status_code=response.status_code, code=code)
    if code == "service_unavailable":
        return BlobServiceNotAvailable(message, status_code=response.status_code, code=code)
    if code == "rate_limited":
        seconds = _parse_retry_after(response.headers.get("retry-after", ""))
        return BlobServiceRateLimited(seconds, status_code=response.status_code, code=code)

    if response.status_code == 404:
        return BlobNotFoundError(message, status_code=404, code="blob_not_found")
    if response.status_code in (401, 403):
        return BlobAccessError(message, status_code=response.status_code, code="forbidden")
    if response.status_code == 412:
        return BlobPreconditionFailedError(message, status_code=412, code="precondition_failed")
    if response.status_code == 429:
        seconds = _parse_retry_after(response.headers.get("retry-after", ""))
        return BlobServiceRateLimited(seconds, status_code=429, code="rate_limited")
    if response.status_code == 503:
        return BlobServiceNotAvailable(message, status_code=503, code="service_unavailable")
    if response.status_code == 400:
        return BlobError(message, status_code=400, code="bad_request")
    return BlobUnknownError(message, status_code=response.status_code, code=code)


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
            raise _map_http_error(response)
        return response

    async def put(self, request: _PutRequest, *, credentials: BlobCredentials) -> PutResult:
        response = await self._request(
            "PUT",
            "",
            credentials=credentials,
            params={"pathname": request.pathname},
            headers=request._to_headers(),
            body=RawBody(request.body),
        )
        return _parse_put_response(response)

    async def get_metadata(self, url: str, *, credentials: BlobCredentials) -> HeadResult:
        response = await self._request("GET", "", credentials=credentials, params={"url": url})
        return _parse_head_response(response)

    async def delete(self, urls: Sequence[str], *, credentials: BlobCredentials) -> None:
        request = _DeleteRequest(urls=list(urls))
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
            metadata = _validate_and_parse_download_response(url, stream.response)
        except BaseException:
            await stream.aclose()
            raise
        return stream, metadata
