"""Pydantic wire models, format codecs, and error mapping for Vercel Blob."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Annotated, TypeVar

import httpx2 as httpx
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StrictBool,
    StringConstraints,
    ValidationError,
    field_serializer,
    field_validator,
    model_validator,
)

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
from vercel.blob.models import Access, DownloadMetadata, HeadResult, PutResult

from .validation import validate_access, validate_pathname, validate_put_body


class _ApiModel(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True, strict=True)


class PutRequest(_ApiModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    pathname: Annotated[str, BeforeValidator(validate_pathname)]
    body: Annotated[bytes, BeforeValidator(validate_put_body)]
    access: Annotated[Access, BeforeValidator(validate_access)] = Field(
        serialization_alias="x-vercel-blob-access"
    )
    add_random_suffix: StrictBool = Field(default=True, serialization_alias="x-add-random-suffix")
    allow_overwrite: StrictBool = Field(default=False, serialization_alias="x-allow-overwrite")
    content_type: (
        Annotated[str, StringConstraints(min_length=1, pattern=r"^[\x20-\x7e]+$")] | None
    ) = Field(default=None, serialization_alias="x-content-type")
    cache_control_max_age: Annotated[int, Field(ge=0)] | None = Field(
        default=None, serialization_alias="x-cache-control-max-age"
    )

    @classmethod
    def from_input(cls, **values: object) -> PutRequest:
        try:
            return cls.model_validate(values)
        except ValidationError as exc:
            error = exc.errors()[0]
            field = error["loc"][0]
            if field in ("add_random_suffix", "allow_overwrite"):
                raise TypeError(
                    f"{field} must be bool, got {type(error['input']).__name__}"
                ) from exc
            if field == "cache_control_max_age":
                raise TypeError("cache_control_max_age must be an integer, not bool") from exc
            if field == "content_type":
                if error["type"] == "string_too_short":
                    message = "content_type cannot be empty"
                elif error["type"] == "string_type":
                    message = "content_type must be a string"
                elif isinstance(error["input"], str) and not error["input"].isascii():
                    message = "content_type must be ASCII"
                else:
                    message = "content_type cannot contain control characters"
                raise ValueError(message) from exc
            raise

    @field_serializer("add_random_suffix", "allow_overwrite")
    def _serialize_bool(self, value: bool) -> str:
        return "1" if value else "0"

    @field_serializer("cache_control_max_age")
    def _serialize_cache_age(self, value: int | None) -> str | None:
        return str(value) if value is not None else None

    def to_headers(self) -> dict[str, str]:
        return {
            name: str(value)
            for name, value in self.model_dump(
                by_alias=True, exclude={"pathname", "body"}, exclude_none=True
            ).items()
        }


class DeleteRequest(_ApiModel):
    urls: list[str]


class _PutResponse(_ApiModel):
    url: str
    download_url: str = Field(alias="downloadUrl")
    pathname: str
    content_type: str = Field(alias="contentType")
    content_disposition: str = Field(alias="contentDisposition")
    etag: str

    def to_result(self) -> PutResult:
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

    def to_result(self) -> HeadResult:
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

    def to_metadata(
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


def parse_put_response(response: httpx.Response) -> PutResult:
    return _parse_response(response, _PutResponse, "PUT").to_result()


def parse_head_response(response: httpx.Response) -> HeadResult:
    return _parse_response(response, _HeadResponse, "HEAD").to_result()


def validate_and_parse_download_response(url: str, response: httpx.Response) -> DownloadMetadata:
    if not response.is_success:
        raise map_http_error(response)
    headers = dict(response.headers)
    try:
        parsed = _DownloadHeaders.model_validate(headers)
    except ValidationError as exc:
        field = exc.errors()[0]["loc"][0]
        raise BlobStreamError(
            f"Blob GET response has invalid {field} header: {headers[str(field)]!r}"
        ) from exc
    return parsed.to_metadata(url, response.status_code, headers)


def parse_retry_after(value: str) -> int | None:
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


def map_http_error(response: httpx.Response) -> Exception:
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
        seconds = parse_retry_after(response.headers.get("retry-after", ""))
        return BlobServiceRateLimited(seconds, status_code=response.status_code, code=code)

    if response.status_code == 404:
        return BlobNotFoundError(message, status_code=404, code="blob_not_found")
    if response.status_code in (401, 403):
        return BlobAccessError(message, status_code=response.status_code, code="forbidden")
    if response.status_code == 412:
        return BlobPreconditionFailedError(message, status_code=412, code="precondition_failed")
    if response.status_code == 429:
        seconds = parse_retry_after(response.headers.get("retry-after", ""))
        return BlobServiceRateLimited(seconds, status_code=429, code="rate_limited")
    if response.status_code == 503:
        return BlobServiceNotAvailable(message, status_code=503, code="service_unavailable")
    if response.status_code == 400:
        return BlobError(message, status_code=400, code="bad_request")
    return BlobUnknownError(message, status_code=response.status_code, code=code)
