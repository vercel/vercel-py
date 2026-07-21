"""Wire-aware client for the experimental Blob service."""

import base64
import binascii
import hashlib
import hmac
import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, NoReturn, TypeVar, cast
from urllib.parse import quote, urlencode, urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from vercel._internal.blob.core import map_blob_error
from vercel._internal.blob.types import Access
from vercel._internal.byte_stream import ReadableByteStream
from vercel._internal.http import BaseTransport, JSONBody, ReadResponsePolicy
from vercel._internal.time import to_seconds_int
from vercel._internal.unstable.blob.errors import (
    BlobAlreadyExistsError,
    BlobCredentialsError,
    BlobError,
    BlobNotFoundError,
    BlobPreconditionFailedError,
    BlobStreamError,
    BlobUnknownError,
)
from vercel._internal.unstable.blob.headers import create_put_headers, get_api_version
from vercel._internal.unstable.blob.models import (
    BlobListItemState,
    BlobPageState,
    BlobPrefixState,
    BlobRangeResponse,
    BlobStatResult,
    MultipartPartState,
    MultipartUploadState,
    PresignedOperation,
    PresignedUrl,
    ScandirMode,
)
from vercel._internal.unstable.blob.options import (
    BlobCredentials,
    BlobCredentialsFactory,
    _AsyncBlobCredentialsResolver,
    _BlobCredentialsResolver,
    _normalize_blob_credentials,
)

_TRANSFER_CHUNK_SIZE = 64 * 1024
_MAX_DELEGATION_AGE = timedelta(days=7)
_MAX_PATHNAME_LENGTH = 950
_MAX_CONTENT_TYPES = 100
_MAX_CONTENT_TYPES_CSV_LENGTH = 16_384
_MAX_CACHE_CONTROL_AGE = timedelta(seconds=31_536_000)
_CONTENT_RANGE = re.compile(r"^bytes (\d+)-(\d+)/(\d+)$")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _milliseconds(value: datetime) -> int:
    if value.tzinfo is None:
        raise ValueError("expires_at must be timezone-aware")
    return int(value.timestamp() * 1000)


def _datetime_from_milliseconds(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, timezone.utc)


def _normalize_wire_datetime(value: datetime) -> datetime:
    return _datetime_from_milliseconds(_milliseconds(value))


def _valid_content_types(values: object, *, require_list: bool) -> bool:
    if require_list:
        if not isinstance(values, list):
            return False
    elif isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        return False
    return len(values) <= _MAX_CONTENT_TYPES and all(
        isinstance(value, str)
        and bool(value)
        and value.strip() == value
        and "," not in value
        and not any(ord(character) < 32 or ord(character) == 127 for character in value)
        for value in values
    )


class _ApiModel(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)


_ApiModelT = TypeVar("_ApiModelT", bound=_ApiModel)


def _has_control_character(value: str) -> bool:
    return any(ord(character) < 32 or ord(character) == 127 for character in value)


def _validate_wire_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("value must be an integer")
    return value


class _BlobStatPayload(_ApiModel):
    pathname: str
    url: str
    download_url: str = Field(validation_alias="downloadUrl")
    size: int
    etag: str
    uploaded_at: datetime = Field(validation_alias="uploadedAt")
    content_type: str | None = Field(default=None, validation_alias="contentType")
    content_disposition: str = Field(default="", validation_alias="contentDisposition")
    cache_control: str = Field(default="", validation_alias="cacheControl")

    @field_validator("pathname", "etag")
    @classmethod
    def _validate_required_string(cls, value: str) -> str:
        if not value or _has_control_character(value):
            raise ValueError("value must be a nonempty wire string")
        return value

    @field_validator("content_disposition", "cache_control")
    @classmethod
    def _validate_optional_header_string(cls, value: str) -> str:
        if _has_control_character(value):
            raise ValueError("value must be a wire string")
        return value

    @field_validator("url", "download_url")
    @classmethod
    def _validate_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("value must be an absolute HTTP URL")
        return value

    @field_validator("size", mode="before")
    @classmethod
    def _validate_size(cls, value: object) -> int:
        value = _validate_wire_int(value)
        if value < 0:
            raise ValueError("value must be nonnegative")
        return value

    @field_validator("uploaded_at")
    @classmethod
    def _validate_uploaded_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("value must include a timezone")
        return value

    def to_state(self) -> BlobStatResult:
        return BlobStatResult(
            pathname=self.pathname,
            url=self.url,
            download_url=self.download_url,
            size=self.size,
            etag=self.etag,
            uploaded_at=self.uploaded_at,
            content_type=self.content_type,
            content_disposition=self.content_disposition,
            cache_control=self.cache_control,
        )


class _BlobListItemPayload(_ApiModel):
    pathname: str
    url: str
    download_url: str = Field(validation_alias="downloadUrl")
    size: int
    etag: str
    uploaded_at: datetime = Field(validation_alias="uploadedAt")

    @field_validator("pathname", "etag")
    @classmethod
    def _validate_required_string(cls, value: str) -> str:
        return _BlobStatPayload._validate_required_string(value)

    @field_validator("url", "download_url")
    @classmethod
    def _validate_url(cls, value: str) -> str:
        return _BlobStatPayload._validate_url(value)

    @field_validator("size", mode="before")
    @classmethod
    def _validate_size(cls, value: object) -> int:
        return _BlobStatPayload._validate_size(value)

    @field_validator("uploaded_at")
    @classmethod
    def _validate_uploaded_at(cls, value: datetime) -> datetime:
        return _BlobStatPayload._validate_uploaded_at(value)

    def to_state(self) -> BlobListItemState:
        return BlobListItemState(
            pathname=self.pathname,
            url=self.url,
            download_url=self.download_url,
            size=self.size,
            etag=self.etag,
            uploaded_at=self.uploaded_at,
        )


class _BlobListPayload(_ApiModel):
    blobs: list[_BlobListItemPayload] = Field(default_factory=list)
    folders: list[str] = Field(default_factory=list)
    cursor: str | None = None
    has_more: bool = Field(default=False, validation_alias="hasMore")

    @field_validator("blobs", "folders", mode="before")
    @classmethod
    def _validate_list(cls, value: object) -> object:
        if not isinstance(value, list):
            raise ValueError("value must be a list")
        return value

    @field_validator("folders")
    @classmethod
    def _validate_folders(cls, values: list[str]) -> list[str]:
        for value in values:
            if not value or not value.endswith("/") or _has_control_character(value):
                raise ValueError("folders must contain valid strings")
        return values

    @field_validator("has_more", mode="before")
    @classmethod
    def _validate_has_more(cls, value: object) -> bool:
        if not isinstance(value, bool):
            raise ValueError("hasMore must be a boolean")
        return value


class _SignedTokenPayload(_ApiModel):
    delegation_token: str = Field(validation_alias="delegationToken")
    client_signing_token: str = Field(validation_alias="clientSigningToken")
    valid_until: int = Field(validation_alias="validUntil")

    @field_validator("delegation_token", "client_signing_token")
    @classmethod
    def _validate_token(cls, value: str) -> str:
        if not value:
            raise ValueError("token must be nonempty")
        return value

    @field_validator("valid_until", mode="before")
    @classmethod
    def _validate_valid_until(cls, value: object) -> int:
        value = _validate_wire_int(value)
        if value <= 0:
            raise ValueError("validUntil must be positive")
        return value


class _DelegationPayload(_ApiModel):
    store_id: str = Field(validation_alias="storeId")
    owner_id: str = Field(validation_alias="ownerId")
    pathname: str
    operations: list[str]
    valid_until: int = Field(validation_alias="validUntil")
    issued_at: int = Field(validation_alias="iat")
    maximum_size_in_bytes: int | None = Field(default=None, validation_alias="maximumSizeInBytes")
    allowed_content_types: list[str] | None = Field(
        default=None, validation_alias="allowedContentTypes"
    )

    @field_validator("store_id", "owner_id", "pathname")
    @classmethod
    def _validate_required_string(cls, value: str) -> str:
        if not value:
            raise ValueError("value must be nonempty")
        return value

    @field_validator("valid_until", "issued_at", mode="before")
    @classmethod
    def _validate_timestamp(cls, value: object) -> int:
        value = _validate_wire_int(value)
        if value <= 0:
            raise ValueError("timestamp must be positive")
        return value

    @field_validator("maximum_size_in_bytes", mode="before")
    @classmethod
    def _validate_maximum_size(cls, value: object) -> int | None:
        if value is None:
            return None
        value = _validate_wire_int(value)
        if value is not None and value <= 0:
            raise ValueError("maximumSizeInBytes must be positive")
        return value

    @field_validator("allowed_content_types")
    @classmethod
    def _validate_allowed_content_types(cls, value: list[str] | None) -> list[str] | None:
        if value is not None and not _valid_content_types(value, require_list=True):
            raise ValueError("allowedContentTypes is invalid")
        return value


def _headers(credentials: BlobCredentials) -> dict[str, str]:
    headers = {"x-api-version": get_api_version()}
    if credentials.kind == "oidc":
        headers["x-vercel-blob-store-id"] = credentials.store_id
    return headers


def _raise_control_error(response: httpx.Response, *, create_only: bool = False) -> NoReturn:
    try:
        error = response.json().get("error", {})
        code = error.get("code", "")
    except Exception:
        code = ""
    if create_only and (
        response.status_code in (409, 412) or code in {"already_exists", "blob_already_exists"}
    ):
        raise BlobAlreadyExistsError("Blob already exists")
    if code == "precondition_failed" or response.status_code == 412:
        raise BlobPreconditionFailedError("Blob ETag precondition failed")
    if code == "client_token_not_allowed":
        message = error.get("message")
        raise BlobError(
            message
            if isinstance(message, str) and message
            else "This operation is not available when using a client token"
        )
    _, mapped = map_blob_error(response)
    raise mapped


def _json(response: httpx.Response) -> dict[str, Any]:
    try:
        value = response.json()
    except Exception as exc:
        raise BlobStreamError("Blob API returned malformed JSON") from exc
    if not isinstance(value, dict):
        raise BlobStreamError("Blob API returned malformed JSON")
    return cast(dict[str, Any], value)


def _validate_response(
    model: type[_ApiModelT], data: Mapping[str, Any], message: str
) -> _ApiModelT:
    try:
        return model.model_validate(data)
    except ValidationError as exc:
        raise BlobStreamError(message) from exc


def _stat(data: Mapping[str, Any]) -> BlobStatResult:
    payload = _validate_response(_BlobStatPayload, data, "Blob API returned malformed metadata")
    return payload.to_state()


def _publication_etag(data: Mapping[str, Any], operation: str) -> str:
    value = data.get("etag")
    if not isinstance(value, str) or not value:
        raise BlobStreamError(f"Malformed {operation} response")
    return value


def _validate_size(size: int) -> None:
    if isinstance(size, bool) or size < 0:
        raise ValueError("size must not be negative")


def _validate_presign_pathname(pathname: str) -> None:
    if not pathname:
        raise ValueError("pathname must be non-empty")
    if len(pathname) > _MAX_PATHNAME_LENGTH:
        raise ValueError("pathname exceeds 950 characters")
    if (
        pathname == "*"
        or "*" in pathname
        or "?" in pathname
        or any(ord(character) < 32 or ord(character) == 127 for character in pathname)
    ):
        raise ValueError("pathname must identify a concrete object")


def _cache_control_max_age_seconds(value: timedelta | None) -> int | None:
    if value is None:
        return None

    is_valid_range = timedelta(seconds=0) <= value <= _MAX_CACHE_CONTROL_AGE
    if not is_valid_range:
        raise ValueError("cache_control_max_age is invalid")

    seconds = to_seconds_int(value)
    is_integer_seconds = seconds == value.total_seconds()
    if not is_integer_seconds:
        raise ValueError("cache_control_max_age is invalid")

    return seconds


def _validate_presign(
    operation: PresignedOperation,
    expires_at: datetime,
    *,
    maximum_size: int | None,
    allowed_content_types: Sequence[str] | None,
    allow_overwrite: bool | None,
    cache_control_max_age: timedelta | None,
    if_match: str | None,
) -> None:
    now = _now()
    if expires_at.tzinfo is None or expires_at <= now:
        raise ValueError("expires_at must be a future timezone-aware datetime")
    if expires_at - now > _MAX_DELEGATION_AGE:
        raise ValueError("expires_at cannot be more than seven days in the future")
    if operation in (PresignedOperation.GET, PresignedOperation.HEAD) and any(
        value is not None
        for value in (
            maximum_size,
            allowed_content_types,
            allow_overwrite,
            cache_control_max_age,
            if_match,
        )
    ):
        raise ValueError(f"{operation.value} presigning accepts only expiry")
    if operation is PresignedOperation.DELETE and any(
        value is not None
        for value in (
            maximum_size,
            allowed_content_types,
            allow_overwrite,
            cache_control_max_age,
        )
    ):
        raise ValueError("delete presigning accepts only expiry and if_match")
    if maximum_size is not None and (isinstance(maximum_size, bool) or maximum_size <= 0):
        raise ValueError("maximum_size must be a positive integer")
    if allowed_content_types is not None and not _valid_content_types(
        allowed_content_types, require_list=False
    ):
        raise ValueError("allowed_content_types is invalid")
    if allow_overwrite is not None and not isinstance(allow_overwrite, bool):
        raise ValueError("allow_overwrite must be a boolean")
    if (
        allowed_content_types is not None
        and len(",".join(sorted(allowed_content_types))) > _MAX_CONTENT_TYPES_CSV_LENGTH
    ):
        raise ValueError("allowed_content_types query value is too long")
    _cache_control_max_age_seconds(cache_control_max_age)
    if if_match is not None and (not 1 <= len(if_match) <= 256 or _has_control_character(if_match)):
        raise ValueError("if_match is invalid")
    if operation is PresignedOperation.PUT and if_match is not None and allow_overwrite is False:
        raise ValueError("if_match cannot be combined with allow_overwrite=False")


def _delegation_payload(token: str) -> _DelegationPayload:
    parts = token.split(".")
    if len(parts) != 2 or not all(parts):
        raise BlobStreamError("Malformed Blob delegation token")
    segment = parts[0]
    if re.fullmatch(r"[A-Za-z0-9_-]+", segment) is None or len(segment) % 4 == 1:
        raise BlobStreamError("Malformed Blob delegation token")
    try:
        padded = segment + "=" * (-len(segment) % 4)
        decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
        value = json.loads(decoded.decode("utf-8"))
    except (ValueError, binascii.Error, UnicodeError, json.JSONDecodeError) as exc:
        raise BlobStreamError("Malformed Blob delegation token") from exc
    if not isinstance(value, dict):
        raise BlobStreamError("Malformed Blob delegation token")
    try:
        return _DelegationPayload.model_validate(value)
    except ValidationError as exc:
        raise BlobCredentialsError("Signed-token delegation scope is invalid") from exc


def _assert_delegation_scope(
    delegation: _DelegationPayload,
    *,
    credentials: BlobCredentials,
    pathname: str,
    operation: PresignedOperation,
    signed_valid_until: int,
    delegated_expires_at: datetime,
    requested_expires_at: datetime,
) -> None:
    delegated_store = delegation.store_id.removeprefix("store_")
    if (
        not delegated_store
        or delegated_store != credentials.store_id
        or delegation.pathname != pathname
        or delegation.operations != [operation.value]
        or delegation.valid_until != signed_valid_until
        or delegated_expires_at <= _now()
        or delegated_expires_at > requested_expires_at
    ):
        raise BlobCredentialsError("Signed-token delegation scope is invalid")


def _assert_concrete_constraints_within_delegation(
    *,
    maximum_size: int | None,
    allowed_content_types: Sequence[str] | None,
    delegated_maximum: int | None,
    delegated_types: Sequence[str] | None,
) -> None:
    if (
        maximum_size is not None
        and delegated_maximum is not None
        and maximum_size > delegated_maximum
    ):
        raise BlobCredentialsError("Concrete size widens its delegation")
    if (
        allowed_content_types is not None
        and delegated_types is not None
        and any(
            concrete not in delegated_types
            and f"{concrete.partition('/')[0]}/*" not in delegated_types
            for concrete in allowed_content_types
        )
    ):
        raise BlobCredentialsError("Concrete content types widen their delegation")


def _presign_issuance_body(
    *,
    pathname: str,
    operation: PresignedOperation,
    requested_expires_at: datetime,
    maximum_size: int | None,
    allowed_content_types: Sequence[str] | None,
) -> dict[str, object]:
    issuance: dict[str, object] = {
        "pathname": pathname,
        "operations": [operation.value],
        "validUntil": _milliseconds(requested_expires_at),
    }
    if operation is PresignedOperation.PUT and maximum_size is not None:
        issuance["maximumSizeInBytes"] = maximum_size
    if operation is PresignedOperation.PUT and allowed_content_types is not None:
        issuance["allowedContentTypes"] = list(allowed_content_types)
    return issuance


def _validate_signed_delegation(
    signed_token: _SignedTokenPayload,
    *,
    credentials: BlobCredentials,
    pathname: str,
    operation: PresignedOperation,
    requested_expires_at: datetime,
    delegated_expires_at: datetime,
    maximum_size: int | None,
    allowed_content_types: Sequence[str] | None,
) -> _DelegationPayload:
    delegation = _delegation_payload(signed_token.delegation_token)
    _assert_delegation_scope(
        delegation,
        credentials=credentials,
        pathname=pathname,
        operation=operation,
        signed_valid_until=signed_token.valid_until,
        delegated_expires_at=delegated_expires_at,
        requested_expires_at=requested_expires_at,
    )
    _assert_concrete_constraints_within_delegation(
        maximum_size=maximum_size,
        allowed_content_types=allowed_content_types,
        delegated_maximum=delegation.maximum_size_in_bytes,
        delegated_types=delegation.allowed_content_types,
    )
    return delegation


def _presigned_constraints(
    *,
    operation: PresignedOperation,
    requested_expires_at: datetime,
    delegated_expires_at: datetime,
    maximum_size: int | None,
    allowed_content_types: Sequence[str] | None,
    allow_overwrite: bool | None,
    cache_control_max_age: timedelta | None,
    if_match: str | None,
) -> dict[str, str]:
    effective_expires_at = min(requested_expires_at, delegated_expires_at)
    cache_control_max_age_seconds = _cache_control_max_age_seconds(cache_control_max_age)
    constraints: dict[str, str] = {}
    if effective_expires_at != delegated_expires_at:
        constraints["vercel-blob-valid-until"] = str(_milliseconds(effective_expires_at))
    if maximum_size is not None:
        constraints["vercel-blob-maximum-size-in-bytes"] = str(maximum_size)
    if allowed_content_types is not None:
        allowed_content_types_csv = ",".join(sorted(allowed_content_types))
        if len(allowed_content_types_csv) > _MAX_CONTENT_TYPES_CSV_LENGTH:
            raise ValueError("allowed_content_types query value is too long")
        constraints["vercel-blob-allowed-content-types"] = allowed_content_types_csv
    effective_overwrite = allow_overwrite
    if operation is PresignedOperation.PUT and if_match is not None and allow_overwrite is None:
        effective_overwrite = True
    if effective_overwrite is not None:
        constraints["vercel-blob-allow-overwrite"] = str(effective_overwrite).lower()
    if cache_control_max_age_seconds is not None:
        constraints["vercel-blob-cache-control-max-age"] = str(cache_control_max_age_seconds)
    if if_match is not None:
        constraints["vercel-blob-if-match"] = if_match
    return constraints


def _canonical(
    *,
    operation: PresignedOperation,
    pathname: str,
    constraints: Mapping[str, str],
) -> str:
    lines = [f"operation={operation.value}", f"pathname={pathname}"]
    lines.extend(f"{key}={value}" for key, value in constraints.items() if value)
    return "\n".join(sorted(lines, key=lambda line: line.encode("utf-8")))


def _sign(
    signing_token: str,
    *,
    operation: PresignedOperation,
    pathname: str,
    constraints: Mapping[str, str],
) -> str:
    canonical = _canonical(operation=operation, pathname=pathname, constraints=constraints)
    return (
        base64.urlsafe_b64encode(
            hmac.new(signing_token.encode(), canonical.encode(), hashlib.sha256).digest()
        )
        .rstrip(b"=")
        .decode()
    )


def _presigned_url(
    *,
    base_url: str,
    credentials: BlobCredentials,
    access: Access,
    operation: PresignedOperation,
    pathname: str,
    constraints: Mapping[str, str],
    delegation: str,
    signing_token: str,
) -> str:
    signature = _sign(
        signing_token,
        operation=operation,
        pathname=pathname,
        constraints=constraints,
    )
    query = {
        **constraints,
        "vercel-blob-delegation": delegation,
        "vercel-blob-signature": signature,
    }
    if operation in (PresignedOperation.GET, PresignedOperation.HEAD):
        delivery_host = f"https://{credentials.store_id}.{access}.blob.vercel-storage.com"
        target = f"{delivery_host}/{quote(pathname, safe='/')}"
    else:
        target = f"{base_url}/?{urlencode({'pathname': pathname})}"
    separator = "&" if "?" in target else "?"
    return f"{target}{separator}{urlencode(query)}"


class BlobApiClient:
    """Borrowed-transport Blob API client bound to one credential store."""

    def __init__(
        self,
        *,
        base_url: str,
        transport: BaseTransport,
        credentials_factory: BlobCredentialsFactory | None = None,
        credentials_resolver: _BlobCredentialsResolver | None = None,
    ) -> None:
        if (credentials_factory is None) == (credentials_resolver is None):
            raise TypeError("provide exactly one Blob credential source")
        self._base_url = base_url.rstrip("/")
        self._credentials_resolver = (
            _AsyncBlobCredentialsResolver(credentials_factory)
            if credentials_factory is not None
            else cast(_BlobCredentialsResolver, credentials_resolver)
        )
        self._transport = transport
        self._store_id: str | None = None

    async def _credentials(self) -> BlobCredentials:
        credentials = _normalize_blob_credentials(await self._credentials_resolver.resolve())
        store_id = credentials.store_id
        if self._store_id is None:
            self._store_id = store_id
        elif store_id != self._store_id:
            raise BlobCredentialsError("Blob credential factory changed store ID")
        return BlobCredentials(credentials.token, store_id, credentials.kind)

    async def _send(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, object] | None = None,
        body: JSONBody | None = None,
        headers: Mapping[str, str] | None = None,
        create_only: bool = False,
    ) -> httpx.Response:
        credentials = await self._credentials()
        merged = {**_headers(credentials), **dict(headers or {})}
        try:
            response = await self._transport.send(
                method,
                path,
                token=credentials.token,
                params=cast(Any, params),
                body=body,
                headers=merged,
                read_response=ReadResponsePolicy.ALWAYS,
            )
        except httpx.HTTPError as exc:
            raise BlobUnknownError() from exc
        if not response.is_success:
            _raise_control_error(response, create_only=create_only)
        return response

    async def _published_stat(self, pathname: str, etag: str) -> BlobStatResult:
        result = await self.stat(pathname)
        if result.etag != etag:
            raise BlobPreconditionFailedError(
                "Blob publication succeeded but metadata provenance was lost"
            )
        return result

    async def _request_signed_token(
        self,
        credentials: BlobCredentials,
        issuance: Mapping[str, object],
    ) -> _SignedTokenPayload:
        try:
            response = await self._transport.send(
                "POST",
                f"{self._base_url}/signed-token",
                token=credentials.token,
                body=JSONBody(issuance),
                headers={
                    **_headers(credentials),
                    "x-vercel-blob-store-id": credentials.store_id,
                },
                read_response=ReadResponsePolicy.ALWAYS,
            )
        except httpx.HTTPError as exc:
            raise BlobUnknownError() from exc
        if not response.is_success:
            _raise_control_error(response)
        return _validate_response(
            _SignedTokenPayload,
            _json(response),
            "Malformed signed-token response",
        )

    async def stat(self, pathname: str) -> BlobStatResult:
        """Fetch metadata for one object from the Blob control API.

        Args:
            pathname: Store-relative object pathname.

        Returns:
            Parsed and validated object metadata.
        """
        response = await self._send("GET", self._base_url, params={"url": pathname})
        return _stat(_json(response))

    async def list_page(
        self,
        *,
        prefix: str,
        mode: ScandirMode,
        page_size: int | None,
        cursor: str | None,
    ) -> BlobPageState:
        """Fetch and validate one Blob listing page.

        Args:
            prefix: Store-relative listing prefix.
            mode: Listing mode requested from the API.
            page_size: Optional backend page size hint.
            cursor: Optional continuation cursor.

        Returns:
            Parsed object and prefix entries plus continuation state.
        """
        params: dict[str, object] = {"prefix": prefix, "mode": mode.value}
        if page_size is not None:
            params["limit"] = page_size
        if cursor is not None:
            params["cursor"] = cursor
        payload = cast(
            _BlobListPayload,
            _validate_response(
                _BlobListPayload,
                _json(await self._send("GET", self._base_url, params=params)),
                "Blob API returned malformed list metadata",
            ),
        )
        if mode is ScandirMode.EXPANDED and payload.folders:
            raise BlobStreamError("Blob API returned malformed list metadata")
        return BlobPageState(
            entries=tuple(item.to_state() for item in payload.blobs)
            + tuple(BlobPrefixState(folder) for folder in payload.folders),
            cursor=payload.cursor,
            has_more=payload.has_more,
        )

    async def delete(self, pathname: str, *, if_match: str | None = None) -> None:
        """Delete one exact object pathname.

        Args:
            pathname: Store-relative object pathname.
            if_match: Optional ETag precondition.
        """
        headers = {"x-if-match": if_match} if if_match is not None else None
        await self._send(
            "POST", f"{self._base_url}/delete", body=JSONBody({"urls": [pathname]}), headers=headers
        )

    async def delete_batch(self, pathnames: Sequence[str]) -> None:
        """Delete a batch of object pathnames.

        Args:
            pathnames: Store-relative object pathnames.
        """
        await self._send(
            "POST", f"{self._base_url}/delete", body=JSONBody({"urls": list(pathnames)})
        )

    async def _stream_request(
        self,
        *,
        method: str = "PUT",
        path: str | None = None,
        pathname: str,
        source: ReadableByteStream,
        size: int,
        headers: Mapping[str, str],
    ) -> dict[str, Any]:
        try:
            return await self._stream_request_impl(
                method=method,
                path=path,
                pathname=pathname,
                source=source,
                size=size,
                headers=headers,
            )
        except httpx.HTTPError as exc:
            raise BlobUnknownError() from exc

    async def _stream_request_impl(
        self,
        *,
        method: str = "PUT",
        path: str | None = None,
        pathname: str,
        source: ReadableByteStream,
        size: int,
        headers: Mapping[str, str],
    ) -> dict[str, Any]:
        _validate_size(size)
        credentials = await self._credentials()
        request_headers = {
            **_headers(credentials),
            **dict(headers),
            "content-length": str(size),
        }
        async with self._transport.request_stream(
            method,
            self._base_url if path is None else path,
            token=credentials.token,
            params={"pathname": pathname},
            headers=request_headers,
            read_response=ReadResponsePolicy.NON_SUCCESS_ONLY,
        ) as request:
            remaining = size
            while remaining:
                chunk = await source.read(min(remaining, _TRANSFER_CHUNK_SIZE))
                if not chunk:
                    raise BlobStreamError("Byte source ended before its declared size")
                if len(chunk) > remaining:
                    raise BlobStreamError("Byte source exceeded its declared size")
                await request.write(chunk)
                remaining -= len(chunk)
            if await source.read(1):
                raise BlobStreamError("Byte source exceeded its declared size")
            stream = await request.finish()
            try:
                body = await stream.read()
            finally:
                await stream.aclose()
            response = stream.response
            if not response.is_success:
                _raise_control_error(
                    response, create_only=request_headers.get("x-allow-overwrite") == "0"
                )
            try:
                value = json.loads(body)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise BlobStreamError("Blob API returned malformed JSON") from exc
            if not isinstance(value, dict):
                raise BlobStreamError("Blob API returned malformed JSON")
            return cast(dict[str, Any], value)

    async def put(
        self,
        pathname: str,
        source: ReadableByteStream,
        *,
        size: int,
        access: Access,
        content_type: str | None,
        cache_control_max_age: timedelta | None,
        exclusive: bool,
        if_match: str | None = None,
    ) -> BlobStatResult:
        """Upload one complete object and verify the published metadata.

        Args:
            pathname: Store-relative object pathname.
            source: Normalized byte stream with exactly ``size`` bytes.
            size: Declared source length.
            access: Delivery access to publish.
            content_type: Optional content type to publish.
            cache_control_max_age: Optional cache max-age to publish.
            exclusive: Whether the object must not already exist.
            if_match: Optional ETag precondition.

        Returns:
            Complete metadata for the object that was published.
        """
        _validate_size(size)
        headers = cast(
            dict[str, str],
            create_put_headers(
                content_type=content_type,
                add_random_suffix=False,
                allow_overwrite=not exclusive,
                cache_control_max_age=_cache_control_max_age_seconds(cache_control_max_age),
                access=access,
                if_match=if_match,
            ),
        )
        data = await self._stream_request(
            pathname=pathname, source=source, size=size, headers=headers
        )
        etag = _publication_etag(data, "put")
        return await self._published_stat(pathname, etag)

    async def create_marker(
        self, pathname: str, *, access: Access, exist_ok: bool
    ) -> BlobStatResult:
        """Create a zero-byte marker object for a prefix pathname.

        Args:
            pathname: Store-relative marker pathname ending in ``/``.
            access: Delivery access to publish.
            exist_ok: Whether an existing marker should be accepted.

        Returns:
            Metadata for the marker object.
        """
        if not pathname.endswith("/"):
            raise ValueError("marker pathname must end with '/'")
        credentials = await self._credentials()
        headers = cast(
            dict[str, str],
            {
                **_headers(credentials),
                **create_put_headers(add_random_suffix=False, allow_overwrite=False, access=access),
            },
        )
        try:
            response = await self._transport.send(
                "PUT",
                self._base_url,
                token=credentials.token,
                params={"pathname": pathname},
                headers=headers,
                read_response=ReadResponsePolicy.ALWAYS,
            )
        except httpx.HTTPError as exc:
            raise BlobUnknownError() from exc
        if not response.is_success:
            try:
                _raise_control_error(response, create_only=True)
            except BlobAlreadyExistsError:
                if not exist_ok:
                    raise FileExistsError(pathname) from None
        return await self.stat(pathname)

    async def presign(
        self,
        pathname: str,
        *,
        operation: PresignedOperation,
        access: Access,
        expires_at: datetime,
        maximum_size: int | None = None,
        allowed_content_types: Sequence[str] | None = None,
        allow_overwrite: bool | None = None,
        cache_control_max_age: timedelta | None = None,
        if_match: str | None = None,
    ) -> PresignedUrl:
        """Create and locally constrain a Blob delegation URL.

        Args:
            pathname: Store-relative object pathname.
            operation: Operation the URL may perform.
            access: Delivery access for read URLs.
            expires_at: Requested absolute expiry.
            maximum_size: Optional maximum upload size for PUT URLs.
            allowed_content_types: Optional content type allow-list for PUT
                URLs.
            allow_overwrite: Optional overwrite policy for PUT URLs.
            cache_control_max_age: Optional cache max-age for PUT URLs.
            if_match: Optional ETag precondition for PUT or DELETE URLs.

        Returns:
            A presigned URL with the effective constraints encoded.
        """
        _validate_presign_pathname(pathname)
        _validate_presign(
            operation,
            expires_at,
            maximum_size=maximum_size,
            allowed_content_types=allowed_content_types,
            allow_overwrite=allow_overwrite,
            cache_control_max_age=cache_control_max_age,
            if_match=if_match,
        )
        credentials = await self._credentials()
        requested_expires_at = _normalize_wire_datetime(expires_at)
        issuance = _presign_issuance_body(
            pathname=pathname,
            operation=operation,
            requested_expires_at=requested_expires_at,
            maximum_size=maximum_size,
            allowed_content_types=allowed_content_types,
        )
        signed_token = await self._request_signed_token(credentials, issuance)
        delegated_expires_at = _datetime_from_milliseconds(signed_token.valid_until)
        _validate_signed_delegation(
            signed_token,
            credentials=credentials,
            pathname=pathname,
            operation=operation,
            requested_expires_at=requested_expires_at,
            delegated_expires_at=delegated_expires_at,
            maximum_size=maximum_size,
            allowed_content_types=allowed_content_types,
        )
        constraints = _presigned_constraints(
            operation=operation,
            requested_expires_at=requested_expires_at,
            delegated_expires_at=delegated_expires_at,
            maximum_size=maximum_size,
            allowed_content_types=allowed_content_types,
            allow_overwrite=allow_overwrite,
            cache_control_max_age=cache_control_max_age,
            if_match=if_match,
        )
        url = _presigned_url(
            base_url=self._base_url,
            credentials=credentials,
            access=access,
            operation=operation,
            pathname=pathname,
            constraints=constraints,
            delegation=signed_token.delegation_token,
            signing_token=signed_token.client_signing_token,
        )
        effective_expires_at = min(requested_expires_at, delegated_expires_at)
        return PresignedUrl(
            url=url,
            operation=operation,
            expires_at=effective_expires_at,
            required_headers={},
        )

    async def create_multipart_upload(
        self,
        pathname: str,
        *,
        access: Access,
        content_type: str | None,
        cache_control_max_age: timedelta | None,
    ) -> MultipartUploadState:
        """Create a multipart upload session for one object.

        Args:
            pathname: Store-relative object pathname.
            access: Delivery access to publish.
            content_type: Optional content type to publish.
            cache_control_max_age: Optional cache max-age to publish.

        Returns:
            Multipart upload identifiers needed for subsequent parts.
        """
        headers = cast(
            dict[str, str],
            {
                **create_put_headers(
                    content_type=content_type,
                    add_random_suffix=False,
                    cache_control_max_age=_cache_control_max_age_seconds(cache_control_max_age),
                    access=access,
                ),
                "x-mpu-action": "create",
            },
        )
        data = _json(
            await self._send(
                "POST", f"{self._base_url}/mpu", params={"pathname": pathname}, headers=headers
            )
        )
        upload_id = data.get("uploadId")
        key = data.get("key")
        if not isinstance(upload_id, str) or not upload_id or not isinstance(key, str) or not key:
            raise BlobStreamError("Malformed multipart create response")
        return MultipartUploadState(pathname, upload_id, key)

    async def upload_part(
        self,
        upload: MultipartUploadState,
        *,
        part_number: int,
        source: ReadableByteStream,
        size: int,
    ) -> MultipartPartState:
        """Upload one multipart part from a bounded byte stream.

        Args:
            upload: Multipart upload identifiers.
            part_number: One-based part number.
            source: Normalized byte stream with exactly ``size`` bytes.
            size: Declared part length.

        Returns:
            Metadata needed to complete the part.
        """
        if isinstance(part_number, bool) or not isinstance(part_number, int) or part_number <= 0:
            raise ValueError("part_number must be a positive integer")
        headers = {
            "x-mpu-action": "upload",
            "x-mpu-key": quote(upload.key, safe=""),
            "x-mpu-upload-id": upload.upload_id,
            "x-mpu-part-number": str(part_number),
        }
        data = await self._stream_request(
            method="POST",
            path=f"{self._base_url}/mpu",
            pathname=upload.pathname,
            source=source,
            size=size,
            headers=headers,
        )
        etag = data.get("etag")
        if not isinstance(etag, str) or not etag:
            raise BlobStreamError("Malformed multipart part response")
        return MultipartPartState(part_number, etag)

    async def complete_multipart_upload(
        self,
        upload: MultipartUploadState,
        parts: Sequence[MultipartPartState],
        *,
        exclusive: bool,
        if_match: str | None,
    ) -> BlobStatResult:
        """Complete a multipart upload and verify published metadata.

        Args:
            upload: Multipart upload identifiers.
            parts: Uploaded part metadata.
            exclusive: Whether the object must not already exist.
            if_match: Optional ETag precondition.

        Returns:
            Complete metadata for the object that was published.
        """
        # The backend currently has no abort operation; abandoned uploads rely on
        # service lifecycle cleanup and must never be completed by failed writers.
        ordered = sorted(parts, key=lambda part: part.part_number)
        if any(part.part_number <= 0 for part in ordered):
            raise ValueError("multipart part numbers must be positive")
        if len({part.part_number for part in ordered}) != len(ordered):
            raise ValueError("multipart part numbers must be unique")
        headers = {
            "x-mpu-action": "complete",
            "x-mpu-key": quote(upload.key, safe=""),
            "x-mpu-upload-id": upload.upload_id,
            "x-allow-overwrite": "0" if exclusive else "1",
        }
        if if_match is not None:
            headers["x-if-match"] = if_match
        data = _json(
            await self._send(
                "POST",
                f"{self._base_url}/mpu",
                params={"pathname": upload.pathname},
                body=JSONBody(
                    [{"partNumber": part.part_number, "etag": part.etag} for part in ordered]
                ),
                headers=headers,
                create_only=exclusive,
            )
        )
        etag = _publication_etag(data, "multipart complete")
        return await self._published_stat(upload.pathname, etag)

    async def read_range(
        self,
        stat: BlobStatResult,
        *,
        access: Access,
        start: int,
        end: int,
    ) -> BlobRangeResponse:
        """Open an ETag-pinned delivery range response.

        Args:
            stat: Metadata captured when the reader opened.
            access: Delivery access to use for the request.
            start: Inclusive byte offset.
            end: Inclusive byte offset.

        Returns:
            A streaming response for the requested range.
        """
        if access not in ("public", "private"):
            raise ValueError('access must be "public" or "private"')
        if isinstance(start, bool) or not isinstance(start, int):
            raise TypeError("start must be an integer")
        if isinstance(end, bool) or not isinstance(end, int):
            raise TypeError("end must be an integer")
        if start < 0 or end < start:
            raise ValueError("invalid Blob byte range")
        if start > stat.size or end > stat.size or (start < stat.size and end == stat.size):
            raise ValueError("Blob byte range exceeds object size")
        credentials = await self._credentials()
        headers = {"range": f"bytes={start}-{end}", "if-match": stat.etag}
        if access == "private":
            headers["authorization"] = f"Bearer {credentials.token}"
        try:
            stream = await self._transport.open_response_stream(
                "GET",
                stat.url,
                headers=headers,
                follow_redirects=True,
                read_response=ReadResponsePolicy.NON_SUCCESS_ONLY,
            )
        except httpx.HTTPError as exc:
            raise BlobUnknownError() from exc
        response = stream.response
        try:
            if response.status_code == 404:
                raise BlobNotFoundError()
            if response.status_code == 412:
                raise BlobPreconditionFailedError("Blob ETag precondition failed")
            if response.status_code == 416:
                if start == stat.size:
                    await stream.aclose()
                    return BlobRangeResponse(None, start=start, end=start - 1, total=stat.size)
                raise BlobStreamError("Blob delivery rejected the requested range")
            if response.status_code != 206:
                raise BlobStreamError("Blob delivery did not return a partial response")
            match = _CONTENT_RANGE.fullmatch(response.headers.get("content-range", ""))
            if response.headers.get("etag") != stat.etag:
                raise BlobPreconditionFailedError("Blob ETag precondition failed")
            if match is None or tuple(map(int, match.groups())) != (start, end, stat.size):
                raise BlobStreamError("Blob delivery returned invalid range metadata")
            return BlobRangeResponse(stream, start=start, end=end, total=stat.size)
        except BaseException:
            await stream.aclose()
            raise


__all__ = ["BlobApiClient"]
