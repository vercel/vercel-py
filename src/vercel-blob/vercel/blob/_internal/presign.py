"""Local signing of presigned Blob URLs from an issued delegation token.

The canonical string and query names must stay aligned with the TypeScript
SDK (`signed-token.ts`, `presign-query-params.ts`) and the Blob edge
(`signed_url.go`), which recomputes the same HMAC.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from urllib.parse import urlencode, urlsplit

from vercel.blob.errors import BlobError
from vercel.blob.models import Access, BlobSignedToken, PresignOperation

from .validation import (
    construct_delivery_url,
    has_control_character,
    normalize_store_id,
    validate_access,
    validate_bool_flag,
    validate_content_type,
    validate_pathname,
)

_VALID_UNTIL = "vercel-blob-valid-until"
_MAXIMUM_SIZE = "vercel-blob-maximum-size-in-bytes"
_ALLOWED_CONTENT_TYPES = "vercel-blob-allowed-content-types"
_ADD_RANDOM_SUFFIX = "vercel-blob-add-random-suffix"
_ALLOW_OVERWRITE = "vercel-blob-allow-overwrite"
_CACHE_CONTROL_MAX_AGE = "vercel-blob-cache-control-max-age"
_IF_MATCH = "vercel-blob-if-match"
_TTL_DAYS = "vercel-blob-ttl-days"
_CALLBACK_URL = "vercel-blob-callback-url"
_CALLBACK_TOKEN_PAYLOAD = "vercel-blob-callback-token-payload"

_CANONICAL_KEYS = tuple(
    sorted(
        (
            _VALID_UNTIL,
            _MAXIMUM_SIZE,
            _ALLOWED_CONTENT_TYPES,
            _ADD_RANDOM_SUFFIX,
            _ALLOW_OVERWRITE,
            _CACHE_CONTROL_MAX_AGE,
            _IF_MATCH,
            _TTL_DAYS,
            _CALLBACK_URL,
            _CALLBACK_TOKEN_PAYLOAD,
        )
    )
)

_OPERATIONS: tuple[PresignOperation, ...] = ("get", "head", "put", "delete")

# Limits match the TypeScript SDK, which measures lengths in UTF-16 code units.
_MAX_CALLBACK_URL_LENGTH = 4096
_MAX_CALLBACK_TOKEN_PAYLOAD_LENGTH = 8192
_MAX_CONTENT_TYPES_LENGTH = 16_384
_MAX_CACHE_CONTROL_MAX_AGE = 365 * 24 * 60 * 60
_MAX_TTL_DAYS = 730
_MAX_IF_MATCH_LENGTH = 256


@dataclass(frozen=True, slots=True)
class PresignOptions:
    """Per-URL constraints; upload options apply only to ``put``."""

    use_cache: bool = True
    allowed_content_types: Sequence[str] | None = None
    maximum_size_in_bytes: int | None = None
    add_random_suffix: bool | None = None
    allow_overwrite: bool | None = None
    cache_control_max_age: int | None = None
    if_match: str | None = None
    ttl_days: int | None = None
    callback_url: str | None = None
    callback_token_payload: str | None = None


@dataclass(frozen=True, slots=True)
class _DelegationScope:
    store_id: str
    pathname: str
    operations: frozenset[str]
    valid_until_ms: int
    maximum_size_in_bytes: int | None
    allowed_content_types: tuple[str, ...] | None


def _utf16_length(value: str) -> int:
    return len(value.encode("utf-16-le")) // 2


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def validate_operations(operations: object) -> list[PresignOperation] | None:
    if operations is None:
        return None
    if isinstance(operations, str) or not isinstance(operations, Sequence):
        raise BlobError("operations must be a sequence of 'get', 'head', 'put', or 'delete'")
    if not operations:
        raise BlobError("operations cannot be empty")
    unique: list[PresignOperation] = []
    for operation in operations:
        if operation not in _OPERATIONS:
            raise BlobError(f"Unknown presign operation: {operation!r}")
        if operation not in unique:
            unique.append(operation)
    return unique


def validate_content_types(value: object) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise BlobError("allowed_content_types must be a sequence of strings")
    if not value:
        raise BlobError("allowed_content_types cannot be empty")
    try:
        return [validate_content_type(item) or "" for item in value]
    except ValueError as exc:
        raise BlobError(f"allowed_content_types entry is invalid: {exc}") from None


def validate_maximum_size(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise BlobError("maximum_size_in_bytes must be a positive integer")
    return value


def _b64url_decode(segment: str) -> bytes:
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def decode_delegation_scope(delegation_token: str) -> _DelegationScope:
    """Read the unverified scope that the Blob API embedded in a delegation token."""
    segment, dot, _ = delegation_token.partition(".")
    if not dot:
        raise BlobError("Invalid delegation token format")
    try:
        payload = json.loads(_b64url_decode(segment))
    except (binascii.Error, ValueError):
        raise BlobError("Invalid or unreadable delegation token payload") from None
    if not isinstance(payload, dict):
        raise BlobError("Invalid or unreadable delegation token payload")

    store_id = payload.get("storeId")
    if not isinstance(store_id, str) or not store_id:
        raise BlobError("Delegation token payload is missing storeId")
    pathname = payload.get("pathname", "")
    operations = payload.get("operations")
    valid_until = payload.get("validUntil")
    maximum_size = payload.get("maximumSizeInBytes")
    content_types = payload.get("allowedContentTypes")
    if (
        not isinstance(pathname, str)
        or not isinstance(operations, list)
        or not all(isinstance(op, str) for op in operations)
        or isinstance(valid_until, bool)
        or not isinstance(valid_until, (int, float))
        or (maximum_size is not None and not isinstance(maximum_size, (int, float)))
        or (
            content_types is not None
            and (
                not isinstance(content_types, list)
                or not all(isinstance(ct, str) for ct in content_types)
            )
        )
    ):
        raise BlobError("Invalid or unreadable delegation token payload")
    return _DelegationScope(
        store_id=normalize_store_id(store_id),
        pathname=pathname,
        operations=frozenset(operations),
        valid_until_ms=int(valid_until),
        maximum_size_in_bytes=int(maximum_size) if maximum_size is not None else None,
        allowed_content_types=tuple(content_types) if content_types is not None else None,
    )


def _content_type_allowed(content_type: str, allowed: Sequence[str]) -> bool:
    kind = content_type.split("/", 1)[0]
    return content_type in allowed or (bool(kind) and f"{kind}/*" in allowed)


def _validate_callback_url(callback_url: object) -> str:
    if not isinstance(callback_url, str) or not callback_url:
        raise BlobError("callback_url must be a non-empty string")
    if _utf16_length(callback_url) > _MAX_CALLBACK_URL_LENGTH:
        raise BlobError("callback_url is too long")
    parts = urlsplit(callback_url)
    if not parts.scheme or not parts.netloc or has_control_character(callback_url):
        raise BlobError("callback_url must be an absolute URL")
    return callback_url


def _validate_if_match(if_match: object) -> str:
    if not isinstance(if_match, str) or not if_match:
        raise BlobError("if_match must be a non-empty string")
    if _utf16_length(if_match) > _MAX_IF_MATCH_LENGTH:
        raise BlobError("if_match is too long")
    if has_control_character(if_match):
        raise BlobError("if_match cannot contain control characters")
    return if_match


def _reject_inapplicable(operation: PresignOperation, options: PresignOptions) -> None:
    if not options.use_cache and operation != "get":
        raise BlobError("use_cache only applies to 'get' presigned URLs")
    if options.if_match is not None and operation not in ("put", "delete"):
        raise BlobError("if_match only applies to 'put' and 'delete' presigned URLs")
    if operation == "put":
        return
    upload_only = {
        "allowed_content_types": options.allowed_content_types,
        "maximum_size_in_bytes": options.maximum_size_in_bytes,
        "add_random_suffix": options.add_random_suffix,
        "allow_overwrite": options.allow_overwrite,
        "cache_control_max_age": options.cache_control_max_age,
        "ttl_days": options.ttl_days,
        "callback_url": options.callback_url,
        "callback_token_payload": options.callback_token_payload,
    }
    for name, value in upload_only.items():
        if value is not None:
            raise BlobError(f"{name} only applies to 'put' presigned URLs")


def _resolve_expiry(scope: _DelegationScope, expires_at_ms: int | None, now_ms: int) -> int:
    expires_at = scope.valid_until_ms
    if expires_at_ms is not None:
        expires_at = min(expires_at_ms, scope.valid_until_ms)
    if expires_at <= now_ms:
        raise BlobError(
            "Presigned URL expiry is not in the future; issue a new signed token "
            "or pass a later expiry"
        )
    return expires_at


def _upload_entries(scope: _DelegationScope, options: PresignOptions) -> dict[str, str]:
    entries: dict[str, str] = {}

    content_types = validate_content_types(options.allowed_content_types)
    if content_types is not None:
        if scope.allowed_content_types:
            for content_type in content_types:
                if not _content_type_allowed(content_type, scope.allowed_content_types):
                    raise BlobError(
                        f"allowed_content_types entry {content_type!r} is not permitted "
                        "by the signed token"
                    )
        # JavaScript sorts strings by UTF-16 code units.
        csv = ",".join(sorted(content_types, key=lambda ct: ct.encode("utf-16-be")))
        if _utf16_length(csv) > _MAX_CONTENT_TYPES_LENGTH:
            raise BlobError("allowed_content_types is too long")
        entries[_ALLOWED_CONTENT_TYPES] = csv

    maximum_size = validate_maximum_size(options.maximum_size_in_bytes)
    if maximum_size is not None:
        if scope.maximum_size_in_bytes is not None and maximum_size > scope.maximum_size_in_bytes:
            raise BlobError(
                "maximum_size_in_bytes cannot exceed the signed token's limit of "
                f"{scope.maximum_size_in_bytes}"
            )
        entries[_MAXIMUM_SIZE] = str(maximum_size)

    if options.add_random_suffix is not None:
        flag = validate_bool_flag(options.add_random_suffix, name="add_random_suffix")
        entries[_ADD_RANDOM_SUFFIX] = "true" if flag else "false"
    if options.allow_overwrite is not None:
        flag = validate_bool_flag(options.allow_overwrite, name="allow_overwrite")
        entries[_ALLOW_OVERWRITE] = "true" if flag else "false"

    max_age = options.cache_control_max_age
    if max_age is not None:
        if not _is_int(max_age) or not 0 <= max_age <= _MAX_CACHE_CONTROL_MAX_AGE:
            raise BlobError(
                "cache_control_max_age must be an integer between 0 and "
                f"{_MAX_CACHE_CONTROL_MAX_AGE}"
            )
        entries[_CACHE_CONTROL_MAX_AGE] = str(max_age)

    if options.if_match is not None:
        entries[_IF_MATCH] = _validate_if_match(options.if_match)

    ttl_days = options.ttl_days
    if ttl_days is not None:
        if not _is_int(ttl_days) or not 1 <= ttl_days <= _MAX_TTL_DAYS:
            raise BlobError(f"ttl_days must be an integer between 1 and {_MAX_TTL_DAYS}")
        entries[_TTL_DAYS] = str(ttl_days)

    if options.callback_url is not None:
        entries[_CALLBACK_URL] = _validate_callback_url(options.callback_url)
    payload = options.callback_token_payload
    if payload is not None:
        if options.callback_url is None:
            raise BlobError("callback_token_payload requires callback_url")
        if not isinstance(payload, str):
            raise BlobError("callback_token_payload must be a string")
        if _utf16_length(payload) > _MAX_CALLBACK_TOKEN_PAYLOAD_LENGTH:
            raise BlobError("callback_token_payload is too long")
        if payload:
            entries[_CALLBACK_TOKEN_PAYLOAD] = payload

    return entries


def canonical_string(operation: str, pathname: str, entries: Mapping[str, str]) -> str:
    """Join the signed fields as ``key=value`` lines sorted by UTF-8 bytes."""
    lines = [f"operation={operation}", f"pathname={pathname}"]
    lines.extend(f"{key}={value}" for key in _CANONICAL_KEYS if (value := entries.get(key)))
    lines.sort(key=lambda line: line.encode("utf-8"))
    return "\n".join(lines)


def presign_url(
    signed_token: BlobSignedToken,
    pathname: str,
    *,
    operation: PresignOperation,
    access: Access | None,
    expires_at_ms: int | None,
    options: PresignOptions,
    api_url: str,
    now_ms: int,
) -> str:
    """Validate a request against the token scope and return the signed URL."""
    if not isinstance(signed_token, BlobSignedToken):
        raise BlobError("signed_token must be a BlobSignedToken")
    if not signed_token.delegation_token or not signed_token.client_signing_token:
        raise BlobError("signed_token requires delegation_token and client_signing_token")
    if operation not in _OPERATIONS:
        raise BlobError(f"Unknown presign operation: {operation!r}")
    clean_path = validate_pathname(pathname)
    if operation in ("get", "head"):
        if access is None:
            raise BlobError(f"access is required for '{operation}' presigned URLs")
        clean_access = validate_access(access)
    elif access is not None:
        raise BlobError(f"access does not apply to '{operation}' presigned URLs")
    _reject_inapplicable(operation, options)

    scope = decode_delegation_scope(signed_token.delegation_token)
    if scope.pathname not in ("", "*") and scope.pathname != clean_path:
        raise BlobError(
            f"pathname does not match the signed token scope; expected {scope.pathname!r}, "
            f"got {clean_path!r}"
        )
    if now_ms > scope.valid_until_ms:
        raise BlobError("The signed token has expired; issue a new one first")
    if operation not in scope.operations:
        raise BlobError(
            f"The signed token does not allow '{operation}'; include it in operations "
            "when issuing the token"
        )

    entries: dict[str, str] = {}
    expires_at = _resolve_expiry(scope, expires_at_ms, now_ms)
    if expires_at < scope.valid_until_ms:
        entries[_VALID_UNTIL] = str(expires_at)
    if operation == "delete" and options.if_match is not None:
        entries[_IF_MATCH] = _validate_if_match(options.if_match)
    elif operation == "put":
        entries |= _upload_entries(scope, options)

    canonical = canonical_string(operation, clean_path, entries)
    digest = hmac.new(
        signed_token.client_signing_token.encode("utf-8"),
        canonical.encode("utf-8"),
        hashlib.sha256,
    ).digest()

    params: dict[str, str] = {}
    if operation in ("get", "head"):
        base = construct_delivery_url(scope.store_id, clean_path, clean_access)
        if not options.use_cache and clean_access == "private":
            params["cache"] = "0"
    else:
        base = f"{api_url.rstrip('/')}/"
        params["pathname"] = clean_path
    params |= entries
    params["vercel-blob-delegation"] = signed_token.delegation_token
    params["vercel-blob-signature"] = _b64url_encode(digest)
    return f"{base}?{urlencode(params)}"
