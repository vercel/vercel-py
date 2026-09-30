"""Input validation and URL formatting for Vercel Blob."""

from __future__ import annotations

import re
from typing import Literal
from urllib.parse import quote, urlsplit, urlunsplit

from vercel.blob.errors import BlobError

_STORE_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")
_DELIVERY_DOMAIN_SUFFIX = ".blob.vercel-storage.com"
_URL_SCHEME_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*://")


def has_control_character(value: str) -> bool:
    return any(ord(char) < 32 or ord(char) == 127 for char in value)


def normalize_store_id(store_id: str) -> str:
    """Strip optional 'store_' prefix while preserving case per API requirements."""
    if not isinstance(store_id, str):
        raise BlobError("Blob store ID must be a string")
    normalized = store_id.removeprefix("store_").strip()
    if not normalized:
        raise BlobError("Blob store ID cannot be empty")
    if has_control_character(normalized) or not _STORE_ID_PATTERN.match(normalized):
        raise BlobError(f"Invalid Blob store ID format: {store_id!r}")
    return normalized


def validate_access(access: object) -> Literal["public", "private"]:
    if access == "public":
        return "public"
    if access == "private":
        return "private"
    raise BlobError("access must be 'public' or 'private'")


def validate_pathname(pathname: object) -> str:
    if not isinstance(pathname, str):
        raise BlobError("pathname must be a string")
    if not pathname:
        raise BlobError("pathname cannot be empty")
    if has_control_character(pathname):
        raise BlobError("pathname cannot contain control characters")

    normalized = pathname.lstrip("/")
    if not normalized:
        raise BlobError("pathname cannot be root or empty")

    segments = normalized.split("/")
    for seg in segments:
        if seg in (".", ".."):
            raise BlobError("Pathname cannot contain dot segments ('.' or '..')")

    return normalized


def validate_put_body(body: object) -> bytes:
    if type(body) is not bytes:
        raise TypeError(f"put body must be bytes, got {type(body).__name__}")
    return body


def is_url(value: str) -> bool:
    if not isinstance(value, str):
        return False
    return (
        bool(_URL_SCHEME_PATTERN.match(value))
        or value.startswith("http://")
        or value.startswith("https://")
    )


def parse_and_validate_delivery_url(
    url: str,
    *,
    access: Literal["public", "private"] | None = None,
    expected_store_id: str | None = None,
) -> tuple[str, str, str, str]:
    """Validate a delivery URL and return (clean_url, store_id, access_level, pathname)."""
    if not isinstance(url, str) or not url:
        raise BlobError("Blob URL must be a non-empty string")
    if has_control_character(url):
        raise BlobError("Blob URL cannot contain control characters")

    parsed = urlsplit(url)
    if parsed.scheme != "https":
        raise BlobError(f"Blob delivery URL must use HTTPS scheme, got {parsed.scheme!r}")
    if "@" in (parsed.netloc or "") or parsed.username or parsed.password:
        raise BlobError("Blob delivery URL must not contain userinfo")
    if parsed.port is not None and parsed.port != 443:
        raise BlobError(f"Blob delivery URL must not specify non-default port: {parsed.port}")

    hostname = (parsed.hostname or "").lower()
    if not hostname.endswith(_DELIVERY_DOMAIN_SUFFIX):
        msg = f"Invalid URL: hostname {hostname!r} does not point to a Vercel Blob store"
        raise BlobError(f"{msg} (*{_DELIVERY_DOMAIN_SUFFIX})")

    prefix = hostname[: -len(_DELIVERY_DOMAIN_SUFFIX)]
    parts = prefix.split(".")
    if len(parts) != 2:
        msg = f"Invalid URL: expected '<store_id>.<access>{_DELIVERY_DOMAIN_SUFFIX}'"
        raise BlobError(f"{msg}, got {hostname!r}")

    url_store_id, url_access = parts
    if not url_store_id or not _STORE_ID_PATTERN.match(url_store_id):
        raise BlobError(f"Invalid store ID in URL hostname: {url_store_id!r}")

    if url_access not in ("public", "private"):
        raise BlobError(f"Invalid URL access level in hostname: {url_access!r}")

    if access is not None and url_access != access:
        raise BlobError(
            f"URL access level '{url_access}' does not match requested access '{access}'"
        )

    if expected_store_id is not None:
        expected_norm = normalize_store_id(expected_store_id)
        if url_store_id.lower() != expected_norm.lower():
            msg = f"URL store ID '{url_store_id}' does not match credential store ID"
            raise BlobError(f"{msg} '{expected_norm}'")

    pathname = validate_pathname(parsed.path)

    # Reassemble URL without fragment
    clean_url = urlunsplit(("https", hostname, parsed.path, parsed.query, ""))
    return clean_url, url_store_id, url_access, pathname


def validate_url_or_pathname(url_or_pathname: object) -> str:
    """Validate that input is either a valid Blob URL or valid pathname."""
    if not isinstance(url_or_pathname, str) or not url_or_pathname:
        raise BlobError("url_or_pathname must be a non-empty string")
    if has_control_character(url_or_pathname):
        raise BlobError("url_or_pathname cannot contain control characters")
    if is_url(url_or_pathname):
        clean_url, _, _, _ = parse_and_validate_delivery_url(url_or_pathname)
        return clean_url
    return validate_pathname(url_or_pathname)


def construct_delivery_url(
    store_id: str,
    pathname: str,
    access: Literal["public", "private"],
) -> str:
    clean_store_id = normalize_store_id(store_id)
    clean_path = validate_pathname(pathname)
    quoted_path = quote(clean_path, safe="/")
    return f"https://{clean_store_id.lower()}.{access}{_DELIVERY_DOMAIN_SUFFIX}/{quoted_path}"
