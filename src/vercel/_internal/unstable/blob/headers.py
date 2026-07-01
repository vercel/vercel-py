"""Header and endpoint helpers owned by the unstable Blob implementation."""

import os
from typing import TypedDict

DEFAULT_VERCEL_BLOB_API_URL = "https://vercel.com/api/blob"


def get_api_url(pathname: str = "") -> str:
    base_url = os.getenv("VERCEL_BLOB_API_URL") or os.getenv("NEXT_PUBLIC_VERCEL_BLOB_API_URL")
    return f"{base_url or DEFAULT_VERCEL_BLOB_API_URL}{pathname}"


def get_api_version() -> str:
    override = os.getenv("VERCEL_BLOB_API_VERSION_OVERRIDE") or os.getenv(
        "NEXT_PUBLIC_VERCEL_BLOB_API_VERSION_OVERRIDE"
    )
    return str(override or 12)


def extract_store_id_from_token(token: str) -> str:
    try:
        parts = token.split("_")
        return parts[3] if len(parts) > 3 else ""
    except Exception:
        return ""


PutHeaders = TypedDict(
    "PutHeaders",
    {
        "x-cache-control-max-age": str,
        "x-add-random-suffix": str,
        "x-allow-overwrite": str,
        "x-content-type": str,
        "x-vercel-blob-access": str,
        "x-if-match": str,
    },
    total=False,
)


def create_put_headers(
    content_type: str | None = None,
    add_random_suffix: bool | None = None,
    allow_overwrite: bool | None = None,
    cache_control_max_age: int | None = None,
    access: str | None = None,
    if_match: str | None = None,
) -> PutHeaders:
    headers: PutHeaders = {}
    if content_type:
        headers["x-content-type"] = content_type
    if add_random_suffix is not None:
        headers["x-add-random-suffix"] = "1" if add_random_suffix else "0"
    if allow_overwrite is not None:
        headers["x-allow-overwrite"] = "1" if allow_overwrite else "0"
    if cache_control_max_age is not None:
        headers["x-cache-control-max-age"] = str(cache_control_max_age)
    if access is not None:
        headers["x-vercel-blob-access"] = access
    if if_match is not None:
        headers["x-if-match"] = if_match
    return headers
