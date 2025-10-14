from __future__ import annotations

from typing import Any

from ._helpers import MAXIMUM_PATHNAME_LENGTH, validate_pathname, require_public_access
from .errors import BlobError


PUT_OPTION_HEADER_MAP: dict[str, str] = {
    "cacheControlMaxAge": "x-cache-control-max-age",
    "addRandomSuffix": "x-add-random-suffix",
    "allowOverwrite": "x-allow-overwrite",
    "contentType": "x-content-type",
}


def create_put_headers(allowed_options: list[str], options: dict[str, Any]) -> dict[str, str]:
    headers: dict[str, str] = {}
    if "contentType" in allowed_options and options.get("contentType"):
        headers[PUT_OPTION_HEADER_MAP["contentType"]] = str(options["contentType"])
    if "addRandomSuffix" in allowed_options and options.get("addRandomSuffix") is not None:
        headers[PUT_OPTION_HEADER_MAP["addRandomSuffix"]] = (
            "1" if options.get("addRandomSuffix") else "0"
        )
    if "allowOverwrite" in allowed_options and options.get("allowOverwrite") is not None:
        headers[PUT_OPTION_HEADER_MAP["allowOverwrite"]] = (
            "1" if options.get("allowOverwrite") else "0"
        )
    if "cacheControlMaxAge" in allowed_options and options.get("cacheControlMaxAge") is not None:
        headers[PUT_OPTION_HEADER_MAP["cacheControlMaxAge"]] = str(options["cacheControlMaxAge"])
    return headers


async def create_put_options(
    *, pathname: str, options: dict[str, Any] | None, extra_checks=None, get_token=None
) -> dict[str, Any]:
    validate_pathname(pathname)
    if not options:
        raise BlobError("missing options, see usage")
    require_public_access(options)
    if extra_checks:
        extra_checks(options)
    if get_token:
        options["token"] = await get_token(pathname, options)  # type: ignore[assignment]
    return options
