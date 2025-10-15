from __future__ import annotations

from typing import Any, Callable

from ._helpers import validate_path, require_public_access
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


def create_put_options(
    *,
    path: str,
    options: dict[str, Any] | None,
    extra_checks: Callable[[dict[str, Any]], None] | None = None,
    get_token: Callable[[str, dict[str, Any]], str] | None = None,
) -> dict[str, Any]:
    validate_path(path)
    if not options:
        raise BlobError("missing options, see usage")
    require_public_access(options)
    if extra_checks:
        extra_checks(options)
    if get_token:
        options["token"] = get_token(path, options)
    return options
