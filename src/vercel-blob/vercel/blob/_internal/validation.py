"""Validation for Blob paths and file open options."""

from __future__ import annotations

import codecs
import os
import re
from datetime import timedelta

from .models import DurationInput

_MAX_CACHE_CONTROL_AGE = timedelta(days=365)


def normalize_path(pathname: os.PathLike[str] | str) -> str:
    value = os.fspath(pathname)
    if not isinstance(value, str):
        raise TypeError("pathname must be str or a path-like object returning str")
    value = value.replace("\\", "/")
    if value.startswith("//"):
        raise ValueError("pathname must not have a double root")
    if value.startswith("/"):
        value = value[1:]
    if not value:
        raise ValueError("pathname must not be empty or root")
    if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", value):
        raise ValueError("pathname must be store-relative, not a URL")
    if len(value) > 950:
        raise ValueError("pathname must not exceed 950 characters")
    return value


def validate_open(
    pathname: os.PathLike[str] | str,
    mode: str,
    *,
    encoding: str | None,
    errors: str | None,
    newline: str | None,
    content_type: str | None,
    cache_control_max_age: DurationInput,
) -> tuple[str, str, bool, timedelta | None]:
    path = normalize_path(pathname)
    if path.endswith("/"):
        raise ValueError("pathname must identify an object, not a prefix")
    if mode not in ("r", "rb", "w", "wb"):
        raise ValueError(f"invalid mode: {mode!r}; Slice 1 supports r, rb, w, and wb")
    binary = "b" in mode
    reading = mode.startswith("r")
    if binary and any(value is not None for value in (encoding, errors, newline)):
        raise ValueError("encoding, errors, and newline are not supported in binary mode")
    if reading and (content_type is not None or cache_control_max_age is not None):
        raise ValueError("content_type and cache_control_max_age are not supported in read mode")
    if not binary:
        codecs.lookup(encoding or "utf-8")
        if errors is not None:
            codecs.lookup_error(errors)
        if newline not in (None, "", "\n", "\r", "\r\n"):
            raise ValueError("illegal newline value")
    return path, mode, binary, _parse_cache_control_max_age(cache_control_max_age)


def _parse_cache_control_max_age(value: DurationInput) -> timedelta | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, timedelta)):
        raise TypeError("cache_control_max_age must be an int, float, timedelta, or None")
    duration = value if isinstance(value, timedelta) else timedelta(seconds=value)
    if duration < timedelta(0) or duration > _MAX_CACHE_CONTROL_AGE:
        raise ValueError("cache_control_max_age is invalid")
    if duration.total_seconds() != int(duration.total_seconds()):
        raise ValueError("cache_control_max_age is invalid")
    return duration
