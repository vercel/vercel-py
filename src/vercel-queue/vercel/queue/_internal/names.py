from __future__ import annotations

import re
from collections import UserString

VQS_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


class SanitizedName(UserString):
    """A VQS-safe name that should not be sanitized again."""

    __slots__ = ()

    def __init__(self, value: str) -> None:
        if not value:
            raise ValueError("name must be a non-empty string")
        if not _is_valid_vqs_name(value):
            raise ValueError(f"Invalid queue name: {value!r}; must match {VQS_NAME_PATTERN}")
        super().__init__(value)


def _is_valid_vqs_name(name: str) -> bool:
    return bool(VQS_NAME_PATTERN.fullmatch(name))


# Keep this in sync with @vercel/build-utils:lambda.ts/sanitizeConsumerName
# https://raw.githubusercontent.com/vercel/vercel/refs/heads/main/packages/build-utils/src/lambda.ts
def _sanitize_name(name: str, *, fallback: str) -> str:
    result = ""
    for char in name:
        if char == "_":
            result += "__"
        elif char == ".":
            result += "_D"
        elif char == "/":
            result += "_S"
        elif char.isascii() and (char.isalnum() or char == "-"):
            result += char
        else:
            result += f"_{ord(char):02X}"
    return result or fallback


def sanitize_name(name: str | SanitizedName, *, fallback: str = "queue") -> str:
    """Return a valid VQS queue name for arbitrary input.

    Plain strings are reversibly encoded, including underscores. Use
    ``SanitizedName`` for values that are already VQS-safe and must not be
    encoded again.
    """
    if isinstance(name, SanitizedName):
        return str(name)
    return _sanitize_name(name, fallback=fallback)


def validate_name(name: object, *, field: str = "name") -> str:
    name = getattr(name, "name", name)
    if isinstance(name, SanitizedName):
        return str(name)
    if not isinstance(name, str) or not name:
        raise ValueError(f"{field} must be a non-empty string")
    if not _is_valid_vqs_name(name):
        raise ValueError(f"Invalid queue {field}: {name!r}; must match {VQS_NAME_PATTERN}")
    return name


def validate_topic_name(topic: object) -> str:
    return validate_name(topic, field="topic")


def validate_subscription_pattern(topic: str) -> str:
    if topic == "*":
        return topic
    topic_name = topic.removesuffix("*")
    if not _is_valid_vqs_name(topic_name):
        raise ValueError(f"Invalid queue topic: {topic!r}; must match {VQS_NAME_PATTERN}")
    return topic


def normalize_name(
    name: str | SanitizedName,
    *,
    fallback: str = "queue",
    field: str = "name",
) -> str:
    if isinstance(name, SanitizedName):
        return str(name)
    if not name:
        raise ValueError(f"{field} must be a non-empty string")
    return sanitize_name(name, fallback=fallback)


# Only add public symbols to __all__; internal helpers must stay unexported.
__all__ = (
    "SanitizedName",
    "normalize_name",
    "sanitize_name",
    "validate_name",
    "validate_subscription_pattern",
)
