"""Rules for a run's plaintext attributes, ported from `@workflow/world`'s
`attributes.ts`."""

from __future__ import annotations

from collections.abc import Iterable

RESERVED_ATTRIBUTE_KEY_PREFIX = "$"
"""Reserved for framework and library code; user code has to opt in."""

ATTRIBUTE_KEY_MAX_LENGTH = 256
"""Max length of an attribute key, in UTF-16 code units."""

ATTRIBUTE_VALUE_MAX_BYTES = 256
"""Max length of an attribute value, in UTF-8 bytes."""

ATTRIBUTE_MAX_PER_RUN = 64
"""Max number of attributes on a single run, counted after the merge."""


class AttributeValidationError(Exception):
    pass


def _key_length(key: str) -> int:
    """Calculates the number of UTF-16 code units in the given key."""
    return sum(2 if ord(char) > 0xFFFF else 1 for char in key)


def validate_key(key: str, *, allow_reserved: bool = False) -> None:
    if not key:
        raise AttributeValidationError("Attribute key must not be empty")
    length = _key_length(key)
    if length > ATTRIBUTE_KEY_MAX_LENGTH:
        raise AttributeValidationError(
            f"Attribute key length {length} exceeds limit {ATTRIBUTE_KEY_MAX_LENGTH}: {key[:32]}..."
        )
    if not allow_reserved and key.startswith(RESERVED_ATTRIBUTE_KEY_PREFIX):
        raise AttributeValidationError(
            f"Attribute key {key!r} starts with reserved prefix {RESERVED_ATTRIBUTE_KEY_PREFIX!r}."
        )


def validate_value(value: str | None) -> None:
    if value is None:
        # `None` is the unset, and always valid.
        return
    length = len(value.encode())
    if length > ATTRIBUTE_VALUE_MAX_BYTES:
        raise AttributeValidationError(
            f"Attribute value byte length {length} exceeds limit {ATTRIBUTE_VALUE_MAX_BYTES}"
        )


def validate_attribute_changes(
    changes: Iterable[tuple[str, str | None]],
    *,
    existing_keys: Iterable[str] | None = None,
    allow_reserved: bool = False,
) -> None:
    seen = set()
    merged_keys = set() if existing_keys is None else set(existing_keys)
    for key, value in changes:
        validate_key(key, allow_reserved=allow_reserved)
        validate_value(value)
        if key in seen:
            raise AttributeValidationError(
                f"Attribute key {key!r} appears more than once in the same batch"
            )
        seen.add(key)
        if value is None:
            merged_keys.discard(key)
        else:
            merged_keys.add(key)

    if len(merged_keys) > ATTRIBUTE_MAX_PER_RUN:
        raise AttributeValidationError(
            f"Run attribute count would exceed limit: {len(merged_keys)} > {ATTRIBUTE_MAX_PER_RUN} "
        )


def apply_attribute_changes(
    existing: dict[str, str] | None, changes: Iterable[tuple[str, str | None]]
) -> dict[str, str]:
    """The post-merge map. Does not mutate *existing*."""
    merged = dict(existing or {})
    for key, value in changes:
        if value is None:
            merged.pop(key, None)
        else:
            merged[key] = value
    return merged
