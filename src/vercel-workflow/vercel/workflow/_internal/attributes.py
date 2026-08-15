"""Rules for a run's plaintext attributes.

A port of `@workflow/world`'s `attributes.ts`, kept as a leaf module so both
the authoring API (which validates before writing anything) and `LocalWorld`
(which validates again before materializing a write) can use it.

The wording of the errors is part of the contract, not decoration: the same
rules are enforced by workflow-server and by the TypeScript SDK, and the
conformance suite asserts on the substrings below, so a caller sees one message
for a broken rule regardless of which side caught it.
"""

from __future__ import annotations

import json
from collections.abc import Iterable

RESERVED_ATTRIBUTE_KEY_PREFIX = "$"
"""Reserved for framework and library code -- telemetry, agent metadata, and
whatever the platform adds next. User code has to opt in explicitly."""

ATTRIBUTE_KEY_MAX_LENGTH = 256
"""Max length of an attribute key, in characters."""

ATTRIBUTE_VALUE_MAX_BYTES = 256
"""Max length of an attribute value, in UTF-8 bytes."""

ATTRIBUTE_MAX_PER_RUN = 64
"""Max number of attributes on a single run, counted after the merge."""


class AttributeValidationError(Exception):
    """A key or value that breaks one of the rules above.

    A plain exception rather than a `FatalError`, so the caller decides: the
    authoring API re-raises it as a `FatalError` the workflow body can catch,
    and a World may answer it as a 400 instead.
    """


def validate_attribute_key(key: str, *, allow_reserved: bool = False) -> None:
    if not isinstance(key, str):
        raise AttributeValidationError(f"Attribute key must be a string, got {type(key).__name__}")
    if not key:
        raise AttributeValidationError("Attribute key must not be empty")
    if len(key) > ATTRIBUTE_KEY_MAX_LENGTH:
        raise AttributeValidationError(
            f"Attribute key length {len(key)} exceeds limit "
            f"{ATTRIBUTE_KEY_MAX_LENGTH}: {json.dumps(key[:32])}…"
        )
    if not allow_reserved and key.startswith(RESERVED_ATTRIBUTE_KEY_PREFIX):
        raise AttributeValidationError(
            f"Attribute key {json.dumps(key)} starts with reserved prefix "
            f'"{RESERVED_ATTRIBUTE_KEY_PREFIX}" — that namespace is reserved for '
            "framework/library code. Set allow_reserved_attributes=True only if "
            "your caller is framework-level."
        )


def validate_attribute_value(value: str | None) -> None:
    """`None` is the unset, and always valid."""
    if value is None:
        return
    if not isinstance(value, str):
        raise AttributeValidationError(
            f"Attribute value must be a string or None, got {type(value).__name__}"
        )
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
    """Check a whole batch, raising on the first violation.

    Pass *existing_keys* to count the cap against the real post-merge total: an
    upsert of a key the run already has adds nothing, and unsetting one it does
    not have removes nothing. Without them every upsert counts as an add, which
    can reject an update at the cap -- conservative, and the same shape
    `@workflow/world` falls back to. The authoritative count is the World's,
    which does have the run in hand.
    """
    existing = None if existing_keys is None else set(existing_keys)
    seen: set[str] = set()
    net_adds = 0
    net_deletes = 0
    for key, value in changes:
        validate_attribute_key(key, allow_reserved=allow_reserved)
        validate_attribute_value(value)
        if key in seen:
            raise AttributeValidationError(
                f"Attribute key {json.dumps(key)} appears more than once in the same batch"
            )
        seen.add(key)
        if value is not None:
            if existing is None or key not in existing:
                net_adds += 1
        elif existing is None or key in existing:
            net_deletes += 1
    post_merge = (0 if existing is None else len(existing)) + net_adds - net_deletes
    if post_merge > ATTRIBUTE_MAX_PER_RUN:
        raise AttributeValidationError(
            f"Run attribute count would exceed limit {ATTRIBUTE_MAX_PER_RUN} "
            f"(post-merge {post_merge})"
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
