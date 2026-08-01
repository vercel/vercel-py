"""Payload encoding for workflow runs, steps and hooks.

The wire format is the one `@workflow/core` defines in
`src/serialization-format.ts`: a 4-byte ASCII format tag followed by the
payload. The only format this SDK writes is ``devl`` — UTF-8
`devalue.stringify` output — so a payload written here parses with
`devalue.parse` in JavaScript and vice versa.

The remaining tags are recognized so that an envelope written by the
TypeScript SDK is reported as unsupported by name instead of as corrupt
data. None of them are implemented: `encr`/`encp` need the run's key
material, and `gzip`/`zstd` wrap another prefixed payload.
"""

from __future__ import annotations

from typing import Any

from vercel._internal import devalue

FORMAT_PREFIX_LENGTH = 4

DEVALUE_V1 = b"devl"
"""`devalue.stringify` output, UTF-8 encoded."""

ENCRYPTED = b"encr"
"""Symmetrically encrypted; the plaintext carries its own format prefix."""

SEALED = b"encp"
"""Sealed to a run's X25519 public key; likewise prefixed once opened."""

GZIP = b"gzip"
ZSTD = b"zstd"

KNOWN_FORMATS = (DEVALUE_V1, ENCRYPTED, SEALED, GZIP, ZSTD)


class SerializationError(RuntimeError):
    """A payload could not be encoded, or arrived in a format we cannot read."""


def dehydrate(value: Any) -> bytes:
    """Encode *value* as a ``devl``-prefixed devalue payload."""
    try:
        return DEVALUE_V1 + devalue.stringify(value).encode()
    except devalue.DevalueError as error:
        at = f" at {error.path}" if error.path else ""
        raise SerializationError(f"Cannot serialize value{at}: {error}") from error


def hydrate(data: Any, *, what: str) -> Any:
    """Decode a format-prefixed payload written by either SDK.

    Takes ``Any`` because it is the boundary that checks: a payload field can
    hold whatever the backend put there — an unresolved remote reference, or
    the ``'[Circular]'`` marker a `run_created` response carries in place of
    the input — and this is where that is turned into a readable error.

    *what* names the payload in the error message — it is the only context
    the caller has that would help someone reading the traceback.
    """
    if not isinstance(data, bytes | bytearray | memoryview):
        raise SerializationError(f"{what} is not serialized data: {type(data).__name__}")
    data = bytes(data)
    if len(data) < FORMAT_PREFIX_LENGTH:
        raise SerializationError(f"{what} is too short to carry a format prefix: {len(data)} bytes")

    prefix, payload = data[:FORMAT_PREFIX_LENGTH], data[FORMAT_PREFIX_LENGTH:]
    if prefix == DEVALUE_V1:
        try:
            return devalue.parse(payload.decode())
        except (devalue.DevalueError, ValueError, TypeError) as error:
            raise SerializationError(f"Cannot deserialize {what}: {error}") from error
    if prefix in KNOWN_FORMATS:
        raise SerializationError(
            f"{what} uses the {prefix.decode()!r} format, which this SDK cannot read"
        )
    raise SerializationError(f"{what} has an unknown serialization format: {prefix!r}")
