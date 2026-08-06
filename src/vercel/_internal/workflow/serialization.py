"""Payload encoding for workflow runs, steps and hooks.

The wire format is the one `@workflow/core` defines in
`src/serialization-format.ts`: a 4-byte ASCII format tag followed by the
payload. The only format this SDK writes is ``devl`` — UTF-8
`devalue.stringify` output — so a payload written here parses with
`devalue.parse` in JavaScript and vice versa.

``encr`` — an encrypted payload — is read too.

The remaining tags are recognized so that an envelope written by the
TypeScript SDK is reported as unsupported by name instead of as corrupt data:
`encp` is a different (X25519) construction, and `gzip`/`zstd` wrap another
prefixed payload.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from vercel._internal import devalue

from . import encryption, serde

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


def _reduce_writable_stream(value: Any) -> Any:
    """devalue reducer for a run's stream. Falsy declines the value.

    Rides `@workflow/core`'s own ``WritableStream`` tag rather than the
    ``Instance`` rail :mod:`.serde` uses for new types, because a stream is not
    a new type: the TypeScript SDK already defines this tag and revives it into
    a real `WritableStream` against the same ``(runId, name)``. A Python
    workflow can therefore hand one of its streams to a JavaScript peer.
    """
    from . import streams

    if isinstance(value, streams.WorkflowStreamHandle | streams.WorkflowStreamWriter):
        return {"name": value.name, "runId": value.run_id}
    return False


def _revive_writable_stream(value: Any) -> Any:
    """devalue reviver for the ``WritableStream`` tag."""
    from . import runtime

    if not isinstance(value, dict) or not isinstance(value.get("name"), str):
        raise ValueError(f"malformed WritableStream payload: {value!r}")
    run_id = value.get("runId")
    return runtime.open_writable(run_id if isinstance(run_id, str) else None, value["name"])


# `serde` owns the custom-type rail; streams sit alongside it on a tag
# `@workflow/core` composes itself.
REDUCERS: dict[str, Any] = {**serde.REDUCERS, "WritableStream": _reduce_writable_stream}
REVIVERS: dict[str, Any] = {**serde.REVIVERS, "WritableStream": _revive_writable_stream}


def dehydrate(value: Any) -> bytes:
    """Encode *value* as a ``devl``-prefixed devalue payload."""
    try:
        return DEVALUE_V1 + devalue.stringify(value, REDUCERS).encode()
    except devalue.DevalueError as error:
        at = f" at {error.path}" if error.path else ""
        raise SerializationError(
            f"Cannot serialize value{at}: {error}.{serde.registration_hint(error.value)}"
        ) from error
    except (ValueError, TypeError) as error:
        # A registered serializer failed. `serde` has already named the class,
        # so this only puts the codec-level frame behind a typed error.
        raise SerializationError(f"Cannot serialize value: {error}") from error


def is_encrypted(data: Any) -> bool:
    """Whether reading *data* would need the run's key.

    Lets a caller skip resolving a key it does not need, which can cost an API
    call. Takes ``Any`` for the same reason :func:`hydrate` does, and answers
    ``False`` for anything that is not a payload at all.
    """
    if not isinstance(data, bytes | bytearray | memoryview):
        return False
    return bytes(data[:FORMAT_PREFIX_LENGTH]) == ENCRYPTED


def hydrate(data: Any, *, what: str, key: bytes | None = None) -> Any:
    """Decode a format-prefixed payload written by either SDK.

    Takes ``Any`` because it is the boundary that checks: a payload field can
    hold whatever the backend put there — an unresolved remote reference, or
    the ``'[Circular]'`` marker a `run_created` response carries in place of
    the input — and this is where that is turned into a readable error.

    *what* names the payload in the error message — it is the only context
    the caller has that would help someone reading the traceback.

    *key* is the run's 32-byte key, needed only for an ``encr`` payload;
    :func:`is_encrypted` says in advance whether one will be asked for.
    """
    if not isinstance(data, bytes | bytearray | memoryview):
        raise SerializationError(f"{what} is not serialized data: {type(data).__name__}")
    data = bytes(data)
    if len(data) < FORMAT_PREFIX_LENGTH:
        raise SerializationError(f"{what} is too short to carry a format prefix: {len(data)} bytes")

    prefix, payload = data[:FORMAT_PREFIX_LENGTH], data[FORMAT_PREFIX_LENGTH:]
    if prefix == DEVALUE_V1:
        try:
            return devalue.parse(payload.decode(), REVIVERS)
        except (devalue.DevalueError, ValueError, TypeError) as error:
            raise SerializationError(f"Cannot deserialize {what}: {error}") from error
    if prefix == ENCRYPTED:
        if key is None:
            raise SerializationError(
                f"{what} is encrypted, and no key was resolved for the run that wrote it"
            )
        try:
            plaintext = encryption.open_envelope(key, payload)
        except (encryption.DecryptionError, ValueError) as error:
            raise SerializationError(f"Cannot decrypt {what}: {error}") from error
        # The plaintext carries its own prefix, normally `devl`.
        return hydrate(plaintext, what=what, key=key)
    if prefix in KNOWN_FORMATS:
        raise SerializationError(
            f"{what} uses the {prefix.decode()!r} format, which this SDK cannot read"
        )
    raise SerializationError(f"{what} has an unknown serialization format: {prefix!r}")


def _is_argument_object(value: Any) -> bool:
    """Whether :func:`call_arguments` would read *value* as the kwargs object.

    A dict with a non-string key cannot be a JavaScript object — it is a
    devalue `Map`, so a positional argument. `dehydrate` refuses to write one,
    so that case only arises reading a payload a JavaScript caller wrote.
    """
    return isinstance(value, dict) and all(isinstance(key, str) for key in value)


def argument_array(args: Sequence[Any], kwargs: Mapping[str, Any]) -> list[Any]:
    """The argument array a call is recorded as.

    `@workflow/core` records a call as the array of its positional arguments
    and spreads it back into the callee (`workflowFn(...args)`). A Python call
    also carries keyword arguments, and the array has one slot per value with
    nowhere to name anything — so the keyword arguments ride in a single object
    appended after the positional ones. Takes the two halves
    `core._bind_arguments` produced, not a raw call:

    ================  ==========================  ==========================
    positional        keyword                     array
    ================  ==========================  ==========================
    ``()``            ``{}``                      ``[]``
    ``(21,)``         ``{}``                      ``[21]``
    ``()``            ``{"amount": 21}``          ``[{"amount": 21}]``
    ``(21,)``         ``{"currency": "usd"}``     ``[21, {"currency": "usd"}]``
    ``({"a": 1},)``   ``{}``                      ``[{"a": 1}, {}]``
    ================  ==========================  ==========================

    :func:`call_arguments` reads that back by taking a trailing object as the
    keyword arguments. The one call that rule would misread is one whose last
    *positional* argument is itself an object, so the encoder appends an empty
    object in that case and the rule holds unconditionally. A JavaScript callee
    receives one extra ``{}`` argument there, which is harmless.

    Both ends of the table are what TypeScript writes for the same call:
    ``[{"amount": 21}]`` is `start(wf, [{amount: 21}])`, the object argument a
    JavaScript callee expects, and ``[]`` is the empty array TS writes for a
    call with no arguments rather than an empty object a JavaScript callee
    would receive as a stray parameter.
    """
    encoded = list(args)
    if kwargs:
        encoded.append(dict(kwargs))
    elif encoded and _is_argument_object(encoded[-1]):
        encoded.append({})
    return encoded


def call_arguments(args: Any, *, what: str) -> tuple[list[Any], dict[str, Any]]:
    """Read an argument array back, the inverse of :func:`argument_array`.

    Returns the positional arguments and the keyword arguments, to be splatted
    into the workflow or step function. An array a JavaScript caller wrote
    carries positional arguments only — except for the idiomatic single-object
    call, which arrives as keyword arguments and so lands on a Python callee
    declaring the matching parameters.
    """
    if not isinstance(args, list):
        raise SerializationError(f"{what} is not an argument array: {type(args).__name__}")
    if args and _is_argument_object(args[-1]):
        return args[:-1], args[-1]
    return args, {}


def step_arguments(args: Sequence[Any], kwargs: Mapping[str, Any]) -> dict[str, Any]:
    """The object `@workflow/core` records a *step* call as.

    A step's arguments are wrapped where a workflow's are not. TS puts
    ``closureVars`` and ``thisVal`` in the same object, neither of which has a
    Python analogue; its reader defaults both when they are absent.
    """
    return {"args": argument_array(args, kwargs)}


def step_call_arguments(data: Any, *, what: str) -> tuple[list[Any], dict[str, Any]]:
    """Read a step's recorded call back, the inverse of :func:`step_arguments`."""
    if not isinstance(data, dict):
        raise SerializationError(f"{what} is not a step argument object: {type(data).__name__}")
    return call_arguments(data.get("args"), what=what)
