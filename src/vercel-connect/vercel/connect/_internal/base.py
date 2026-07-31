"""Shared pydantic base for every Connect value type."""

from collections.abc import Iterable, Mapping
from typing import Annotated, Any, TypeAlias

from pydantic import BaseModel, BeforeValidator, ConfigDict, ValidationError

from vercel.connect._internal.errors import ConnectValidationError

StringContainer: TypeAlias = list[str] | tuple[str, ...] | set[str] | frozenset[str]
"""A container of strings, spelled as concrete types so `str` is not one of them.

`str` satisfies `Sequence[str]`, so a parameter typed that way accepts
`scopes="repo:read"` and expands it to one entry per character. Naming the
containers instead makes a type checker reject that, and unlike a protocol this is
an ordinary annotation, so the same alias types a pydantic field.

Static only, and deliberately advisory: the runtime accepts any iterable, so a
caller holding a `Sequence[str]` sees a type error suggesting `list(...)` rather
than a failure.
"""


def coerce_string_container(value: Any) -> Any:
    """Accept any container of strings and store it as a tuple.

    A bare string is rejected rather than expanded: `scopes="repo:read"` would
    otherwise become nine one-character scopes, which is a wrong request rather than
    an error. `StringContainer` rejects it statically; this is the runtime half, for
    untyped callers.

    Anything else iterable becomes a tuple, so a stored value is immutable and
    hashable, and a caller who ignores the type error still gets what they meant.
    """
    if isinstance(value, (str, bytes, bytearray, Mapping)):
        raise ConnectValidationError(
            f"must be a container of strings, not {type(value).__name__}; "
            f"pass [{value!r}] for one value"
            if isinstance(value, str)
            else f"must be a container of strings, not {type(value).__name__}"
        )
    return tuple(value) if isinstance(value, Iterable) else value


StringField: TypeAlias = Annotated[StringContainer, BeforeValidator(coerce_string_container)]
"""A `StringContainer` as a model field: any container in, a tuple stored."""


def reject_bool(value: Any) -> Any:
    """Keep `True` out of a numeric field, where pydantic would read it as 1."""
    if isinstance(value, bool):
        raise ConnectValidationError("must be a number of seconds or a timedelta, not a bool")
    return value


def as_connect_error(exc: ValidationError) -> ConnectValidationError:
    """Render a pydantic failure as the SDK's documented validation error.

    A guard that raised `ConnectValidationError` itself is unwrapped and re-raised
    verbatim, so its remedy survives; anything else is reported with the field path
    pydantic produced.
    """
    for error in exc.errors():
        original = (error.get("ctx") or {}).get("error")
        if isinstance(original, ConnectValidationError):
            location = ".".join(str(part) for part in error["loc"])
            return ConnectValidationError(f"{location} {original}" if location else str(original))
    details = "; ".join(
        f"{'.'.join(str(part) for part in error['loc']) or 'value'}: {error['msg']}"
        for error in exc.errors()
    )
    return ConnectValidationError(details)


class ConnectModel(BaseModel):
    """Frozen, validated value type.

    `extra="forbid"` so a misspelled keyword is an error rather than a silently
    dropped field, and validation failures surface as `ConnectValidationError`
    rather than pydantic's own error, which is the documented contract and spares
    callers an import of pydantic.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except ValidationError as exc:
            raise as_connect_error(exc) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        # Assigning to a frozen model is a pydantic ValidationError; report it the
        # same way as every other misuse of a Connect value.
        try:
            super().__setattr__(name, value)
        except ValidationError as exc:
            raise as_connect_error(exc) from exc


__all__ = [
    "ConnectModel",
    "StringContainer",
    "StringField",
    "as_connect_error",
    "coerce_string_container",
    "reject_bool",
]
