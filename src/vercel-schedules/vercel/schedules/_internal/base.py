"""Shared pydantic base for every Schedules value type."""

from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError

from vercel.schedules._internal.errors import SchedulesValidationError


def as_schedules_error(exc: ValidationError) -> SchedulesValidationError:
    """Render a pydantic failure as the SDK's documented validation error."""
    for error in exc.errors():
        original = (error.get("ctx") or {}).get("error")
        if isinstance(original, SchedulesValidationError):
            location = ".".join(str(part) for part in error["loc"])
            return SchedulesValidationError(f"{location} {original}" if location else str(original))
    details = "; ".join(
        f"{'.'.join(str(part) for part in error['loc']) or 'value'}: {error['msg']}"
        for error in exc.errors()
    )
    return SchedulesValidationError(details)


class SchedulesModel(BaseModel):
    """Frozen, validated value type.

    `extra="forbid"` so a misspelled keyword is an error rather than a silently
    dropped field, and validation failures surface as `SchedulesValidationError`
    rather than pydantic's own error, which spares callers an import of pydantic.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except ValidationError as exc:
            raise as_schedules_error(exc) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        try:
            super().__setattr__(name, value)
        except ValidationError as exc:
            raise as_schedules_error(exc) from exc


__all__ = ["SchedulesModel", "as_schedules_error"]
