"""Shared Pydantic base and immutable value helpers for Board models."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Annotated, Any, Self, TypeAlias, TypeVar

from pydantic import AfterValidator, BaseModel, ConfigDict, ValidationError, WrapSerializer

from vercel.board._internal.errors import BoardValidationError

_MappingValueT = TypeVar("_MappingValueT")
FrozenMapping: TypeAlias = Annotated[
    Mapping[str, _MappingValueT],
    AfterValidator(MappingProxyType),
    WrapSerializer(lambda value, handler: handler(dict(value))),
]


def _validate_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise BoardValidationError("datetime must be timezone-aware")
    return value.astimezone(UTC)


UtcDatetime: TypeAlias = Annotated[datetime, AfterValidator(_validate_utc_datetime)]


def as_board_validation_error(exc: ValidationError) -> BoardValidationError:
    """Render a Pydantic validation failure as ``BoardValidationError``."""
    for error in exc.errors():
        original = (error.get("ctx") or {}).get("error")
        if isinstance(original, BoardValidationError):
            location = ".".join(str(part) for part in error["loc"])
            message = f"{location}: {original}" if location else str(original)
            return BoardValidationError(message, code=original.code, hint=original.hint)
    details = "; ".join(
        f"{'.'.join(str(part) for part in error['loc']) or 'value'}: {error['msg']}"
        for error in exc.errors()
    )
    return BoardValidationError(details)


class BoardValueModel(BaseModel):
    """Frozen, validated value type for the Vercel Board SDK.

    ``extra="forbid"`` turns misspelled keywords into errors instead of silently
    dropping fields, and validation failures surface as ``BoardValidationError``
    rather than Pydantic's ``ValidationError``.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except ValidationError as exc:
            raise as_board_validation_error(exc) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        try:
            super().__setattr__(name, value)
        except ValidationError as exc:
            raise as_board_validation_error(exc) from exc

    def __delattr__(self, name: str) -> None:
        try:
            super().__delattr__(name)
        except ValidationError as exc:
            raise as_board_validation_error(exc) from exc

    @classmethod
    def model_validate(
        cls,
        obj: Any,
        *,
        strict: bool | None = None,
        extra: Any = None,
        from_attributes: bool | None = None,
        context: Any | None = None,
        by_alias: bool | None = None,
        by_name: bool | None = None,
    ) -> Self:
        try:
            return super().model_validate(
                obj,
                strict=strict,
                extra=extra,
                from_attributes=from_attributes,
                context=context,
                by_alias=by_alias,
                by_name=by_name,
            )
        except ValidationError as exc:
            raise as_board_validation_error(exc) from exc

    @classmethod
    def model_validate_json(
        cls,
        json_data: str | bytes | bytearray,
        *,
        strict: bool | None = None,
        extra: Any = None,
        context: Any | None = None,
        by_alias: bool | None = None,
        by_name: bool | None = None,
    ) -> Self:
        try:
            return super().model_validate_json(
                json_data,
                strict=strict,
                extra=extra,
                context=context,
                by_alias=by_alias,
                by_name=by_name,
            )
        except ValidationError as exc:
            raise as_board_validation_error(exc) from exc

    @classmethod
    def model_validate_strings(
        cls,
        obj: Any,
        *,
        strict: bool | None = None,
        extra: Any = None,
        context: Any | None = None,
        by_alias: bool | None = None,
        by_name: bool | None = None,
    ) -> Self:
        try:
            return super().model_validate_strings(
                obj,
                strict=strict,
                extra=extra,
                context=context,
                by_alias=by_alias,
                by_name=by_name,
            )
        except ValidationError as exc:
            raise as_board_validation_error(exc) from exc


__all__ = [
    "BoardValueModel",
    "FrozenMapping",
    "UtcDatetime",
    "as_board_validation_error",
]
