"""Shared configuration for immutable public Sandbox values."""

from typing import Any

from pydantic import BaseModel, ConfigDict


class SandboxValueModel(BaseModel):
    """Validate public value objects while preserving their frozen fields."""

    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
