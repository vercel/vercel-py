"""Shared configuration for Sandbox data models."""

from typing import Any

from pydantic import BaseModel, ConfigDict


# Explicit **data constructors keep Pydantic's mypy plugin from synthesizing
# empty base signatures for subclass constructors that call super().__init__.
class SandboxModel(BaseModel):
    """Validate data while allowing live runtime state to change."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)


class SandboxValueModel(SandboxModel):
    """Validate value objects while preserving their frozen fields."""

    model_config = ConfigDict(frozen=True)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
