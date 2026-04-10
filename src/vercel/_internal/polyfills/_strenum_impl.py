import enum as _enum
from typing import Any


class StrEnum(str, _enum.Enum):
    __str__ = str.__str__
    __format__ = str.__format__  # type: ignore[assignment]

    @staticmethod
    def _generate_next_value_(name: str, start: int, count: int, last_values: list[Any]) -> str:
        return name.lower()


__all__ = ("StrEnum",)
