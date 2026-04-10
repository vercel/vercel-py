"""
Polyfills for modern Python features used internally.
"""

from ._datetime import UTC
from ._strenum import StrEnum
from ._typing import Self

__all__ = ("Self", "StrEnum", "UTC")
