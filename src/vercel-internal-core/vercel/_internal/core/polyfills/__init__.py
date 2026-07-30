"""
Polyfills for modern Python features used by internal core.
"""

from ._datetime import UTC
from ._strenum import StrEnum
from ._typing import Self

__all__ = ("Self", "StrEnum", "UTC")
