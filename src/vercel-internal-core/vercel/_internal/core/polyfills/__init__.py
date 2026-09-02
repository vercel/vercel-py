"""
Polyfills for modern Python features used by internal core.
"""

from ._datetime import UTC
from ._strenum import StrEnum
from ._typing import Buffer, Self

__all__ = ("Buffer", "Self", "StrEnum", "UTC")
