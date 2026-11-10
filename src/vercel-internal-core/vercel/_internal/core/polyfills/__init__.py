"""Polyfills for modern Python features used by internal core."""

from ._datetime import UTC
from ._typing import Buffer

__all__ = ("Buffer", "UTC")
