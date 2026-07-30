"""Public shared error types."""

from vercel._internal.core.errors import (
    VercelError,
    VercelServiceOptionsError,
    VercelSessionClosedError,
    VercelSessionError,
)

__all__ = [
    "VercelError",
    "VercelServiceOptionsError",
    "VercelSessionClosedError",
    "VercelSessionError",
]
