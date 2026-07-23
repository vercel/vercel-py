"""Public root API shared by Vercel Python service packages."""

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

from vercel.internal.core.errors import (
    VercelError,
    VercelServiceOptionsError,
    VercelSessionClosedError,
    VercelSessionError,
)
from vercel.internal.core.session import session

__all__ = [
    "VercelError",
    "VercelServiceOptionsError",
    "VercelSessionClosedError",
    "VercelSessionError",
    "session",
]
