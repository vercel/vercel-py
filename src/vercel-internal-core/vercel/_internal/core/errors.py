"""Shared errors for Vercel Python services."""


class VercelError(Exception):
    """Base error for Vercel Python SDK failures."""


class VercelSessionError(VercelError):
    """Base error for SDK session failures."""


class VercelSessionClosedError(VercelSessionError):
    """Raised when code uses an SDK session after it has been closed."""


class VercelServiceOptionsError(VercelSessionError):
    """Raised when SDK session service options are invalid."""
