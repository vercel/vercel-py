"""Shared errors for the experimental SDK surface."""


class VercelError(Exception):
    """Base error for all `vercel.unstable` exceptions."""


class VercelSessionError(VercelError):
    """Base error for unstable SDK session failures."""


class VercelSessionClosedError(VercelSessionError):
    """Raised when code uses an SDK session after it has been closed."""


class VercelServiceOptionsError(VercelSessionError):
    """Raised when SDK session service options are invalid."""
