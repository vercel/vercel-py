"""Base errors for the unstable SDK surface."""

from __future__ import annotations


class VercelError(Exception):
    """Base class for errors raised by `vercel.unstable`."""


class SessionLifecycleError(VercelError):
    """Base class for unstable session lifecycle errors."""


class SessionClosedError(SessionLifecycleError):
    """Raised when a closed unstable session is reused."""


class SettingsError(VercelError):
    """Base class for unstable settings-loading errors."""


class SettingsSourceError(SettingsError):
    """Raised when a settings source cannot produce a value."""

    def __init__(self, message: str, *, field: str, source: str) -> None:
        super().__init__(message)
        self.field = field
        self.source = source


class SettingsValidationError(SettingsError):
    """Raised when a resolved settings value is invalid."""

    def __init__(self, message: str, *, field: str, source: str) -> None:
        super().__init__(message)
        self.field = field
        self.source = source
