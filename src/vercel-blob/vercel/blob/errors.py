"""Public errors raised by the Vercel Blob file API."""

from __future__ import annotations

import httpx


class BlobError(Exception):
    """Base class for Blob failures."""

    def __init__(self, message: str, *, response: httpx.Response | None = None) -> None:
        super().__init__(f"Vercel Blob: {message}")
        self.response = response


class BlobCredentialsError(BlobError):
    """Blob credentials are missing or invalid."""


class BlobNotFoundError(BlobError, FileNotFoundError):
    """The requested Blob object does not exist."""

    def __init__(self, *, response: httpx.Response | None = None) -> None:
        super().__init__("The requested blob does not exist", response=response)


class BlobAccessError(BlobError, PermissionError):
    """The credentials cannot access the requested object."""

    def __init__(self, *, response: httpx.Response | None = None) -> None:
        super().__init__(
            "Access denied, please provide a valid token for this resource.",
            response=response,
        )


class BlobStoreNotFoundError(BlobError):
    """The configured Blob store does not exist."""

    def __init__(self, *, response: httpx.Response | None = None) -> None:
        super().__init__("This store does not exist.", response=response)


class BlobStoreSuspendedError(BlobError):
    """The configured Blob store is suspended."""

    def __init__(self, *, response: httpx.Response | None = None) -> None:
        super().__init__("This store has been suspended.", response=response)


class BlobContentTypeNotAllowedError(BlobError):
    """The store rejected the requested content type."""

    def __init__(
        self,
        message: str,
        *,
        response: httpx.Response | None = None,
    ) -> None:
        super().__init__(f"Content type mismatch, {message}.", response=response)


class BlobPathnameMismatchError(BlobError):
    """A client token does not permit the requested pathname."""

    def __init__(
        self,
        message: str,
        *,
        response: httpx.Response | None = None,
    ) -> None:
        super().__init__(
            f"Pathname mismatch, {message}. "
            "Check the pathname used in upload() or put() "
            "matches the one from the client token.",
            response=response,
        )


class BlobFileTooLargeError(BlobError):
    """The object exceeds a configured size constraint."""

    def __init__(
        self,
        message: str,
        *,
        response: httpx.Response | None = None,
    ) -> None:
        super().__init__(f"File is too large, {message}.", response=response)


class BlobServiceNotAvailable(BlobError):
    """The Blob service is temporarily unavailable."""

    def __init__(self, *, response: httpx.Response | None = None) -> None:
        super().__init__(
            "The blob service is currently not available. Please try again.",
            response=response,
        )


class BlobServiceRateLimited(BlobError):
    """The Blob service rate limited the request."""

    def __init__(
        self,
        seconds: int | None = None,
        *,
        response: httpx.Response | None = None,
    ) -> None:
        retry = f" - try again in {seconds} seconds" if seconds else ""
        super().__init__(
            f"Too many requests please lower the number of concurrent requests{retry}.",
            response=response,
        )
        self.retry_after = seconds or 0


class BlobUnknownError(BlobError):
    """The service returned an unrecognized failure."""

    def __init__(self, *, response: httpx.Response | None = None) -> None:
        super().__init__(
            "Unknown error, please visit https://vercel.com/help.",
            response=response,
        )


class BlobPreconditionFailedError(BlobError):
    """The object changed while a pinned reader was active."""


class BlobStreamError(BlobError, OSError):
    """A Blob stream response was malformed or unusable."""


__all__ = [
    "BlobAccessError",
    "BlobContentTypeNotAllowedError",
    "BlobCredentialsError",
    "BlobError",
    "BlobFileTooLargeError",
    "BlobNotFoundError",
    "BlobPathnameMismatchError",
    "BlobPreconditionFailedError",
    "BlobServiceNotAvailable",
    "BlobServiceRateLimited",
    "BlobStoreNotFoundError",
    "BlobStoreSuspendedError",
    "BlobStreamError",
    "BlobUnknownError",
]
