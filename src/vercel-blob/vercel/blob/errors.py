"""Public errors raised by the Vercel Blob SDK."""

from __future__ import annotations


class BlobError(Exception):
    """Base class for Blob failures."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        code: str | None = None,
    ) -> None:
        super().__init__(f"Vercel Blob: {message}")
        self.status_code = status_code
        self.code = code


class BlobCredentialsError(BlobError):
    """Blob credentials are missing or invalid."""


class BlobNotFoundError(BlobError, FileNotFoundError):
    """The requested Blob object does not exist."""

    def __init__(
        self,
        message: str = "The requested blob does not exist",
        *,
        status_code: int = 404,
        code: str = "blob_not_found",
    ) -> None:
        super().__init__(message, status_code=status_code, code=code)


class BlobAccessError(BlobError, PermissionError):
    """The credentials cannot access the requested object."""

    def __init__(
        self,
        message: str = "Access denied, please provide a valid token for this resource.",
        *,
        status_code: int = 403,
        code: str = "forbidden",
    ) -> None:
        super().__init__(message, status_code=status_code, code=code)


class BlobStoreNotFoundError(BlobError):
    """The configured Blob store does not exist."""

    def __init__(
        self,
        message: str = "This store does not exist.",
        *,
        status_code: int = 400,
        code: str = "store_not_found",
    ) -> None:
        super().__init__(message, status_code=status_code, code=code)


class BlobStoreSuspendedError(BlobError):
    """The configured Blob store is suspended."""

    def __init__(
        self,
        message: str = "This store has been suspended.",
        *,
        status_code: int = 400,
        code: str = "store_suspended",
    ) -> None:
        super().__init__(message, status_code=status_code, code=code)


class BlobContentTypeNotAllowedError(BlobError):
    """The store rejected the requested content type."""

    def __init__(
        self,
        message: str = "Content type mismatch",
        *,
        status_code: int = 400,
        code: str = "content_type_not_allowed",
    ) -> None:
        super().__init__(
            f"Content type mismatch, {message}.",
            status_code=status_code,
            code=code,
        )


class BlobPathnameMismatchError(BlobError):
    """A client token does not permit the requested pathname."""

    def __init__(
        self,
        message: str = "Pathname mismatch",
        *,
        status_code: int = 400,
        code: str = "client_token_pathname_mismatch",
    ) -> None:
        super().__init__(
            f"Pathname mismatch, {message}. "
            "Check the pathname used in upload() or put() "
            "matches the one from the client token.",
            status_code=status_code,
            code=code,
        )


class BlobFileTooLargeError(BlobError):
    """The object exceeds a configured size constraint."""

    def __init__(
        self,
        message: str = "File is too large",
        *,
        status_code: int = 400,
        code: str = "file_too_large",
    ) -> None:
        super().__init__(
            f"File is too large, {message}.",
            status_code=status_code,
            code=code,
        )


class BlobServiceNotAvailable(BlobError):
    """The Blob service is temporarily unavailable."""

    def __init__(
        self,
        message: str = "The blob service is currently not available. Please try again.",
        *,
        status_code: int = 503,
        code: str = "service_unavailable",
    ) -> None:
        super().__init__(message, status_code=status_code, code=code)


class BlobServiceRateLimited(BlobError):
    """The Blob service rate limited the request."""

    def __init__(
        self,
        seconds: int | None = None,
        *,
        status_code: int = 429,
        code: str = "rate_limited",
    ) -> None:
        retry = f" - try again in {seconds} seconds" if seconds else ""
        super().__init__(
            f"Too many requests please lower the number of concurrent requests{retry}.",
            status_code=status_code,
            code=code,
        )
        self.retry_after = seconds or 0


class BlobUnknownError(BlobError):
    """The service returned an unrecognized failure."""

    def __init__(
        self,
        message: str = "Unknown error, please visit https://vercel.com/help.",
        *,
        status_code: int | None = None,
        code: str | None = None,
    ) -> None:
        super().__init__(message, status_code=status_code, code=code)


class BlobPreconditionFailedError(BlobError):
    """The object changed while an operation was in flight."""

    def __init__(
        self,
        message: str = "Blob precondition failed",
        *,
        status_code: int = 412,
        code: str = "precondition_failed",
    ) -> None:
        super().__init__(message, status_code=status_code, code=code)


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
