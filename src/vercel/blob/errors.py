class BlobError(Exception):
    """Base class for Vercel Blob SDK errors."""

    def __init__(self, message: str) -> None:
        super().__init__(f"Vercel Blob: {message}")


class BlobAccessError(BlobError):
    def __init__(self) -> None:
        super().__init__("Access denied, please provide a valid token for this resource.")


class BlobContentTypeNotAllowedError(BlobError):
    def __init__(self, message: str) -> None:
        super().__init__(f"Content type mismatch, {message}.")


class BlobPathnameMismatchError(BlobError):
    def __init__(self, message: str) -> None:
        super().__init__(
            f"Pathname mismatch, {message}. Check the pathname used in upload() or put() matches the one from the client token."
        )


class BlobClientTokenExpiredError(BlobError):
    def __init__(self) -> None:
        super().__init__("Client token has expired.")


class BlobFileTooLargeError(BlobError):
    def __init__(self, message: str) -> None:
        super().__init__(f"File is too large, {message}.")


class BlobStoreNotFoundError(BlobError):
    def __init__(self) -> None:
        super().__init__("This store does not exist.")


class BlobStoreSuspendedError(BlobError):
    def __init__(self) -> None:
        super().__init__("This store has been suspended.")


class BlobUnknownError(BlobError):
    def __init__(self) -> None:
        super().__init__("Unknown error, please visit https://vercel.com/help.")


class BlobNotFoundError(BlobError):
    def __init__(self) -> None:
        super().__init__("The requested blob does not exist")


class BlobServiceNotAvailable(BlobError):
    def __init__(self) -> None:
        super().__init__("The blob service is currently not available. Please try again.")


class BlobServiceRateLimited(BlobError):
    def __init__(self, seconds: int | None = None) -> None:
        retry = f" - try again in {seconds} seconds" if seconds else ""
        super().__init__(
            f"Too many requests please lower the number of concurrent requests{retry}."
        )
        self.retry_after: int = seconds or 0


class BlobRequestAbortedError(BlobError):
    def __init__(self) -> None:
        super().__init__("The request was aborted.")
