from .base import SDownloadError


class CommunicationError(SDownloadError):
    """Base class for all communication and protocol-related errors."""

    def __init__(
        self, message: str, url: str | None = None, original: Exception | None = None
    ):
        super().__init__(message, original=original)
        self.url = url


class DownloadRequestError(CommunicationError):
    """Raised when a download request fails (e.g. timeout, connection error)."""

    def __init__(self, url: str, original: Exception):
        super().__init__(
            f"Failed to request {url}: {original}",
            url=url,
            original=original,
        )


class NetworkError(DownloadRequestError):
    """Raised when a network-level error occurs (e.g. DNS failure, connection refused)."""

    pass


class ProtocolError(DownloadRequestError):
    """Raised when a protocol-level error occurs (e.g. proxy issue, SSL error, HTTP violation)."""

    pass


class DownloadTimeoutError(DownloadRequestError):
    """Raised when a download request times out."""

    def __init__(self, url: str, original: Exception):
        super().__init__(url=url, original=original)


class ResourceInfoError(CommunicationError):
    """Raised when failed to obtain resource information from a URL."""

    def __init__(self, url: str, reason: str, original: Exception | None = None):
        super().__init__(
            f"Failed to obtain resource info from {url}: {reason}",
            url=url,
            original=original,
        )
        self.reason = reason


class ResourceNotFoundError(CommunicationError):
    """Raised when the requested resource does not exist."""

    def __init__(self, url: str, original: Exception | None = None):
        super().__init__(f"Resource not found: {url}", url=url, original=original)


class AccessDeniedError(CommunicationError):
    """Raised when access to the resource is denied."""

    def __init__(self, url: str, original: Exception | None = None):
        super().__init__(f"Access denied to: {url}", url=url, original=original)


class ServerUnavailableError(CommunicationError):
    """Raised when the server is unavailable (e.g. overloaded, maintenance, rate-limiting)."""

    def __init__(self, url: str, original: Exception | None = None):
        super().__init__(
            f"Server unavailable at: {url}",
            url=url,
            original=original,
        )
