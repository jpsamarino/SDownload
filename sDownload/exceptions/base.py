class SDownloadError(Exception):
    """Base class for all exceptions in the SDownload project."""

    def __init__(self, message: str, original: Exception | None = None):
        super().__init__(message)
        self.message = message
        self.original = original


class LifecycleError(SDownloadError):
    """Raised when an operation is requested on a component that is in an invalid
    state for that operation (e.g. trying to resize a chunk that is still initializing).
    """

    pass
