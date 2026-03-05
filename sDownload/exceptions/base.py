class SDownloadError(Exception):
    """Base class for all exceptions in the SDownload project."""

    def __init__(self, message: str, original: Exception | None = None):
        super().__init__(message)
        self.message = message
        self.original = original
