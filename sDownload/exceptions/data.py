from .base import SDownloadError


class DataError(SDownloadError):
    """Base class for data integrity and logic errors."""

    pass


class ReconstructionError(DataError):
    """Raised when file reconstruction (merging chunks) fails."""

    def __init__(self, message: str, original: Exception | None = None):
        super().__init__(message, original=original)


class FileIDMismatchError(DataError):
    """Raised when the remote file ID (e.g. ETag) differs from the expected one."""

    def __init__(
        self,
        expected_id: str,
        received_id: str | None,
        url: str | None = None,
        original: Exception | None = None,
    ):
        super().__init__(
            f"File ID mismatch: expected '{expected_id}', got '{received_id}'",
            original=original,
        )
        self.expected_id = expected_id
        self.received_id = received_id
        self.url = url


class IntegrityError(DataError):
    """Raised when data integrity check fails."""

    def __init__(
        self,
        message: str = "Data integrity check failed",
        original: Exception | None = None,
    ):
        super().__init__(message, original=original)
