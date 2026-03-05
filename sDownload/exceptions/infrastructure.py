from .base import SDownloadError


class InfrastructureError(SDownloadError):
    """Base class for all system and infrastructure-related errors."""

    pass


class StorageError(InfrastructureError):
    """Base class for storage-related operations errors."""

    def __init__(self, message: str, original: Exception | None = None):
        super().__init__(message, original=original)


class StorageFullError(StorageError):
    """Raised when storage space or quota is exhausted."""

    def __init__(self, path: str | None = None, original: Exception | None = None):
        msg = (
            f"No space left on storage device: {path}"
            if path
            else "No space left on storage device"
        )
        super().__init__(msg, original=original)
        self.path = path


class StoragePermissionError(StorageError):
    """Raised when there is no permission to read/write to the storage location."""

    def __init__(self, path: str | None = None, original: Exception | None = None):
        msg = f"Permission denied for: {path}" if path else "Permission denied"
        super().__init__(msg, original=original)
        self.path = path
