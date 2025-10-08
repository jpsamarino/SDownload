class DownloaderError(Exception):
    """Base class for all downloader-related errors."""
    pass


class FileIDMismatchError(DownloaderError):
    def __init__(self, expected_id: str, received_id: str | None):
        super().__init__(
            f"File ID mismatch: expected '{expected_id}', got '{received_id}'")
        self.expected_id = expected_id
        self.received_id = received_id


class DownloadRequestError(DownloaderError):
    def __init__(self, url: str, original: Exception):
        super().__init__(
            f"Failed to initiate or process request to {url}: {original}")
        self.url = url
        self.original = original


class FileInfoExtractionError(DownloaderError):
    def __init__(self, url: str, reason: str, original: Exception | None = None):
        msg = f"Failed to extract file info from {url}: {reason}"
        if original:
            msg += f" ({original})"
        super().__init__(msg)
        self.url = url
        self.reason = reason
        self.original = original
