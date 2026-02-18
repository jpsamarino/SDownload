from .downloader_errors import DownloaderError


class ReconstructionError(DownloaderError):
    """Exception raised when file reconstruction (merging chunks) fails."""

    pass
