import asyncio
import errno
from pathlib import Path
from sDownload.exceptions import (
    SDownloadError,
    StorageError,
    StorageFullError,
    StoragePermissionError,
    StorageNotFoundError,
)


def map_os_error(exc: Exception, path: str | Path | None = None) -> Exception:
    """Standalone mapping of OS/IO errors to SDownload storage exceptions."""
    if isinstance(exc, (ValueError, TypeError, KeyError, asyncio.CancelledError)):
        return exc
    path_str = str(path) if path else None

    if isinstance(exc, FileNotFoundError):
        return StorageNotFoundError(path_str, exc)
    if isinstance(exc, PermissionError) or (
        isinstance(exc, OSError) and exc.errno in (errno.EACCES, errno.EPERM)
    ):
        return StoragePermissionError(path_str, exc)
    if isinstance(exc, OSError) and exc.errno == errno.ENOSPC:
        return StorageFullError(path_str, exc)
    if isinstance(exc, SDownloadError):
        return exc
    return StorageError(f"Storage operation failed: {exc}", original=exc)
