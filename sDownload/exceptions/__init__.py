from .base import SDownloadError
from .communication import (
    CommunicationError,
    DownloadRequestError,
    DownloadTimeoutError,
    ResourceInfoError,
    ResourceNotFoundError,
    AccessDeniedError,
    ServerUnavailableError,
)
from .infrastructure import (
    InfrastructureError,
    StorageError,
    StorageFullError,
    StoragePermissionError,
)
from .data import (
    DataError,
    ReconstructionError,
    FileIDMismatchError,
    IntegrityError,
)

__all__ = [
    # Base
    "SDownloadError",
    # Communication
    "CommunicationError",
    "DownloadRequestError",
    "DownloadTimeoutError",
    "ResourceInfoError",
    "ResourceNotFoundError",
    "AccessDeniedError",
    "ServerUnavailableError",
    # Infrastructure
    "InfrastructureError",
    "StorageError",
    "StorageFullError",
    "StoragePermissionError",
    # Data
    "DataError",
    "ReconstructionError",
    "FileIDMismatchError",
    "IntegrityError",
]
