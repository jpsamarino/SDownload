from .base import SDownloadError, LifecycleError
from .communication import (
    CommunicationError,
    DownloadRequestError,
    DownloadTimeoutError,
    ResourceInfoError,
    ResourceNotFoundError,
    AccessDeniedError,
    ServerUnavailableError,
    NetworkError,
    ProtocolError,
)
from .infrastructure import (
    InfrastructureError,
    StorageError,
    StorageFullError,
    StoragePermissionError,
    StorageNotFoundError,
)
from .data import (
    DataError,
    ReconstructionError,
    FileIDMismatchError,
    IntegrityError,
    ChunkSuccessionError,
)

__all__ = [
    # Base
    "SDownloadError",
    "LifecycleError",
    # Communication
    "CommunicationError",
    "DownloadRequestError",
    "DownloadTimeoutError",
    "ResourceInfoError",
    "ResourceNotFoundError",
    "AccessDeniedError",
    "ServerUnavailableError",
    "NetworkError",
    "ProtocolError",
    # Infrastructure
    "InfrastructureError",
    "StorageError",
    "StorageFullError",
    "StoragePermissionError",
    "StorageNotFoundError",
    # Data
    "DataError",
    "ReconstructionError",
    "FileIDMismatchError",
    "IntegrityError",
    "ChunkSuccessionError",
]
