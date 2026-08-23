from .base import LifecycleError, SDownloadError
from .communication import (
    AccessDeniedError,
    CommunicationError,
    DownloadRequestError,
    DownloadTimeoutError,
    NetworkError,
    ProtocolError,
    ResourceInfoError,
    ResourceNotFoundError,
    ServerUnavailableError,
)
from .data import (
    ChunkSuccessionError,
    DataError,
    FileIDMismatchError,
    IntegrityError,
    ReconstructionError,
)
from .infrastructure import (
    FileAlreadyExistsError,
    InfrastructureError,
    StorageError,
    StorageFullError,
    StorageNotFoundError,
    StoragePermissionError,
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
    "FileAlreadyExistsError",
    # Data
    "DataError",
    "ReconstructionError",
    "FileIDMismatchError",
    "IntegrityError",
    "ChunkSuccessionError",
]
