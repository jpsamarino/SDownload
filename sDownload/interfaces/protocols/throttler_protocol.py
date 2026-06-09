from typing import Protocol, runtime_checkable
from collections.abc import AsyncGenerator
from sDownload.interfaces.models import ChunkDownloadStats


@runtime_checkable
class ThrottlerProtocol(Protocol):
    """
    Protocol for throttling strategies that track and limit download speed.
    """

    def wrap(
        self, it: AsyncGenerator[bytes, None], stats: ChunkDownloadStats
    ) -> AsyncGenerator[bytes, None]:
        """
        Wraps an async generator to apply throttling and track downloaded bytes.
        """
        ...
