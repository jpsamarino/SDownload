import httpx
from collections.abc import AsyncGenerator
from typing import Protocol


class ResourceExtractorProtocol(Protocol):
    """
    Protocol for resource extractors (HTML, JSON, WebDAV).

    Extractors are responsible for fetching (if necessary) and parsing content
    to discover links/resources. They must always return absolute URLs.
    """

    async def extract(
        self, url: str, client: httpx.AsyncClient
    ) -> AsyncGenerator[str, None]:
        """
        Extract absolute URLs from the resource at the given URL.

        Args:
            url (str): The URL of the resource to extract from.
            client (httpx.AsyncClient): The HTTP client to use for requests.

        Yields:
            AsyncGenerator[str, None]: An asynchronous generator of absolute URLs found.
        """
        ...
