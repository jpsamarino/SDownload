import asyncio
import httpx
from sDownload.exceptions import (
    SDownloadError,
    DownloadRequestError,
    DownloadTimeoutError,
    ResourceNotFoundError,
    AccessDeniedError,
    ServerUnavailableError,
    NetworkError,
    ProtocolError,
    CommunicationError,
)


def map_httpx_error(exc: Exception, url: str) -> Exception:
    """Standalone mapping of httpx errors to SDownload domain exceptions."""
    if isinstance(exc, (ValueError, TypeError, KeyError, asyncio.CancelledError)):
        return exc
    if isinstance(exc, httpx.TimeoutException):
        return DownloadTimeoutError(url, exc)
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        if code == 404:
            return ResourceNotFoundError(url, exc)
        if code in (401, 403):
            return AccessDeniedError(url, exc)
        if code in (429, 503, 504):
            return ServerUnavailableError(url, exc)
        return DownloadRequestError(url, exc)
    if isinstance(exc, (httpx.ConnectError, httpx.NetworkError)):
        return NetworkError(url, exc)
    if isinstance(exc, (httpx.ProtocolError, httpx.ProxyError)):
        return ProtocolError(url, exc)
    if isinstance(exc, (httpx.HTTPError, IOError)):
        return DownloadRequestError(url, exc)
    if isinstance(exc, SDownloadError):
        return exc
    return CommunicationError(
        f"Unexpected communication error: {exc}", url=url, original=exc
    )
