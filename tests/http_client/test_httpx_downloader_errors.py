import asyncio
import httpx
import pytest
from unittest.mock import MagicMock, AsyncMock
from contextlib import asynccontextmanager
from sDownload.http_client.httpx_downloader import HttpxDownloader
from sDownload.interfaces.models import HttpConfigModel
from sDownload.exceptions import (
    DownloadTimeoutError,
    ResourceNotFoundError,
    AccessDeniedError,
    ServerUnavailableError,
    NetworkError,
    ProtocolError,
    DownloadRequestError,
    CommunicationError,
    SDownloadError,
)
import sDownload.exceptions as sd_exc


@pytest.fixture
def downloader():
    return HttpxDownloader(HttpConfigModel())


def mock_response(status_code):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.request = MagicMock(spec=httpx.Request)
    resp.request.url = "http://example.com"
    return resp


def test_map_httpx_error_logic_errors(downloader):
    url = "http://test.com"
    for err_type in [ValueError, TypeError, KeyError]:
        err = err_type("test")
        assert downloader._map_httpx_error(err, url) is err


def test_map_httpx_error_cancellation(downloader):
    url = "http://test.com"
    err = asyncio.CancelledError()
    assert downloader._map_httpx_error(err, url) is err


def test_map_httpx_error_timeout(downloader):
    url = "http://test.com"
    err = httpx.ConnectTimeout("timeout")
    mapped = downloader._map_httpx_error(err, url)
    assert isinstance(mapped, DownloadTimeoutError)
    assert mapped.original is err


def test_map_httpx_error_status_codes(downloader):
    url = "http://test.com"

    # 404
    err = httpx.HTTPStatusError("404", request=None, response=mock_response(404))
    assert isinstance(downloader._map_httpx_error(err, url), ResourceNotFoundError)

    # 401, 403
    for code in [401, 403]:
        err = httpx.HTTPStatusError(
            str(code), request=None, response=mock_response(code)
        )
        assert isinstance(downloader._map_httpx_error(err, url), AccessDeniedError)

    # 429, 503, 504
    for code in [429, 503, 504]:
        err = httpx.HTTPStatusError(
            str(code), request=None, response=mock_response(code)
        )
        assert isinstance(downloader._map_httpx_error(err, url), ServerUnavailableError)

    # Other status error
    err = httpx.HTTPStatusError("500", request=None, response=mock_response(500))
    assert isinstance(downloader._map_httpx_error(err, url), DownloadRequestError)


def test_map_httpx_error_network(downloader):
    url = "http://test.com"
    for err_type in [httpx.ConnectError, httpx.NetworkError]:
        err = err_type("net error")
        assert isinstance(downloader._map_httpx_error(err, url), NetworkError)


def test_map_httpx_error_protocol(downloader):
    url = "http://test.com"
    for err_type in [httpx.ProtocolError, httpx.ProxyError]:
        err = err_type("proto error")
        assert isinstance(downloader._map_httpx_error(err, url), ProtocolError)


def test_map_httpx_error_generic_http(downloader):
    url = "http://test.com"
    err = httpx.HTTPError("gen error")
    assert isinstance(downloader._map_httpx_error(err, url), DownloadRequestError)


def test_map_httpx_error_unrecognized(downloader):
    url = "http://test.com"
    err = RuntimeError("unknown")
    mapped = downloader._map_httpx_error(err, url)
    assert isinstance(mapped, CommunicationError)
    assert "Unexpected" in mapped.message


@pytest.mark.asyncio
async def test_download_chunk_cancellation_flow(downloader):
    url = "http://test.com"

    async def mock_iter():
        raise asyncio.CancelledError()
        yield b""

    mock_client = AsyncMock(spec=httpx.AsyncClient)
    downloader._get_client = AsyncMock(return_value=mock_client)
    mock_client.__aenter__.return_value = mock_client

    mock_resp = AsyncMock(spec=httpx.Response)
    mock_resp.headers = httpx.Headers({})

    @asynccontextmanager
    async def mock_stream_fn(*args, **kwargs):
        yield mock_resp

    mock_client.stream.side_effect = mock_stream_fn
    mock_resp.aiter_bytes.side_effect = mock_iter

    with pytest.raises(asyncio.CancelledError):
        async for _ in downloader.download_chunk(url):
            pass
