import asyncio
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import AsyncIterator, Optional
import httpx
from sDownload.exceptions.downloader_errors import FileIDMismatchError, DownloadRequestError, FileInfoExtractionError
from sDownload.interfaces.protocols.dowloader_protocol import DownloaderProtocol
from sDownload.interfaces.protocols.http_config_model import HttpConfigModel
from sDownload.interfaces.protocols.file_info_model import FileInfoModel
from sDownload.utils.url_to_file_name import url_to_file_name


class HttpxDownloader(DownloaderProtocol):
    """
    Implementation of DownloaderProtocol using httpx.AsyncClient.
    Handles ranged downloads, retries, timeout, SSL verification, cookies and proxies.
    """

    def __init__(self, config: HttpConfigModel):
        self.config = config

    def _build_proxy_url(self, spc) -> str:
        """
        Construct a proxy URL from SingleProxyConfig.
        """
        auth = f"{spc.username}:{spc.password}@" if spc.username and spc.password else ""
        return f"{spc.protocol.value}://{auth}{spc.host}:{spc.port}"

    async def _get_client(self) -> httpx.AsyncClient:
        """
        Create and configure an httpx.AsyncClient per HttpConfigModel.
        """
        proxies: Optional[dict[str, str]] = None
        if self.config.proxy:
            mapping: dict[str, str] = {}
            for scheme in ("http", "https"):
                spc = getattr(self.config.proxy,
                              scheme) or self.config.proxy.default
                if spc:
                    mapping[f"{scheme}://"] = self._build_proxy_url(spc)
            if mapping:
                proxies = mapping

        # timeout = httpx.Timeout(connect=self.config.timeout_connect)
        return httpx.AsyncClient(
            headers=self.config.headers,
            timeout=self.config.timeout_connect,
            verify=self.config.valid_ssl,
            cookies=self.config.cookies
        )

    async def download_chunk(
        self,
        url: str,
        start_byte: int = 0,
        end_byte: Optional[int] = None,
        file_id: Optional[str] = None
    ) -> AsyncIterator[bytes]:

        headers: dict[str, str] = {}

        if start_byte >= 0 and end_byte is None:
            headers["Range"] = f"bytes={start_byte}-"
        elif 0 <= start_byte <= end_byte:
            headers["Range"] = f"bytes={start_byte}-{end_byte}"

        async with await self._get_client() as client:
            try:
                async with client.stream("GET", url, headers=headers) as response:
                    response.raise_for_status()

                    etag = response.headers.get('ETag')
                    if file_id and etag != file_id:
                        raise FileIDMismatchError(file_id, etag)

                    async for chunk in response.aiter_bytes():
                        yield chunk

            except httpx.HTTPError as http_err:
                raise DownloadRequestError(url, http_err) from http_err
            except FileIDMismatchError:
                raise
            except Exception as err:
                raise DownloadRequestError(url, err) from err

    async def get_file_info(self, url: str) -> list[FileInfoModel]:
        try:
            async with await self._get_client() as client:
                async with client.stream("GET", url, headers={'Range': 'bytes=0-0'}) as response:
                    response.raise_for_status()
                    headers = response.headers
                    status_code = response.status_code

                    content_range = headers.get('Content-Range')
                    try:
                        if status_code == 206 and content_range and '/' in content_range:
                            full_size = int(content_range.split('/', 1)[1])
                            resumable = True
                        else:
                            full_size = int(headers.get('Content-Length', 0))
                            resumable = False
                    except Exception as size_err:
                        raise FileInfoExtractionError(
                            url, "Failed to parse file size", size_err) from size_err

                    file_id = headers.get('ETag')
                    cd = headers.get('Content-Disposition', '')
                    file_name = url_to_file_name(url)
                    if 'filename=' in cd:
                        file_name = cd.split('filename=')[-1].strip('"')

                    async for chunk in response.aiter_bytes():
                        break

                    last_mod = headers.get('Last-Modified')
                    try:
                        date_created = parsedate_to_datetime(
                            last_mod) if last_mod else datetime.now(timezone.utc)
                    except Exception as date_err:
                        raise FileInfoExtractionError(
                            url, "Invalid Last-Modified header", date_err) from date_err

                    return [FileInfoModel(
                        file_name=file_name,
                        file_dir=".",
                        file_size=full_size,
                        file_id=file_id,
                        download_url=str(response.url),
                        transmission_protocol=response.url.scheme,
                        server_accept_ranges=resumable,
                        file_created_at=date_created,
                        protocol_data=dict(headers),
                    )]

        except httpx.HTTPError as http_err:
            raise FileInfoExtractionError(
                url, "HTTP error", http_err) from http_err
        except Exception as err:
            raise FileInfoExtractionError(
                url, "Unexpected error", err) from err
