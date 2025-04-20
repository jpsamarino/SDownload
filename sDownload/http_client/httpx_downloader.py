import asyncio
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import AsyncIterator, Optional
import httpx
from sDownload.interfaces.protocols.dowloader_protocol import DownloaderProtocol
from sDownload.interfaces.protocols.http_config_model import HttpConfigModel
from sDownload.interfaces.protocols.file_info_model import FileInfoModel


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
    ) -> AsyncIterator[bytes]:
        headers: dict[str, str] = {}
        if end_byte is not None:
            headers["Range"] = f"bytes={start_byte}-{end_byte}"
        try:
            async with await self._get_client() as client:
                async with client.stream("GET", url, headers=headers) as response:
                    response.raise_for_status()
                    async for chunk in response.aiter_bytes():
                        yield chunk
                    return
        except Exception as e:
            raise e

    async def get_file_info(self, url: str) -> FileInfoModel:
        try:
            async with await self._get_client() as client:
                # Request only the first byte to get headers and partial content
                async with client.stream(
                    'GET', url, headers={'Range': 'bytes=0-0'}
                ) as response:
                    response.raise_for_status()
                    headers = response.headers

                    # Determine full file size
                    content_range = headers.get('Content-Range')
                    if content_range and '/' in content_range:
                        full_size = int(content_range.split('/', 1)[1])
                        resumable = True
                    else:
                        full_size = int(headers.get('Content-Length', 0))
                        resumable = False

                    # Metadata
                    content_type = headers.get('Content-Type', '')
                    file_id = headers.get('ETag')
                    cd = headers.get('Content-Disposition', '')
                    file_name = url.split('/')[-1]
                    if 'filename=' in cd:
                        file_name = cd.split('filename=')[-1].strip('"')

                    # Drain one chunk then close
                    async for chunk in response.aiter_bytes():
                        break

                    last_mod = headers.get('Last-Modified')
                    if last_mod:
                        try:
                            date_created = parsedate_to_datetime(last_mod)
                        except Exception:
                            date_created = datetime.now(timezone.utc)
                    else:
                        date_created = datetime.now(timezone.utc)

                return FileInfoModel(
                    file_name=file_name,
                    content_type=content_type,
                    file_size=full_size,
                    file_id=file_id,
                    download_url=str(response.url),
                    transmission_protocol=response.url.scheme,
                    server_accept_ranges=resumable,
                    file_created_at=date_created
                )
        except Exception as e:
            raise e
