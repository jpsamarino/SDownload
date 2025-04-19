import asyncio
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

    async def _is_range_supported(self, client: httpx.AsyncClient, url: str) -> bool:
        try:
            head_resp = await client.head(url)
            head_resp.raise_for_status()
            full_size = int(head_resp.headers.get('Content-Length', 0))
            if full_size <= 0:
                return False

            # Stream a small byte range to detect range support
            async with client.stream('GET', url, headers={'Range': 'bytes=0-3'}) as resp:
                resp.raise_for_status()
                cl = resp.headers.get('Content-Length')
                if cl is None:
                    # No Content-Length header => assume no range support
                    return False

                partial_size = int(cl)
                # Drain only the first chunk, then close to cancel further data
                async for chunk in resp.aiter_bytes():
                    break

            # If partial response size differs, server served the requested range
            return partial_size != full_size
        except Exception:
            return False

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

        timeout = httpx.Timeout(connect=self.config.timeout_connect)
        return httpx.AsyncClient(
            headers=self.config.headers,
            timeout=timeout,
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
                response = await client.head(url)
                response.raise_for_status()

                headers = response.headers
                # File size
                size = int(headers.get("Content-Length", 0))
                # MIME type
                content_type = headers.get("Content-Type", "")
                # Unique file identifier (ETag or protocol-specific)
                file_id = headers.get("ETag")
                # Range support
                accept = headers.get("Accept-Ranges", "").lower()
                resumable = self._is_range_supported(
                    client, url)  # accept == "bytes"
                # File name from Content-Disposition or URL path
                cd = headers.get("Content-Disposition", "")
                file_name = url.split("/")[-1]
                if "filename=" in cd:
                    file_name = cd.split("filename=")[-1].strip('"')

                return FileInfoModel(
                    file_name=file_name,
                    content_type=content_type,
                    file_size=size,
                    file_id=file_id,
                    download_url=str(response.url),
                    transmission_protocol=response.url.scheme,
                    server_accept_ranges=resumable,
                )
        except Exception as e:
            raise e
