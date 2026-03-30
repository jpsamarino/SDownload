import asyncio
import re
import logging
from urllib.parse import urljoin, urlparse
from collections.abc import AsyncGenerator, AsyncIterable
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from collections import deque
import httpx
from sDownload.exceptions import (
    FileIDMismatchError,
    ResourceInfoError,
)
from sDownload.interfaces.protocols import DownloaderProtocol
from sDownload.interfaces.models import HttpConfigModel, ResourceInfo
from sDownload.utils import url_to_file_name
from .httpx_error_mapper import map_httpx_error
from .resource_explorer import explore_resource, DiscoveryTask
from .extractors.protocol import DiscoveryMethod, ExtractedLink


logger = logging.getLogger(__name__)


class HttpxDownloader(DownloaderProtocol):
    """
    Implementation of DownloaderProtocol using httpx.AsyncClient.
    Handles ranged downloads, retries, timeout, SSL verification, cookies and proxies.
    """

    def __init__(self, config: HttpConfigModel | None = None):
        self.config = config or HttpConfigModel()

    def _build_proxy_url(self, spc) -> str:
        """
        Construct a proxy URL from SingleProxyConfig.
        """
        auth = (
            f"{spc.username}:{spc.password}@" if spc.username and spc.password else ""
        )
        return f"{spc.protocol.value}://{auth}{spc.host}:{spc.port}"

    async def _get_client(self) -> httpx.AsyncClient:
        """
        Create and configure an httpx.AsyncClient per HttpConfigModel.
        """
        proxies: dict[str, str] | None = None
        if self.config.proxy:
            mapping: dict[str, str] = {}
            for scheme in ("http", "https"):
                spc = getattr(self.config.proxy, scheme) or self.config.proxy.default
                if spc:
                    mapping[f"{scheme}://"] = self._build_proxy_url(spc)
            if mapping:
                proxies = mapping

        return httpx.AsyncClient(
            headers=self.config.headers,
            timeout=self.config.timeout_connect_s,
            verify=self.config.valid_ssl,
            cookies=self.config.cookies,
        )

    async def download_chunk(
        self,
        url: str,
        start_byte: int = 0,
        end_byte: int | None = None,
        file_id: str | None = None,
    ) -> AsyncGenerator[bytes, None]:

        headers: dict[str, str] = {}

        if start_byte < 0:
            raise ValueError("start_byte must be non-negative")

        if end_byte is None:
            headers["Range"] = f"bytes={start_byte}-"
        elif 0 <= start_byte <= end_byte:
            headers["Range"] = f"bytes={start_byte}-{end_byte}"

        async with await self._get_client() as client:
            try:
                async with client.stream("GET", url, headers=headers) as response:
                    response.raise_for_status()

                    etag = response.headers.get("ETag")
                    if file_id and etag != file_id:
                        raise FileIDMismatchError(file_id, etag, url)

                    try:
                        async for chunk in response.aiter_bytes():
                            yield chunk
                    except GeneratorExit:
                        logger.debug(f"Download cancelled for {url}")
                        return

            except Exception as err:
                raise map_httpx_error(err, url) from err

    async def get_file_info(self, url: str) -> list[ResourceInfo]:
        try:
            async with await self._get_client() as client:
                async with client.stream(
                    "GET", url, headers={"Range": "bytes=0-0"}
                ) as response:
                    response.raise_for_status()
                    headers = response.headers
                    status_code = response.status_code

                    content_range = headers.get("Content-Range")
                    try:
                        if (
                            status_code == 206
                            and content_range
                            and "/" in content_range
                        ):
                            full_size = int(content_range.split("/", 1)[1])
                            resumable = True
                        else:
                            full_size = int(headers.get("Content-Length", 0))
                            resumable = False
                    except Exception as size_err:
                        raise ResourceInfoError(
                            url, "Failed to parse file size", size_err
                        ) from size_err

                    file_id = headers.get("ETag")
                    cd = headers.get("Content-Disposition", "")
                    file_name = url_to_file_name(url)
                    if "filename=" in cd:
                        file_name = cd.split("filename=")[-1].strip('"')

                    async for chunk in response.aiter_bytes():
                        break

                    last_mod = headers.get("Last-Modified")
                    try:
                        date_created = (
                            parsedate_to_datetime(last_mod)
                            if last_mod
                            else datetime.now(timezone.utc)
                        )
                    except Exception as date_err:
                        logger.warning(f"FAILED on get_file_info for {url}: {date_err}")
                        raise ResourceInfoError(
                            url, "Invalid Last-Modified header", date_err
                        ) from date_err

                    return ResourceInfo(
                        file_name=file_name,
                        file_dir=".",
                        file_size=full_size,
                        file_id=file_id,
                        download_url=str(response.url),
                        transmission_protocol=response.url.scheme,
                        server_accept_ranges=resumable,
                        file_created_at=date_created,
                        protocol_data=dict(headers),
                    )

        except Exception as err:
            raise map_httpx_error(err, url) from err

    async def list_resources(
        self,
        url: str,
        pattern: str | None = None,
        level: int = 1,
        max_size_bytes: int = 1048576,
    ) -> AsyncGenerator[str, None]:
        """
        List resources using Strategy Pattern (Extractors).
        Returns strings (URLs) instead of ResourceInfo to save performance during mass discovery.
        """
        if level < 1:
            raise ValueError("level must be greater than 0")

        regex = re.compile(pattern) if pattern else None
        seen_urls = {url}
        queue = deque([DiscoveryTask(url=url, level=1)])

        def _enqueue_links(links: list[ExtractedLink], is_last_level: bool = False):
            """Helper to enqueue links with proper seen_urls dedup and filtering."""
            for link in links:
                if link.url in seen_urls:
                    continue
                if is_last_level and regex and not regex.search(link.url):
                    continue
                seen_urls.add(link.url)
                queue.append(
                    DiscoveryTask(
                        url=link.url,
                        level=task.level + 1,
                        method_hint=link.method_hint,
                        process_only_files=is_last_level,
                    )
                )

        async with await self._get_client() as client:
            while queue:
                task = queue.popleft()

                try:
                    result = await explore_resource(
                        task.url,
                        client,
                        method_hint=task.method_hint,
                        max_scrape_size=max_size_bytes,
                        process_only_files=task.process_only_files,
                    )

                    for file_url in result.files:
                        filename = url_to_file_name(file_url)
                        if not regex or regex.search(filename):
                            yield file_url

                    if task.level < level:
                        _enqueue_links(result.directories)
                        _enqueue_links(result.unresolved_links)
                    elif task.level == level:
                        _enqueue_links(result.unresolved_links, is_last_level=True)

                except Exception as e:
                    logger.warning(f"Failed to process {task.url}: {e}")
                    continue
