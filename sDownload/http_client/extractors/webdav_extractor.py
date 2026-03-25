import httpx
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse
from collections.abc import AsyncGenerator
from .protocol import ResourceExtractorProtocol


class WebDavExtractor(ResourceExtractorProtocol):
    """
    Extractor for WebDAV resources.
    Uses PROPFIND to list files and directories.
    """

    async def extract(
        self, url: str, client: httpx.AsyncClient, max_scrape_size: int = 1048576
    ) -> AsyncGenerator[str, None]:

        headers = {"Depth": "1", "Content-Type": "application/xml"}

        try:
            async with client.stream(
                "PROPFIND", url, headers=headers, follow_redirects=True
            ) as response:
                response.raise_for_status()

                # 207 Multi-Status is the success code for PROPFIND
                if response.status_code != 207:
                    return

                body = ""
                async for chunk in response.aiter_text():
                    body += chunk
                    if len(body) >= max_scrape_size:
                        break

                root = ET.fromstring(body)

            ns = {"d": "DAV:"}

            responses = root.findall(".//d:response", ns)

            requested_path = urlparse(url).path.rstrip("/")

            for resp in responses:
                href_el = resp.find("d:href", ns)
                if href_el is None or href_el.text is None:
                    continue

                href = href_el.text

                absolute_url = urljoin(url, href)

                if urlparse(absolute_url).path.rstrip("/") == requested_path:
                    continue

                yield absolute_url

        except Exception as e:
            raise e
