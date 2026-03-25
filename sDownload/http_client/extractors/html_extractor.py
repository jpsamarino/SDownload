import re
from urllib.parse import urljoin
from typing import AsyncGenerator
import httpx

from .protocol import ResourceExtractorProtocol


class HtmlExtractor(ResourceExtractorProtocol):
    """
    Extractor for HTML/JS/CSS resources.
    Uses resilient Regular Expressions to find links in messy HTML or embedded JavaScript.

    Responsibilities:
    - Safely download content up to a 1MB limit.
    - Extract anything that looks like a URL or file path in common attributes.
    - Extract any absolute URL found in the text.
    - Resolve relative URLs to absolute URLs based on the origin URL.
    - Yield unique absolute URLs.
    """

    _ATTR_REGEX = re.compile(
        r'(?i)(?:href|src|data-url|data-href)\s*=\s*(["\'])(.*?)\1'
    )

    _ABS_URL_REGEX = re.compile(r'(?i)(["\'])(https?://[^\s"\'<>]+)\1')

    async def extract(
        self, url: str, client: httpx.AsyncClient
    ) -> AsyncGenerator[str, None]:

        MAX_SCRAPE_SIZE = 1024 * 1024

        try:
            async with client.stream("GET", url, follow_redirects=True) as response:
                response.raise_for_status()

                content_type = response.headers.get("Content-Type", "").lower()
                is_text_based = any(
                    t in content_type
                    for t in [
                        "text/",
                        "application/json",
                        "application/javascript",
                        "application/x-javascript",
                        "application/xml",
                        "application/xhtml+xml",
                    ]
                )

                if content_type and not is_text_based:
                    return

                body = ""
                async for chunk in response.aiter_text():
                    body += chunk
                    if len(body) >= MAX_SCRAPE_SIZE:
                        break

                seen_links = set()

                for match in self._ATTR_REGEX.finditer(body):
                    raw_link = match.group(2).strip()
                    if not raw_link:
                        continue

                    absolute_url = urljoin(url, raw_link)
                    if absolute_url not in seen_links:
                        seen_links.add(absolute_url)
                        yield absolute_url

                for match in self._ABS_URL_REGEX.finditer(body):
                    raw_link = match.group(2).strip()
                    if raw_link not in seen_links:
                        seen_links.add(raw_link)
                        yield raw_link

        except Exception as e:
            raise e
