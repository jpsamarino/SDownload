import re
from typing import AsyncGenerator
import httpx

from .protocol import ResourceExtractorProtocol


class JsonExtractor(ResourceExtractorProtocol):
    """
    Extractor for JSON resources.
    Uses resilient Regular Expressions to find URLs directly in the raw JSON text.
    This avoids crashes on malformed JSON and finds URLs nested deep in mixed content.
    """

    _ABS_URL_REGEX = re.compile(r'(?i)(["\'])(https?://[^\s"\'<>]+)\1')

    async def extract(
        self, url: str, client: httpx.AsyncClient, max_scrape_size: int = 1048576
    ) -> AsyncGenerator[str, None]:

        try:
            async with client.stream("GET", url, follow_redirects=True) as response:
                response.raise_for_status()

                content_type = response.headers.get("Content-Type", "").lower()
                is_text_based = any(
                    t in content_type for t in ["text/", "application/json"]
                )

                if content_type and not is_text_based:
                    return

                body = ""
                async for chunk in response.aiter_text():
                    body += chunk
                    if len(body) >= max_scrape_size:
                        break

                seen_links = set()

                for match in self._ABS_URL_REGEX.finditer(body):
                    raw_link = match.group(2).strip()
                    if raw_link not in seen_links:
                        seen_links.add(raw_link)
                        yield raw_link

        except Exception as e:
            raise e
