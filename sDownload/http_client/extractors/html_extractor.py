import httpx
from html.parser import HTMLParser
from urllib.parse import urljoin
from collections.abc import AsyncGenerator
from .protocol import ResourceExtractorProtocol


class LinkParser(HTMLParser):
    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    # Clean the link
                    clean_value = value.strip()
                    
                    # Ignore navigation-only links
                    if clean_value in (".", "..", "./", "../") or clean_value.startswith("#"):
                        continue
                        
                    # Resolve relative URL to absolute
                    absolute_url = urljoin(self.base_url, clean_value)
                    self.links.append(absolute_url)


class HtmlExtractor(ResourceExtractorProtocol):
    """
    Extractor for HTML resources.
    Uses native html.parser with a 1MB safety limit and navigation filtering.
    """

    async def extract(
        self, url: str, client: httpx.AsyncClient
    ) -> AsyncGenerator[str, None]:
        # Safety limit: 1MB for scraping
        MAX_SCRAPE_SIZE = 1024 * 1024
        
        try:
            async with client.stream("GET", url, follow_redirects=True) as response:
                response.raise_for_status()
                
                body = ""
                # Read chunks until 1MB or end
                async for chunk in response.aiter_text():
                    body += chunk
                    if len(body) >= MAX_SCRAPE_SIZE:
                        break
                
                parser = LinkParser(url)
                parser.feed(body)
                
                for link in parser.links:
                    yield link
                
        except Exception as e:
            raise e
