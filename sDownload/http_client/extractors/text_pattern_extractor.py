import re
from urllib.parse import urljoin
from .protocol import ResourceExtractorProtocol, ExtractedLink, DiscoveryMethod


class TextPatternExtractor(ResourceExtractorProtocol):
    """
    Parser for HTML/JS/CSS resources.
    Uses resilient Regular Expressions to find links in messy text.
    Strictly synchronous and stateless.
    """

    _ATTR_REGEX = re.compile(
        r'(?i)(?:href|src|data-url|data-href)\s*=\s*(["\'])(.*?)\1'
    )

    _ABS_URL_REGEX = re.compile(r'(?i)(["\'])(https?://[^\s"\'<>]+)\1')

    def extract(self, content: str, base_url: str) -> list[ExtractedLink]:
        seen_links = set()
        final_links = []

        for match in self._ATTR_REGEX.finditer(content):
            raw_link = match.group(2).strip()
            if not raw_link:
                continue

            absolute_url = urljoin(base_url, raw_link)
            if absolute_url not in seen_links:
                seen_links.add(absolute_url)
                # Links from HTML attributes are likely standard HTTP resources
                final_links.append(
                    ExtractedLink(
                        url=absolute_url,
                        method_hint=DiscoveryMethod.GET,
                    )
                )

        for match in self._ABS_URL_REGEX.finditer(content):
            raw_link = match.group(2).strip()
            if raw_link not in seen_links:
                seen_links.add(raw_link)
                # Generic absolute URLs (often from JS strings) are UNKNOWN
                final_links.append(ExtractedLink(url=raw_link))

        return final_links
