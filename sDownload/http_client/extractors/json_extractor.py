import re

from .protocol import ExtractedLink, ResourceExtractorProtocol


class JsonExtractor(ResourceExtractorProtocol):
    """
    Parser for JSON responses.
    Uses a RegEx pattern to quickly sweep for absolute URLs inside the values.
    Strictly synchronous and stateless.
    """

    _ABS_URL_REGEX = re.compile(r'(?i)(["\'])(https?://[^\s"\'<>]+)\1')

    def extract(self, content: str, base_url: str) -> list[ExtractedLink]:
        seen_links = set()
        final_links = []

        for match in self._ABS_URL_REGEX.finditer(content):
            raw_link = match.group(2).strip()
            if raw_link not in seen_links:
                seen_links.add(raw_link)
                final_links.append(ExtractedLink(url=raw_link))

        return final_links
