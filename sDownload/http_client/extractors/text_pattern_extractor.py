import re
from sDownload.utils import get_url_extension, is_navigable, normalize_url
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
        final_links: list[ExtractedLink] = []

        for regex in (self._ATTR_REGEX, self._ABS_URL_REGEX):
            from_native_html = regex is self._ATTR_REGEX
            for match in regex.finditer(content):
                raw_link = match.group(2)
                url = normalize_url(raw_link, base_url)
                url_normalized = url.rstrip("/")
                if not url or url_normalized in seen_links or "http" not in url:
                    continue
                seen_links.add(url_normalized)

                ext = get_url_extension(url)
                is_dir = is_navigable(ext)

                method_hint = (
                    DiscoveryMethod.GET
                    if from_native_html or (is_dir is False)
                    else DiscoveryMethod.UNKNOWN
                )

                final_links.append(
                    ExtractedLink(
                        url=url,
                        method_hint=method_hint,
                        is_dir=is_dir,
                    )
                )

        return final_links
