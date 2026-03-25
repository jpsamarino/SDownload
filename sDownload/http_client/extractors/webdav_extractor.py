import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse
from .protocol import ResourceExtractorProtocol, ExtractedLink


class WebDavExtractor(ResourceExtractorProtocol):
    """
    Parser for WebDAV XML responses.
    Parses PROPFIND Multi-Status (207) bodies to list items.
    Strictly synchronous and stateless.
    """

    def extract(self, content: str, base_url: str) -> list[ExtractedLink]:
        if not content.strip():
            return []

        try:
            root = ET.fromstring(content)
        except ET.ParseError:
            return []

        ns = {"d": "DAV:"}
        responses = root.findall(".//d:response", ns)
        requested_path = urlparse(base_url).path.rstrip("/")
        final_links = []

        for resp in responses:
            href_el = resp.find("d:href", ns)
            if href_el is None or href_el.text is None:
                continue

            href = href_el.text
            absolute_url = urljoin(base_url, href)

            if urlparse(absolute_url).path.rstrip("/") == requested_path:
                continue

            propstat = resp.find(".//d:propstat/d:prop", ns)
            is_dir = False
            if propstat is not None:
                resourcetype = propstat.find("d:resourcetype", ns)
                if resourcetype is not None:
                    collection = resourcetype.find("d:collection", ns)
                    if collection is not None:
                        is_dir = True

            final_links.append(ExtractedLink(url=absolute_url, is_dir=is_dir))

        return final_links
