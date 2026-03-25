from typing import NamedTuple, List
import httpx
import logging
import re
from .extractors.factory import ExtractorFactory

logger = logging.getLogger(__name__)


class DiscoveryResult(NamedTuple):
    """
    The result of a resource exploration.
    - files: URLs that are confirmed downloadable files.
    - sub_nodes: URLs that are confirmed navigable nodes (pages or folders).
    """

    files: List[str]
    sub_nodes: List[str]


async def explore_resource(
    url: str,
    client: httpx.AsyncClient,
    known_webdav: set[str],
    max_scrape_size: int = 1048576,
) -> DiscoveryResult:
    """
    The 'Scout': Performs network I/O, classifies the resource via headers/status,
    and uses Parsers to find more links.
    """
    is_webdav_mode = any(url.startswith(d) for d in known_webdav)
    method = "PROPFIND" if is_webdav_mode else "GET"
    headers = {"Depth": "1"} if method == "PROPFIND" else {}

    found_files = []
    found_sub_nodes = []

    try:
        async with client.stream(
            method, url, headers=headers, follow_redirects=True
        ) as resp:
            if "DAV" in resp.headers or "PROPFIND" in resp.headers.get("Allow", ""):
                root_url = "/".join(url.split("/")[:4])
                known_webdav.add(root_url)

            status = resp.status_code
            content_type = resp.headers.get("Content-Type", "").lower()

            is_attachment = (
                "attachment" in resp.headers.get("Content-Disposition", "").lower()
            )

            parser = ExtractorFactory.get_extractor(content_type, status)

            if not parser or is_attachment:
                found_files.append(str(resp.url))
                return DiscoveryResult(files=found_files, sub_nodes=found_sub_nodes)

            body = ""
            async for chunk in resp.aiter_text():
                body += chunk
                if len(body) >= max_scrape_size:
                    break

            extracted_links = parser.extract(body, str(resp.url))

            is_pure_container = (
                any(t in content_type for t in ["html", "json"]) or status == 207
            )

            if not is_pure_container:
                found_files.append(str(resp.url))

            for link in extracted_links:
                if link.is_dir is True:
                    found_sub_nodes.append(link.url)
                elif link.is_dir is False:
                    found_files.append(link.url)
                else:
                    found_sub_nodes.append(link.url)

            return DiscoveryResult(files=found_files, sub_nodes=found_sub_nodes)

    except Exception as e:
        logger.warning(f"Failed to explore {url}: {e}")
        return DiscoveryResult(files=found_files, sub_nodes=found_sub_nodes)
