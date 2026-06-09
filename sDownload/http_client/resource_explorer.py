from dataclasses import dataclass, field
from typing import List, Tuple
import httpx
import logging
from sDownload.utils import is_navigable, get_url_extension
from .extractors.factory import ExtractorFactory
from .extractors.protocol import ExtractedLink, DiscoveryMethod

logger = logging.getLogger(__name__)


@dataclass
class DiscoveryResult:
    """
    The result of a resource exploration.
    - files: URLs that are confirmed downloadable files.
    - directories: ExtractedLink objects for confirmed navigable nodes (pages or folders).
    - unresolved_links: ExtractedLink objects that need probing.
    """

    files: List[str] = field(default_factory=list)
    directories: List[ExtractedLink] = field(default_factory=list)
    unresolved_links: List[ExtractedLink] = field(default_factory=list)


@dataclass
class DiscoveryTask:
    """
    Represents a task for the crawler to discover resources.
    """

    url: str
    level: int
    method_hint: DiscoveryMethod = DiscoveryMethod.UNKNOWN
    process_only_files: bool = False


def _method_to_string(method: DiscoveryMethod) -> str:
    """Convert DiscoveryMethod enum to HTTP method string."""
    match method:
        case DiscoveryMethod.PROPFIND:
            return "PROPFIND"
        case DiscoveryMethod.POST:
            return "POST"
        case _:
            return "GET"


def _build_headers(method: DiscoveryMethod) -> dict:
    """Build headers for the HTTP request."""
    if method == DiscoveryMethod.PROPFIND:
        return {"Depth": "1"}
    return {}


def _classify_process_only_files_response(
    url: str, resp: httpx.Response
) -> DiscoveryResult:
    """Fast path: check if URL is a file based on extension and Content-Type."""
    ext = get_url_extension(url)
    if is_navigable(ext, resp.headers.get("Content-Type", "")) is False:
        return DiscoveryResult(files=[str(resp.url)])
    return DiscoveryResult()


async def _stream_body(
    client: httpx.AsyncClient,
    method_str: str,
    url: str,
    headers: dict,
    max_scrape_size: int,
) -> Tuple[str, int]:
    """Stream response body until max_scrape_size, return (body, total_bytes)."""
    chunks = []
    current_total = 0
    async with client.stream(
        method_str, url, headers=headers, follow_redirects=True
    ) as resp:
        async for chunk in resp.aiter_text():
            chunks.append(chunk)
            current_total += len(chunk)
            if current_total >= max_scrape_size:
                break
    return "".join(chunks), current_total


def _classify_and_extract(body: str, resp: httpx.Response) -> DiscoveryResult:
    """Extract links from body and classify them."""
    content_type = resp.headers.get("Content-Type", "").lower()
    parser = ExtractorFactory.get_extractor(
        content_type, DiscoveryMethod(resp.request.method)
    )
    is_attachment = "attachment" in resp.headers.get("Content-Disposition", "").lower()

    if not parser or is_attachment:
        return DiscoveryResult(files=[str(resp.url)])

    extracted_links = parser.extract(body, str(resp.url))

    is_directory = is_navigable("", content_type) or resp.status_code == 207
    found_files = [str(resp.url)] if not is_directory else []
    directories = []
    unresolved_links = []

    for link in extracted_links:
        if link.is_dir is False:
            found_files.append(link.url)
        elif link.is_dir is True:
            directories.append(link)
        else:
            unresolved_links.append(link)

    return DiscoveryResult(
        files=found_files,
        directories=directories,
        unresolved_links=unresolved_links,
    )


async def explore_resource(
    url: str,
    client: httpx.AsyncClient,
    method_hint: DiscoveryMethod = DiscoveryMethod.UNKNOWN,
    max_scrape_size: int = 1048576,
    process_only_files: bool = False,
) -> DiscoveryResult:

    if method_hint != DiscoveryMethod.UNKNOWN:
        return await _explore_with_method(
            url, client, method_hint, max_scrape_size, process_only_files
        )

    logger.debug(f"Probing (OPTIONS) unknown resource: {url}")
    try:
        opt_resp = await client.options(url, timeout=2.0, follow_redirects=True)

        dav_header = opt_resp.headers.get("DAV")
        allow_header = opt_resp.headers.get("Allow", "")

        if dav_header or "PROPFIND" in allow_header and not process_only_files:
            return await _explore_with_method(
                url, client, DiscoveryMethod.PROPFIND, max_scrape_size
            )

        ct = opt_resp.headers.get("Content-Type", "")

        if not is_navigable("", ct):
            logger.debug(f"Confirmed static file via OPTIONS: {url}")
            return DiscoveryResult(files=[url])

    except Exception as e:
        logger.warning(f"Failed to probe {url}: {e}")

    if process_only_files:
        return DiscoveryResult()
    return await _explore_with_method(url, client, DiscoveryMethod.GET, max_scrape_size)


async def _explore_with_method(
    url: str,
    client: httpx.AsyncClient,
    method: DiscoveryMethod,
    max_scrape_size: int,
    process_only_files: bool = False,
    is_retry: bool = False,
) -> DiscoveryResult:

    method_str = _method_to_string(method)
    headers = _build_headers(method)

    try:
        async with client.stream(
            method_str, url, headers=headers, follow_redirects=True
        ) as resp:

            if process_only_files:
                return _classify_process_only_files_response(url, resp)

            body, current_total = await _stream_body(
                client, method_str, url, headers, max_scrape_size
            )

            # Check for DAV upgrade during streaming (only on first GET pass)
            if method == DiscoveryMethod.GET and not is_retry:
                if (
                    'xmlns:d="DAV:"' in body
                    or "<d:multistatus" in body
                    or resp.status_code == 207
                ):
                    return await _explore_with_method(
                        url,
                        client,
                        DiscoveryMethod.PROPFIND,
                        max_scrape_size,
                        is_retry=True,
                    )

            # PROPFIND fallback to GET
            if (
                method == DiscoveryMethod.PROPFIND
                and resp.status_code in (405, 501)
                and not is_retry
            ):
                return await _explore_with_method(
                    url, client, DiscoveryMethod.GET, max_scrape_size, is_retry=True
                )

            return _classify_and_extract(body, resp)

    except Exception as e:
        logger.warning(f"Failed to explore {url}: {e}")
        return DiscoveryResult()
