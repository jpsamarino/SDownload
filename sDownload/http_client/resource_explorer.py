from typing import NamedTuple, List
import httpx
import logging
from .extractors.factory import ExtractorFactory
from .extractors.protocol import ExtractedLink, DiscoveryMethod

logger = logging.getLogger(__name__)


class DiscoveryResult(NamedTuple):
    """
    The result of a resource exploration.
    - files: URLs that are confirmed downloadable files.
    - sub_nodes: ExtractedLink objects for confirmed navigable nodes (pages or folders).
    """

    files: List[str]
    sub_nodes: List[ExtractedLink]


async def explore_resource(
    url: str,
    client: httpx.AsyncClient,
    method_hint: DiscoveryMethod = DiscoveryMethod.UNKNOWN,
    max_scrape_size: int = 1048576,
) -> DiscoveryResult:

    if method_hint != DiscoveryMethod.UNKNOWN:
        return await _explore_with_method(url, client, method_hint, max_scrape_size)

    logger.debug(f"Probing (OPTIONS) unknown resource: {url}")
    try:
        opt_resp = await client.options(url, timeout=2.0, follow_redirects=True)

        dav_header = opt_resp.headers.get("DAV")
        allow_header = opt_resp.headers.get("Allow", "")

        if dav_header or "PROPFIND" in allow_header:
            return await _explore_with_method(
                url, client, DiscoveryMethod.PROPFIND, max_scrape_size
            )

    except Exception as e:
        logger.warning(f"Failed to probe {url}: {e}")

    return await _explore_with_method(url, client, DiscoveryMethod.GET, max_scrape_size)


def is_probably_navigable(content_type: str) -> bool:
    """
    Checks if a content type is likely to contain discoverable links
    and should be treated primarily as a container (not a download).
    """
    content_type = content_type.lower()
    navigable_types = [
        "html",
        "json",
        "xml",
        "javascript",
        "css",
    ]
    return any(t in content_type for t in navigable_types)


async def _explore_with_method(
    url: str,
    client: httpx.AsyncClient,
    method: DiscoveryMethod,
    max_scrape_size: int,
    is_retry: bool = False,
) -> DiscoveryResult:

    match method:
        case DiscoveryMethod.PROPFIND:
            method_str = "PROPFIND"
        case DiscoveryMethod.POST:
            method_str = "POST"
        case _:
            method_str = "GET"

    headers = {"Depth": "1"} if method == DiscoveryMethod.PROPFIND else {}

    try:
        async with client.stream(
            method_str, url, headers=headers, follow_redirects=True
        ) as resp:
            chunks = []
            current_total = 0
            checked_for_upgrade = False

            async for chunk in resp.aiter_text():
                chunks.append(chunk)
                current_total += len(chunk)

                if (
                    method == DiscoveryMethod.GET
                    and not is_retry
                    and not checked_for_upgrade
                ):
                    partial_body = "".join(chunks)
                    if (
                        'xmlns:d="DAV:"' in partial_body
                        or "<d:multistatus" in partial_body
                        or resp.status_code == 207
                    ):
                        return await _explore_with_method(
                            url,
                            client,
                            DiscoveryMethod.PROPFIND,
                            max_scrape_size,
                            is_retry=True,
                        )

                    if current_total > 32768:  # 32KB is enough for any DAV header/start
                        checked_for_upgrade = True

                if current_total >= max_scrape_size:
                    break

            body = "".join(chunks)

            if (
                method == DiscoveryMethod.PROPFIND
                and resp.status_code in (405, 501)
                and not is_retry
            ):
                return await _explore_with_method(
                    url, client, DiscoveryMethod.GET, max_scrape_size, is_retry=True
                )

            content_type = resp.headers.get("Content-Type", "").lower()
            parser = ExtractorFactory.get_extractor(content_type, method)
            is_attachment = (
                "attachment" in resp.headers.get("Content-Disposition", "").lower()
            )

            if not parser or is_attachment:
                return DiscoveryResult(files=[str(resp.url)], sub_nodes=[])

            extracted_links = parser.extract(body, str(resp.url))

            is_pure_container = (
                is_probably_navigable(content_type) or resp.status_code == 207
            )
            found_files = [str(resp.url)] if not is_pure_container else []
            found_sub_nodes = []

            for link in extracted_links:
                if link.is_dir is False:
                    found_files.append(link.url)
                else:
                    found_sub_nodes.append(link)

            return DiscoveryResult(files=found_files, sub_nodes=found_sub_nodes)

    except Exception as e:
        logger.warning(f"Failed to explore {url}: {e}")
        return DiscoveryResult(files=[], sub_nodes=[])
