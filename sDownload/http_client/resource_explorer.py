from sDownload.utils import get_url_extension
from typing import NamedTuple, List
import httpx
import logging
from sDownload.utils import is_navigable
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
    unknown_links: List[ExtractedLink]


async def explore_resource(
    url: str,
    client: httpx.AsyncClient,
    method_hint: DiscoveryMethod = DiscoveryMethod.UNKNOWN,
    max_scrape_size: int = 1048576,
    only_files: bool = False,
) -> DiscoveryResult:

    if method_hint != DiscoveryMethod.UNKNOWN:
        return await _explore_with_method(
            url, client, method_hint, max_scrape_size, only_files
        )

    logger.debug(f"Probing (OPTIONS) unknown resource: {url}")
    try:
        opt_resp = await client.options(url, timeout=2.0, follow_redirects=True)

        dav_header = opt_resp.headers.get("DAV")
        allow_header = opt_resp.headers.get("Allow", "")

        if dav_header or "PROPFIND" in allow_header and not only_files:
            return await _explore_with_method(
                url, client, DiscoveryMethod.PROPFIND, max_scrape_size
            )

        ct = opt_resp.headers.get("Content-Type", "")

        if not is_navigable("", ct):
            logger.debug(f"Confirmed static file via OPTIONS: {url}")
            return DiscoveryResult(files=[url], sub_nodes=[])

    except Exception as e:
        logger.warning(f"Failed to probe {url}: {e}")

    if only_files:
        return DiscoveryResult(files=[], sub_nodes=[], unknown_links=[])
    return await _explore_with_method(url, client, DiscoveryMethod.GET, max_scrape_size)


async def _explore_with_method(
    url: str,
    client: httpx.AsyncClient,
    method: DiscoveryMethod,
    max_scrape_size: int,
    only_files: bool = False,
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

            if only_files:
                ext = get_url_extension(url)
                if is_navigable(ext, resp.headers.get("Content-Type", "")) == False:
                    return DiscoveryResult(
                        files=[str(resp.url)], sub_nodes=[], unknown_links=[]
                    )
                else:
                    return DiscoveryResult(files=[], sub_nodes=[], unknown_links=[])

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

            # Decide if the current URL is a file or just a container
            is_pure_container = (
                is_navigable("", content_type) or resp.status_code == 207
            )
            found_files = [str(resp.url)] if not is_pure_container else []
            found_sub_nodes = []  # error
            unknown_links = []

            for link in extracted_links:
                if link.is_dir is False:
                    found_files.append(link.url)
                elif link.is_dir is True:
                    found_sub_nodes.append(link)
                else:
                    unknown_links.append(link)

            return DiscoveryResult(
                files=found_files,
                sub_nodes=found_sub_nodes,
                unknown_links=unknown_links,
            )

    except Exception as e:
        logger.warning(f"Failed to explore {url}: {e}")
        return DiscoveryResult(files=[], sub_nodes=[], unknown_links=[])
