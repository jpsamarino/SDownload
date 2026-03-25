import pytest
import httpx
from sDownload.http_client.extractors.webdav_extractor import WebDavExtractor


@pytest.mark.asyncio
async def test_webdav_extractor_integration_private(webdav_server):
    """
    Verifies that WebDavExtractor works with the private authenticated container.
    """
    extractor = WebDavExtractor()
    url = webdav_server["url"]
    auth = webdav_server["auth"]

    async with httpx.AsyncClient(auth=auth) as client:
        links = []
        async for link in extractor.extract(url, client):
            links.append(link)

    # Check for expected files in the complex structure
    # Note: WebDAV often returns directories with a trailing slash
    assert any("root_file.txt" in l for l in links)
    assert any("folder1" in l for l in links)
    assert any("large_file_1mb.bin" in l for l in links)

    # All links must be absolute and start with the base URL
    for link in links:
        assert link.startswith(url)
        assert link != url  # Should not include the root itself


@pytest.mark.asyncio
async def test_webdav_extractor_integration_public(webdav_public_server):
    """
    Verifies that WebDavExtractor works with the public (anonymous) container.
    """
    extractor = WebDavExtractor()
    url = webdav_public_server

    async with httpx.AsyncClient() as client:
        links = []
        async for link in extractor.extract(url, client):
            links.append(link)

    assert len(links) > 0
    assert any("root_file.txt" in l for l in links)

    for link in links:
        assert link.startswith(url)


@pytest.mark.asyncio
async def test_webdav_extractor_deep_navigation(webdav_server):
    """
    Verifies that we can use the extractor to navigate into subfolders.
    """
    extractor = WebDavExtractor()
    base_url = webdav_server["url"]
    auth = webdav_server["auth"]

    # Target a subfolder
    subfolder_url = f"{base_url.rstrip('/')}/folder1/"

    async with httpx.AsyncClient(auth=auth) as client:
        links = []
        async for link in extractor.extract(subfolder_url, client):
            links.append(link)

    # Check for content inside folder1
    # Current structure has "file with spaces.txt" and "subdir_with_data/"
    try:
        assert any(
            "file%20with%20spaces.txt" in l.lower()
            or "file with spaces.txt" in l.lower()
            for l in links
        )
        assert any("subdir_with_data" in l for l in links)
    except AssertionError:
        print(f"\nDiscovered links in subfolder: {links}")
        raise
