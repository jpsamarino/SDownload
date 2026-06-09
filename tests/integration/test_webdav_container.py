import httpx
import pytest


@pytest.mark.asyncio
async def test_webdav_sanity(webdav_server):
    """
    Verifies that the WebDAV container is up and responding to PROPFIND.
    """
    url = webdav_server["url"]
    auth = webdav_server["auth"]

    headers = {"Depth": "1", "Content-Type": "application/xml"}

    async with httpx.AsyncClient(auth=auth) as client:
        response = await client.request("PROPFIND", url, headers=headers)

    assert response.status_code == 207
    assert "multistatus" in response.text.lower()
    assert "root_file.txt" in response.text

    print(f"\nWebDAV Sanity Check Passed for: {url}")


@pytest.mark.asyncio
async def test_webdav_public_sanity(webdav_public_server):
    """
    Verifies that the Public WebDAV container is up and responding WITHOUT auth.
    """
    url = webdav_public_server

    headers = {"Depth": "1", "Content-Type": "application/xml"}

    async with httpx.AsyncClient() as client:
        response = await client.request("PROPFIND", url, headers=headers)

    assert response.status_code == 207
    assert "multistatus" in response.text.lower()
    assert "root_file.txt" in response.text

    print(f"\nWebDAV Public Sanity Check Passed for: {url}")


@pytest.mark.asyncio
async def test_webdav_options_includes_propfind(webdav_server):
    """
    Verifies that the OPTIONS response includes PROPFIND in the Allow header.
    This is how the ExtractorFactory will detect WebDAV support.
    """
    url = webdav_server["url"]
    auth = webdav_server["auth"]

    async with httpx.AsyncClient(auth=auth) as client:
        response = await client.request("OPTIONS", url)

    assert response.status_code == 200

    allow = response.headers.get("Allow", "")
    assert "PROPFIND" in allow, f"PROPFIND not found in Allow header: {allow}"

    dav = response.headers.get("DAV", "")
    assert dav, "DAV header is missing from OPTIONS response"

    print(f"\nOPTIONS check passed. Allow: {allow} | DAV: {dav}")
