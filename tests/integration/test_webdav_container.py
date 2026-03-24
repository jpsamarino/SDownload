import httpx
import pytest


@pytest.mark.asyncio
async def test_webdav_sanity(webdav_server):
    """
    Verifies that the WebDAV container is up and responding to PROPFIND.
    """
    url = webdav_server

    # PROPFIND is the standard WebDAV method for listing resources
    # We use Depth: 1 to get the root directory content
    headers = {"Depth": "1", "Content-Type": "application/xml"}

    # Simple PROPFIND body to ask for basic properties
    # Most WebDAV servers respond to an empty body PROPFIND with all properties
    async with httpx.AsyncClient(auth=("admin", "admin")) as client:
        response = await client.request("PROPFIND", url, headers=headers)

    # WebDAV success code for PROPFIND is 207 Multi-Status
    assert response.status_code == 207
    assert "multistatus" in response.text.lower()

    # Check if our sample file is listed
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
        # Should NOT require auth
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
    url = webdav_server

    async with httpx.AsyncClient(auth=("admin", "admin")) as client:
        response = await client.request("OPTIONS", url)

    assert response.status_code == 200

    allow = response.headers.get("Allow", "")
    assert "PROPFIND" in allow, f"PROPFIND not found in Allow header: {allow}"

    # Bonus: check DAV header exists (standard WebDAV indicator)
    dav = response.headers.get("DAV", "")
    assert dav, "DAV header is missing from OPTIONS response"

    print(f"\nOPTIONS check passed. Allow: {allow} | DAV: {dav}")
