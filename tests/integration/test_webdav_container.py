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
