import httpx
import pytest


@pytest.mark.asyncio
async def test_container_nginx(nginx_custom):
    async with httpx.AsyncClient(verify=False) as client:
        response = await client.get(f"{nginx_custom['https']}/json-data")
        assert response.status_code == 200


@pytest.mark.asyncio
async def test_container_nginx_stream_chunked(nginx_custom):
    async with httpx.AsyncClient(verify=False) as client:
        # Request with Accept-Encoding: gzip to trigger dynamic on-the-fly compression
        headers = {"Accept-Encoding": "gzip"}
        response = await client.get(
            f"{nginx_custom['http']}/stream_chunked/file_100k.bin", headers=headers
        )
        assert response.status_code == 200
        assert response.headers.get("Transfer-Encoding") == "chunked"
        assert "Content-Length" not in response.headers
        assert len(response.content) == 100 * 1024


@pytest.mark.asyncio
async def test_container_nginx_stream_no_resume(nginx_custom):
    async with httpx.AsyncClient(verify=False) as client:
        response = await client.get(
            f"{nginx_custom['http']}/stream_no_resume/file_100k.bin",
            headers={"Range": "bytes=0-0"},
        )
        # max_ranges 0 causes Nginx to return 200 OK instead of 206 Partial Content
        assert response.status_code == 200
        assert response.headers.get("Accept-Ranges") == "none"
