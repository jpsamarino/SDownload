import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock
from sDownload.http_client.extractors.json_extractor import JsonExtractor


@pytest.mark.asyncio
async def test_json_extractor_finds_urls():
    extractor = JsonExtractor()
    json_content = """
    {
        "status": "ok",
        "file_url": "http://example.com/files/1.zip",
        "path": "/internal/path/2.bin",
        "nested": {
            "more_urls": ["https://other.com/ext.mp4", "relative/path/3.txt"]
        }
    }
    """
    
    # We test the parser regex logic directly for simplicity
    links = []
    seen = set()
    for match in extractor._ABS_URL_REGEX.finditer(json_content):
        raw = match.group(2).strip()
        if raw not in seen:
            seen.add(raw)
            links.append(raw)
            
    # The new brutal Regex only looks for ABSOLUTE URLs starting with http:// or https://
    # It will find 1.zip and ext.mp4, but skip the relative paths (2.bin and 3.txt) 
    # because they don't look like absolute URLs in raw text.
    assert "http://example.com/files/1.zip" in links
    assert "https://other.com/ext.mp4" in links
    assert len(links) == 2
@pytest.mark.asyncio
async def test_json_extractor_integration(nginx_custom):
    """
    Real integration test against the Nginx test server.
    """
    extractor = JsonExtractor()
    url = f"{nginx_custom['http']}/json-data"
    
    async with httpx.AsyncClient() as client:
        links = [l async for l in extractor.extract(url, client)]
        
    # The Dockerfile generates: 
    # {"message":"hello","links": ["http://localhost/default/file_100k.bin", ...]}
    assert any("file_100k.bin" in l for l in links)
    assert any("file_1M.bin" in l for l in links)
    assert len(links) >= 2
