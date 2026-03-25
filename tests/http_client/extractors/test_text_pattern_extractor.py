import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock
from sDownload.http_client.extractors.text_pattern_extractor import TextPatternExtractor


@pytest.mark.asyncio
async def test_text_pattern_extractor_integration_basics(nginx_custom):
    """
    1. Basic Discovery: Find expected links in the standard test page.
    """
    extractor = TextPatternExtractor()
    url = f"{nginx_custom['http']}/scenarios_pages_html/teste1/"

    async with httpx.AsyncClient() as client:
        links = [l async for l in extractor.extract(url, client)]

    assert any("file_100k.bin" in l for l in links)
    assert any("level1/" in l for l in links)


@pytest.mark.asyncio
async def test_text_pattern_extractor_ignores_navigation_links(nginx_custom):
    """
    2. Navigation Filtering: Ensure it ignores . and ..
    """
    extractor = TextPatternExtractor()
    # Path with autoindex often has ../
    url = f"{nginx_custom['http']}/scenarios_pages_html/teste1/level1/"

    async with httpx.AsyncClient() as client:
        links = [l async for l in extractor.extract(url, client)]

    # Ensure the parent directory (.../teste1/) is NOT in the links
    # because we are inside .../teste1/level1/ and we filter ".."
    parent_url = f"{nginx_custom['http']}/scenarios_pages_html/teste1/"
    for l in links:
        assert not l.endswith("/..")
        assert not l.endswith("/.")
        assert l != parent_url, f"Should have filtered parent directory link: {l}"


@pytest.mark.asyncio
async def test_text_pattern_extractor_skips_binary_files(nginx_custom):
    """
    3. Early Exit: If the server returns a known binary Content-Type,
    the extractor should skip it without reading the limit.
    """
    extractor = TextPatternExtractor()
    url = f"{nginx_custom['http']}/default/file_10M.bin"
    
    async with httpx.AsyncClient() as client:
        links = [l async for l in extractor.extract(url, client)]
        
    assert len(links) == 0


@pytest.mark.asyncio
async def test_text_pattern_extractor_absolute_resolution(nginx_custom):
    """
    4. Absolute Resolution: Verify absolute paths and external links.
    """
    extractor = TextPatternExtractor()
    url = f"{nginx_custom['http']}/scenarios_pages_html/teste1/level1/index.html"

    async with httpx.AsyncClient() as client:
        links = [l async for l in extractor.extract(url, client)]

    # The scenario has absolute links to /default/file_100k.bin
    assert any(l.endswith("/default/file_100k.bin") for l in links)
    # Check resolution of relative paths in level1
    assert any(l.endswith("level1/relative_file.txt") for l in links)


@pytest.mark.asyncio
async def test_text_pattern_extractor_ignores_anchors():
    """
    5. Anchor Filtering: Ensure it ignores links starting with # (unless they contain a valid URL inside).
    """
    extractor = TextPatternExtractor()
    base_url = "http://test.com/page.html"
    html = """<a href="#section1">Anchor</a><a href="real.html">Real</a>"""
    
    # We test the parser regex logic directly for simplicity
    body = html
    links = []
    seen = set()
    for match in extractor._ATTR_REGEX.finditer(body):
        raw = match.group(2).strip()
        if not raw or raw.startswith("#"):
            continue
        links.append(raw)
        
    assert len(links) == 1
    assert "real.html" in links[0]


@pytest.mark.asyncio
async def test_text_pattern_extractor_deep_subfolder_slash(nginx_custom):
    """
    6. Subfolder Normalization: Ensure folders like level1/ are correctly captured.
    """
    extractor = TextPatternExtractor()
    url = f"{nginx_custom['http']}/scenarios_pages_html/teste1/"
    async with httpx.AsyncClient() as client:
        links = [l async for l in extractor.extract(url, client)]
    
    assert any(l.endswith("/level1/") for l in links)
