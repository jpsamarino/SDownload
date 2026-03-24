import pytest
import httpx
from sDownload.http_client.extractors.html_extractor import HtmlExtractor


@pytest.mark.asyncio
async def test_html_extractor_integration_basics(nginx_custom):
    """
    1. Basic Discovery: Find expected links in the standard test page.
    """
    extractor = HtmlExtractor()
    url = f"{nginx_custom['http']}/scenarios_pages_html/teste1/"
    
    async with httpx.AsyncClient() as client:
        links = [l async for l in extractor.extract(url, client)]
        
    assert any("file_100k.bin" in l for l in links)
    assert any("level1/" in l for l in links)


@pytest.mark.asyncio
async def test_html_extractor_ignores_navigation_links(nginx_custom):
    """
    2. Navigation Filtering: Ensure it ignores . and ..
    """
    extractor = HtmlExtractor()
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
async def test_html_extractor_memory_limit(nginx_custom):
    """
    3. Memory Limit: Try to extract from a huge binary file (10MB).
    It should stop at 1MB and not crash or hang.
    """
    extractor = HtmlExtractor()
    url = f"{nginx_custom['http']}/default/file_10M.bin"
    
    async with httpx.AsyncClient() as client:
        # This will treat a binary file as text, but it should stop after 1MB
        links = [l async for l in extractor.extract(url, client)]
        
    # Should find 0 links in a random binary file, and it should return quickly
    assert len(links) == 0


@pytest.mark.asyncio
async def test_html_extractor_absolute_resolution(nginx_custom):
    """
    4. Absolute Resolution: Verify absolute paths and external links.
    """
    extractor = HtmlExtractor()
    url = f"{nginx_custom['http']}/scenarios_pages_html/teste1/level1/index.html"
    
    async with httpx.AsyncClient() as client:
        links = [l async for l in extractor.extract(url, client)]
        
    # The scenario has absolute links to /default/file_100k.bin
    assert any(l.endswith("/default/file_100k.bin") for l in links)
    # Check resolution of relative paths in level1
    assert any(l.endswith("level1/relative_file.txt") for l in links)


@pytest.mark.asyncio
async def test_html_extractor_ignores_anchors():
    """
    5. Anchor Filtering: Ensure it ignores links starting with #.
    """
    extractor = HtmlExtractor()
    base_url = "http://test.com/page.html"
    html = """<a href="#section1">Anchor</a><a href="real.html">Real</a>"""
    
    mock_response = httpx.Response(200, content=html.encode("utf-8"), request=httpx.Request("GET", base_url))
    mock_client = AsyncMock()
    # We need to simulate the stream context manager
    mock_stream = MagicMock()
    mock_stream.__aenter__.return_value = mock_response
    mock_client.stream.return_value = mock_stream
    # And the aiter_text
    mock_response.aiter_text = AsyncMock()
    mock_response.aiter_text.return_value = ["<a href=\"#section1\">Anchor</a>", "<a href=\"real.html\">Real</a>"].__iter__()
    
    # Actually, simpler: just test the LinkParser logic which is the core of this filter
    from sDownload.http_client.extractors.html_extractor import LinkParser
    parser = LinkParser(base_url)
    parser.feed(html)
    assert len(parser.links) == 1
    assert "real.html" in parser.links[0]

@pytest.mark.asyncio
async def test_html_extractor_deep_subfolder_slash(nginx_custom):
    """
    5. Subfolder Normalization: Ensure folders like level1/ are correctly captured.
    """
    extractor = HtmlExtractor()
    url = f"{nginx_custom['http']}/scenarios_pages_html/teste1/"
    async with httpx.AsyncClient() as client:
        links = [l async for l in extractor.extract(url, client)]
    
    # level1/ should be returned exactly as found or absolute
    assert any(l.endswith("/level1/") for l in links)
