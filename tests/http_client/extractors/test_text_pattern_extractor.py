import pytest
from sDownload.http_client.extractors.text_pattern_extractor import TextPatternExtractor


def test_text_pattern_extractor_finds_links():
    extractor = TextPatternExtractor()
    html = """
    <html>
        <body>
            <a href="page1.html">Link 1</a>
            <img src="/images/logo.png">
            <div data-url="https://external.com/api"></div>
        </body>
    </html>
    """
    base_url = "http://example.com/sub/"

    links = extractor.extract(html, base_url)
    urls = [l.url for l in links]

    assert "http://example.com/sub/page1.html" in urls
    assert "http://example.com/images/logo.png" in urls
    assert "https://external.com/api" in urls


def test_text_pattern_extractor_finds_absolute_urls_in_text():
    extractor = TextPatternExtractor()
    text = "Find this: 'https://other.com/ext.mp4' and maybe 'http://localhost:8080/data.csv'"

    links = extractor.extract(text, "http://any.com")
    urls = [l.url for l in links]

    assert "https://other.com/ext.mp4" in urls
    assert "http://localhost:8080/data.csv" in urls
    assert len(urls) == 2
