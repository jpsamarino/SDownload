import pytest
from sDownload.http_client.extractors.json_extractor import JsonExtractor


def test_json_extractor_finds_urls():
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
    
    links = extractor.extract(json_content, "http://any.com")
    urls = [l.url for l in links]
    
    assert "http://example.com/files/1.zip" in urls
    assert "https://other.com/ext.mp4" in urls
    assert len(urls) == 2
    assert all(l.is_dir is None for l in links)
