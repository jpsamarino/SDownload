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
    urls = [link.url for link in links]

    assert "http://example.com/files/1.zip" in urls
    assert "https://other.com/ext.mp4" in urls
    assert len(urls) == 2
    assert all(link.is_dir is None for link in links)
