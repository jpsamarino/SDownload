import pytest
from sDownload.http_client.extractors.factory import ExtractorFactory
from sDownload.http_client.extractors.webdav_extractor import WebDavExtractor
from sDownload.http_client.extractors.json_extractor import JsonExtractor
from sDownload.http_client.extractors.text_pattern_extractor import TextPatternExtractor


def test_factory_returns_correct_parsers_by_content_type():
    # WebDAV (status 207)
    assert ExtractorFactory.get_extractor("application/xml", 207) is ExtractorFactory._WEBDAV
    
    # JSON
    assert ExtractorFactory.get_extractor("application/json", 200) is ExtractorFactory._JSON
    
    # HTML/Text
    assert ExtractorFactory.get_extractor("text/html", 200) is ExtractorFactory._TEXT
    assert ExtractorFactory.get_extractor("application/javascript", 200) is ExtractorFactory._TEXT
    
    # Binaries should return None
    assert ExtractorFactory.get_extractor("application/zip", 200) is None
    assert ExtractorFactory.get_extractor("image/png", 200) is None


def test_factory_fallback_on_empty_content_type():
    # If no content-type is provided, fallback to text parser
    assert ExtractorFactory.get_extractor("", 200) is ExtractorFactory._TEXT
