from typing import Optional
from .protocol import ResourceExtractorProtocol
from .webdav_extractor import WebDavExtractor
from .json_extractor import JsonExtractor
from .text_pattern_extractor import TextPatternExtractor


class ExtractorFactory:
    """
    Static Registry for Parsers (Extractors).
    Maps Content-Type and HTTP Status to the appropriate Parser strategy.

    Now uses Singleton-like instances to avoid memory allocation for stateless classes.
    """

    _WEBDAV = WebDavExtractor()
    _JSON = JsonExtractor()
    _TEXT = TextPatternExtractor()

    @classmethod
    def get_extractor(
        cls, content_type: str, status_code: int
    ) -> Optional[ResourceExtractorProtocol]:
        """
        Returns the appropriate Parser for the given response metadata.
        """
        content_type = content_type.lower()

        # 1. Multi-Status always goes to WebDAV
        if status_code == 207:
            return cls._WEBDAV

        if "application/json" in content_type:
            return cls._JSON

        if any(t in content_type for t in ["text/", "xml", "javascript", "html"]):
            return cls._TEXT

        if not content_type:
            return cls._TEXT

        return None
