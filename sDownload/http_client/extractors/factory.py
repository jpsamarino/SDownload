from .json_extractor import JsonExtractor
from .protocol import DiscoveryMethod, ResourceExtractorProtocol
from .text_pattern_extractor import TextPatternExtractor
from .webdav_extractor import WebDavExtractor


class ExtractorFactory:
    """
    Static Registry for Parsers (Extractors).
    Maps Content-Type and DiscoveryMethod to the appropriate Parser strategy.
    Uses Singleton-like instances to avoid memory allocation for stateless classes.
    """

    _WEBDAV = WebDavExtractor()
    _JSON = JsonExtractor()
    _TEXT = TextPatternExtractor()

    @classmethod
    def get_extractor(
        cls, content_type: str, method: DiscoveryMethod
    ) -> ResourceExtractorProtocol | None:
        """
        Returns the appropriate Parser for the given response metadata.
        """
        content_type = content_type.lower()

        if method == DiscoveryMethod.PROPFIND:
            return cls._WEBDAV

        if "application/json" in content_type:
            return cls._JSON

        if any(t in content_type for t in ["text/", "xml", "javascript", "html"]):
            return cls._TEXT

        if not content_type:
            return cls._TEXT

        return None
