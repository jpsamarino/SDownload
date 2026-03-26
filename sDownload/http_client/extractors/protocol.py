from dataclasses import dataclass
from typing import Protocol
from enum import Enum


class DiscoveryMethod(Enum):
    GET = "GET"
    PROPFIND = "PROPFIND"
    POST = "POST"
    UNKNOWN = "UNKNOWN"


@dataclass
class ExtractedLink:
    """
    Represents a link found during parsing.
    is_dir can be True (confirmed directory), False (confirmed file),
    or None (unknown, needs probing).
    """

    url: str
    is_dir: bool | None = None
    method_hint: DiscoveryMethod = DiscoveryMethod.UNKNOWN


class ResourceExtractorProtocol(Protocol):
    """
    Protocol for resource parsers (HTML, JSON, WebDAV).
    Parsers are strictly synchronous and operate on raw strings/content.
    """

    def extract(self, content: str, base_url: str) -> list[ExtractedLink]:
        """
        Parses the content and returns a list of ExtractedLink objects.
        """
        ...
