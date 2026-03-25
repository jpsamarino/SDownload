from dataclasses import dataclass
from typing import Protocol

@dataclass
class ExtractedLink:
    """
    Represents a link found during parsing.
    is_dir can be True (confirmed directory), False (confirmed file), 
    or None (unknown, needs probing).
    """
    url: str
    is_dir: bool | None = None

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
