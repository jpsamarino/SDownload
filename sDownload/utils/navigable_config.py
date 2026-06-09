class NavigableExtensions:
    """Manages extensions that the crawler considers navigable."""

    _DEFAULT = {
        "html",
        "htm",
        "php",
        "json",
        "xml",
        "asp",
        "jsp",
        "aspx",
        "cgi",
        "js",
        "css",
    }

    __slots__ = ("_extensions",)

    def __init__(self, extensions: set[str] | None = None):
        self._extensions = set(extensions or self._DEFAULT)

    @staticmethod
    def _normalize(ext: str) -> str:
        """Normalize the extension: remove dot and lowercase."""
        return ext.lower().strip(".")

    def add(self, ext: str):
        self._extensions.add(self._normalize(ext))

    def __contains__(self, ext: str) -> bool:
        """Allow usage like: 'ext in navigable_extensions'"""
        return self._normalize(ext) in self._extensions

    def __iter__(self):
        """Allow iteration over extensions."""
        return iter(self._extensions)


navigable_extensions = NavigableExtensions()


class NavigableContentTypes:
    __slots__ = ("_patterns",)

    _DEFAULT = {"html", "json", "xml"}

    def __init__(self, patterns: set[str] | None = None):
        self._patterns = set(patterns or self._DEFAULT)

    def add(self, pattern: str):
        self._patterns.add(pattern.lower())

    def __contains__(self, content_type: str) -> bool:
        if not content_type:
            return False

        ct = content_type.lower()
        return any(pattern in ct for pattern in self._patterns)

    def __iter__(self):
        return iter(self._patterns)


navigable_content_types = NavigableContentTypes()
