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
        "txt",
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

    def remove(self, ext: str):
        self._extensions.discard(self._normalize(ext))

    def reset_to_default(self):
        """Restore default extensions."""
        self._extensions = set(self._DEFAULT)

    def set_all(self, extensions: set[str]):
        """Replace all extensions with a new set."""
        self._extensions = {self._normalize(ext) for ext in extensions}

    def get_all(self) -> set[str]:
        """Return all registered extensions."""
        return self._extensions

    def __contains__(self, ext: str) -> bool:
        """Allow usage like: 'ext in navigable_extensions'"""
        return self._normalize(ext) in self._extensions

    def __iter__(self):
        """Allow iteration over extensions."""
        return iter(self._extensions)


navigable_extensions = NavigableExtensions()
