import re
from urllib.parse import urlparse, urljoin, urlunparse
import html


def url_to_file_name(url: str, max_length: int = 100) -> str:
    parsed = urlparse(url)

    common_name = parsed.path.rstrip("/").split("/")[-1]
    if common_name and re.search(r"\.[a-zA-Z0-9]+$", common_name):
        return common_name

    relevant = f"{parsed.path}_{parsed.query}" if parsed.query else parsed.path
    filename = safe_slugify(relevant) or safe_slugify(parsed.netloc)
    return filename[-max_length:] + ".bin"


def safe_slugify(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", s).strip("_")


def normalize_url(raw_link: str, base_url: str) -> str | None:
    """
    Converts a raw link from HTML/JS/CSS into an absolute, normalized URL.
    Handles:
        - Relative URLs
        - HTML entities (e.g., &amp;)
    """
    if not raw_link:
        return None

    link = html.unescape(raw_link.strip())
    if not link:
        return None

    absolute = urljoin(base_url, link)
    parsed = urlparse(absolute)
    absolute = urlunparse(parsed._replace(fragment=""))

    return absolute
