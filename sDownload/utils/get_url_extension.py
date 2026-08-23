from urllib.parse import urlparse


def get_url_extension(url: str) -> str:
    """
    Extracts the real extension of the URL (without inventing .bin and without query parameters).
    """
    path = urlparse(url).path
    if not path or path.endswith("/"):
        return ""

    last_part = path.split("/")[-1]

    return last_part.split(".")[-1].lower() if "." in last_part else ""
