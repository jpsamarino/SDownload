import re
from urllib.parse import urlparse


def url_to_file_name(url: str, max_length: int = 100) -> str:
    parsed = urlparse(url)

    common_name = parsed.path.rstrip('/').split('/')[-1]
    if common_name and re.search(r'\.[a-zA-Z0-9]+$', common_name):
        return common_name

    relevant = f"{parsed.path}_{parsed.query}" if parsed.query else parsed.path
    filename = re.sub(r'[^a-zA-Z0-9]+', '_', relevant).strip('_')

    if filename == "":
        filename = re.sub(r'[^a-zA-Z0-9]+', '_', parsed.netloc).strip('_')

    return filename[-max_length:] + ".bin"
