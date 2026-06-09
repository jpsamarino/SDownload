import pytest

from sDownload.utils import url_to_file_name, normalize_url


@pytest.mark.parametrize(
    "url, expected",
    [
        ("https://example.com/files/data.json", "data.json"),
        (
            "https://example.com/api/search?q=test&limit=10",
            "api_search_q_test_limit_10.bin",
        ),
        ("https://x.com///path+++weird==chars", "path_weird_chars.bin"),
        ("https://example.com", "example_com.bin"),
        ("https://example.com?aa=1&bb=2", "aa_1_bb_2.bin"),
        ("https://site.com/files/report", "files_report.bin"),
        (
            "https://host.com/finalfile.verylongextension",
            "finalfile.verylongextension",
        ),  # verify max_length
        ("https://justdomain.com///?", "justdomain_com.bin"),
        ("https://long.com/" + "a" * 200, ("a" * 100) + ".bin"),
    ],
)
def test_url_to_file_name(url, expected):
    result = url_to_file_name(url)
    assert result == expected


def test_respects_max_length():
    url = "https://truncate.com/" + "x" * 200
    result = url_to_file_name(url, max_length=50)
    assert result.endswith(".bin")
    assert len(result) == 50 + 4  # filename length + ".bin"


BASE_URL = "https://example.com/subpath/"


@pytest.mark.parametrize(
    "raw_link, expected",
    [
        # Absolute URL
        ("https://example.com/file.png", "https://example.com/file.png"),
        # Relative URL
        ("/images/logo.png", "https://example.com/images/logo.png"),
        # Relative URL with query
        (
            "/_next/image?url=%2Fimages%2Foperadoras%2F301949-h.png&amp;w=1080&amp;q=75",
            "https://example.com/_next/image?url=%2Fimages%2Foperadoras%2F301949-h.png&w=1080&q=75",
        ),
        # Relative path without leading slash
        ("docs/readme.md", "https://example.com/subpath/docs/readme.md"),
        # URL with HTML entities
        ("/page?param=1&amp;other=2", "https://example.com/page?param=1&other=2"),
        # Empty link
        ("", None),
        # Only whitespace
        ("   ", None),
        # protocol-relative URL
        ("//cdn.example.com/lib.js", "https://cdn.example.com/lib.js"),
        ("https://example.com/subpath/", "https://example.com/subpath/"),
        ("http://example.com/subpath/", "http://example.com/subpath/"),
        ("https://example.com/subpath/#fragment", "https://example.com/subpath/"),
    ],
)
def test_normalize_url(raw_link, expected):
    result = normalize_url(raw_link, BASE_URL)
    assert result == expected
