import pytest

from sDownload.utils.url_to_file_name import url_to_file_name


@pytest.mark.parametrize("url, expected", [
    ("https://example.com/files/data.json", "data.json"),
    ("https://example.com/api/search?q=test&limit=10",
     "api_search_q_test_limit_10.bin"),
    ("https://x.com///path+++weird==chars", "path_weird_chars.bin"),
    ("https://example.com", "example_com.bin"),
    ("https://example.com?aa=1&bb=2", "aa_1_bb_2.bin"),
    ("https://site.com/files/report", "files_report.bin"),
    ("https://host.com/finalfile.verylongextension",
     "finalfile.verylongextension"),  # verify max_length
    ("https://justdomain.com///?", "justdomain_com.bin"),
    ("https://long.com/" + "a" * 200, ("a" * 100) + ".bin"),
])
def test_url_to_file_name(url, expected):
    result = url_to_file_name(url)
    assert result == expected


def test_respects_max_length():
    url = "https://truncate.com/" + "x" * 200
    result = url_to_file_name(url, max_length=50)
    assert result.endswith(".bin")
    assert len(result) == 50 + 4  # filename length + ".bin"
