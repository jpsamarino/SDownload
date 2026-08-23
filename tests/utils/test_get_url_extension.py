from sDownload.utils.get_url_extension import get_url_extension


def test_get_url_extension():
    assert get_url_extension("https://site.com/file.zip") == "zip"
    assert get_url_extension("https://site.com/v1.0/archive.7z") == "7z"
    assert get_url_extension("https://site.com/document.PDF") == "pdf"
    assert get_url_extension("https://site.com/blabla.besdep") == "besdep"
    assert get_url_extension("https://site.com/page.html?id=123") == "html"
    assert get_url_extension("https://site.com/download.php?file=test.zip") == "php"
    assert get_url_extension("https://site.com/v2.0/folder/") == ""
    assert get_url_extension("https://site.com/v2.0/folder") == ""
    assert get_url_extension("https://site.com/no-extension") == ""
    assert get_url_extension("https://site.com/") == ""
