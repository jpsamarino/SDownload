import pytest
from sDownload.http_client.extractors.webdav_extractor import WebDavExtractor


def test_webdav_extractor_parses_xml_structure():
    extractor = WebDavExtractor()
    xml_content = """<?xml version="1.0" encoding="utf-8"?>
    <d:multistatus xmlns:d="DAV:">
        <d:response>
            <d:href>/public.php/dav/files/user/folder1/</d:href>
            <d:propstat>
                <d:prop>
                    <d:resourcetype><d:collection/></d:resourcetype>
                </d:prop>
                <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
        <d:response>
            <d:href>/public.php/dav/files/user/file.txt</d:href>
            <d:propstat>
                <d:prop>
                    <d:resourcetype/>
                </d:prop>
                <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
    </d:multistatus>
    """
    base_url = "http://localhost/public.php/dav/files/user/"
    
    links = extractor.extract(xml_content, base_url)
    
    # 1. Should find folder1 as a directory
    folder1 = next(l for l in links if "folder1" in l.url)
    assert folder1.is_dir is True
    assert folder1.url == "http://localhost/public.php/dav/files/user/folder1/"
    
    # 2. Should find file.txt as a file
    file1 = next(l for l in links if "file.txt" in l.url)
    assert file1.is_dir is False
    assert file1.url == "http://localhost/public.php/dav/files/user/file.txt"


def test_webdav_extractor_skips_self_reference():
    extractor = WebDavExtractor()
    xml_content = """<?xml version="1.0" encoding="utf-8"?>
    <d:multistatus xmlns:d="DAV:">
        <d:response>
            <d:href>/current/</d:href>
            <d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop></d:propstat>
        </d:response>
    </d:multistatus>
    """
    # If base_url is the same as the folder in XML, it should be skipped
    links = extractor.extract(xml_content, "http://host/current/")
    assert len(links) == 0
