import pytest

from sDownload.exceptions import DownloadRequestError, SDownloadError
from sDownload.http_client.httpx_downloader import HttpxDownloader
from sDownload.interfaces.models import HttpConfigModel

# @pytest.mark.asyncio
# async def test_httpx_downloader():
#     config = HttpConfigModel(timeout_connect=20)
#     downloader = HttpxDownloader(config)
#     result_list = await downloader.get_file_info("https://www.anatel.gov.br/dadosabertos/paineis_de_dados/areastarifarias/pgcn.zip")
#     result2 = await downloader.get_file_info("https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-03/Empresas0.zip")
#     result3 = await downloader.get_file_info("https://viacep.com.br/ws/01001000/json/")
#     result4 = await downloader.get_file_info("https://viacep.com.br/ws/01001000/json?aa=20&bb=30")
#     print(result)
#     print(result2)
#     print(result3)


@pytest.mark.asyncio
async def test_httpx_get_file_info_common_case(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result = await downloader.get_file_info(f"{nginx_custom['http']}/default/file_100k.bin")
    assert result.file_name == "file_100k.bin"
    assert result.file_size == 102400
    assert result.server_accept_ranges is True


@pytest.mark.asyncio
async def test_httpx_get_file_info_without_range_support(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result = await downloader.get_file_info(f"{nginx_custom['http']}/no_resume/file_10M.bin")
    assert result.file_name == "file_10M.bin"
    assert result.file_size == 10485760
    assert result.server_accept_ranges is False


@pytest.mark.asyncio
async def test_httpx_get_file_info_with_wrong_url(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    with pytest.raises(SDownloadError):
        await downloader.get_file_info(f"{nginx_custom['http']}/its_not_there")


@pytest.mark.asyncio
async def test_httpx_get_file_info_json_and_data_returns(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result = await downloader.get_file_info(f"{nginx_custom['http']}/json-data")
    assert result.file_name == "json_data.bin"
    assert result.server_accept_ranges is False


@pytest.mark.asyncio
async def test_httpx_get_file_info_no_name_in_url(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result = await downloader.get_file_info(f"{nginx_custom['http']}/no_filename")
    assert result.file_name == "no_filename.bin"
    assert result.file_size == 1048576


@pytest.mark.asyncio
async def test_httpx_get_file_info_https_without_valid_ssl(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=20.0, valid_ssl=False)
    downloader = HttpxDownloader(config)
    result = await downloader.get_file_info(f"{nginx_custom['https']}/default/file_100k.bin")
    assert result.file_name == "file_100k.bin"
    assert result.file_size == 102400
    assert result.server_accept_ranges is True
    with pytest.raises(SDownloadError):
        # should raise because https has a self-signed certificate
        config_ssl = HttpConfigModel(timeout_connect_s=20.0, valid_ssl=True)
        downloader_ssl = HttpxDownloader(config_ssl)
        await downloader_ssl.get_file_info(f"{nginx_custom['https']}/default/file_100k.bin")


@pytest.mark.asyncio
async def test_download_chunk_full(nginx_custom):

    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    url = f"{nginx_custom['http']}/default/file_100k.bin"
    chunks = [chunk async for chunk in downloader.download_chunk(url)]
    data = b"".join(chunks)
    assert len(data) == 102400


@pytest.mark.asyncio
async def test_download_chunk_partial(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    url = f"{nginx_custom['http']}/default/file_100k.bin"
    partial = [
        chunk async for chunk in downloader.download_chunk(url, start_byte=1000, end_byte=1999)
    ]
    data = b"".join(partial)
    assert len(data) == 1000
    full = b"".join([c async for c in downloader.download_chunk(url)])
    assert data == full[1000:2000]


@pytest.mark.asyncio
async def test_download_chunk_invalid_range(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=20.0, valid_ssl=False)
    downloader = HttpxDownloader(config)
    url = f"{nginx_custom['http']}/default/file_100k.bin"
    with pytest.raises(Exception) as exc_info:
        _ = [
            chunk
            async for chunk in downloader.download_chunk(url, start_byte=200000, end_byte=300000)
        ]
    assert isinstance(exc_info.value, DownloadRequestError)


@pytest.mark.asyncio
async def test_download_chunk_invalid_end_range(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=20.0, valid_ssl=False)
    downloader = HttpxDownloader(config)
    url = f"{nginx_custom['http']}/default/file_100k.bin"
    # download only a part of the file (from byte 102390 to byte 102400)
    partial = [
        chunk async for chunk in downloader.download_chunk(url, start_byte=102390, end_byte=511990)
    ]
    data = b"".join(partial)
    total_size = 100 * 1024
    assert len(data) == total_size - 102390


@pytest.mark.asyncio
async def test_download_chunk_invalid_url():
    from sDownload.exceptions import DownloadRequestError

    config = HttpConfigModel(timeout_connect_s=1.0, valid_ssl=False)
    downloader = HttpxDownloader(config)
    bad_url = "http://localhost:9999/nonexistent.bin"
    with pytest.raises(DownloadRequestError):
        async for _ in downloader.download_chunk(bad_url):
            pass


@pytest.mark.asyncio
async def test_httpx_get_file_info_404(nginx_custom):
    from sDownload.exceptions import ResourceNotFoundError

    config = HttpConfigModel(timeout_connect_s=5.0)
    downloader = HttpxDownloader(config)
    with pytest.raises(ResourceNotFoundError):
        await downloader.get_file_info(f"{nginx_custom['http']}/non_existent_file")


@pytest.mark.asyncio
async def test_download_chunk_404(nginx_custom):
    from sDownload.exceptions import ResourceNotFoundError

    config = HttpConfigModel(timeout_connect_s=5.0)
    downloader = HttpxDownloader(config)
    url = f"{nginx_custom['http']}/non_existent_file"
    with pytest.raises(ResourceNotFoundError):
        async for _ in downloader.download_chunk(url):
            pass


@pytest.mark.asyncio
async def test_httpx_get_file_info_timeout():
    from sDownload.exceptions import DownloadTimeoutError

    # Use a non-routable IP to force a timeout
    config = HttpConfigModel(timeout_connect_s=0.001)
    downloader = HttpxDownloader(config)
    with pytest.raises(DownloadTimeoutError):
        await downloader.get_file_info("http://10.255.255.1/timeout")


@pytest.mark.asyncio
async def test_download_chunk_timeout():
    from sDownload.exceptions import DownloadTimeoutError

    config = HttpConfigModel(timeout_connect_s=0.001)
    downloader = HttpxDownloader(config)
    with pytest.raises(DownloadTimeoutError):
        async for _ in downloader.download_chunk("http://10.255.255.1/timeout"):
            pass


@pytest.mark.asyncio
async def test_httpx_list_resources_html_scraping(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=5.0)
    downloader = HttpxDownloader(config)

    # Test discovery page (Level 2 to hit and confirm the files)
    url = f"{nginx_custom['http']}/scenarios_pages_html/teste1/"
    resources = [r async for r in downloader.list_resources(url, level=2)]

    # Should find file_100k.bin (Confirmed as file by the Scout at level 2)
    assert any("file_100k.bin" in r for r in resources)
    # Should NOT find 'level1/' as a file (endswith check is safer)
    assert not any(r.endswith("level1/") for r in resources)


@pytest.mark.asyncio
async def test_httpx_list_resources_recursive(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=5.0)
    downloader = HttpxDownloader(config)

    # Test recursive depth Level 3 (Root + Level 1 + Level 2 files)
    url = f"{nginx_custom['http']}/scenarios_pages_html/teste1/level1/"
    resources = [r async for r in downloader.list_resources(url, level=3)]

    # Level 1 should have: relative_file.txt, file_100k.bin (absolute link)
    # Level 2 should have: leaf.txt, file_1M.bin (absolute link)
    assert any("relative_file.txt" in r for r in resources)
    assert any("file_100k.bin" in r for r in resources)
    assert any("leaf.txt" in r for r in resources)
    assert any("file_1M.bin" in r for r in resources)


@pytest.mark.asyncio
async def test_httpx_list_resources_recursive_with_pattern(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=5.0)
    downloader = HttpxDownloader(config)
    url = f"{nginx_custom['http']}/scenarios_pages_html/teste1/level1/"
    resources = [r async for r in downloader.list_resources(url, level=3, pattern=r".bin$")]

    assert any("file_100k.bin" in r for r in resources)
    assert any("file_1M.bin" in r for r in resources)


@pytest.mark.asyncio
async def test_httpx_list_resources_with_regex(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=5.0)
    downloader = HttpxDownloader(config)

    url = f"{nginx_custom['http']}/scenarios_pages_html/teste1/level1/"
    # Only find files with "leaf" in name (Level 3 to reach and confirm leaf.txt)
    resources = [r async for r in downloader.list_resources(url, pattern=r"leaf", level=3)]

    assert any("leaf.txt" in r for r in resources)
    assert len(resources) == 1


# @pytest.mark.asyncio
# async def test_httpx_real():
#     config = HttpConfigModel(timeout_connect_s=5.0)
#     downloader = HttpxDownloader(config)

#     url = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK/Dados/Cadastros/CNPJ/"
#     # Only find files with "leaf" in name
#     resources = [r async for r in downloader.list_resources(url, level=2)]

#     assert len(resources) > 1


# @pytest.mark.asyncio
# async def test_httpx_real2():
#     config = HttpConfigModel(timeout_connect_s=5.0)
#     downloader = HttpxDownloader(config)

#     url = f"https://seu-convenio.com"
#     # Only find files with "leaf" in name
#     resources = [r async for r in downloader.list_resources(url, level=1)]

#     assert len(resources) > 1
