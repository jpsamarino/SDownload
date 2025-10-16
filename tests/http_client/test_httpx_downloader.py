import pytest
from sDownload.http_client.httpx_downloader import HttpxDownloader
from sDownload.interfaces.protocols.http_config_model import HttpConfigModel


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
    result_list = await downloader.get_file_info(
        f"{nginx_custom['http']}/default/file_100k.bin"
    )
    result = result_list[0]
    assert result.file_name == "file_100k.bin"
    assert result.file_size == 102400
    assert result.server_accept_ranges is True


@pytest.mark.asyncio
async def test_httpx_get_file_info_without_range_support(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result_list = await downloader.get_file_info(
        f"{nginx_custom['http']}/no_resume/file_100M.bin"
    )
    result = result_list[0]
    assert result.file_name == "file_100M.bin"
    assert result.file_size == 104857600
    assert result.server_accept_ranges is False


@pytest.mark.asyncio
async def test_httpx_get_file_info_with_wrong_url(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    with pytest.raises(Exception):
        await downloader.get_file_info(f"{nginx_custom['http']}/its_not_there")


@pytest.mark.asyncio
async def test_httpx_get_file_info_json_and_data_returns(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result_list = await downloader.get_file_info(f"{nginx_custom['http']}/json-data")
    result = result_list[0]
    assert result.file_name == "json_data.bin"
    assert result.server_accept_ranges is False


@pytest.mark.asyncio
async def test_httpx_get_file_info_no_name_in_url(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=20.0)
    downloader = HttpxDownloader(config)
    result_list = await downloader.get_file_info(f"{nginx_custom['http']}/no_filename")
    result = result_list[0]
    assert result.file_name == "no_filename.bin"
    assert result.file_size == 1048576


@pytest.mark.asyncio
async def test_httpx_get_file_info_https_without_valid_ssl(nginx_custom):
    config = HttpConfigModel(timeout_connect_s=20.0, valid_ssl=False)
    downloader = HttpxDownloader(config)
    result_list = await downloader.get_file_info(
        f"{nginx_custom['https']}/default/file_100k.bin"
    )
    result = result_list[0]
    assert result.file_name == "file_100k.bin"
    assert result.file_size == 102400
    assert result.server_accept_ranges is True
    with pytest.raises(Exception):
        # should raise because https has a self-signed certificate
        config_ssl = HttpConfigModel(timeout_connect_s=20.0, valid_ssl=True)
        downloader_ssl = HttpxDownloader(config_ssl)
        await downloader_ssl.get_file_info(
            f"{nginx_custom['https']}/default/file_100k.bin"
        )


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
        chunk
        async for chunk in downloader.download_chunk(
            url, start_byte=1000, end_byte=1999
        )
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
    with pytest.raises(Exception):
        _ = [
            chunk
            async for chunk in downloader.download_chunk(
                url, start_byte=200000, end_byte=300000
            )
        ]


@pytest.mark.asyncio
async def test_download_chunk_invalid_url():
    config = HttpConfigModel(timeout_connect_s=1.0, valid_ssl=False)
    downloader = HttpxDownloader(config)
    bad_url = "http://localhost:9999/nonexistent.bin"
    with pytest.raises(Exception):
        _ = [chunk async for chunk in downloader.download_chunk(bad_url)]
