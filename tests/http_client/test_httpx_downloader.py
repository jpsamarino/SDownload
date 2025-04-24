import pytest
from sDownload.http_client.httpx_downloader import HttpxDownloader
from sDownload.interfaces.protocols.http_config_model import HttpConfigModel


# @pytest.mark.asyncio
# async def test_httpx_downloader():
#     config = HttpConfigModel(timeout_connect=20.0)
#     downloader = HttpxDownloader(config)
#     result = await downloader.get_file_info("https://www.anatel.gov.br/dadosabertos/paineis_de_dados/areastarifarias/pgcn.zip")
#     result2 = await downloader.get_file_info("https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-03/Empresas0.zip")
#     result3 = await downloader.get_file_info("https://viacep.com.br/ws/01001000/json/")
#     result4 = await downloader.get_file_info("https://viacep.com.br/ws/01001000/json?aa=20&bb=30")
#     print(result)
#     print(result2)
#     print(result3)


@pytest.mark.asyncio
async def test_httpx_get_file_info_common_case(nginx_custom):
    config = HttpConfigModel(timeout_connect=20.0)
    downloader = HttpxDownloader(config)
    result = await downloader.get_file_info(f"{nginx_custom['http']}/default/file_100k.bin")
    assert result.file_name == "file_100k.bin"
    assert result.file_size == 102400
    assert result.server_accept_ranges is True


@pytest.mark.asyncio
async def test_httpx_get_file_info_without_range_support(nginx_custom):
    config = HttpConfigModel(timeout_connect=20.0)
    downloader = HttpxDownloader(config)
    result = await downloader.get_file_info(f"{nginx_custom['http']}/no_resume/file_100M.bin")
    assert result.file_name == "file_100M.bin"
    assert result.file_size == 104857600
    assert result.server_accept_ranges is False


@pytest.mark.asyncio
async def test_httpx_get_file_info_with_wrong_url(nginx_custom):
    config = HttpConfigModel(timeout_connect=20.0)
    downloader = HttpxDownloader(config)
    with pytest.raises(Exception):
        await downloader.get_file_info(f"{nginx_custom['http']}/its_not_there")


@pytest.mark.asyncio
async def test_httpx_get_file_info_json_and_data_returns(nginx_custom):
    config = HttpConfigModel(timeout_connect=20.0)
    downloader = HttpxDownloader(config)
    result = await downloader.get_file_info(f"{nginx_custom['http']}/json-data")
    assert result.file_name == "json_data.bin"
    assert result.server_accept_ranges is False


@pytest.mark.asyncio
async def test_httpx_get_file_info_no_name_in_url(nginx_custom):
    config = HttpConfigModel(timeout_connect=20.0)
    downloader = HttpxDownloader(config)
    result = await downloader.get_file_info(f"{nginx_custom['http']}/no_filename")
    assert result.file_name == "no_filename.bin"
    assert result.file_size == 1048576


@pytest.mark.asyncio
async def test_httpx_get_file_info_https_without_valid_ssl(nginx_custom):
    config = HttpConfigModel(timeout_connect=20.0, valid_ssl=False)
    downloader = HttpxDownloader(config)
    result = await downloader.get_file_info(f"{nginx_custom['https']}/default/file_100k.bin")
    assert result.file_name == "file_100k.bin"
    assert result.file_size == 102400
    assert result.server_accept_ranges is True
    with pytest.raises(Exception):
        # should raise because https has a self-signed certificate
        config_ssl = HttpConfigModel(timeout_connect=20.0, valid_ssl=True)
        downloader_ssl = HttpxDownloader(config_ssl)
        await downloader_ssl.get_file_info(f"{nginx_custom['https']}/default/file_100k.bin")
