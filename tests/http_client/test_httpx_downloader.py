import pytest
from sDownload.http_client.httpx_downloader import HttpxDownloader
from sDownload.interfaces.protocols.http_config_model import HttpConfigModel


@pytest.mark.asyncio
async def test_httpx_downloader():
    config = HttpConfigModel()
    downloader = HttpxDownloader(config)
    result = await downloader.get_file_info("https://www.anatel.gov.br/dadosabertos/paineis_de_dados/areastarifarias/pgcn.zip")
    result2 = await downloader.get_file_info("https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-03/Empresas0.zip")
    result3 = await downloader.get_file_info("https://viacep.com.br/ws/01001000/json/")
    result4 = await downloader.get_file_info("https://viacep.com.br/ws/01001000/json?aa=20&bb=30")
    print(result)
    print(result2)
    print(result3)
