import asyncio
import logging
import pytest
from datetime import datetime

from sDownload.file_system.local_storage import LocalStorage
from sDownload.http_client.httpx_downloader import HttpxDownloader
from sDownload.interfaces.protocols.http_config_model import HttpConfigModel
from sDownload.services.downloader_manager.download_task import DownloadConfig, DownloadTask


class DummyDownloader:
    async def download_chunk(self, url, start, end):
        _end = end if end is not None else 11
        data = b"123456780123"
        _data = data[start:_end+1]  # verify
        for i in range(0, len(_data), 2):
            chunk = _data[i:i+2]
            yield chunk
            await asyncio.sleep(0.01)


class DummyStorage:
    async def save_binary_data(self, name, it):
        async for _ in it:
            pass

    async def merge_binary_files(self, arg1, arg2):
        pass

    async def delete_data(self, arg1):
        pass


@pytest.fixture
def storage(tmp_path: str):
    return LocalStorage(storage_dir="./delet")
# https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-03/Estabelecimentos9.zip
# https://ftp.dadosabertos.ans.gov.br/FTP/PDA/dados_de_beneficiarios_por_operadora/sib_ativo_MG.zip
# https://storage.live.com/downloadfiles/V1/Zip?application=1141147648&authkey=!ANB8GHSnGvZzWFM


@pytest.mark.asyncio
async def test_httpx_and_dowload_task_real(storage):
    config = HttpConfigModel(timeout_connect=20.0, valid_ssl=False)
    downloader = HttpxDownloader(config)
    # result_list = await downloader.get_file_info(f"https://storage.live.com/downloadfiles/V1/Zip?application=1141147648&authkey=!ANB8GHSnGvZzWFM")
    result_list = await downloader.get_file_info(f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-03/Estabelecimentos9.zip")
    # result_list = await downloader.get_file_info(f"https://ftp.dadosabertos.ans.gov.br/FTP/PDA/dados_de_beneficiarios_por_operadora/sib_ativo_MG.zip")
    result = result_list[0]

    config = DownloadConfig(
        file_name=result.file_name,
        file_dir=result.file_dir,  # remove
        file_size=result.file_size,
        file_id=result.file_id,
        download_url=result.download_url,
        file_created_at=datetime.utcnow(),
        protocol_data=None,
        max_connections_per_download=10,
        max_speed_bytes_per_second=1024*1024*10,
    )

    logger = logging.getLogger("download-task")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    task = DownloadTask(
        cfg=config,
        downloader=downloader,
        storage=storage,
        logger=logger,
    )
    task.start()
    await task.wait_util_done()

    # # verify if the file was downloaded and the size is correct
    # received = b""
    # async for chunk in storage.get_binary_data(config.file_name):
    #     received += chunk

    # assert len(received) == config.file_size


@pytest.mark.asyncio
async def test_httpx_and_dowload_task(nginx_custom, storage):
    config = HttpConfigModel(timeout_connect=20.0)
    downloader = HttpxDownloader(config)
    result_list = await downloader.get_file_info(f"{nginx_custom['http']}/limited_speed/file_100k.bin")
    result = result_list[0]
    assert result.file_name == "file_100k.bin"
    assert result.file_size == 102400

    config = DownloadConfig(
        file_name=result.file_name,
        file_dir=result.file_dir,  # remove
        file_size=result.file_size,
        file_id=result.file_id,
        download_url=result.download_url,
        file_created_at=datetime.utcnow(),
        protocol_data=None,
        max_connections_per_download=2,
    )

    task = DownloadTask(
        cfg=config,
        downloader=downloader,
        storage=storage,
    )
    await task.start()

    # verify if the file was downloaded and the size is correct
    received = b""
    async for chunk in storage.get_binary_data(config.file_name):
        received += chunk

    assert len(received) == config.file_size


@pytest.mark.asyncio
async def test_download_task_logs_debug(caplog):
    config = DownloadConfig(
        file_name="test.bin",
        file_dir=None,
        file_size=12,
        file_id="123",
        download_url="http://example.com/file",
        file_created_at=datetime.utcnow(),
        protocol_data=None,
        max_connections_per_download=2,
    )

    logger = logging.getLogger("sDownload.test")
    logger.setLevel(logging.DEBUG)

    with caplog.at_level(logging.DEBUG, logger="sDownload.test"):
        task = DownloadTask(
            cfg=config,
            downloader=DummyDownloader(),
            storage=DummyStorage(),
            logger=logger
        )
        await task.start()

    # Mostra todos os logs capturados no terminal
    for record in caplog.records:
        print(f"{record.levelname}: {record.message}")

    # Teste opcional: verificar se mensagens de debug foram emitidas
    assert any("starting" in r.message.lower()
               or "chunk" in r.message.lower() for r in caplog.records)
