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
    async def save_binary_data(self, it, name):
        async for _ in it:
            pass


@pytest.fixture
def storage(tmp_path: str):
    return LocalStorage(storage_dir="./delet")


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
        max_connections_per_download=10,
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
